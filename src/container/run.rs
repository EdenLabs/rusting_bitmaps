use std::ops::{Deref, DerefMut};
use std::io;
use std::iter;
use std::mem;
use std::ptr;
use std::fmt;
use std::iter::Iterator;
use std::slice;

use tinybit::Endian;

use crate::container::*;
use crate::container::array_ops;

/// A RLE word storing the value and the length of that run
/// 
/// # Remarks
/// Type is marked `[repr(c)]` to match the roaring spec
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Rle16 {
    /// The value of the run
    pub value: u16,

    /// The number of values in this run
    pub length: u16
}

impl Rle16 {
    /// Create a new rle run
    pub fn new(value: u16, length: u16) -> Self {
        Self {
            value,
            length
        }
    }

    /// The last value in the run
    #[inline] 
    pub fn end(self) -> u16 {
        self.value + self.length
    }

    /// Get the start and end value of the run
    #[inline]
    pub fn range(self) -> (u16, u16) {
        (self.value, self.value + self.length + 1)
    }
    
    #[inline]
    pub fn into_range(self) -> Range<u32> {
        u32::from(self.value)..u32::from(self.value + self.length + 1)
    }
}

impl fmt::Debug for Rle16 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{:?}, {:?}]", self.value, self.length)
    }
}

/// The result for a binary search
#[allow(clippy::enum_variant_names)] // Clippy is wrong in this instance
enum SearchResult {
    /// An exact match was found, the index is contained
    ExactMatch(usize),
    
    /// A match was not found but there was a place it could be inserted, the index is contained 
    PossibleMatch(usize),

    /// No match was found
    NoMatch
}

/// A container using run length encoding to store it's values.
/// 
/// # Structure
/// Runs are stored as `Rle16` words
#[derive(Clone, Debug)]
pub struct RunContainer {
    /// The rle encoded runs of this run container
    runs: Vec<Rle16>,

    /// The cardinality of this container. Lazily computed on demand if dirty
    cardinality: LazyCardinality
}

impl RunContainer {
    /// Create a new run container
    pub fn new() -> Self {
        Self {
            runs: Vec::new(),
            cardinality: LazyCardinality::none()
        }
    }
    
    /// Create a new run container with a specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            runs: Vec::with_capacity(capacity),
            cardinality: LazyCardinality::none()
        }
    }
    
    /// Shrink the run container's backing memory to fit it's contents
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.runs.shrink_to_fit()
    }
    
    /// Add a value to the run container
    pub fn add(&mut self, value: u16) {
        match self.binary_search(value) {
            SearchResult::ExactMatch(_index) => { },
            SearchResult::PossibleMatch(index) => {
                let v = self.runs[index];
                let offset = value - v.value;
                
                if offset <= v.length {
                    return;
                }

                if offset == v.length + 1 {
                    if index + 1 < self.runs.len() {
                        // Check if necessary to fuse, if so fuse the runs
                        let v1 = self.runs[index + 1];
                        if v1.value == value + 1 {
                            self.runs[index].length = v1.end() - v.value;
                            self.runs.remove(index + 1);
                            return;
                        }
                    }

                    self.runs[index].length += 1;
                    return;
                }

                if index + 1 < self.runs.len() {
                    // Check if necessary to fuse. If so fuse the runs
                    let v1 = &mut self.runs[index + 1];
                    if v1.value == value + 1 {
                        v1.value = value;
                        v1.length += 1;
                        return;
                    }
                }

                self.runs.insert(index + 1, Rle16::new(value, 0));
            },
            SearchResult::NoMatch => {
                // Check if the run needs extended, if so extend it
                if !self.runs.is_empty() {
                    let v0 = &mut self.runs[0];
                    if v0.value == value + 1 {
                        v0.length += 1;
                        v0.value -= 1;
                        return;
                    }
                }

                self.runs.push(Rle16::new(value, 0));
            }
        }
    }
    
    /// Add all values in the range [min-max) to the run container
    pub fn add_range(&mut self, range: Range<u32>) {
        let min = range.start;
        let max = range.end;

        let runs_min = self.rle_count_less(min);
        let runs_max = self.rle_count_greater(max);

        let common = self.runs.len() - runs_min - runs_max;
        if common == 0 {
            self.runs.insert(
                runs_min,
                Rle16::new(min as u16, (max - min - 1) as u16)
            );
        }
        else {
            let common_min = self.runs[runs_min].value;
            let common_max = self.runs[runs_min + common - 1].end();
            let result_min = common_min.min(min as u16);
            let result_max = common_max.max(max as u16);

            self.runs[runs_min] = Rle16::new(result_min, result_max - result_min);
            self.runs.splice((runs_min + 1)..runs_max, iter::empty());
        }
    }
    
    /// Remove a value from the run container
    pub fn remove(&mut self, value: u16) -> bool {
        match self.binary_search(value) {
            SearchResult::ExactMatch(index) => {
                let rle = &mut self.runs[index];
                if rle.length == 0 {
                    self.runs.remove(index);
                }
                else {
                    rle.value += 1;
                    rle.length -= 1;
                }

                true
            },
            SearchResult::PossibleMatch(prev_index) => {
                let rle = self.runs[prev_index];
                let offset = value - rle.value;
                
                if offset < rle.length {
                    self.runs[prev_index].length = offset - 1;

                    let new_rle = Rle16::new(value + 1, rle.length - offset - 1);
                    self.runs.insert(prev_index + 1, new_rle);

                    return true;
                }
                
                if offset == rle.length {
                    self.runs[prev_index].length -= 1;
                    return true;
                }

                false
            },
            SearchResult::NoMatch => {
                false
            }
        }
    }
    
    /// Remove all values in the range [min-max) from the run container
    pub fn remove_range(&mut self, range: Range<u32>) {
        let min = range.start as u16;
        let max = (range.end - 1) as u16;

        let mut si0 = None;
        let mut si1 = None;

        // Update the left-most run
        match self.find_run(min) {
            SearchResult::ExactMatch(mut index) => {
                let run = self.runs[index as usize];
                if min > run.value && max < run.end() {
                    // Split into two runs

                    // Right interval
                    self.runs.insert(index as usize + 1, Rle16::new(max + 1, run.end() - (max + 1)));

                    // Left interval
                    self.runs[index as usize].length = (min - 1) - run.value;
                    return;
                }

                if min > run.value {
                    self.runs[index as usize].length = (min - 1) - run.value;
                    index += 1;
                }

                si0 = Some(index);
            },
            SearchResult::PossibleMatch(index) => {
                si0 = Some(index);
            },
            _ => ()
        }

        // Update the right-most run
        match self.find_run(max) {
            SearchResult::ExactMatch(mut index) => {
                let run_max = self.runs[index as usize].end();
                if run_max > max {
                    self.runs[index as usize] = Rle16::new(max + 1, run_max - (max + 1));
                    index = index.checked_sub(1)
                        .unwrap_or(0);
                }

                si1 = Some(index);
            },
            SearchResult::PossibleMatch(index) => {
                si1 = Some(index);                
            },
            _ => ()
        }

        if let (Some(si0), Some(si1)) = (si0, si1) {
            let start = si0 + 1;
            let end = start + (si1 + 1);

            if end < self.runs.len() {
                self.runs.splice(start..end, iter::empty());
            }
        }
    }
    
    /// Check if a value is in the run container
    pub fn contains(&self, value: u16) -> bool {
        match self.binary_search(value) {
            SearchResult::ExactMatch(_index) => {
                true
            },
            SearchResult::PossibleMatch(index) => {
                let v = self.runs[index];

                value - v.value <= v.length
            },
            SearchResult::NoMatch => {
                false
            }
        }
    }
    
    /// Check if the container contains all the values in [min-max)
    pub fn contains_range(&self, range: Range<u32>) -> bool {
        let mut count = 0;
        let index;

        let min = range.start as u16;
        let max = (range.end - 1) as u16;

        match self.binary_search(min) {
            SearchResult::ExactMatch(i) => {
                index = i;
            },
            SearchResult::PossibleMatch(i) => {
                let v = self.runs[i];

                if min - v.value > v.length {
                    return false;
                }
                else {
                    index = i;
                }
            },
            SearchResult::NoMatch => {
                return false;
            }
        };

        for run in &self.runs[index..self.runs.len()] {
            let stop = run.value + run.length;
            if run.value >= max {
                break;
            }

            if stop >= max {
                if max > run.value {
                    count += max - run.value;
                }

                break;
            }

            let min_length = {
                if stop > min { 
                    stop - min 
                }
                else {
                    0
                }
            };

            if min_length < run.length {
                count += min_length;
            }
            else {
                count += run.length;
            }
        }

        count >= max - min - 1
    }
    
    /// The cardinality of the run container
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.cardinality.get(|| self.compute_cardinality())
    }

    /// Compute the cardinality of this run container
    fn compute_cardinality(&self) -> usize {
        let mut card = self.len(); // Accounts for runs with a 0 length
        for rle in self.iter_runs() {
            card += rle.length as usize;
        }
        
        card
    }

    /// The number of runs in the run container
    #[inline]
    pub fn num_runs(&self) -> usize {
        self.runs.len()
    }

    /// Check whether the container is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }
    
    /// Check whether the container is full
    pub fn is_full(&self) -> bool {
        if self.runs.is_empty() {
            return false;
        }

        let run = self.runs[0];
        let len = self.runs.len();

        len == 1 && run.value == 0 && run.length == std::u16::MAX
    }

    /// Get the minimum value of this container
    pub fn min(&self) -> Option<u16> {
        if self.runs.is_empty() {
            return None;
        }

        Some(self.runs[0].value)
    }
    
    /// Get the maximum value of this container
    pub fn max(&self) -> Option<u16> {
        if self.runs.is_empty() {
            return None;
        }

        let run = self.runs[self.runs.len() - 1];
        
        Some(run.value + run.length)
    }
    
    /// Get the rank of a value in the set. The relative position of an element in the set
    pub fn rank(&self, value: u16) -> usize {
        let mut sum = 0;
        for run in self.runs.iter() {
            let start = run.value;
            let length = run.length;
            let end = start + length;

            if value < end {
                if value < start {
                    break;
                }
                
                return (sum + (value - start) + 1) as usize;
            }
            else {
                sum += length + 1;
            }
        }

        sum as usize
    }

    /// Select the element with `rank` starting the search from `start_rank`
    pub fn select(&self, rank: u32, start_rank: &mut u32) -> Option<u16> {
        for run in self.runs.iter() {
            let length = u32::from(run.length);
            let value = u32::from(run.value);

            if rank <= *start_rank + length {
                return Some((value + rank - *start_rank) as u16);
            }
            else {
                *start_rank += length + 1;
            }
        }

        None
    }

    /// Convert self into the most efficient container. Returns self if already optimal
    pub fn into_efficient_container(self) -> Container {
        let cardinality = self.cardinality();
        let size_as_run = RunContainer::serialized_size(self.num_runs());
        let size_as_bitset = BitsetContainer::serialized_size();
        let size_as_array = ArrayContainer::serialized_size(cardinality);
        let min_size_other = size_as_array.min(size_as_bitset);

        // Run is still smallest, leave as is
        if size_as_run < min_size_other {
            return Container::Run(self);
        }

        // Array is smallest, convert
        if cardinality < DEFAULT_MAX_SIZE {
            return Container::Array(self.into());
        }

        // Bitset is smallest, convert
        Container::Bitset(self.into())
    }
    
    /// Iterate over the values of the run container
    pub fn iter(&self) -> Iter {
        Iter {
            runs: &self.runs,
            rle_index: 0,
            value_index: 0
        }
    }
    
    /// Iterate over the runs of the run container
    #[inline]
    pub fn iter_runs(&self) -> slice::Iter<Rle16> {
        self.runs.iter()
    }

    /// Perform a binary search for a given key in the set
    fn binary_search(&self, key: u16) -> SearchResult {
        if self.runs.is_empty() {
            return SearchResult::NoMatch;
        }

        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low <= high {
            let middle = (low + high) >> 1;
            let value = self.runs[middle].value;

            if value < key {
                low = middle + 1;
            }
            else if value > key {
                high = middle - 1;
            }
            else {
                return SearchResult::ExactMatch(middle);
            }
        }

        if low == 0 {
            SearchResult::NoMatch
        }
        else {
            SearchResult::PossibleMatch(low - 1)
        }
    }

    /// Find the run containing `key`
    fn find_run(&self, key: u16) -> SearchResult {
        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low <= high {
            let middle = (low + high) >> 1;
            let min = self.runs[middle].value;
            let max = self.runs[middle].end();
            if key > max {
                low = middle + 1;
            }
            else if key < min {
                high = middle - 1;
            }
            else {
                return SearchResult::ExactMatch(middle);
            }
        }

        if low == 0 {
            SearchResult::NoMatch
        }
        else {
            SearchResult::PossibleMatch(low - 1)
        }
    }

    /// Get the number of runs before `value` 
    fn rle_count_less(&self, value: u32) -> usize {
        if self.runs.is_empty() {
            return 0;
        }

        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low <= high {
            let middle = (low + high) >> 1;
            let min_value = u32::from(self.runs[middle].value);
            let max_value = min_value + u32::from(self.runs[middle].length);

            if max_value + 1 < value {
                low = middle + 1;
            }
            else if value < min_value {
                high = middle - 1;
            }
            else {
                return middle;
            }
        }

        low
    }

    /// Get the number of runs after `value`
    fn rle_count_greater(&self, value: u32) -> usize {
        if self.runs.is_empty() {
            return 0;
        }

        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low <= high {
            let middle = (low + high) >> 1;
            let min_value = u32::from(self.runs[middle].value);
            let max_value = min_value + u32::from(self.runs[middle].length);

            if max_value < value {
                low = middle + 1;
            }
            else if value + 1 < min_value {
                high = middle - 1;
            }
            else {
                return self.runs.len() - (middle + 1);
            }
        }

        self.runs.len() - low
    }
}

impl RunContainer {
    /// Get the size in bytes of a container with `num_runs`
    pub fn serialized_size(num_runs: usize) -> usize {
        mem::size_of::<u16>() + mem::size_of::<Rle16>() * num_runs
    }

    /// Serialize the run container into the provided writer
    #[cfg(target_endian = "little")]
    pub fn serialize<W: Write>(&self, buf: &mut W) -> io::Result<usize> {
        let mut num_written = 0;

        num_written += (self.num_runs() as u16)
            .write_le(buf)?;

        unsafe {
            let ptr = self.as_ptr() as *const u8;
            let num_bytes = self.num_runs() * mem::size_of::<Rle16>();
            let slice = slice::from_raw_parts(ptr, num_bytes);
            num_written += buf.write(slice)?;
        }

        Ok(num_written)
    }

    /// Deserialize a run container from the provided buffer
    #[cfg(target_endian = "little")]
    pub fn deserialize<R: Read>(buf: &mut R) -> io::Result<Self> {
        let num_runs = u16::read_le(buf)? as usize;

        let mut result = Self::with_capacity(num_runs);

        unsafe {
            let num_bytes = num_runs * mem::size_of::<Rle16>();
            let ptr = result.as_mut_ptr() as *mut u8;
            let slice = slice::from_raw_parts_mut(ptr, num_bytes);

            let num_read = buf.read(slice)?;
            if num_read != num_bytes {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
        }

        Ok(result)
    }
}

impl From<ArrayContainer> for RunContainer {
    #[inline]
    fn from(container: ArrayContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut ArrayContainer> for RunContainer {
    #[inline]
    fn from(container: &'a mut ArrayContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a ArrayContainer> for RunContainer {
    fn from(container: &'a ArrayContainer) -> Self {
        if container.is_empty() {
            return RunContainer::new();
        }
        
        let mut result = RunContainer::with_capacity(container.num_runs());

        let mut run: Option<Rle16> = None;
        for value in container.iter() {
            if let Some(mut r) = run.as_mut() {
                // The next value isn't in the current run, push and start the next run
                if *value != r.end() + 1 {
                    result.runs.push(*r);

                    run = Some(Rle16::new(*value, 0));
                }
                // Next value is in the run, just increment
                else {
                    r.length += 1;
                }
            }
            else {
                run = Some(Rle16::new(*value, 0));
            }
        }

        if let Some(r) = run {
            result.runs.push(r);
        }

        result
    }
}

impl From<BitsetContainer> for RunContainer {
    #[inline]
    fn from(container: BitsetContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut BitsetContainer> for RunContainer {
    #[inline]
    fn from(container: &'a mut BitsetContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a BitsetContainer> for RunContainer {
    fn from(container: &'a BitsetContainer) -> Self {
        if container.is_empty() {
            return RunContainer::new();
        }
        
        let mut result = RunContainer::with_capacity(container.num_runs());

        let mut run: Option<Rle16> = None;
        for value in container.iter() {
            if let Some(mut r) = run.as_mut() {
                // The next value isn't in the current run, push and start the next run
                if value != r.end() + 1 {
                    result.runs.push(*r);

                    run = Some(Rle16::new(value, 0));
                }
                // Next value is in the run, just increment
                else {
                    r.length += 1;
                }
            }
            else {
                run = Some(Rle16::new(value, 0));
            }
        }

        if let Some(r) = run {
            result.runs.push(r);
        }

        result
    }
}

impl PartialEq for RunContainer {
    fn eq(&self, other: &RunContainer) -> bool {
        self.runs == other.runs
    }
}

impl Deref for RunContainer {
    type Target = [Rle16];

    fn deref(&self) -> &Self::Target {
        &self.runs
    }
}

impl DerefMut for RunContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runs
    }
}

impl SetOr<Self> for RunContainer {
    fn or(&self, other: &Self) -> Container {
        // If one of the containers is empty or completely full
        // just append the other one as it represents the full set
        if self.is_empty() || other.is_full() {
            return Container::Run(other.clone());
        }
        
        if other.is_empty() || self.is_full() {
            return Container::Run(self.clone());
        }

        let mut result = RunContainer::with_capacity(self.num_runs() + other.num_runs());
        let mut i_a = 0;
        let mut i_b = 0;
        let mut prev;

        let mut run_a = self.runs[i_a];
        let mut run_b = other.runs[i_b];
        
        if run_a.value <= run_b.value {
            result.runs.push(run_a);
            
            prev = run_a;
            i_a += 1;
        }
        else {
            result.runs.push(run_b);
            
            prev = run_b;
            i_b += 1;
        }
        
        while i_a < self.runs.len() && i_b < other.runs.len() {
            run_a = self.runs[i_a];
            run_b = other.runs[i_b];

            let new_run = {
                if run_a.value <= run_b.value {
                    i_a += 1;
                    run_a
                }
                else {
                    i_b += 1;
                    run_b
                }
            };

            append(&mut result.runs, new_run, &mut prev);
        }

        while i_a < self.runs.len() {
            append(&mut result.runs, self.runs[i_a], &mut prev);
            i_a += 1;
        }

        while i_b < other.runs.len() {
            append(&mut result.runs, other.runs[i_b], &mut prev);
            i_b += 1;
        }

        Container::Run(result)
    }
    
    fn inplace_or(mut self, other: &Self) -> Container {
        // Self contains the final result
        if self.is_full() || other.is_empty() {
            return Container::Run(self);
        }

        // Other contians the final result
        if other.is_full() || self.is_empty() {
            self.runs.clear();
            self.runs.reserve(other.runs.len());
            self.runs.extend_from_slice(&other.runs);

            return Container::Run(self);
        }

        // Check for and reserve enough space to hold the contents
        let max_runs = self.num_runs() + other.num_runs();
        self.runs.reserve(max_runs);

        unsafe {
            let num_runs = self.num_runs();
    
            // Move the current contents to the end of the array
            let src = self.runs.as_ptr();
            let dst = src.add(max_runs) as *mut _;

            ptr::copy_nonoverlapping(src, dst, num_runs);

            // Set the length to 0 to act as an unfilled vector
            self.runs.set_len(0);

            // At this point all the contents from 0..max_runs is random memory
            // that will have the final result copied into it
            // and the current values are at max_runs..len
            
            let ptr_old = src.add(max_runs);
            let mut pos_0 = 0;
            let mut pos_1 = 0;

            let mut prev_run;
            let self_run = *(ptr_old.add(pos_0));
            let other_run = *other.get_unchecked(pos_1);

            if self_run.value <= other_run.value { 
                self.runs.push(self_run);
                prev_run = self_run;

                pos_0 += 1;
            }
            else {
                self.runs.push(other_run);
                prev_run = other_run;

                pos_1 += 1;
            }

            while pos_1 < other.num_runs() && pos_0 < num_runs {
                let self_run = *(ptr_old.add(pos_0));
                let other_run = *other.get_unchecked(pos_1);

                let new_run;
                if self_run.value <= other_run.value {
                    new_run = self_run;
                    pos_0 += 1;
                }
                else {
                    new_run = other_run;
                    pos_1 += 1;
                }

                append(&mut self.runs, new_run, &mut prev_run);
            }

            while pos_1 < other.num_runs() {
                append(&mut self.runs, other.runs[pos_1], &mut prev_run);
                pos_1 += 1;
            }

            while pos_0 < num_runs {
                append(&mut self.runs, *(ptr_old.add(pos_0)), &mut prev_run);
                pos_0 += 1;
            }

            // After this point we don't care what happens with the values.
            // There's also no need to drop them since they're `Copy` and 
            // are guaranteed to not require dropping
        }

        Container::Run(self)
    }
}

impl SetOr<ArrayContainer> for RunContainer {
    fn or(&self, other: &ArrayContainer) -> Container {
        if self.is_full() {
            return Container::Run(self.clone());
        }

        let mut result = RunContainer::with_capacity(other.cardinality() + self.runs.len());

        let mut rle_index = 0;
        let mut array_index = 0;
        let mut prev_rle;

        if self.runs[rle_index].value <= other[array_index] {
            prev_rle = self.runs[rle_index];
            result.runs.push(prev_rle);
            rle_index += 1;
        }
        else {
            prev_rle = Rle16::new(other[array_index], 0);
            result.runs.push(prev_rle);
            array_index += 1;
        }

        while rle_index < self.runs.len() && array_index < other.cardinality() {
            if self.runs[rle_index].value <= other[array_index] {
                append(&mut result.runs, self.runs[rle_index], &mut prev_rle);
                rle_index += 1;
            }
            else {
                append_value(&mut result.runs, other[array_index], &mut prev_rle);
                array_index += 1;
            }
        }

        if array_index < other.cardinality() {
            while array_index < other.cardinality() {
                append_value(&mut result.runs, other[array_index], &mut prev_rle);
                array_index += 1;
            }
        }
        else {
            while rle_index < self.runs.len() {
                append(&mut result.runs, self.runs[rle_index], &mut prev_rle);
                rle_index += 1;
            }
        }

        Container::Run(result)
    }

    fn inplace_or(mut self, other: &ArrayContainer) -> Container {
        if self.is_full() {
            return Container::Run(self);
        }

        // Make sure there's enough room to fit the new runs
        let max_runs = other.cardinality() + self.num_runs();
        self.runs.reserve(max_runs);

        unsafe {
            // Move the original contents of the run to the end of the buffer
            let len = self.runs.len();
            let src = self.as_ptr();
            let dst = (src as *mut Rle16).add(max_runs);

            ptr::copy_nonoverlapping(src, dst, len);

            self.runs.truncate(0);

            let ptr_old = dst as *const Rle16;
            
            let mut pos_run = 0;
            let mut pos_arr = 0;
            let mut prev_rle;

            let rle = *(ptr_old.add(pos_run));
            let val = other[pos_arr];
            if rle.value < val {
                self.runs.push(rle);
                
                prev_rle = rle;
                pos_run += 1;
            }
            else {
                let new_rle = Rle16::new(val, 0);
                self.runs.push(new_rle);

                prev_rle = new_rle;
                pos_arr += 1;
            }

            while pos_run < len && pos_arr < other.len() {
                let rle = *(ptr_old.add(pos_run));
                let val = other[pos_arr];

                if rle.value < val {
                    append(&mut self.runs, rle, &mut prev_rle);
                    pos_run += 1;
                }
                else {
                    append_value(&mut self.runs, val, &mut prev_rle);
                    pos_arr += 1;
                }
            }

            if pos_arr < other.len() {
                while pos_arr < other.len() {
                    append_value(&mut self.runs, other[pos_arr], &mut prev_rle);
                    pos_arr += 1;
                }
            }
            else {
                while pos_run < len {
                    append(&mut self.runs, *(ptr_old.add(pos_run)), &mut prev_rle);
                    pos_run += 1;
                }
            }
        }

        Container::Run(self)
    }
}

impl SetOr<BitsetContainer> for RunContainer {
    fn or(&self, other: &BitsetContainer) -> Container {
        let mut result = other.clone();

        for rle in self.iter_runs() {
            let min = u32::from(rle.value);
            let max = u32::from(rle.end() + 1);

            result.set_range(min..max);
        }

        Container::Bitset(result)
    }

    fn inplace_or(self, other: &BitsetContainer) -> Container {
        if self.is_full() {
            return Container::Run(self)
        }

        let mut result = other.clone();
        for run in self.iter_runs() {
            result.set_range(run.into_range());
        }

        Container::Bitset(result)
    }
}

impl SetAnd<Self> for RunContainer {
    fn and(&self, other: &Self) -> Container {
        if self.is_full() {
            return Container::Run(other.clone());
        }

        if other.is_full() {
            return Container::Run(self.clone());
        }

        let req_cap = self.num_runs() + other.num_runs();
        let mut result = RunContainer::with_capacity(req_cap);
        
        let mut i0 = 0;
        let mut i1 = 0;

        let mut start0 = self.runs[i0].value;
        let mut start1 = other.runs[i1].value;
        let mut end0 = self.runs[i0].end() + 1;
        let mut end1 = other.runs[i1].end() + 1;

        while i0 < self.num_runs() && i1 < other.num_runs() {
            // Runs don't overlap, advance either or
            if end0 <= start1 {
                i0 += 1;

                if i0 < self.num_runs() {
                    start0 = self.runs[i0].value;
                    end0 = self.runs[i0].end() + 1;
                }
            }
            else if end1 <= start0 {
                i1 += 1;

                if i1 < other.num_runs() {
                    start1 = other.runs[i1].value;
                    end1 = other.runs[i1].end() + 1;
                }
            }
            // Runs overlap, try to merge if possible
            else {
                let last_start = start0.max(start1);
                let first_end;

                if end0 == end1 {
                    first_end = end0;
                    i0 += 1;
                    i1 += 1;

                    if i0 < self.num_runs() {
                        start0 = self.runs[i0].value;
                        end0 = self.runs[i0].end() + 1;
                    }

                    if i1 < other.num_runs() {
                        start1 = other.runs[i1].value;
                        end1 = other.runs[i1].end() + 1;
                    }
                }
                else if end0 < end1 {
                    first_end = end0;

                    i0 += 1;
                    if i0 < self.num_runs() {
                        start0 = self.runs[i0].value;
                        end0 = self.runs[i0].end() + 1;
                    }
                }
                else {
                    first_end = end1;

                    i1 += 1;
                    if i1 < other.num_runs() {
                        start1 = other.runs[i1].value;
                        end1 = other.runs[i1].end() + 1;
                    }
                }

                let run = Rle16::new(last_start, first_end - last_start - 1);
                result.runs.push(run);
            }
        }

        result.into_efficient_container()
    }

    fn and_cardinality(&self, other: &Self) -> usize {
        if self.is_full() {
            return other.cardinality();
        }
        
        if other.is_full() {
            return self.cardinality();
        }

        let mut card = 0;
        
        let mut i0 = 0;
        let mut i1 = 0;

        let mut start0 = self.runs[i0].value;
        let mut start1 = other.runs[i1].value;
        let mut end0 = self.runs[i0].end() + 1;
        let mut end1 = other.runs[i1].end() + 1;

        while i0 < self.num_runs() && i1 < other.num_runs() {
            // Runs don't overlap, advance either or
            if end0 <= start1 {
                i0 += 1;

                if i0 < self.num_runs() {
                    start0 = self.runs[i0].value;
                    end0 = self.runs[i0].end() + 1;
                }
            }
            else if end1 <= start0 {
                i1 += 1;

                if i1 < other.num_runs() {
                    start1 = other.runs[i1].value;
                    end1 = other.runs[i1].end() + 1;
                }
            }
            // Runs overlap, try to merge if possible
            else {
                let last_start = start0.max(start1);
                let first_end;

                if end0 == end1 {
                    first_end = end0;
                    i0 += 1;
                    i1 += 1;

                    if i0 < self.num_runs() {
                        start0 = self.runs[i0].value;
                        end0 = self.runs[i0].end() + 1;
                    }

                    if i1 < other.num_runs() {
                        start1 = other.runs[i1].value;
                        end1 = other.runs[i1].end() + 1;
                    }
                }
                else if end0 < end1 {
                    first_end = end0;

                    i0 += 1;
                    if i0 < self.num_runs() {
                        start0 = self.runs[i0].value;
                        end0 = self.runs[i0].end() + 1;
                    }
                }
                else {
                    first_end = end1;

                    i1 += 1;
                    if i1 < other.num_runs() {
                        start1 = other.runs[i1].value;
                        end1 = other.runs[i1].end() + 1;
                    }
                }

                card += (first_end - last_start) as usize;
            }
        }

        card
    }

    fn inplace_and(self, other: &Self) -> Container {
        SetAnd::and(&self, other)
    }
}

impl SetAnd<ArrayContainer> for RunContainer {
    fn and(&self, other: &ArrayContainer) -> Container {
        if self.is_full() {
            return Container::Array(other.clone());
        }

        if self.runs.is_empty() {
            return Container::Array(ArrayContainer::new());
        }

        let mut result = ArrayContainer::with_capacity(other.cardinality());

        let mut rle_index = 0;
        let mut array_index = 0;
        let mut rle = self.runs[rle_index];

        while array_index < other.cardinality() {
            let value = other[array_index];
            
            while rle.end() < value {
                rle_index += 1;

                if rle_index == self.runs.len() {
                    break;
                }

                rle = self.runs[rle_index];
            }

            if rle.value > value {
                array_index = array_ops::advance_until(
                    &other,
                    array_index,
                    rle.value
                );
            }
            else {
                result.push(value);
                array_index += 1;
            }
        }

        Container::Array(result)
    }

    fn and_cardinality(&self, other: &ArrayContainer) -> usize {
        if self.is_full() {
            return other.cardinality();
        }

        if self.runs.is_empty() {
            return 0;
        }

        let mut card = 0;

        let mut rle_index = 0;
        let mut array_index = 0;
        let mut rle = self.runs[rle_index];

        while array_index < other.cardinality() {
            let value = other[array_index];
            
            while rle.end() < value {
                rle_index += 1;

                if rle_index == self.runs.len() {
                    break;
                }

                rle = self.runs[rle_index];
            }

            if rle.value > value {
                array_index = array_ops::advance_until(
                    &other,
                    array_index,
                    rle.value
                );
            }
            else {
                card += 1;
                array_index += 1;
            }
        }

        card
    }

    fn inplace_and(self, other: &ArrayContainer) -> Container {
        SetAnd::and(&self, other)
    }
}

impl SetAnd<BitsetContainer> for RunContainer {
    fn and(&self, other: &BitsetContainer) -> Container {
        if self.is_full() {
            return Container::Bitset(other.clone());
        }

        let mut card = self.cardinality();
        if card <= DEFAULT_MAX_SIZE {
            if card > other.cardinality() {
                card = other.cardinality();
            }

            let mut array = ArrayContainer::with_capacity(card);
            for run in self.runs.iter() {
                let min = run.value as u16;
                let max = run.end() as u16;

                for value in min..=max {
                    if other.contains(value) {
                        array.push(value);
                    }
                }
            }

            Container::Array(array)
        }
        else {
            let mut bitset = other.clone();

            // Unset all bits in between the runs
            let mut start = 0;
            for run in self.runs.iter() {
                let end = u32::from(run.value);
                bitset.unset_range(start..end);

                start = end + u32::from(run.length) + 1;
            }

            bitset.unset_range(start..(1 << 16));

            if bitset.cardinality() > DEFAULT_MAX_SIZE {
                Container::Bitset(bitset)
            }
            else {
                Container::Array(bitset.into())
            }
        }
    }

    fn and_cardinality(&self, other: &BitsetContainer) -> usize {
        if self.is_full() {
            return other.cardinality();
        }

        let mut card = 0;
        for run in self.runs.iter() {
            card += other.cardinality_range(run.into_range());
        }

        card
    }
    
    fn inplace_and(self, other: &BitsetContainer) -> Container {
        SetAnd::and(&self, other)
    }
}

impl SetAndNot<Self> for RunContainer {
    fn and_not(&self, other: &Self) -> Container {
        // Self or other is the empty set, by definition the and_not is the same as self
        if self.is_empty() || other.is_empty() {
            return Container::Run(self.clone());
        }

        let mut result = RunContainer::with_capacity(self.runs.len() + other.runs.len());

        let mut i_a = 0;
        let mut i_b = 0;

        let (mut start_a, mut start_b, mut end_a, mut end_b) = {
            let run_a = self.runs[i_a];
            let run_b = other.runs[i_b];

            (
                run_a.value,
                run_b.value,
                run_a.end() + 1,
                run_b.end() + 1
            )
        };

        while i_a < self.runs.len() && i_b < other.runs.len() {
            if end_a <= start_b {
                result.runs.push(
                    Rle16::new(start_a, end_a - start_a - 1)
                );

                i_a += 1;
                if i_a < self.runs.len() {
                    let run = self.runs[i_a];
                    start_a = run.value;
                    end_a = run.end() + 1;
                }
            }
            else if end_b <= start_a {
                i_b += 1;
                if i_b < other.runs.len() {
                    let run = other.runs[i_b];
                    start_b = run.value;
                    end_b = run.end() + 1;
                }
            }
            else {
                if start_a < start_b {
                    result.runs.push(
                        Rle16::new(start_a, start_b - start_a - 1)
                    );
                }
                
                if end_b < end_a {
                    start_a = end_b;
                }
                else {
                    i_a += 1;
                    if i_a < self.runs.len() {
                        let run = self.runs[i_a];
                        start_a = run.value;
                        end_a = run.end() + 1;
                    }
                }
            }
        }

        if i_a < self.runs.len() {
            result.runs.push(Rle16::new(start_a, end_a - start_a - 1));

            i_a += 1;
            if i_a < self.runs.len() {
                result.runs.extend_from_slice(&self.runs[i_a..]);
            }
        }

        result.into_efficient_container()
    }

    fn inplace_and_not(self, other: &Self) -> Container {
        SetAndNot::and_not(&self, other)
    }
}

impl SetAndNot<ArrayContainer> for RunContainer {
    fn and_not(&self, other: &ArrayContainer) -> Container {
        const ARBITRARY_THRESHOLD: usize = 32;

        let cardinality = self.cardinality();

        if cardinality <= ARBITRARY_THRESHOLD {
            if other.cardinality() == 0 {
                return Container::Run(self.clone());
            }

            let mut result = RunContainer::with_capacity(cardinality + other.cardinality());
            let mut rle0_pos = 0;
            let mut rle1_pos = 0;

            let rle = self.runs[rle0_pos];
            let mut rle0_start = rle.value;
            let mut rle0_end = rle.end();
            let mut rle1_start = other[rle1_pos];

            while rle0_pos < self.num_runs() && rle1_pos < other.cardinality() {
                if rle0_end == rle1_start {
                    result.runs.push(Rle16::new(rle0_start, rle0_end - rle0_start - 1));
                    rle0_pos += 1;

                    if rle0_pos < self.num_runs() {
                        let r = self.runs[rle0_pos];

                        rle0_start = r.value;
                        rle0_end = r.end();
                    }
                }
                else if rle1_start < rle0_start {
                    rle1_pos += 1;

                    if rle1_pos < other.cardinality() {
                        rle1_start = other[rle1_pos];
                    }
                }
                else {
                    if rle0_start < rle1_start {
                        result.runs.push(Rle16::new(rle0_start, rle1_start - rle0_start - 1));
                    }

                    if rle1_start + 1 < rle0_end {
                        rle0_start = rle1_start + 1;
                    }
                    else {
                        rle0_pos += 1;

                        if rle0_pos < self.num_runs() {
                            let r = self.runs[rle0_pos];

                            rle0_start = r.value;
                            rle0_end = r.end();
                        }
                    }
                }

                if rle0_pos < self.num_runs() {
                    result.runs.push(Rle16::new(rle0_start, rle0_end - rle0_start - 1));
                    rle0_pos += 1;

                    if rle0_pos < self.num_runs() {
                        let len = self.num_runs() - rle0_pos;
                        result.copy_from_slice(&self.runs[rle0_pos..len]);
                    }
                }
            }

            return result.into_efficient_container();
        }

        if cardinality <= DEFAULT_MAX_SIZE {
            let mut array = ArrayContainer::with_capacity(cardinality);
            
            let mut index = 0;
            for run in self.runs.iter() {
                let start = run.value;
                let end = run.end() + 1;

                index = array_ops::advance_until(&other, index, start);

                if index >= other.cardinality() {
                    for i in start..end {
                        array.push(i as u16);
                    }
                }
                else {
                    let mut next = other[index];
                    if next >= end {
                        for i in start..end {
                            array.push(i as u16);
                        }

                        index -= 1;
                    }
                    else {
                        for i in start..end {
                            if i != next {
                                array.push(i as u16);
                            }
                            else {
                                next = {
                                    if index + 1 >= other.cardinality() {
                                        0
                                    }
                                    else {
                                        index += 1;
                                        
                                        other[index]
                                    }
                                };
                            }
                        }

                        index -= 1;
                    }
                }
            }
            
            return Container::Array(array);
        }

        let bitset: BitsetContainer = self.into();
        bitset.inplace_and_not(other)
    }

    fn inplace_and_not(self, other: &ArrayContainer) -> Container {
        SetAndNot::and_not(&self, other)
    }
}

impl SetAndNot<BitsetContainer> for RunContainer {
    fn and_not(&self, other: &BitsetContainer) -> Container {
        let cardinality = self.cardinality();
        
        // Result is an array
        if cardinality <= DEFAULT_MAX_SIZE {
            let mut array = ArrayContainer::with_capacity(cardinality);
            for run in self.runs.iter() {
                for value in run.value..(run.end() + 1) {
                    if !other.get(value) {
                        array.push(value);
                    }
                }
            }

            Container::Array(array)
        }
        // Result may be a bitset
        else {
            let mut bitset = other.clone();

            let mut last_pos = 0;
            for rle in self.runs.iter() {
                let start = u32::from(rle.value);
                let end = u32::from(rle.end()) + 1;

                bitset.unset_range(last_pos..start);
                bitset.flip_range(start..end);

                last_pos = end;
            }

            bitset.unset_range(last_pos..(1 << 16));

            // Result is not a bitset, convert to array
            if bitset.cardinality() <= DEFAULT_MAX_SIZE {
                Container::Array(bitset.into())
            }
            // Result is a bitset
            else {
                Container::Bitset(bitset)
            }
        }
    }

    fn inplace_and_not(self, other: &BitsetContainer) -> Container {
        SetAndNot::and_not(&self, other)
    }
}

impl SetXor<Self> for RunContainer {
    fn xor(&self, other: &Self) -> Container {
        if self.is_empty() {
            return Container::Run(other.clone());
        }

        if other.is_empty() {
            return Container::Run(self.clone());
        }

        let mut result = RunContainer::with_capacity(
            self.runs.len() + other.runs.len()
        );

        let mut i_a = 0;
        let mut i_b = 0;
        let mut v_a;
        let mut v_b;

        while i_a < self.runs.len() && i_b < other.runs.len() {
            v_a = self.runs[i_a];
            v_b = other.runs[i_b];

            if v_a.value <= v_b.value {
                append_exclusive(&mut result.runs, v_a.value, v_a.length);
                i_a += 1;
            }
            else {
                append_exclusive(&mut result.runs, v_b.value, v_b.length);
                i_b += 1;
            }
        }

        while i_a < self.runs.len() {
            v_a = self.runs[i_a];

            append_exclusive(&mut result.runs, v_a.value, v_a.length);
            i_a += 1;
        }

        while i_b < other.runs.len() {
            v_b = other.runs[i_b];

            append_exclusive(&mut result.runs, v_b.value, v_b.length);
            i_b += 1;
        }

        Container::Run(result)
    }

    fn inplace_xor(self, other: &Self) -> Container {
        SetXor::xor(&self, other)
    }
}

impl SetXor<ArrayContainer> for RunContainer {
    fn xor(&self, other: &ArrayContainer) -> Container {
        let req_cap = self.num_runs() + other.len();
        let mut result = RunContainer::with_capacity(req_cap);

        let mut pos_run = 0;
        let mut pos_arr = 0;

        while pos_run < self.num_runs() && pos_arr < other.len() {
            let run = self.runs[pos_run];
            let val = other[pos_arr];

            if run.value < val {
                append_exclusive(&mut result.runs, run.value, run.length);

                pos_run += 1;
            }
            else {
                append_exclusive(&mut result.runs, val, 0);

                pos_arr += 1;
            }
        }

        while pos_arr < other.len() {
            append_exclusive(&mut result.runs, other[pos_arr], 0);

            pos_arr += 1;
        }

        while pos_run < self.num_runs() {
            let run = self.runs[pos_run];
            append_exclusive(&mut result.runs, run.value, run.length);

            pos_run += 1;
        }

        result.into_efficient_container()
    }

    fn inplace_xor(self, other: &ArrayContainer) -> Container {
        SetXor::xor(&self, other)
    }
}

impl SetXor<BitsetContainer> for RunContainer {
    fn xor(&self, other: &BitsetContainer) -> Container {
        let mut result = other.clone();
        for rle in self.runs.iter() {
            result.flip_range(u32::from(rle.value)..(u32::from(rle.end()) + 1));
        }

        if result.cardinality() <= DEFAULT_MAX_SIZE {
            Container::Array(result.into())
        }
        else {
            Container::Bitset(result)
        }
    }

    fn inplace_xor(self, other: &BitsetContainer) -> Container {
        SetXor::xor(&self, other)
    }
}

impl Subset<Self> for RunContainer {
    fn subset_of(&self, other: &Self) -> bool {
        if self.cardinality() > other.cardinality() {
            return false;
        }

        let mut i_0 = 0;
        let mut i_1 = 0;

        while i_0 < self.runs.len() && i_1 < other.runs.len() {
            let start_0 = self.runs[i_0].value;
            let start_1 = other.runs[i_1].value;
            let stop_0 = start_0 + self.runs[i_0].length;
            let stop_1 = start_1 + other.runs[i_1].length;

            if start_0 < start_1 {
                return false;
            }
            else {
                if stop_0 < stop_1 {
                    i_0 += 1;
                }
                else if stop_0 == stop_1 {
                    i_0 += 1;
                    i_1 += 1;
                }
                else {
                    i_1 += 1;
                }
            }
        }

        i_0 == self.runs.len()
    }
}

impl Subset<ArrayContainer> for RunContainer {
    fn subset_of(&self, other: &ArrayContainer) -> bool {
        if self.cardinality() > other.cardinality() {
            return false;
        }

        let mut start_pos;
        let mut stop_pos = 0;
        for rle in self.iter_runs() {
            let start = rle.value;
            let stop = rle.end();

            start_pos = array_ops::advance_until(&other, stop_pos, start);
            stop_pos = array_ops::advance_until(&other, stop_pos, stop);

            if start_pos == other.cardinality() {
                return false;
            }
            else {
                let not_same = || stop_pos - start_pos != (stop - start) as usize;
                let not_start = || other[start_pos] != start;
                let not_stop = || other[stop_pos] != stop;
                
                if not_same() || not_start() || not_stop() {
                    return false;
                }
            }
        }

        true
    }
}

impl Subset<BitsetContainer> for RunContainer {
    fn subset_of(&self, other: &BitsetContainer) -> bool {
        if self.cardinality() > other.cardinality() {
            return false;
        }

        for rle in self.iter_runs() {
            if !other.contains_range(rle.into_range()) {
                return false;
            }
        }

        true
    }
}

impl SetNot for RunContainer {
    fn not(&self, range: Range<u32>) -> Container {
        if range.is_empty() {
            return Container::Run(self.clone());
        }

        let mut result = RunContainer::with_capacity(self.num_runs() + 1);
        let mut k = 0;
        while k < self.num_runs() && u32::from(self.runs[k].value) < range.start {
            result.runs[k] = self.runs[k];

            k += 1;
        }

        let min = range.start as u16;
        let max = (range.end - 1) as u16;

        append_exclusive(&mut result.runs, min, max - min);

        while k < self.num_runs() {
            let rle = self.runs[k];

            append_exclusive(&mut result.runs, rle.value, rle.length);

            k += 1;
        }

        result.into_efficient_container()
    }

    fn inplace_not(mut self, range: Range<u32>) -> Container {
        // Check to see if the result will fit in the currently allocated container
        // if not fallback to the allocating version
        if self.runs.capacity() == self.runs.len() {
            let last_before_range = {
                if range.start > 0 { 
                    self.contains((range.start - 1) as u16) 
                }
                else { 
                    false
                }
            };

            let first_in_range = self.contains(range.start as u16);

            if last_before_range == first_in_range {
                let last_in_range = self.contains((range.end - 1) as u16);
                let first_after_range = self.contains(range.end as u16);

                // Contents won't fit in the current allocation
                if last_in_range == first_after_range {
                    return SetNot::not(&self, range);
                }
            }
        }

        // Perform the not in place with the current allocation
        let mut k = 0;
        while k < self.num_runs() && u32::from(self.runs[k].value) < range.start {
            k += 1;
        }

        let mut buffered = Rle16::new(0, 0);
        let mut next = buffered;
        if k < self.num_runs() {
            buffered = self.runs[k];
        }

        append_exclusive(&mut self.runs, range.start as u16, (range.end - range.start - 1) as u16);

        while k < self.num_runs() {
            if k + 1 < self.num_runs() {
                next = self.runs[k + 1];
            }

            append_exclusive(&mut self.runs, buffered.value, buffered.length);

            buffered = next;
            k += 1;
        }

        self.runs.truncate(k);
        self.into_efficient_container()
    }
}

/// An iterator over the values of a run structure
pub struct Iter<'a> {
    /// The rle encoded words we're reading from
    runs: &'a [Rle16],

    /// The index of the rle word we're reading
    rle_index: usize,

    /// The index of the last read value in the rle
    value_index: u16
}

impl<'a> Iterator for Iter<'a> {
    type Item = u16;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.rle_index < self.runs.len() {
            let rle = self.runs[self.rle_index];
            
            if self.value_index <= rle.length {
                // Extract value
                let value = rle.value + self.value_index;

                // Bump index
                let next = self.value_index.checked_add(1);

                // Increment run if necessary
                if next.is_none() || next.unwrap() > rle.length {
                    self.rle_index += 1;
                    self.value_index = 0;
                }
                else {
                    self.value_index = next.unwrap();
                }

                Some(value)
            }
            else {
                None
            }
        }
        else {
            None
        }
    }
}

fn append_exclusive(runs: &mut Vec<Rle16>, start: u16, length: u16) {
    let is_empty = runs.is_empty();
    let old_end = runs.last_mut()
        .and_then(|x| Some(x.end().saturating_add(1)));

    let last_run = runs.last_mut();

    if is_empty || (old_end.is_some() && start > old_end.unwrap()) {
        runs.push(Rle16::new(start, length));
        return;
    }

    let last_run = last_run.unwrap();
    let old_end = old_end.unwrap();

    if old_end == start {
        last_run.length += length + 1;
        return;
    }

    let new_end = start + length + 1;
    if start == last_run.value {
        if new_end < old_end {
            *last_run = Rle16::new(new_end, old_end - new_end - 1);
            return;
        }
        else if new_end > old_end {
            *last_run = Rle16::new(old_end, new_end - old_end - 1);
            return;
        }
        else {
            runs.pop();
            return;
        }
    }

    // Checked version of `start - last_run.value - 1`
    last_run.length = start
        .saturating_sub(last_run.value)
        .saturating_sub(1);

    if new_end < old_end {
        runs.push(Rle16::new(new_end, old_end - new_end - 1));
    }
    else if new_end > old_end {
        runs.push(Rle16::new(old_end, new_end - old_end - 1));
    }
}

/// Appends a run to `runs` or merges it with `previous_run`
/// 
/// # Notes
/// Expects `runs` to have at least 1 element and `previous_run` to point to that last element. 
fn append(runs: &mut Vec<Rle16>, run: Rle16, previous_run: &mut Rle16) {
    let prev_end = previous_run.end();

    // Add a new run
    if run.value > prev_end + 1 {
        runs.push(run);

        *previous_run = run;
    }
    // Merge runs
    else {
        let new_end = run.value + run.length + 1;
        if new_end > prev_end {
            previous_run.length = new_end - 1 - previous_run.value;

            let len = runs.len();
            runs[len - 1] = *previous_run;
        }
    }
}

fn append_value(runs: &mut Vec<Rle16>, value: u16, prev_rle: &mut Rle16) {
    let prev_end = prev_rle.end();
    if value > prev_end + 1 {
        let rle = Rle16::new(value, 0);
        runs.push(rle);

        *prev_rle = rle;
    }
    else if value == prev_end + 1 {
        prev_rle.length += 1;

        let len = runs.len();
        runs[len - 1] = *prev_rle;
    }
}

#[cfg(test)]
mod test {
    use crate::container::*;
    use crate::test::*;

    impl TestShim<u16> for RunContainer {
        fn from_data(data: &[u16]) -> Self {
            let mut result = Self::new();

            for value in data.iter() {
                result.add(*value);
            }

            result
        }

        fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=u16> + 'a> {
            Box::new(self.iter())
        }

        fn card(&self) -> usize {
            self.cardinality()
        }
    }

    #[test]
    fn add() {
        let mut a = RunContainer::new();
        for value in 0..20 {
            a.add(value);
        }

        assert_eq!(a.cardinality(), 20);

        for (found, expected) in a.iter().zip(0..20) {
            assert_eq!(found, expected);
        }
    }

    #[test]
    fn add_range() {
        let range = 0..(1 << 16);
        let mut a = RunContainer::new();
        a.add_range(range.clone());

        assert_eq!(a.cardinality(), range.len());

        for (found, expected) in a.iter().zip(range) {
            assert_eq!(found, expected as u16);
        }
    }

    #[test]
    fn remove() {
        let mut a = RunContainer::new();
        a.add_range(0..20);

        a.remove(10);

        assert_eq!(a.cardinality(), 19);
        assert!(!a.contains(10));
    }

    #[test]
    fn remove_range() {
        let mut a = RunContainer::new();
        a.add_range(0..20);
        a.remove_range(0..10);

        assert_eq!(a.cardinality(), 10);

        for (found, expected) in a.iter().zip(10..20) {
            assert_eq!(found, expected);
        }
    }

    #[test]
    fn contains() {
        let mut a = RunContainer::new();
        a.add_range(0..50);

        assert!(a.contains(10));
        assert!(!a.contains(100));
    }

    #[test]
    fn contains_range() {
        let mut a = RunContainer::new();
        a.add_range(0..100);

        assert!(a.contains_range(25..75));
    }

    #[test]
    fn cardinality() {
        let data = generate_data(0..65535, 20_000);
        let a = RunContainer::from_data(&data);
        let card = a.cardinality();

        assert_eq!(card, data.len());
    }

    #[test]
    fn is_empty() {
        let a = RunContainer::new();

        assert!(a.is_empty());
        assert!(!a.is_full());
    }   

    #[test]
    fn is_full() {
        let a = RunContainer::new()
            .not(0..(1 << 16));

        assert!(!a.is_empty());
        assert!(a.is_full());
    } 

    #[test]
    fn min() {
        let mut a = RunContainer::new();
        a.add_range(0..100);

        let min = a.min();
        assert!(min.is_some());
        assert_eq!(min.unwrap(), 0);
    }

    #[test]
    fn max() {
        let mut a = RunContainer::new();
        a.add_range(0..100);

        let max = a.max();
        assert!(max.is_some());
        assert_eq!(max.unwrap(), 99);
    }

    #[test]
    fn rank() {
        let mut a = RunContainer::new();
        a.add_range(0..10);

        let rank = a.rank(5);
        assert_eq!(rank, 6);
    }

    #[test]
    fn select() {
        let range = 0..30;
        let mut a = RunContainer::new();
        a.add_range(range);

        let mut start_rank = 5;
        let selected = a.select(20, &mut start_rank);
        
        assert!(selected.is_some());
        assert_eq!(selected.unwrap(), 15);
    }

    #[test]
    fn from_array() {
        let data = generate_data(0..65535, 3_000);
        let a = ArrayContainer::from_data(&data);
        let b = RunContainer::from(a.clone());

        for (before, after) in a.iter().zip(b.iter()) {
            assert_eq!(*before, after);
        }
    }

    #[test]
    fn from_bitset() {
        let data = generate_data(0..65535, 7_000);
        let a = BitsetContainer::from_data(&data);
        let b = RunContainer::from(a.clone());

        for (before, after) in a.iter().zip(b.iter()) {
            assert_eq!(before, after);
        }
    }

    #[test]
    fn round_trip_serialize() {
        let data = generate_data(0..65535, 20_000);
        let a = RunContainer::from_data(&data);
        let num_bytes = RunContainer::serialized_size(a.num_runs());
        let mut buffer = Vec::<u8>::with_capacity(num_bytes);
        
        let num_written = a.serialize(&mut buffer);
        assert!(num_written.is_ok());
        assert_eq!(num_written.unwrap(), num_bytes);

        let mut cursor = std::io::Cursor::new(&buffer);
        let deserialized = RunContainer::deserialize(&mut cursor);
        assert!(deserialized.is_ok());

        let deserialized = deserialized.unwrap();
        let iter = deserialized.iter()
            .zip(a.iter());

        for (found, expected) in iter {
            assert_eq!(found, expected);
        }
    }

    #[test]
    fn run_run_or() {
        op_test::<RunContainer, RunContainer, u16, _, Container>(
            OpType::Or, |a, b| a.or(&b)
        );
    }

    #[test]
    fn run_run_and() {
        op_test::<RunContainer, RunContainer, u16, _, Container>(
            OpType::And, |a, b| a.and(&b)
        );
    }

    #[test]
    fn run_run_and_cardinality() {
        op_card_test::<RunContainer, RunContainer, u16, _>(
            OpType::And, |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn run_run_and_not() {
        op_test::<RunContainer, RunContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn run_run_xor() {
        op_test::<RunContainer, RunContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.xor(&b)
        );
    }

    #[test]
    fn run_not() {
        let data = generate_data(0..65535, 20_000);
        let a = RunContainer::from_data(&data);
        let not_a = a.not(0..(1 << 16));

        assert_eq!(
            not_a.cardinality(), 
            usize::from(std::u16::MAX) - a.cardinality()
        );

        // Ensure that `not_a` contains no elements of A
        for value in a.iter() {
            assert!(!not_a.contains(value), "Found {:?} in `not_a`", value);
        }
    }

    #[test]
    fn run_run_inplace_or() {
        op_test::<RunContainer, RunContainer, u16, _, Container>(
            OpType::Or, |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn run_run_inplace_and() {
        op_test::<RunContainer, RunContainer, u16, _, Container>(
            OpType::And, |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn run_run_inplace_and_not() {
        op_test::<RunContainer, RunContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn run_run_inplace_xor() {
        op_test::<RunContainer, RunContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.xor(&b)
        );
    }

    #[test]
    fn run_run_subset_of() {
        op_subset_test::<RunContainer, RunContainer, u16>();
    }

    #[test]
    fn run_array_or() {
        op_test::<RunContainer, ArrayContainer, u16, _, Container>(
            OpType::Or, |a, b| a.or(&b)
        );
    }

    #[test]
    fn run_array_and() {
        op_test::<RunContainer, ArrayContainer, u16, _, Container>(
            OpType::And, |a, b| a.and(&b)
        );
    }

    #[test]
    fn run_array_and_cardinality() {
        op_card_test::<RunContainer, ArrayContainer, u16, _>(
            OpType::And, |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn run_array_and_not() {
        op_test::<RunContainer, ArrayContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn run_array_xor() {
        op_test::<RunContainer, ArrayContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.xor(&b)
        );
    }

    #[test]
    fn run_array_inplace_or() {
        op_test::<RunContainer, ArrayContainer, u16, _, Container>(
            OpType::Or, |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn run_array_inplace_and() {
        op_test::<RunContainer, ArrayContainer, u16, _, Container>(
            OpType::And, |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn run_array_inplace_and_not() {
        op_test::<RunContainer, ArrayContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn run_array_inplace_xor() {
        op_test::<RunContainer, ArrayContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn run_array_subset_of() {
        let data_a = generate_seeded_data(0..65535, 4000, 0);

        let count = data_a.len() / 2;
        let mut data_b = Vec::with_capacity(count);
        data_b.extend_from_slice(&data_a[..count]);

        let a = ArrayContainer::from_data(&data_a);
        let b = RunContainer::from_data(&data_b);

        // Check that the cardinality matches the precomputed result
        assert!(b.subset_of(&a));
        assert!(!a.subset_of(&b));
    }

    #[test]
    fn run_bitset_or() {
        op_test::<RunContainer, BitsetContainer, u16, _, Container>(
            OpType::Or, |a, b| a.or(&b)
        );
    }

    #[test]
    fn run_bitset_and() {
        op_test::<RunContainer, BitsetContainer, u16, _, Container>(
            OpType::And, |a, b| a.and(&b)
        );
    }

    #[test]
    fn run_bitset_and_cardinality() {
        op_card_test::<RunContainer, BitsetContainer, u16, _>(
            OpType::And, |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn run_bitset_and_not() {
        op_test::<RunContainer, BitsetContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn run_bitset_xor() {
        op_test::<RunContainer, BitsetContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.xor(&b)
        );
    }

    #[test]
    fn run_bitset_inplace_or() {
        op_test::<RunContainer, BitsetContainer, u16, _, Container>(
            OpType::Or, |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn run_bitset_inplace_and() {
        op_test::<RunContainer, BitsetContainer, u16, _, Container>(
            OpType::And, |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn run_bitset_inplace_and_not() {
        op_test::<RunContainer, BitsetContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn run_bitset_inplace_xor() {
        op_test::<RunContainer, BitsetContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn run_bitset_subset_of() {
        op_subset_test::<RunContainer, BitsetContainer, u16>();
    }
}