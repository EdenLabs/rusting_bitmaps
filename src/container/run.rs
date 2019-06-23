use std::ops::{Deref, DerefMut};
use std::iter;
use std::mem;
use std::ptr;
use std::fmt;
use std::iter::Iterator;
use std::slice;

use crate::utils;
use crate::container::*;
use crate::container::array_ops;
use crate::container::run_ops;

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
    pub fn end(&self) -> u16 {
        self.value + self.length + 1
    }
    
    /// Get the start and end value of the run
    #[inline]
    pub fn range(&self) -> (u16, u16) {
        (self.value, self.value + self.length + 1)
    }
}

impl IntoRange<u16> for Rle16 {
    #[inline]
    fn into_range(self) -> Range<u16> {
        self.value..(self.value + self.length + 1)
    }
}

impl IntoRange<usize> for Rle16 {
    #[inline]
    fn into_range(self) -> Range<usize> {
        (self.value as usize)..((self.value + self.length) as usize)
    }
}

impl fmt::Debug for Rle16 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{:?}, {:?}]", self.value, self.length)
    }
}

/// The result for a binary search
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
            cardinality: LazyCardinality::with_value(0)
        }
    }
    
    /// Create a new run container with a specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            runs: Vec::with_capacity(capacity),
            cardinality: LazyCardinality::with_value(0)
        }
    }
    
    /// Shrink the run container's backing memory to fit it's contents
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.runs.shrink_to_fit()
    }
    
    /// Reserve space for `additional` runs in the run container
    pub fn reserve(&mut self, additional: usize) {
        self.runs.reserve(additional);
    }
    
    /// Add a value to the run container
    pub fn add(&mut self, value: u16) {
        match self.binary_search(value) {
            SearchResult::ExactMatch(_index) => {
                return;
            },
            SearchResult::PossibleMatch(index) => {
                let v = self.runs[index];
                let offset = value - v.value;
                
                if offset <= v.length {
                    return;
                }

                if offset + 1 == v.length {
                    if index + 1 < self.runs.len() {
                        // Check if necessary to fuse, if so fuse the runs
                        let v1 = self.runs[index + 1];
                        if v1.value == value + 1 {
                            self.runs[index].length = v1.value + v1.length - v.value;
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

                let new_run = Rle16::new(value, 0);
                self.runs.insert(index + 1, new_run);
            },
            SearchResult::NoMatch => {
                // Check if the run needs extended, if so extend it
                if self.runs.len() > 0 {
                    let v0 = &mut self.runs[0];
                    if v0.value == value + 1 {
                        v0.length += 1;
                        v0.value -= 1;
                        return;
                    }
                }

                let new_run = Rle16::new(value, 0);
                self.runs.insert(0, new_run);
            }
        }
    }
    
    /// Add all values in the range [min-max) to the run container
    pub fn add_range(&mut self, range: Range<u16>) {
        let max = range.start;
        let min = range.end - 1;

        let runs_min = self.rle_count_less(min);
        let runs_max = self.rle_count_greater(max);

        let common = self.runs.len() - runs_min - runs_max;
        if common == 0 {
            self.runs.insert(
                runs_min,
                Rle16::new(min, max - min)
            );
        }
        else {
            let common_min = self.runs[runs_min].value;
            let common_max = self.runs[runs_min + common - 1].end();
            let result_min = common_min.min(min);
            let result_max = common_max.max(max);

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

                return true;
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

                return false;
            },
            SearchResult::NoMatch => {
                return false;
            }
        }
    }
    
    /// Remove all values in the range [min-max) from the run container
    pub fn remove_range(&mut self, range: Range<u16>) {
        fn result_to_compressed_index(value: SearchResult) -> isize {
            match value {
                SearchResult::ExactMatch(index) => {
                    index as isize
                },
                SearchResult::PossibleMatch(index) => {
                    -(index as isize + 1)
                },
                SearchResult::NoMatch => {
                    -1
                }
            }
        }

        let min = range.start;
        let max = range.end - 1;

        let mut first = result_to_compressed_index(self.find_run(min));
        let mut last = result_to_compressed_index(self.find_run(max));

        if first >= 0 {
            let v_first = self.runs[first as usize];
            if min > v_first.value && max < v_first.end() {
                // Split into two runs

                // Right interval
                self.runs.insert(first as usize + 1, Rle16::new(max + 1, v_first.end() - (max + 1)));

                // Left interval
                self.runs[first as usize].length = (min - 1) - v_first.value;
                return;
            }

            if min > v_first.value {
                self.runs[first as usize].length = (min - 1) - v_first.value;
                first += 1;
            }
        }
        else {
            first = -first - 1;
        }

        if last >= 0 {
            let run_max = self.runs[last as usize].end();
            if run_max > max {
                self.runs[last as usize] = Rle16::new(max + 1, run_max - (max + 1));
                last -= 1;
            }
        }
        else {
            last = (-last - 1) - 1;
        }

        if first <= last {
            self.runs.splice((self.runs.len() - (last as usize + 1))..(-(last - first + 1) as usize), iter::empty());
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

                if value - v.value <= v.length {
                    true
                }
                else {
                    false
                }
            },
            SearchResult::NoMatch => {
                false
            }
        }
    }
    
    /// Check if the container contains all the values in [min-max)
    pub fn contains_range(&self, range: Range<u16>) -> bool {
        let mut count = 0;
        let index;

        let min = range.start;
        let max = range.end - 1;

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

            let min_length;
            if stop > min {
                min_length = stop - min;
            }
            else {
                min_length = 0;
            }

            if min_length < run.length {
                count += min_length;
            }
            else {
                count += run.length;
            }
        }

        return count >= max - min - 1;
    }
    
    /// The cardinality of the run container
    pub fn cardinality(&self) -> usize {
        self.cardinality.get(|| self.compute_cardinality())
    }

    /// Compute the cardinality of this run container
    fn compute_cardinality(&self) -> usize {
        let mut card = 0;
        for rle in self.iter_runs() {
            card += rle.length as usize;
        }

        card
    }

    /// The number of runs in the run container
    pub fn num_runs(&self) -> usize {
        self.runs.len()
    }

    /// Check whether the container is empty
    pub fn is_empty(&self) -> bool {
        self.runs.len() == 0
    }
    
    /// Check whether the container is full
    pub fn is_full(&self) -> bool {
        if self.runs.len() == 0 {
            return false;
        }

        unsafe {
            run_ops::is_full(&self.runs)
        }
    }

    /// Get the minimum value of this container
    pub fn min(&self) -> Option<u16> {
        if self.runs.len() == 0 {
            return None;
        }

        Some(self.runs[0].value)
    }
    
    /// Get the maximum value of this container
    pub fn max(&self) -> Option<u16> {
        if self.runs.len() == 0 {
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

        return sum as usize;
    }

    /// Select the element with `rank` starting the search from `start_rank`
    pub fn select(&self, rank: u32, start_rank: &mut u32) -> Option<u16> {
        for run in self.runs.iter() {
            let length = run.length as u32;
            let value = run.value as u32;

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
        return Container::Bitset(self.into());
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
        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low < high {
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
        while low < high {
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
    fn rle_count_less(&self, value: u16) -> usize {
        if self.runs.len() == 0 {
            return 0;
        }

        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low <= high {
            let middle = (low + high) >> 1;
            let min_value = self.runs[middle].value;
            let max_value = min_value + self.runs[middle].length;

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

        return low;
    }

    /// Get the number of runs after `value`
    fn rle_count_greater(&self, value: u16) -> usize {
        if self.runs.len() == 0 {
            return 0;
        }

        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low <= high {
            let middle = (low + high) >> 1;
            let min_value = self.runs[middle].value;
            let max_value = min_value + self.runs[middle].length;

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

        return self.runs.len() - low;
    }

    /// Ensure that there is enough room for at least `capacity` elements
    #[inline]
    fn ensure_fit(&mut self, capacity: usize) {
        if self.runs.capacity() > capacity {
            self.runs.reserve(capacity - self.runs.capacity());
        }
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

        let num_runs = self.num_runs();
        let runs = (num_runs as u16).to_le_bytes();
        num_written += buf.write(&runs)?;

        unsafe {
            let num_bytes = num_runs * mem::size_of::<Rle16>();
            let ptr = self.as_ptr() as *const u8;
            let slice = slice::from_raw_parts(ptr, num_bytes);
            num_written += buf.write(slice)?;
        }

        Ok(num_written)
    }

    /// Deserialize a run container from the provided buffer
    #[cfg(target_endian = "little")]
    pub fn deserialize<R: Read>(buf: &mut R) -> io::Result<Self> {
        let num_runs = unsafe {
            let mut read_buf: [u8; 2] = [0; 2];
            buf.read(&mut read_buf)?;

            ptr::read(read_buf.as_ptr() as *const u16) as usize
        };

        let mut result = Self::with_capacity(num_runs);

        unsafe {
            let num_bytes = num_runs * mem::size_of::<Rle16>();
            let ptr = result.as_mut_ptr() as *mut u8;
            let slice = slice::from_raw_parts_mut(ptr, num_bytes);

            buf.read(slice)?;
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
        let num_runs = container.num_runs();
        let mut run_container = RunContainer::with_capacity(num_runs);

        let mut prev: isize = -2;
        let mut run_start: isize = -1;
        let cardinality = container.cardinality();

        if cardinality == 0 {
            return run_container;
        }
        
        for value in container.iter() {
            if *value != (prev + 1) as u16 {
                if run_start != -1 {
                    run_container.runs.push(
                        Rle16::new(run_start as u16, prev as u16)
                    );
                }

                run_start = *value as isize;
            }

            prev = *value as isize;
        }

        run_container.runs.push(
            Rle16::new(run_start as u16, prev as u16)
        );

        run_container
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
        let num_runs = container.num_runs();
        let mut run_container = RunContainer::with_capacity(num_runs);

        let mut prev: isize = -2;
        let mut run_start: isize = -1;
        let cardinality = container.cardinality();

        if cardinality == 0 {
            return run_container;
        }
        
        for value in container.iter() {
            if value != (prev + 1) as u16 {
                if run_start != -1 {
                    run_container.runs.push(
                        Rle16::new(run_start as u16, prev as u16)
                    );
                }

                run_start = value as isize;
            }

            prev = value as isize;
        }

        run_container.runs.push(
            Rle16::new(run_start as u16, prev as u16)
        );

        run_container
    }
}

impl PartialEq for RunContainer {
    fn eq(&self, other: &RunContainer) -> bool {
        utils::mem_equals(&self.runs, &other.runs)
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
        let mut result = RunContainer::new();
        run_ops::or(&self.runs, &other.runs, &mut result.runs);

        Container::Run(result)
    }
    
    fn inplace_or(mut self, other: &Self) -> Container {
        let is_full_self = self.is_full();
        let is_full_other = other.is_full();

        if is_full_self || is_full_other {
            if is_full_other {
                return Container::Run(self);
            }
            else {
                self.ensure_fit(other.runs.len());

                self.runs.clear();
                self.runs.extend_from_slice(&other.runs);

                return Container::Run(self);
            }
        }

        // Check for and reserve enough space to hold the contents
        let max_runs = self.num_runs() + other.num_runs();
        let req_cap = max_runs + self.num_runs();
        if self.runs.capacity() < req_cap {
            self.runs.reserve(req_cap - self.runs.capacity());
        }


        unsafe {
            let len = self.len();
    
            // Move the current contents to the end of the array
            self.runs.set_len(0);
            
            let src = self.runs.as_ptr();
            let dst = src.add(max_runs) as *mut _;

            ptr::copy_nonoverlapping(src, dst, len);

            // At this point all the contents from 0..max_runs is random memory
            // and the current values are at max_runs..len
            // We set the len to 0 to make sure the vector treats it's contents like normal
            // and to ensure than the contents at the later buffer is left alone
            
            let ptr_old = src.add(max_runs);
            let mut pos_0 = 0;
            let mut pos_1 = 0;

            let mut prev_rle;
            let rle = *(ptr_old.add(pos_0));
            if rle.value <= other.runs[pos_1].value { 
                self.runs[pos_0] = rle;
                prev_rle = rle;

                pos_0 += 1;
            }
            else {
                self.runs[pos_0] = other.runs[pos_1];
                prev_rle = other.runs[pos_1];

                pos_1 += 1;
            }

            while pos_1 < other.num_runs() && pos_0 < len {
                let rle = *(ptr_old.add(pos_0));
                let new_rle;
                if rle.value <= other.runs[pos_1].value {
                    new_rle = rle;
                    pos_0 += 1;
                }
                else {
                    new_rle = other.runs[pos_1];
                    pos_1 += 1;
                }

                run_ops::append(&mut self.runs, new_rle, &mut prev_rle);
            }

            while pos_1 < other.num_runs() {
                run_ops::append(&mut self.runs, other.runs[pos_1], &mut prev_rle);
                pos_1 += 1;
            }

            while pos_0 < len {
                run_ops::append(&mut self.runs, *(ptr_old.add(pos_0)), &mut prev_rle);
                pos_0 += 1;
            }

            // After this point we don't care what happens with the values.
            // There's also no need to drop them since they're `Copy` and don't implement `Drop`
        }

        Container::Run(self)
    }
}

impl SetOr<ArrayContainer> for RunContainer {
    fn or(&self, other: &ArrayContainer) -> Container {
        if self.is_full() {
            return Container::Run(self.clone());
        }

        let mut result = RunContainer::new();
        result.reserve(2 * other.cardinality() + self.runs.len());

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
            if self.runs[rle_index].value < other[array_index] {
                run_ops::append(&mut result.runs, self.runs[rle_index], &mut prev_rle);
                rle_index += 1;
            }
            else {
                run_ops::append_value(&mut result.runs, other[array_index], &mut prev_rle);
                array_index += 1;
            }
        }

        if array_index < other.cardinality() {
            while array_index < other.cardinality() {
                run_ops::append_value(&mut result.runs, other[array_index], &mut prev_rle);
                array_index += 1;
            }
        }
        else {
            while rle_index < self.runs.len() {
                run_ops::append(&mut result.runs, self.runs[rle_index], &mut prev_rle);
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
        let req_cap = max_runs + self.num_runs();
        self.ensure_fit(req_cap);

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
                    run_ops::append(&mut self.runs, rle, &mut prev_rle);
                    pos_run += 1;
                }
                else {
                    run_ops::append_value(&mut self.runs, val, &mut prev_rle);
                    pos_arr += 1;
                }
            }

            if pos_arr < other.len() {
                while pos_arr < other.len() {
                    run_ops::append_value(&mut self.runs, other[pos_arr], &mut prev_rle);
                    pos_arr += 1;
                }
            }
            else {
                while pos_run < len {
                    run_ops::append(&mut self.runs, *(ptr_old.add(pos_run)), &mut prev_rle);
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
            let min = rle.value as usize;
            let max = rle.end() as usize;

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
        let if0 = self.is_full();
        let if1 = other.is_full();
        if if0 || if1 {
            if if0 {
                return Container::Run(other.clone());
            }
            
            if if1 {
                return Container::Run(self.clone());
            }
        }

        let req_cap = self.num_runs() + other.num_runs();
        let mut result = RunContainer::with_capacity(req_cap);
        
        let mut i0 = 0;
        let mut i1 = 0;

        let mut start0 = self.runs[i0].value;
        let mut start1 = other.runs[i1].value;
        let mut end0 = self.runs[i0].end();
        let mut end1 = other.runs[i1].end();

        while i0 < self.num_runs() && i1 < other.num_runs() {
            // Runs don't overlap, advance either or
            if end0 <= start1 {
                i0 += 1;

                if i0 < self.num_runs() {
                    start0 = self.runs[i0].value;
                    end0 = self.runs[i0].end();
                }
            }
            else if end1 <= start0 {
                i1 += 1;

                if i1 < other.num_runs() {
                    start1 = other.runs[i1].value;
                    end1 = other.runs[i1].end();
                }
            }
            // Runs overlap, try to merge if possible
            else {
                let last_start = start0.max(start1);
                let first_end;

                if unlikely!(end0 == end1) {
                    first_end = end0;
                    i0 += 1;
                    i1 += 1;

                    if i0 < self.num_runs() {
                        start0 = self.runs[i0].value;
                        end0 = self.runs[i0].end();
                    }

                    if i1 < other.num_runs() {
                        start1 = other.runs[i1].value;
                        end1 = other.runs[i1].end();
                    }
                }
                else if end0 < end1 {
                    first_end = end0;

                    i0 += 1;
                    if i0 < self.num_runs() {
                        start0 = self.runs[i0].value;
                        end0 = self.runs[i0].end();
                    }
                }
                else {
                    first_end = end1;

                    i1 += 1;
                    if i1 < other.num_runs() {
                        start1 = other.runs[i1].value;
                        end1 = other.runs[i1].end();
                    }
                }

                let run = Rle16::new(last_start, first_end - last_start - 1);
                result.runs.push(run);
            }
        }

        result.into_efficient_container()
    }

    fn and_cardinality(&self, other: &Self) -> usize {
        let if0 = self.is_full();
        let if1 = other.is_full();
        if if0 || if1 {
            if if0 {
                return other.cardinality();
            }
            
            if if1 {
                return self.cardinality();
            }
        }

        let mut card = 0;
        
        let mut i0 = 0;
        let mut i1 = 0;

        let mut start0 = self.runs[i0].value;
        let mut start1 = other.runs[i1].value;
        let mut end0 = self.runs[i0].end();
        let mut end1 = other.runs[i1].end();

        while i0 < self.num_runs() && i1 < other.num_runs() {
            // Runs don't overlap, advance either or
            if end0 <= start1 {
                i0 += 1;

                if i0 < self.num_runs() {
                    start0 = self.runs[i0].value;
                    end0 = self.runs[i0].end();
                }
            }
            else if end1 <= start0 {
                i1 += 1;

                if i1 < other.num_runs() {
                    start1 = other.runs[i1].value;
                    end1 = other.runs[i1].end();
                }
            }
            // Runs overlap, try to merge if possible
            else {
                let last_start = start0.max(start1);
                let first_end;

                if unlikely!(end0 == end1) {
                    first_end = end0;
                    i0 += 1;
                    i1 += 1;

                    if i0 < self.num_runs() {
                        start0 = self.runs[i0].value;
                        end0 = self.runs[i0].end();
                    }

                    if i1 < other.num_runs() {
                        start1 = other.runs[i1].value;
                        end1 = other.runs[i1].end();
                    }
                }
                else if end0 < end1 {
                    first_end = end0;

                    i0 += 1;
                    if i0 < self.num_runs() {
                        start0 = self.runs[i0].value;
                        end0 = self.runs[i0].end();
                    }
                }
                else {
                    first_end = end1;

                    i1 += 1;
                    if i1 < other.num_runs() {
                        start1 = other.runs[i1].value;
                        end1 = other.runs[i1].end();
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

        if self.runs.len() == 0 {
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

        if self.runs.len() == 0 {
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

                array.add_range(min..max);
            }

            return Container::Array(array);
        }
        else {
            let mut bitset = other.clone();

            // Unset all bits in between the runs
            let mut start = 0;
            for run in self.runs.iter() {
                let end = run.value as usize;
                bitset.unset_range(start..end);

                start = end + run.length as usize + 1;
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
        let mut r = RunContainer::new();
        run_ops::and_not(&self.runs, &other.runs, &mut r.runs);

        r.into_efficient_container()
    }

    fn inplace_and_not(self, other: &Self) -> Container {
        SetAndNot::and_not(&self, other)
    }
}

impl SetAndNot<ArrayContainer> for RunContainer {
    fn and_not(&self, other: &ArrayContainer) -> Container {
        const ARBITRARY_THRESHOLD: usize = 32;

        let cardinality = self.cardinality();

        if cardinality < ARBITRARY_THRESHOLD {
            if other.cardinality() == 0 {
                return Container::Run(self.clone());
            }

            let mut result = RunContainer::with_capacity(cardinality + other.cardinality());

            unsafe {
                let mut rle0_pos = 0;
                let mut rle1_pos = 0;

                let rle = *self.runs.get_unchecked(rle0_pos);
                let mut rle0_start = rle.value;
                let mut rle0_end = rle.end();
                let mut rle1_start = *other.get_unchecked(rle1_pos);

                while rle0_pos < self.num_runs() && rle1_pos < other.cardinality() {
                    if rle0_end == rle1_start {
                        result.runs.push(Rle16::new(rle0_start, rle0_end - rle0_start - 1));
                        rle0_pos += 1;

                        if rle0_pos < self.num_runs() {
                            let r = self.runs.get_unchecked(rle0_pos);

                            rle0_start = r.value;
                            rle0_end = r.end();
                        }
                    }
                    else if rle1_start + 1 <= rle0_start {
                        rle1_pos += 1;

                        if rle1_pos < other.cardinality() {
                            rle1_start = *other.get_unchecked(rle1_pos);
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
                                let r = self.runs.get_unchecked(rle0_pos);

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
        }

        if cardinality <= DEFAULT_MAX_SIZE {
            let mut array = ArrayContainer::with_capacity(cardinality);
            
            unsafe {
                let mut index = 0;
                for run in self.runs.iter() {
                    let start = run.value;
                    let end = run.end();

                    index = array_ops::advance_until(&other, index, start);

                    if index >= other.cardinality() {
                        for i in start..end {
                            array.push(i as u16);
                        }
                    }
                    else {
                        let mut next = *other.get_unchecked(index);
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
                                            *other.get_unchecked(index)
                                        }
                                    };
                                }
                            }

                            index -= 1;
                        }
                    }
                }
            }
            
            return Container::Array(array);
        }

        let bitset: BitsetContainer = self.into();
        bitset.and_not(other)// TODO: In place variants
    }

    fn inplace_and_not(self, other: &ArrayContainer) -> Container {
        SetAndNot::and_not(&self, other)
    }
}

impl SetAndNot<BitsetContainer> for RunContainer {
    fn and_not(&self, other: &BitsetContainer) -> Container {
        let cardinality = self.cardinality();
        
        // Result is an array
        if cardinality < DEFAULT_MAX_SIZE {
            let mut array = ArrayContainer::with_capacity(cardinality);
            for rle in self.runs.iter() {
                for value in rle.value..rle.end() {
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
                let start = rle.value;
                let end = rle.end();

                bitset.unset_range(last_pos..(start as usize));
                bitset.flip_range(start..end);

                last_pos = end as usize;
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
        let mut result = RunContainer::new();
        run_ops::xor(&self, &other, &mut result.runs);

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
                run_ops::append_exclusive(&mut result.runs, run.value, run.length);

                pos_run += 1;
            }
            else {
                run_ops::append_exclusive(&mut result.runs, val, 0);

                pos_arr += 1;
            }
        }

        while pos_arr < other.len() {
            run_ops::append_exclusive(&mut result.runs, other[pos_arr], 0);

            pos_arr += 1;
        }

        while pos_run < self.num_runs() {
            let run = self.runs[pos_run];
            run_ops::append_exclusive(&mut result.runs, run.value, run.length);

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
            result.flip_range(rle.value..(rle.end()));
        }

        if result.cardinality() < DEFAULT_MAX_SIZE {
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

        unsafe {
            let mut i_0 = 0;
            let mut i_1 = 0;

            while i_0 < self.runs.len() && i_1 < other.runs.len() {
                let start_0 = self.runs.get_unchecked(i_0).value;
                let start_1 = other.runs.get_unchecked(i_1).value;
                let stop_0 = start_0 + self.runs.get_unchecked(i_0).length;
                let stop_1 = start_1 + other.runs.get_unchecked(i_1).length;

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

            if i_0 == self.runs.len() {
                return true;
            }
            else {
                return false;
            }
        }
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
    fn not(&self, range: Range<u16>) -> Container {
        let mut result = self.clone();

        let mut k = 0;
        while k < self.num_runs() && self.runs[k].value < range.start {
            result.runs[k] = self.runs[k];

            k += 1;
        }

        run_ops::append_exclusive(&mut result.runs, range.start, range.end - range.start - 1);

        while k < self.num_runs() {
            let rle = self.runs[k];

            run_ops::append_exclusive(&mut result.runs, rle.value, rle.length);

            k += 1;
        }

        result.into_efficient_container()
    }

    fn inplace_not(mut self, range: Range<u16>) -> Container {
        // Check to see if the result will fit in the currently allocated container
        // if not fallback to the allocating version
        if self.runs.capacity() == self.runs.len() {
            let last_before_range = {
                if range.start > 0 { 
                    self.contains(range.start - 1) 
                }
                else { 
                    false
                }
            };

            let first_in_range = self.contains(range.start);

            if last_before_range == first_in_range {
                let last_in_range = self.contains(range.end - 1);
                let first_after_range = self.contains(range.end);

                // Contents won't fit in the current allocation
                if last_in_range == first_after_range {
                    return SetNot::not(&self, range);
                }
            }
        }

        // Perform the not in place with the current allocation
        let mut k = 0;
        while k < self.num_runs() && self.runs[k].value < range.start {
            k += 1;
        }

        let mut buffered = Rle16::new(0, 0);
        let mut next = buffered;
        if k < self.num_runs() {
            buffered = self.runs[k];
        }

        run_ops::append_exclusive(&mut self.runs, range.start, range.end - range.start - 1);

        while k < self.num_runs() {
            if k + 1 < self.num_runs() {
                next = self.runs[k + 1];
            }

            run_ops::append_exclusive(&mut self.runs, buffered.value, buffered.length);

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
        unsafe {
            if self.rle_index < self.runs.len() {
                let rle = self.runs.get_unchecked(self.rle_index);
                
                if self.value_index < rle.length {
                    // Extract value
                    let value = rle.value + self.value_index; // TODO: Double check that this isn't an off by one error

                    // Bump index
                    self.value_index += 1;

                    // Increment run if necessary
                    if self.value_index >= rle.length {
                        self.rle_index += 1;
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
}