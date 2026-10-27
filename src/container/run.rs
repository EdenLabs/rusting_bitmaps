use std::ops::{Deref, DerefMut};
use std::iter;

use crate::container::*;
use crate::write_counted;

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

    /// The values of the run as a half-open range. The end is `u32` so that a run ending at
    /// 65 535 can be represented
    #[inline]
    pub fn into_range(self) -> Range<u32> {
        u32::from(self.value)..(u32::from(self.value) + u32::from(self.length) + 1)
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
}

impl RunContainer {
    /// Create a new run container
    pub fn new() -> Self {
        Self {
            runs: Vec::new(),
        }
    }
    
    /// Create a new run container with a specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            runs: Vec::with_capacity(capacity),
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

                self.runs.insert(0, Rle16::new(value, 0));
            }
        }
    }
    
    /// Add all values in the range [min-max) to the run container
    pub fn add_range(&mut self, range: Range<u32>) {
        let min = range.start;
        let last = range.end - 1;

        let runs_min = self.rle_count_less(min);
        let runs_max = self.rle_count_greater(last);

        let common = self.runs.len() - runs_min - runs_max;
        if common == 0 {
            self.runs.insert(
                runs_min,
                Rle16::new(min as u16, (last - min) as u16)
            );
        }
        else {
            let common_min = u32::from(self.runs[runs_min].value);
            let common_max = u32::from(self.runs[runs_min + common - 1].end());
            let result_min = common_min.min(min);
            let result_max = common_max.max(last);

            self.runs[runs_min] = Rle16::new(result_min as u16, (result_max - result_min) as u16);
            self.runs.splice((runs_min + 1)..(runs_min + common), iter::empty());
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

        // Trim the run holding `min`, then drop the runs from `first` up to `end`
        let first = match self.find_run(min) {
            SearchResult::ExactMatch(index) => {
                let run = self.runs[index];
                if min > run.value && max < run.end() {
                    // Split into two runs

                    // Right interval
                    self.runs.insert(index + 1, Rle16::new(max + 1, run.end() - (max + 1)));

                    // Left interval
                    self.runs[index].length = (min - 1) - run.value;
                    return;
                }

                if min > run.value {
                    self.runs[index].length = (min - 1) - run.value;
                    index + 1
                }
                else {
                    index
                }
            },
            SearchResult::PossibleMatch(index) => index + 1,
            SearchResult::NoMatch => 0
        };

        // Trim the run holding `max`
        let end = match self.find_run(max) {
            SearchResult::ExactMatch(index) => {
                let run_end = self.runs[index].end();
                if run_end > max {
                    self.runs[index] = Rle16::new(max + 1, run_end - (max + 1));
                    index
                }
                else {
                    index + 1
                }
            },
            SearchResult::PossibleMatch(index) => index + 1,
            SearchResult::NoMatch => 0
        };

        if first < end {
            self.runs.drain(first..end);
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
        let min = range.start as u16;
        let last = (range.end - 1) as u16;

        let mut index = match self.find_run(min) {
            SearchResult::ExactMatch(i) => i,
            _ => return false
        };

        // Stored runs may touch, a range can span several of them
        let mut end = self.runs[index].end();
        while end < last {
            index += 1;
            match self.runs.get(index) {
                Some(run) if run.value == end + 1 => end = run.end(),
                _ => return false
            }
        }

        true
    }
    
    /// The cardinality of the run container
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.compute_cardinality()
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
        // A full container ranks 65 536 values, more than a `u16` holds
        let mut sum = 0_usize;
        for run in self.runs.iter() {
            let start = run.value;
            let length = run.length;
            let end = start + length;

            if value < end {
                if value < start {
                    break;
                }

                return sum + usize::from(value - start) + 1;
            }
            else {
                sum += usize::from(length) + 1;
            }
        }

        sum
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

    /// The runs as ascending half-open ranges
    #[inline]
    pub(crate) fn intervals(&self) -> impl Iterator<Item = Range<u32>> + '_ {
        self.runs.iter().map(|run| run.into_range())
    }

    /// Perform a binary search for a given key in the set
    fn binary_search(&self, key: u16) -> SearchResult {
        if self.runs.is_empty() {
            return SearchResult::NoMatch;
        }

        let mut low = 0;
        let mut high = self.runs.len();
        while low < high {
            let middle = (low + high) >> 1;
            let value = self.runs[middle].value;

            if value < key {
                low = middle + 1;
            }
            else if value > key {
                high = middle;
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
        let mut high = self.runs.len();
        while low < high {
            let middle = (low + high) >> 1;
            let min = self.runs[middle].value;
            let max = self.runs[middle].end();
            if key > max {
                low = middle + 1;
            }
            else if key < min {
                high = middle;
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
        let mut low = 0;
        let mut high = self.runs.len();
        while low < high {
            let middle = (low + high) >> 1;
            let min_value = u32::from(self.runs[middle].value);
            let max_value = min_value + u32::from(self.runs[middle].length);

            if max_value + 1 < value {
                low = middle + 1;
            }
            else if value < min_value {
                high = middle;
            }
            else {
                return middle;
            }
        }

        low
    }

    /// Get the number of runs after `value`
    fn rle_count_greater(&self, value: u32) -> usize {
        let mut low = 0;
        let mut high = self.runs.len();
        while low < high {
            let middle = (low + high) >> 1;
            let min_value = u32::from(self.runs[middle].value);
            let max_value = min_value + u32::from(self.runs[middle].length);

            if max_value < value {
                low = middle + 1;
            }
            else if value + 1 < min_value {
                high = middle;
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

        num_written += write_counted(buf, &(self.num_runs() as u16).to_le_bytes())?;

        unsafe {
            let ptr = self.as_ptr() as *const u8;
            let num_bytes = self.num_runs() * mem::size_of::<Rle16>();
            let slice = slice::from_raw_parts(ptr, num_bytes);
            num_written += write_counted(buf, slice)?;
        }

        Ok(num_written)
    }

    /// Deserialize a run container from the provided buffer
    #[cfg(target_endian = "little")]
    pub fn deserialize<R: Read>(buf: &mut R) -> io::Result<Self> {
        let mut bytes = [0; 2];
        buf.read_exact(&mut bytes)?;

        let num_runs    = u16::from_le_bytes(bytes) as usize;
        let mut result  = Self::with_capacity(num_runs);
        result.runs.resize(num_runs, Rle16::new(0, 0));

        // `Rle16` is two `u16` with no padding, any bytes are a valid run
        unsafe {
            let num_bytes = num_runs * mem::size_of::<Rle16>();
            let ptr = result.runs.as_mut_ptr() as *mut u8;
            let slice = slice::from_raw_parts_mut(ptr, num_bytes);

            buf.read_exact(slice)?;
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
            if let Some(r) = run.as_mut() {
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
            if let Some(r) = run.as_mut() {
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
        combine(self.intervals(), other.intervals(), |a, b| a || b).into_efficient_container()
    }

    fn inplace_or(self, other: &Self) -> Container {
        SetOr::or(&self, other)
    }
}

impl SetOr<ArrayContainer> for RunContainer {
    fn or(&self, other: &ArrayContainer) -> Container {
        combine(self.intervals(), value_intervals(other), |a, b| a || b).into_efficient_container()
    }

    fn inplace_or(self, other: &ArrayContainer) -> Container {
        SetOr::or(&self, other)
    }
}

impl SetOr<BitsetContainer> for RunContainer {
    fn or(&self, other: &BitsetContainer) -> Container {
        let mut result = other.clone();

        for rle in self.iter_runs() {
            result.set_range(rle.into_range());
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
        combine(self.intervals(), other.intervals(), |a, b| a && b).into_efficient_container()
    }

    fn and_cardinality(&self, other: &Self) -> usize {
        combined_cardinality(self.intervals(), other.intervals(), |a, b| a && b)
    }

    fn inplace_and(self, other: &Self) -> Container {
        SetAnd::and(&self, other)
    }
}

impl SetAnd<ArrayContainer> for RunContainer {
    fn and(&self, other: &ArrayContainer) -> Container {
        combine(self.intervals(), value_intervals(other), |a, b| a && b).into_efficient_container()
    }

    fn and_cardinality(&self, other: &ArrayContainer) -> usize {
        combined_cardinality(self.intervals(), value_intervals(other), |a, b| a && b)
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
        combine(self.intervals(), other.intervals(), |a, b| a && !b).into_efficient_container()
    }

    fn inplace_and_not(self, other: &Self) -> Container {
        SetAndNot::and_not(&self, other)
    }
}

impl SetAndNot<ArrayContainer> for RunContainer {
    fn and_not(&self, other: &ArrayContainer) -> Container {
        combine(self.intervals(), value_intervals(other), |a, b| a && !b).into_efficient_container()
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
                for value in run.value..=run.end() {
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
        combine(self.intervals(), other.intervals(), |a, b| a != b).into_efficient_container()
    }

    fn inplace_xor(self, other: &Self) -> Container {
        SetXor::xor(&self, other)
    }
}

impl SetXor<ArrayContainer> for RunContainer {
    fn xor(&self, other: &ArrayContainer) -> Container {
        combine(self.intervals(), value_intervals(other), |a, b| a != b).into_efficient_container()
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
        combined_cardinality(self.intervals(), other.intervals(), |a, b| a && !b) == 0
    }
}

impl Subset<ArrayContainer> for RunContainer {
    fn subset_of(&self, other: &ArrayContainer) -> bool {
        if self.cardinality() > other.cardinality() {
            return false;
        }

        combined_cardinality(self.intervals(), value_intervals(other), |a, b| a && !b) == 0
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
        combine(self.intervals(), iter::once(range), |a, b| a != b).into_efficient_container()
    }

    fn inplace_not(self, range: Range<u32>) -> Container {
        SetNot::not(&self, range)
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

/// The values of a sorted array as half-open intervals of one value each
pub(crate) fn value_intervals(values: &[u16]) -> impl Iterator<Item = Range<u32>> + '_ {
    values.iter().map(|&value| u32::from(value)..(u32::from(value) + 1))
}

/// Walk two ascending lists of half-open intervals together and call `emit` with each maximal
/// interval whose values satisfy `keep(in_a, in_b)`. Intervals within a list are disjoint but may
/// touch. Ends are `u32` so that 65 536 can close an interval
fn sweep<A, B, K, E>(a: A, b: B, keep: K, mut emit: E)
    where A: Iterator<Item = Range<u32>>,
          B: Iterator<Item = Range<u32>>,
          K: Fn(bool, bool) -> bool,
          E: FnMut(Range<u32>)
{
    // Values in neither list are never kept, so the sweep can stop when both lists are done
    debug_assert!(!keep(false, false));

    let mut a = a.peekable();
    let mut b = b.peekable();
    let mut position = 0;
    let mut pending: Option<Range<u32>> = None;

    loop {
        while a.peek().map_or(false, |r| r.end <= position) {
            a.next();
        }
        while b.peek().map_or(false, |r| r.end <= position) {
            b.next();
        }

        let (next_a, next_b) = (a.peek().cloned(), b.peek().cloned());
        if next_a.is_none() && next_b.is_none() {
            break;
        }

        let in_a = next_a.as_ref().map_or(false, |r| r.start <= position);
        let in_b = next_b.as_ref().map_or(false, |r| r.start <= position);

        // The next position where membership in either list can change
        let boundary = |r: &Option<Range<u32>>, inside: bool| {
            r.as_ref().map_or(1 << 16, |r| if inside { r.end } else { r.start })
        };
        let next = boundary(&next_a, in_a).min(boundary(&next_b, in_b));

        if keep(in_a, in_b) {
            pending = match pending {
                Some(kept) if kept.end == position => Some(kept.start..next),
                Some(kept) => {
                    emit(kept);
                    Some(position..next)
                },
                None => Some(position..next)
            };
        }

        position = next;
    }

    if let Some(kept) = pending {
        emit(kept);
    }
}

/// The run container holding the values of two interval lists that satisfy `keep(in_a, in_b)`
pub(crate) fn combine<A, B, K>(a: A, b: B, keep: K) -> RunContainer
    where A: Iterator<Item = Range<u32>>,
          B: Iterator<Item = Range<u32>>,
          K: Fn(bool, bool) -> bool
{
    let mut result = RunContainer::new();
    sweep(a, b, keep, |range| {
        result.runs.push(Rle16::new(range.start as u16, (range.end - range.start - 1) as u16));
    });

    result
}

/// The number of values of two interval lists that satisfy `keep(in_a, in_b)`
pub(crate) fn combined_cardinality<A, B, K>(a: A, b: B, keep: K) -> usize
    where A: Iterator<Item = Range<u32>>,
          B: Iterator<Item = Range<u32>>,
          K: Fn(bool, bool) -> bool
{
    let mut cardinality = 0;
    sweep(a, b, keep, |range| cardinality += (range.end - range.start) as usize);

    cardinality
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
            (1 << 16) - a.cardinality()
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

    #[test]
    fn add_below_the_first_run() {
        let mut a = RunContainer::new();
        a.add_range(10..21);
        a.add(5);
        a.add(9);

        assert!(a.iter().eq([5, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]));
        assert!(a.contains(5) && a.contains(9) && !a.contains(4) && !a.contains(6));
        assert!(!a.remove(0));
        assert_eq!(a.cardinality(), 13);
    }

    #[test]
    fn remove_range_below_the_first_run() {
        let mut a = RunContainer::new();
        a.add_range(10..21);
        a.remove_range(0..5);

        assert!(a.iter().eq(10..21));
    }

    #[test]
    fn iter_runs_merges_touching_runs() {
        // Stored runs are disjoint but nothing keeps them from touching
        let mut a = RunContainer::new();
        a.runs.extend_from_slice(&[Rle16::new(20, 9), Rle16::new(30, 9), Rle16::new(50, 0)]);

        let runs: Vec<_> = Container::Run(a).iter_runs(1 << 16).collect();
        assert_eq!(runs, vec![(65_556, 65_575), (65_586, 65_586)]);
    }

    #[test]
    fn deserialize_reads_every_run() {
        let mut a = RunContainer::new();
        a.add_range(10..21);
        a.add_range(40..50);

        let mut buf = Vec::new();
        a.serialize(&mut buf).unwrap();
        let read = RunContainer::deserialize(&mut std::io::Cursor::new(&buf)).unwrap();

        assert_eq!(read.num_runs(), 2);
        assert!(read.iter().eq(a.iter()));

        // A truncated buffer is an error
        let short = &buf[..buf.len() - 1];
        assert!(RunContainer::deserialize(&mut std::io::Cursor::new(short)).is_err());
    }

    #[test]
    fn add_range_below_the_first_run() {
        let mut a = RunContainer::new();
        a.add_range(10..21);
        a.add_range(0..5);

        assert!(a.iter().eq((0..5).chain(10..21)));
    }

    #[test]
    fn add_range_merges_overlapping_runs() {
        let mut a = RunContainer::new();
        a.add_range(0..10);
        a.add_range(20..30);
        a.add_range(30..40);
        assert!(a.iter().eq((0..10).chain(20..40)));

        // The exclusive end stays out, a run starting just past it stays separate
        a.add_range(50..60);
        a.add_range(45..49);
        assert!(a.iter().eq((0..10).chain(20..40).chain(45..49).chain(50..60)));

        // One range covering several runs
        a.add_range(5..55);
        assert!(a.iter().eq(0..60));
        assert_eq!(a.num_runs(), 1);

        // A range ending at the end of the container
        a.add_range(65_000..(1 << 16));
        assert!(a.iter().eq((0..60).chain(65_000..=65_535)));
    }

    #[test]
    fn contains_range_needs_every_value() {
        let mut a = RunContainer::new();
        a.add_range(0..10);
        a.add_range(20..30);

        assert!(a.contains_range(0..10));
        assert!(a.contains_range(22..25));
        assert!(!a.contains_range(0..11));
        assert!(!a.contains_range(5..25));
        assert!(!a.contains_range(10..20));
        assert!(!a.contains_range(25..31));

        // Touching stored runs hold a range across them
        let mut b = RunContainer::new();
        b.runs.extend_from_slice(&[super::Rle16::new(0, 9), super::Rle16::new(10, 9)]);
        assert!(b.contains_range(5..20));
        assert!(!b.contains_range(5..21));
    }

    #[test]
    fn remove_range_across_runs() {
        let runs = || {
            let mut a = RunContainer::new();
            a.add_range(0..10);
            a.add_range(20..30);
            a.add_range(40..50);
            a
        };

        // Trims the runs at both ends and drops the one between
        let mut a = runs();
        a.remove_range(5..45);
        assert!(a.iter().eq((0..5).chain(45..50)));

        // Starts and ends in gaps
        let mut a = runs();
        a.remove_range(15..35);
        assert!(a.iter().eq((0..10).chain(40..50)));

        // Splits one run
        let mut a = runs();
        a.remove_range(22..25);
        assert!(a.iter().eq((0..10).chain(20..22).chain(25..30).chain(40..50)));

        // Removes the last runs
        let mut a = runs();
        a.remove_range(25..(1 << 16));
        assert!(a.iter().eq((0..10).chain(20..25)));

        // Removes every run
        let mut a = runs();
        a.remove_range(0..50);
        assert!(a.is_empty());
    }

    #[test]
    fn rank_of_a_full_container() {
        let mut a = RunContainer::new();
        a.add_range(0..(1 << 16));

        assert_eq!(a.rank(0), 1);
        assert_eq!(a.rank(65_534), 65_535);
        assert_eq!(a.rank(65_535), 1 << 16);
    }
}