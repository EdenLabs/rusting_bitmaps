use std::slice::{Iter, IterMut};
use std::ops::{Deref, DerefMut};
use std::iter;
use std::mem;

use crate::utils;
use crate::container::*;
use crate::container::array_ops;
use crate::container::run_ops;

/// A RLE word storing the value and the length of that run
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

    /// Get the total size of the run. `value + length`
    pub fn sum(&self) -> u16 {
        self.value + self.length
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
#[derive(Clone)]
pub struct RunContainer {
    runs: Vec<Rle16>
}

impl RunContainer {
    /// Create a new run container
    pub fn new() -> Self {
        Self {
            runs: Vec::new()
        }
    }
    
    /// Create a new run container with a specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            runs: Vec::with_capacity(capacity)
        }
    }
    
    /// Shrink the run container's backing memory to fit it's contents
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
    
    /// Add all values in the range [min-max] to the run container
    pub fn add_range(&mut self, min: u16, max: u16) {
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
            let common_max = self.runs[runs_min + common - 1].sum();
            let result_min = utils::min(common_min, min);
            let result_max = utils::max(common_max, max);

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
    
    /// Remove all values in the range [min-max] from the run container
    pub fn remove_range(&mut self, min: u16, max: u16) {
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

        let mut first = result_to_compressed_index(self.find_run(min));
        let mut last = result_to_compressed_index(self.find_run(max));

        if first >= 0 {
            let v_first = self.runs[first as usize];
            if min > v_first.value && max < v_first.sum() {
                // Split into two runs

                // Right interval
                self.runs.insert(first as usize + 1, Rle16::new(max + 1, v_first.sum() - (max + 1)));

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
            let run_max = self.runs[last as usize].sum();
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
    
    /// Check if the container contains all the values in [min-max]
    pub fn contains_range(&self, min: u16, max: u16) -> bool {
        let mut count = 0;
        let index;

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
        unimplemented!()
    }

    /// The number of runs in the run container
    pub fn num_runs(&self) -> usize {
        self.runs.len()
    }

    /// The capacity of the container in runs
    pub fn capacity(&self) -> usize {
        self.runs.capacity()
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
    
    /// Clear all elements from the container
    pub fn clear(&mut self) {
        self.runs.clear()
    }

    // TODO: Delete this, redundant with `clone()`
    /// Copy another container into this one replacing it's contents
    pub fn copy_from(&mut self, other: &RunContainer) {
        self.runs.clear();
        self.runs.copy_from_slice(&other.runs);
    }
    
    /// Get an iterator over the runs of this container
    pub fn iter(&self) -> Iter<Rle16> {
        self.runs.iter()
    }
    
    /// Get a mutable iterator over the runs of this container
    pub fn iter_mut(&mut self) -> IterMut<Rle16> {
        self.runs.iter_mut()
    }
    
    /// Get the minimum value of this container
    pub fn min(&self) -> u16 {
        if self.runs.len() == 0 {
            return 0;
        }

        self.runs[0].value
    }
    
    /// Get the maximum value of this container
    pub fn max(&self) -> u16 {
        if self.runs.len() == 0 {
            return 0;
        }

        let run = self.runs[self.runs.len() - 1];
        
        run.value + run.length
    }
    
    /// Get the rank of a value in the set. The relative position of an element in the set
    pub fn rank(&self, value: u32) -> usize {
        let mut sum = 0;
        for run in self.runs.iter() {
            let start = run.value as u32;
            let length = run.length as u32;
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

    /// Select the element with `rank` starting the search from `start_rank` and outputting the result into `element`.
    /// Returns true if found, false if there is no element with that rank in the set
    pub fn select(&self, rank: u32, start_rank: &mut u32, element: &mut u32) -> bool {
        for run in self.runs.iter() {
            let length = run.length as u32;
            let value = run.value as u32;

            if rank <= *start_rank + length {
                *element = value + rank - *start_rank;
                return true;
            }
            else {
                *start_rank += length + 1;
            }
        }

        return false;
    }

    /// Convert self into the most efficient container. Returns self if already optimal
    pub fn into_efficient_container(self) -> ContainerType {
        let cardinality = self.cardinality();
        let size_as_run = RunContainer::serialized_size(self.num_runs());
        let size_as_bitset = BitsetContainer::serialized_size();
        let size_as_array = ArrayContainer::serialized_size(cardinality);
        let min_size_other = utils::min(size_as_array, size_as_bitset);

        // Run is still smallest, leave as is
        if size_as_run < min_size_other {
            return ContainerType::Run(self);
        }

        // Array is smallest, convert
        if cardinality < DEFAULT_MAX_SIZE {
            return ContainerType::Array(self.into());
        }

        // Bitset is smallest, convert
        return ContainerType::Bitset(self.into());
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
            let max = self.runs[middle].sum();
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
}

impl RunContainer {
    /// Get the size in bytes of a container with `num_runs`
    pub fn serialized_size(num_runs: usize) -> usize {
        mem::size_of::<u16>() + mem::size_of::<Rle16>() * num_runs
    }
}

impl From<ArrayContainer> for RunContainer {
    fn from(container: ArrayContainer) -> Self {
        From::from(&container)
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
    fn from(container: BitsetContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a BitsetContainer> for RunContainer {
    fn from(container: &'a BitsetContainer) -> Self {
        unimplemented!()
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

impl Container for RunContainer { }

impl Union<Self> for RunContainer {
    type Output = Self;

    fn union_with(&self, other: &Self, out: &mut Self::Output) {
        run_ops::union(&self.runs, &other.runs, &mut out.runs);
    }
}

impl Union<ArrayContainer> for RunContainer {
    type Output = Self;

    fn union_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        if self.is_full() {
            out.copy_from(self);
            return;
        }

        out.reserve(2 * other.cardinality() + self.runs.len());

        let mut rle_index = 0;
        let mut array_index = 0;
        let mut prev_rle;

        if self.runs[rle_index].value <= other[array_index] {
            prev_rle = self.runs[rle_index];
            out.runs.push(prev_rle);
            rle_index += 1;
        }
        else {
            prev_rle = Rle16::new(other[array_index], 0);
            out.runs.push(prev_rle);
            array_index += 1;
        }

        while rle_index < self.runs.len() && array_index < other.cardinality() {
            if self.runs[rle_index].value < other[array_index] {
                unsafe { run_ops::append(&mut out.runs, &self.runs[rle_index], &mut prev_rle) };// TODO: Refactor this
                rle_index += 1;
            }
            else {
                run_ops::append_value(&mut out.runs, other[array_index], &mut prev_rle);
                array_index += 1;
            }
        }

        if array_index < other.cardinality() {
            while array_index < other.cardinality() {
                run_ops::append_value(&mut out.runs, other[array_index], &mut prev_rle);
                array_index += 1;
            }
        }
        else {
            while rle_index < self.runs.len() {
                unsafe { run_ops::append(&mut out.runs, &self.runs[rle_index], &mut prev_rle) };
                rle_index += 1;
            }
        }
    }
}

impl Union<BitsetContainer> for RunContainer {
    type Output = BitsetContainer;

    fn union_with(&self, other: &BitsetContainer, out: &mut Self::Output) {
        out.copy_from(other);

        for rle in self.iter() {
            out.set_range(rle.value as usize, rle.sum() as usize);
        }
    }
}

impl Intersection<Self> for RunContainer {
    type Output = Self;

    fn intersect_with(&self, other: &Self, out: &mut Self::Output) {
        run_ops::intersect(&self.runs, &other.runs, &mut out.runs);
    }
}

impl Intersection<ArrayContainer> for RunContainer {
    type Output = ArrayContainer;

    fn intersect_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        if self.is_full() {
            out.copy_from(other);
            return;
        }

        if out.capacity() < other.cardinality() {
            out.reserve(other.cardinality() - out.capacity());
        }

        if self.runs.len() == 0 {
            return;
        }

        unsafe {
            let mut rle_index = 0;
            let mut array_index = 0;
            let mut rle = self.runs[rle_index];

            while array_index < other.cardinality() {
                let value = other[array_index];
                
                while rle.sum() < value {
                    rle_index += 1;

                    if rle_index == self.runs.len() {
                        return;
                    }

                    rle = self.runs[rle_index];
                }

                if rle.value > value {
                    array_index = array_ops::advance_until(
                        &other,
                        array_index,
                        rle.value,
                        other.cardinality()
                    );
                }
                else {
                    out.push(value);
                    array_index += 1;
                }
            }
        }
    }
}

impl Intersection<BitsetContainer> for RunContainer {
    type Output = ContainerType;

    fn intersect_with(&self, other: &BitsetContainer, out: &mut ContainerType) {
        if self.is_full() {
            *out = {
                let mut bitset = BitsetContainer::new();
                bitset.copy_from(other);

                ContainerType::Bitset(bitset)
            };
            
            return;
        }

        let mut card = self.cardinality();
        if card <= DEFAULT_MAX_SIZE {
            if card > other.cardinality() {
                card = other.cardinality();
            }

            let mut array = ArrayContainer::with_capacity(card);
            for run in self.runs.iter() {
                array.add_from_range(run.value, run.sum());
            }

            *out = ContainerType::Array(array);

            return;
        }
        else {
            let mut bitset = BitsetContainer::new();
            bitset.copy_from(other);

            // Unset all bits in between the runs
            let mut start = 0;
            for run in self.runs.iter() {
                let end = run.value as usize;
                bitset.unset_range(start, end);

                start = end + run.length as usize + 1;
            }

            bitset.unset_range(start, 1 << 16);

            if bitset.cardinality() > DEFAULT_MAX_SIZE {
                *out = ContainerType::Bitset(bitset);
                return;
            }
            else {
                *out = ContainerType::Array(bitset.into());
            }
            
            return;
        }
    }
}

impl Difference<Self> for RunContainer {
    type Output = ContainerType;

    fn difference_with(&self, other: &Self, out: &mut Self::Output) {
        let mut r = RunContainer::new();
        run_ops::difference(&self.runs, &other.runs, &mut r.runs);

        *out = r.into_efficient_container();
    }
}

impl Difference<ArrayContainer> for RunContainer {
    type Output = ArrayContainer;

    fn difference_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        const ARBITRARY_THRESHOLD: usize = 32;

        let cardinality = self.cardinality();

        if cardinality < ARBITRARY_THRESHOLD {

        }

        if cardinality <= DEFAULT_MAX_SIZE {
            let mut array = ArrayContainer::with_capacity(cardinality);
            
            unimplemented!();
            
            return;
        }

        // TODO: Implement non consuming conversion operation for bitset from run
        unimplemented!()
    }
}

impl Difference<BitsetContainer> for RunContainer {
    type Output = ContainerType;

    fn difference_with(&self, other: &BitsetContainer, out: &mut Self::Output) {
        let cardinality = self.cardinality();
        
        // Result is an array
        if cardinality < DEFAULT_MAX_SIZE {
            let mut array = ArrayContainer::with_capacity(cardinality);
            for rle in self.runs.iter() {
                for value in rle.value..rle.sum() {
                    if !other.get(value) {
                        array.push(value);
                    }
                }
            }

            *out = ContainerType::Array(array);
        }
        // Result may be a bitset
        else {
            let mut bitset = other.clone();

            let mut last_pos = 0;
            for rle in self.runs.iter() {
                let start = rle.value as usize;
                let end = (rle.sum() + 1) as usize;

                bitset.unset_range(last_pos, start);
                bitset.flip_range(start, end);

                last_pos = end;
            }

            bitset.unset_range(last_pos, 1 << 16);

            // Result is not a bitset, convert to array
            if bitset.cardinality() <= DEFAULT_MAX_SIZE {
                *out = ContainerType::Array(bitset.into());
                return;
            }
            // Result is a bitset
            else {
                *out = ContainerType::Bitset(bitset);
            }
        }
    }
}

impl SymmetricDifference<Self> for RunContainer {
    type Output = ContainerType;

    fn symmetric_difference_with(&self, other: &Self, out: &mut Self::Output) {
        unimplemented!()
    }
}

impl SymmetricDifference<ArrayContainer> for RunContainer {
    type Output = ContainerType;

    fn symmetric_difference_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        unimplemented!()
    }
}

impl SymmetricDifference<BitsetContainer> for RunContainer {
    type Output = ContainerType;

    fn symmetric_difference_with(&self, other: &BitsetContainer, out: &mut Self::Output) {
        unimplemented!()
    }
}

impl Subset<Self> for RunContainer {
    fn subset_of(&self, other: &Self) -> bool {
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
        unimplemented!()
    }
}

impl Subset<BitsetContainer> for RunContainer {
    fn subset_of(&self, other: &BitsetContainer) -> bool {
        unimplemented!()
    }
}