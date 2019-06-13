use std::mem;
use std::ops::{Deref, DerefMut, Range};
use std::slice::Iter;

use crate::utils::mem_equals;
use crate::container::*;
use crate::container::array_ops;

/// An array container. Elements are sorted numerically and represented as individual values in the array
#[derive(Clone, Debug)]
pub struct ArrayContainer {
    array: Vec<u16>
}

impl ArrayContainer {
    /// Create a new array container
    pub fn new() -> Self {
        Self {
            array: Vec::with_capacity(DEFAULT_MAX_SIZE)
        }
    }

    /// Create a new array container with a specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            array: Vec::with_capacity(capacity)
        }
    }

    /// Convert the array container into it's raw representation
    pub fn into_raw(self) -> Vec<u16> {
        self.array
    }

    /// The cardinality of the array container
    #[inline]
    pub fn cardinality(&self) -> usize {
        // Len is the same as the cardinality for raw sets of integers
        self.array.len()
    }

    /// Set the cardinality of the array container
    /// 
    /// # Safety
    /// Assumes that the container's capacity is < `cardinality`
    #[inline]
    pub unsafe fn set_cardinality(&mut self, cardinality: usize) {
        self.array.set_len(cardinality);
    }

    /// The capacity of the array container
    #[inline]
    pub fn capacity(&self) -> usize {
        self.array.capacity()
    }

    /// Shrink the capacity of the array container to match the cardinality
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.array.shrink_to_fit();
    }

    /// Reserve space for `additional` elements
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    /// Copy the elements from `other` into `self`
    pub fn copy_from(&mut self, other: &ArrayContainer) {
        self.copy_from_slice(&other.array);
    }

    /// Push a value onto the end of the array
    /// 
    /// # Notes
    /// Assumes that the value is greater than all other elements in the array
    pub fn push(&mut self, value: u16) {
        assert!({
            // Ensure that value is greater than any element in the set
            if self.len() > 0 {
                value > self.max().unwrap()
            }
            else {
                true
            }
        });

        self.array.push(value);
    }

    /// Add a value to the array
    pub fn add(&mut self, value: u16) -> bool {
        self.add_with_cardinality(value, std::usize::MAX)
    }

    /// Add a value to the array with a max cardinality
    pub fn add_with_cardinality(&mut self, value: u16, max_cardinality: usize) -> bool {
        let can_append = {
            let is_max_value = match self.max() {
                Some(max) => max < value,
                None => true
            };

            is_max_value && self.cardinality() < max_cardinality
        };

        if can_append {
            self.push(value);
            return true;
        }

        match self.array.binary_search(&value) {
            Ok(_index) => {
                return true;
            },
            Err(index) => {
                if self.cardinality() < max_cardinality {
                    self.array.insert(index, value);

                    return true;
                }
                else {
                    return false;
                }
            }
        }
    }

    /// Add all values within the specified range
    pub fn add_range(&mut self, range: Range<u16>) {
        assert!(range.len() > 0);

        // Resize to fit all new elements
        let len = self.len();
        let cap = self.capacity();
        let slack = cap - len;
        if slack < range.len() {
            self.array.reserve(range.len() - slack);
        }

        // Append new elements
        for i in range {
            // This is technically valid since we only store the lower 16 bits
            // inside containers. The upper 16 are stored as keys in the roaring bitmap
            self.array.push(i as u16);
        }
    }

    /// Remove a specified value from the array
    pub fn remove(&mut self, value: u16) -> bool {
        match self.array.binary_search(&value) {
            Ok(index) => {
                self.array.remove(index);

                true
            },
            Err(_index) => false
        }
    }

    /// Remove all elements within the spefied range, exclusive
    pub fn remove_range(&mut self, range: Range<usize>) {
        if range.len() == 0 || range.end as usize > self.len() {
            return;
        }

        let len = self.len();

        self.array.copy_within((range.end)..len, range.start);
    }

    /// Check if the array contains a specified value
    pub fn contains(&self, value: u16) -> bool {
        self.array.binary_search(&value).is_ok()
    }

    /// Check if the array contains all values within [min-max)
    pub fn contains_range(&self, range: Range<u16>) -> bool {
        let rs = range.start;
        let re = range.end - 1;

        let min = array_ops::exponential_search(&self.array, self.len(), rs);
        let max = array_ops::exponential_search(&self.array, self.len(), re);

        if let (Ok(min_index), Ok(max_index)) = (min, max) {
            return max_index - min_index == (re - rs) as usize && self.array[min_index] == rs && self.array[max_index] == re;
        }

        false
    }

    /// Check if the array is full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.array.len() == DEFAULT_MAX_SIZE
    }

    pub fn select(&self, rank: u32, start_rank: &mut u32) -> Option<u16> {
        let cardinality = self.cardinality() as u32;
        if *start_rank + cardinality <= rank {
            *start_rank += cardinality;

            None
        }
        else {
            unsafe {
                let index = (rank - *start_rank) as usize;
                let element = self.array.get_unchecked(index);

                Some(*element)
            }
        }
    }

    /// The smallest element in the array. Returns `None` if `cardinality` is 0
    #[inline]
    pub fn min(&self) -> Option<u16> {
        if self.array.len() == 0 {
            None
        }
        else {
            Some(self.array[0])
        }
    }

    /// The largest element in the array. Returns `None` if the cardinality is 0
    #[inline]
    pub fn max(&self) -> Option<u16> {
        if self.array.len() == 0 {
            None
        }
        else {
            Some(self.array[self.array.len() - 1])
        }
    }

    /// Return the number of values equal to or smaller than `value`
    #[inline]
    pub fn rank(&self, value: u16) -> usize {
        match self.array.binary_search(&value) {
            Ok(index) => index + 1,
            Err(index) => index - 1
        }
    }

    /// Return the index of the first value equal to or smaller than x
    pub fn equal_or_larger(&self, value: u16) -> Option<usize> {
        if self.len() == 0 {
            return None;
        }

        match self.array.binary_search(&value) {
            Ok(index) => Some(index),
            Err(index) => {
                if index == 0 {
                    Some(index)
                }
                else {
                    Some(index - 1)
                }
            }
        }
    }

    /// Compute the number of runs in the array
    pub fn num_runs(&self) -> usize {
        let mut num_runs = 0;
        let mut previous = 0;

        for value in self.array.iter() {
            if *value != previous + 1 {
                num_runs += 1;
            }

            previous = *value;
        }

        num_runs
    }

    /// Get an iterator over the elements of the array
    pub fn iter(&self) -> Iter<u16> {
        self.array.iter()
    }
}

impl ArrayContainer {
    /// Get the size in bytes of a container with `cardinality`
    pub fn serialized_size(cardinality: usize) -> usize {
        cardinality * mem::size_of::<u16>() + 2
    }
}

impl Deref for ArrayContainer {
    type Target = [u16];

    fn deref(&self) -> &[u16] {
        &self.array
    }
}

impl DerefMut for ArrayContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.array
    }
}

impl PartialEq for ArrayContainer {
    fn eq(&self, other: &Self) -> bool {
        mem_equals(self, other)
    }

    fn ne(&self, other: &Self) -> bool {
        !mem_equals(self, other)
    }
}

impl Eq for ArrayContainer { }

impl From<BitsetContainer> for ArrayContainer {
    fn from(container: BitsetContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut BitsetContainer> for ArrayContainer {
    fn from(container: &'a mut BitsetContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a BitsetContainer> for ArrayContainer {
    fn from(container: &'a BitsetContainer) -> Self {
        unimplemented!()
    }
}

impl From<RunContainer> for ArrayContainer {
    fn from(container: RunContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut RunContainer> for ArrayContainer {
    fn from(container: &'a mut RunContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a RunContainer> for ArrayContainer {
    fn from(container: &'a RunContainer) -> Self {
        let cardinality = container.cardinality();

        let mut array = ArrayContainer::with_capacity(cardinality);
        for run in container.iter_runs() {
            let run_start = run.value;
            let run_end = run_start + run.length;

            for i in run_start..run_end {
                array.push(i);
            }
        }

        array
    }
}

impl SetOr<Self> for ArrayContainer {
    fn or(&self, other: &Self) -> Container {
        let result = ArrayContainer {
            array: array_ops::or(&self.array, &other.array)
        };

        Container::Array(result)
    }
    
    fn inplace_or(self, other: &Self) -> Container {
        unimplemented!()
    }
}

impl SetOr<BitsetContainer> for ArrayContainer {
    fn or(&self, other: &BitsetContainer) -> Container {
        SetOr::or(other, self)
    }
    
    fn inplace_or(self, other: &BitsetContainer) -> Container {
        unimplemented!()
    }
}

impl SetOr<RunContainer> for ArrayContainer {
    fn or(&self, other: &RunContainer) -> Container {
        SetOr::or(other, self)
    }
    
    fn inplace_or(self, other: &RunContainer) -> Container {
        unimplemented!()
    }
}

impl SetAnd<Self> for ArrayContainer {
    fn and(&self, other: &Self) -> Container {
        let result = ArrayContainer {
            array: array_ops::and(&self.array, &other.array)
        };

        Container::Array(result)
    }
    
    fn and_cardinality(&self, other: &Self) -> usize {
        unimplemented!()
    }
    
    fn inplace_and(self, other: &Self) -> Container {
        unimplemented!()
    }
}

impl SetAnd<BitsetContainer> for ArrayContainer {
    fn and(&self, other: &BitsetContainer) -> Container {
        let mut result = ArrayContainer::with_capacity(self.cardinality());

        unsafe {
            let mut new_card = 0;
            let card = self.cardinality();

            for i in 0..card {
                let key = *self.array.get_unchecked(i);
                *result.array.get_unchecked_mut(new_card) = key;
                new_card += other.contains(key) as usize;
            }

            result.array.set_len(new_card);
        }

        Container::Array(result)
    }

    fn and_cardinality(&self, other: &BitsetContainer) -> usize {
        unimplemented!()
    }
    
    fn inplace_and(self, other: &BitsetContainer) -> Container {
        unimplemented!()
    }
}

impl SetAnd<RunContainer> for ArrayContainer {
    fn and(&self, other: &RunContainer) -> Container {
        SetAnd::and(other, self)
    }

    fn and_cardinality(&self, other: &RunContainer) -> usize {
        unimplemented!()
    }
    
    fn inplace_and(self, other: &RunContainer) -> Container {
        unimplemented!()
    }
}

impl SetAndNot<Self> for ArrayContainer {
    fn and_not(&self, other: &Self) -> Container {
        let result = ArrayContainer {
            array: array_ops::and_not(&self.array, &other.array)
        };

        Container::Array(result)
    }
    
    fn inplace_and_not(self, other: &Self) -> Container {
        unimplemented!()
    }
}

impl SetAndNot<BitsetContainer> for ArrayContainer {
    fn and_not(&self, other: &BitsetContainer) -> Container {
        let mut result = ArrayContainer::with_capacity(self.cardinality());

        unsafe {
            let mut card = 0;
            for key in self.array.iter() {
                *result.get_unchecked_mut(card) = *key;
                card += !other.contains(*key) as usize;
            }

            result.set_cardinality(card);
        }

        Container::Array(result)
    }
    
    fn inplace_and_not(self, other: &BitsetContainer) -> Container {
        unimplemented!()
    }
}

impl SetAndNot<RunContainer> for ArrayContainer {
    fn and_not(&self, other: &RunContainer) -> Container {
        let mut result = ArrayContainer::with_capacity(self.cardinality());

        if other.num_runs() == 0 {
            result.copy_from(self);
            
            return Container::Array(result);
        }

        unsafe {
            let runs = other.deref();
            let mut run_start = runs.get_unchecked(0).value as usize;
            let mut run_end = run_start + runs.get_unchecked(0).length as usize;
            let mut which_run = 0;

            let mut i = 0;
            while i < self.cardinality() {
                let val = *self.array.get_unchecked(0) as usize;
                if val < run_start {
                    result.push(val as u16);
                    continue;
                }

                if val <= run_end {
                    continue;
                }

                loop {
                    if which_run + 1 < runs.len() {
                        which_run += 1;

                        let rle = runs.get_unchecked(which_run);
                        run_start = rle.value as usize;
                        run_end = rle.sum() as usize;
                    }
                    else {
                        run_start = (1 << 16) + 1;
                        run_end = (1 << 16) + 1;
                    }

                    if val <= run_end {
                        break;
                    }
                }

                i -= 1;
            }
        }
        
        Container::Array(result)
    }
    
    fn inplace_and_not(self, other: &RunContainer) -> Container {
        unimplemented!()
    }
}

impl SetXor<Self> for ArrayContainer {
    fn xor(&self, other: &Self) -> Container {
        let result = ArrayContainer {
            array: array_ops::xor(&self.array, &other.array)
        };

        Container::Array(result)
    }
    
    fn inplace_xor(self, other: &Self) -> Container {
        unimplemented!()
    }
}

impl SetXor<BitsetContainer> for ArrayContainer {
    fn xor(&self, other: &BitsetContainer) -> Container {
        let mut result = BitsetContainer::new();
        result.copy_from(other);
        result.flip_list(&self.array);

        // Array is a better representation for this set, convert
        if self.cardinality() <= DEFAULT_MAX_SIZE {
            Container::Array(result.into())
        }
        // Bitset is a better representation
        else {
            Container::Bitset(result)
        }
    }
    
    fn inplace_xor(self, other: &BitsetContainer) -> Container {
        unimplemented!()
    }
}

impl SetXor<RunContainer> for ArrayContainer {
    fn xor(&self, other: &RunContainer) -> Container {
        const THRESHOLD: usize = 32;
        if self.cardinality() < THRESHOLD {
            return SetXor::xor(other, self);
        }

        // Process as an array since the final result is probably an array
        if other.cardinality() <= DEFAULT_MAX_SIZE {
            return SetXor::xor(other.into(), self);
        }
        // Process as a bitset since the final result may be a bitset
        else {
            return SetXor::xor(other.into(), self);
        }
    }
    
    fn inplace_xor(self, other: &RunContainer) -> Container {
        unimplemented!()
    }
}

impl Subset<Self> for ArrayContainer {
    fn subset_of(&self, other: &Self) -> bool {
        if self.len() > other.len() {
            return false;
        }

        let mut i1 = 0;
        let mut i2 = 0;
        while i1 < self.array.len() && i2 < other.array.len() {
            if self.array[i1] == other.array[i2] {
                i1 += 1;
                i2 += 1;
            }
            else if self.array[i1] > other.array[i2] {
                i2 += 1;
            }
            else {
                return false;
            }
        }

        if i1 == self.array.len() {
            return true;
        }
        else {
            return false;
        }
    }
}

impl Subset<BitsetContainer> for ArrayContainer {
    fn subset_of(&self, other: &BitsetContainer) -> bool {
        unimplemented!()
    }
}

impl Subset<RunContainer> for ArrayContainer {
    fn subset_of(&self, other: &RunContainer) -> bool {
        unimplemented!()
    }
}

impl SetNot for ArrayContainer {
    fn not(&self, range: Range<u16>) -> Container {
        let mut bitset = BitsetContainer::new();
        bitset.set_all();
        bitset.clear_list(&self.array[(range.start as usize)..(range.end as usize)]);

        Container::Bitset(bitset)
    }

    fn inplace_not(self, range: Range<u16>) -> Container {
        unimplemented!()
    }
}