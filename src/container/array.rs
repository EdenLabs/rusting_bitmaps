use std::fmt;
use std::ops::{Deref};
use std::slice::Iter;

use crate::utils::mem_equals;
use crate::container::*;
use crate::container::array_ops::*;

pub const DEFAULT_MAX_SIZE: usize = 4096;

#[derive(Clone)]
pub struct ArrayContainer {
    array: Vec<u16>
}

impl ArrayContainer {
    pub fn new() -> Self {
        Self {
            array: Vec::with_capacity(DEFAULT_MAX_SIZE)
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            array: Vec::with_capacity(capacity)
        }
    }

    pub fn with_range(min: usize, max: usize, step: usize) -> Self {
        assert!(min < max);
        assert!(step != 0);
        
        let mut container = Self {
            array: Vec::with_capacity(max - min + 1)
        };

        for i in (min..max).step_by(step) {
            container.array.push(i as u16);
        }

        container
    }

    pub fn into_raw(self) -> Vec<u16> {
        self.array
    }

    #[inline]
    pub fn cardinality(&self) -> usize {
        // Len is the same as the cardinality for raw sets of integers
        self.array.len()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.array.capacity()
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.array.shrink_to_fit();
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    pub fn copy_into(&self, target: &mut ArrayContainer) {
        let cap = self.capacity();
        let target_cap = target.capacity();

        if cap > target_cap {
            target.array.reserve(cap - target_cap);
        }

        target.array.clear();
        target.array.extend(self.array.iter());
    }

    pub fn add(&mut self, value: u16) -> bool {
        self.add_with_cardinality(value, std::usize::MAX)
    }

    pub fn add_with_cardinality(&mut self, value: u16, max_cardinality: usize) -> bool {
        let can_append = {
            let is_max_value = match self.max() {
                Some(max) => max < value,
                None => true
            };

            is_max_value && self.cardinality() < max_cardinality
        };

        if can_append {
            self.append(value);
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
    pub fn add_from_range(&mut self, min: u32, max: u32) {
        assert!(min < max);

        let range = min..max;

        // Resize to fit all new elements
        let len = self.len();
        let cap = self.capacity();
        let slack = cap - len;
        if slack < range.len() {
            self.array.reserve(range.len() - slack);
        }

        // Append new elements
        for i in range {
            self.array.push(i as u16);
        }
    }

    pub fn append(&mut self, value: u16) {
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
    pub fn remove_range(&mut self, min: usize, max: usize) {
        assert!(min < max);

        if (min..max).len() == 0 || max > self.len() {
            return;
        }

        let range = max..self.array.len();

        self.array.copy_within(range, min);
    }

    /// Check if the array contains a specified value
    pub fn contains(&self, value: u16) -> bool {
        self.array.binary_search(&value).is_ok()
    }

    /// Check if the array contains all values within [min-max] (exclusive)
    pub fn contains_range(&self, min: u16, max: u16) -> bool {
        assert!(min < max);

        let min = min as usize;
        let max = max as usize;

        if min as usize > self.len() || max as usize > self.len() {
            return false;
        }

        let min_val = exponential_search(&self.array, self.len(), min as u16);
        let max_val = exponential_search(&self.array, self.len(), (max - 1) as u16);

        match (min_val, max_val) {
            (Ok(min_index), Ok(max_index)) =>  {
                max_index - min_index == max - min
            },
            _ => false
        }
    }

    pub fn select(&self, rank: usize, start_rank: &mut usize) -> Option<u32> {
        let cardinality = self.cardinality();
        if *start_rank + cardinality <= rank {
            *start_rank += cardinality;

            None
        }
        else {
            unsafe {
                let index = rank - *start_rank;
                let element = self.array.get_unchecked(index) as *const u16 as *const u32;

                Some(*element)
            }
        }
    }

    #[inline]
    pub fn min(&self) -> Option<u16> {
        if self.array.len() == 0 {
            None
        }
        else {
            Some(self.array[0])
        }
    }

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

    pub fn iter(&self) -> Iter<u16> {
        self.array.iter()
    }
}

impl fmt::Debug for ArrayContainer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ArrayContainer {:?}", self.array)
    }
}

impl Deref for ArrayContainer {
    type Target = [u16];

    fn deref(&self) -> &[u16] {
        &self.array
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
        unimplemented!()
    }
}

impl From<RunContainer> for ArrayContainer {
    fn from(container: RunContainer) -> Self {
        let cardinality = container.cardinality();

        let mut array = ArrayContainer::with_capacity(cardinality);
        for run in container.iter() {
            let run_start = run.value;
            let run_end = run_start + run.length;

            for i in run_start..run_end {
                array.append(i);
            }
        }

        array
    }
}

impl Container for ArrayContainer { }

impl Difference<Self> for ArrayContainer {
    fn difference_with(&self, other: &Self, out: &mut Self) {
        difference(&self.array, &other.array, &mut out.array);
    }
}

impl Difference<BitsetContainer> for ArrayContainer {
    fn difference_with(&self, other: &BitsetContainer, out: &mut BitsetContainer) {
        unimplemented!()
    }
}

impl Difference<RunContainer> for ArrayContainer {
    fn difference_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl SymmetricDifference<Self> for ArrayContainer {
    fn symmetric_difference_with(&self, other: &Self, out: &mut Self) {
        symmetric_difference(&self.array, &other.array, &mut out.array);
    }
}

impl SymmetricDifference<BitsetContainer> for ArrayContainer {
    fn symmetric_difference_with(&self, other: &BitsetContainer, out: &mut BitsetContainer) {
        unimplemented!()
    }
}

impl SymmetricDifference<RunContainer> for ArrayContainer {
    fn symmetric_difference_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl Union<Self> for ArrayContainer {
    type Output = Self;

    fn union_with(&self, other: &Self, out: &mut Self::Output) {
        union(&self.array, &other.array, &mut out.array);
    }
}

impl Union<BitsetContainer> for ArrayContainer {
    type Output = BitsetContainer;

    fn union_with(&self, other: &BitsetContainer, out: &mut Self::Output) {
        other.union_with(self, out)
    }
}

impl Union<RunContainer> for ArrayContainer {
    type Output = RunContainer;

    fn union_with(&self, other: &RunContainer, out: &mut Self::Output) {
        other.union_with(self, out)
    }
}

impl Intersection<Self> for ArrayContainer {
    fn intersect_with(&self, other: &Self, out: &mut Self) {
        intersect(&self.array, &other.array, &mut out.array);
    }
}

impl Intersection<BitsetContainer> for ArrayContainer {
    fn intersect_with(&self, other: &BitsetContainer, out: &mut BitsetContainer) {
        unimplemented!()
    }
}

impl Intersection<RunContainer> for ArrayContainer {
    fn intersect_with(&self, other: &RunContainer, out: &mut RunContainer) {
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

impl Negation for ArrayContainer {
    fn negate(&self, out: &mut ContainerType) {
        let mut bitset = BitsetContainer::new();
        bitset.set_all();
        bitset.clear_list(&self.array);

        *out = ContainerType::Bitset(bitset);
    }
}