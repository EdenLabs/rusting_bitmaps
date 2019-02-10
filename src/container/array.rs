use std::fmt;
use crate::container::*;

const DEFAULT_MAX_SIZE: usize = 4096;

pub enum AddError {
    AlreadyPresent,
    ExceededCapacity
}

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

    pub fn with_range(min: usize, max: usize) -> Self {
        let mut container = Self {
            array: Vec::with_capacity(max - min + 1)
        };

        for i in min..max {
            container.array.push(i as u16);
        }

        container
    }

    pub fn into_raw(self) -> Vec<u16> {
        self.array
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
        unimplemented!()
    }

    pub fn add_from_range(&mut self, min: usize, max: usize, step: usize) {
        let range = min..max;

        // Resize to fit all new elements
        let len = self.len();
        let cap = self.capacity();
        let slack = cap - len;
        if slack < range.len() {
            self.array.reserve(range.len() - slack);
        }

        // Append new elements
        for i in (min..max).step_by(step) {
            self.array.push(i as u16);
        }
    }

    pub fn append(&mut self, value: u16) {
        unimplemented!()
    }

    pub fn try_add(&mut self, value: u16, max_capacity: usize) -> Result<(), AddError> {
        unimplemented!()
    }

    pub fn remove(&mut self, value: u16) -> bool {
        unimplemented!()
    }

    pub fn contains(&self, value: u16) -> bool {
        unimplemented!()
    }

    pub fn min(&self) -> u16 {
        unimplemented!();
    }

    pub fn max(&self) -> u16 {
        unimplemented!()
    }

    pub fn rank(&self) -> u16 {
        unimplemented!()
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

    pub fn iter(&self) -> RoaringIter<Self> {
        unimplemented!()
    }
}

impl fmt::Debug for ArrayContainer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ArrayContainer {:?}", self.array)
    }
}

impl From<BitsetContainer> for ArrayContainer {
    fn from(container: BitsetContainer) -> Self {
        unimplemented!()
    }
}

impl From<RunContainer> for ArrayContainer {
    fn from(container: RunContainer) -> Self {
        unimplemented!()
    }
}

impl Container for ArrayContainer { }

impl Difference<Self> for ArrayContainer {
    fn difference_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Difference<BitsetContainer> for ArrayContainer {
    fn difference_with(&self, other: &BitsetContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Difference<RunContainer> for ArrayContainer {
    fn difference_with(&self, other: &RunContainer) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<Self> for ArrayContainer {
    fn symmetric_difference_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<BitsetContainer> for ArrayContainer {
    fn symmetric_difference_with(&self, other: &BitsetContainer) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<RunContainer> for ArrayContainer {
    fn symmetric_difference_with(&self, other: &RunContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Union<Self> for ArrayContainer {
    fn union_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Union<BitsetContainer> for ArrayContainer {
    fn union_with(&self, other: &BitsetContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Union<RunContainer> for ArrayContainer {
    fn union_with(&self, other: &RunContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<Self> for ArrayContainer {
    fn intersect_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<BitsetContainer> for ArrayContainer {
    fn intersect_with(&self, other: &BitsetContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<RunContainer> for ArrayContainer {
    fn intersect_with(&self, other: &RunContainer) -> ContainerType {
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

// Simd utils
use std::arch::x86_64::{
    __m256i,
    _mm256_min_epu16,
    _mm256_max_epu16,
    _mm256_alignr_epi8
};

pub unsafe fn avx_merge(a: &__m256i, b: &__m256i, min: &mut __m256i, max: &mut __m256i) {
    let mut temp = _mm256_min_epu16(*a, *b);
    *max = _mm256_max_epu16(*a, *b);
    temp = _mm256_alignr_epi8(temp, temp, 2);

    for _i in 0..14 {
        *min = _mm256_min_epu16(temp, *max);
        *max = _mm256_max_epu16(temp, *max);
        temp = _mm256_alignr_epi8(*min, *min, 2);
    }

    *min = _mm256_min_epu16(temp, *max);
    *max = _mm256_max_epu16(temp, *max);
    *min = _mm256_alignr_epi8(*min, *min, 2);
}

/// Compute the union of of two u16 vectors
/// 
/// # Safety
/// A 16 byte alignment for the vecs is assumed
pub unsafe fn union_u16(a: &Vec<u16>, b: &Vec<u16>, target: &mut Vec<u16>) {
    //let range 
}