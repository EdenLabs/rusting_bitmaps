use std::slice::{Iter, IterMut};
use std::ops::{Deref, DerefMut};

use crate::utils;
use crate::container::*;
use crate::align::{Align, A32};

use super::bitset_ops;

/// The size of the bitset in 64bit words
pub const BITSET_SIZE_IN_WORDS: usize = (1 << 16) >> 6;

/// A bitset container used in a roaring bitmap. 
/// 
/// # Structure
/// Bits are aligned to the 32byte boundary and stored as 64bit words
#[derive(Clone)]
pub struct BitsetContainer {
    bitset: Align<Vec<u64>, A32>,
    cardinality: usize
}

impl BitsetContainer {
    // Create a new bitset
    pub fn new() -> Self {
        let mut bitset = Vec::with_capacity(BITSET_SIZE_IN_WORDS);
        for _i in 0..BITSET_SIZE_IN_WORDS {
            bitset.push(std::u64::MAX);
        }

        Self {
            bitset: Align::new(bitset),
            cardinality: 0
        }
    }

    /// Set the bit at `index`
    pub fn set(&mut self, index: usize) {
        assert!(index < BITSET_SIZE_IN_WORDS * 64);

        let word_index = index >> 6;
        let bit_index = index & 0x3F;
        let word = self.bitset[word_index];
        let new_word = word | (1 << bit_index);

        self.bitset[word_index] = new_word;

        self.cardinality += ((word ^ new_word) >> 1) as usize;
    }

    /// Set all the bits within the range denoted by [min-max]
    pub fn set_range(&mut self, min: usize, max: usize) {
        assert!(min < max);
        assert!(max < BITSET_SIZE_IN_WORDS * 64);

        let first_index = min >> 6;
        let last_index = (max >> 6) - 1;

        if first_index == last_index {
            self.bitset[first_index] |= (!0_u64 << (min as u64 & 0x3F)) & (!0_u64 >> ((!max as u64 + 1) >> & 0x3F));
            return;
        }

        self.bitset[first_index] |= !0_u64 << (min as u64 & 0x3F);
        
        for i in (first_index + 1)..last_index {
            self.bitset[i] = !0;
        }

        self.bitset[last_index] |= !0_u64 >> ((!max as u64 + 1) >> & 0x3F);
        // TODO: Update cardinality
    }

    /// Set bits for the elements in `list`
    pub fn set_list(&mut self, list: &[u16]) {
        for value in list {
            let offset = (*value >> 6) as usize;
            let index = *value % 64;
            let load = self.bitset[offset];
            let new_load = load | (1 << index);
            
            self.cardinality += ((load ^ new_load) >> index) as usize;

            self.bitset[offset] = new_load;
        }
    }

    /// Set all the bits in the bitset
    pub fn set_all(&mut self) {
        // TODO: Vectorize
        for word in &mut *self.bitset {
            *word = std::u64::MAX;
        }
        
        self.cardinality = 1 << 16;
    }

    /// Copy the contents of `other` into self
    #[inline]
    pub fn copy_from(&mut self, other: &BitsetContainer) {
        self.bitset.copy_from_slice(&other.bitset);
    }

    /// Unset the bit at `index`
    pub fn unset(&mut self, index: usize) {
        assert!(index < BITSET_SIZE_IN_WORDS * 64);

        let word_index = index >> 6;
        let bit_index = index & 0x3F;
        let word = self.bitset[word_index];
        let new_word = word & (!1 << bit_index);

        self.bitset[word_index] = new_word;

        self.cardinality += ((word ^ new_word) >> 1) as usize;
    }

    // TODO: Unify these index types

    /// Unset all the bits between [min-max]
    pub fn unset_range(&mut self, min: usize, max: usize) {
        if min == max {
            self.unset(min);
            return;
        }

        let first_word = min >> 6;
        let last_word = (max - 1) >> 6;

        self.bitset[first_word] ^= !(!0 << (min % 64));

        for word in first_word..last_word {
            self.bitset[word] = !self.bitset[word];
        }

        self.bitset[last_word] ^= !0 >> ((!max + 1) % 64)
    }

    /// Clear all bits in the bitset
    pub fn clear(&mut self) {
        // TODO: Vectorize
        for word in &mut *self.bitset {
            *word = 0;
        }
        
        self.cardinality = 0;
    }

    /// Clear the elements specified in the list from the bitset
    pub fn clear_list(&mut self, list: &[u16]) {
        for value in list.iter() {
            let offset = *value >> 6;
            let index = *value & 64;
            let load = self.bitset[offset as usize];
            let new_load = load & !(1 << index);

            self.bitset[offset as usize] = new_load;
            self.cardinality -= ((load ^ new_load) >> index) as usize;
        }
    }

    /// Add `value` to the set and return true if it was set
    pub fn add(&mut self, value: u16) -> bool {
        assert!(value < (BITSET_SIZE_IN_WORDS * 64) as u16);
        
        let word_index = (value >> 6) as usize;
        let bit_index = value & 0x3F;
        let word = self.bitset[word_index];
        let new_word = word | (1 << bit_index);

        self.bitset[word_index] = new_word;

        let increment = ((word ^ new_word) >> 1) as usize;

        self.cardinality += increment;

        increment > 0
    }

    /// Add all values in [min-max] to the bitset
    pub fn add_range(&mut self, min: u16, max: u16) {
        assert!(min < max);
        
        self.set_range(min as usize, max as usize)
    }

    /// Add `value` from the set and return true if it was removed
    pub fn remove(&mut self, value: u16) -> bool {
        assert!(value < (BITSET_SIZE_IN_WORDS * 64) as u16);

        let word_index = (value >> 6) as usize;
        let bit_index = value & 0x3F;
        let word = self.bitset[word_index];
        let new_word = word & (!1 << bit_index);

        self.bitset[word_index] = new_word;

        let increment = ((word ^ new_word) >> 1) as usize;

        self.cardinality -= increment;

        return increment > 0;
    }

    /// Get the value of the bit at `index`
    pub fn get(&self, index: u16) -> bool {
        assert!(index < (BITSET_SIZE_IN_WORDS * 64) as u16);

        let word = self.bitset[(index >> 6) as usize];
        return (word >> (index & 0x3F)) & 1 > 0;
    }

    /// Check if all bits within a range are true
    pub fn get_range(&self, min: u16, max: u16) -> bool {
        assert!(min < max);
        assert!(max < (BITSET_SIZE_IN_WORDS * 64) as u16);

        let start = (min >> 6) as usize;
        let end = (max >> 6) as usize;

        let first = !((1 << (start & 0x3F)) - 1);
        let last = (1 << (end & 0x3F)) - 1;

        // Start and end are the same, check if the range of bits are set
        if start == end {
            return self.bitset[end] & first & last == first & last;
        }

        if self.bitset[start] & first != first {
            return false;
        }

        if self.bitset[end] & last != last {
            return false;
        }

        for i in (start + 1)..end {
            if self.bitset[i] != std::u64::MAX {
                return false;
            }
        }

        return true;
    }

    /// Flip all bits in the range [min-max]
    pub fn flip_range(&mut self, min: usize, max: usize) {
        if min == max {
            return;
        }

        let first_word = min / 64;
        let last_word = (max - 1) / 64;
        
        self.bitset[first_word] ^= !(!0 << (min % 64));
        
        for i in first_word..last_word {
            self.bitset[i] = !self.bitset[i];
        }

        self.bitset[last_word] ^= !0 >> ((!max + 1) % 64);
    }

    /// Flip all bits contained in `list`
    pub fn flip_list(&mut self, list: &[u16]) {
        unsafe {
            let ptr = list.as_ptr();
            let mut i = 0;
            while i < list.len() {
                let val = *ptr.offset(i as isize);
                let word_index = (val >> 6) as usize;// Index / word_size
                let index = val % 64;
                let load = self.bitset[word_index];
                let store = load ^ (1 << index);

                self.cardinality += (1 - 2 * (((1 << index) & load) >> index)) as usize;// Update with -1 or +1

                self.bitset[word_index] = store;

                i += 1;
            }
        }
    }

    /// Check if the bitset contains a specific value
    pub fn contains(&self, value: u16) -> bool {
        self.get(value)
    }

    /// Check if the bitset contains all bits in the range [min-max]
    pub fn contains_range(&self, min: u16, max: u16) -> bool {
        self.get_range(min, max)
    }

    /// The cardinality of the bitset
    pub fn cardinality(&self) -> usize {
        self.cardinality as usize
    }
    
    /// Get the smallest value in the bitset
    pub fn min(&self) -> Option<u16> {
        for (i, word) in (*self.bitset).iter().enumerate() {
            if *word != 0 {
                let r = word.trailing_zeros() as u16;

                return Some(r + i as u16 * 64);
            }
        }

        None
    }

    /// Get the largest value in the bitset
    pub fn max(&self) -> Option<u16> {
        for (i, word) in (*self.bitset).iter().enumerate().rev() {
            if *word != 0 {
                let r = word.leading_zeros() as u16;

                return Some(i as u16 * 64 + 63 - r);
            }
        }

        None
    }

    /// Get the number of runs in the bitset
    pub fn num_runs(&self) -> usize {
        let mut num_runs = 0;

        unsafe {
            let mut next_word = self.bitset[0];
            let mut i = 0;

            while i < BITSET_SIZE_IN_WORDS {
                let word = next_word;
                next_word = *self.bitset.get_unchecked(i);
                num_runs += (!word & (word << 1) + ((word >> 63) & !next_word)).count_ones();
            }

            let word = next_word;
            num_runs += (!word & (word << 1) + ((word >> 63) & !next_word)).count_ones();

            if word & 0x8000000000000000 != 0 {
                num_runs += 1;
            }
        }

        num_runs as usize
    }

    /// Get an iterator over the words of the bitset
    pub fn iter(&self) -> Iter<u64> {
        self.bitset.iter()
    }

    /// Get a mutable iterator over the words of the bitset
    pub fn iter_mut(&mut self) -> IterMut<u64> {
        self.bitset.iter_mut()
    }
}

impl BitsetContainer {
    /// Get the size in bytes of a bitset container
    pub fn serialized_size() -> usize {
        BITSET_SIZE_IN_WORDS * 8
    }
}

impl From<ArrayContainer> for BitsetContainer {
    fn from(container: ArrayContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut ArrayContainer> for BitsetContainer {
    fn from(container: &'a mut ArrayContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a ArrayContainer> for BitsetContainer {
    fn from(container: &'a ArrayContainer) -> Self {
        let mut bitset = BitsetContainer::new();

        for value in container.iter() {
            bitset.set(*value as usize);
        }

        bitset
    }
}

impl From<RunContainer> for BitsetContainer {
    fn from(container: RunContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut RunContainer> for BitsetContainer {
    fn from(container: &'a mut RunContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a RunContainer> for BitsetContainer {
    fn from(container: &'a RunContainer) -> Self {
        let cardinality = container.cardinality();

        let mut bitset = BitsetContainer::new();
        for run in container.iter() {
            bitset.set_range(run.value as usize, (run.length - 1) as usize);
        }

        bitset.cardinality = cardinality;
        bitset
    }
}

impl PartialEq for BitsetContainer {
    fn eq(&self, other: &BitsetContainer) -> bool {
        utils::mem_equals(&self.bitset, &other.bitset)
    }
}

impl Deref for BitsetContainer {
    type Target = [u64];

    fn deref(&self) -> &Self::Target {
        &self.bitset
    }
}

impl DerefMut for BitsetContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bitset
    }
}

impl Union<Self> for BitsetContainer {
    type Output = Self;

    fn union_with(&self, other: &Self, out: &mut Self::Output) {
        unsafe {
            let cardinality = bitset_ops::union(&self.bitset, &other.bitset, &mut out.bitset);
            out.cardinality = cardinality;
        }
    }
}

impl Union<ArrayContainer> for BitsetContainer {
    type Output = BitsetContainer;

    fn union_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        out.copy_from(self);
        out.set_list(&other);
    }
}

impl Union<RunContainer> for BitsetContainer {
    type Output = BitsetContainer;

    fn union_with(&self, other: &RunContainer, out: &mut Self::Output) {
        other.union_with(self, out)
    }
}

impl Intersection<Self> for BitsetContainer {
    type Output = Self;

    fn intersect_with(&self, other: &Self, out: &mut Self::Output) {
        unsafe {
            let cardinality = bitset_ops::intersect(&self.bitset, &other.bitset, &mut out.bitset);
            out.cardinality = cardinality;
        }
    }
}

impl Intersection<ArrayContainer> for BitsetContainer {
    type Output = ArrayContainer;

    fn intersect_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        other.intersect_with(self, out)
    }
}

impl Intersection<RunContainer> for BitsetContainer {
    type Output = Container;
    
    fn intersect_with(&self, other: &RunContainer, out: &mut Self::Output) {
        other.intersect_with(self, out)
    }
}

impl Difference<Self> for BitsetContainer {
    type Output = Container;

    fn difference_with(&self, other: &Self, out: &mut Self::Output) {
        unsafe {
            let mut bitset = BitsetContainer::new();
            let cardinality = bitset_ops::difference(
                &self.bitset,
                &other.bitset,
                &mut bitset.bitset
            );

            if cardinality <= DEFAULT_MAX_SIZE {
                *out = Container::Array(bitset.into());
            }
            else {
                *out = Container::Bitset(bitset);
            }
        }
    }
}

impl Difference<ArrayContainer> for BitsetContainer {
    type Output = Container;

    fn difference_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        let mut bitset = BitsetContainer::new();
        bitset.copy_from(self);
        bitset.clear_list(&other);

        if bitset.cardinality() <= DEFAULT_MAX_SIZE {
            *out = Container::Array(bitset.into());
            return;
        }

        *out = Container::Bitset(bitset);
    }
}

impl Difference<RunContainer> for BitsetContainer {
    type Output = Container;

    fn difference_with(&self, other: &RunContainer, out: &mut Self::Output) {
        let mut bitset = BitsetContainer::new();
        bitset.copy_from(self);

        for run in other.iter() {
            bitset.unset_range(
                run.value as usize,
                (run.sum() + 1) as usize
            );
        }

        if bitset.cardinality() <= DEFAULT_MAX_SIZE {
            *out = Container::Array(bitset.into());
            return;
        }

        *out = Container::Bitset(bitset);
    }
}

impl SymmetricDifference<Self> for BitsetContainer {
    type Output = Container;

    fn symmetric_difference_with(&self, other: &Self, out: &mut Self::Output) {
        let mut result = BitsetContainer::new();
        let cardinality = unsafe {
            bitset_ops::symmetric_difference(&self.bitset, &other.bitset, &mut result.bitset)
        };

        if cardinality <= DEFAULT_MAX_SIZE {
            *out = Container::Array(result.into());
            return;
        }
        else {
            *out = Container::Bitset(result);
            return;
        }
    }
}

impl SymmetricDifference<ArrayContainer> for BitsetContainer {
    type Output = Container;

    fn symmetric_difference_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        other.symmetric_difference_with(self, out)
    }
}

impl SymmetricDifference<RunContainer> for BitsetContainer {
    type Output = Container;

    fn symmetric_difference_with(&self, other: &RunContainer, out: &mut Self::Output) {
        other.symmetric_difference_with(self, out)
    }
}

impl Subset<Self> for BitsetContainer {
    fn subset_of(&self, other: &Self) -> bool {
        if self.cardinality > other.cardinality {
            return false;
        }

        for (word_0, word_1) in self.bitset.iter().zip(other.bitset.iter()) {
            if *word_0 & *word_1 != *word_0 {
                return false;
            }
        }

        return true;
    }
}

impl Subset<ArrayContainer> for BitsetContainer {
    fn subset_of(&self, other: &ArrayContainer) -> bool {
        unimplemented!()
    }
}

impl Subset<RunContainer> for BitsetContainer {
    fn subset_of(&self, other: &RunContainer) -> bool {
        unimplemented!()
    }
}

impl Negation for BitsetContainer {
    fn negate(&self, out: &mut Container) {
        let mut bitset = self.clone();
        bitset.unset_range(0, self.cardinality());

        if bitset.cardinality() > DEFAULT_MAX_SIZE {
            *out = Container::Bitset(bitset);
        }
        else {
            *out = Container::Array(bitset.into());
        }
    }
}