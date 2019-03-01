use std::slice::{Iter, IterMut};

use crate::container::*;
use crate::align::{Align, A32};

const BITSET_SIZE_IN_WORDS: usize = (1 << 16) / 64;

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

    /// Set all the bits within the range denoted by `min`->`max`
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
    }

    /// Set all the bits in the bitset
    pub fn set_all(&mut self) {
        // TODO: Vectorize
        for word in &mut *self.bitset {
            *word = std::u64::MAX;
        }
        
        self.cardinality = 1 << 16;
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

    pub fn clear(&mut self) {
        // TODO: Vectorize
        for word in &mut *self.bitset {
            *word = 0;
        }
        
        self.cardinality = 0;
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

    pub fn add_from_range(&mut self, min: u16, max: u16, step: u16) {
        assert!(min < max);
        assert!(step != 0);

        unimplemented!()
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

    pub fn contains(&self, value: u16) -> bool {
        self.get(value)
    }

    pub fn contains_range(&self, min: u16, max: u16) -> bool {
        self.get_range(min, max)
    }

    pub fn cardinality(&self) -> usize {
        self.cardinality
    }
    
    pub fn min(&self) -> Option<u16> {
        for (i, word) in (*self.bitset).iter().enumerate() {
            if *word != 0 {
                let r = word.trailing_zeros() as u16;

                return Some(r + i as u16 * 64);
            }
        }

        None
    }

    pub fn max(&self) -> Option<u16> {
        for (i, word) in (*self.bitset).iter().enumerate().rev() {
            if *word != 0 {
                let r = word.leading_zeros() as u16;

                return Some(i as u16 * 64 + 63 - r);
            }
        }

        None
    }

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

    pub fn iter(&self) -> Iter<u64> {
        self.bitset.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<u64> {
        self.bitset.iter_mut()
    }
}

impl From<ArrayContainer> for BitsetContainer {
    fn from(container: ArrayContainer) -> Self {
        unimplemented!()
    }
}

impl From<RunContainer> for BitsetContainer {
    fn from(container: RunContainer) -> Self {
        unimplemented!()
    }
}

impl Container for BitsetContainer { }

impl Difference<Self> for BitsetContainer {
    fn difference_with(&self, other: &Self, out: &mut Self) {
        unimplemented!()
    }
}

impl Difference<ArrayContainer> for BitsetContainer {
    fn difference_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl Difference<RunContainer> for BitsetContainer {
    fn difference_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl SymmetricDifference<Self> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &Self, out: &mut Self) {
        unimplemented!()
    }
}

impl SymmetricDifference<ArrayContainer> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl SymmetricDifference<RunContainer> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl Union<Self> for BitsetContainer {
    fn union_with(&self, other: &Self, out: &mut Self) {
        unimplemented!()
    }
}

impl Union<ArrayContainer> for BitsetContainer {
    fn union_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl Union<RunContainer> for BitsetContainer {
    fn union_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl Intersection<Self> for BitsetContainer {
    fn intersect_with(&self, other: &Self, out: &mut Self) {
        unimplemented!()
    }
}

impl Intersection<ArrayContainer> for BitsetContainer {
    fn intersect_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl Intersection<RunContainer> for BitsetContainer {
    fn intersect_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
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