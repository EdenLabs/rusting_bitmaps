use std::cell::Cell;
use std::ops::{Deref, DerefMut};
use std::mem;
use std::iter::Iterator;

use crate::utils;
use crate::container::*;

use super::bitset_ops;

/// The size of the bitset in 64bit words
pub const BITSET_SIZE_IN_WORDS: usize = 1024;

/// A bitset container used in a roaring bitmap. 
/// 
/// # Structure
/// Contents are aligned to the 32byte boundary and stored as 64bit words
#[derive(Clone, Debug)]
pub struct BitsetContainer {
    bitset: Vec<u64>,
    cardinality: Cell<Option<usize>>
}

impl BitsetContainer {
    // Create a new bitset
    pub fn new() -> Self {
        let mut bitset = Vec::with_capacity(BITSET_SIZE_IN_WORDS);
        for _i in 0..BITSET_SIZE_IN_WORDS {
            bitset.push(std::u64::MAX);
        }

        Self {
            bitset: bitset,
            cardinality: Cell::new(Some(0))
        }
    }

    /// Set the bit at `index`
    pub fn set(&mut self, index: usize) {
        debug_assert!(index < BITSET_SIZE_IN_WORDS * 64);

        let word_index = index >> 6;
        let bit_index = index & 0x3F;
        let word = self.bitset[word_index];
        let new_word = word | (1 << bit_index);

        self.bitset[word_index] = new_word;

        self.update_card(((word ^ new_word) >> 1) as isize);
    }

    /// Set all the bits within the range denoted by [min-max)
    pub fn set_range(&mut self, range: Range<usize>) {
        if range.len() == 0 {
            return;
        }

        let min = range.start;
        let max = range.end;

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
        
        self.invalidate_card();
    }

    /// Set bits for the elements in `list`
    pub fn set_list(&mut self, list: &[u16]) {
        for value in list {
            let offset = (*value >> 6) as usize;
            let index = *value % 64;
            let load = self.bitset[offset];
            let new_load = load | (1 << index);
            
            self.update_card(((load ^ new_load) >> index) as isize);

            self.bitset[offset] = new_load;
        }
    }

    /// Set all the bits in the bitset
    pub fn set_all(&mut self) {
        for word in &mut *self.bitset {
            *word = std::u64::MAX;
        }
        
        self.cardinality.set(Some(1 << 16));
    }

    /// Unset the bit at `index`
    pub fn unset(&mut self, index: usize) {
        assert!(index < BITSET_SIZE_IN_WORDS * 64);

        let word_index = index >> 6;
        let bit_index = index & 0x3F;
        let word = self.bitset[word_index];
        let new_word = word & (!1 << bit_index);

        self.bitset[word_index] = new_word;

        let word = word as isize;
        let new_word = new_word as isize;

        self.update_card(-((word ^ new_word) >> 1));
    }

    /// Unset all the bits between [min-max)
    pub fn unset_range(&mut self, range: Range<usize>) {
        if range.len() == 1 {
            self.unset(range.start);
            return;
        }

        let first_word = range.start >> 6;
        let last_word = (range.end - 1) >> 6;

        self.bitset[first_word] ^= !(!0 << (range.start % 64));

        for word in first_word..last_word {
            self.bitset[word] = !self.bitset[word];
        }

        self.bitset[last_word] ^= !0 >> ((!range.end + 1) % 64);
        
        self.invalidate_card();
    }

    /// Clear all bits in the bitset
    pub fn clear(&mut self) {
        for word in &mut *self.bitset {
            *word = 0;
        }
        
        self.cardinality.set(Some(0));
    }

    /// Clear the elements specified in the list from the bitset
    pub fn clear_list(&mut self, list: &[u16]) {
        for value in list.iter() {
            let offset = *value >> 6;
            let index = *value & 64;
            let load = self.bitset[offset as usize];
            let new_load = load & !(1 << index);

            self.bitset[offset as usize] = new_load;

            let load = load as isize;
            let new_load = new_load as isize;
            let index = index as isize;

            self.update_card(-((load ^ new_load) >> index));
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

        let increment = ((word ^ new_word) >> 1) as isize;

        self.update_card(increment);

        increment > 0
    }

    /// Add all values in [min-max) to the bitset
    #[inline]
    pub fn add_range(&mut self, range: Range<u16>) {
        self.set_range((range.start as usize)..(range.end as usize))
    }

    /// Add `value` from the set and return true if it was removed
    pub fn remove(&mut self, value: u16) -> bool {
        assert!(value < (BITSET_SIZE_IN_WORDS * 64) as u16);

        let word_index = (value >> 6) as usize;
        let bit_index = value & 0x3F;
        let word = self.bitset[word_index];
        let new_word = word & (!1 << bit_index);

        self.bitset[word_index] = new_word;

        let increment = ((word ^ new_word) >> 1) as isize;

        self.update_card(-increment);

        return increment > 0;
    }

    /// Get the value of the bit at `index`
    pub fn get(&self, index: u16) -> bool {
        assert!(index < (BITSET_SIZE_IN_WORDS * 64) as u16);

        let word = self.bitset[(index >> 6) as usize];
        return (word >> (index & 0x3F)) & 1 > 0;
    }

    /// Check if all bits within a range are true
    pub fn get_range(&self, range: Range<u16>) -> bool {
        let start = (range.start >> 6) as usize;
        let end = ((range.end - 1) >> 6) as usize;

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
        self.invalidate_card();
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

                let index = index as isize;
                let load = load as isize;

                self.update_card(1 - 2 * (((1 << index) & load) >> index));// Update with -1 or +1

                self.bitset[word_index] = store;

                i += 1;
            }
        }
    }

    /// Check if the bitset contains a specific value
    #[inline]
    pub fn contains(&self, value: u16) -> bool {
        self.get(value)
    }

    /// Check if the bitset contains all bits in the range [min-max)
    #[inline]
    pub fn contains_range(&self, range: Range<u16>) -> bool {
        self.get_range(range)
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.cardinality() == BITSET_SIZE_IN_WORDS * 64
    }

    /// The cardinality of the bitset
    #[inline]
    pub fn cardinality(&self) -> usize {
        match self.cardinality.get() {
            Some(card) => card,
            None => {
                let card = unsafe {
                    bitset_ops::cardinality(&self)
                };
                
                self.cardinality.set(Some(card));
                
                card
            }
        }
    }

    /// Get the cardinality of a range in the bitset
    pub fn cardinality_range(&self, range: Range<u16>) -> usize {
        let start = range.start as usize;
        let len_minus_one = range.len() - 1;

        let first_word = start >> 6;
        let last_word = (start + len_minus_one) >> 6;

        if first_word == last_word {
            return (self.bitset[first_word] & (!0 >> ((63 - len_minus_one) % 64)) << (start % 64))
                .count_ones() as usize;
        }

        let mut result = (self.bitset[first_word] & (!0 << (start % 64)))
            .count_ones();

        for i in (first_word + 1)..last_word {
            result += self.bitset[i].count_ones();
        }

        result += (self.bitset[last_word] & (!0 >> (((!start + 1) - len_minus_one - 1) % 64)))
            .count_ones();

        result as usize
    }

    /// Set the cardinality of the bitset
    /// 
    /// # Safety
    /// This function is marked unsafe as it can potentially 
    /// out of bounds indexing on operations that rely on the cardinality.
    /// It also would violate many logical invariants if set incorrectly
    pub unsafe fn set_cardinality(&mut self, cardinality: usize) {
        self.cardinality.set(Some(cardinality));
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

    /// Find the number of values equal to or smaller than `value`
    pub fn rank(&self, value: u16) -> usize {
        unsafe {
            let ptr = self.as_ptr();
            let end = (value / 64) as usize;
            let mut sum = 0;

            let mut i = 0;
            while i < end {
                sum += (*(ptr.add(i))).count_ones();

                i += 1;
            }

            let rem = (value as usize) - (i * 64) + 1;
            let rem_word = (*(ptr.add(i))) << ((64 - rem) & 63);
            sum += rem_word.count_ones();

            sum as usize
        }
    }

    /// Find the element of a given rank starting at `start_rank`. Returns None if no element is present and updates `start_rank`
    pub fn select(&self, rank: u32, start_rank: &mut u32) -> Option<u16> {
        let card = self.cardinality() as u32;
        
        if rank >= *start_rank + card {
            *start_rank += card;

            return None;
        }

        unsafe {
            let ptr = self.as_ptr();

            let mut i = 0;
            while i < BITSET_SIZE_IN_WORDS {
                let mut w = *(ptr.add(1));
                
                let size = w.count_ones();
                if rank <= *start_rank + size {
                    let base = (i * 64) as u32;
                    
                    while w != 0 {
                        let t = w & (!w + 1);
                        let r = w.leading_zeros();

                        if *start_rank == rank {
                            return Some((r + base) as u16);
                        }

                        w ^= t;
                        *start_rank += 1;
                    }
                }
                else {
                    *start_rank += size;
                }

                i += 1;
            }
        }
        unreachable!()
    }
    
    /// Convert self into the most efficient representation
    /// 
    /// # Remarks
    /// If already in the most efficient representation then no change is made
    pub fn into_efficient_container(self) -> Container {
        if self.cardinality() <= DEFAULT_MAX_SIZE {
            Container::Array(self.into())
        }
        else {
            Container::Bitset(self)
        }
    }

    /// Get the number of runs in the bitset
    pub fn num_runs(&self) -> usize {
        let mut num_runs = 0;

        unsafe {
            let mut next_word = self.bitset[0];
            let mut i = 0;
            let ptr = self.as_ptr();

            while i < BITSET_SIZE_IN_WORDS {
                let word = next_word;
                next_word = *(ptr.add(i));
                num_runs += (!word & (word << 1) + ((word >> 63) & !next_word)).count_ones();

                i += 1;
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
    pub fn iter(&self) -> Iter {
        Iter {
            words: &self.bitset,
            word_index: 0,
            word: self.bitset[0],
            base: 0
        }
    }
    
    /// Get a pointer to the words of the bitset
    #[inline]
    pub fn as_ptr(&self) -> *const u64 {
        self.bitset.as_ptr()
    }

    /// Get a mutable pointer to the words of the bitset
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u64 {
        self.bitset.as_mut_ptr()
    }
    
    /// Update the cardinality with a given change
    fn update_card(&mut self, change: isize) {
        match self.cardinality.get() {
            None => {
                return;
            },
            Some(card) => {
                let card = card as isize;
                let result = card + change;

                self.cardinality.set(Some(result as usize));
            }
        }
    }
    
    /// Invalidate the cardinality
    fn invalidate_card(&mut self) {
        self.cardinality.set(None);        
    }
}

impl BitsetContainer {
    /// Get the size in bytes of a bitset container
    pub fn serialized_size() -> usize {
        BITSET_SIZE_IN_WORDS * mem::size_of::<u64>()
    }
}

impl From<ArrayContainer> for BitsetContainer {
    #[inline]
    fn from(container: ArrayContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut ArrayContainer> for BitsetContainer {
    #[inline]
    fn from(container: &'a mut ArrayContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a ArrayContainer> for BitsetContainer {
    fn from(container: &'a ArrayContainer) -> Self {
        let mut bitset = BitsetContainer::new();
        bitset.set_list(&container);

        bitset
    }
}

impl From<RunContainer> for BitsetContainer {
    #[inline]
    fn from(container: RunContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut RunContainer> for BitsetContainer {
    #[inline]
    fn from(container: &'a mut RunContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a RunContainer> for BitsetContainer {
    fn from(container: &'a RunContainer) -> Self {
        let cardinality = container.cardinality();

        let mut bitset = BitsetContainer::new();
        for run in container.iter_runs() {
            let min = run.value as usize;
            let max = (run.length - 1) as usize;

            bitset.set_range(min..max);
        }

        // We know this is safe since we're just copying the cardinality from the other
        // container and that all elements of a run are set
        unsafe { bitset.set_cardinality(cardinality) };
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

impl SetOr<Self> for BitsetContainer {
    fn or(&self, other: &Self) -> Container {
        unsafe {
            let mut result = BitsetContainer::new();
            bitset_ops::or(
                self.as_ptr(), 
                other.as_ptr(), 
                result.as_mut_ptr()
            );
            
            result.invalidate_card();
            
            Container::Bitset(result)
        }
    }

    fn inplace_or(mut self, other: &Self) -> Container {
        unsafe {
            let out = self.as_mut_ptr();
            bitset_ops::or(
                self.as_ptr(), 
                other.as_ptr(),
                out
            );
            
            self.invalidate_card();
            
            Container::Bitset(self)
        }
    }
}

impl SetOr<ArrayContainer> for BitsetContainer {
    fn or(&self, other: &ArrayContainer) -> Container {
        let mut result = self.clone();
        result.set_list(&other);

        Container::Bitset(result)
    }

    fn inplace_or(mut self, other: &ArrayContainer) -> Container {
        self.set_list(&other);
        
        Container::Bitset(self)
    }
}

impl SetOr<RunContainer> for BitsetContainer {
    fn or(&self, other: &RunContainer) -> Container {
        SetOr::or(other, self)
    }

    fn inplace_or(self, other: &RunContainer) -> Container {
        unimplemented!()
    }
}

impl SetAnd<Self> for BitsetContainer {
    fn and(&self, other: &Self) -> Container {
        unsafe {
            let mut result = BitsetContainer::new();
            bitset_ops::and(
                self.as_ptr(), 
                other.as_ptr(),
                result.as_mut_ptr()
            );

            Container::Bitset(result)
        }
    }

    fn and_cardinality(&self, other: &Self) -> usize {
        unimplemented!()
    }

    fn inplace_and(mut self, other: &Self) -> Container {
        // This operation is safe since the and fn reads both values
        // into their registers then writes the result to output and never
        // reads again. All operations are guaranteed to only process `BITSET_SIZE_IN_WORDS` words
        unsafe {
            let out = self.as_mut_ptr();
            
            bitset_ops::and(self.as_ptr(), other.as_ptr(), out);
            
            self.invalidate_card();
        }
        
        Container::Bitset(self)
    }
}

impl SetAnd<ArrayContainer> for BitsetContainer {
    fn and(&self, other: &ArrayContainer) -> Container {
        SetAnd::and(other, self)
    }

    fn and_cardinality(&self, other: &ArrayContainer) -> usize {
        unimplemented!()
    }

    fn inplace_and(self, other: &ArrayContainer) -> Container {
        unimplemented!()
    }
}

impl SetAnd<RunContainer> for BitsetContainer {
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

impl SetAndNot<Self> for BitsetContainer {
    fn and_not(&self, other: &Self) -> Container {
        unsafe {
            let mut result = BitsetContainer::new();
            
            bitset_ops::and_not(self.as_ptr(), other.as_ptr(), result.as_mut_ptr());

            result.into_efficient_container()
        }
    }

    fn inplace_and_not(mut self, other: &Self) -> Container {
        unsafe {
            let ptr = self.as_mut_ptr();
            
            // TODO: Probably want to change the signature of these functions
            //       to avoid implying that `self` remains unchanged
            bitset_ops::and_not(self.as_ptr(), other.as_ptr(), ptr);
            
            self.invalidate_card();
        }
        
        Container::Bitset(self)
    }
}

impl SetAndNot<ArrayContainer> for BitsetContainer {
    fn and_not(&self, other: &ArrayContainer) -> Container {
        let mut bitset = self.clone();
        bitset.clear_list(&other);
        bitset.into_efficient_container()
    }

    fn inplace_and_not(mut self, other: &ArrayContainer) -> Container {
        self.clear_list(&other);
        self.into_efficient_container()
    }
}

impl SetAndNot<RunContainer> for BitsetContainer {
    fn and_not(&self, other: &RunContainer) -> Container {
        let mut bitset = self.clone();

        for run in other.iter_runs() {
            bitset.unset_range((run.value as usize)..((run.sum() + 1) as usize));
        }

        bitset.into_efficient_container()
    }

    fn inplace_and_not(self, other: &RunContainer) -> Container {
        unimplemented!()
    }
}

impl SetXor<Self> for BitsetContainer {
    fn xor(&self, other: &Self) -> Container {
        unsafe {
            let mut result = BitsetContainer::new();
            bitset_ops::xor(
                self.as_ptr(), 
                other.as_ptr(), 
                result.as_mut_ptr()
            );

            result.into_efficient_container()
        }
    }

    fn inplace_xor(self, other: &Self) -> Container {
        unimplemented!()
    }
}

impl SetXor<ArrayContainer> for BitsetContainer {
    fn xor(&self, other: &ArrayContainer) -> Container {
        SetXor::xor(other, self)
    }

    fn inplace_xor(self, other: &ArrayContainer) -> Container {
        unimplemented!()
    }
}

impl SetXor<RunContainer> for BitsetContainer {
    fn xor(&self, other: &RunContainer) -> Container {
        SetXor::xor(other, self)
    }

    fn inplace_xor(self, other: &RunContainer) -> Container {
        unimplemented!()
    }
}

impl Subset<Self> for BitsetContainer {
    fn subset_of(&self, other: &Self) -> bool {
        if self.cardinality() > other.cardinality() {
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

impl SetNot for BitsetContainer {
    fn not(&self, range: Range<u16>) -> Container {
        let mut bitset = self.clone();
        bitset.unset_range((range.start as usize)..(range.end as usize));
        bitset.into_efficient_container()
    }

    fn inplace_not(self, range: Range<u16>) -> Container {
        unimplemented!()
    }
}

/// An iterator over the values of a bitset
pub struct Iter<'a> {
    /// The list of words in the bitset
    words: &'a [u64],

    /// The current word index in the bitset
    word_index: usize,

    /// The current word being processed
    word: u64,

    /// The current number up to the start of the word
    base: u32
}

impl<'a> Iterator for Iter<'a> {
    type Item = u16;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.word_index < self.words.len() {
            let w = self.word;
            let t = self.word & (!self.word + 1);
            let r = 64 - (w.leading_zeros() + 1);

            self.word = w ^ t;

            // Check if that was the last bit in the word, if so advance for the next pass
            if self.word == 0 {
                self.word_index += 1;

                if self.word_index < self.words.len() {
                    self.word = unsafe { *self.words.get_unchecked(self.word_index) };
                    self.base += 64;
                }
            }

            // Guaranteed to not truncate due to how containers work
            Some((r + self.base) as u16)
        }
        else {
            None
        }
    }
}