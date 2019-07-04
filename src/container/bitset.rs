use std::io::{self, Read, Write};
use std::iter::Iterator;
use std::mem;
use std::ops::{Deref, DerefMut, RangeBounds};

use crate::IntoBound;
use crate::container::*;

use super::bitset_ops;

// Notes on inplace ops:
//
// We cheat a bit here to get the operations inplace
// The routine is designed in a way that it reads in the word from each 
// bitset then outputs the result into the out set.
// 
// By telling it to read and write from the same set we can operate
// inplace and still maintain vectorization

/// The size of the bitset in 64bit words
pub const BITSET_SIZE_IN_WORDS: usize = 1024;

/// A bitset container used in a roaring bitmap. 
/// 
/// # Structure
/// Contents are aligned to the 32byte boundary and stored as 64bit words
#[derive(Clone, Debug)]
pub struct BitsetContainer {
    bitset: Vec<u64>,
    cardinality: LazyCardinality
}

impl BitsetContainer {
    // Create a new bitset
    pub fn new() -> Self {
        Self {
            bitset: vec![0; BITSET_SIZE_IN_WORDS],
            cardinality: LazyCardinality::none()
        }
    }

    // TODO: See if this is still necessary

    /// Set the bit at `index`
    pub fn set(&mut self, index: u16) -> bool {
        let word_index = usize::from(index / 64);
        let bit_index = index % 64;
        let word = self.bitset[word_index];
        let new_word = word | (1 << bit_index);
        let change = ((word ^ new_word) >> 1) as usize;

        self.bitset[word_index] = new_word;
        self.cardinality.increment(change);

        change > 0
    }

    /// Set all the bits within the range denoted by [min-max]
    pub fn set_range<R: RangeBounds<u16>>(&mut self, range: R) {
        let (min, max) = range.into_bound();

        if min == max {
            return;
        }

        let first_word = usize::from(min / 64);
        let last_word = usize::from((max - 1) / 64);

        if first_word == last_word {
            let w0 = std::u64::MAX << (min % 64);
            let w1 = std::u64::MAX >> ((!max + 1) % 64);
            self.bitset[first_word] |= w0 & w1;
            self.cardinality.invalidate();
        }
        else {
            self.bitset[first_word] |= std::u64::MAX << (min % 64);
            
            for word in self.bitset[(first_word + 1)..last_word].iter_mut() {
                *word = std::u64::MAX;
            }

            self.bitset[last_word] |= std::u64::MAX >> ((!max + 1) % 64);
            
            self.cardinality.invalidate();
        }
    }

    /// Set bits for the elements in `list`
    pub fn set_list(&mut self, list: &[u16]) {
        for value in list.iter() {
            let offset = (*value / 64) as usize;
            let index = *value % 64;
            let load = self.bitset[offset];
            let new_load = load | (1_u64 << index);
            self.bitset[offset] = new_load;
        }

        self.cardinality.invalidate();
    }

    /// Set all the bits in the bitset
    pub fn set_all(&mut self) {
        for word in &mut *self.bitset {
            *word = std::u64::MAX;
        }
        
        self.cardinality.set(1 << 16);
    }

    /// Unset the bit at `index`
    pub fn unset(&mut self, index: u16) -> bool {
        let word_index = usize::from(index / 64);
        let bit_index = u64::from(index % 64);

        let word = self.bitset[word_index];
        let mask = 1_u64 << bit_index;
        let new_word = word & !mask;
        let delta = (word ^ new_word) & mask;
        let change = (delta >> bit_index) as usize;

        self.bitset[word_index] = new_word;
        self.cardinality.decrement(change);

        change > 0
    }

    /// Unset all the bits between [min-max)
    pub fn unset_range<R: RangeBounds<u16>>(&mut self, range: R) {
        let (min, max) = range.into_bound();

        if min == max {
            return;
        }

        let first_word = usize::from(min / 64);
        let last_word = usize::from((max - 1) / 64);

        if first_word == last_word {
            let w0 = std::u64::MAX << (min % 64);
            let w1 = std::u64::MAX >> ((!max + 1) % 64);
            self.bitset[first_word] &= !(w0 & w1);
            self.cardinality.invalidate();

            return;
        }

        self.bitset[first_word] &= !(std::u64::MAX << (min % 64));
        
        for word in self.bitset[(first_word + 1)..last_word].iter_mut() {
            *word = 0;
        }

        self.bitset[last_word] &= !(std::u64::MAX >> ((!max + 1) % 64));
    
        self.cardinality.invalidate();
    }

    /// Clear the elements specified in the list from the bitset
    pub fn clear_list(&mut self, list: &[u16]) {
        for value in list.iter() {
            let offset = *value >> 6;
            let index = u32::from(*value % 64);
            let load = self.bitset[offset as usize];
            let new_load = load & !(1_u64)
                .checked_shl(index)
                .unwrap_or(0);

            self.bitset[offset as usize] = new_load;

            self.cardinality.decrement(
                (load ^ new_load).checked_shr(index).unwrap_or(0) as usize
            );
        }
    }

    /// Add `value` to the set and return true if it was set
    #[inline]
    pub fn add(&mut self, value: u16) -> bool {
        self.set(value)
    }

    /// Add all values in [min-max) to the bitset
    #[inline]
    pub fn add_range<R: RangeBounds<u16>>(&mut self, range: R) {
        self.set_range(range)
    }

    /// Remove `value` from the set and return true if it was removed
    #[inline]
    pub fn remove(&mut self, value: u16) -> bool {
        self.unset(value)
    }

    /// Get the value of the bit at `index`
    pub fn get(&self, index: u16) -> bool {
        let word_index = usize::from(index / 64);
        let bit_index = index % 64;
        let word = self.bitset[word_index];
        let mask = 1 << bit_index;

        (word & mask) > 0
    }

    /// Check if all bits within a range are true
    pub fn get_range<R: RangeBounds<u16>>(&self, range: R) -> bool {
        let (min, max) = range.into_bound();

        if min == max {
            return self.get(min);
        }

        let first_word = (min >> 6) as usize;
        let last_word = ((max + 1) >> 6) as usize;
        let w0 = std::u64::MAX << (min % 64);
        let w1 = std::u64::MAX >> ((!max + 1) % 64);

        // Start and end are the same, check if the range of bits are set
        if first_word == last_word {
            return (self.bitset[last_word] & w0 & w1) == (w0 & w1);
        }

        if self.bitset[first_word] & w0 != w0 {
            return false;
        }

        if self.bitset[last_word] & w1 != w1 {
            return false;
        }

        for i in (first_word + 1)..last_word {
            if self.bitset[i] != std::u64::MAX {
                return false;
            }
        }

        true
    }

    /// Flip a specific bit in the bitset
    pub fn flip(&mut self, index: u16) {
        let word_index = index / 64;
        let bit_index = index % 64;

        self.bitset[word_index as usize] ^= !(1 << bit_index);
        self.cardinality.invalidate();
    }

    /// Flip all bits in the range [min-max)
    pub fn flip_range<R: RangeBounds<u16>>(&mut self, range: R) {
        let (min, max) = range.into_bound();

        if min == max {
            return self.flip(min);
        }

        let min = u32::from(min);
        let max = u32::from(max);
        let first_word = (min / 64) as usize;
        let last_word = ((max - 1) / 64) as usize;
        
        self.bitset[first_word] ^= !((!0) << (min % 64));
        
        for i in first_word..last_word {
            self.bitset[i] = !self.bitset[i];
        }

        self.bitset[last_word] ^= (!0) >> ((!max).wrapping_add(1) % 64);
        self.cardinality.invalidate();
    }

    /// Flip all bits contained in `list`
    pub fn flip_list(&mut self, list: &[u16]) {
        for value in list.iter() {
            let word_index = (*value >> 6) as usize;
            let index = *value % 64;
            let load = self.bitset[word_index];
            let store = load ^ (1_u64 << index);
            let change = 1 - 2 * ((((1_u64 << index) & load) >> index) as isize);// Update with -1 or +1
 
            self.cardinality.add(change);
            self.bitset[word_index] = store;
        }
    }

    /// Check if the bitset contains a specific value
    #[inline]
    pub fn contains(&self, value: u16) -> bool {
        self.get(value)
    }

    /// Check if the bitset contains all bits in the range [min-max)
    #[inline]
    pub fn contains_range<R: RangeBounds<u16>>(&self, range: R) -> bool {
        self.get_range(range)
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.cardinality() == BITSET_SIZE_IN_WORDS * 64
    }

    /// The cardinality of the bitset
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.cardinality.get(|| bitset_ops::cardinality(&self))
    }

    /// Get the cardinality of a range in the bitset
    pub fn cardinality_range<R: RangeBounds<u16>>(&self, range: R) -> usize {
        let (min, max) = range.into_bound();

        if min == max {
            return if self.get(min) { 1 } else { 0 };
        }

        let start = min as usize;
        let len_minus_one = ((max - min) - 1) as usize;

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
        let end = (value / 64) as usize;
        let mut sum = 0;

        let iter = self.bitset[0..end].iter()
            .enumerate();

        let mut last = 0;
        for (i, word) in iter {
            sum += word.count_ones();
            last = i;
        }

        let rem = (value as usize) - (last * 64) + 1;
        let rem_word = self.bitset[last] << ((64 - rem) & 63);
        sum += rem_word.count_ones();

        sum as usize
    }

    /// Find the element of a given rank starting at `start_rank`. Returns None if no element is present and updates `start_rank`
    pub fn select(&self, rank: u32, start_rank: &mut u32) -> Option<u16> {
        let card = self.cardinality() as u32;
        
        if rank >= *start_rank + card {
            *start_rank += card;

            return None;
        }

        let iter = self.bitset.iter()
            .enumerate();

        for (i, word) in iter {
            let size = word.count_ones();
            
            if rank <= *start_rank + size {
                let mut w = *word;
                let base = (i * 64) as u32;
                
                while w != 0 {
                    let t = w & (!w + 1);
                    let r = w.trailing_zeros();

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
        #[inline]
        fn count_runs(word: u64, next_word: u64) -> u32 {
            let w0 = (!word) & (word << 1);
            let w1 = (word >> 63) & (!next_word);

            (w0 + w1).count_ones()
        }

        let mut num_runs = 0;
        let mut next_word = self.bitset[0];

        for w in self.bitset[1..].iter() {
            let word = next_word;
            next_word = *w;

            num_runs += count_runs(word, next_word);
        }

        let word = next_word;
        num_runs += count_runs(word, next_word);

        if word & (1 << 31) != 0 {
            num_runs += 1;
        }

        num_runs as usize
    }

    /// Get an iterator over the words of the bitset
    pub fn iter(&self) -> Iter {
        let (index, first_word) = {
            let mut index = 0;
            let mut word = self.bitset[0];
            for (i, w) in self.bitset.iter().enumerate() {
                if *w != 0 {
                    word = *w;
                    index = i;
                    break;
                }
            }

            (index, word)
        };

        Iter {
            words: &self.bitset,
            word_index: index,
            word: first_word,
            base: (index * 64) as u32// TODO: Fixme
        }
    }
    
    /// Get a mutable pointer to the words of the bitset
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u64 {
        self.bitset.as_mut_ptr()
    }
}

impl BitsetContainer {
    /// Get the size in bytes of a bitset container
    pub fn serialized_size() -> usize {
        BITSET_SIZE_IN_WORDS * mem::size_of::<u64>()
    }

    /// Serialize the array into `buf` according to the roaring format spec
    #[cfg(target_endian = "little")]
    pub fn serialize<W: Write>(&self, buf: &mut W) -> io::Result<usize> {
        unsafe {
            let ptr = self.bitset.as_ptr() as *const u8;
            let num_bytes = mem::size_of::<u64>() * self.bitset.len();
            let byte_slice = slice::from_raw_parts(ptr, num_bytes);

            buf.write(byte_slice)
        }
    }

    /// Deserialize an array container according to the roaring format spec
    #[cfg(target_endian = "little")]
    pub fn deserialize<R: Read>(buf: &mut R) -> io::Result<Self> {
        unsafe {
            let mut result = BitsetContainer::new();
            let ptr = result.as_mut_ptr() as *mut u8;
            let num_bytes = mem::size_of::<u64>() * BITSET_SIZE_IN_WORDS;
            let bytes_slice = slice::from_raw_parts_mut(ptr, num_bytes);

            let num_read = buf.read(bytes_slice)?;
            if num_read != num_bytes {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }

            result.cardinality.invalidate();

            Ok(result)
        }
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
        let mut bitset = BitsetContainer::new();
        for run in container.iter_runs() {
            let min = run.value;
            let max = run.length - 1;

            bitset.set_range(min..max);
        }

        // We know this is safe since we're just copying the cardinality from the other
        // container and that all elements of a run are set
        bitset.cardinality.set(container.cardinality());
        bitset
    }
}

impl PartialEq for BitsetContainer {
    fn eq(&self, other: &BitsetContainer) -> bool {
        self.bitset == other.bitset
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
        let mut result = BitsetContainer::new();
        bitset_ops::or(&self, &other, &mut result);
        
        result.cardinality.invalidate();
        
        Container::Bitset(result)
    }

    fn inplace_or(mut self, other: &Self) -> Container {
        // See the notes in inplace operations at the top of this module for details
        unsafe {
            let ptr = self.as_mut_ptr();
            let out_slice = slice::from_raw_parts_mut(ptr, BITSET_SIZE_IN_WORDS);
            bitset_ops::or(&self, &other, out_slice);
            
            self.cardinality.invalidate();
            
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
        if other.is_full() {
            Container::Run(other.clone())
        }
        else {
            let mut result = self.clone();
            for run in other.iter_runs() {
                result.set_range(run.into_range());
            }

            Container::Bitset(result)
        }
    }

    fn inplace_or(mut self, other: &RunContainer) -> Container {
        if other.is_full() {
            Container::Run(other.clone())
        }
        else {
            for rle in other.iter_runs() {
                self.set_range(rle.into_range());
            }

            Container::Bitset(self)
        }
    }
}

impl SetAnd<Self> for BitsetContainer {
    fn and(&self, other: &Self) -> Container {
        let mut result = BitsetContainer::new();
        bitset_ops::and(&self, &other, &mut result);

        Container::Bitset(result)
    }

    fn and_cardinality(&self, other: &Self) -> usize {
        bitset_ops::and_cardinality(&self, &other)
    }

    fn inplace_and(mut self, other: &Self) -> Container {
         // See the notes in inplace operations at the top of this module for details
        unsafe {
            let ptr = self.as_mut_ptr();
            let out_slice = slice::from_raw_parts_mut(ptr, BITSET_SIZE_IN_WORDS);
            bitset_ops::and(&self, &other, out_slice);
            
            self.cardinality.invalidate();
        }
        
        Container::Bitset(self)
    }
}

impl SetAnd<ArrayContainer> for BitsetContainer {
    fn and(&self, other: &ArrayContainer) -> Container {
        SetAnd::and(other, self)
    }

    fn and_cardinality(&self, other: &ArrayContainer) -> usize {
        let mut count = 0;
        for value in other.iter() {
            if self.contains(*value) {
                count += 1;
            }
        }

        count
    }

    fn inplace_and(self, other: &ArrayContainer) -> Container {
        SetAnd::and(other, &self)
    }
}

impl SetAnd<RunContainer> for BitsetContainer {
    fn and(&self, other: &RunContainer) -> Container {
        SetAnd::and(other, self)
    }

    fn and_cardinality(&self, other: &RunContainer) -> usize {
        if other.is_full() {
            self.cardinality()
        }
        else {
            let mut card = 0;
            for rle in other.iter_runs() {
                card += self.cardinality_range(rle.into_range());
            }

            card
        }
    }

    fn inplace_and(mut self, other: &RunContainer) -> Container {
        // Other is full therefore all elements in self are present in other
        if other.is_full() {
            return Container::Bitset(self);
        }

        let mut start = 0;
        for run in other.iter_runs() {
            let end = run.value;
            self.unset_range(start..end);

            start = end + run.length + 1;

        }

        self.unset_range(start..(!0));
        //self.into_efficient_container()
        Container::Bitset(self)
    }
}

impl SetAndNot<Self> for BitsetContainer {
    fn and_not(&self, other: &Self) -> Container {
        let mut result = BitsetContainer::new();
        bitset_ops::and_not(&self, &other, &mut result);

        result.into_efficient_container()
    }

    fn inplace_and_not(mut self, other: &Self) -> Container {
         // See the notes in inplace operations at the top of this module for details
        unsafe {
            let ptr = self.as_mut_ptr();
            let out_slice = slice::from_raw_parts_mut(ptr, BITSET_SIZE_IN_WORDS);
            bitset_ops::and_not(&self, &other, out_slice);
            
            self.cardinality.invalidate();
        }
        
        self.into_efficient_container()
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
            bitset.unset_range(run.into_range());
        }

        bitset.into_efficient_container()
    }

    fn inplace_and_not(mut self, other: &RunContainer) -> Container {
        for rle in other.iter_runs() {
            self.unset_range(rle.into_range());
        }

        self.into_efficient_container()
    }
}

impl SetXor<Self> for BitsetContainer {
    fn xor(&self, other: &Self) -> Container {
        let mut result = BitsetContainer::new();
        bitset_ops::xor(&self, &other, &mut result);

        result.into_efficient_container()
    }

    fn inplace_xor(mut self, other: &Self) -> Container {
         // See the notes in inplace operations at the top of this module for details
        unsafe {
            let ptr = self.as_mut_ptr();
            let out_slice = slice::from_raw_parts_mut(ptr, BITSET_SIZE_IN_WORDS);
            bitset_ops::xor(&self, &other, out_slice);
            
            self.cardinality.invalidate();
        }

       self.into_efficient_container()
    }
}

impl SetXor<ArrayContainer> for BitsetContainer {
    fn xor(&self, other: &ArrayContainer) -> Container {
        SetXor::xor(other, self)
    }

    fn inplace_xor(mut self, other: &ArrayContainer) -> Container {
        self.flip_list(&other);
        self.into_efficient_container()
    }
}

impl SetXor<RunContainer> for BitsetContainer {
    fn xor(&self, other: &RunContainer) -> Container {
        SetXor::xor(other, self)
    }

    fn inplace_xor(self, other: &RunContainer) -> Container {
        SetXor::xor(other, &self)
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

        true
    }
}

impl Subset<ArrayContainer> for BitsetContainer {
    fn subset_of(&self, other: &ArrayContainer) -> bool {
        if self.cardinality() > other.cardinality() {
            return false;
        }

        for value in self.iter() {
            if !other.contains(value) {
                return false;
            }
        }

        true
    }
}

impl Subset<RunContainer> for BitsetContainer {
    fn subset_of(&self, other: &RunContainer) -> bool {
        if self.cardinality() > other.cardinality() {
            return false;
        }

        for value in self.iter() {
            if !other.contains(value) {
                return false;
            }
        }

        true
    }
}

impl SetNot for BitsetContainer {
    fn not(&self, range: Range<u16>) -> Container {
        let mut bitset = self.clone();
        bitset.flip_range(range);
        bitset.into_efficient_container()
    }

    fn inplace_not(mut self, range: Range<u16>) -> Container {
        self.flip_range(range);
        self.into_efficient_container()
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
        if self.word == 0 {
            None
        }
        else {
            let t = self.word & (!self.word).wrapping_add(1);
            let r = self.word.trailing_zeros();

            self.word ^= t;

            // Advance to the next word if the current one is 0
            let mut new_base = self.base;
            if self.word == 0 {
                for word in self.words[(self.word_index + 1)..].iter() {
                    self.word_index += 1;
                    self.word = *word;
                    new_base += 64;

                    if *word != 0 {
                        break;
                    }
                }
            }

            // Guaranteed to not truncate due to how containers work
            let value = (r + self.base) as u16;

            self.base = new_base;

            Some(value)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::container::*;
    use crate::test::*;
    use super::BITSET_SIZE_IN_WORDS;

    impl TestUtils for BitsetContainer {
        fn create() -> Self {
            Self::new()
        }

        fn fill(&mut self, data: &[u16]) {
            self.set_list(data);
        }
    }

    #[test]
    fn set() {
        let mut a = BitsetContainer::new();
        a.set(9);
        a.set(80);
        a.set(100);
        a.set(3879);
        
        assert!(a.get(9));
        assert!(a.get(80));
        assert!(a.get(100));
        assert!(a.get(3879));
        assert_eq!(a.cardinality(), 4);
    }

    #[test]
    fn set_range() {
        let mut a = BitsetContainer::new();
        a.set_range(0..10);

        assert_eq!(a.cardinality(), 10);
        assert!(a.contains_range(0..10));
    }

    #[test]
    fn set_list() {
        let mut a = BitsetContainer::new();
        a.set_list(&INPUT_A);

        assert_eq!(a.cardinality(), INPUT_A.len());
        
        let iter = a.iter()
            .zip(INPUT_A.iter());

        for (found, expected) in iter {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn set_all() {
        let mut a = BitsetContainer::new();
        a.set_all();

        assert_eq!(a.cardinality(), BITSET_SIZE_IN_WORDS * 64);
    }

    #[test]
    fn unset() {
        let mut a = BitsetContainer::new();
        a.set_range(0..10);
        a.unset(5);
        a.unset(4);

        assert!(!a.contains_range(4..6));
        assert_eq!(a.cardinality(), 8);
    }

    #[test]
    fn unset_range() {
        let mut a = BitsetContainer::new();
        a.set_range(0..10);
        a.unset_range(4..6);

        assert!(!a.contains_range(4..6));
        assert_eq!(a.cardinality(), 8);
    }

    #[test]
    fn clear_list() {
        let mut a = BitsetContainer::new();
        a.set_list(&INPUT_A);
        a.clear_list(&INPUT_A[..6]);

        assert_eq!(a.cardinality(), INPUT_A.len() - 6);
        
        let iter = a.iter()
            .zip(INPUT_A[6..].iter());

        for (found, expected) in iter {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn add() {
        let mut a = BitsetContainer::new();
        a.add(9);
        a.add(80);
        a.add(100);
        a.add(3879);
        
        assert!(a.contains(9));
        assert!(a.contains(80));
        assert!(a.contains(100));
        assert!(a.contains(3879));
        assert_eq!(a.cardinality(), 4);
    }

    #[test]
    fn get() {
        let mut a = BitsetContainer::new();
        a.set_range(0..10);

        assert!(a.get(6));
        assert!(!a.get(11));
    }

    #[test]
    fn get_range() {
        let mut a = BitsetContainer::new();
        a.set_range(0..100);

        assert!(a.get_range(25..75));
        assert!(!a.get_range(100..150));
    }

    #[test]
    fn flip_range() {
        let mut a = BitsetContainer::new();
        a.set_range(0..100);
        a.flip_range(25..75);

        assert_eq!(a.cardinality(), 50);
    }

    #[test]
    fn flip_list() {
        let mut a = BitsetContainer::new();
        a.set_list(&INPUT_A);
        a.flip_list(&INPUT_A[..6]);

        assert_eq!(a.cardinality(), INPUT_A.len() - 6);
        
        let iter = a.iter()
            .zip(INPUT_A[6..].iter());

        for (found, expected) in iter {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn contains() {
        let mut a = BitsetContainer::new();
        a.set_range(0..10);

        assert!(a.contains(6));
        assert!(!a.contains(11));
    }

    #[test]
    fn contains_range() {
        let mut a = BitsetContainer::new();
        a.set_range(0..100);

        assert!(a.contains_range(25..75));
        assert!(!a.contains_range(100..150));
    }

    #[test]
    fn is_full() {
        let mut a = BitsetContainer::new();
        a.set_all();

        assert!(a.is_full());
        assert!(!a.is_empty());
        assert_eq!(a.cardinality(), BITSET_SIZE_IN_WORDS * 64);
    }

    #[test]
    fn cardinality() {
        let range = 50..100;
        let mut a = BitsetContainer::new();
        a.set_range(range.clone());

        assert_eq!(a.cardinality(), range.len());
    }

    #[test]
    fn cardinality_range() {
        let range = 50..100;
        let mut a = BitsetContainer::new();
        a.set_range(range.clone());

        assert_eq!(a.cardinality(), range.len());

        let iter = a.iter()
            .zip(range);

        for (found, expected) in iter {
            assert_eq!(found, expected);
        }
    }

    #[test]
    fn min() {
        let a = make_container::<BitsetContainer>(&INPUT_A);
        
        let min = a.min();
        assert!(min.is_some());
        assert_eq!(min.unwrap(), INPUT_A[0]);
    }

    #[test]
    fn max() {
        let a = make_container::<BitsetContainer>(&INPUT_A);
        
        let max = a.max();
        assert!(max.is_some());
        assert_eq!(max.unwrap(), INPUT_A[INPUT_A.len() - 1]);
    }

    #[test]
    fn rank() {
        let mut a = BitsetContainer::new();
        a.add_range(0..10);

        let rank = a.rank(5);
        assert_eq!(rank, 6);
    }

    #[test]
    fn select() {
        let range = 0..30;
        let mut a = BitsetContainer::new();
        a.add_range(range);

        let mut start_rank = 5;
        let selected = a.select(20, &mut start_rank);
        
        assert!(selected.is_some());
        assert_eq!(selected.unwrap(), 15);
    }

    #[test]
    fn num_runs() {
        let a = make_container::<BitsetContainer>(&RUNS);
        
        assert_eq!(a.num_runs(), NUM_RUNS);
    }

    #[test]
    fn iter() {
        let a = make_container::<BitsetContainer>(&INPUT_A);

        assert_eq!(a.cardinality(), INPUT_A.len());

        let iter = a.iter()
            .zip(INPUT_A.iter());

        for (found, expected) in iter {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn round_trip_serialize() {
        let mut a = BitsetContainer::new();
        a.set_list(&INPUT_A);

        // Setup
        let num_bytes = BitsetContainer::serialized_size();
        let mut buffer = Vec::<u8>::with_capacity(num_bytes);

        // Serialize the bitset and validate
        let num_written = a.serialize(&mut buffer);
        assert!(num_written.is_ok());
        assert_eq!(num_written.unwrap(), num_bytes);

        // Deserialize the bitset and validate
        let mut cursor = std::io::Cursor::new(buffer);
        let deserialized = BitsetContainer::deserialize(&mut cursor);
        assert!(deserialized.is_ok());

        let deserialized = deserialized.unwrap();
        let iter = deserialized.iter()
            .zip(INPUT_A.iter());

        for (found, expected) in iter {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn bitset_bitset_or() {
        run_test::<BitsetContainer, BitsetContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_OR,
            |a, b| a.or(&b)
        );
    }

    #[test]
    fn bitset_bitset_and() {
        run_test::<BitsetContainer, BitsetContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND,
            |a, b| a.and(&b)
        );
    }

    #[test]
    fn bitset_bitset_and_cardinality() {
        let a = make_container::<BitsetContainer>(&INPUT_A);
        let b = make_container::<BitsetContainer>(&INPUT_B);

        let card = a.and_cardinality(&b);

        assert_eq!(card, RESULT_AND.len());
    }

    #[test]
    fn bitset_bitset_and_not() {
        run_test::<BitsetContainer, BitsetContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND_NOT,
            |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn bitset_bitset_xor() {
        run_test::<BitsetContainer, BitsetContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_XOR,
            |a, b| a.xor(&b)
        );
    }

    #[test]
    fn bitset_bitset_subset_of() {
        let a = make_container::<BitsetContainer>(&SUBSET_A);
        let b = make_container::<BitsetContainer>(&SUBSET_B);

        assert!(a.subset_of(&b));
        assert!(!b.subset_of(&a));
    }

    #[test]
    fn bitset_not() {
        let a = make_container::<BitsetContainer>(&INPUT_A);
        let not_a = a.not(0..(((BITSET_SIZE_IN_WORDS * 64) - 1) as u16));

        for value in a.iter() {
            assert!(!not_a.contains(value), "{} found in set", value);
        }
    }

    #[test]
    fn bitset_bitset_inplace_or() {
        run_test::<BitsetContainer, BitsetContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_OR,
            |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn bitset_bitset_inplace_and() {
        run_test::<BitsetContainer, BitsetContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND,
            |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn bitset_bitset_inplace_and_not() {
        run_test::<BitsetContainer, BitsetContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND_NOT,
            |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn bitset_bitset_inplace_xor() {
        run_test::<BitsetContainer, BitsetContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_XOR,
            |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn bitset_array_or() {
        run_test::<BitsetContainer, ArrayContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_OR,
            |a, b| a.or(&b)
        );
    }

    #[test]
    fn bitset_array_and() {
        run_test::<BitsetContainer, ArrayContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND,
            |a, b| a.and(&b)
        );
    }

    #[test]
    fn bitset_array_and_cardinality() {
        let a = make_container::<BitsetContainer>(&INPUT_A);
        let b = make_container::<ArrayContainer>(&INPUT_B);

        let card = a.and_cardinality(&b);

        assert_eq!(card, RESULT_AND.len());
    }

    #[test]
    fn bitset_array_and_not() {
        run_test::<BitsetContainer, ArrayContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND_NOT,
            |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn bitset_array_xor() {
        run_test::<BitsetContainer, ArrayContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_XOR,
            |a, b| a.xor(&b)
        );
    }

    #[test]
    fn bitset_array_inplace_or() {
        run_test::<BitsetContainer, ArrayContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_OR,
            |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn bitset_array_inplace_and() {
        run_test::<BitsetContainer, ArrayContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND,
            |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn bitset_array_inplace_and_not() {
        run_test::<BitsetContainer, ArrayContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND_NOT,
            |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn bitset_array_inplace_xor() {
        run_test::<BitsetContainer, ArrayContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_XOR,
            |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn bitset_array_subset_of() {
        let a = make_container::<BitsetContainer>(&SUBSET_A);
        let b = make_container::<ArrayContainer>(&SUBSET_B);

        assert!(a.subset_of(&b));
        assert!(!b.subset_of(&a));
    }

    #[test]
    fn bitset_run_or() {
        run_test::<BitsetContainer, RunContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_OR,
            |a, b| a.or(&b)
        );
    }

    #[test]
    fn bitset_run_and() {
        run_test::<BitsetContainer, RunContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND,
            |a, b| a.and(&b)
        );
    }

    #[test]
    fn bitset_run_and_cardinality() {
        let a = make_container::<BitsetContainer>(&INPUT_A);
        let b = make_container::<RunContainer>(&INPUT_B);

        let card = a.and_cardinality(&b);

        assert_eq!(card, RESULT_AND.len());
    }

    #[test]
    fn bitset_run_and_not() {
        run_test::<BitsetContainer, RunContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND_NOT,
            |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn bitset_run_xor() {
        run_test::<BitsetContainer, RunContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_XOR,
            |a, b| a.xor(&b)
        );
    }

    #[test]
    fn bitset_run_inplace_or() {
        run_test::<BitsetContainer, RunContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_OR,
            |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn bitset_run_inplace_and() {
        run_test::<BitsetContainer, RunContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND,
            |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn bitset_run_inplace_and_not() {
        run_test::<BitsetContainer, RunContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_AND_NOT,
            |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn bitset_run_inplace_xor() {
        run_test::<BitsetContainer, RunContainer, _>(
            &INPUT_A,
            &INPUT_B,
            &RESULT_XOR,
            |a, b| a.inplace_xor(&b)
        );
    }
    
    #[test]
    fn bitset_run_subset_of() {
        let a = make_container::<BitsetContainer>(&SUBSET_A);
        let b = make_container::<RunContainer>(&SUBSET_B);

        assert!(a.subset_of(&b));
        assert!(!b.subset_of(&a));
    }
}