use std::ops::{Deref, DerefMut};
use std::ptr;

use crate::IntoBound;
use crate::container::*;

/// The size of the bitset in 64bit words
pub const BITSET_SIZE_IN_WORDS: usize = 1024;

/// A bitset container used in a roaring bitmap. 
/// 
/// # Structure
/// Contents are aligned to the 32byte boundary and stored as 64bit words
#[derive(Clone, Debug)]
pub struct BitsetContainer {
    bitset: Vec<u64>,
}

impl BitsetContainer {
    // Create a new bitset
    pub fn new() -> Self {
        Self {
            bitset: vec![0; BITSET_SIZE_IN_WORDS],
        }
    }

    /// Set the bit at `index`
    pub fn set(&mut self, index: u16) -> bool {
        let word_index = usize::from(index / 64);
        let bit_index = index % 64;
        let word = self.bitset[word_index];
        let new_word = word | (1 << bit_index);
        let change = ((word ^ new_word) >> 1) as usize;

        self.bitset[word_index] = new_word;

        change > 0
    }

    /// Set all the bits within the range denoted by [min-max)
    pub fn set_range(&mut self, range: Range<u32>) {
        let (min, max) = (range.start, range.end);

        if min == max {
            return;
        }

        let first_word = (min / 64) as usize;
        let last_word = ((max - 1) / 64) as usize;

        if first_word == last_word {
            let w0 = std::u64::MAX << (min % 64);
            let w1 = std::u64::MAX >> ((!max + 1) % 64);
            self.bitset[first_word] |= w0 & w1;
        }
        else {
            self.bitset[first_word] |= std::u64::MAX << (min % 64);
            
            for word in self.bitset[(first_word + 1)..last_word].iter_mut() {
                *word = std::u64::MAX;
            }

            self.bitset[last_word] |= std::u64::MAX >> ((!max + 1) % 64);
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
    }

    /// Set all the bits in the bitset
    #[allow(dead_code)]
    pub fn set_all(&mut self) {
        for word in &mut *self.bitset {
            *word = std::u64::MAX;
        }
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

        change > 0
    }

    /// Unset all the bits between [min-max)
    pub fn unset_range(&mut self, range: Range<u32>) {
        let (min, max) = range.into_bound();

        if min == max {
            return;
        }

        let first_word = (min / 64) as usize;
        let last_word = ((max - 1) / 64) as usize;

        if first_word == last_word {
            let w0 = !0_u64 << (min % 64);
            let w1 = !0_u64 >> ((!max + 1) % 64);
            self.bitset[first_word] &= !(w0 & w1);

            return;
        }

        self.bitset[first_word] &= !(!0_u64 << (min % 64));
        
        for i in (first_word + 1)..last_word {
            self.bitset[i] = 0;
        }

        self.bitset[last_word] &= !(!0_u64 >> ((!max + 1) % 64));
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
        }
    }

    /// Add `value` to the set and return true if it was set
    #[inline]
    pub fn add(&mut self, value: u16) -> bool {
        self.set(value)
    }

    /// Add all values in [min-max) to the bitset
    #[inline]
    pub fn add_range(&mut self, range: Range<u32>) {
        self.set_range(range)
    }

    /// Remove `value` from the set and return true if it was removed
    #[inline]
    pub fn remove(&mut self, value: u16) -> bool {
        self.unset(value)
    }

    /// Get the value of the bit at `index`
    pub fn get(&self, index: u16) -> bool {
        (self.bitset[usize::from(index >> 6)] >> (index & 0x3F)) & 1 != 0
    }

    /// Check if all bits within a range are true
    pub fn get_range(&self, range: Range<u32>) -> bool {
        let (min, max) = (range.start, range.end);

        if min == max {
            return self.get(min as u16);
        }

        let first_word = (min >> 6) as usize;
        let last_word = (max >> 6) as usize;
        let w0 = !((1 << (min & 0x3F)) - 1);
        let w1 = (1 << (max & 0x3F)) - 1;

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
    }

    /// Flip all bits in the range [min-max)
    pub fn flip_range(&mut self, range: Range<u32>) {
        if range.start == range.end {
            return self.flip(range.start as u16);
        }

        let first_word = (range.start / 64) as usize;
        let last_word = ((range.end - 1) / 64) as usize;
        
        self.bitset[first_word] ^= !((!0) << (range.start % 64));
        
        for i in first_word..last_word {
            self.bitset[i] = !self.bitset[i];
        }

        self.bitset[last_word] ^= (!0) >> ((!range.end).saturating_add(1) % 64);
    }

    /// Flip all bits contained in `list`
    pub fn flip_list(&mut self, list: &[u16]) {
        for value in list.iter() {
            let word_index = (*value >> 6) as usize;
            let index = *value % 64;
            let load = self.bitset[word_index];
            let store = load ^ (1_u64 << index);
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
    pub fn contains_range(&self, range: Range<u32>) -> bool {
        self.get_range(range)
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.cardinality() == BITSET_SIZE_IN_WORDS * 64
    }

    /// The cardinality of the bitset
    #[inline]
    pub fn cardinality(&self) -> usize {
        let mut count = 0;
        for word in self.bitset.iter() {
            count += word.count_ones();
        }

        count as usize
    }

    /// Get the cardinality of the range [min-max)
    pub fn cardinality_range(&self, range: Range<u32>) -> usize {
        let min = range.start;
        let max = range.end;

        if min == max {
            return self.get(min as u16) as usize;
        }

        let start = min as usize;
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

    /// Get an iterator over the elements of the bitset
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

    /// Get an iterator over the words of the bitset
    #[inline]
    pub fn iter_words(&self) -> impl Iterator<Item=&u64> {
        self.bitset.iter()
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
            let num_bytes = BITSET_SIZE_IN_WORDS * mem::size_of::<u64>();
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
            let num_bytes = BITSET_SIZE_IN_WORDS * mem::size_of::<u64>();
            let bytes_slice = slice::from_raw_parts_mut(ptr, num_bytes);

            let num_read = buf.read(bytes_slice)?;
            if num_read != num_bytes {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }

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
            let min = u32::from(run.value);
            let max = u32::from(run.end() + 1);

            bitset.set_range(min..max);
        }

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

        unsafe {
            let out = result.as_mut_ptr();
            
            let pass = self.iter_words()
                .zip(other.iter_words())
                .enumerate();

            for (i, (wa, wb)) in pass {
                ptr::write(out.add(i), wa | wb);
            }
        }
        
        Container::Bitset(result)
    }

    fn inplace_or(mut self, other: &Self) -> Container {
        // See the notes in inplace operations at the top of this module for details
        unsafe {
            let out = self.as_mut_ptr();
            let pass = self.iter_words()
                .zip(other.iter_words())
                .enumerate();

            for (i, (wa, wb)) in pass {
                ptr::write(out.add(i), wa | wb);
            }
            
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
        unsafe {
            let out = result.as_mut_ptr();
            
            let pass = self.iter_words()
                .zip(other.iter_words())
                .enumerate();

            for (i, (wa, wb)) in pass {
                ptr::write(out.add(i), wa & wb);
            }
        }

        Container::Bitset(result)
    }

    fn and_cardinality(&self, other: &Self) -> usize {
        let mut count = 0;
        let pass = self.iter_words()
            .zip(other.iter_words());

        for (a, b) in pass {
            count += (a & b).count_ones();
        }

        count as usize
    }

    fn inplace_and(mut self, other: &Self) -> Container {
         // See the notes in inplace operations at the top of this module for details
        unsafe {
            let out = self.as_mut_ptr();
            let pass = self.iter_words()
                .zip(other.iter_words())
                .enumerate();

            for (i, (wa, wb)) in pass {
                ptr::write(out.add(i), wa & wb);
            }
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
            let end = u32::from(run.value);
            self.unset_range(start..end);

            start = end + u32::from(run.length) + 1;

        }

        self.unset_range(start..(1 << 16));
        self.into_efficient_container()
    }
}

impl SetAndNot<Self> for BitsetContainer {
    fn and_not(&self, other: &Self) -> Container {
        let mut result = BitsetContainer::new();

        unsafe {
            let out = result.as_mut_ptr();
            
            let pass = self.iter_words()
                .zip(other.iter_words())
                .enumerate();

            for (i, (wa, wb)) in pass {
                ptr::write(out.add(i), wa & !wb);
            }
        }

        result.into_efficient_container()
    }

    fn inplace_and_not(mut self, other: &Self) -> Container {
         // See the notes in inplace operations at the top of this module for details
        unsafe {
            let out = self.as_mut_ptr();
            let pass = self.iter_words()
                .zip(other.iter_words())
                .enumerate();

            for (i, (wa, wb)) in pass {
                ptr::write(out.add(i), wa & !wb);
            }
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
        
        unsafe {
            let out = result.as_mut_ptr();
            
            let pass = self.iter_words()
                .zip(other.iter_words())
                .enumerate();

            for (i, (wa, wb)) in pass {
                ptr::write(out.add(i), wa ^ wb);
            }
        }

        result.into_efficient_container()
    }

    fn inplace_xor(mut self, other: &Self) -> Container {
         // See the notes in inplace operations at the top of this module for details
        unsafe {
            let out = self.as_mut_ptr();
            let pass = self.iter_words()
                .zip(other.iter_words())
                .enumerate();

            for (i, (wa, wb)) in pass {
                ptr::write(out.add(i), wa ^ wb);
            }
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
    fn not(&self, range: Range<u32>) -> Container {
        let mut bitset = self.clone();
        bitset.flip_range(range);
        bitset.into_efficient_container()
    }

    fn inplace_not(mut self, range: Range<u32>) -> Container {
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

    impl TestShim<u16> for BitsetContainer {
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
        a.set_range(0..(1 << 16));

        assert_eq!(a.cardinality(), (1 << 16));
        assert!(a.contains_range(0..10));
    }

    #[test]
    fn set_list() {
        let data = generate_data(0..65535, 6_000);
        let mut a = BitsetContainer::new();
        a.set_list(&data);

        assert_eq!(a.cardinality(), data.len());
        
        let iter = a.iter()
            .zip(data.iter());

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
        let data = generate_data(0..65535, 6_000);
        let mut a = BitsetContainer::new();
        a.set_list(&data);
        a.clear_list(&data[..6]);

        assert_eq!(a.cardinality(), data.len() - 6);
        
        let iter = a.iter()
            .zip(data[6..].iter());

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
        let data = generate_data(0..65535, 6_000);
        let mut a = BitsetContainer::new();
        a.set_list(&data);
        a.flip_list(&data[..6]);

        assert_eq!(a.cardinality(), data.len() - 6);
        
        let iter = a.iter()
            .zip(data[6..].iter());

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
            .zip(50..100);

        for (found, expected) in iter {
            assert_eq!(found, expected);
        }
    }

    #[test]
    fn min() {
        let data = generate_data(0..65535, 6_000);
        let a = BitsetContainer::from_data(&data);
        
        let min = a.min();
        assert!(min.is_some());
        assert_eq!(min.unwrap(), data[0]);
    }

    #[test]
    fn max() {
        let data = generate_data(0..65535, 6_000);
        let a = BitsetContainer::from_data(&data);
        
        let max = a.max();
        assert!(max.is_some());
        assert_eq!(max.unwrap(), data[data.len() - 1]);
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
    fn iter() {
        let data = generate_data(0..65535, 6_000);
        let a = BitsetContainer::from_data(&data);

        assert_eq!(a.cardinality(), data.len());

        let iter = a.iter()
            .zip(data.iter());

        for (found, expected) in iter {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn from_array() {
        let data = generate_data(0..65535, 3_000);
        let a = ArrayContainer::from_data(&data);
        let b = BitsetContainer::from(a.clone());

        for (before, after) in a.iter().zip(b.iter()) {
            assert_eq!(*before, after);
        }
    }

    #[test]
    fn from_run() {
        let data = generate_data(0..65535, 12_000);
        let a = RunContainer::from_data(&data);
        let b = BitsetContainer::from(a.clone());

        for (before, after) in a.iter().zip(b.iter()) {
            assert_eq!(before, after);
        }
    }

    #[test]
    fn round_trip_serialize() {
        let data = generate_data(0..65535, 6_000);
        let mut a = BitsetContainer::new();
        a.set_list(&data);

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
            .zip(a.iter());

        for (found, expected) in iter {
            assert_eq!(found, expected);
        }
    }

    #[test]
    fn bitset_bitset_or() {
        op_test::<BitsetContainer, BitsetContainer, u16, _, Container>(
            OpType::Or, |a, b| a.or(&b)
        );
    }

    #[test]
    fn bitset_bitset_and() {
        op_test::<BitsetContainer, BitsetContainer, u16, _, Container>(
            OpType::And, |a, b| a.and(&b)
        );
    }

    #[test]
    fn bitset_bitset_and_cardinality() {
        op_card_test::<BitsetContainer, BitsetContainer, u16, _>(
            OpType::And, |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn bitset_bitset_and_not() {
        op_test::<BitsetContainer, BitsetContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn bitset_bitset_xor() {
        op_test::<BitsetContainer, BitsetContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.xor(&b)
        );
    }

    #[test]
    fn bitset_bitset_subset_of() {
        op_subset_test::<BitsetContainer, BitsetContainer, u16>();
    }

    #[test]
    fn bitset_not() {
        let data = generate_data(0..65535, 6_000);
        let a = BitsetContainer::from_data(&data);
        let not_a = a.not(0..(((BITSET_SIZE_IN_WORDS * 64) - 1) as u32));

        for value in a.iter() {
            assert!(!not_a.contains(value), "{} found in set", value);
        }
    }

    #[test]
    fn bitset_bitset_inplace_or() {
        op_test::<BitsetContainer, BitsetContainer, u16, _, Container>(
            OpType::Or, |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn bitset_bitset_inplace_and() {
        op_test::<BitsetContainer, BitsetContainer, u16, _, Container>(
            OpType::And, |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn bitset_bitset_inplace_and_not() {
        op_test::<BitsetContainer, BitsetContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn bitset_bitset_inplace_xor() {
        op_test::<BitsetContainer, BitsetContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn bitset_array_or() {
        op_test::<BitsetContainer, ArrayContainer, u16, _, Container>(
            OpType::Or, |a, b| a.or(&b)
        );
    }

    #[test]
    fn bitset_array_and() {
        op_test::<BitsetContainer, ArrayContainer, u16, _, Container>(
            OpType::And, |a, b| a.and(&b)
        );
    }

    #[test]
    fn bitset_array_and_cardinality() {
        op_card_test::<BitsetContainer, ArrayContainer, u16, _>(
            OpType::And, |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn bitset_array_and_not() {
        op_test::<BitsetContainer, ArrayContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn bitset_array_xor() {
        op_test::<BitsetContainer, ArrayContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.xor(&b)
        );
    }

    #[test]
    fn bitset_array_inplace_or() {
        op_test::<BitsetContainer, ArrayContainer, u16, _, Container>(
            OpType::Or, |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn bitset_array_inplace_and() {
        op_test::<BitsetContainer, ArrayContainer, u16, _, Container>(
            OpType::And, |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn bitset_array_inplace_and_not() {
        op_test::<BitsetContainer, ArrayContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn bitset_array_inplace_xor() {
        op_test::<BitsetContainer, ArrayContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn bitset_array_subset_of() {
        op_subset_test::<BitsetContainer, ArrayContainer, u16>();
    }

    #[test]
    fn bitset_run_or() {
        op_test::<BitsetContainer, RunContainer, u16, _, Container>(
            OpType::Or, |a, b| a.or(&b)
        );
    }

    #[test]
    fn bitset_run_and() {
        op_test::<BitsetContainer, RunContainer, u16, _, Container>(
            OpType::And, |a, b| a.and(&b)
        );
    }

    #[test]
    fn bitset_run_and_cardinality() {
        op_card_test::<BitsetContainer, RunContainer, u16, _>(
            OpType::And, |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn bitset_run_and_not() {
        op_test::<BitsetContainer, RunContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn bitset_run_xor() {
        op_test::<BitsetContainer, RunContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.xor(&b)
        );
    }

    #[test]
    fn bitset_run_inplace_or() {
        op_test::<BitsetContainer, RunContainer, u16, _, Container>(
            OpType::Or, |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn bitset_run_inplace_and() {
        op_test::<BitsetContainer, RunContainer, u16, _, Container>(
            OpType::And, |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn bitset_run_inplace_and_not() {
        op_test::<BitsetContainer, RunContainer, u16, _, Container>(
            OpType::AndNot, |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn bitset_run_inplace_xor() {
        op_test::<BitsetContainer, RunContainer, u16, _, Container>(
            OpType::Xor, |a, b| a.inplace_xor(&b)
        );
    }
    
    #[test]
    fn bitset_run_subset_of() {
        op_subset_test::<BitsetContainer, RunContainer, u16>();
    }
}