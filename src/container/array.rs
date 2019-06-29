use std::io::{self, Read, Write};
use std::mem;
use std::ptr;
use std::ops::{Deref, DerefMut, Range};
use std::slice::{self, Iter};

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
            array: Vec::new()
        }
    }

    /// Create a new array container with a specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            array: Vec::with_capacity(capacity)
        }
    }

    /// The cardinality of the array container
    #[inline]
    pub fn cardinality(&self) -> usize {
        // Len is the same as the cardinality for raw sets of integers
        self.array.len()
    }

    /// Get the number of values in the array
    #[inline]
    pub fn len(&self) -> usize {
        self.array.len()
    }

    /// Clear the contents of the array container
    #[inline]
    pub fn clear(&mut self) {
        self.array.clear()
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
        let can_append = {
            let is_max_value = match self.max() {
                Some(max) => max < value,
                None => true
            };

            is_max_value && self.cardinality() < (std::u16::MAX as usize)
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
                if self.cardinality() < (std::u16::MAX as usize) {
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

    /// Find the element of a given rank starting at `start_rank`. Returns None if no element is present and updates `start_rank`
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
    #[inline]
    pub fn iter(&self) -> Iter<u16> {
        self.array.iter()
    }

    /// Get a pointer to the array
    #[inline]
    pub fn as_ptr(&self) -> *const u16 {
        self.array.as_ptr()
    }

    /// Get a mutable pointer to the array
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u16 {
        self.array.as_mut_ptr()
    }
}

impl ArrayContainer {
    /// Get the size in bytes of a container with `cardinality`
    pub fn serialized_size(cardinality: usize) -> usize {
        cardinality * mem::size_of::<u16>() + 2
    }

    /// Serialize the array into `buf` according to the roaring format spec
    #[cfg(target_endian = "little")]
    pub fn serialize<W: Write>(&self, buf: &mut W) -> io::Result<usize> {
        unsafe {
            let ptr = self.array.as_ptr() as *const u8;
            let num_bytes = mem::size_of::<u16>() * self.len();
            let byte_slice = slice::from_raw_parts(ptr, num_bytes);

            buf.write(byte_slice)
        }
    }

    /// Deserialize an array container according to the roaring format spec
    #[cfg(target_endian = "little")]
    pub fn deserialize<R: Read>(cardinality: usize, buf: &mut R) -> io::Result<Self> {
        unsafe {
            let mut result = ArrayContainer::with_capacity(cardinality);
            let ptr = result.as_mut_ptr() as *mut u8;
            let num_bytes = mem::size_of::<u16>() * cardinality;
            let bytes_slice = slice::from_raw_parts_mut(ptr, num_bytes);

            buf.read(bytes_slice)?;

            Ok(result)
        }
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
        self == other
    }
}

impl Eq for ArrayContainer { }

impl From<BitsetContainer> for ArrayContainer {
    #[inline]
    fn from(container: BitsetContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut BitsetContainer> for ArrayContainer {
    #[inline]
    fn from(container: &'a mut BitsetContainer) -> Self {
        From::from(&*container)
    }
}

impl<'a> From<&'a BitsetContainer> for ArrayContainer {
    fn from(container: &'a BitsetContainer) -> Self {
        let len = container.cardinality();
        let mut array = ArrayContainer::with_capacity(len);
        
        for value in container.iter() {
            array.push(value);
        }

        array
    }
}

impl From<RunContainer> for ArrayContainer {
    #[inline]
    fn from(container: RunContainer) -> Self {
        From::from(&container)
    }
}

impl<'a> From<&'a mut RunContainer> for ArrayContainer {
    #[inline]
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
        let len = self.len() + other.len();
        let mut result = ArrayContainer::with_capacity(len);
        let ptr = result.array.as_mut_ptr();

        unsafe {
            let len = array_ops::or(
                self.array.as_slice(), 
                other.array.as_slice(),
                ptr
            );

            result.array.set_len(len);
        }

        Container::Array(result)
    }
    
    fn inplace_or(mut self, other: &Self) -> Container {
        let max_cardinality = self.len() + other.len();
        
        // Contents will end up as an array container, work inplace
        if max_cardinality <= DEFAULT_MAX_SIZE {
            // Make sure the contents will fit
            let required = max_cardinality - self.capacity();
            if required > 0 {
                self.reserve(required);
            }

            unsafe {
                // Offset the contents of self so we can put the result in the beginning of the array
                let start = self.as_mut_ptr();
                let end = start.add(other.len());

                ptr::copy_nonoverlapping(start, end, self.len());

                // Run the optimized union code on the contents
                let s0 = slice::from_raw_parts(end, self.len());
                let s1 = other.array.as_slice();

                array_ops::or(s0, s1, start);
            }

            Container::Array(self)
        }
        // Contents will probably end up as a bitset
        else {
            let mut bitset = BitsetContainer::new();
            bitset.set_list(&self);
            bitset.set_list(&other);

            // Result is going to be an array, convert back
            let len = bitset.len();
            if len <= DEFAULT_MAX_SIZE {
                let required = len - self.capacity();
                if required > 0 {
                    self.reserve(required);
                }

                // Load the contenst of the bitset into the array
                self.clear();

                let mut iter = bitset.iter();
                let mut value = iter.next();
                while value.is_some() {
                    self.push(value.unwrap());

                    value = iter.next();
                }

                Container::Array(self)
            }
            // Result remains a bitset
            else {
                Container::Bitset(bitset)
            }
        }
    }
}

impl SetOr<BitsetContainer> for ArrayContainer {
    fn or(&self, other: &BitsetContainer) -> Container {
        // Container can't possibly be an array, realloca as a bitset
        let mut result = other.clone();
        result.set_list(&self);
        
        Container::Bitset(result)
    }
    
    fn inplace_or(self, other: &BitsetContainer) -> Container {
        SetOr::or(&self, other)
    }
}

impl SetOr<RunContainer> for ArrayContainer {
    fn or(&self, other: &RunContainer) -> Container {
        SetOr::or(other, self)
    }
    
    fn inplace_or(self, other: &RunContainer) -> Container {
        SetOr::or(&self, other)
    }
}

impl SetAnd<Self> for ArrayContainer {
    fn and(&self, other: &Self) -> Container {
        let len = self.len().max(other.len());
        let mut result = ArrayContainer::with_capacity(len);
        let ptr = result.array.as_mut_ptr();

        unsafe {
            let len = array_ops::and(
                self.array.as_slice(), 
                other.array.as_slice(),
                ptr
            );

            result.array.set_len(len);
        }

        Container::Array(result)
    }
    
    fn and_cardinality(&self, other: &Self) -> usize {
        array_ops::and_cardinality(&self.array, &other.array)
    }
    
    fn inplace_and(mut self, other: &Self) -> Container {
        unsafe {
            // Shift the elements of self over to accomodate new contents
            let len = self.len();
            let req = len.max(other.len());
            let slack = req + len;
            if self.capacity() < slack {
                self.reserve(slack - self.capacity());

                let src = self.as_mut_ptr();
                let dst = src.add(req);

                ptr::copy(src, dst, len);
            }
            
            let ptr = self.as_ptr();
            let slice = slice::from_raw_parts(ptr, self.len());
            
            let card = array_ops::and(slice, &other, self.as_mut_ptr());
            self.set_cardinality(card);
        }
        
        Container::Array(self)
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
        let mut card = 0;
        for value in self.array.iter() {
            if other.contains(*value) {
                card += 1;
            }
        }
        
        card
    }
    
    fn inplace_and(self, other: &BitsetContainer) -> Container {
        SetAnd::and(&self, other)
    }
}

impl SetAnd<RunContainer> for ArrayContainer {
    fn and(&self, other: &RunContainer) -> Container {
        SetAnd::and(other, self)
    }

    fn and_cardinality(&self, other: &RunContainer) -> usize {
        if other.is_full() {
            return self.len();
        }
        
        if other.num_runs() == 0 {
            return 0;
        }
        
        unsafe {
            let ptr_a = self.as_ptr();
            let ptr_r = other.as_ptr();

            let mut i_a = 0;
            let mut i_r = 0;
            let mut card = 0;

            while i_a < self.len() {
                let value = *(ptr_a.add(i_a));
                let (mut start, mut end) = (*(ptr_r.add(i_r))).range();

                while end < value {
                    i_r += 1;
                    if i_r == other.num_runs() {
                        return card;
                    }

                    let se = (*(ptr_r.add(i_r))).range();
                    start = se.0;
                    end = se.1;
                }

                if start > value {
                    i_a = array_ops::advance_until(&self, i_a, start);
                }
                else {
                    card += 1;
                    i_a += 1;
                }
            }
        
            card
        }
    }
    
    fn inplace_and(self, other: &RunContainer) -> Container {
        SetAnd::and(&self, other)
    }
}

impl SetAndNot<Self> for ArrayContainer {
    fn and_not(&self, other: &Self) -> Container {
        let len = self.len().max(other.len());
        let mut result = ArrayContainer::with_capacity(len);
        let ptr = result.array.as_mut_ptr();

        unsafe {
            let len = array_ops::and_not(
                self.array.as_slice(), 
                other.array.as_slice(),
                ptr
            );

            result.array.set_len(len);
        }

        Container::Array(result)
    }
    
    fn inplace_and_not(mut self, other: &Self) -> Container {
        unsafe {
            // Shift the elements of self over to accomodate new contents
            let len = self.len();
            let slack = len * 2;
            if self.capacity() < slack {
                self.reserve(slack - self.capacity());

                let src = self.as_mut_ptr();
                let dst = src.add(len);

                ptr::copy(src, dst, len);
            }
            
            let ptr = self.as_ptr();
            let slice = slice::from_raw_parts(ptr, self.len());

            let card = array_ops::and_not(slice, &other, self.as_mut_ptr());
            self.set_cardinality(card);
        }
        
        Container::Array(self)
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
        SetAndNot::and_not(&self, other)
    }
}

impl SetAndNot<RunContainer> for ArrayContainer {
    fn and_not(&self, other: &RunContainer) -> Container {
        let mut result = ArrayContainer::with_capacity(self.cardinality());

        if other.num_runs() == 0 {
            return Container::Array(self.clone());
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
                        run_end = rle.end() as usize;
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
        SetAndNot::and_not(&self, other)
    }
}

impl SetXor<Self> for ArrayContainer {
    fn xor(&self, other: &Self) -> Container {
        let len = self.len() + other.len();
        let mut result = ArrayContainer::with_capacity(len);
        let ptr = result.as_mut_ptr();

        unsafe {
            let len = array_ops::xor(&self, &other, ptr);

            result.set_cardinality(len);
        }

        Container::Array(result)
    }
    
    fn inplace_xor(mut self, other: &Self) -> Container {
        unsafe {
            // Shift the elements of self over to accomodate new contents
            let len = self.len();
            let req = len + other.len();
            let slack = req + len;
            if self.capacity() < slack {
                self.reserve(slack - self.capacity());

                let src = self.as_mut_ptr();
                let dst = src.add(req);

                ptr::copy(src, dst, len);
            }
            
            let ptr = self.as_ptr();
            let slice = slice::from_raw_parts(ptr, self.len());

            let card = array_ops::xor(slice, &other, self.as_mut_ptr());
            self.set_cardinality(card);
        }
        
        Container::Array(self)
    }
}

impl SetXor<BitsetContainer> for ArrayContainer {
    fn xor(&self, other: &BitsetContainer) -> Container {
        let mut result = other.clone();
        result.flip_list(&self.array);

        // Array is a better representation for this set, convert
        if result.cardinality() <= DEFAULT_MAX_SIZE {
            Container::Array(result.into())
        }
        // Bitset is a better representation
        else {
            Container::Bitset(result)
        }
    }
    
    fn inplace_xor(self, other: &BitsetContainer) -> Container {
        SetXor::xor(&self, other)
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
            let array = ArrayContainer::from(other);
            return SetXor::xor(&array, self);
        }
        // Process as a bitset since the final result may be a bitset
        else {
            return SetXor::xor(other.into(), self);
        }
    }
    
    fn inplace_xor(self, other: &RunContainer) -> Container {
        SetXor::xor(&self, other)
    }
}

impl Subset<Self> for ArrayContainer {
    fn subset_of(&self, other: &Self) -> bool {
        let card0 = self.cardinality();
        let card1 = other.cardinality();

        if card0 > card1 {
            return false;
        }

        let mut i0 = 0;
        let mut i1 = 0;
        while i0 < card0 && i1 < card1 {
            if self.array[i0] == other.array[i1] {
                i0 += 1;
                i1 += 1;
            }
            else if self.array[i0] > other.array[i1] {
                i1 += 1;
            }
            else {
                return false;
            }
        }

        if i0 == card0 {
            return true;
        }
        else {
            return false;
        }
    }
}

impl Subset<BitsetContainer> for ArrayContainer {
    fn subset_of(&self, other: &BitsetContainer) -> bool {
        if self.len() > other.len() {
            return false;
        }
        
        for value in self.array.iter() {
            if !other.contains(*value) {
                return false;
            }
        }
        
        true
    }
}

impl Subset<RunContainer> for ArrayContainer {
    fn subset_of(&self, other: &RunContainer) -> bool {
        if self.len() > other.len() {
            return false;
        }
        
        unsafe {
            let ptr_a = self.as_ptr();
            let ptr_r = other.as_ptr();
            
            let mut i_a = 0;
            let mut i_r = 0;
            
            while i_a < self.len() && i_r < other.len() {
                let (start, end) = (*(ptr_r.add(i_r))).range();
                let value = *(ptr_a.add(i_a));
                
                if value < start {
                    return false;
                }
                else if value > end {
                    i_r += 1;
                }
                else {
                    i_a += 1;
                }
            }
            
            if i_a == self.len() {
                true
            }
            else {
                false
            }
        }
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
        SetNot::not(&self, range)
    }
}

#[cfg(test)]
mod test {
    use crate::container::*;
    use crate::test::*;
    use crate::test::short::*;
    use super::*;

    impl TestUtils for ArrayContainer {
        fn create() -> Self {
            Self::new()
        }

        fn fill(&mut self, data: &[u16]) {
            for value in data.iter() {
                self.add(*value);
            }
        }
    }

    // Common bookkeeping
    #[test]
    fn load() {
        // Load the container
        let mut c = ArrayContainer::with_capacity(INPUT_A.len());
        for value in INPUT_A.iter() {
            c.add(*value);
        }

        assert_eq!(c.cardinality(), INPUT_A.len());
        
        // Check that the contents match
        let pass = c.iter()
            .zip(INPUT_A.iter());

        let mut failed = false;
        for (found, expected) in pass {
            if *found != *expected {
                failed = true;
                break;
            }
        }

        assert!(!failed);
    }

    // Ops
    #[test]
    fn array_array_or() {
        run_test::<ArrayContainer, ArrayContainer, _>(
            &INPUT_A, 
            &INPUT_B, 
            &RESULT_OR,
            |a, b| a.or(b)
        );
    }

    #[test]
    fn array_array_and() {
        run_test::<ArrayContainer, ArrayContainer, _>(
            &INPUT_A, 
            &INPUT_B, 
            &RESULT_AND,
            |a, b| a.and(b)
        );
    }

    #[test]
    fn array_array_and_not() {
        run_test::<ArrayContainer, ArrayContainer, _>(
            &INPUT_A, 
            &INPUT_B, 
            &RESULT_AND_NOT,
            |a, b| a.and_not(b)
        );
    }

    #[test]
    fn array_array_xor() {
        run_test::<ArrayContainer, ArrayContainer, _>(
            &INPUT_A, 
            &INPUT_B, 
            &RESULT_XOR,
            |a, b| a.xor(b)
        );
    }

    #[test]
    fn array_array_is_subset() {
        let a = make_container::<ArrayContainer>(&SUBSET_A);
        let b = make_container::<ArrayContainer>(&SUBSET_B);

        assert!(a.subset_of(&b));
        assert!(!b.subset_of(&a));
    }

    #[test]
    fn not() {
        let a = make_container::<ArrayContainer>(&INPUT_A);
        let not_a = a.not(0..(a.cardinality() as u16));

        let mut failed = false;
        for value in a.iter() {
            if not_a.contains(*value) {
                failed = true;
                break;
            }
        }

        assert!(!failed);
    }

    #[test]
    fn array_bitset_or() {
        run_test::<ArrayContainer, BitsetContainer, _>(
            &INPUT_A, 
            &INPUT_B, 
            &RESULT_OR,
            |a, b| a.or(b)
        );
    }

    #[test]
    fn array_bitset_and() {
        run_test::<ArrayContainer, BitsetContainer, _>(
            &INPUT_A, 
            &INPUT_B, 
            &RESULT_AND,
            |a, b| a.and(b)
        );
    }

    #[test]
    fn array_bitset_and_not() {
        run_test::<ArrayContainer, BitsetContainer, _>(
            &INPUT_A, 
            &INPUT_B, 
            &RESULT_AND_NOT,
            |a, b| a.and_not(b)
        );
    }

    #[test]
    fn array_bitset_xor() {
        run_test::<ArrayContainer, BitsetContainer, _>(
            &INPUT_A, 
            &INPUT_B, 
            &RESULT_XOR,
            |a, b| a.xor(b)
        );
    }

    #[test]
    fn array_bitset_is_subset() {
        let a = make_container::<ArrayContainer>(&SUBSET_A);
        let b = make_container::<BitsetContainer>(&SUBSET_B);

        assert!(a.subset_of(&b));
        assert!(!b.subset_of(&a));
    }

    /*
    #[test]
    fn array_run_or() {

    }

    #[test]
    fn array_run_and() {

    }

    #[test]
    fn array_run_and_not() {

    }

    #[test]
    fn array_run_xor() {

    }

    #[test]
    fn array_run_is_subset() {

    }
    */
}