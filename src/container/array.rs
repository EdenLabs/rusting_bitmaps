use std::io::{self, Read, Write};
use std::mem;
use std::ptr;
use std::ops::{Deref, DerefMut};
use std::slice::{self, Iter};

use crate::container::*;
use crate::container::array_ops;

/// An array container. Elements are sorted numerically and represented as individual values in the array
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArrayContainer {
    /// The internal array containing container values
    array: Vec<u16>
}

impl ArrayContainer {
    /// Create a new array container
    #[inline]
    pub fn new() -> Self {
        Self {
            array: Vec::new()
        }
    }

    /// Create a new array container with a specified capacity
    #[inline]
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
        debug_assert!({
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
                true
            },
            Err(index) => {
                if self.cardinality() < (std::u16::MAX as usize) {
                    self.array.insert(index, value);

                    true
                }
                else {
                    false
                }
            }
        }
    }

    /// Add all values within the specified range
    pub fn add_range(&mut self, range: Range<u32>) {
        debug_assert!(is_valid_range(range.clone()));

        if range.is_empty() {
            self.add(range.start as u16);
        }
        else {
            // Resize to fit all new elements
            self.reserve(range.len());

            // Append new elements
            for i in range {
                // This is technically valid since we only store the lower 16 bits
                // inside containers. The upper 16 are stored as keys in the roaring bitmap
                self.array.push(i as u16);
            }
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
        if range.is_empty() || range.end as usize > self.len() {
            return;
        }

        let len = self.len();
        let remainder = (range.end)..len;
        self.array.copy_within(remainder.clone(), range.start);
        self.array.truncate(range.start + remainder.len());
    }

    /// Check if the array contains a specified value
    #[inline]
    pub fn contains(&self, value: u16) -> bool {
        self.array.binary_search(&value).is_ok()
    }

    /// Check if the array contains all values within [min-max)
    pub fn contains_range(&self, range: Range<u32>) -> bool {
        debug_assert!(is_valid_range(range.clone()));

        let rs = range.start as u16;
        let re = (range.end - 1) as u16;

        let min = array_ops::advance_until(&self.array, 0, rs);
        let max = array_ops::advance_until(&self.array, 0, re);

        if  min < self.len() && max < self.len() {
            return max - min == (re - rs) as usize && self.array[min] == rs && self.array[max] == re;
        }

        false
    }

    /// Check if the array is full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.array.len() == DEFAULT_MAX_SIZE
    }

    /// Find the element of a given rank from `start_rank`. 
    /// 
    /// # Returns 
    /// None if no element is present and updates `start_rank` accordingly
    pub fn select(&self, rank: u32, start_rank: &mut u32) -> Option<u16> {
        let cardinality = self.cardinality() as u32;
        if *start_rank + cardinality <= rank {
            *start_rank += cardinality;

            None
        }
        else {
            Some(self.array[(rank - *start_rank) as usize])
        }
    }

    /// The smallest element in the array. Returns `None` if `cardinality` is 0
    #[inline]
    pub fn min(&self) -> Option<u16> {
        if self.array.is_empty() {
            None
        }
        else {
            Some(self.array[0])
        }
    }

    /// The largest element in the array. Returns `None` if the cardinality is 0
    #[inline]
    pub fn max(&self) -> Option<u16> {
        if self.array.is_empty() {
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
            Err(index) => if index > 0 { index - 1 } else { 0 }
        }
    }

    /// Compute the number of runs in the array
    pub fn num_runs(&self) -> usize {
        if self.is_empty() {
            return 0;
        }

        let mut num_runs = 0; // Always at least one run
        let mut previous = self.array[0];

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
        cardinality * mem::size_of::<u16>()
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

            let num_read = buf.read(bytes_slice)?;
            if num_read != num_bytes {
                return Err(
                    io::Error::new(io::ErrorKind::UnexpectedEof, 
                    "Unexpected end of stream"
                ));
            }

            result.set_cardinality(cardinality);

            Ok(result)
        }
    }
}

impl Deref for ArrayContainer {
    type Target = [u16];

    #[inline]
    fn deref(&self) -> &[u16] {
        &self.array
    }
}

impl DerefMut for ArrayContainer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.array
    }
}

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

            for i in run_start..=run_end {
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
            unsafe {
                // Make sure the contents will fit
                let required = max_cardinality - self.len();
                self.reserve(required);

                // Offset the contents of self so we can put the result in the beginning of the array
                let start = self.as_mut_ptr();
                let end = start.add(other.len());

                ptr::copy(start, end, self.len());

                // Run the optimized union code on the contents
                let s0 = slice::from_raw_parts(end, self.len());
                let s1 = other.array.as_slice();

                let card = array_ops::or(s0, s1, start);

                self.set_cardinality(card);
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

                // Load the contents of the bitset into the array
                self.clear();
                for value in bitset.iter() {
                    self.push(value);
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
    
    // TODO: Find a way to do this inplace
    #[inline]
    fn inplace_or(self, other: &BitsetContainer) -> Container {
        SetOr::or(&self, other)
    }
}

impl SetOr<RunContainer> for ArrayContainer {
    #[inline]
    fn or(&self, other: &RunContainer) -> Container {
        SetOr::or(other, self)
    }
    
    // TODO: Find a way to do this inplace
    #[inline]
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
    
    #[inline]
    fn and_cardinality(&self, other: &Self) -> usize {
        array_ops::and_cardinality(&self.array, &other.array)
    }
    
    fn inplace_and(mut self, other: &Self) -> Container {
        unsafe {
            // Shift the elements of self over to accomodate new contents
            let len = self.len();
            let req = len.max(other.len());
            self.reserve(req);

            let src = self.as_mut_ptr();
            let dst = src.add(req);

            ptr::copy(src, dst, len);
            
            let ptr = self.as_ptr().add(req);
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
    
    // TODO: Find a way to do this inplace
    #[inline]
    fn inplace_and(self, other: &BitsetContainer) -> Container {
        SetAnd::and(&self, other)
    }
}

impl SetAnd<RunContainer> for ArrayContainer {
    #[inline]
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
    
    // TODO: Find a way to do this inplace
    #[inline]
    fn inplace_and(self, other: &RunContainer) -> Container {
        SetAnd::and(&self, other)
    }
}

impl SetAndNot<Self> for ArrayContainer {
    fn and_not(&self, other: &Self) -> Container {
        let len = self.len();
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
            self.reserve(len);

            let src = self.as_mut_ptr();
            let dst = src.add(len);
            ptr::copy(src, dst, len);
            
            let slice = slice::from_raw_parts(
                self.as_ptr().add(len),
                self.len()
            );

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
    
    // TODO: Find a way to do this inplace
    fn inplace_and_not(self, other: &BitsetContainer) -> Container {
        SetAndNot::and_not(&self, other)
    }
}

impl SetAndNot<RunContainer> for ArrayContainer {
    fn and_not(&self, other: &RunContainer) -> Container {
        if other.is_empty() {
            return Container::Array(self.clone());
        }

        if other.is_full() {
            return Container::Array(ArrayContainer::new());
        }

        let mut result = ArrayContainer::with_capacity(self.cardinality());
 
        let runs = other.deref();
        let run = runs[0];
        let mut run_start: usize = usize::from(run.value);
        let mut run_end: usize = usize::from(run.end());
        let mut which_run = 0;

        let mut i = 0;
        while i < self.cardinality() {
            let value = usize::from(self.array[i]);

            if value < run_start {
                result.push(value as u16);
            } else if value <= run_end {
                ;
            }
            else {
                while value > run_end {
                    if which_run + 1 < runs.len() {
                        which_run += 1;

                        let run = runs[which_run];
                        run_start = usize::from(run.value);
                        run_end = usize::from(run.end());
                    }
                    else {
                        run_end = (1 << 16) + 1;
                        run_start = run_end;
                    }
                }

                if i > 0 {
                    i -= 1;
                }
            }

            i += 1;
        }

        Container::Array(result)
    }
    
    // TODO: Find a way to do this inplace
    #[inline]
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
        // TODO: See if this wouldn't be more optimal using a scalar approach to avoid realloc
        unsafe {
            // Shift the elements of self over to accomodate new contents
            let len = self.len();
            let req = len + other.len();
            self.reserve(req);

            let src = self.as_mut_ptr();
            let dst = src.add(req);
            ptr::copy_nonoverlapping(src, dst, len);
            
            let slice = slice::from_raw_parts(
                self.as_ptr().add(req),
                len
            );

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
    
    // TODO: Find a way to do this inplace
    #[inline]
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
            SetXor::inplace_xor(ArrayContainer::from(other), self)
        }
        // Process as a bitset since the final result may be a bitset
        else {
            SetXor::inplace_xor(BitsetContainer::from(other), self)
        }
    }
    
    // TODO: Find a way to do this inplace
    #[inline]
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

        i0 == card0
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
            
            i_a == self.len()
        }
    }
}

impl SetNot for ArrayContainer {
    fn not(&self, range: Range<u32>) -> Container {
        debug_assert!(is_valid_range(range.clone()));

        let mut bitset = BitsetContainer::new();
        bitset.set_all();
        bitset.clear_list(&self.array[(range.start as usize)..(range.end as usize)]);

        Container::Bitset(bitset)
    }

    fn inplace_not(self, range: Range<u32>) -> Container {
        debug_assert!(is_valid_range(range.clone()));

        SetNot::not(&self, range)
    }
}

#[cfg(test)]
mod test {
    use crate::container::*;
    use crate::test::*;
    use super::*;

    impl TestShim<u16> for ArrayContainer {
        fn from_data(data: &[u16]) -> Self {
            let mut result = Self::new();

            for value in data.iter() {
                result.add(*value);
            }

            result
        }

        fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=u16> + 'a> {
            Box::new(self.clone().array.into_iter())
        }

        fn card(&self) -> usize {
            self.cardinality()
        }
    }

    // Common bookkeeping
    #[test]
    fn load() {
        // Load the container
        let data = generate_data(0..65535, 100, 5);
        let mut c = ArrayContainer::with_capacity(data.len());
        for value in data.iter() {
            c.add(*value);
        }

        assert_eq!(c.cardinality(), data.len());
        
        // Check that the contents match
        let pass = c.iter()
            .zip(data.iter());

        let mut failed = false;
        for (found, expected) in pass {
            if *found != *expected {
                failed = true;
                break;
            }
        }

        assert!(!failed);
    }

    #[test]
    fn load_range() {
        let range = 0..(1 << 16);
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range.clone());

        assert_eq!(array.cardinality(), range.len());

        for (found, expected) in array.iter().zip(range) {
            assert_eq!(*found, expected as u16);
        }
    }

    #[test]
    fn remove() {
        let range = 0..10;
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range.clone());

        array.remove(8);
        array.remove(9);

        assert_eq!(array.cardinality(), 8);
        
        let pass = array.iter()
            .zip(0..10);

        for (found, expected) in pass {
            assert_eq!(*found, expected);
        }
    }

    #[test]
    fn remove_range() {
        let range = 0..60;
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range.clone());

        array.remove_range(45..60);

        assert_eq!(array.cardinality(), 45);

        let pass = array.iter()
            .zip(0..60);

        for (found, expected) in pass {
            assert_eq!(*found, expected);
        }
    }

    #[test]
    fn contains() {
        let range = 0..10;
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range);

        assert!(array.contains(5));
    }

    #[test]
    fn contains_range() {
        let range = 0..30;
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range);

        assert!(array.contains_range(10..20));
    }

    #[test]
    fn select() {
        let range = 0..30;
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range);

        let mut start_rank = 0;
        let selected = array.select(10, &mut start_rank);
        
        assert!(selected.is_some());
        assert_eq!(selected.unwrap(), 10);
    }

    #[test]
    fn min() {
        let range = 0..10;
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range);

        let min = array.min();
        assert!(min.is_some());
        assert_eq!(min.unwrap(), 0);
    }

    #[test]
    fn max() {
        let range = 0..10;
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range);

        let max = array.max();
        assert!(max.is_some());
        assert_eq!(max.unwrap(), 9);
    }

    #[test]
    fn rank() {
        let range = 0..100;
        let mut array = ArrayContainer::with_capacity(range.len());
        array.add_range(range);

        let rank = array.rank(20);
        assert_eq!(rank, 21);
    }
    
    #[test]
    fn round_trip_serialize() {
        let data = generate_data(0..65535, 100, 5);
        let array = ArrayContainer::from_data(&data);

        let serialized_size = ArrayContainer::serialized_size(array.cardinality());
        let mut buffer: Vec<u8> = Vec::with_capacity(serialized_size);

        // Serialize and check that it worked as expected
        let bytes = {
            let result = array.serialize(&mut buffer);
            assert!(result.is_ok());

            result.unwrap()
        };

        // We have to subtract 2 since `serialized_size` includes space for a 2 byte header
        assert_eq!(bytes, serialized_size, "Invalid serialized size");

        let result = {
            let mut read = std::io::Cursor::new(&buffer);
            let result = ArrayContainer::deserialize(array.cardinality(), &mut read);
            assert!(result.is_ok());

            result.unwrap()
        };

        // Check that the deserialized version matches the initial one
        assert_eq!(result.cardinality(), array.cardinality());

        let pass = result.iter()
            .zip(array.iter());

        for (found, expected) in pass {
            assert_eq!(*found, *expected);
        }
    }

    // Ops
    #[test]
    fn array_array_or() {
        op_test::<ArrayContainer, ArrayContainer, u16, _, Container>(
            OpType::Or, 
            0..65535, 
            10, 
            1, 
            |a, b| a.or(&b)
        );
    }

    #[test]
    fn array_array_and() {
        op_test::<ArrayContainer, ArrayContainer, u16, _, Container>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and(&b)
        );
    }

    #[test]
    fn array_array_and_cardinality() {
        op_card_test::<ArrayContainer, ArrayContainer, u16, _>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn array_array_and_not() {
        op_test::<ArrayContainer, ArrayContainer, u16, _, Container>(
            OpType::AndNot, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn array_array_xor() {
        op_test::<ArrayContainer, ArrayContainer, u16, _, Container>(
            OpType::Xor, 
            0..65535, 
            10, 
            1, 
            |a, b| a.xor(&b)
        );
    }

    #[test]
    fn array_array_subset_of() {
        op_subset_test::<ArrayContainer, ArrayContainer, u16>(0..65535, 100, 10);
    }

    #[test]
    fn array_not() {
        let data_a = generate_data(0..65535, 100, 10);
        let a = ArrayContainer::from_data(&data_a);
        let not_a = a.not(0..(a.cardinality() as u32));

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
    fn array_array_inplace_or() {
        op_test::<ArrayContainer, ArrayContainer, u16, _, Container>(
            OpType::Or, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn array_array_inplace_and() {
        op_test::<ArrayContainer, ArrayContainer, u16, _, Container>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn array_array_inplace_and_not() {
        op_test::<ArrayContainer, ArrayContainer, u16, _, Container>(
            OpType::AndNot, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn array_array_inplace_xor() {
        op_test::<ArrayContainer, ArrayContainer, u16, _, Container>(
            OpType::Xor, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn array_bitset_or() {
        op_test::<ArrayContainer, BitsetContainer, u16, _, Container>(
            OpType::Or, 
            0..65535, 
            10, 
            1, 
            |a, b| a.or(&b)
        );
    }

    #[test]
    fn array_bitset_and() {
        op_test::<ArrayContainer, BitsetContainer, u16, _, Container>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and(&b)
        );
    }

        #[test]
    fn array_bitset_and_cardinality() {
        op_card_test::<ArrayContainer, BitsetContainer, u16, _>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn array_bitset_and_not() {
        op_test::<ArrayContainer, BitsetContainer, u16, _, Container>(
            OpType::AndNot, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn array_bitset_xor() {
        op_test::<ArrayContainer, BitsetContainer, u16, _, Container>(
            OpType::Xor, 
            0..65535, 
            10, 
            1, 
            |a, b| a.xor(&b)
        );
    }

    #[test]
    fn array_bitset_subset_of() {
        op_subset_test::<ArrayContainer, BitsetContainer, u16>(0..65535, 100, 10);
    }

    #[test]
    fn array_bitset_inplace_or() {
        op_test::<ArrayContainer, BitsetContainer, u16, _, Container>(
            OpType::Or, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn array_bitset_inplace_and() {
        op_test::<ArrayContainer, BitsetContainer, u16, _, Container>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn array_bitset_inplace_and_not() {
        op_test::<ArrayContainer, BitsetContainer, u16, _, Container>(
            OpType::AndNot, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn array_bitset_inplace_xor() {
        op_test::<ArrayContainer, BitsetContainer, u16, _, Container>(
            OpType::Xor, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn array_run_or() {
        op_test::<ArrayContainer, RunContainer, u16, _, Container>(
            OpType::Or, 
            0..65535, 
            10, 
            1, 
            |a, b| a.or(&b)
        );
    }

    #[test]
    fn array_run_and() {
        op_test::<ArrayContainer, RunContainer, u16, _, Container>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and(&b)
        );
    }

        #[test]
    fn array_run_and_cardinality() {
        op_card_test::<ArrayContainer, RunContainer, u16, _>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn array_run_and_not() {
        op_test::<ArrayContainer, RunContainer, u16, _, Container>(
            OpType::AndNot, 
            0..65535, 
            10, 
            1, 
            |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn array_run_xor() {
        op_test::<ArrayContainer, RunContainer, u16, _, Container>(
            OpType::Xor, 
            0..65535, 
            10, 
            1, 
            |a, b| a.xor(&b)
        );
    }

    #[test]
    fn array_run_inplace_or() {
        op_test::<ArrayContainer, RunContainer, u16, _, Container>(
            OpType::Or, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_or(&b)
        );
    }

    #[test]
    fn array_run_inplace_and() {
        op_test::<ArrayContainer, RunContainer, u16, _, Container>(
            OpType::And, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_and(&b)
        );
    }

    #[test]
    fn array_run_inplace_and_not() {
        op_test::<ArrayContainer, RunContainer, u16, _, Container>(
            OpType::AndNot, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_and_not(&b)
        );
    }

    #[test]
    fn array_run_inplace_xor() {
        op_test::<ArrayContainer, RunContainer, u16, _, Container>(
            OpType::Xor, 
            0..65535, 
            10, 
            1, 
            |a, b| a.inplace_xor(&b)
        );
    }

    #[test]
    fn array_run_subset_of() {
        op_subset_test::<ArrayContainer, RunContainer, u16>(0..65535, 100, 10);
    }
}