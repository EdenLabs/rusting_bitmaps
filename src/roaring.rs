#![allow(exceeding_bitshifts)]

use std::ops::{Range};

use crate::utils;
use crate::container::*;
use crate::container::array_ops;

// TODO: Add support for custom allocators
// TODO: Implement checked variants?

/// A Roaring Bitmap
///
/// Roaring bitmaps are an optimized bitmap implementation for 32 bit integer sets 
/// that support high performance queries and a compact memory representation.
/// 
/// # How it works
/// Internally data is split into a 16 bit key consisting of the upper 16 bits of the value, and a 16 bit
/// value that contains the lower 16 bits. Only the lower 16 bits are stored and the value is reconstructed
/// from the key on demand. The storage method used changes dynamically based on the number of values
/// contained within the bitmap.
/// 
/// Generallly the representation selected is as follows
/// ```
/// Less than 4096 elements       : Array
/// Less than `u16::MAX` elements : Bitset
/// More than `u16::MAX` elements : RLE encoded
/// ```
/// 
/// # Performance Remarks
/// Frequent modification of a bitmap may result in high memory churn due to transitions between
/// in memory representations of the bitmap contents. As such, if the bitmap is to be modified frequently 
/// it is best to aggregate operations and apply them at once. Providing an optimized allocator that somewhat
/// preserves memory locality and has a low cost is desireable.
/// 
/// If building a custom allocator the memory characteristics of a roaring bitmap are as follows
/// 
/// Roaring Bitmap:
///  - Nonlinear growable vectors to store a 16 bit key and container pointer + data for each bucket
/// 
/// Containers:
///  - Arrays require a maximum of `4096 * 2` bytes (grown with contents)
///  - Bitmaps require `1024 * 8` bytes (fixed at allocation)
///  - Run containers are nonlinear and depend on the distribution of the set contents over the universe of 32 bit integers
/// 
/// Once a bitmap is built queries done via the `inplace_<op>` variants will only incur a cost for the query bitmap.
/// Queries using the normal ops will create a new bitmap for every operation.
/// 
/// `cardinality()` (`len()`) queries may lazily evaluate the cardinality of some containers if they are determined to be out of date
#[derive(Clone, Debug)]
pub struct RoaringBitmap {
    /// List of containers in this roaring bitmap
    containers: Vec<Container>,

    /// List of keys corresponding to the containers in the bitmap
    keys: Vec<u16>
}

impl RoaringBitmap {
    /// Create a new empty roaring bitmap
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }
    
    /// Create a new roaring bitmap with the specified capacity for storing containers
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            containers: Vec::with_capacity(capacity),
            keys: Vec::with_capacity(capacity)
        }
    }
    
    /// Create a new roaring bitmap with the specified range and step
    pub fn from_range(range: Range<u32>) -> Self {
        // No elements, just return an empty bitmap
        if range.len() == 0 {
            return Self::new();
        }

        let min = range.start;
        let max = range.end;

        let mut bitmap = Self::new();

        let mut value = min;
        while value < max {
            let key = value >> 16;
            let container_min = value as u16;
            let container_max = (max - (key << 16)).min(1 << 16) as u16;

            if let Some(container) = Container::from_range(container_min..container_max) {
                bitmap.containers.push(container);
                bitmap.keys.push(key as u16);
            }

            value += (container_max - container_min) as u32;
        }

        bitmap
    }
    
    /// Create a new roaring bitmap from a set of elements
    pub fn from_slice(slice: &[u32]) -> Self {
        let mut bitmap = Self::new();
        bitmap.add_slice(slice);

        bitmap
    }

    /// Copy the contents of `other` into self overwriting any existing values
    pub fn copy_from(&mut self, other: &RoaringBitmap) {
        self.containers.clear();
        self.keys.clear();

        self.containers.extend_from_slice(&other.containers);
        self.keys.extend_from_slice(&other.keys);
    }
    
    /// Add a value to the bitmap
    pub fn add(&mut self, value: u32) {
        let x_high = (value >> 16) as u16;

        match self.keys.binary_search(&x_high) {
            Ok(i) => {
                self.containers[i].add(value as u16)
            },
            Err(i) => {
                let mut array = ArrayContainer::new();
                array.add(value as u16);

                self.containers.insert(i, Container::Array(array));
                self.keys.insert(i, x_high);
            }
        }
    }

    /// Add a value to the bitmap and return the index of the container that the value was added to.
    /// This allows accelerating some operations.
    /// 
    /// # Remarks
    /// The index container is only guaranteed to be valid immediately after the call assuming
    /// no containers are removed in subsequent operations.
    fn add_fetch_container(&mut self, value: u32) -> usize {
        let x_high = (value >> 16) as u16;

        if let Some(i) = self.get_index(&x_high) {
            self.containers[i].add(value as u16);

            i
        }
        else {
            let mut array = ArrayContainer::new();
            array.add(value as u16);

            self.containers.push(Container::Array(array));
            self.keys.push(x_high);

            self.containers.len() - 1
        }
    }

    /// Add a range of values to the bitmap
    pub fn add_range(&mut self, range: Range<u32>) {
        let min = range.start;
        let max = range.end;

        // Determine keys
        let min_key = (range.start >> 16) as u16;
        let max_key = ((range.end - 1) >> 16) as u16;
        let span = (max_key - min_key) as isize;
        
        // Determine lengths
        let prefix_len = array_ops::count_less(&self.keys, min_key) as isize;
        let suffix_len = array_ops::count_greater(&self.keys, max_key) as isize;
        let common_len = (self.keys.len() as isize) - prefix_len - suffix_len;

        // Reserve extra space for the new containers
        if span > common_len {
            let required = (span - common_len) as usize;
            self.containers.reserve(required);
            self.keys.reserve(required);
        }

        let mut src: isize = prefix_len + common_len - 1; // isize as this could potentially be -1
        let mut dst: isize = (self.keys.len() as isize) - suffix_len - 1;
        for key in (min_key..max_key).rev() {
            let container_min = if min_key == key { min as u16 } else { 0 };
            let container_max = if max_key == key { max as u16 } else { 0 };

            if src >= 0 && self.keys[src as usize] == key {
               let container = &mut self.containers[src as usize];
               container.add_range(container_min..container_max);

               src -= 1;
            }
            else {
                if let Some(container) = Container::from_range(container_min..container_max) {
                    self.containers.insert(dst as usize, container);
                    self.keys.insert(dst as usize, key);
                }
            }

            dst -= 1;
        }
    }
    
    /// Add a list of values to the bitmap
    pub fn add_slice(&mut self, slice: &[u32]) {
        // Add the first value so we can nab the container index
        if slice.len() == 0 {
            return;
        }

        unsafe {
            let mut value = *slice.get_unchecked(0);
            let mut prev = value;
            let mut i = 1;
            let mut c_index = self.add_fetch_container(value);

            while i < slice.len() {
                value = *slice.get_unchecked(i);
                // Check if the upper 16 bits match the previous value, if so the value goes
                // into the same container and we can just append to that one
                if (prev ^ value) >> 16 == 0 {
                    self.containers.get_unchecked_mut(c_index)
                        .add(value as u16);
                }
                else {
                    c_index = self.add_fetch_container(value);
                }

                prev = value;
                i += 1;
            }
        }
    }
    
    /// Remove a value from the bitmap
    pub fn remove(&mut self, value: u32) {
        let x_high = (value >> 16) as u16;
        
        if let Some(i) = self.get_index(&x_high) {
            self.containers[i].remove(value as u16);
            
            if self.containers[i].cardinality() == 0 {
                self.containers.pop();
                self.keys.pop();
            }
        }
    }

    /// Remove a range of values from the bitmap
    pub fn remove_range(&mut self, range: Range<u32>) {
        debug_assert!(range.len() > 0);

        let min = range.start;
        let max = range.end;
        
        let min_key = (range.start >> 16) as u16;
        let max_key = (range.end >> 16) as u16;

        let mut src = array_ops::count_less(&self.keys, min_key);
        let mut dst = src;

        while src < self.keys.len() && self.keys[src] <= max_key {
            let container_min = if min_key == self.keys[src] { min as u16 } else { 0 };
            let container_max = if max_key == self.keys[src] { max as u16 } else { 0xFFFF };

            let has_elements = self.containers[src]
                .remove_range(container_min..container_max);

            if has_elements {
                dst += 1;
            }

            src += 1;
        }

        // Check if any containers can be removed
        if src > dst {
            let count = dst - src;
            let start = self.containers.len() - src;
            let end = start + count;

            self.containers.drain(start..end);
            self.keys.drain(start..end);
        }
    }
    
    /// Remove a list of values from the bitmap
    pub fn remove_slice(&mut self, slice: &[u32]) {
        if slice.len() == 0 {
            return;
        }

        unsafe {
            let mut c_index = None;
            for value in slice.iter() {
                let key = (*value >> 16) as u16;

                if c_index.is_none() || key != self.keys[c_index.unwrap()] {
                    c_index = self.get_index(&key);
                }
                
                if let Some(index) = c_index {
                    let container = self.containers.get_unchecked_mut(index);

                    container.remove(*value as u16);

                    if container.cardinality() == 0 {
                        self.containers.pop();
                        c_index = None;
                    }
                }
            }
        }
    }
    
    /// Check if the bitmap contains a value
    pub fn contains(&self, value: u32) -> bool {
        let high = (value >> 16) as u16;

        if let Some(i) = self.get_index(&high) {
            return self.containers[i].contains(value as u16);
        }

        false
    }
    
    /// Check if the bitmap contains a range of values
    pub fn contains_range(&self, range: Range<u32>) -> bool {
        // We always contain the empty set
        if range.len() == 0 {
            return true;
        }

        // Do an optimized single value contains if there's only one element in the set
        if range.len() == 1 {
            return self.contains(range.start);
        }

        // Do a ranged contains operation
        let key_min = (range.start >> 16) as u16;
        let key_max = (range.end >> 16) as u16;
        let key_span = (key_max - key_min) as usize;

        // Key range exceeds those stored in this bitmap, can't possibly contain the set
        if self.keys.len() < key_span + 1 {
            return false;
        }

        let ci_min = self.get_index(&key_min);
        let ci_max = self.get_index(&key_max);

        // One or both containers don't exist in this bitmap
        if ci_min.is_none() || ci_max.is_none() {
            return false;
        }

        let ci_min = ci_min.unwrap();
        let ci_max = ci_max.unwrap();

        // Not enough intermediate keys are present
        if ci_max - ci_min != key_span {
            return false;
        }

        let val_min = range.start as u16;
        let val_max = range.end as u16;
        let container = &self.containers[ci_min];

        // Min and max are the same, do contains on the single container
        if key_min == key_max {
            return container.contains_range(val_min..val_max);
        }

        // Check if the min container contains [val_min-container_max]
        if !container.contains_range(val_min..std::u16::MAX) {
            return false;
        }

        // Check if the max container contains [container_min-val_max]
        let container = &self.containers[ci_max];
        if !container.contains_range(0..val_max) {
            return false;
        }

        // Check if all containers in between are full
        for container in self.containers[(ci_min + 1)..(ci_max - 1)].iter() {
            if !container.is_full() {
                return false;
            }
        }

        // Range is contained in the bitmap
        true
    }

    /// Get the length of the bitmap
    ///
    /// This is the same as cardinality
    #[inline]
    pub fn len(&self) -> usize {
        self.cardinality()
    }
    
    /// Get the cardinality of the bitmap
    pub fn cardinality(&self) -> usize {
        let mut cardinality = 0;
        for container in self.containers.iter() {
            cardinality += container.cardinality();
        }

        cardinality
    }

    /// Check if the bitmap is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.containers.len() == 0
    }
    
    /// Clear the contents of this bitmap
    #[inline]
    pub fn clear(&mut self) {
        self.containers.clear();
        self.keys.clear();
    }
    
    /// Shrink the memory used by the bitmap to fit it's contents
    pub fn shrink_to_fit(&mut self) {
        self.containers.shrink_to_fit();

        for container in self.containers.iter_mut() {
            container.shrink_to_fit();
        }
    }
    
    /// Find the element of a given rank in the bitmap,
    /// Returns None if the bitmap is smaller than `rank`
    pub fn select(&self, rank: u32) -> Option<u32> {
        let iter = self.keys.iter()
            .zip(&self.containers);

        let mut start_rank = 0;
        for (key, container) in iter {
            if let Some(element) = container.select(rank, &mut start_rank) {
                return Some((element as u32) | ((*key as u32) << 16));
            }
        }

        None
    }
    
    /// Find the number of integers smaller or equal to `x`
    pub fn rank(&self, x: u32) -> usize {
        let x_high = (x >> 16) as u16;

        let mut cardinality = 0;

        let iter = self.keys.iter()
            .zip(&self.containers);

        for (key, container) in iter {
            if x_high > *key {
                cardinality += container.cardinality();
            }
            else if x_high == *key {
                cardinality += container.rank(x as u16);

                break;
            }
            else {
                break;
            }
        }

        cardinality
    }
    
    /// Find the smallest value in the bitmap. Returns None if empty
    pub fn min(&self) -> Option<u32> {
        if self.containers.len() == 0 {
            return None;
        }

        unsafe {
            let key = self.keys.get_unchecked(0);
            let container = self.containers.get_unchecked(0);
            let low = container.min()? as u32;

            Some(low | ((*key as u32) << 16))
        }
    }
    
    /// Find the largest value in the bitmap. Returns None if empty
    pub fn max(&self) -> Option<u32> {
        if self.containers.len() == 0 {
            return None;
        }

        unsafe {
            let last = self.keys.len() - 1;
            let key = self.keys.get_unchecked(last);
            let container = self.containers.get_unchecked(last);
            let low = container.max()? as u32;

            Some(low | ((*key as u32) << 16))
        }
    }

    /// Check if this bitmap is a subset of other
    pub fn subset_of(&self, other: &Self) -> bool {
        // Convention used is as follows
        // 0 = self
        // 1 = other

        let len0 = self.containers.len();   // lengths
        let len1 = other.containers.len();

        let mut i0 = 0; // Indices
        let mut i1 = 0;
        
        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];     // Keys
            let k1 = other.keys[i1];

            if k0 == k1 {
                let c0 = &self.containers[i0]; // Containers
                let c1 = &self.containers[i1];

                if !c0.subset_of(c1) {
                    return false;
                }
                else {
                    i0 += 1;
                    i1 += 1;
                }
            }
            else if k0 < k1 {
                return false;
            }
            else {
                i1 = array_ops::advance_until(&other.keys, i1, k0);
            }
        }
        
        i0 == len0
    }

    /// Compute the Jaccard index between `self` and `other`. 
    /// (Also known as the Tanimoto distance or Jaccard similarity coefficient)
    /// 
    /// Returns `None` if both bitmaps are empty
    pub fn jaccard_index(&self, other: &Self) -> Option<f64> {
        if self.is_empty() && other.is_empty() {
            None
        }
        else {
            let c0 = self.cardinality();
            let c1 = other.cardinality();
            let shared = self.and_cardinality(other);

            Some((shared as f64) / ((c0 + c1 - shared) as f64))
        }
    }

    /// Or this bitmap with `other` (union)
    pub fn or(&self, other: &Self) -> Self {
                let len0 = self.cardinality();
        let len1 = other.cardinality();

        if len0 == 0 {
            return other.clone();
        }

        if len1 == 0 {
            return self.clone();
        }

        let mut result = Self::with_capacity(len0 + len1);
        let mut i0 = 0;
        let mut i1 = 0;

        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];
            let k1 = other.keys[i1];

            if k0 == k1 {
                let c0 = &self.containers[i0];
                let c1 = &other.containers[i1];
                let c = c0.or(c1);

                if !c.is_empty() {
                    result.containers.push(c);
                    result.keys.push(k0);
                }

                i0 += 1;
                i1 += 1;
            }
            else if k0 < k1 {
                let c0 = &self.containers[i0];
                
                result.containers.push(c0.clone());
                result.keys.push(k0);

                i0 += 1;
            }
            else {
                let c1 = &other.containers[i1];

                result.containers.push(c1.clone());
                result.keys.push(k1);

                i1 += 1;
            }
        }
        
        if i0 == len0 {
            result.containers.extend_from_slice(&other.containers[i1..len1]);
            result.keys.extend_from_slice(&other.keys[i1..len1]);
        }
        
        if i1 == len1 {
            result.containers.extend_from_slice(&self.containers[i0..len0]);
            result.keys.extend_from_slice(&self.keys[i0..len0]);
        }

        result
    }
    
    /// And this bitmap with `other` (intersect)
    pub fn and(&self, other: &Self) -> Self {
        let len0 = self.cardinality();
        let len1 = other.cardinality();

        let capacity = len0.min(len1);
        let mut result = Self::with_capacity(capacity);

        let mut i0 = 0;
        let mut i1 = 0;

        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];
            let k1 = other.keys[i1];

            if k0 == k1 {
                let c0 = &self.containers[i0];
                let c1 = &other.containers[i1];
                let c = c0.and(c1);

                if !c.is_empty() {
                    result.containers.push(c);
                    result.keys.push(k0);
                }

                i0 += 1;
                i1 += 1;
            }
            else if k0 < k1 {
                i0 = array_ops::advance_until(&self.keys, i0, k1);
            }
            else {
                i1 = array_ops::advance_until(&other.keys, i1, k0);
            }
        }

        result
    }

    /// And not this bitmap with `other` (difference)
    pub fn and_not(&self, other: &Self) -> Self {
        let len0 = self.cardinality();
        let len1 = other.cardinality();
        
        if len0 == 0 {
            return RoaringBitmap::new();
        }

        if len1 == 0 {
            return self.clone();
        }

        let mut result = RoaringBitmap::with_capacity(len0);
        let mut i0 = 0;
        let mut i1 = 0;

        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];
            let k1 = other.keys[i1];

            if k0 == k1 {
                let c0 = &self.containers[i0];
                let c1 = &other.containers[i1];
                let c = c0.and_not(c1);

                if !c.is_empty() {
                    result.containers.push(c);
                    result.keys.push(k0);
                }
                
                i0 += 1;
                i1 += 1;
            }
            else if k0 < k1 {
                let i0_next = array_ops::advance_until(&self.keys, i0, k1);

                result.containers.extend_from_slice(&self.containers[i0..i0_next]);
                result.keys.extend_from_slice(&self.keys[i0..i0_next]);

                i0 = i0_next;
            }
            else {
                i1 = array_ops::advance_until(&other.keys, i1, k0);
            }
        }

        if i1 == len1 {
            result.containers.extend_from_slice(&self.containers[i0..len0]);
            result.keys.extend_from_slice(&self.keys[i0..len0]);
        }

        result
    }

    /// Xor this bitmap with `other` ()
    pub fn xor(&self, other: &Self) -> Self {
        let len0 = self.cardinality();
        let len1 = other.cardinality();

        if len0 == 0 {
            return other.clone();
        }

        if len1 == 0 {
            return self.clone();
        }

        let mut result = Self::with_capacity(len0 + len1);
        let mut i0 = 0;
        let mut i1 = 0;

        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];
            let k1 = other.keys[i1];

            if k0 == k1 {
                let c0 = &self.containers[i0];
                let c1 = &other.containers[i1];
                let c = c0.xor(c1);

                if !c.is_empty() {
                    result.containers.push(c);
                    result.keys.push(k0);
                }

                i0 += 1;
                i1 += 1;
            }
            else if k0 < k1 {
                let c0 = &self.containers[i0];
                
                result.containers.push(c0.clone());
                result.keys.push(k0);

                i0 += 1;
            }
            else {
                let c1 = &other.containers[i1];

                result.containers.push(c1.clone());
                result.keys.push(k1);

                i1 += 1;
            }
        }
        
        if i0 == len0 {
            result.containers.extend_from_slice(&other.containers[i1..len1]);
            result.keys.extend_from_slice(&other.keys[i1..len1]);
        }
        
        if i1 == len1 {
            result.containers.extend_from_slice(&self.containers[i0..len0]);
            result.keys.extend_from_slice(&self.keys[i0..len0]);
        }

        result
    }

    /// Negate all elements within `range` in this bitmap
    pub fn flip(&self, range: Range<u32>) -> Self {
        let mut result = Self::new();

        let mut start_high = (range.start >> 16) as u16;
        let start_low = range.start as u16;

        let mut end_high = (range.end >> 16) as u16;
        let end_low = range.end as u16;

        // Append all preceding elements that are not to be flipped
        let end = array_ops::advance_until(&self.keys, 0, start_high);
        result.containers.extend_from_slice(&self.containers[0..end]);
        result.keys.extend_from_slice(&self.keys[0..end]);

        // Range occupies the same container, just flip that
        if start_high == end_high {
            result.append_flipped(self, start_high, start_low..end_low);
        }
        // Else flip a cross container range
        else {
            // Handle a partial start container
            if start_low > 0 {
                result.append_flipped(self, start_high, start_low..std::u16::MAX);

                start_high += 1;
            }

            if end_low != std::u16::MAX {
                end_high -= 1;
            }

            // Handle all containers in the middle of the range skipping the last container
            for bound in start_high..end_high {
                result.append_flipped(self, bound, 0..std::u16::MAX);
            }

            // Handle a partial final container
            if end_low != std::u16::MAX {
                end_high += 1;

                result.append_flipped(self, end_high, 0..end_low);
            }
        }

        // Append any remaining containers
        if let Some(mut i_last) = self.get_index(&end_high) {
            i_last += 1; // Increment to get the next container after the last flipped one

            if i_last < self.containers.len() {
                result.containers.extend_from_slice(&self.containers[i_last..]);
                result.keys.extend_from_slice(&self.keys[i_last..]);
            }
        }

        result
    }

    /// Insert the negation of the container within `range` with the given key.
    /// Creates a new full container if no container is found
    fn append_flipped(&mut self, other: &Self, key: u16, range: Range<u16>) {
        if let Some(i) = other.get_index(&key) {
            let unflipped = &other.containers[i];
            let flipped = unflipped.not(range);

            if flipped.cardinality() > 0 {
                self.containers.push(flipped);
                self.keys.push(key);
            }
        }
        else {
            if let Some(c) = Container::from_range(range) {
                self.containers.push(c);
                self.keys.push(key);
            }
        }
    }

    /// Same as [`or`] but operates in place on `self`
    /// 
    /// [`or`]: RoaringBitmap::or
    pub fn inplace_or(&mut self, other: &Self) {
        unimplemented!()
    }

    /// Same as [`and`] but operates in place on `self`
    /// 
    /// [`and`]: RoaringBitmap::and
    pub fn inplace_and(&mut self, other: &Self) {
        unimplemented!()
    }

    /// Same as [`and_not`] but operates in place on `self`
    /// 
    /// [`and_not`]: RoaringBitmap::and_not
    pub fn inplace_and_not(&mut self, other: &Self) {
        unimplemented!()
    }
    
    /// Same as [`xor`] but operates in place on `self`
    /// 
    /// [`xor`]: RoaringBitmap::xor
    pub fn inplace_xor(&mut self, other: &Self) {
        unimplemented!()
    }

    /// Same as [`not`] but operates in place on `self`
    /// 
    /// [`not`]: RoaringBitmap::not
    pub fn inplace_not(&mut self) {
        unimplemented!()
    }

    /// Compute the cardinality of `or` on `self` and `other` without storing the result
    /// 
    /// # Remarks
    /// This only computes cardinality in place, no allocations are made
    pub fn or_cardinality(&self, other: &Self) -> usize {
        let c0 = self.cardinality();
        let c1 = other.cardinality();
        let shared = self.and_cardinality(other);

        c0 + c1 - shared
    }

    /// Compute the cardinality of `and` on `self` and `other` without storing the result
    /// 
    /// # Remarks
    /// This computes cardinality in place, no allocations are made
    pub fn and_cardinality(&self, other: &Self) -> usize {
        let len0 = self.containers.len();
        let len1 = other.containers.len();

        let mut result = 0;
        let mut i0 = 0;
        let mut i1 = 0;

        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];
            let k1 = other.keys[i1];

            if k0 == k1 {
                let c0 = &self.containers[i0];
                let c1 = &other.containers[i1];

                result += c0.and_cardinality(c1);

                i0 += 1;
                i1 += 1;
            }
            else if k0 < k1 {
                i0 = array_ops::advance_until(&self.keys, i0, k1);
            }
            else {
                i1 = array_ops::advance_until(&other.keys, i1, k0);
            }
        }

        result
    }

    /// Compute the cardinality of `and_not` on `self` and `other` without storing the result
    /// 
    /// # Remarks
    /// This computes cardinality in place, no allocations are made
    pub fn and_not_cardinality(&self, other: &Self) -> usize {
        let c0 = self.cardinality();
        let shared = self.and_cardinality(other);

        c0 - shared
    }

    /// Compute the cardinality of `xor` on `self` and `other` without storing the result
    /// 
    /// # Remarks
    /// This computes cardinality in place, no allocations are made
    pub fn xor_cardinality(&self, other: &Self) -> usize {
        let c0 = self.cardinality();
        let c1 = other.cardinality();
        let shared = self.and_cardinality(other);

        c0 + c1 - 2 * shared
    }

    /// Find the index for a given key
    #[inline]
    fn get_index(&self, x: &u16) -> Option<usize> {
        self.keys.binary_search(x)
            .ok()
    }
}
