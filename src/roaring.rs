#![deny(arithmetic_overflow)]

use std::io::{self, Read, Write, Seek, SeekFrom};
use std::ops::{RangeBounds, Range};
use std::fmt;
use std::mem;

use crate::{inclusive_bounds, write_counted};
use crate::container::{self, *, array_ops};

// TODO: Add support for custom allocators

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
///  - At most 4096 elements         : Array (a bitset that shrinks stays one a while, see below)
///  - Less than `u16::MAX` elements : Bitset
///  - More than `u16::MAX` elements : RLE encoded
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
///  - Arrays require a maximum of `4096 * 2` bytes (grown with contents). Removing from the front of
///    an array leaves room there, which the next insert near the front or a push into a full array reuses
///  - Bitmaps require `1024 * 8` bytes (fixed at allocation). A bitmap that shrinks below 4096 elements
///    by `remove` stays a bitmap until it falls `BITSET_RETAIN_MARGIN` (256) below the limit, so a set
///    hovering at the limit does not convert on every other operation. `remove_range` converts at the limit
///  - Run containers are nonlinear and depend on the distribution of the set contents over the universe of 32 bit integers
/// 
/// Once a bitmap is built queries done via the `inplace_<op>` variants will only incur a cost for the query bitmap.
/// Queries using the normal ops will create a new bitmap for every operation.
/// 
/// `cardinality()` (`len()`) queries may lazily evaluate the cardinality of some containers if they are determined to be out of date
#[derive(Clone, Debug, Default)]
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
    pub fn from_range<R: RangeBounds<u32>>(range: R) -> Self {
        // No elements, just return an empty bitmap
        let (min, max) = match inclusive_bounds(range) {
            Some((first, last)) => (u64::from(first), u64::from(last) + 1),
            None => return Self::new()
        };

        let mut bitmap = Self::new();

        let mut value = min;
        while value < max {
            let key = value >> 16;
            let container_min = (value & 0xFFFF) as u32;
            let container_max = ((max - (key << 16)).min(1 << 16)) as u32;
            let container = Container::from_range(container_min..container_max);

            bitmap.containers.push(container);
            bitmap.keys.push(key as u16);

            value = (key + 1) << 16;
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

        match self.get_index(x_high) {
            Ok(i) => {
                self.containers[i].add(value as u16);

                i
            },
            Err(i) => {
                let mut array = ArrayContainer::new();
                array.add(value as u16);

                self.containers.insert(i, Container::Array(array));
                self.keys.insert(i, x_high);

                i
            }
        }
    }

    /// Add a range of values to the bitmap. An empty or inverted range adds nothing
    pub fn add_range<R: RangeBounds<u32>>(&mut self, range: R) {
        let (min, last) = match inclusive_bounds(range) {
            Some(bounds) => bounds,
            None => return
        };

        let min_key = min >> 16;
        let max_key = last >> 16;

        let mut index = self.keys.partition_point(|&key| u32::from(key) < min_key);
        for key in min_key..=max_key {
            let container_min = if key == min_key { min & 0xFFFF } else { 0 };
            let container_max = if key == max_key { (last & 0xFFFF) + 1 } else { 1 << 16 };

            if index < self.keys.len() && u32::from(self.keys[index]) == key {
                self.containers[index].add_range(container_min..container_max);
            }
            else {
                self.containers.insert(index, Container::from_range(container_min..container_max));
                self.keys.insert(index, key as u16);
            }

            index += 1;
        }
    }
    
    /// Add a list of values to the bitmap
    pub fn add_slice(&mut self, slice: &[u32]) {
        // Add the first value so we can nab the container index
        if slice.is_empty() {
            return;
        }

        let mut value = slice[0];
        let mut prev = value;
        let mut i = 1;
        let mut c_index = self.add_fetch_container(value);

        while i < slice.len() {
            value = slice[i];
            // Check if the upper 16 bits match the previous value, if so the value goes
            // into the same container and we can just append to that one
            if (prev ^ value) >> 16 == 0 {
                self.containers[c_index].add(value as u16);
            }
            else {
                c_index = self.add_fetch_container(value);
            }

            prev = value;
            i += 1;
        }
    }
    
    /// Remove a value from the bitmap, returning whether it was present
    pub fn remove(&mut self, value: u32) -> bool {
        let x_high = (value >> 16) as u16;

        match self.get_index(x_high) {
            Ok(i) => {
                let removed = self.containers[i].remove(value as u16);

                if removed && self.containers[i].is_empty() {
                    self.containers.remove(i);
                    self.keys.remove(i);
                }

                removed
            },
            Err(_) => false
        }
    }

    /// Remove and return the smallest value in the bitmap. Returns None if empty
    ///
    /// Equivalent to `min()` followed by `remove()` without searching for the container.
    /// Removing the last value of the lowest container shifts the key and container vectors
    pub fn pop_min(&mut self) -> Option<u32> {
        let key = *self.keys.first()?;
        let low = self.containers[0].pop_min()?;

        if self.containers[0].is_empty() {
            self.containers.remove(0);
            self.keys.remove(0);
        }

        Some(u32::from(low) | (u32::from(key) << 16))
    }

    /// Remove a range of values from the bitmap
    pub fn remove_range<R: RangeBounds<u32>>(&mut self, range: R) {
        let (min, last) = match inclusive_bounds(range) {
            Some(bounds) => bounds,
            None => return
        };

        let min_key = (min >> 16) as u16;
        let max_key = (last >> 16) as u16;

        let mut i = array_ops::count_less(&self.keys, min_key);
        while i < self.keys.len() && self.keys[i] <= max_key {
            let container_min = if min_key == self.keys[i] { min & 0xFFFF } else { 0 };
            let container_max = if max_key == self.keys[i] { last & 0xFFFF } else { 0xFFFF };

            let has_elements = self.containers[i]
                .remove_range(container_min..(container_max + 1));

            if has_elements {
                i += 1;
            }
            else {
                self.keys.remove(i);
                self.containers.remove(i);
            }
        }
    }
    
    /// Remove a list of values from the bitmap
    pub fn remove_slice(&mut self, slice: &[u32]) {
        if slice.is_empty() {
            return;
        }

        let mut c_index = None;
        for value in slice.iter() {
            let key = (*value >> 16) as u16;

            if c_index.is_none() || key != self.keys[c_index.unwrap()] {
                c_index = self.get_index(key).ok();
            }
            
            if let Some(index) = c_index {
                let container = &mut self.containers[index];

                container.remove(*value as u16);

                if container.is_empty() {
                    self.containers.remove(index);
                    self.keys.remove(index);
                    c_index = None;
                }
            }
        }
    }
    
    /// Check if the bitmap contains a value
    pub fn contains(&self, value: u32) -> bool {
        let high = (value >> 16) as u16;

        if let Ok(i) = self.get_index(high) {
            return self.containers[i].contains(value as u16);
        }

        false
    }
    
    /// Check if the bitmap contains a range of values
    pub fn contains_range<R: RangeBounds<u32>>(&self, range: R) -> bool {
        // We always contain the empty set
        let (min, last) = match inclusive_bounds(range) {
            Some(bounds) => bounds,
            None => return true
        };

        // Do an optimized single value contains if there's only one element in the set
        if min == last {
            return self.contains(min);
        }

        // Do a ranged contains operation
        let key_min = min >> 16;
        let key_max = last >> 16;
        let key_span = (key_max - key_min) as usize;

        // Key range exceeds those stored in this bitmap, can't possibly contain the set
        if self.keys.len() < key_span + 1 {
            return false;
        }

        let ci_min = self.get_index(key_min as u16);
        let ci_max = self.get_index(key_max as u16);

        // One or both containers don't exist in this bitmap
        if ci_min.is_err() || ci_max.is_err() {
            return false;
        }

        let ci_min = ci_min.unwrap();
        let ci_max = ci_max.unwrap();

        // Not enough intermediate keys are present
        if ci_max - ci_min != key_span {
            return false;
        }

        let container = &self.containers[ci_min];
        let value_min = min & 0xFFFF;
        let value_end = (last & 0xFFFF) + 1;

        // Min and max are the same, do contains on the single container
        if key_min == key_max {
            return container.contains_range(value_min..value_end);
        }

        // Check if the min container contains [val_min-container_max]
        if !container.contains_range(value_min..(1 << 16)) {
            return false;
        }

        // Check if the max container contains [container_min-val_max]
        let container = &self.containers[ci_max];
        if !container.contains_range(0..value_end) {
            return false;
        }

        // Check if all containers in between are full
        for container in self.containers[(ci_min + 1)..ci_max].iter() {
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
        self.containers.is_empty()
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
                return Some(u32::from(element) | (u32::from(*key) << 16));
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
        if self.containers.is_empty() {
            return None;
        }

        let key = self.keys[0];
        let container = &self.containers[0];
        let low = u32::from(container.min()?);

        Some(low | (u32::from(key) << 16))
    }
    
    /// Find the largest value in the bitmap. Returns None if empty
    pub fn max(&self) -> Option<u32> {
        if self.containers.is_empty() {
            return None;
        }

        let last = self.keys.len() - 1;
        let key = self.keys[last];
        let container = &self.containers[last];
        let low = u32::from(container.max()?);

        Some(low | (u32::from(key) << 16))
    }

    /// Check if this bitmap is a subset of other
    pub fn subset_of(&self, other: &Self) -> bool {
        let len0 = self.containers.len();   // lengths
        let len1 = other.containers.len();

        let mut i0 = 0; // Indices
        let mut i1 = 0;
        
        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];     // Keys
            let k1 = other.keys[i1];

            if k0 == k1 {
                let c0 = &self.containers[i0]; // Containers
                let c1 = &other.containers[i1];

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
        if self.is_empty() {
            return other.clone();
        }

        if other.is_empty() {
            return self.clone();
        }

        let len0 = self.keys.len();
        let len1 = other.keys.len();

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
        let len0 = self.keys.len();
        let len1 = other.keys.len();

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
        if self.is_empty() {
            return RoaringBitmap::new();
        }

        if other.is_empty() {
            return self.clone();
        }

        let len0 = self.keys.len();
        let len1 = other.keys.len();
        
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
        if self.is_empty() {
            return other.clone();
        }

        if other.is_empty() {
            return self.clone();
        }

        let len0 = self.keys.len();
        let len1 = other.keys.len();

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
    pub fn not<R: RangeBounds<u32>>(&self, range: R) -> Self {
        let (first, last) = match inclusive_bounds(range) {
            Some(bounds) => bounds,
            None => return self.clone()
        };
        let (first_key, last_key) = (first >> 16, last >> 16);

        let start = self.keys.partition_point(|&key| u32::from(key) < first_key);
        let end = self.keys.partition_point(|&key| u32::from(key) <= last_key);

        let mut result = Self::new();
        result.keys.extend_from_slice(&self.keys[..start]);
        result.containers.extend_from_slice(&self.containers[..start]);

        let mut index = start;
        for key in first_key..=last_key {
            let range = key_range(key, first, last);
            let flipped = if index < end && u32::from(self.keys[index]) == key {
                index += 1;
                self.containers[index - 1].not(range)
            }
            else {
                Container::from_range(range)
            };

            if !flipped.is_empty() {
                result.keys.push(key as u16);
                result.containers.push(flipped);
            }
        }

        result.keys.extend_from_slice(&self.keys[end..]);
        result.containers.extend_from_slice(&self.containers[end..]);

        result
    }

    /// Same as [`or`] but operates in place on `self`
    /// 
    /// [`or`]: RoaringBitmap::or
    pub fn inplace_or(&mut self, other: &Self) {
        // Other is the empty set, self is unchanged
        if other.is_empty() {
            return;
        }

        // Self is the empty set, copy all of other
        if self.is_empty() {
            self.copy_from(other);
            return;
        }

        let mut len0    = self.keys.len();
        let len1        = other.keys.len();

        // Handle shared containers in place
        let mut i0 = 0;
        let mut i1 = 0;
        let mut k0 = self.keys[i0];
        let mut k1 = other.keys[i1];

        loop {
            if k0 == k1 {
                let c0 = &mut self.containers[i0];

                if !c0.is_full() {
                    c0.inplace_or(&other.containers[i1]);
                }

                i0 += 1;
                i1 += 1;

                if i0 == len0 { break; }
                if i1 == len1 { break; }

                k0 = self.keys[i0];
                k1 = other.keys[i1];
            }
            else if k0 < k1 {
                i0 += 1;

                if i0 == len0 { break; }

                k0 = self.keys[i0];
            }
            else {
                let c1 = other.containers[i1].clone();
                self.containers.insert(i0, c1);
                self.keys.insert(i0, k1);

                i0      += 1;
                i1      += 1;
                len0    += 1;

                if i1 == len1 { break; }
                
                k1 = other.keys[i1];
            }
        }

        // Copy in the remaining contents from other
        if i0 == len0 {
            self.containers.extend_from_slice(&other.containers[i1..]);
            self.keys.extend_from_slice(&other.keys[i1..])
        }
    }

    /// Same as [`and`] but operates in place on `self`
    /// 
    /// [`and`]: RoaringBitmap::and
    pub fn inplace_and(&mut self, other: &Self) {
        let mut i0 = 0;
        let mut i1 = 0;

        while i0 < self.keys.len() && i1 < other.keys.len() {
            let k0 = self.keys[i0];
            let k1 = other.keys[i1];

            if k0 == k1 {
                let is_empty = {
                    let c0 = &mut self.containers[i0];
                    let c1 = &other.containers[i1];

                    c0.inplace_and(c1);
                    c0.is_empty()
                };
                
                if is_empty {
                    self.containers.remove(i0);
                    self.keys.remove(i0);
                }
                else {
                    i0 += 1;
                    i1 += 1;
                }
            }
            // Remove any elements in self not shared between the bitmaps
            else if k0 < k1 {
                // The containers after the drained ones move down to `i0`
                let iend = array_ops::advance_until(&self.keys, i0, k1);

                self.containers.drain(i0..iend);
                self.keys.drain(i0..iend);
            }
            // Skip past all elements not in ourself
            else {
                i1 = array_ops::advance_until(&other.keys, i1, k0);
            }
        }

        // Other ran out of elements, remove the remainder from self since
        // they obviously don't intersect
        self.containers.truncate(i0);
        self.keys.truncate(i0);
    }

    /// Same as [`and_not`] but operates in place on `self`
    /// 
    /// [`and_not`]: RoaringBitmap::and_not
    pub fn inplace_and_not(&mut self, other: &Self) {
        // If either is the empty set then there are no chanegs to be made
        if self.is_empty() || other.is_empty() {
            return;
        }
        
        let mut len0 = self.keys.len();
        let len1 = other.keys.len();
        
        let mut i0 = 0;
        let mut i1 = 0;
        
        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];
            let k1 = other.keys[i1];
            
            // Key exists in both bitmaps, compare containers
            if k0 == k1 {
                let is_empty = {
                    let c0 = &mut self.containers[i0];
                    let c1 = &other.containers[i1];

                    c0.inplace_and_not(c1);
                    c0.is_empty()
                };
                
                // Remove the container if it was emptied
                if is_empty {
                    self.containers.remove(i0);
                    self.keys.remove(i0);
                    
                    len0 -= 1;
                }
                else {
                    i0 += 1;
                    i1 += 1;
                }
            }
            // Keys don't exist in the other bitmap, keep them
            else if k0 < k1 {
                i0 = array_ops::advance_until(&self.keys, i0, k1);
            }
            // Keys don't exist in this bitmap, skip
            else {
                i1 = array_ops::advance_until(&other.keys, i1, k0);
            }
        }
    }
    
    /// Same as [`xor`] but operates in place on `self`
    /// 
    /// [`xor`]: RoaringBitmap::xor
    pub fn inplace_xor(&mut self, other: &Self) {
        // No items in other, self is unchanged
        if other.is_empty() {
            return;
        }

        // Self is empty, we contain everything in other
        if self.is_empty() {
            self.clear();
            self.copy_from(other);
            return;
        }

        let mut len0 = self.keys.len();
        let len1 = other.keys.len();

        let mut i0 = 0;
        let mut i1 = 0;
        while i0 < len0 && i1 < len1 {
            let k0 = self.keys[i0];
            let k1 = other.keys[i1];

            if k0 == k1 {
                let is_empty = {
                    let c0 = &mut self.containers[i0];
                    let c1 = &other.containers[i1];

                    c0.inplace_xor(c1);
                    c0.is_empty()
                };

                if is_empty {
                    self.containers.remove(i0);
                    self.keys.remove(i0);

                    len0 -= 1;
                }
                else {
                    i0 += 1;
                }

                i1 += 1;
            }
            else if k0 < k1 {
                i0 += 1;
            }
            else {
                let c1 = &other.containers[i1];

                self.containers.insert(i0, c1.clone());
                self.keys.insert(i0, k1);

                i0 += 1;
                i1 += 1;
                len0 += 1;
            }
        }

        if i0 == len0 {
            self.containers.extend_from_slice(&other.containers[i1..]);
            self.keys.extend_from_slice(&other.keys[i1..]);
        }
    }

    /// Same as [`not`] but operates in place on `self`
    ///
    /// [`not`]: RoaringBitmap::not
    pub fn inplace_not<R: RangeBounds<u32>>(&mut self, range: R) {
        let (first, last) = match inclusive_bounds(range) {
            Some(bounds) => bounds,
            None => return
        };
        let (first_key, last_key) = (first >> 16, last >> 16);

        // Rebuild the key and container lists, moving the containers outside the range and
        // flipping the ones inside it in place
        let keys = mem::take(&mut self.keys);
        let containers = mem::take(&mut self.containers);
        let mut existing = keys.into_iter().zip(containers).peekable();

        while let Some((key, container)) = existing.next_if(|(key, _)| u32::from(*key) < first_key) {
            self.keys.push(key);
            self.containers.push(container);
        }

        for key in first_key..=last_key {
            let range = key_range(key, first, last);
            let flipped = match existing.next_if(|(existing, _)| u32::from(*existing) == key) {
                Some((_, container)) => container.inplace_not(range),
                None => Container::from_range(range)
            };

            if !flipped.is_empty() {
                self.keys.push(key as u16);
                self.containers.push(flipped);
            }
        }

        for (key, container) in existing {
            self.keys.push(key);
            self.containers.push(container);
        }
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
    fn get_index(&self, x: u16) -> Result<usize, usize> {
        self.keys.binary_search(&x)
    }
    
    /// Get an iterator over the values of the bitmap
    pub fn iter(&self) -> Iter {
        let iter = {
            if !self.containers.is_empty() {
                self.containers[0].iter()
            }
            else {
                container::Iter::empty()
            }
        };
        
        Iter {
            containers: &self.containers,
            keys: &self.keys,
            iter,
            index: 0
        }
    }
}

// Serialization

/// An error that occured while deserializing a bitmap
#[derive(Debug)]
pub enum DeserializeError {
    /// An invalid cookie was detected. This is likely not a bitmap. Contains the found value
    InvalidCookie(u32),

    /// An invalid container count was detected. Contains the value found
    InvalidContainerCount(u32),

    /// An IO error occured during deserialization, Contains the underlying error
    IoError(io::Error)
}

impl fmt::Display for DeserializeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeserializeError::InvalidCookie(cookie) => write!(f, "Invalid cookie: {}", cookie),
            DeserializeError::InvalidContainerCount(count) => write!(f, "Invalid container count: {}", count),
            DeserializeError::IoError(err) => write!(f, "{}", err)
        }
    }
}

impl RoaringBitmap {
    // Constants denoted by the roaring bitmap format spec.
    // See https://github.com/RoaringBitmap/RoaringFormatSpec for details
    const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
    const SERIAL_COOKIE: u32 = 12347;
    const NO_OFFSET_THRESHOLD: u32 = 4;

    /// Get the serialized size of the bitmap
    pub fn serialized_size(&self) -> usize {
        let mut count = self.header_size();
        
        for c in self.containers.iter() {
            count += c.serialized_size();
        }
        
        count
    }
    
    /// Serialize the bitmap to a stream. The serialized bitmap is little endian encoded.
    ///
    /// # Returns
    /// The number of bytes written to the buffer
    #[cfg(target_endian = "little")]
    pub fn serialize<W: Write>(&self, buf: &mut W) -> io::Result<usize> {
        let mut start_offset;
        let mut bytes_written = 0;

        // Write the header
        let has_run = self.has_run();
        if has_run {
            let len       = self.containers.len();
            let len_bytes = (Self::SERIAL_COOKIE | (((len - 1) << 16) as u32)).to_le_bytes();

            bytes_written += write_counted(buf, &len_bytes)?;

            let s = (len + 7) / 8;
            let mut bitmap: Vec<u8> = vec![0; s];

            for (i, c) in self.containers.iter().enumerate() {
                if c.is_run() {
                    bitmap[i / 8] |= 1 << (i % 8);
                }
            }

            bytes_written += write_counted(buf, &bitmap)?;

            if (len as u32) < Self::NO_OFFSET_THRESHOLD {
                start_offset = 4 + 4 * len + s;
            }
            else {
                start_offset = 4 + 8 * len + s;
            }
        }
        else {
            bytes_written += write_counted(buf, &Self::SERIAL_COOKIE_NO_RUNCONTAINER.to_le_bytes())?;
            bytes_written += write_counted(buf, &(self.containers.len() as u32).to_le_bytes())?;

            let len = self.containers.len();
            start_offset = 4 + 4 + 4 * len + 4 * len;
        }

        let pass = self.keys.iter()
            .zip(self.containers.iter());

        // Write the keys and cardinality
        for (key, c) in pass {
            bytes_written += write_counted(buf, &key.to_le_bytes())?;
            bytes_written += write_counted(buf, &((c.cardinality() - 1) as u16).to_le_bytes())?;
        }

        // Write the container offsets if there's no run containers or we're above the no offset threshold
        if !has_run || (self.containers.len() as u32) >= Self::NO_OFFSET_THRESHOLD {
            for c in self.containers.iter() {
                bytes_written += write_counted(buf, &(start_offset as u32).to_le_bytes())?;
                start_offset  += c.serialized_size();
            }
        }

        for c in self.containers.iter() {
            bytes_written += c.serialize(buf)?;
        }

        Ok(bytes_written)
    }
    
    /// Deserialize a bitmap from a stream. The stream must be little endian encoded
    ///
    /// # Returns
    /// The deserialized bitmap
    #[cfg(target_endian = "little")]
    pub fn deserialize<R: Read + Seek>(buf: &mut R) -> Result<Self, DeserializeError> {
        // Read out the cookie and number of containers
        let (cookie, size) = {
            let mut cookie_bytes = [0; mem::size_of::<u32>()];
            buf.read_exact(&mut cookie_bytes)
                .map_err(DeserializeError::IoError)?;

            let cookie = u32::from_le_bytes(cookie_bytes);

            // Validate cookie
            if (cookie & 0xFFFF) != Self::SERIAL_COOKIE && cookie != Self::SERIAL_COOKIE_NO_RUNCONTAINER {
                return Err(DeserializeError::InvalidCookie(cookie));
            }

            // Deserialize the size
            let size = {
                if cookie & 0xFFFF == Self::SERIAL_COOKIE {
                    (cookie >> 16) + 1
                }
                else {
                    let mut size_bytes = [0; mem::size_of::<u32>()];
                    buf.read_exact(&mut size_bytes)
                        .map_err(DeserializeError::IoError)?;

                    u32::from_le_bytes(size_bytes)
                }
            };

            // Validate size
            if size > (1 << 16) {
                return Err(DeserializeError::InvalidContainerCount(size));
            }

            (cookie, size)
        };

        // Read out the bitmap if present
        let mut bitmap: Vec<u8> = Vec::new();

        let has_run = (cookie & 0xFFFF) == Self::SERIAL_COOKIE;
        if has_run {
            // Initialized, as a reader may read the buffer it fills
            bitmap.resize(((size + 7) / 8) as usize, 0);

            buf.read_exact(&mut bitmap)
                .map_err(DeserializeError::IoError)?;
        }

        // Setup the resulting bitmap
        let mut result = Self::with_capacity(size as usize);

        // Read out the keys into the bitmap and save the cards for later
        let mut cards = Vec::with_capacity(size as usize);
        for _i in 0..size {
            let mut key_bytes  = [0; mem::size_of::<u16>()];
            let mut card_bytes = [0; mem::size_of::<u16>()];

            buf.read_exact(&mut key_bytes)
                .map_err(DeserializeError::IoError)?;
            buf.read_exact(&mut card_bytes)
                .map_err(DeserializeError::IoError)?;


            let key  = u16::from_le_bytes(key_bytes);
            let card = u16::from_le_bytes(card_bytes);

            result.keys.push(key);
            cards.push(card);
        }

        // Bypass the offset header if necessary. 
        // This implementation doesn't support container streaming
        // so the offset header is ignored
        if !has_run || size >= Self::NO_OFFSET_THRESHOLD {
            let offset_header = i64::from(size * 4);

            buf.seek(SeekFrom::Current(offset_header))
                .map_err(DeserializeError::IoError)?;
        }

        // Load in the containers
        for i in 0..(size as usize) {
            let card = (cards[i] as usize) + 1;
            let (is_bitset, is_run) = {
                if has_run && bitmap[i / 8] & (1 << (i % 8)) != 0 {
                    (false, true)
                }
                else {
                    (card > DEFAULT_MAX_SIZE, false)
                }
            };

            // Container is a bitset
            if is_bitset {
                let bitset = BitsetContainer::deserialize(buf)
                    .map_err(DeserializeError::IoError)?;

                result.containers.push(Container::Bitset(bitset));
            }
            // Container is a run container
            else if is_run {
                let run = RunContainer::deserialize(buf)
                    .map_err(DeserializeError::IoError)?;
                
                result.containers.push(Container::Run(run));
            }
            // Container is an array
            else {
                let array = ArrayContainer::deserialize(card, buf)
                    .map_err(DeserializeError::IoError)?;

                result.containers.push(Container::Array(array));
            }
        }

        Ok(result)
    }

    /// Calculate the size of the serialized header for the bitmap
    fn header_size(&self) -> usize {
        let contains_run = self.has_run();
        
        let len = self.containers.len();
        if contains_run {
            if len < (Self::NO_OFFSET_THRESHOLD as usize) {
                4 + (len + 7) / 8 + 4 * len
            }
            else {
                4 + (len + 7) / 8 + 8 * len
            }
        }
        else {
            4 + 4 + 8 * len
        }
    }

    /// Check if the bitmap contains any run containers
    fn has_run(&self) -> bool {
        for c in self.containers.iter() {
            if c.is_run() {
                return true;
            }
        }
        
        false
    }
}

/// An iterator over a roaring bitmap
pub struct Iter<'a> {
    /// The containers we're iterating
    containers: &'a Vec<Container>,
    
    /// The keys we're iterating
    keys: &'a Vec<u16>,
    
    /// The container iterator we're currently iterating
    iter: container::Iter<'a>,
    
    /// The index of the container we're iterating
    index: usize
}

impl<'a> Iterator for Iter<'a> {
    type Item = u32;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Try to advance the container iterator
        let mut item = self.iter.next();

        // If we reached the end of the iterator try to advance to the next container
        if item.is_none() {
            let next_index = self.index + 1;
            if next_index < self.containers.len() {
                // Advance the index
                self.index = next_index;
                self.iter = self.containers[next_index].iter();
                
                // Get the next value
                item = self.iter.next();
            }
        }

        item.map(|low| {
            let key = self.keys[self.index];
            
            (u32::from(key) << 16) | u32::from(low)
        })
    }
}

/// The low bits of the values from `first` to `last` that fall in the container with `key`
fn key_range(key: u32, first: u32, last: u32) -> Range<u32> {
    let start = if key == first >> 16 { first & 0xFFFF } else { 0 };
    let end = if key == last >> 16 { (last & 0xFFFF) + 1 } else { 1 << 16 };

    start..end
}

#[cfg(test)]
mod test {
    use crate::RoaringBitmap;
    use crate::test::*;

    impl TestShim<u32> for RoaringBitmap {
        fn from_data(data: &[u32]) -> Self {
            Self::from_slice(data)
        }

        fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=u32> + 'a> {
            Box::new(self.iter())
        }

        fn card(&self) -> usize {
            self.cardinality()
        }
    }

    #[test]
    fn from_range() {
        let bitmap = RoaringBitmap::from_range(0..std::u32::MAX);

        assert_eq!(bitmap.cardinality(), std::u32::MAX as usize);
    }

    #[test]
    fn from_slice() {
        let input = generate_data(0..20_000_000, 500_000);
        let bitmap = RoaringBitmap::from_slice(&input);

        assert_eq!(bitmap.len(), input.len());

        for (found, expected) in bitmap.iter().zip(input.iter()) {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn copy_from() {
        let input = generate_data(0..20_000_000, 500_000);
        let bitmap_a = RoaringBitmap::from_slice(&input);

        let mut bitmap_b = RoaringBitmap::new();
        bitmap_b.copy_from(&bitmap_a);

        assert_eq!(bitmap_a.len(), bitmap_b.len());

        let iter = bitmap_a.iter()
            .zip(bitmap_b.iter());

        for (a, b) in iter {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn add() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.add(10);
        bitmap.add(255678);

        assert_eq!(bitmap.cardinality(), 2);
        assert!(bitmap.contains(10));
        assert!(bitmap.contains(255678));
    }

    #[test]
    fn add_range() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(0..1_000_000);

        assert_eq!(bitmap.cardinality(), 1_000_000);
    }

    #[test]
    fn add_slice() {
        let input = generate_data(0..20_000_000, 500_000);
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_slice(&input);

        assert_eq!(bitmap.len(), input.len());

        for (found, expected) in bitmap.iter().zip(input.iter()) {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn remove() {
        let input = generate_data(0..20_000_000, 500_000);
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_slice(&input);
        
        // Remove the first 100 elements from the bitmap
        for i in 0..100 {
            bitmap.remove(input[i]);
        }

        assert_eq!(bitmap.len(), input.len() - 100);

        for (found, expected) in bitmap.iter().zip(input[100..].iter()) {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn remove_range() {
        const MIN: u32 = 5_000_000;
        const MAX: u32 = 10_000_000;

        let input = generate_data(0..20_000_000, 500_000);
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_slice(&input);

        // Remove the first all elements in [min-max)
        bitmap.remove_range(MIN..MAX);

        let mut result = Vec::with_capacity(input.len());
        for value in input.iter() {
            if *value < MIN || *value >= MAX {
                result.push(*value);
            }
        }

        assert_eq!(bitmap.cardinality(), result.len());

        let iter = bitmap.iter()
            .zip(result.iter());

        for (found, expected) in iter {
            assert_eq!(
                found,
                *expected,
                "Missmatched elements. Found {:?}, expected {:?}",
                found,
                *expected
            );
        }
    }

    #[test]
    fn remove_slice() {
        let input = generate_data(0..20_000_000, 500_000);
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_slice(&input);
        
        // Remove the first 100 elements from the bitmap
        bitmap.remove_slice(&input[..100]);

        assert_eq!(bitmap.len(), input.len() - 100);

        for (found, expected) in bitmap.iter().zip(input[100..].iter()) {
            assert_eq!(found, *expected);
        }
    }

    #[test]
    fn contains() {
        let input = generate_data(0..20_000_000, 500_000);
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_slice(&input);

        for value in &input[..100] {
            assert!(bitmap.contains(*value));
        }
    }

    #[test]
    fn contains_range() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(0..10_000);

        assert!(bitmap.contains_range(1000..6000));
    }

    #[test]
    fn cardinality() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(0..5_000);

        assert_eq!(bitmap.cardinality(), 5000);
    }

    #[test]
    fn select() {
        let input = generate_data(0..20_000_000, 500_000);
        let bitmap = RoaringBitmap::from_slice(&input);

        let exp_value = input[100];
        let rank = 100;
        
        // Oddly they're not self consistent. Rank and Select should be the exact inverse
        // of each other but select is about 1 ahead mirroring their implementation
        let value = bitmap.select(rank);

        assert!(value.is_some());
        assert_eq!(value.unwrap(), exp_value);
    }

    #[test]
    fn rank() {
        let input = generate_data(0..20_000_000, 500_000);
        let bitmap = RoaringBitmap::from_slice(&input);

        let value = input[99];
        let exp_rank = 100;

        let rank = bitmap.rank(value);

        assert_eq!(rank, exp_rank);
    }

    #[test]
    fn min() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(0..5_000);

        let min = bitmap.min();
        assert!(min.is_some());
        assert_eq!(min.unwrap(), 0);
    }

    #[test]
    fn max() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(0..=5_000);

        let max = bitmap.max();
        assert!(max.is_some());
        assert_eq!(max.unwrap(), 5_000);
    }

    #[test]
    fn subset_of() {
        let input = generate_data(0..20_000_000, 500_000);
        let mut a = RoaringBitmap::new();
        let mut b = RoaringBitmap::new();

        a.add_slice(&input);
        b.add_slice(&input[0..(input.len() / 2)]);

        assert!(b.subset_of(&a));
        assert!(!a.subset_of(&b));
    }

    #[test]
    fn round_trip_serialize() {
        let input = generate_data(0..20_000_000, 500_000);
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_slice(&input);

        // Serialize and verify
        let num_bytes = bitmap.serialized_size();
        let mut output = Vec::<u8>::with_capacity(num_bytes);
        let num_written = bitmap.serialize(&mut output);

        assert!(num_written.is_ok());
        assert_eq!(num_written.unwrap(), num_bytes);
        
        // Deserialize
        let mut cursor = std::io::Cursor::new(&output);
        let deserialized = RoaringBitmap::deserialize(&mut cursor);

        assert!(deserialized.is_ok());
        
        let deserialized = deserialized.unwrap();
        let iter = deserialized.iter()
            .zip(bitmap.iter());

        for (found, expected) in iter {
            assert_eq!(found, expected);
        }
    } 

    #[test]
    fn round_trip_serialize_bitset_in_the_retain_margin() {
        // A bitset that popping brought to 3897 values, which a reader takes for an array
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(0..4_097);
        for _ in 0..200 {
            bitmap.pop_min();
        }
        bitmap.add(100_000);

        let mut output = Vec::new();
        let num_written = bitmap.serialize(&mut output).unwrap();
        assert_eq!(num_written, bitmap.serialized_size());
        assert_eq!(num_written, output.len());

        let deserialized = RoaringBitmap::deserialize(&mut std::io::Cursor::new(&output)).unwrap();
        assert_eq!(deserialized.len(), bitmap.len());
        assert!(deserialized.iter().eq(bitmap.iter()));
    }

    #[test]
    fn or() {
        op_test::<RoaringBitmap, RoaringBitmap, u32, _, RoaringBitmap>(
            OpType::Or, |a, b| a.or(&b)
        );
    }

    #[test]
    fn and() {
        op_test::<RoaringBitmap, RoaringBitmap, u32, _, RoaringBitmap>(
            OpType::And, |a, b| a.and(&b)
        );
    }

    #[test]
    fn and_not() {
        op_test::<RoaringBitmap, RoaringBitmap, u32, _, RoaringBitmap>(
            OpType::AndNot, |a, b| a.and_not(&b)
        );
    }

    #[test]
    fn xor() {
        op_test::<RoaringBitmap, RoaringBitmap, u32, _, RoaringBitmap>(
            OpType::Xor, |a, b| a.xor(&b)
        );
    }

    #[test]
    fn not() {
        let input = generate_data(0..20_000_000, 500_000);
        let bitmap = RoaringBitmap::from_slice(&input);
        let not_bitmap = bitmap.not(..);

        assert_eq!(not_bitmap.cardinality(), (1 << 32) - bitmap.cardinality());
    }

    #[test]
    fn inplace_or() {
        op_test::<RoaringBitmap, RoaringBitmap, u32, _, RoaringBitmap>(
            OpType::Or, |mut a, b| { a.inplace_or(&b); a }
        );
    }

    #[test]
    fn inplace_and() {
        op_test::<RoaringBitmap, RoaringBitmap, u32, _, RoaringBitmap>(
            OpType::And, |mut a, b| { a.inplace_and(&b); a }
        );
    }

    #[test]
    fn inplace_and_not() {
        op_test::<RoaringBitmap, RoaringBitmap, u32, _, RoaringBitmap>(
            OpType::AndNot, |mut a, b| { a.inplace_and_not(&b); a }
        );
    }

    #[test]
    fn inplace_xor() {
        op_test::<RoaringBitmap, RoaringBitmap, u32, _, RoaringBitmap>(
            OpType::Xor, |mut a, b| { a.inplace_xor(&b); a }
        );
    }

    #[test]
    fn inplace_not() {
        let input = generate_data(0..20_000_000, 500_000);
        let mut bitmap = RoaringBitmap::from_slice(&input);
        bitmap.inplace_not(..);

        assert_eq!(bitmap.cardinality(), (1 << 32) - input.len());
    }

    #[test]
    fn or_cardinality() {
        op_card_test::<RoaringBitmap, RoaringBitmap, u32, _>(
            OpType::Or, |a, b| a.or_cardinality(&b)
        );
    }

    #[test]
    fn and_cardinality() {
        op_card_test::<RoaringBitmap, RoaringBitmap, u32, _>(
            OpType::And, |a, b| a.and_cardinality(&b)
        );
    }

    #[test]
    fn and_not_cardinality() {
        op_card_test::<RoaringBitmap, RoaringBitmap, u32, _>(
            OpType::AndNot, |a, b| a.and_not_cardinality(&b)
        );
    }

    #[test]
    fn xor_cardinality() {
        op_card_test::<RoaringBitmap, RoaringBitmap, u32, _>(
            OpType::Xor, |a, b| a.xor_cardinality(&b)
        );
    }

    #[test]
    fn external_data() {
        use std::fs::File;
        
        const FILE_PATHS: [(&'static str, bool); 9] = [
            ("res/test_data/no_runs.bin", true),
            ("res/test_data/with_runs.bin", true),
            ("res/test_data/crash_prone_0.bin", false),
            ("res/test_data/crash_prone_1.bin", false),
            ("res/test_data/crash_prone_2.bin", false),
            ("res/test_data/crash_prone_3.bin", false),
            ("res/test_data/crash_prone_4.bin", false),
            ("res/test_data/crash_prone_5.bin", false),
            ("res/test_data/crash_prone_6.bin", false)
        ];

        fn run_test(test: (&'static str, bool)) {
            match File::open(test.0) {
                Ok(mut file) => {
                    let bitmap = RoaringBitmap::deserialize(&mut file);

                    if test.1 {
                        assert!(bitmap.is_ok(), "Failed on '{}' with '{}'", test.0, bitmap.unwrap_err());
                    }
                    else {
                        assert!(bitmap.is_err(), "Failed on '{}'", test.0);
                    }
                },
                Err(error) => {
                    assert!(false, "Failed on '{}' with '{}'", test.0, error);
                }
            }
        }

        for test in FILE_PATHS.iter() {
            run_test(*test);
        }
    }

    #[test]
    fn add_range_empty_is_noop() {
        // An empty range adds nothing, whether or not a container exists at its key
        let mut empty = RoaringBitmap::new();
        empty.add_range(0..0);
        empty.add_range(5..5);
        empty.add_range(65_536..65_536);
        empty.add_range(10..5);
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);

        let mut bitmap = RoaringBitmap::from_slice(&[7, 100_000]);
        bitmap.add_range(0..0);
        bitmap.add_range(5..5);
        bitmap.add_range(100_000..100_000);
        bitmap.add_range(65_536..65_536);
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![7, 100_000]);
    }

    #[test]
    fn add_range_container_boundaries() {
        // A range ending exactly on a container boundary must not touch the next container
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(0..65_536);
        assert_eq!(bitmap.len(), 65_536);
        assert_eq!(bitmap.max(), Some(65_535));

        // A range straddling a boundary, over existing containers
        let mut bitmap = RoaringBitmap::from_slice(&[1, 65_537, 200_000]);
        bitmap.add_range(65_530..65_540);
        let expected: Vec<u32> = [1, 200_000].iter().copied()
            .chain(65_530..65_540)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn add_range_onto_existing_array() {
        // A range joining an array container must stay sorted and deduplicated
        let mut bitmap = RoaringBitmap::from_slice(&[3, 5, 9, 70]);
        bitmap.add_range(4..10);
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![3, 4, 5, 6, 7, 8, 9, 70]);

        // and past the array limit becomes a bitset with the right contents
        let mut bitmap = RoaringBitmap::from_slice(&[1, 3, 5]);
        bitmap.add_range(1_000..9_000);
        assert_eq!(bitmap.len(), 8_003);
        assert!(bitmap.contains(3) && bitmap.contains(1_000) && bitmap.contains(8_999));
        assert!(!bitmap.contains(999) && !bitmap.contains(9_000));
    }

    #[test]
    fn remove_emptied_container_keeps_the_others() {
        let mut bitmap = RoaringBitmap::from_slice(&[5, 70_000, 200_000]);
        bitmap.remove(5);
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![70_000, 200_000]);

        bitmap.remove(70_000);
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![200_000]);
        assert!(bitmap.contains(200_000));
        assert!(!bitmap.contains(70_000));

        bitmap.remove(200_000);
        assert!(bitmap.is_empty());
    }

    #[test]
    fn remove_reports_whether_the_value_was_present() {
        let mut bitmap = RoaringBitmap::from_slice(&[5, 70_000]);
        bitmap.add_range(200_000..210_000);

        assert!(bitmap.remove(5));
        assert!(!bitmap.remove(5));
        assert!(!bitmap.remove(6));
        assert!(!bitmap.remove(70_001));
        assert!(!bitmap.remove(130_000));
        assert!(bitmap.remove(70_000));
        assert!(bitmap.remove(205_000));
        assert!(!bitmap.remove(205_000));
        assert_eq!(bitmap.len(), 9_999);
    }

    #[test]
    fn pop_min_drains_in_order() {
        let mut bitmap = RoaringBitmap::new();
        assert_eq!(bitmap.pop_min(), None);

        // An array container, a bitset container and a container of one
        bitmap.add_range(3..10);
        bitmap.add_range(70_000..80_000);
        bitmap.add(400_000);
        let expected: Vec<u32> = bitmap.iter().collect();

        let mut popped = Vec::new();
        while let Some(value) = bitmap.pop_min() {
            popped.push(value);
            assert_eq!(bitmap.len(), expected.len() - popped.len());
        }

        assert_eq!(popped, expected);
        assert!(bitmap.is_empty());
        assert_eq!(bitmap.pop_min(), None);
    }

    #[test]
    fn remove_slice_emptied_container_keeps_the_others() {
        let mut bitmap = RoaringBitmap::from_slice(&[5, 6, 70_000, 200_000]);
        bitmap.remove_slice(&[5, 6]);
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![70_000, 200_000]);
    }

    /// Random add, remove, pop and range operations against a `BTreeSet`, steering the first
    /// container across the array/bitset threshold.
    #[test]
    fn churn_matches_model() {
        use std::collections::BTreeSet;

        struct Rng(u64);
        impl Rng {
            fn next(&mut self) -> u64 {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                self.0
            }

            fn below(&mut self, n: u64) -> u64 {
                (self.next() >> 11) % n
            }
        }

        fn check(bitmap: &RoaringBitmap, model: &BTreeSet<u32>) {
            assert_eq!(bitmap.len(), model.len());
            assert_eq!(bitmap.is_empty(), model.is_empty());
            assert_eq!(bitmap.min(), model.iter().next().copied());
            assert_eq!(bitmap.max(), model.iter().next_back().copied());
            assert!(bitmap.iter().eq(model.iter().copied()));
        }

        // Keys 0 (hovering around the array/bitset threshold), 2 and 7 (sparse)
        fn pick(rng: &mut Rng) -> u32 {
            match rng.below(8) {
                0..=4 => rng.below(8_192) as u32,
                5 | 6 => (2 << 16) | rng.below(300) as u32,
                _ => (7 << 16) | rng.below(65_536) as u32
            }
        }

        for seed in 1..=4u64 {
            let mut rng = Rng(0x2545_F491_4F6C_DD1D ^ seed);
            let mut bitmap = RoaringBitmap::new();
            let mut model = BTreeSet::new();

            // Steer the cardinality of the first container up and down across 4096
            let mut growing = true;
            let mut peak = 0;
            let mut trough = usize::MAX;
            for step in 0..40_000 {
                if step % 64 == 0 {
                    let low = model.range(..8_192).count();
                    peak = peak.max(low);
                    if peak > 4_096 { trough = trough.min(low); }
                    if low > 4_400 { growing = false; }
                    if low < 3_800 { growing = true; }
                }

                let add_bias = if growing { 7 } else { 3 };

                match rng.below(20) {
                    0..=9 => {
                        let value = pick(&mut rng);
                        if rng.below(10) < add_bias {
                            bitmap.add(value);
                            model.insert(value);
                        }
                        else {
                            assert_eq!(bitmap.remove(value), model.remove(&value));
                        }

                        assert_eq!(bitmap.contains(value), model.contains(&value));
                    },
                    10..=13 => {
                        // Remove the minimum, the allocator's pop
                        let min = model.iter().next().copied();
                        assert_eq!(bitmap.min(), min);
                        assert_eq!(bitmap.pop_min(), min);
                        if let Some(min) = min {
                            model.remove(&min);
                        }
                    },
                    14..=15 => {
                        let probe = pick(&mut rng);
                        assert_eq!(bitmap.contains(probe), model.contains(&probe));
                    },
                    16..=17 => {
                        // Small ranges, sometimes empty, sometimes over a container boundary
                        let start = if rng.below(4) == 0 {
                            (rng.below(9) as u32 * 65_536).saturating_sub(rng.below(4) as u32 % 3)
                        }
                        else {
                            pick(&mut rng)
                        };
                        let start = start.min(10 << 16);
                        // Keep ranges out of the steered container while it shrinks
                        let start = if !growing && start < 8_192 { start + (3 << 16) } else { start };
                        let len = rng.below(40) as u32;

                        bitmap.add_range(start..start + len);
                        model.extend(start..start + len);
                    },
                    _ => {
                        // Take, the allocator's reserve of a specific id
                        let value = pick(&mut rng);
                        assert_eq!(bitmap.remove(value), model.remove(&value));
                    }
                }

                if step % 1_000 == 0 {
                    check(&bitmap, &model);
                }
            }

            check(&bitmap, &model);

            // The first container crossed the array limit in both directions, and the margin
            assert!(peak > 4_096 && trough < 3_840, "peak {} trough {}", peak, trough);
        }
    }

    #[test]
    fn inplace_or_with_disjoint_keys() {
        let mut a = RoaringBitmap::from_slice(&[1, (2 << 16) | 5]);
        let b = RoaringBitmap::from_slice(&[(1 << 16) | 7, (2 << 16) | 9]);
        a.inplace_or(&b);

        assert_eq!(a.iter().collect::<Vec<_>>(), vec![1, (1 << 16) | 7, (2 << 16) | 5, (2 << 16) | 9]);
        assert_eq!(a.len(), 4);
    }

    #[test]
    fn inplace_or_into_array_at_the_limit() {
        // 4 096 values is the array limit, not a full container
        let even: Vec<u32> = (0..4_096).map(|i| i * 2).collect();
        let mut a = RoaringBitmap::from_slice(&even);
        a.inplace_or(&RoaringBitmap::from_slice(&[1]));

        assert!(a.contains(1));
        assert_eq!(a.len(), 4_097);
    }

    /// Two disjoint arrays of 3 000 values, whose union of 6 000 must be stored as a bitset
    fn arrays_past_the_limit() -> (RoaringBitmap, RoaringBitmap) {
        let a = RoaringBitmap::from_slice(&(0..3_000).map(|i| i * 2).collect::<Vec<_>>());
        let b = RoaringBitmap::from_slice(&(0..3_000).map(|i| i * 2 + 1).collect::<Vec<_>>());

        (a, b)
    }

    fn assert_round_trips(bitmap: &RoaringBitmap) {
        let mut buf = Vec::new();
        bitmap.serialize(&mut buf).unwrap();
        let read = RoaringBitmap::deserialize(&mut std::io::Cursor::new(&buf)).unwrap();

        assert!(read.iter().eq(bitmap.iter()));
    }

    #[test]
    fn or_past_the_array_limit_round_trips() {
        let (a, b) = arrays_past_the_limit();
        let union = a.or(&b);

        assert!(union.iter().eq(0..6_000));
        assert_round_trips(&union);
    }

    #[test]
    fn inplace_or_past_the_array_limit_round_trips() {
        let (mut a, b) = arrays_past_the_limit();
        a.inplace_or(&b);

        assert!(a.iter().eq(0..6_000));
        assert_round_trips(&a);
    }

    #[test]
    fn xor_past_the_array_limit_round_trips() {
        let (a, b) = arrays_past_the_limit();
        let difference = a.xor(&b);

        assert!(difference.iter().eq(0..6_000));
        assert_round_trips(&difference);
    }

    #[test]
    fn inplace_xor_past_the_array_limit_round_trips() {
        let (mut a, b) = arrays_past_the_limit();
        a.inplace_xor(&b);

        assert!(a.iter().eq(0..6_000));
        assert_round_trips(&a);
    }

    #[test]
    fn inplace_xor_drops_the_key_of_an_emptied_container() {
        let mut a = RoaringBitmap::from_slice(&[1]);
        let b = RoaringBitmap::from_slice(&[1, (1 << 16) | 1]);
        a.inplace_xor(&b);

        assert!(a.iter().eq([(1 << 16) | 1]));
    }

    #[test]
    fn subset_of_compares_matching_containers() {
        let a = RoaringBitmap::from_slice(&[3, (1 << 16) | 5]);
        let b = RoaringBitmap::from_slice(&[3, (1 << 16) | 6]);

        assert!(!a.subset_of(&b));
        assert!(!b.subset_of(&a));
        assert!(a.subset_of(&RoaringBitmap::from_slice(&[3, 4, (1 << 16) | 5])));
    }

    #[test]
    fn add_slice_keeps_keys_sorted() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.add_slice(&[(2 << 16) | 1, 5, (1 << 16) | 3, 6]);

        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![5, 6, (1 << 16) | 3, (2 << 16) | 1]);
        assert!(bitmap.contains(5) && bitmap.contains((1 << 16) | 3) && bitmap.contains((2 << 16) | 1));
        assert_eq!(bitmap.min(), Some(5));
        assert_eq!(bitmap.max(), Some((2 << 16) | 1));
    }

    #[test]
    fn contains_range_across_containers() {
        let mut full = RoaringBitmap::from_range(0..(4 << 16));
        assert!(full.contains_range(10..(2 << 16) + 10));
        assert!(full.contains_range(0..(1 << 16)));
        assert!(full.contains_range(5..(4 << 16)));
        assert!(!full.contains_range(5..(4 << 16) + 1));
        assert!(full.contains_range(7..7));

        // A hole in the second of two middle containers
        full.remove((2 << 16) | 5);
        assert!(!full.contains_range(10..(3 << 16) + 10));
        assert!(full.contains_range(10..(2 << 16) + 5));

        // The last container ends inside the range
        let partial = RoaringBitmap::from_range(0..(1 << 16) + 100);
        assert!(partial.contains_range(10..(1 << 16) + 50));
        assert!(!partial.contains_range(10..(1 << 16) + 200));
    }

    #[test]
    fn remove_range_keeps_the_end() {
        let mut a = RoaringBitmap::from_range(0..20);
        a.remove_range(0..10);
        assert!(a.iter().eq(10..20));

        // A range ending on a container boundary leaves the next container alone
        let mut b = RoaringBitmap::from_range(0..(2 << 16));
        b.remove_range(5..(1 << 16));
        assert!(b.iter().eq((0..5).chain((1 << 16)..(2 << 16))));

        let mut c = RoaringBitmap::from_range(0..20);
        c.remove_range(8..8);
        assert!(c.iter().eq(0..20));
    }

    #[test]
    fn range_apis_reach_u32_max() {
        let top = u32::MAX - 2..=u32::MAX;

        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(top.clone());
        assert!(bitmap.iter().eq(top.clone()));
        assert!(bitmap.contains_range(top.clone()));
        assert!(bitmap.contains_range(u32::MAX..));
        assert!(!bitmap.contains_range(u32::MAX - 3..));

        assert!(RoaringBitmap::from_range(top.clone()).iter().eq(top.clone()));
        assert_eq!(RoaringBitmap::from_range(u32::MAX - 70_000..).cardinality(), 70_001);
        assert!(RoaringBitmap::from_range(100..200_000).iter().eq(100..200_000));

        bitmap.remove_range(u32::MAX..);
        assert!(bitmap.iter().eq(u32::MAX - 2..u32::MAX));

        let mut bitmap = RoaringBitmap::new();
        bitmap.add_range(u32::MAX - 1..);
        assert!(bitmap.iter().eq(u32::MAX - 1..=u32::MAX));

        bitmap.remove_range(..);
        assert!(bitmap.is_empty());
    }

    #[test]
    fn not_flips_only_the_range() {
        let mut bitmap = RoaringBitmap::new();
        for value in [1, (1 << 16) | 5, (3 << 16) | 7] {
            bitmap.add(value);
        }

        let expected: Vec<u32> = std::iter::once(1)
            .chain((1 << 16)..(2 << 16)).filter(|&v| v != (1 << 16) | 5)
            .chain(std::iter::once((3 << 16) | 7))
            .collect();

        // The end key is absent from the bitmap, the containers after it stay
        let not = bitmap.not((1 << 16)..(2 << 16));
        assert!(not.iter().eq(expected.iter().cloned()));

        let mut inplace = bitmap.clone();
        inplace.inplace_not((1 << 16)..(2 << 16));
        assert!(inplace.iter().eq(expected.iter().cloned()));

        // An empty range changes nothing
        assert!(bitmap.not(5..5).iter().eq(bitmap.iter()));

        // Flipping a container's only values removes it
        let mut inplace = bitmap.clone();
        inplace.inplace_not(1..2);
        assert_eq!(inplace.keys, [1, 3]);
        assert!(bitmap.not(1..2).iter().eq(bitmap.iter().skip(1)));

        // Ranges that reach u32::MAX
        let mut top = RoaringBitmap::new();
        top.add(u32::MAX);
        assert!(top.not(u32::MAX - 1..).iter().eq(std::iter::once(u32::MAX - 1)));
        top.inplace_not(u32::MAX - 1..=u32::MAX);
        assert!(top.iter().eq(std::iter::once(u32::MAX - 1)));
    }

    #[test]
    fn serialize_round_trips_through_byte_at_a_time_streams() {
        use std::io::{Cursor, Read, Seek, SeekFrom, Write};

        // Takes at most one byte per call, as a pipe or socket may
        struct Trickle<T>(T);

        impl<T: Write> Write for Trickle<T> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.write(&buf[..buf.len().min(1)])
            }

            fn flush(&mut self) -> std::io::Result<()> {
                self.0.flush()
            }
        }

        impl<T: Read> Read for Trickle<T> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let len = buf.len().min(1);
                self.0.read(&mut buf[..len])
            }
        }

        impl<T: Seek> Seek for Trickle<T> {
            fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
                self.0.seek(position)
            }
        }

        // A bitmap with a run container: cookie with one container, the run flag, key 0 with
        // cardinality 6, and one run of 10..=15
        let mut runs = Vec::new();
        for word in [12_347_u16, 0, 1, 0, 5, 1, 10, 5] {
            runs.extend_from_slice(&word.to_le_bytes());
        }
        runs.remove(5);

        let mut bitmap = RoaringBitmap::deserialize(&mut Trickle(Cursor::new(&runs))).unwrap();
        assert!(bitmap.iter().eq(10..16));

        bitmap.add_range(1 << 16..(1 << 16) + 5_000);
        bitmap.add_range(3 << 16..(3 << 16) + 50);

        let mut output = Vec::new();
        let num_written = bitmap.serialize(&mut Trickle(&mut output)).unwrap();
        assert_eq!(num_written, bitmap.serialized_size());
        assert_eq!(output.len(), num_written);

        let read = RoaringBitmap::deserialize(&mut Trickle(Cursor::new(&output))).unwrap();
        assert!(read.iter().eq(bitmap.iter()));
    }

    #[test]
    fn inplace_and_keeps_shared_keys_after_unshared_ones() {
        let mut a = RoaringBitmap::new();
        let mut b = RoaringBitmap::new();
        for key in [0, 1, 2, 5, 7] {
            a.add(key << 16);
            a.add((key << 16) | 1);
        }
        for key in [2, 5, 9] {
            b.add(key << 16);
        }

        a.inplace_and(&b);
        assert!(a.iter().eq([2 << 16, 5 << 16]));
    }

    /// Random bitmaps of array, bitset and run containers against a set of the values they
    /// hold, operation by operation. Values sit in a few low keys and the two highest keys, and
    /// cluster at container and word edges
    #[test]
    fn bitmap_ops_match_a_model() {
        use rand::prelude::*;
        use std::collections::BTreeSet;
        use std::io::Cursor;
        use std::panic::{self, AssertUnwindSafe};

        type Model = BTreeSet<u32>;
        const KEYS: [u32; 6] = [0, 1, 2, 3, 0xFFFE, 0xFFFF];

        fn edge_low(rng: &mut SmallRng) -> u32 {
            match rng.random_range(0..4) {
                0 => rng.random_range(0..64),
                1 => 0xFFFF - rng.random_range(0..64),
                2 => rng.random_range(0..1024) * 64 + rng.random_range(0..2) * 63,
                _ => rng.random_range(0..(1 << 16))
            }
        }

        fn edge_value(rng: &mut SmallRng) -> u32 {
            (KEYS[rng.random_range(0..KEYS.len())] << 16) | edge_low(rng)
        }

        fn keys(rng: &mut SmallRng) -> Vec<u32> {
            KEYS.iter().copied().filter(|_| rng.random_bool(0.6)).collect()
        }

        // Up to 4 096 values per key make arrays, more make bitsets
        fn sets(rng: &mut SmallRng) -> (RoaringBitmap, Model) {
            let mut model = Model::new();
            for key in keys(rng) {
                let count = match rng.random_range(0..3) {
                    0 => rng.random_range(1..100),
                    1 => rng.random_range(3_800..4_400),
                    _ => rng.random_range(4_000..30_000)
                };
                for _ in 0..count {
                    model.insert((key << 16) | if rng.random_bool(0.3) { edge_low(rng) } else { rng.random_range(0..(1 << 16)) });
                }
            }

            let mut bitmap = RoaringBitmap::new();
            for &value in &model {
                bitmap.add(value);
            }

            (bitmap, model)
        }

        // Run containers only come from deserializing, so write the portable format by hand
        fn runs(rng: &mut SmallRng) -> (RoaringBitmap, Model) {
            let mut model = Model::new();
            let mut containers = Vec::new();
            for key in keys(rng) {
                let mut runs = Vec::new();
                let mut start = if rng.random_bool(0.3) { 0 } else { rng.random_range(0..2_000) };
                while start < (1 << 16) {
                    let len = rng.random_range(1..3_000).min((1 << 16) - start);
                    let len = if rng.random_range(0..6) == 0 { (1 << 16) - start } else { len };
                    runs.push((start as u16, (len - 1) as u16));
                    model.extend(((key << 16) | start)..=((key << 16) | (start + len - 1)));
                    start += len + rng.random_range(1..5_000);
                }
                containers.push((key as u16, runs));
            }

            let mut bytes = Vec::new();
            if containers.is_empty() {
                bytes.extend_from_slice(&12_346_u32.to_le_bytes());
                bytes.extend_from_slice(&0_u32.to_le_bytes());
            }
            else {
                let n = containers.len();
                bytes.extend_from_slice(&(12_347 | ((n as u32 - 1) << 16)).to_le_bytes());
                bytes.extend((0..(n + 7) / 8).map(|i| if i == n / 8 { (1_u16 << (n % 8)) as u8 - 1 } else { 0xFF }));
                for (key, runs) in &containers {
                    let card: u32 = runs.iter().map(|&(_, len)| u32::from(len) + 1).sum();
                    bytes.extend_from_slice(&key.to_le_bytes());
                    bytes.extend_from_slice(&((card - 1) as u16).to_le_bytes());
                }
                if n >= 4 {
                    bytes.extend(std::iter::repeat(0).take(4 * n));
                }
                for (_, runs) in &containers {
                    bytes.extend_from_slice(&(runs.len() as u16).to_le_bytes());
                    for &(value, len) in runs {
                        bytes.extend_from_slice(&value.to_le_bytes());
                        bytes.extend_from_slice(&len.to_le_bytes());
                    }
                }
            }

            (RoaringBitmap::deserialize(&mut Cursor::new(&bytes)).unwrap(), model)
        }

        fn any(rng: &mut SmallRng, kind: usize) -> (RoaringBitmap, Model) {
            match kind {
                0 => sets(rng),
                _ => runs(rng)
            }
        }

        // First and last value, possibly crossing keys or reaching u32::MAX
        fn range(rng: &mut SmallRng) -> (u32, u32) {
            let first = if rng.random_range(0..5) == 0 { u32::MAX - rng.random_range(0..70_000) } else { edge_value(rng) };
            let len = match rng.random_range(0..4) {
                0 => 0,
                1 => 0x1_0000 - (first & 0xFFFF) - 1,
                2 => rng.random_range(0..200_000),
                _ => rng.random_range(0..100)
            };

            (first, first.saturating_add(len))
        }

        fn same(bitmap: &RoaringBitmap, model: &Model) -> Result<(), String> {
            if !bitmap.iter().eq(model.iter().copied()) || bitmap.cardinality() != model.len() {
                return Err(format!("{} values, expected {}", bitmap.cardinality(), model.len()));
            }
            if bitmap.containers.iter().any(|c| c.is_empty()) {
                return Err("holds an empty container".to_string());
            }
            Ok(())
        }

        const KINDS: [&str; 2] = ["sets", "runs"];
        let mut failures: Vec<String> = Vec::new();
        let mut check = |name: String, f: &mut dyn FnMut() -> Result<(), String>| {
            match panic::catch_unwind(AssertUnwindSafe(|| f())) {
                Ok(Ok(())) => (),
                Ok(Err(message)) => failures.push(format!("{}: {}", name, message)),
                Err(_) => failures.push(format!("{}: panicked", name))
            }
        };

        let mut rng = SmallRng::seed_from_u64(0xb17_5e7);
        for round in 0..12 {
            for k0 in 0..2 {
                for k1 in 0..2 {
                    let (a, ma) = any(&mut rng, k0);
                    let (b, mb) = any(&mut rng, k1);
                    let pair = format!("round {} {} {}", round, KINDS[k0], KINDS[k1]);

                    let expected: [(&str, Model); 4] = [
                        ("or", ma.union(&mb).copied().collect()),
                        ("and", ma.intersection(&mb).copied().collect()),
                        ("and_not", ma.difference(&mb).copied().collect()),
                        ("xor", ma.symmetric_difference(&mb).copied().collect())
                    ];

                    for (op, expected) in expected.iter() {
                        let (a, b) = (&a, &b);
                        check(format!("{} {}", op, pair), &mut || {
                            let result = match *op {
                                "or" => a.or(b),
                                "and" => a.and(b),
                                "and_not" => a.and_not(b),
                                _ => a.xor(b)
                            };
                            same(&result, expected)
                        });

                        check(format!("inplace_{} {}", op, pair), &mut || {
                            let mut result = a.clone();
                            match *op {
                                "or" => result.inplace_or(b),
                                "and" => result.inplace_and(b),
                                "and_not" => result.inplace_and_not(b),
                                _ => result.inplace_xor(b)
                            }
                            same(&result, expected)
                        });

                        check(format!("{}_cardinality {}", op, pair), &mut || {
                            let found = match *op {
                                "or" => a.or_cardinality(b),
                                "and" => a.and_cardinality(b),
                                "and_not" => a.and_not_cardinality(b),
                                _ => a.xor_cardinality(b)
                            };
                            if found != expected.len() { Err(format!("{}, expected {}", found, expected.len())) } else { Ok(()) }
                        });
                    }

                    let subsets = (ma.is_subset(&mb), mb.is_subset(&ma));
                    check(format!("subset_of {}", pair), &mut || {
                        let found = (a.subset_of(&b), b.subset_of(&a));
                        if found != subsets { Err(format!("{:?}, expected {:?}", found, subsets)) } else { Ok(()) }
                    });
                }
            }

            for kind in 0..2 {
                let (bitmap, model) = any(&mut rng, kind);
                let name = format!("round {} {}", round, KINDS[kind]);

                for _ in 0..3 {
                    let (first, last) = range(&mut rng);
                    let in_range: Model = (first..=last).collect();
                    let r = format!("{}..={}", first, last);

                    let flipped: Model = model.symmetric_difference(&in_range).copied().collect();
                    check(format!("not {} {}", r, name), &mut || same(&bitmap.not(first..=last), &flipped));
                    check(format!("inplace_not {} {}", r, name), &mut || {
                        let mut result = bitmap.clone();
                        result.inplace_not(first..=last);
                        same(&result, &flipped)
                    });

                    let added: Model = model.union(&in_range).copied().collect();
                    check(format!("add_range {} {}", r, name), &mut || {
                        let mut result = bitmap.clone();
                        result.add_range(first..=last);
                        same(&result, &added)
                    });

                    let removed: Model = model.difference(&in_range).copied().collect();
                    check(format!("remove_range {} {}", r, name), &mut || {
                        let mut result = bitmap.clone();
                        result.remove_range(first..=last);
                        same(&result, &removed)
                    });

                    let contained = in_range.is_subset(&model);
                    check(format!("contains_range {} {}", r, name), &mut || {
                        let found = bitmap.contains_range(first..=last);
                        if found != contained { Err(format!("{}, expected {}", found, contained)) } else { Ok(()) }
                    });

                    // The exclusive form of the same range
                    check(format!("not exclusive {} {}", r, name), &mut || match last.checked_add(1) {
                        Some(end) => same(&bitmap.not(first..end), &flipped),
                        None => same(&bitmap.not(first..), &flipped)
                    });
                }

                check(format!("empty range {}", name), &mut || {
                    let mut result = bitmap.not(5..5);
                    result.inplace_not(7..7);
                    result.add_range(9..9);
                    result.remove_range(11..11);
                    if !bitmap.contains_range(13..13) { return Err("does not contain the empty range".to_string()); }
                    same(&result, &model)
                });

                for _ in 0..20 {
                    let value = edge_value(&mut rng);
                    let rank = model.range(..=value).count();
                    check(format!("rank {} {}", value, name), &mut || {
                        let found = bitmap.rank(value);
                        if found != rank { Err(format!("{}, expected {}", found, rank)) } else { Ok(()) }
                    });

                    let contains = model.contains(&value);
                    check(format!("contains {} {}", value, name), &mut || {
                        if bitmap.contains(value) != contains { Err(format!("expected {}", contains)) } else { Ok(()) }
                    });

                    let mut without = model.clone();
                    let was_present = without.remove(&value);
                    check(format!("remove {} {}", value, name), &mut || {
                        let mut result = bitmap.clone();
                        if result.remove(value) != was_present { return Err(format!("returned {}", !was_present)); }
                        same(&result, &without)
                    });
                }

                if !model.is_empty() {
                    for _ in 0..10 {
                        let rank = rng.random_range(0..model.len());
                        let expected = model.iter().nth(rank).copied();
                        check(format!("select {} {}", rank, name), &mut || {
                            let found = bitmap.select(rank as u32);
                            if found != expected { Err(format!("{:?}, expected {:?}", found, expected)) } else { Ok(()) }
                        });
                    }
                }

                let bounds = (model.iter().next().copied(), model.iter().next_back().copied());
                check(format!("min max {}", name), &mut || {
                    let found = (bitmap.min(), bitmap.max());
                    if found != bounds { Err(format!("{:?}, expected {:?}", found, bounds)) } else { Ok(()) }
                });

                check(format!("pop_min {}", name), &mut || {
                    let mut result = bitmap.clone();
                    let mut rest = model.clone();
                    for _ in 0..50 {
                        let expected = rest.iter().next().copied();
                        if let Some(value) = expected {
                            rest.remove(&value);
                        }
                        let found = result.pop_min();
                        if found != expected { return Err(format!("{:?}, expected {:?}", found, expected)); }
                    }
                    same(&result, &rest)
                });

                check(format!("serialize {}", name), &mut || {
                    let mut bytes = Vec::new();
                    let written = bitmap.serialize(&mut bytes).map_err(|e| e.to_string())?;
                    if written != bytes.len() || written != bitmap.serialized_size() {
                        return Err(format!("wrote {}, {} bytes, size {}", written, bytes.len(), bitmap.serialized_size()));
                    }
                    let read = RoaringBitmap::deserialize(&mut Cursor::new(&bytes)).map_err(|e| format!("{:?}", e))?;
                    same(&read, &model)
                });
            }
        }

        let mut kinds: Vec<String> = Vec::new();
        for failure in &failures {
            let head = failure.split(':').next().unwrap();
            let words: Vec<&str> = head.split(' ').collect();
            let mut key = vec![words[0]];
            key.extend(words.iter().skip(1).filter(|w| KINDS.contains(*w) || **w == "exclusive"));
            let key = key.join(" ") + ":";
            if !kinds.iter().any(|k| k.starts_with(&key)) {
                kinds.push(format!("{} -- e.g. {}", key, failure));
            }
        }
        assert!(failures.is_empty(), "{} failures, by operation:\n{}", failures.len(), kinds.join("\n"));
    }
}
