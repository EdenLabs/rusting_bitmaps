#![allow(exceeding_bitshifts)]

use std::ops::{Range};

use crate::container::*;
use crate::container::array;

// TODO: Add support for custom allocators
// TODO: Implement checked variants?

/// Mask used for removing the lower half of a 32 bit integer to generate a key in a roaring bitmap.
/// The same can be accomplished by casting to a u16 to truncate the upper bits
const MASK: u32 = 0xFFFF;

/// A Roaring Bitmap
///
/// TODO: Description
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
            let container_min = value & MASK;
            let container_max = (max - (key << 16)).min(1 << 16);

            if let Some(container) = Container::from_range(range.clone()) {
                bitmap.containers.push(container);
                bitmap.keys.push(key as u16);
            }

            value += container_max - container_min;
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
    pub fn add(&mut self, x: u32) {
        let x_high = (x >> 16) as u16;

        if let Some(i) = self.get_index(&x_high) {
            self.containers[i].add(x as u16);
        }
        else {
            let mut array = ArrayContainer::new();
            array.add(x as u16);

            self.containers.push(Container::Array(array));
            self.keys.push(x_high);
        }
    }

    /// Add a value to the bitmap and return the index of the container that the value was added to.
    /// This allows accelerating some operations.
    /// 
    /// # Remarks
    /// The index container is only guaranteed to be valid immediately after the call assuming
    /// no containers are removed in subsequent operations.
    fn add_fetch_container(&mut self, x: u32) -> usize {
        let x_high = (x >> 16) as u16;

        if let Some(i) = self.get_index(&x_high) {
            self.containers[i].add(x as u16);

            i
        }
        else {
            let mut array = ArrayContainer::new();
            array.add(x as u16);

            self.containers.push(Container::Array(array));
            self.keys.push(x_high);

            self.containers.len() - 1
        }
    }

    /// Add a range of values to the bitmap
    pub fn add_range(&mut self, range: Range<u32>) {
        // TODO: Make this use container min and container max

        // Add the first value so we can nab the container index
        if range.len() == 0 {
            return;
        }

        unsafe {
            let max = range.end;
            let mut value = range.start;
            let mut prev = value;
            let mut c_index = self.add_fetch_container(value);

            value += 1;

            while value < max {
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
                value += 1;
            }
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
    pub fn remove(&mut self, x: u32) {
        let x_high = (x >> 16) as u16;
        
        if let Some(i) = self.get_index(&x_high) {
            self.containers[i].remove(x as u16);
            
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

        let src = array::count_less(&self.keys, min_key);
        let dst = src;

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

        if src > dst {
            unimplemented!()
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
        unimplemented!()
    }
    
    /// Check if the bitmap contains a range of values
    pub fn contains_range(&self, range: Range<u32>) -> bool {
        unimplemented!()
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
    pub fn subset_of(&self, other: &RoaringBitmap) -> RoaringBitmap {
        unimplemented!()
    }

    /// Compute the Jaccard index between `self` and `other`. 
    /// (Also known as the Tanimoto distance or Jaccard similarity coefficient)
    /// 
    /// Returns `None` if both bitmaps are empty
    pub fn jaccard_index(&self, other: &RoaringBitmap) -> Option<f64> {
        unimplemented!()
    }

    /// Or this bitmap with `other` (union)
    pub fn or(&self, other: &RoaringBitmap) -> RoaringBitmap {
        unimplemented!()
    }
    
    /// And this bitmap with `other` (intersect)
    pub fn and(&self, other: &RoaringBitmap) -> RoaringBitmap {
        unimplemented!()
    }

    /// And not this bitmap with `other` (difference)
    pub fn and_not(&self, other: &RoaringBitmap) -> RoaringBitmap {
        unimplemented!()
    }

    /// Xor this bitmap with `other` ()
    pub fn xor(&self, other: &RoaringBitmap) -> RoaringBitmap {
        unimplemented!()
    }

    /// Invert all elements in this bitmap
    pub fn not(&self) -> RoaringBitmap {
        unimplemented!()
    }

    /// Same as [`or`] but operates in place on `self`
    /// 
    /// [`or`]: RoaringBitmap::or
    pub fn inplace_or(&mut self, other: &RoaringBitmap) {
        unimplemented!()
    }

    /// Same as [`and`] but operates in place on `self`
    /// 
    /// [`and`]: RoaringBitmap::and
    pub fn inplace_and(&mut self, other: &RoaringBitmap) {
        unimplemented!()
    }

    /// Same as [`and_not`] but operates in place on `self`
    /// 
    /// [`and_not`]: RoaringBitmap::and_not
    pub fn inplace_and_not(&mut self, other: &RoaringBitmap) {
        unimplemented!()
    }
    
    /// Same as [`xor`] but operates in place on `self`
    /// 
    /// [`xor`]: RoaringBitmap::xor
    pub fn inplace_xor(&mut self, other: &RoaringBitmap) {
        unimplemented!()
    }

    /// Same as [`not`] but operates in place on `self`
    /// 
    /// [`not`]: RoaringBitmap::not
    pub fn inplace_not(&mut self) {
        unimplemented!()
    }

    /// Get the container index for a given key
    #[inline]
    fn get_index(&self, x: &u16) -> Option<usize> {
        self.keys.binary_search(x)
            .ok()
    }
}
