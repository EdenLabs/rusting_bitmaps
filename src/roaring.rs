#![allow(exceeding_bitshifts)]

use std::convert::TryFrom;
use std::fmt;
use std::ops::{Range};

use crate::container::*;

// TODO: Add support for custom allocators
// TODO: Implement checked variants?

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
    pub fn with_range(range: Range<usize>, step: usize) -> Self {
        unimplemented!()
    }
    
    /// Create a new roaring bitmap from a set of elements
    pub fn from_slice(slice: &[usize]) -> Self {
        unimplemented!()
    }

    /// Copy the contents of `other` into self overwriting any existing values
    pub fn copy_from(&mut self, other: &RoaringBitmap) {
        unimplemented!()
    }
    
    /// Add a value to the bitmap
    pub fn add(&mut self, x: usize) {
        let bound = (x >> 16) as u16;
        let x = (x & 0xFFFF) as u16;
        if let Some(i) = self.get_index(&bound) {
            self.containers[i].add(x);
        }
        else {
            let mut array = ArrayContainer::new();
            array.add(x);

            self.containers.push(Container::Array(array));
            self.keys.push(bound);
        }
    }

    /// Add a range of values to the bitmap
    pub fn add_range(&mut self, range: Range<usize>) {
        unimplemented!()
    }
    
    /// Add a list of values to the bitmap
    pub fn add_slice(&mut self, slice: &[usize]) {
        unimplemented!()
    }
    
    /// Remove a value from the bitmap
    pub fn remove(&mut self, x: usize) -> Self {
        let bound = (x >> 16) as u16;
        let x = (x & 0xFFFF) as u16;
        
        if let Some(i) = self.get_index(&bound) {
            self.containers[i].remove(x);
            
            if self.containers[i].cardinality() == 0 {
                self.containers.pop();
                self.keys.pop();
            }
        }
    }

    /// Remove a range of values from the bitmap
    pub fn remove_range(&mut self, range: Range<usize>) {
        unimplemented!()
    }
    
    /// Remove a list of values from the bitmap
    pub fn remove_slice(&mut self, slice: &[usize]) {
        unimplemented!()
    }
    
    /// Check if the bitmap contains a value
    pub fn contains(&self, value: usize) -> bool {
        unimplemented!()
    }
    
    /// Check if the bitmap contains a range of values
    pub fn contains_range(&self, range: Range<usize>) -> bool {
        unimplemented!()
    }

    /// Get the length of the bitmap
    ///
    /// This is the same as cardinality
    pub fn len(&self) -> usize {
        unimplemented!()
    }
    
    /// Get the cardinality of the bitmap
    pub fn cardinality(&self) -> usize {
        unimplemented!()
    }

    /// Check if the bitmap is empty
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }
    
    /// Clear the contents of this bitmap
    pub fn clear(&mut self) {
        self.containers.clear();
        self.keys.clear();
    }
    
    /// Shrink the memory used by the bitmap to fit it's contents
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.containers.shrink_to_fit()
    }
    
    /// Find the element of a given rank in the bitmap,
    /// Returns None if the bitmap is smaller than `rank`
    pub fn select(&self, rank: usize) -> Option<usize> {
        unimplemented!()
    }
    
    /// Find the number of integers smaller or equal to `x`
    pub fn rank(&self, x: usize) -> usize {
        unimplemented!()
    }
    
    /// Find the smallest value in the bitmap. Returns None if empty
    pub fn min(&self) -> Option<usize> {
        unimplemented!()
    }
    
    /// Find the largest value in the bitmap. Returns None if empty
    pub fn max(&self) -> Option<usize> {
        unimplemented!()
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

impl fmt::Display for RoaringBitmap {
    /// Pretty print the contents of the bitmap
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut result = String::new();
        for container in self.containers.iter() {
            result.push(format!("{}", container));
        }
        
        write!(f, "[ {} ]", result);
    }
}