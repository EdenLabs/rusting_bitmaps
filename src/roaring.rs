use std::ops::{Range};

use crate::container::*;

/// A Roaring Bitmap
///
/// TODO: Description
#[derive(Clone)]
pub struct RoaringBitmap {
    /// List of containers in this roaring bitmap
    containers: Vec<Container>
}

impl RoaringBitmap {
    /// Create a new empty roaring bitmap
    pub fn new() -> Self {
        
    }
    
    /// Create a new roaring bitmap with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        
    }
    
    /// Create a new roaring bitmap with the specified range and step
    pub fn with_range(range: Range<usize>, step: usize) -> Self {
        
    }
    
    /// Create a new roaring bitmap from a set of elements
    pub fn from_slice(slice: &[usize]) -> Self {
        
    }
    
    /// Add a value to the bitmap
    pub fn add(&mut self, x: usize) -> bool {
        
    }

    /// Add a range of values to the bitmap
    pub fn add_range(&mut self, range: Range<usize>) {
        
    }
    
    /// Add a list of values to the bitmap
    pub fn add_slice(&mut self, slice: &[usize]) {
        
    }
    
    /// Remove a value from the bitmap
    pub fn remove(&mut self, x: usize) -> Self {
        
    }

    /// Remove a range of values from the bitmap
    pub fn remove_range(&mut self, range: Range<usize>) {
        
    }
    
    /// Remove a list of values from the bitmap
    pub fn remove_slice(&mut self, slice: &[usize]) {
        
    }
    
    /// Check if the bitmap contains a value
    pub fn contains(&self, value: usize) -> bool {
        
    }
    
    /// Check if the bitmap contains a range of values
    pub fn contains_range(&self, range: Range<usize>) -> bool {
        
    }

    /// Get the length of the bitmap
    ///
    /// This is the same as cardinality
    pub fn len(&self) -> usize {
        
    }
    
    /// Get the cardinality of the bitmap
    pub fn cardinality(&self) -> usize {
        
    }

    /// Check if the bitmap is empty
    pub fn is_empty(&self) -> bool {
        
    }
    
    /// Clear the contents of this bitmap
    pub fn clear(&mut self) {
        
    }
    
    /// Shrink the memory used by the bitmap to fit it's contents
    pub fn shrink_to_fit(&mut self) {
        
    }
    
    /// Find the element of a given rank in the bitmap,
    /// Returns None if the bitmap is smaller than `rank`
    pub fn select(&self, rank: usize) -> Option<Element> {
        
    }
    
    /// Find the number of integers smaller or equal to `x`
    pub fn rank(&self, x: usize) -> usize {
        
    }
    
    /// Find the smallest value in the bitmap. Returns None if empty
    pub fn min(&self) -> Option<usize> {
        
    }
    
    /// Find the largest value in the bitmap. Returns None if empty
    pub fn max(&self) -> Option<usize> {
        
    }
    
    
}