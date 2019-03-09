use std::slice::{Iter, IterMut};

use crate::container::*;

#[derive(Clone, Copy)]
pub struct Rle16 {
    pub value: u16,
    pub length: u16
}

impl Rle16 {
    pub fn new(value: u16, length: u16) -> Self {
        Self {
            value,
            length
        }
    }
}

#[derive(Clone)]
pub struct RunContainer {
    runs: Vec<Rle16>,
    cardinality: usize
}

impl RunContainer {
    pub fn new() -> Self {
        unimplemented!()
    }
    
    pub fn with_capacity(capacity: usize) -> Self {
        unimplemented!()
    }
    
    pub fn shrink_to_fit(&mut self) {
        unimplemented!()
    }
    
    pub fn reserve(&mut self, additional: usize) {
        self.runs.reserve(additional);
    }
    
    pub fn add(&mut self, value: u16) {
        unimplemented!()
    }
    
    pub fn add_range(&mut self, min: u16, max: u16) {
        unimplemented!()
    }
    
    pub fn remove(&mut self, value: u16) {
        unimplemented!()
    }
    
    pub fn remove_range(&mut self, min: u16, max: u16) {
        unimplemented!()
    }
    
    pub fn contains(&self, value: u16) -> bool {
        unimplemented!()
    }
    
    pub fn contains_range(&self, min: u16, max: u16) -> bool {
        unimplemented!()
    }
    
    pub fn cardinality(&self) -> usize {
        unimplemented!()
    }
    
    pub fn is_empty(&self) -> bool {
        unimplemented!()
    }
    
    pub fn is_full(&self) -> bool {
        unimplemented!()
    }
    
    pub fn clear(&mut self) {
        unimplemented!()
    }
    
    pub fn append(&mut self, run: Rle16) {
        unimplemented!()
    }
    
    pub fn iter(&self) -> Iter<Rle16> {
        self.runs.iter()
    }
    
    pub fn iter_mut(&mut self) -> IterMut<Rle16> {
        self.runs.iter_mut()
    }
    
    pub fn min(&self) -> u16 {
        unimplemented!()
    }
    
    pub fn max(&self) -> u16 {
        unimplemented!()
    }
    
    pub fn rank(&self, value: u16) -> usize {
        unimplemented!()
    }
}

impl From<ArrayContainer> for RunContainer {
    fn from(container: ArrayContainer) -> Self {
        unimplemented!()
    }
}

impl From<BitsetContainer> for RunContainer {
    fn from(container: BitsetContainer) -> Self {
        unimplemented!()
    }
}

impl Container for RunContainer { }

/*
impl Difference<Self> for RunContainer {
    fn difference_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Difference<ArrayContainer> for RunContainer {
    fn difference_with(&self, other: &ArrayContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Difference<BitsetContainer> for RunContainer {
    fn difference_with(&self, other: &BitsetContainer) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<Self> for RunContainer {
    fn symmetric_difference_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<ArrayContainer> for RunContainer {
    fn symmetric_difference_with(&self, other: &ArrayContainer) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<BitsetContainer> for RunContainer {
    fn symmetric_difference_with(&self, other: &BitsetContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Union<Self> for RunContainer {
    fn union_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Union<ArrayContainer> for RunContainer {
    fn union_with(&self, other: &ArrayContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Union<BitsetContainer> for RunContainer {
    fn union_with(&self, other: &BitsetContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<Self> for RunContainer {
    fn intersect_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<ArrayContainer> for RunContainer {
    fn intersect_with(&self, other: &ArrayContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<BitsetContainer> for RunContainer {
    fn intersect_with(&self, other: &BitsetContainer) -> ContainerType {
        unimplemented!()
    }
}
*/

impl Subset<Self> for RunContainer {
    fn subset_of(&self, other: &Self) -> bool {
        unimplemented!()
    }
}

impl Subset<ArrayContainer> for RunContainer {
    fn subset_of(&self, other: &ArrayContainer) -> bool {
        unimplemented!()
    }
}

impl Subset<BitsetContainer> for RunContainer {
    fn subset_of(&self, other: &BitsetContainer) -> bool {
        unimplemented!()
    }
}