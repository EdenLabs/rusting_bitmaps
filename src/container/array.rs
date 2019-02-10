use crate::container::Container;

const DEFAULT_MAX_SIZE: usize = 4096;

#[derive(Clone)]
pub struct ArrayContainer {
    array: Vec<u16>
}

impl ArrayContainer {
    pub fn new() -> Self {
        Self {
            array: Vec::with_capacity(DEFAULT_MAX_SIZE)
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            array: Vec::with_capacity(capacity)
        }
    }

    pub fn with_range(min: usize, max: usize) -> Self {
        let mut container = Self {
            array: Vec::with_capacity(max - min + 1)
        };

        for i in min..max {
            container.array.push(i as u16);
        }

        container
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.array.capacity()
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.array.shrink_to_fit();
    }

    pub fn copy_into(&self, target: &mut ArrayContainer) {
        let cap = self.capacity();
        let target_cap = target.capacity();

        if cap > target_cap {
            target.array.reserve(cap - target_cap);
        }

        target.array.clear();
        target.array.extend(self.array.iter());
    }

    pub fn add_from_range(&mut self, min: usize, max: usize, step: usize) {
        let range = min..max;

        // Resize to fit all new elements
        let len = self.len();
        let cap = self.capacity();
        let slack = cap - len;
        if slack < range.len() {
            self.array.reserve(range.len() - slack);
        }

        // Append new elements
        for i in (min..max).step_by(step) {
            self.array.push(i as u16);
        }
    }

    pub fn union(&self, other: &ArrayContainer, target: &mut ArrayContainer) {
        let max_len = self.len() + other.len();
        let target_cap = target.capacity();
        if target_cap < max_len {
            target.array.reserve(max_len - target_cap);
        }

        
    }
}

impl Container for ArrayContainer { }