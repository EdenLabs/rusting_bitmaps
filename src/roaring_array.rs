use crate::*;
use crate::container::Container;

pub struct RoaringArray {
    containers: Vec<Box<dyn Container>>,
    keys: Vec<Key>
}

impl RoaringArray {
    pub fn new() -> Self {
        Self {
            containers: Vec::new(),
            keys: Vec::new()
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            containers: Vec::with_capacity(capacity),
            keys: Vec::with_capacity(capacity)
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.containers.shrink_to_fit();
        self.keys.shrink_to_fit();
    }

    pub fn copy_into(&self, other: &mut RoaringArray) -> bool {
        unimplemented!()
    }

    pub fn write_into(&self, other: &mut RoaringArray) -> bool {
        unimplemented!()
    }

    pub fn clear(&mut self) {

    }

    pub fn clear_containers(&mut self) {
        self.containers.clear();
    }

    pub fn clear_without_containers(&mut self) {
        unimplemented!()
    }

    pub fn reset(&mut self) {
        self.containers.clear();
        self.keys.clear();
    }

    pub fn index_of(&self, key: Key) -> usize {
        unimplemented!()
    }

    pub fn container_at(&self, index: usize) -> &dyn Container {
        unimplemented!()
    }

    pub fn key_at(&self, index: usize) -> Key {
        unimplemented!()
    }

    pub fn insert_at(&mut self, index: usize, key: Key, container: Box<dyn Container>) {
        unimplemented!()
    }

    pub fn append(&mut self, key: Key, container: Box<dyn Container>) {
        self.containers.push(container);
        self.keys.push(key);
    }

    // TODO: See if supporting COW is a good idea

    pub fn append_range(&mut self, other: &RoaringArray, start: usize, end: usize) {
        unimplemented!()
    }

    pub fn append_range_move(&mut self, other: &mut RoaringArray, start: usize, end: usize) {
        unimplemented!()
    }

    pub fn set_container_at(&mut self, index: usize, container: Box<dyn Container>) {
        unimplemented!()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.containers.reserve(additional);
        self.keys.reserve(additional);
    }

    pub fn len(&self) -> usize {
        self.containers.len()
    }
}