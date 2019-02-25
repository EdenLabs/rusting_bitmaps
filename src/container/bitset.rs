use crate::container::*;

const BITSET_SIZE_IN_WORDS: usize = (1 << 16) / 64;

#[derive(Clone)]
pub struct BitsetContainer {
    bitset: Vec<u64>
}

impl BitsetContainer {
    pub fn new() -> Self {
        Self {
            bitset: Vec::with_capacity(BITSET_SIZE_IN_WORDS)
        }
    }

    pub fn add_from_range(&mut self, min: usize, max: usize, step: usize) {
        unimplemented!()
    }

    pub fn clear(&mut self) {
        // TODO: Vectorize
        for word in &mut self.bitset {
            *word = 0;
        }
    }

    pub fn set_all(&mut self) {
        // TODO: Vectorize
        for word in &mut self.bitset {
            *word = std::u64::MAX;
        }
    }

    pub fn set_range(&mut self, start: usize, end: usize) {
        unimplemented!()
    }
}

impl From<ArrayContainer> for BitsetContainer {
    fn from(container: ArrayContainer) -> Self {
        unimplemented!()
    }
}

impl From<RunContainer> for BitsetContainer {
    fn from(container: RunContainer) -> Self {
        unimplemented!()
    }
}

impl Container for BitsetContainer { }

impl Difference<Self> for BitsetContainer {
    fn difference_with(&self, other: &Self, out: &mut Self) {
        unimplemented!()
    }
}

impl Difference<ArrayContainer> for BitsetContainer {
    fn difference_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl Difference<RunContainer> for BitsetContainer {
    fn difference_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl SymmetricDifference<Self> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &Self, out: &mut Self) {
        unimplemented!()
    }
}

impl SymmetricDifference<ArrayContainer> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl SymmetricDifference<RunContainer> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl Union<Self> for BitsetContainer {
    fn union_with(&self, other: &Self, out: &mut Self) {
        unimplemented!()
    }
}

impl Union<ArrayContainer> for BitsetContainer {
    fn union_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl Union<RunContainer> for BitsetContainer {
    fn union_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl Intersection<Self> for BitsetContainer {
    fn intersect_with(&self, other: &Self, out: &mut Self) {
        unimplemented!()
    }
}

impl Intersection<ArrayContainer> for BitsetContainer {
    fn intersect_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl Intersection<RunContainer> for BitsetContainer {
    fn intersect_with(&self, other: &RunContainer, out: &mut RunContainer) {
        unimplemented!()
    }
}

impl Subset<Self> for BitsetContainer {
    fn subset_of(&self, other: &Self) -> bool {
        unimplemented!()
    }
}

impl Subset<ArrayContainer> for BitsetContainer {
    fn subset_of(&self, other: &ArrayContainer) -> bool {
        unimplemented!()
    }
}

impl Subset<RunContainer> for BitsetContainer {
    fn subset_of(&self, other: &RunContainer) -> bool {
        unimplemented!()
    }
}