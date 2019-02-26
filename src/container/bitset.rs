use crate::container::*;

const BITSET_SIZE_IN_WORDS: usize = (1 << 16) / 64;

#[derive(Clone)]
pub struct BitsetContainer {
    bitset: Vec<u64>,
    cardinality: usize
}

impl BitsetContainer {
    pub fn new() -> Self {
        Self {
            bitset: Vec::with_capacity(BITSET_SIZE_IN_WORDS),
            cardinality: 0
        }
    }

    pub fn add_from_range(&mut self, min: usize, max: usize, step: usize) {
        assert!(step != 0);
        assert!(min < max);
        
        if step % 64 == 0 {
            let mask: u64 = 0;
            
            let value = min % step;
            while value < 64 {
                mask |= 1 << value;
                
                value += step;
            }
            
            let first_word = min / 64;
            let last_word = (max - 1) / 64;
            
            self.cardinality
        }
    }

    pub fn clear(&mut self) {
        // TODO: Vectorize
        for word in &mut self.bitset {
            *word = 0;
        }
        
        self.cardinality = 0;
    }

    pub fn set_all(&mut self) {
        // TODO: Vectorize
        for word in &mut self.bitset {
            *word = std::u64::MAX;
        }
        
        self.cardinality = 1 << 16;
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