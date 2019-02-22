use crate::container::*;

struct Rle16 {
    value: u16,
    length: u16
}

pub struct RunContainer {
    run_count: usize,
    runs: Vec<Rle16>
}

impl RunContainer {
    
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