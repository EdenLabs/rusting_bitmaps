use crate::container::*;

pub struct BitsetContainer {
    array: Vec<u64>
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

/*
impl Difference<Self> for BitsetContainer {
    fn difference_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Difference<ArrayContainer> for BitsetContainer {
    fn difference_with(&self, other: &ArrayContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Difference<RunContainer> for BitsetContainer {
    fn difference_with(&self, other: &RunContainer) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<Self> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<ArrayContainer> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &ArrayContainer) -> ContainerType {
        unimplemented!()
    }
}

impl SymmetricDifference<RunContainer> for BitsetContainer {
    fn symmetric_difference_with(&self, other: &RunContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Union<Self> for BitsetContainer {
    fn union_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Union<ArrayContainer> for BitsetContainer {
    fn union_with(&self, other: &ArrayContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Union<RunContainer> for BitsetContainer {
    fn union_with(&self, other: &RunContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<Self> for BitsetContainer {
    fn intersect_with(&self, other: &Self) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<ArrayContainer> for BitsetContainer {
    fn intersect_with(&self, other: &ArrayContainer) -> ContainerType {
        unimplemented!()
    }
}

impl Intersection<RunContainer> for BitsetContainer {
    fn intersect_with(&self, other: &RunContainer) -> ContainerType {
        unimplemented!()
    }
}
*/

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