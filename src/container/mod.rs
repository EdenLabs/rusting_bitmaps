mod array;
mod array_ops;
mod bitset;
mod bitset_ops;
mod run;
mod run_ops;

pub use self::array::ArrayContainer;
pub use self::bitset::BitsetContainer;
pub use self::run::RunContainer;

use std::any::Any;

pub enum ContainerType {
    Array(ArrayContainer),
    Bitset(BitsetContainer),
    Run(RunContainer)
}

/// Marker trait for container types
pub trait Container: Any {
    // TODO: See about implementing common container functionality on this trait
}

pub trait Union<T: Container> {
    type Output;

    fn union_with(&self, other: &T, out: &mut Self::Output);
}

pub trait Intersection<T: Container> {
    type Output;

    fn intersect_with(&self, other: &T, out: &mut Self::Output);
}

pub trait Difference<T: Container> {
    fn difference_with(&self, other: &T, out: &mut T);
}

pub trait SymmetricDifference<T: Container> {
    fn symmetric_difference_with(&self, other: &T, out: &mut T);
}

pub trait Subset<T: Container> {
    fn subset_of(&self, other: &T) -> bool;
}

pub trait Negation {
    fn negate(&self, out: &mut ContainerType);
}