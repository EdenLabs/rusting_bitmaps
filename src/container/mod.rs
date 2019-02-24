mod array;
mod array_simd;
mod bitset;
mod bitset_simd;
mod run;

pub use self::array::ArrayContainer;
pub use self::bitset::BitsetContainer;
pub use self::run::RunContainer;

use std::any::Any;

/// Marker trait for container types
pub trait Container: Any {
    // TODO: See about implementing common container functionality on this trait
}

/// Enum with the type of container an operation resulted in
pub enum ContainerType {// Is this necessary anymore?
    None,
    Array(ArrayContainer),
    Bitset(BitsetContainer),
    Run(RunContainer)
}

pub trait Difference<T: Container> {
    fn difference_with(&self, other: &T, out: &mut T);
}

pub trait SymmetricDifference<T: Container> {
    fn symmetric_difference_with(&self, other: &T, out: &mut T);
}

pub trait Intersection<T: Container> {
    fn intersect_with(&self, other: &T, out: &mut T);
}

pub trait Union<T: Container> {
    fn union_with(&self, other: &T, out: &mut T);
}

pub trait Negation<T: Container> {
    fn negate_with(&self, other: &T, out: &mut T);
}

pub trait Equality<T: Container> {
    fn equals(&self, other: &T) -> bool;
}

pub trait Subset<T: Container> {
    fn subset_of(&self, other: &T) -> bool;
}