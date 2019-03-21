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

/// Default maximum size of an array container before it is converted to another type
pub const DEFAULT_MAX_SIZE: usize = 4096;

/// Enum representing a container
pub enum ContainerType {
    /// Array container
    Array(ArrayContainer),

    /// Bitset container
    Bitset(BitsetContainer),

    /// Run container
    Run(RunContainer)
}

/// Marker trait for container types
pub trait Container: Any {
    // TODO: See about implementing common container functionality on this trait
}

/// The set union operation
pub trait Union<T: Container> {
    type Output;

    fn union_with(&self, other: &T, out: &mut Self::Output);
}

/// The set intersection operation
pub trait Intersection<T: Container> {
    type Output;

    fn intersect_with(&self, other: &T, out: &mut Self::Output);
}

/// The set difference operation
pub trait Difference<T: Container> {
    type Output;

    fn difference_with(&self, other: &T, out: &mut Self::Output);
}

/// The set symmetric difference operation
pub trait SymmetricDifference<T: Container> {
    type Output;

    fn symmetric_difference_with(&self, other: &T, out: &mut Self::Output);
}

/// The set subset operation
pub trait Subset<T: Container> {
    fn subset_of(&self, other: &T) -> bool;
}

/// The inverse set operation
pub trait Negation {
    fn negate(&self, out: &mut ContainerType);
}