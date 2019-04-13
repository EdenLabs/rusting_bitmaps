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
    None,

    /// Array container
    Array(ArrayContainer),

    /// Bitset container
    Bitset(BitsetContainer),

    /// Run container
    Run(RunContainer)
}

impl ContainerType {
    /// Check if there is no container
    pub fn is_none(&self) -> bool {
        match self {
            ContainerType::None => true,
            _ => false
        }
    }

    /// Check if the container is an array
    pub fn is_array(&self) -> bool {
        match self {
            ContainerType::Array(_) => true,
            _ => false
        }
    }

    /// Check if the container is a bitset
    pub fn is_bitset(&self) -> bool {
        match self {
            ContainerType::Bitset(_) => true,
            _ => false
        }
    }

    /// Check if the container is a run
    pub fn is_run(&self) -> bool {
        match self {
            ContainerType::Run(_) => true,
            _ => false
        }
    }

    /// Unwrap the container as an array
    pub fn unwrap_array(self) -> ArrayContainer {
        match self {
            ContainerType::Array(array) => array,
            _ => panic!("Not an array")
        }
    }

    /// Unwrap the container as a bitset
    pub fn unwrap_bitset(self) -> BitsetContainer {
        match self {
            ContainerType::Bitset(bitset) => bitset,
            _ => panic!("Not a bitset")
        }
    }

    /// Unwrap the container as a run
    pub fn unwrap_run(self) -> RunContainer {
        match self {
            ContainerType::Run(run) => run,
            _ => panic!("Not a run")
        }
    }
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