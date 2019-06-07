mod array;
mod array_ops;
mod bitset;
mod bitset_ops;
mod run;
mod run_ops;

use std::fmt;
use std::ops::Range;

pub use self::array::ArrayContainer;
pub use self::bitset::BitsetContainer;
pub use self::run::RunContainer;

/// Default maximum size of an array container before it is converted to another type
pub const DEFAULT_MAX_SIZE: usize = 4096;

/// The set union operation
pub trait Union<T> {
    type Output;

    fn union_with(&self, other: &T, out: &mut Self::Output);
}

/// The set intersection operation
pub trait Intersection<T> {
    type Output;

    fn intersect_with(&self, other: &T, out: &mut Self::Output);
}

/// The set difference operation
pub trait Difference<T> {
    type Output;

    fn difference_with(&self, other: &T, out: &mut Self::Output);
}

/// The set symmetric difference operation
pub trait SymmetricDifference<T> {
    type Output;

    fn symmetric_difference_with(&self, other: &T, out: &mut Self::Output);
}

/// The set subset operation
pub trait Subset<T> {
    fn subset_of(&self, other: &T) -> bool;
}

/// The inverse set operation
pub trait Negation {
    fn negate(&self, out: &mut Container);
}

/// Enum representing a container of any type
#[derive(Clone, Debug)]
pub enum Container {
    /// Array container
    Array(ArrayContainer),

    /// Bitset container
    Bitset(BitsetContainer),

    /// Run container
    Run(RunContainer)
}

// TODO: Sort these to match `RoaringBitmap` because it's triggering me

impl Container {
    /// Create a container with all values in the specified range
    pub fn from_range(range: Range<u32>) -> Option<Self> {
        debug_assert!(range.len() > 0);

        let size = range.len();

        // Result is an array
        if size < DEFAULT_MAX_SIZE {
            let mut container = ArrayContainer::with_capacity(size);
            container.add_range(range);
            
            Some(Container::Array(container))
        }
        // Result is a bitset
        else {
            let mut container = BitsetContainer::new();
            container.set_range(range);

            Some(Container::Bitset(container))
        }
    }

    /// Shrink the container it fit it's content
    pub fn shrink_to_fit(&mut self) {
        match self {
            Container::Array(c) => c.shrink_to_fit(),
            Container::Bitset(_c) => return,            // Bitsets are fixed in size
            Container::Run(c) => c.shrink_to_fit()
        }
    }

    /// Add a value to the underlying container
    pub fn add(&mut self, value: u16) {
        match self {
            Container::Array(c) => {
                if !c.add(value) {
                    *self = Container::Bitset(c.into());
                }
            },
            Container::Bitset(c) => {
                c.add(value);
            },
            Container::Run(c) => {
                c.add(value);
            }
        };
    }
    
    /// Remove a value from the underlying container
    pub fn remove(&mut self, value: u16) {
        match self {
            Container::Array(c) => {
                c.remove(value);
            },
            Container::Bitset(c) => {
                if c.remove(value) {
                    if c.cardinality() < DEFAULT_MAX_SIZE {
                        *self = Container::Array(c.into());
                    }
                }
            },
            Container::Run(c) => {
                c.remove(value);
            }
        }
    }

    /// Get the cardinality of the container
    pub fn cardinality(&self) -> usize {
        match self {
            Container::Array(c) => c.cardinality(),
            Container::Bitset(c) => c.cardinality(),
            Container::Run(c) => c.cardinality()
        }
    }

    /// Get the minmimu value in the container
    pub fn min(&self) -> Option<u16> {
        match self {
            Container::Array(c) => c.min(),
            Container::Bitset(c) => c.min(),
            Container::Run(c) => c.min()
        }
    }

    /// Get the maximum value in the container
    pub fn max(&self) -> Option<u16> {
        match self {
            Container::Array(c) => c.max(),
            Container::Bitset(c) => c.max(),
            Container::Run(c) => c.max()
        }
    }

    /// Find the number of values smaller or equal to `x`
    pub fn rank(&self, value: u16) -> usize {
        match self {
            Container::Array(c) => c.rank(value),
            Container::Bitset(c) => c.rank(value),
            Container::Run(c) => c.rank(value)
        }
    }

    pub fn select(&self, rank: u32, start_rank: &mut u32) -> Option<u16> {
        match self {
            Container::Array(c) => c.select(rank, start_rank),
            Container::Bitset(c) => c.select(rank, start_rank),
            Container::Run(c) => c.select(rank, start_rank)
        }
    } 
}
