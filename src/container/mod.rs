mod array;
pub mod array_ops;
mod bitset;
mod run;
mod slack;

pub use self::array::ArrayContainer;
pub use self::bitset::BitsetContainer;
pub use self::run::RunContainer;

use std::fmt;
use std::io::{self, Read, Write};
use std::iter::Iterator;
use std::mem;
use std::ops::Range;
use std::slice;

// NOTE: Inplace variants consume self and return either self or a new container

/// Default maximum size of an array container before it is converted to another type
pub const DEFAULT_MAX_SIZE: usize = 4096;

/// How far below `DEFAULT_MAX_SIZE` a bitset container must fall by single-value removals before it
/// becomes an array again. `remove_range` does not apply it
///
/// Without the margin a container whose cardinality hovers at the limit converts on every
/// other operation, each conversion costing a pass over the whole container. A bitset in the
/// margin takes at most `2 * BITSET_RETAIN_MARGIN` bytes more than the array would.
pub const BITSET_RETAIN_MARGIN: usize = 256;

/// The set union operation
pub(crate) trait SetOr<T> {
    fn or(&self, other: &T) -> Container;

    fn inplace_or(self, other: &T) -> Container;
}

/// The set intersection operation
pub(crate) trait SetAnd<T> {
    fn and(&self, other: &T) -> Container;

    fn and_cardinality(&self, other: &T) -> usize;

    fn inplace_and(self, other: &T) -> Container;
}

/// The set difference operation
pub(crate) trait SetAndNot<T> {
    fn and_not(&self, other: &T) -> Container;

    fn inplace_and_not(self, other: &T) -> Container;
}

/// The set symmetric difference operation
pub(crate) trait SetXor<T> {
    fn xor(&self, other: &T) -> Container;

    fn inplace_xor(self, other: &T) -> Container;
}

/// The set subset operation
pub(crate) trait Subset<T> {
    fn subset_of(&self, other: &T) -> bool;
}

/// The inverse set operation
pub(crate) trait SetNot {
    fn not(&self, range: Range<u32>) -> Container;

    fn inplace_not(self, range: Range<u32>) -> Container;
}

macro_rules! op {
    (#[$doc:meta] $fn_name: ident, $ret_val: ty) => {
        #[$doc]
        pub fn $fn_name(&self, other: &Self) -> $ret_val {
            match self {
                Container::Array(c0) => match other {
                    Container::Array(c1) => c0.$fn_name(c1),
                    Container::Bitset(c1) => c0.$fn_name(c1),
                    Container::Run(c1) => c0.$fn_name(c1),
                    Container::None => unreachable!()
                },
                Container::Bitset(c0) => match other {
                    Container::Array(c1) => c0.$fn_name(c1),
                    Container::Bitset(c1) => c0.$fn_name(c1),
                    Container::Run(c1) => c0.$fn_name(c1),
                    Container::None => unreachable!()
                },
                Container::Run(c0) => match other {
                    Container::Array(c1) => c0.$fn_name(c1),
                    Container::Bitset(c1) => c0.$fn_name(c1),
                    Container::Run(c1) => c0.$fn_name(c1),
                    Container::None => unreachable!()
                },
                Container::None => unreachable!()
            }
        }
    }
}

macro_rules! inplace {
    (#[$doc:meta] $fn_name: ident) => {
        #[$doc]
        pub fn $fn_name(&mut self, other: &Self) {
            let owned = mem::replace(self, Container::None);
            let result = match owned {
                Container::Array(c0) => match other {
                    Container::Array(c1) => c0.$fn_name(c1),
                    Container::Bitset(c1) => c0.$fn_name(c1),
                    Container::Run(c1) => c0.$fn_name(c1),
                    Container::None => unreachable!()
                },
                Container::Bitset(c0) => match other {
                    Container::Array(c1) => c0.$fn_name(c1),
                    Container::Bitset(c1) => c0.$fn_name(c1),
                    Container::Run(c1) => c0.$fn_name(c1),
                    Container::None => unreachable!()
                },
                Container::Run(c0) => match other {
                    Container::Array(c1) => c0.$fn_name(c1),
                    Container::Bitset(c1) => c0.$fn_name(c1),
                    Container::Run(c1) => c0.$fn_name(c1),
                    Container::None => unreachable!()
                },
                Container::None => unreachable!()
            };

            debug_assert!(!result.is_none());

            *self = result;
        }
    }
}

/// Enum representing a container of any type
#[derive(Clone, Debug)]
pub enum Container {
    /// Sentinal for an empty container
    None,

    /// Array container
    Array(ArrayContainer),

    /// Bitset container
    Bitset(BitsetContainer),

    /// Run container
    Run(RunContainer)
}

impl Container {
    /// Create an empty container
    #[allow(dead_code)]
    pub fn new() -> Self {
        Container::Array(ArrayContainer::new())
    }

    /// Create a container with all values in the specified range
    pub fn from_range(range: Range<u32>) -> Self {
        debug_assert!(!range.is_empty());

        let size = range.len();

        // Result is an array
        if size < DEFAULT_MAX_SIZE {
            let mut container = ArrayContainer::with_capacity(size);
            container.add_range(range);
            
            Container::Array(container)
        }
        // Result is a bitset
        else {
            let mut container = BitsetContainer::new();
            container.set_range(range);
            container.into_efficient_container()
        }
    }

    /// Check whether the container is a `Container::None`
    pub fn is_none(&self) -> bool {
        match self {
            Container::None => true,
            _ => false
        }
    }

    /// Check whether the container is a `Container::Run`
    pub fn is_run(&self) -> bool {
        match self {
            Container::Run(_c) => true,
            _ => false
        }
    }

    /// Shrink the container it fit it's content
    pub fn shrink_to_fit(&mut self) {
        match self {
            Container::Array(c) => c.shrink_to_fit(),
            Container::Bitset(_c) => (),            // Bitsets are fixed in size
            Container::Run(c) => c.shrink_to_fit(),
            Container::None => unreachable!()
        }
    }

    /// Add a value to the underlying container
    pub fn add(&mut self, value: u16) {
        match self {
            Container::Array(c) => {
                if !c.add(value) {
                    let mut bitset: BitsetContainer = c.into();
                    bitset.add(value);

                    *self = Container::Bitset(bitset);
                }
            },
            Container::Bitset(c) => {
                c.add(value);
            },
            Container::Run(c) => {
                c.add(value);
            },
            Container::None => unreachable!()
        };
    }

    /// Add all values within the non-empty `range`. An array that would exceed `DEFAULT_MAX_SIZE`
    /// becomes a bitset
    pub fn add_range(&mut self, range: Range<u32>) {
        match self {
            Container::Array(c) => {
                if c.cardinality_with_range(&range) > DEFAULT_MAX_SIZE {
                    let mut bitset: BitsetContainer = c.into();
                    bitset.add_range(range);

                    *self = Container::Bitset(bitset);
                }
                else {
                    c.add_range(range);
                }
            },
            Container::Bitset(c) => c.add_range(range),
            Container::Run(c) => c.add_range(range),
            Container::None => unreachable!()
        }
    }
    
    /// Remove a value from the underlying container, returning whether it was present. A bitset
    /// becomes an array once its cardinality falls below `DEFAULT_MAX_SIZE - BITSET_RETAIN_MARGIN`
    pub fn remove(&mut self, value: u16) -> bool {
        match self {
            Container::Array(c) => {
                c.remove(value)
            },
            Container::Bitset(c) => {
                let removed = c.remove(value);
                if removed && c.cached_cardinality() < DEFAULT_MAX_SIZE - BITSET_RETAIN_MARGIN {
                    *self = Container::Array(c.into());
                }

                removed
            },
            Container::Run(c) => {
                c.remove(value)
            },
            Container::None => unreachable!()
        }
    }

    /// Remove and return the smallest value in the container, `None` if it is empty
    ///
    /// Constant time for an array. The other containers find the minimum then remove it
    pub fn pop_min(&mut self) -> Option<u16> {
        match self {
            Container::Array(c) => c.pop_min(),
            _ => {
                let min = self.min()?;
                self.remove(min);

                Some(min)
            }
        }
    }

    /// Remove all elements within [min-max)
    /// 
    /// # Returns
    /// Returns true if the container still has elements, returns false otherwise
    pub fn remove_range(&mut self, range: Range<u32>) -> bool {
        match self {
            Container::Array(c) => {
                c.remove_range(range);
                !c.is_empty()
            },
            Container::Bitset(c) => {
                c.unset_range(range);
                
                if c.is_empty() {
                    false
                }
                else {
                    if c.cardinality() < DEFAULT_MAX_SIZE {
                        *self = Container::Array(c.into());
                    }

                    true
                }
            },
            Container::Run(c) => {
                let num_runs = c.num_runs();
                if num_runs == 0 {
                    return false;
                }

                let min = c.min()
                    .unwrap_or(0) as u32;
                let max = c.max()
                    .unwrap_or(0) as u32;

                if range.start <= min && range.end >= max {
                    return false;
                }

                c.remove_range(range);

                if RunContainer::serialized_size(num_runs) < BitsetContainer::serialized_size() {
                    true
                }
                else {
                    *self = Container::Bitset(c.into());
                    true
                }
            },
            Container::None => unreachable!()
        }
    }

    /// Check if the container contains a specific value
    pub fn contains(&self, value: u16) -> bool {
        match self {
            Container::Array(c) => c.contains(value),
            Container::Bitset(c) => c.contains(value),
            Container::Run(c) => c.contains(value),
            Container::None => unreachable!()
        }
    }

    /// Check if the container contains a range of values
    pub fn contains_range(&self, range: Range<u32>) -> bool {
        match self {
            Container::Array(c) => c.contains_range(range),
            Container::Bitset(c) => c.contains_range(range),
            Container::Run(c) => c.contains_range(range),
            Container::None => unreachable!()
        }
    }

    /// Check if the container is full
    pub fn is_full(&self) -> bool {
        match self {
            // An array holds at most `DEFAULT_MAX_SIZE` of the 65 536 values
            Container::Array(_) => false,
            Container::Bitset(c) => c.is_full(),
            Container::Run(c) => c.is_full(),
            Container::None => unreachable!()
        }
    }

    /// Check if the container is empty
    pub fn is_empty(&self) -> bool {
        match self {
            Container::Array(c) => c.cardinality() == 0,
            Container::Bitset(c) => c.is_empty(),
            Container::Run(c) => c.is_empty(),
            Container::None => unreachable!()
        }
    }

    /// Get the cardinality of the container
    pub fn cardinality(&self) -> usize {
        match self {
            Container::Array(c) => c.cardinality(),
            Container::Bitset(c) => c.cardinality(),
            Container::Run(c) => c.cardinality(),
            Container::None => unreachable!()
        }
    }

    /// Get the minmimu value in the container
    pub fn min(&self) -> Option<u16> {
        match self {
            Container::Array(c) => c.min(),
            Container::Bitset(c) => c.min(),
            Container::Run(c) => c.min(),
            Container::None => unreachable!()
        }
    }

    /// Get the maximum value in the container
    pub fn max(&self) -> Option<u16> {
        match self {
            Container::Array(c) => c.max(),
            Container::Bitset(c) => c.max(),
            Container::Run(c) => c.max(),
            Container::None => unreachable!()
        }
    }

    /// Find the number of values smaller or equal to `x`
    pub fn rank(&self, value: u16) -> usize {
        match self {
            Container::Array(c) => c.rank(value),
            Container::Bitset(c) => c.rank(value),
            Container::Run(c) => c.rank(value),
            Container::None => unreachable!()
        }
    }

    /// Find the element of a given rank starting at `start_rank`. Returns None if no element is present and updates `start_rank`
    pub fn select(&self, rank: u32, start_rank: &mut u32) -> Option<u16> {
        match self {
            Container::Array(c) => c.select(rank, start_rank),
            Container::Bitset(c) => c.select(rank, start_rank),
            Container::Run(c) => c.select(rank, start_rank),
            Container::None => unreachable!()
        }
    }

    op! {
        /// Check whether self is a subset of other
        subset_of, bool
    }

    op! {
        /// Perform an `or` operation between `self` and `other`
        or, Self
    }

    op! {
        /// Perform an `and` operation between `self` and `other`
        and, Self
    }

    op! {
        /// Perform an `and not` operation between `self` and `other`
        and_not, Self
    }

    op! {
        /// Perform an `xor` operation between `self` and `other`
        xor, Self
    }

    /// Compute the negation of this container within the specified range
    pub fn not(&self, range: Range<u32>) -> Self {
        match self {
            Container::Array(c) => c.not(range),
            Container::Bitset(c) => c.not(range),
            Container::Run(c) => c.not(range),
            Container::None => unreachable!()
        }
    }

    op! {
        /// Compute the cardinality of an `and` operation between `self` and `other`
        and_cardinality, usize
    }

    inplace! {
        /// Compute the `or` of self `self` and `other` storing the result in `self`
        inplace_or
    }

    inplace! {
        /// Compute the `and` of self `self` and `other` storing the result in `self`
        inplace_and
    }

    inplace! {
        /// Compute the `and_not` of self `self` and `other` storing the result in `self`
        inplace_and_not
    }

    inplace! {
        /// Compute the `xor` of self `self` and `other` storing the result in `self`
        inplace_xor
    }

    /// Compute the negation of self inplace within the specified range
    pub fn inplace_not(self, range: Range<u32>) -> Container {
        match self {
            Container::Array(c) => c.inplace_not(range),
            Container::Bitset(c) => c.inplace_not(range),
            Container::Run(c) => c.inplace_not(range),
            Container::None => unreachable!()
        }
    }
    
    /// Get a generic iterator over the container values
    pub fn iter(&self) -> Iter {
        let iter = match self {
            Container::Array(c) => ContainerIter::Array(c.iter()),
            Container::Bitset(c) => ContainerIter::Bitset(c.iter()),
            Container::Run(c) => ContainerIter::Run(c.iter()),
            Container::None => unreachable!()
        };
        
        Iter {
            iter
        }
    }
}

impl Container {
    /// Get the serialized size of a container
    pub fn serialized_size(&self) -> usize {
        match self {
            Container::Array(c) => ArrayContainer::serialized_size(c.cardinality()),
            // The format has no tag for the container type, a reader takes a cardinality at or
            // below the array limit to be an array
            Container::Bitset(c) if c.cardinality() <= DEFAULT_MAX_SIZE => {
                ArrayContainer::serialized_size(c.cardinality())
            },
            Container::Bitset(_c) => BitsetContainer::serialized_size(),
            Container::Run(c) => RunContainer::serialized_size(c.num_runs()),
            _ => unreachable!()
        }
    }

    /// Serialize the container into the provided writer
    #[cfg(target_endian = "little")]
    pub fn serialize<W: Write>(&self, buf: &mut W) -> io::Result<usize> {
        match self {
            Container::Array(c) => c.serialize(buf),
            Container::Bitset(c) if c.cardinality() <= DEFAULT_MAX_SIZE => {
                ArrayContainer::from(c).serialize(buf)
            },
            Container::Bitset(c) => c.serialize(buf),
            Container::Run(c) => c.serialize(buf),
            _ => unreachable!()
        }
    }

    // NOTE: Deserialize not implemented on container as information is not easily available here
}

/// An enum containing the iterators for various containers
enum ContainerIter<'a> {
    None,
    Array(slice::Iter<'a, u16>),
    Bitset(bitset::Iter<'a>),
    Run(run::Iter<'a>)
}

/// An iterator over the values in a container
pub struct Iter<'a> {
    iter: ContainerIter<'a>
}

impl<'a> Iter<'a> {
    pub fn empty() -> Self {
        Iter {
            iter: ContainerIter::None
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = u16;
    
    fn next(&mut self) -> Option<Self::Item> {
         match &mut self.iter {
            ContainerIter::None => None,
            ContainerIter::Array(c) => c.next().copied(),
            ContainerIter::Bitset(c) => c.next(),
            ContainerIter::Run(c) => c.next()
        }
    }
}

fn is_valid_range(range: Range<u32>) -> bool {
    let valid_len = range.len() <= (1 << 16);
    let valid_bounds = range.start <= (1 << 16) && range.end <= (1 << 16);

    valid_len && valid_bounds
}

#[cfg(test)]
mod test {
    use crate::test::*;
    use super::*;

    #[test]
    fn bitset_converts_back_below_the_margin() {
        let is_bitset = |c: &Container| match c { Container::Bitset(_) => true, _ => false };

        let mut container = Container::new();
        for value in 0..DEFAULT_MAX_SIZE as u16 {
            container.add(value);
        }
        assert!(!is_bitset(&container));

        // One past the array limit
        container.add(DEFAULT_MAX_SIZE as u16);
        assert!(is_bitset(&container));

        // Removing down to exactly the margin edge keeps the bitset
        for value in 0..=(BITSET_RETAIN_MARGIN as u16) {
            assert!(container.remove(value));
        }
        assert_eq!(container.cardinality(), DEFAULT_MAX_SIZE - BITSET_RETAIN_MARGIN);
        assert!(is_bitset(&container));

        // and one below the margin converts, without losing anything
        assert!(container.remove(BITSET_RETAIN_MARGIN as u16 + 1));
        assert!(!is_bitset(&container));
        assert_eq!(container.cardinality(), DEFAULT_MAX_SIZE - BITSET_RETAIN_MARGIN - 1);
        assert_eq!(container.min(), Some(BITSET_RETAIN_MARGIN as u16 + 2));
        assert_eq!(container.max(), Some(DEFAULT_MAX_SIZE as u16));
        assert!(!container.remove(0));

        // An array fills to the limit before it becomes a bitset again
        for value in 0..=(BITSET_RETAIN_MARGIN as u16 + 1) {
            container.add(value);
        }
        assert_eq!(container.cardinality(), DEFAULT_MAX_SIZE + 1);
        assert!(is_bitset(&container));
    }

    impl TestShim<u16> for Container {
        fn from_data(data: &[u16]) -> Self {
            let mut result = Self::new();

            for value in data.iter() {
                result.add(*value);
            }

            result
        }

        fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=u16> + 'a> {
            Box::new(self.iter())
        }

        fn card(&self) -> usize {
            self.cardinality()
        }
    }

    #[test]
    fn run_set_ops_cover_every_value() {
        use std::iter::once;

        let run = |ranges: &[std::ops::Range<u32>]| {
            let mut run = RunContainer::new();
            for range in ranges {
                run.add_range(range.clone());
            }
            Container::Run(run)
        };
        let array = |values: &[u16]| {
            let mut array = ArrayContainer::new();
            for &value in values {
                array.add(value);
            }
            Container::Array(array)
        };

        // Array values after the last run are not in the intersection
        assert!(run(&[0..10]).and(&array(&[5, 100])).iter().eq(once(5)));
        assert_eq!(run(&[0..10]).and_cardinality(&array(&[5, 100])), 1);

        // Runs that end at the last value of the container
        let high = run(&[60_000..(1 << 16)]);
        let edges = run(&[0..10, 65_000..(1 << 16)]);
        assert!(high.and(&edges).iter().eq(65_000..=65_535));
        assert_eq!(high.and_cardinality(&edges), 536);
        assert!(high.or(&edges).iter().eq((0..10).chain(60_000..=65_535)));
        assert!(high.and_not(&edges).iter().eq(60_000..65_000));
        assert!(high.xor(&edges).iter().eq((0..10).chain(60_000..65_000)));
        assert!(high.or(&array(&[5, 65_535])).iter().eq(once(5).chain(60_000..=65_535)));
        assert!(high.xor(&array(&[5, 65_535])).iter().eq(once(5).chain(60_000..65_535)));
        assert!(array(&[5, 65_535]).and_not(&high).iter().eq(once(5)));
        assert!(high.not(50_000..(1 << 16)).iter().eq(50_000..60_000));
        assert!(edges.subset_of(&run(&[0..(1 << 16)])));
        assert!(!high.subset_of(&edges));
        assert!(array(&[3, 65_535]).subset_of(&edges));
        assert!(!array(&[3, 10]).subset_of(&edges));
    }

    #[test]
    fn rank_counts_values_up_to_and_including() {
        let mut bitset = BitsetContainer::new();
        let mut array = ArrayContainer::new();
        for value in [3, 64, 200, 65_535] {
            bitset.set(value);
            array.add(value);
        }

        for (value, rank) in [(0, 0), (3, 1), (4, 1), (63, 1), (64, 2), (127, 2), (128, 2), (65_534, 3), (65_535, 4)] {
            assert_eq!(bitset.rank(value), rank, "bitset rank of {}", value);
            assert_eq!(array.rank(value), rank, "array rank of {}", value);
        }
    }
}