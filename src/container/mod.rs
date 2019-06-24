mod array;
pub mod array_ops;
mod bitset;
mod bitset_ops;
mod run;
mod run_ops;

pub use self::array::ArrayContainer;
pub use self::bitset::BitsetContainer;
pub use self::run::RunContainer;

pub mod consts {
    pub use super::DEFAULT_MAX_SIZE;
    pub use super::bitset::BITSET_SIZE_IN_WORDS;
}

use std::cell::Cell;
use std::fmt;
use std::io::{self, Read, Write};
use std::iter::Iterator;
use std::mem;
use std::ops::Range;
use std::slice;

// NOTE: Inplace variants consume self and return either self or a new container

/// Default maximum size of an array container before it is converted to another type
pub const DEFAULT_MAX_SIZE: usize = 4096;

/// Convert a range into a range of a different type
trait IntoRange<T> {
    /// Convert self into a range
    fn into_range(self) -> Range<T>;
}

/// The set union operation
trait SetOr<T> {
    fn or(&self, other: &T) -> Container;

    fn inplace_or(self, other: &T) -> Container;
}

/// The set intersection operation
trait SetAnd<T> {
    fn and(&self, other: &T) -> Container;

    fn and_cardinality(&self, other: &T) -> usize;

    fn inplace_and(self, other: &T) -> Container;
}

/// The set difference operation
trait SetAndNot<T> {
    fn and_not(&self, other: &T) -> Container;

    fn inplace_and_not(self, other: &T) -> Container;
}

/// The set symmetric difference operation
trait SetXor<T> {
    fn xor(&self, other: &T) -> Container;

    fn inplace_xor(self, other: &T) -> Container;
}

/// The set subset operation
trait Subset<T> {
    fn subset_of(&self, other: &T) -> bool;
}

/// The inverse set operation
trait SetNot {
    fn not(&self, range: Range<u16>) -> Container;

    fn inplace_not(self, range: Range<u16>) -> Container;
}

/// A struct for managing a lazily evaluated cardinality
#[derive(Clone)]
struct LazyCardinality {
    /// The managed cardinality, set to None if dirty
    card: Cell<Option<usize>>
}

impl LazyCardinality {
    /// Create a new `LazyCardinality` with no value
    pub fn none() -> Self {
        Self {
            card: Cell::new(None)
        }
    }

    /// Create a new `LazyCardinality` with a specified value
    pub fn with_value(value: usize) -> Self {
        Self {
            card: Cell::new(Some(value))
        }
    }

    /// Increment the cardinality by `value` if not dirty
    pub fn increment(&self, value: usize) {
        match self.card.get() {
            Some(card) => self.card.set(Some(card + value)),
            None => return
        }
    }

    /// Decrement the cardinality by `value` if not dirty
    pub fn decrement(&self, value: usize) {
        match self.card.get() {
            Some(card) => self.card.set(Some(card - value)),
            None => return
        }
    }

    /// Mark the cardinality as dirty
    #[inline]
    pub fn invalidate(&self) {
        self.card.set(None)
    }

    /// Get the cardinality
    pub fn get<F>(&self, eval: F) -> usize
        where F: Fn() -> usize
    {
        match self.card.get() {
            Some(card) => card,
            None => {
                let card = (eval)();

                self.card.set(Some(card));
                
                card
            }
        }
    }

    /// Set the cardinality to a specified value
    #[inline]
    pub fn set(&self, value: usize) {
        self.card.set(Some(value))
    }
}

impl fmt::Debug for LazyCardinality {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.card.get() {
            Some(card) => write!(f, "{}", card),
            None => write!(f, "-")
        }
    }
}

macro_rules! op {
    ($fn_name: ident, $ret_val: ty) => {
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
    ($fn_name: ident) => {
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

            mem::replace(self, result);
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

// TODO: Sort these to match `RoaringBitmap` because it's triggering me

impl Container {
    /// Create a container with all values in the specified range
    pub fn from_range(range: Range<u16>) -> Option<Self> {
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
            container.set_range((range.start as usize)..(range.end as usize));

            Some(Container::Bitset(container))
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
            Container::Bitset(_c) => return,            // Bitsets are fixed in size
            Container::Run(c) => c.shrink_to_fit(),
            Container::None => unreachable!()
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
            },
            Container::None => unreachable!()
        };
    }

    pub fn add_range(&mut self, range: Range<u16>) {
        match self {
            Container::Array(c) => c.add_range(range),
            Container::Bitset(c) => c.add_range(range),
            Container::Run(c) => c.add_range(range),
            Container::None => unreachable!()
        }
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
            },
            Container::None => unreachable!()
        }
    }

    /// Remove all elements within [min-max)
    /// 
    /// # Returns
    /// Returns false if no more elements are in the container, returns true otherwise
    pub fn remove_range(&mut self, range: Range<u16>) -> bool {
        match self {
            Container::Array(c) => {
                let vals_greater = array_ops::count_greater(&c[..], range.end as u16);// TODO: Make sure these don't truncate
                let vals_less = array_ops::count_less(&c[0..(c.len() - vals_greater)], range.start as u16);
                let result_card = vals_less + vals_greater;

                if result_card == 0 {
                    return false;
                }
                else {
                    c.remove_range((range.start as usize)..(range.end as usize));

                    return true;
                }
            },
            Container::Bitset(c) => {
                let result_card = c.cardinality() - c.cardinality_range(range.clone());

                if result_card == 0 {
                    return false;
                }
                else if result_card < DEFAULT_MAX_SIZE {
                    c.unset_range((range.start as usize)..(range.end as usize));
                    unsafe { c.set_cardinality(result_card); }
                    
                    *self = Container::Array(c.into());

                    return true;
                }
                else {
                    c.unset_range((range.start as usize)..(range.end as usize));
                    unsafe { c.set_cardinality(result_card); }

                    return true;
                }
            },
            Container::Run(c) => {
                let num_runs = c.num_runs();
                if num_runs == 0 {
                    return false;
                }

                let min = c.min()
                    .unwrap_or(0);
                let max = c.max()
                    .unwrap_or(0);

                if range.start <= min && range.end >= max {
                    return false;
                }

                c.remove_range(range);

                if RunContainer::serialized_size(num_runs) < BitsetContainer::serialized_size() {
                    return true;
                }
                else {
                    *self = Container::Bitset(c.into());
                    return true;
                }
            },
            Container::None => unreachable!()
        };
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
    pub fn contains_range(&self, range: Range<u16>) -> bool {
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
            Container::Array(c) => c.is_full(),
            Container::Bitset(c) => c.is_full(),
            Container::Run(c) => c.is_full(),
            Container::None => unreachable!()
        }
    }

    /// Check if the container is empty
    pub fn is_empty(&self) -> bool {
        match self {
            Container::Array(c) => c.cardinality() == 0,
            Container::Bitset(c) => c.cardinality() == 0,
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

    /// Check whether self is a subset of other
    op!(subset_of, bool);

    /// Perform an `or` operation between `self` and `other`
    op!(or, Self);

    /// Perform an `and` operation between `self` and `other`
    op!(and, Self);

    /// Perform an `and not` operation between `self` and `other`
    op!(and_not, Self);

    /// Perform an `xor` operation between `self` and `other`
    op!(xor, Self);

    /// Compute the negation of this container within the specified range
    pub fn not(&self, range: Range<u16>) -> Self {
        match self {
            Container::Array(c) => c.not(range),
            Container::Bitset(c) => c.not(range),
            Container::Run(c) => c.not(range),
            Container::None => unreachable!()
        }
    }

    /// Compute the cardinality of an `and` operation between `self` and `other`
    op!(and_cardinality, usize);

    /// Compute the `or` of self `self` and `other` storing the result in `self`
    inplace!(inplace_or);

    /// Compute the `and` of self `self` and `other` storing the result in `self`
    inplace!(inplace_and);

    /// Compute the `and_not` of self `self` and `other` storing the result in `self`
    inplace!(inplace_and_not);

    /// Compute the `xor` of self `self` and `other` storing the result in `self`
    inplace!(inplace_xor);
    
    /// Get a generic iterator over the container values
    pub fn iter(&self) -> Iter {
        let iter = match self {
            Container::Array(c) => ContainerIter::Array(c.iter()),
            Container::Bitset(c) => ContainerIter::Bitset(c.iter()),
            Container::Run(c) => ContainerIter::Run(c.iter()),
            Container::None => unreachable!()
        };
        
        Iter {
            iter: iter
        }
    }
}

impl Container {
    /// Get the serialized size of a container
    pub fn serialized_size(&self) -> usize {
        match self {
            Container::Array(c) => ArrayContainer::serialized_size(c.cardinality()),
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
            ContainerIter::Array(c) => c.next().map(|v| *v),
            ContainerIter::Bitset(c) => c.next(),
            ContainerIter::Run(c) => c.next()
        }
    }
}