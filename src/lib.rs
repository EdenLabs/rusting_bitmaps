// To appease clippy
#![feature(exact_size_is_empty)]

// TODO: Enable these lints after sketching out the api
//#![deny(missing_docs)]
#![deny(bare_trait_objects)]
#![allow(clippy::collapsible_if)] // No, just no. These were written that way to simplify reading
#![allow(clippy::range_plus_one)] // Another "programmer knows better" situation

//! Rusting Bitmaps is a loose port of CRoaring with a few differences
//!
//! - An idiomatic and safe Rust API
//! - Additional optimizations (particularly around inplace operations)
//! - Copy-on-Write is unsupported (yet, TBD)

// TODO: Run through and change any operations on intermediate containers to be inplace to reduce memory churn
// TODO: Ensure soundness in the face of panics where necessary

mod container;
mod roaring;

#[cfg(test)] mod test;

pub use roaring::*;

use std::ops::{RangeBounds, Bound};

/// Convert a range into a bounded range based on some internal constraints
trait IntoBound<T>: Sized {
    fn into_bound(self) -> (T, T);
}

macro_rules! impl_into_bound {
    ($type:ident) => {
        impl<T> IntoBound<$type> for T
    where T: RangeBounds<$type>
    {
        /// Convert the range into a range bounded by [0-max]
        fn into_bound(self) -> ($type, $type) {
            let start = match self.start_bound() {
                Bound::Excluded(bound) => *bound + 1,
                Bound::Included(bound) => *bound,
                Bound::Unbounded => 0
            };

            let end = match self.end_bound() {
                Bound::Excluded(bound) => *bound,
                Bound::Included(bound) => *bound + 1,
                Bound::Unbounded => <$type>::max_value()
            };

            (start, end)
        }
    }

    };
}

impl_into_bound!(u16);
impl_into_bound!(usize);
impl_into_bound!(u32);