#![feature(const_fn)]
#![feature(ptr_offset_from)]
#![feature(slice_partition_dedup)]
#![feature(seek_convenience)]

// To appease clippy
#![feature(range_is_empty)]
#![feature(exact_size_is_empty)]

// TODO: Enable these lints after sketching out the api
//#![deny(missing_docs)]
#![deny(bare_trait_objects)]
#![allow(clippy::collapsible_if)] // No, just no. These were written that way to simplify reading
#![allow(clippy::range_plus_one)] // Another "programmer knows better". The api relies on bounded range structs currently

//! Rusting Bitmaps is a loose port of CRoaring with a few differences
//!
//! - An idiomatic and safe Rust API
//! - Additional optimizations (particularly around inplace operations)
//! - Custom allocator support
//! - Copy-on-Write is unsupported

// TODO: Inline trivial fns
// TODO: Reduce unsafe code usage without impacting performance much
// TODO: Ensure ranges aren't cutoff
// TODO: Run through and change any operations on intermediate containers to be inplace to reduce memory churn

mod container;
mod roaring;

#[cfg(test)] mod test;

pub use roaring::RoaringBitmap;

use std::ops::{RangeBounds, Bound};

/// Convert a range into a bounded range based on some internal constraints
trait IntoBound<T>: Sized {
    fn into_bound(self) -> (T, T);
}

impl<T> IntoBound<u16> for T
    where T: RangeBounds<u16>
{
    /// Convert the range into a range bounded by [0-max]
    fn into_bound(self) -> (u16, u16) {
        let start = match self.start_bound() {
            Bound::Excluded(bound) => *bound + 1,
            Bound::Included(bound) => *bound,
            Bound::Unbounded => 0
        };

        let end = match self.end_bound() {
            Bound::Excluded(bound) => *bound,
            Bound::Included(bound) => *bound + 1,
            Bound::Unbounded => std::u16::MAX
        };

        (start, end)
    }
}

impl<T> IntoBound<usize> for T
    where T: RangeBounds<usize>
{
    /// Convert the range into a range bounded by [0-max]
    fn into_bound(self) -> (usize, usize) {
        let start = match self.start_bound() {
            Bound::Excluded(bound) => *bound + 1,
            Bound::Included(bound) => *bound,
            Bound::Unbounded => 0
        };

        let end = match self.end_bound() {
            Bound::Excluded(bound) => *bound,
            Bound::Included(bound) => *bound + 1,
            Bound::Unbounded => std::usize::MAX
        };

        (start, end)
    }
}