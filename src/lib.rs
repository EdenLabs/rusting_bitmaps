#![feature(const_fn)]
#![feature(ptr_offset_from)]
#![feature(slice_partition_dedup)]
#![feature(seek_convenience)]

// TODO: Enable these lints after sketching out the api
//#![deny(missing_docs)]
#![deny(bare_trait_objects)]

//! Rusting Bitmaps is a loose port of CRoaring with a few differences
//!
//! - An idiomatic and safe Rust API
//! - Additional optimizations (particularly around inplace operations)
//! - Custom allocator support
//! - Copy-on-Write is unsupported

// TODO: Inline trivial fns
// TODO: Reduce unsafe code usage without impacting performance much
// TODO: Ensure all bounds are inclusive so ranges aren't cutoff

mod container;
mod roaring;

#[cfg(test)] mod test;

pub use roaring::RoaringBitmap;

use std::ops::{RangeBounds, Bound, Range};

/// Convert a range into a bounded range based on some defined constraints
trait IntoBounded<T> {
    fn into_bounded(self) -> Range<T>;
}

impl<T> IntoBounded<u16> for T
    where T: RangeBounds<u16>
{
    /// Convert the range into a range bounded by [0-max]
    fn into_bounded(self) -> Range<u16> {
        let start = match self.start_bound() {
            Bound::Excluded(bound) => *bound + 1,
            Bound::Included(bound) => *bound,
            Bound::Unbounded => 0
        };

        let end = match self.end_bound() {
            Bound::Excluded(bound) => *bound - 1,
            Bound::Included(bound) => *bound,
            Bound::Unbounded => std::u16::MAX
        };

        start..end
    }
}