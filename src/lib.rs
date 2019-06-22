#![feature(const_fn)]
#![feature(copy_within)]
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
// TODO: Eliminate all bounds checks in critical paths

mod container;
mod roaring;
mod simd;
mod utils;

//#[cfg(test)]
//mod test;

pub use roaring::RoaringBitmap;