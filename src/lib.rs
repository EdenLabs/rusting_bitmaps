#![feature(const_fn)]
#![feature(copy_within)]
#![feature(ptr_offset_from)]
#![feature(slice_partition_dedup)]
#![feature(seek_convenience)]

// TODO: Enable these lints after sketching out the api
//#![deny(missing_docs)]
#![deny(bare_trait_objects)]

//! A native rust port of Roaring Bitmaps based on CRoaring with some updates for newer hardware and various improvements
//! 
//! # External Differences
//!  - Copy on write is currently not supported
//!  - Only supports x86_64
//!     AVX2 and SSE4.2 are enabled depending on the compiler flags
//!  - API and types are consistent and match data structure definition
//!  
//! # Internal Differences
//!  - All SIMD ops work on aligned memory instead of unaligned

// TODO: Look at using Align<T, A> for enforcing alignment on the internal vecs with aligned loads and see whether that has a performance impact
// TODO: Inline trivial fns
// TODO: Eliminate all bounds checks in critical paths

mod container;
mod roaring;
mod simd;
mod utils;

//#[cfg(test)]
//mod test;

pub use roaring::RoaringBitmap;