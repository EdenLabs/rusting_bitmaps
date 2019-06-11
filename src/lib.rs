#![feature(const_fn)]
#![feature(copy_within)]

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
// TODO: Rename the set operations to the binary equivalent to avoid confusion for developers
// TODO: Update api's to be idiomatic rust
// TODO: Update all value ops into containers to take `u16` and all indexing ops to take `usize` for consistency and to
//       make it explicit that the containers only contain the lower 16 bits of a value in the bitmap
// TODO: Inline trivial fns
// TODO: Eliminate all bounds checks in critical paths

mod roaring;
mod container;
mod utils;

pub use roaring::RoaringBitmap;
