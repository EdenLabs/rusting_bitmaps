#![feature(const_fn)]
#![feature(const_generics)]
#![feature(copy_within)]
#![feature(ptr_offset_from)]

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
// TODO: Inline trivial fns
// TODO: Eliminate all bounds checks in critical paths

mod container;
mod roaring;
mod simd;
mod utils;

//#[cfg(test)]
//mod test;

pub use roaring::RoaringBitmap;

use std::fmt;
use std::ops::{Deref, DerefMut};

// TODO: Export from eden as standalone crate and link that instead to avoid parallel impls
//       and to allow access to collections generic over the allocator used.

/// A marker wrapper for a type aligned to an `N` byte boundary
pub struct Aligned<T, const N: usize> {
    /// The inner value
    pub(crate) inner: T
}

impl<T, const N: usize> Aligned<T, {N}> {
    /// Mark a value as aligned to `N` 
    /// 
    /// # Safety
    /// This assumes that the value or it's contents are indeed aligned to `N`.
    /// Violation of this assumption is highly likely to cause UB
    pub unsafe fn new(value: T) -> Self {
        Self {
            inner: value
        }
    }

    /// Unwrap and yield the inner value
    pub fn unwrap(self) -> T {
        self.inner
    }
}

impl<T, const N: usize> Deref for Aligned<T, {N}> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T, const N: usize> DerefMut for Aligned<T, {N}> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: fmt::Debug, const N: usize> fmt::Debug for Aligned<T, {N}> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl<T: fmt::Display, const N: usize> fmt::Display for Aligned<T, {N}> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}