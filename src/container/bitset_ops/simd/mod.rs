#![allow(dead_code)]

#[cfg(target_feature = "avx2")]
mod avx2;
#[cfg(target_feature = "avx2")]
pub use avx2::*;

#[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
mod sse42;
#[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
pub use sse42::*;
