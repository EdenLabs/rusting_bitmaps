#![allow(unused_variables)]
#![allow(unreachable_code)]

//! A collection of simd utilities abstracted over the register size.
//! 
//! # Safety
//! A minimum alignment of 32 bytes is assumed for full compatibility
//! 
//! Mocks are provided in the event that this is compiled with incompatible settings.
//! Running a build without vector instructions will panic

#[cfg(target_feature = "avx2")]
use std::arch::x86_64::{
    _mm256_extract_epi16,
    _mm256_lddqu_si256,
    _mm256_set1_epi16,
    _mm256_setzero_si256,
    _mm256_shuffle_epi8,
    _mm256_storeu_si256
};

#[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
use std::arch::x86_64::{
    _mm_extract_epi16,
    _mm_lddqu_si128,
    _mm_set1_epi16,
    _mm_setzero_si128,
    _mm_shuffle_epi8,
    _mm_storeu_si128
};

pub use consts::*;

/// Convenience macro to simplify avx cfg declarations
#[macro_export]
macro_rules! cfg_avx {
    ($($t:tt)*) => {
        #[cfg(target_feature = "avx2")]
        $($t)*
    };
}

/// Convenience macro to simplify sse cfg declarations
#[macro_export]
macro_rules! cfg_sse {
    ($($t:tt)*) => {
        #[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
        $($t)*
    };
}

/// Convenience macro to simplify scalar cfg declarations
#[macro_export]
macro_rules! cfg_default {
    ($($t:tt)*) => {
        #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx2")))]
        $($t)*
    };
}

#[cfg(target_feature = "avx2")]
mod consts {
    pub type Register = std::arch::x86_64::__m256i;
    pub const SIZE: usize = 16;
    pub const N: i32 = 15; // TODO: move these into the module that uses them
    pub const NM1: i32 = 14;
}

#[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
mod consts {
    pub type Register = std::arch::x86_64::__m128i;
    pub const SIZE: usize = 8;
    pub const N: i32 = 7;
    pub const NM1: i32 = 6;
}

#[cfg(not(any(target_feature = "sse4.2", target_feature = "avx2")))]
mod consts {
    pub type Register = ();
    pub const SIZE: usize = 8;
    pub const N: i32 = 1;
    pub const NM1: i32 = 1;
}

const MISSING_MSG: &'static str = "Failed to find an intrinsic. Did you compile with `+avx2` or `+sse4.2`?";

#[inline(always)]
pub unsafe fn extract_epi16(a: Register, b: i32) -> i16 {
    cfg_avx! { return _mm256_extract_epi16(a, b); }
    cfg_sse! { return _mm_extract_epi16(a, b); }

    panic!(MISSING_MSG);
}

#[inline(always)]
pub unsafe fn lddqu_si(mem_addr: *const Register) -> Register {
    cfg_avx! { return _mm256_lddqu_si256(mem_addr); }
    cfg_sse! { return _mm_lddqu_si128(mem_addr); }

    panic!(MISSING_MSG);
}

#[inline(always)]
pub unsafe fn set1_epi16(a: i16) -> Register {
    cfg_avx! { return _mm256_set1_epi16(a); }
    cfg_sse! { return _mm_set1_epi16(a); }

    panic!(MISSING_MSG);
}

#[inline(always)]
pub unsafe fn setzero_si() -> Register {
    cfg_avx! { return _mm256_setzero_si256(); }
    cfg_sse! { return _mm_setzero_si128(); }

    panic!(MISSING_MSG);
}

#[inline(always)]
pub unsafe fn shuffle_epi8(a: Register, b: Register) -> Register {
    cfg_avx! { return _mm256_shuffle_epi8(a, b); }
    cfg_sse! { return _mm_shuffle_epi8(a, b); }

    panic!(MISSING_MSG);
}

#[inline(always)]
pub unsafe fn storeu_si(mem_addr: *mut Register, a: Register) {
    cfg_avx! { return _mm256_storeu_si256(mem_addr, a); }
    cfg_sse! { return _mm_storeu_si128(mem_addr, a); }

    panic!(MISSING_MSG);
}