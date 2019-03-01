#![feature(const_fn)]
#![feature(copy_within)]

// TODO: Enable these lints after sketching out the api
//#![deny(missing_docs)]
#![deny(bare_trait_objects)]

//! A native rust port of Roaring Bitmaps based on CRoaring with some modifications
//! 
//! # Differences
//!  - Copy on write is not implemented (maybe in the future)
//!  - Only supports x86_64 with AVX2 support

// TODO: Look at using Align<T, A> for enforcing alignment on the internal vecs with aligned loads and see whether that has a performance impact

mod roaring;
mod roaring_array;
mod container;
mod simd;
mod utils;

pub mod prelude {
    // TODO: Write prelude
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(u16);

impl From<u16> for Key {
    fn from(value: u16) -> Key {
        Key(value)
    }
}

impl From<Key> for u16 {
    fn from(key: Key) -> u16 {
        key.0
    }
}

/// Get the max of two values
fn max<T: Copy + PartialOrd>(a: T, b: T) -> T {
    if a < b {
        b
    }
    else {
        a
    }
}

// Get the min of two values
fn min<T: Copy + PartialOrd>(a: T, b: T) -> T {
    if a < b {
        a
    }
    else {
        b
    }
}

mod align {
    mod sealed {
        /// Marker trait for aligned data
        pub unsafe trait Aligned {}
    }

    use std::ops::{Deref, DerefMut};

    /// 16 byte alignment
    #[repr(align(16))] #[derive(Clone)]
    pub struct A16; unsafe impl sealed::Aligned for A16 {}

    /// 32 byte alignment
    #[repr(align(16))] #[derive(Clone)]
    #[repr(align(32))] pub struct A32; unsafe impl sealed::Aligned for A32 {}

    /// 64 byte alignment
    #[repr(align(16))] #[derive(Clone)]
    #[repr(align(64))] pub struct A64; unsafe impl sealed::Aligned for A64 {}

    /// Newtype for ensuring proper alignment of the contained type
    #[derive(Clone)]
    pub struct Align<T, A>
        where A: sealed::Aligned
    {
        _aligned: [A; 0],
        value: T
    }

    impl<T, A> Align<T, A>
        where A: sealed::Aligned
    {
        pub const fn new(value: T) -> Align<T, A> {
            Self {
                _aligned: [],
                value: value
            }
        }
    }

    impl<T, A> Deref for Align<T, A>
        where A: sealed::Aligned
    {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.value
        }
    }

    impl<T, A> DerefMut for Align<T, A>
        where A: sealed::Aligned
    {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.value
        }
    }

    #[cfg(test)]
    mod tests {
        use std::mem;
        use super::*;

        #[test]
        fn alignment_params() {
            assert_eq!(mem::align_of::<A16>(), 16);
            assert_eq!(mem::align_of::<A32>(), 32);
            assert_eq!(mem::align_of::<A64>(), 64);
        }

        #[test]
        fn alignment() {
            let a16: Align<u8, A16> = Align::new(0);
            let a32: Align<u8, A32> = Align::new(0);
            let a64: Align<u8, A64> = Align::new(0);

            assert_eq!(mem::align_of_val(&a16), 16);
            assert_eq!(mem::align_of_val(&a32), 32);
            assert_eq!(mem::align_of_val(&a64), 64);
        }

        #[test]
        fn deref() {
            let mut a: Align<[u8; 4], A16> = Align::new([0, 1, 2, 3]);

            {
                let _ra: &[u8] = &*a;

                assert!(true);
            }
            {
                let _ra_mut: &mut [u8] = &mut *a;

                assert!(true);
            }
        }
    }
}