#![feature(ptr_offset_from)]

//#![deny(missing_docs)]   TODO: Enable these lints after sketching out the api
#![deny(bare_trait_objects)]

//! A native rust port of Roaring Bitmaps based on CRoaring with some modifications
//! 
//! # Differences
//! - Copy on write is not implemented

// # Internal notes on alignment
//
// >I seem to be struggling to find rust's malloc though.
// 
// The typical approach is to use `Vec`. It doesn't currently allow specifying alignment beyond the type, but one can allocate 
// slightly more space and then shift the start to be aligned, `Vec::with_capacity(num_elements + alignment - 1)`.

mod roaring;
mod roaring_array;
mod container;

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