#![cfg(test)]

use std::any::TypeId;
use std::cmp::{PartialEq, PartialOrd};
use std::ops::{Range};
use std::fmt::Debug;

use rand::prelude::*;
use rand::distributions::uniform::SampleUniform;

use num_traits::Unsigned;
use num_traits::cast::{ToPrimitive, FromPrimitive};
use num_traits::ops::checked::CheckedAdd;

use crate::container::*;

/// An internal trait for automating test setup
pub(crate) trait TestShim<T> {
    fn from_data(data: &[T]) -> Self;

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item=T> + 'a>;

    fn card(&self) -> usize;
}

/// The type of operation to perform
pub(crate) enum OpType {
    /// Performs a union of the two input sets
    Or,

    /// Performs an intersection of the two input sets
    And,

    /// Peforms a difference between the two input sets
    AndNot,

    /// Performs a symmetric difference between the two input sets
    Xor
}

const SEED: [u8; 16] = [
    3, 4, 1, 6, 3, 8, 6, 0, 
    9, 5, 4, 7, 6, 8, 1, 2
];

/// Generates a series of random data in the range [min-max) with `span_card` elements
/// generated for every `span` elements in `range`. Elements are then deduplicated and sorted
pub(crate) fn generate_data<T>(range: Range<T>, count: usize) -> Vec<T> 
    where T: Copy + Ord + Unsigned + ToPrimitive + CheckedAdd + SampleUniform
{
    let (min, max) = (range.start, range.end);
    let mut rng = rand::rngs::SmallRng::from_seed(SEED);

    let mut result: Vec<T> = Vec::with_capacity(count.to_usize().unwrap());

    while result.len() < count {
        result.push(rng.gen_range(min, max));
    }

    result.sort();
    result.dedup();

    result
}

/// Compute the result of an operation on two input sets using a known correct algorithm
/// 
/// # Remarks
/// Assumes the inputs are sorted
pub(crate) fn compute_result<T>(a: &[T], b: &[T], result: OpType) -> Vec<T> 
    where T: Ord + PartialOrd + PartialEq + Copy
{
    match result {
        OpType::Or => {
            // Compute A + B - Duplicates and maintain sorting
            let mut result = Vec::with_capacity(a.len() + b.len());
            result.extend_from_slice(a);
            result.extend_from_slice(b);
            result.sort();
            result.dedup();

            result
        },
        OpType::And => {
            let mut result = Vec::with_capacity(a.len().max(b.len()));
            
            let mut i0 = 0;
            let mut i1 = 0;
            while i0 < a.len() && i1 < b.len() {
                // Element only in A
                if a[i0] < b[i1] {
                    i0 += 1;
                }
                // Element only in B
                else if b[i1] < a[i0] {
                    i1 += 1;
                }
                // Element shared
                else {
                    result.push(a[i0]);
                    i0 += 1;
                    i1 += 1;
                }
            }

            result
        },
        OpType::AndNot => {
            let mut result = Vec::with_capacity(a.len());

            let mut i0 = 0;
            let mut i1 = 0;
            while i0 < a.len() && i1 < b.len() {
                // Element only in A
                if a[i0] < b[i1] {
                    result.push(a[i0]);
                    i0 += 1;
                }
                // Element only in B
                else if b[i1] < a[i0] {
                    i1 += 1;
                }
                // Element shared
                else {
                    i0 += 1;
                    i1 += 1;
                }
            }

            if i0 < a.len() {
                result.extend_from_slice(&a[i0..]);
            }

            result
        },
        OpType::Xor => {
            let mut result = Vec::with_capacity(a.len() + b.len());

            let mut i0 = 0;
            let mut i1 = 0;
            while i0 < a.len() && i1 < b.len() {
                // Element only in A
                if a[i0] < b[i1] {
                    result.push(a[i0]);
                    i0 += 1;
                }
                // Element only in B
                else if b[i1] < a[i0] {
                    result.push(b[i1]);
                    i1 += 1;
                }
                // Element shared
                else {
                    i0 += 1;
                    i1 += 1;
                }
            }

            if i0 < a.len() {
                result.extend_from_slice(&a[i0..]);
            }

            if i1 < b.len() {
                result.extend_from_slice(&b[i1..])
            }

            result
        }
    }
}

fn select_range<C, T>() -> (Range<T>, usize) 
    where C: 'static,
          T: Unsigned + FromPrimitive
{
    if TypeId::of::<C>() == TypeId::of::<ArrayContainer>() {
        (T::from_u32(0).unwrap()..T::from_u32(65535).unwrap(), 4000)
    }
    else if TypeId::of::<C>() == TypeId::of::<BitsetContainer>() {
        (T::from_u32(0).unwrap()..T::from_u32(65535).unwrap(), 8000)
    }
    else if TypeId::of::<C>() == TypeId::of::<RunContainer>() {
        (T::from_u32(0).unwrap()..T::from_u32(65535).unwrap(), 65535 /4)
    }
    else {
        (T::from_u32(0).unwrap()..T::from_u32(10_000_000).unwrap(), 2_000_000)
    }
}

pub(crate) fn op_test<C0, C1, T, F, R>(op: OpType, f: F)
    where C0: TestShim<T> + 'static,
          C1: TestShim<T> + 'static,
          R: TestShim<T>,
          T: Debug + Copy + Ord + Unsigned + FromPrimitive + ToPrimitive + CheckedAdd + SampleUniform,
          F: FnOnce(C0, C1) -> R,
          u64: From<T>
{
    let (range0, count0) = select_range::<C0, T>();
    let (range1, count1) = select_range::<C1, T>();

    let data_a = generate_data(range0, count0);
    let data_b = generate_data(range1, count1);
    let data_res = compute_result(&data_a, &data_b, op);

    let a = C0::from_data(&data_a);
    let b = C1::from_data(&data_b);

    let r = (f)(a, b);

    // Check that the cardinality matches the precomputed result
    assert_eq!(
        r.card(), 
        data_res.len(), 
        "Unequal cardinality; found {}, expected {}", 
        r.card(), 
        data_res.len()
    );

    // Check that the output matches the precomputed result
    for (found, expected) in r.iter().zip(data_res.iter()) {
        assert_eq!(found, *expected, "Sets are not equivalent. Found {:?}, expected {:?}", found, *expected);
    }
}

pub(crate) fn op_card_test<C0, C1, T, F>(op: OpType, f: F)
    where C0: TestShim<T> + 'static,
          C1: TestShim<T> + 'static,
          T: Debug + Copy + Ord + Unsigned + FromPrimitive + ToPrimitive + CheckedAdd + SampleUniform,
          F: FnOnce(C0, C1) -> usize,
          u64: From<T>
{
    let (range0, count0) = select_range::<C0, T>();
    let (range1, count1) = select_range::<C1, T>();
    let data_a = generate_data(range0, count0);
    let data_b = generate_data(range1, count1);
    let data_res = compute_result(&data_a, &data_b, op);

    let a = C0::from_data(&data_a);
    let b = C1::from_data(&data_b);

    let r = (f)(a, b);

    // Check that the cardinality matches the precomputed result
    assert_eq!(
        r, 
        data_res.len(), 
        "Unequal cardinality; found {}, expected {}", 
        r, 
        data_res.len()
    );
}

pub(crate) fn op_subset_test<C0, C1, T>()
    where C0: TestShim<T> + Subset<C1> + 'static,
          C1: TestShim<T> + Subset<C0> + 'static,
          T: Debug + Copy + Ord + Unsigned + FromPrimitive + ToPrimitive + CheckedAdd + SampleUniform,
          u64: From<T>
{
    let (range, count) = select_range::<C0, T>();
    let data_a = generate_data(range, count);

    let count = data_a.len() / 2;
    let mut data_b = Vec::with_capacity(count);
    data_b.extend_from_slice(&data_a[..count]);

    let a = C1::from_data(&data_a);
    let b = C0::from_data(&data_b);

    // Check that the cardinality matches the precomputed result
    assert!(b.subset_of(&a));
    assert!(!a.subset_of(&b));
}