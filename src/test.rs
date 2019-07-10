#![cfg(test)]

use std::cmp::{PartialEq, PartialOrd};
use std::ops::{Range};

use rand::prelude::*;

/// An internal trait for automating test setup
pub trait TestShim<T> {
    fn from_data(data: &[T]) -> Self;

    fn iter(&self) -> Box<dyn Iterator<Type=T>>;

    fn card(&self) -> usize;
}

/// The type of operation to perform
pub enum OpType {
    /// Performs a union of the two input sets
    Or,

    /// Performs an intersection of the two input sets
    And,

    /// Peforms a difference between the two input sets
    AndNot,

    /// Performs a symmetric difference between the two input sets
    Xor
}

const SEED: u64 = 4532158965;

/// Generates a series of random data in the range [min-max) with `span_card` elements
/// generated for every `span` elements in `range`. Elements are then deduplicated and sorted
pub fn generate_data<T, R>(range: Range<T>, span: u64, span_card: u64) -> Vec<T> 
    where T: PartialOrd + PartialEq + Into<u64>
{
    assert!(span > span_card);

    let (min, max) = { (u64::from(range.start), u64::from(range.end)) };
    let rng = rand::SeedableRng::from_seed(SEED);

    let est_cap = (range.len() / span) * span_card;
    let mut result = Vec::with_capacity(est_cap);
    let mut block = Vec::with_capacity(span_card);

    let mut start: u64 = u64::from(min);
    let mut end: u64 = span;
    while end <= max {
        while block.len() < span_card {
            block.push(rng.gen_range(start, end));
        }

        result.append(block);

        start = end;
        end = start + span + 1;
    }

    result.sort();
    result.dedup();

    result
}

/// Compute the result of an operation on two input sets using a known correct algorithm
/// 
/// # Remarks
/// Assumes the inputs are sorted
pub fn compute_result<T>(a: &[T], b: &[T], result: OpType) -> Vec<T> 
    where T: PartialOrd + PartialEq
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
                    result.push(a[i1]);
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

pub fn op_test<C0, C1, T, F, R>(op: OpType, range: Range<T>, span: u64, span_card: u64, f: F)
    where C0: TestShim<T>,
          C1: TestShim<T>,
          R: TestShim<T>,
          T: PartialEq + PartialOrd,
          F: FnOnce(C0, C1) -> R
{
    let data_a = generate_data(range, span, span_card);
    let data_b = generate_data(range, span, span_card);
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
        assert_eq!(found, *expected, "Sets are not equivalent. Found {}, expected {}", found, *expected);
    }
}

pub fn op_card_test<C0, C1, T, F, R>(op: OpType, range: Range<T>, span: u64, span_card: u64, f: F)
    where C0: TestShim<T>,
          C1: TestShim<T>,
          R: TestShim<T>,
          T: PartialEq + PartialOrd,
          F: FnOnce(C0, C1) -> R
{
    let data_a = generate_data(range, span, span_card);
    let data_b = generate_data(range, span, span_card);
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
}