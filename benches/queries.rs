#[macro_use]
extern crate criterion;

use std::ops::Range;

use criterion::Criterion;
use criterion::black_box;

use rand::prelude::*;

use rusting_bitmaps::RoaringBitmap;

const SEED0: [u8; 16] = [
    3, 4, 1, 6, 3, 8, 6, 0, 
    9, 5, 4, 7, 6, 8, 1, 2
];

const SEED1: [u8; 16] = [
    3, 4, 1, 6, 3, 8, 6, 0, 
    9, 5, 4, 7, 6, 8, 1, 2
];

fn generate_seeded_data(range: Range<u32>, count: usize, seed: [u8; 16]) -> Vec<u32> {
    let (min, max) = (range.start, range.end);

    let mut rng = rand::rngs::SmallRng::from_seed(seed);
    let mut result = Vec::with_capacity(count);

    // Fill the range
    for i in range {
        result.push(i);
    }

    // Randomly remove values till we have the desired number
    while result.len() > count {
        let index = rng.gen_range(0, result.len());
        result.swap_remove(index);
    }

    result.sort();
    result.dedup();

    result
}

fn setup_large() -> (RoaringBitmap, RoaringBitmap) {
    let data_a = generate_seeded_data(0..5_000_000, 4_000_000, SEED0);
    let data_b = generate_seeded_data(0..5_000_000, 4_000_000, SEED1);

    let a = RoaringBitmap::from_slice(&data_a);
    let b = RoaringBitmap::from_slice(&data_b);

    (a, b)
}

fn setup_small() -> (RoaringBitmap, RoaringBitmap) {
    let data_a = generate_seeded_data(0..2_000_000, 500_000, SEED0);
    let data_b = generate_seeded_data(0..2_000_000, 500_000, SEED1);

    let a = RoaringBitmap::from_slice(&data_a);
    let b = RoaringBitmap::from_slice(&data_b);

    (a, b)
}


fn or_large(c: &mut Criterion) {
    let (a, b) = setup_large();

    c.bench_function("roaring or - large", move |bencher| {
        bencher.iter_with_large_drop(|| a.or(&b) )
    });
}

fn and_large(c: &mut Criterion) {
    let (a, b) = setup_large();

    c.bench_function("roaring and - large", move |bencher| {
        bencher.iter_with_large_drop(|| a.and(&b) )
    });
}

fn and_not_large(c: &mut Criterion) {
    let (a, b) = setup_large();

    c.bench_function("roaring and not - large", move |bencher| {
        bencher.iter_with_large_drop(|| a.and_not(&b) )
    });
}

fn xor_large(c: &mut Criterion) {
    let (a, b) = setup_large();

    c.bench_function("roaring xor - large", move |bencher| {
        bencher.iter_with_large_drop(|| a.xor(&b) )
    });
}

fn or_small(c: &mut Criterion) {
    let (a, b) = setup_small();

    c.bench_function("roaring or - small", move |bencher| {
        bencher.iter_with_large_drop(|| a.or(&b) )
    });
}

fn and_small(c: &mut Criterion) {
    let (a, b) = setup_small();

    c.bench_function("roaring and - small", move |bencher| {
        bencher.iter_with_large_drop(|| a.and(&b) )
    });
}

fn and_not_small(c: &mut Criterion) {
    let (a, b) = setup_small();

    c.bench_function("roaring and not - small", move |bencher| {
        bencher.iter_with_large_drop(|| a.and_not(&b) )
    });
}

fn xor_small(c: &mut Criterion) {
    let (a, b) = setup_small();

    c.bench_function("roaring xor - small", move |bencher| {
        bencher.iter_with_large_drop(|| a.xor(&b) )
    });
}

fn inplace_or(c: &mut Criterion) {
    
}

fn inplace_and(c: &mut Criterion) {

}

fn inplace_and_not(c: &mut Criterion) {

}

fn inplace_xor(c: &mut Criterion) {

}

criterion_group!(roaring_large, or_large, and_large, and_not_large, xor_large);
criterion_group!(roaring_small, or_small, and_small, and_not_small, xor_small);
criterion_group!(roaring_inplace, inplace_or, inplace_and, inplace_and_not, inplace_xor);

criterion_main!(roaring_large, roaring_small);