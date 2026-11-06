//! `RoaringTreemap` against `RoaringBitmap` on the same 1 000 000 element sets.
//!
//! `treemap` holds the elements 0..1M, so one bucket. `treemap_spread` holds the same elements
//! with the upper half of each set to `i >> 14`, so 62 buckets of 16 384 elements.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main};
use criterion::Criterion;
use criterion::BatchSize;

use rusting_bitmaps::{RoaringBitmap, RoaringTreemap};

const N: u32 = 1_000_000;

/// The operations compared, over a set of the elements of `0..N` as `map` places them
trait Set: Clone {
    fn build_by_add() -> Self;
    fn build_by_range() -> Self;
    fn add(&mut self, i: u32);
    fn remove(&mut self, i: u32) -> bool;
    fn contains(&self, i: u32) -> bool;
    fn len(&self) -> usize;
    fn and_in_place(&mut self, other: &Self);
    fn and_cardinality(&self, other: &Self) -> usize;
    fn sum(&self) -> u64;
}

impl Set for RoaringBitmap {
    fn build_by_add() -> Self {
        let mut set = RoaringBitmap::new();
        for i in 0..N {
            set.add(i);
        }

        set
    }

    fn build_by_range() -> Self {
        let mut set = RoaringBitmap::new();
        set.add_range(0..N);

        set
    }

    fn add(&mut self, i: u32) { RoaringBitmap::add(self, i) }
    fn remove(&mut self, i: u32) -> bool { RoaringBitmap::remove(self, i) }
    fn contains(&self, i: u32) -> bool { RoaringBitmap::contains(self, i) }
    fn len(&self) -> usize { RoaringBitmap::len(self) }
    fn and_in_place(&mut self, other: &Self) { self.inplace_and(other) }
    fn and_cardinality(&self, other: &Self) -> usize { RoaringBitmap::and_cardinality(self, other) }
    fn sum(&self) -> u64 { self.iter().map(u64::from).sum() }
}

/// A treemap whose element `i` is stored as `map::<SPREAD>(i)`
#[derive(Clone)]
struct Tree<const SPREAD: bool>(RoaringTreemap);

fn map<const SPREAD: bool>(i: u32) -> u64 {
    if SPREAD { (u64::from(i >> 14) << 32) | u64::from(i & 0x3FFF) } else { u64::from(i) }
}

impl<const SPREAD: bool> Set for Tree<SPREAD> {
    fn build_by_add() -> Self {
        let mut set = RoaringTreemap::new();
        for i in 0..N {
            set.add(map::<SPREAD>(i));
        }

        Tree(set)
    }

    fn build_by_range() -> Self {
        let mut set = RoaringTreemap::new();
        if SPREAD {
            for bucket in 0..(N >> 14) {
                set.add_range(map::<true>(bucket << 14)..=map::<true>((bucket << 14) | 0x3FFF));
            }
            set.add_range(map::<true>(N & !0x3FFF)..=map::<true>(N - 1));
        }
        else {
            set.add_range(0..u64::from(N));
        }

        Tree(set)
    }

    fn add(&mut self, i: u32) { self.0.add(map::<SPREAD>(i)) }
    fn remove(&mut self, i: u32) -> bool { self.0.remove(map::<SPREAD>(i)) }
    fn contains(&self, i: u32) -> bool { self.0.contains(map::<SPREAD>(i)) }
    fn len(&self) -> usize { self.0.len() }
    fn and_in_place(&mut self, other: &Self) { self.0.inplace_and(&other.0) }
    fn and_cardinality(&self, other: &Self) -> usize { self.0.and_cardinality(&other.0) }
    fn sum(&self) -> u64 { self.0.iter().fold(0u64, |sum, value| sum.wrapping_add(value)) }
}

fn bench_set<S: Set + 'static>(c: &mut Criterion, name: &str) {
    let full = S::build_by_add();
    let mut odd = S::build_by_add();
    for i in (0..N).step_by(2) {
        odd.remove(i);
    }

    c.bench_function(&format!("build_by_add/{}", name), |b| b.iter(S::build_by_add));
    c.bench_function(&format!("build_by_range/{}", name), |b| b.iter(S::build_by_range));

    c.bench_function(&format!("contains_1000/{}", name), |b| {
        b.iter(|| {
            let mut hits = 0;
            for i in (0..N).step_by((N / 1000) as usize) {
                hits += full.contains(black_box(i)) as u32;
            }

            hits
        })
    });

    c.bench_function(&format!("remove_then_add_one/{}", name), |b| {
        let mut set = full.clone();
        b.iter(|| {
            set.remove(black_box(500_000));
            set.add(black_box(500_000));
        })
    });

    c.bench_function(&format!("len/{}", name), |b| b.iter(|| full.len()));
    c.bench_function(&format!("clone/{}", name), |b| b.iter_with_large_drop(|| full.clone()));

    c.bench_function(&format!("inplace_and/{}", name), |b| {
        b.iter_batched(|| full.clone(), |mut set| { set.and_in_place(&odd); set }, BatchSize::LargeInput)
    });

    c.bench_function(&format!("and_cardinality/{}", name), |b| b.iter(|| full.and_cardinality(&odd)));
    c.bench_function(&format!("iter_sum/{}", name), |b| b.iter(|| full.sum()));
}

fn treemap_vs_bitmap(c: &mut Criterion) {
    bench_set::<RoaringBitmap>(c, "bitmap");
    bench_set::<Tree<false>>(c, "treemap");
    bench_set::<Tree<true>>(c, "treemap_spread");
}

criterion_group!(benches, treemap_vs_bitmap);
criterion_main!(benches);
