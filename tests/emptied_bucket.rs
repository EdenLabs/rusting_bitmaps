//! A treemap that empties and refills keeps the bitmap of the bucket it dropped, and no empty
//! bucket is ever visible to the treemap's contents.
//!
//! The allocation counter is per thread, so the tests run in parallel without seeing each
//! other's allocations.

use std::collections::BTreeSet;

use rusting_bitmaps::RoaringTreemap;

mod common;

use common::{allocations, Rng};

const CYCLES: u32 = 1_000;

/// A value in the bucket `high`.
const fn at(high: u64, low: u64) -> u64 {
    high << 32 | low
}

/// Everything the treemap exposes, which must be that of the set `expected`.
fn assert_holds(set: &RoaringTreemap, expected: &BTreeSet<u64>) {
    assert_eq!(set.is_empty(), expected.is_empty());
    assert_eq!(set.len(), expected.len());
    assert_eq!(set.cardinality(), expected.len());
    assert!(set.iter().eq(expected.iter().copied()));

    for &value in expected {
        assert!(set.contains(value));
    }
    assert!(!set.contains(at(9, 9)));

    let probe = [3, at(1, 4), at(7, 7)];
    let probe_set: BTreeSet<u64> = probe.into();
    let mut other = RoaringTreemap::new();
    for value in probe {
        other.add(value);
    }

    assert_eq!(set.clone().len(), expected.len());
    assert_eq!(set.and_cardinality(&other), expected.intersection(&probe_set).count());
    assert!(set.and(&other).iter().eq(expected.intersection(&probe_set).copied()));
    assert!(set.or(&other).iter().eq(expected.union(&probe_set).copied()));
    assert!(set.and_not(&other).iter().eq(expected.difference(&probe_set).copied()));
}

/// A cycle of `work` on `set`, which must leave it holding `kept`, allocates nothing once the
/// first has run.
fn assert_cycle_allocates_nothing(
    set: &mut RoaringTreemap,
    kept: &BTreeSet<u64>,
    mut work: impl FnMut(&mut RoaringTreemap)
) {
    work(set);
    assert_holds(set, kept);

    let calls = allocations(|| {
        for _ in 0..CYCLES {
            work(set);
        }
    });

    assert_eq!(calls, 0);
    assert_holds(set, kept);
}

/// The value of the second bucket the cycles never touch, so the tree itself is never emptied and freed.
const NEIGHBOR: u64 = at(500, 1);

fn with_neighbor() -> RoaringTreemap {
    let mut set = RoaringTreemap::new();
    set.add(NEIGHBOR);

    set
}

fn neighbor() -> BTreeSet<u64> {
    [NEIGHBOR].into()
}

/// Removing the neighbor leaves the treemap empty, which it is only if none of the buckets the
/// cycles dropped is still in the tree: `is_empty` does not see an empty bucket beside another.
fn assert_only_the_neighbor_was_left(set: &mut RoaringTreemap) {
    assert_holds(set, &neighbor());
    assert!(set.remove(NEIGHBOR));
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
}

#[test]
fn remove_cycle_allocates_nothing() {
    let mut set = with_neighbor();
    assert_cycle_allocates_nothing(&mut set, &neighbor(), |set| {
        set.add(at(2, 7));
        assert!(set.remove(at(2, 7)));
    });
    assert_only_the_neighbor_was_left(&mut set);
}

/// With no other bucket the tree is empty between cycles, and the cycle still allocates nothing.
#[test]
fn remove_cycle_of_the_only_bucket_allocates_nothing() {
    assert_cycle_allocates_nothing(&mut RoaringTreemap::new(), &BTreeSet::new(), |set| {
        set.add(7);
        assert!(set.remove(7));
    });
}

#[test]
fn remove_range_cycle_allocates_nothing() {
    let mut set = with_neighbor();
    assert_cycle_allocates_nothing(&mut set, &neighbor(), |set| {
        set.add(at(2, 7));
        set.remove_range(at(2, 0)..=at(2, 9));
    });
    assert_only_the_neighbor_was_left(&mut set);
}

#[test]
fn inplace_and_not_cycle_allocates_nothing() {
    let mut other = RoaringTreemap::new();
    other.add(at(2, 7));
    let mut set = with_neighbor();

    assert_cycle_allocates_nothing(&mut set, &neighbor(), |set| {
        set.add(at(2, 7));
        set.inplace_and_not(&other);
    });
    assert_only_the_neighbor_was_left(&mut set);
}

/// The bucket is in `other` and the operation empties it, or it is in `other` nowhere and is
/// dropped with its values.
#[test]
fn inplace_and_cycle_allocates_nothing() {
    let mut shared = RoaringTreemap::new();
    shared.add(at(2, 9));
    shared.add(NEIGHBOR);

    let mut set = with_neighbor();
    assert_cycle_allocates_nothing(&mut set, &neighbor(), |set| {
        set.add(at(2, 7));
        set.inplace_and(&shared);
    });
    assert_holds(&set, &neighbor());

    let mut kept = RoaringTreemap::new();
    kept.add(NEIGHBOR);

    assert_cycle_allocates_nothing(&mut set, &neighbor(), |set| {
        set.add(at(2, 7));
        set.inplace_and(&kept);
    });
    assert_only_the_neighbor_was_left(&mut set);
}

#[test]
fn clear_cycle_allocates_only_the_tree_node() {
    let mut set = RoaringTreemap::new();
    set.add(7);
    set.clear();
    set.add(7);
    set.clear();

    let mut calls = 0;
    for _ in 0..CYCLES {
        calls += allocations(|| set.add(at(2, 7)));
        set.clear();
    }

    // The tree node is allocated again each cycle, which std's `BTreeMap` does not let a caller
    // avoid, and the bucket's bitmap is not: one allocation a cycle, not the bitmap's three
    assert!(calls <= CYCLES as usize, "{} allocations in {} cycles", calls, CYCLES);
    assert_holds(&set, &BTreeSet::new());
}

/// The treemap keeps one bitmap, so the buckets a cycle drops beyond the first are freed.
#[test]
fn treemap_keeps_one_spare() {
    let mut set = RoaringTreemap::new();
    for high in 0..4 {
        set.add(at(high, 1));
    }
    set.clear();
    assert_holds(&set, &BTreeSet::new());

    let calls = allocations(|| set.add(at(0, 1)));
    let one = calls;
    let calls = allocations(|| set.add(at(1, 1)));
    assert!(calls > one, "the second bucket was made from nothing");
    assert_holds(&set, &[at(0, 1), at(1, 1)].into());
}

/// Taking the spare bucket leaves no value of the dropped one behind.
#[test]
fn refill_after_cycle_holds_only_new_values() {
    let mut set = RoaringTreemap::new();
    set.add(at(1, 1));
    set.add(at(1, 70_000));
    set.add(at(2, 2));
    set.clear();
    assert_holds(&set, &BTreeSet::new());

    set.add(at(3, 3));
    assert_holds(&set, &[at(3, 3)].into());

    let clone = set.clone();
    set.remove(at(3, 3));
    set.add(at(5, 5));
    assert_holds(&set, &[at(5, 5)].into());
    assert_holds(&clone, &[at(3, 3)].into());

    set.add_range(at(6, 10)..at(6, 13));
    assert_holds(&set, &[at(5, 5), at(6, 10), at(6, 11), at(6, 12)].into());

    set.shrink_to_fit();
    set.remove_range(..);
    assert_holds(&set, &BTreeSet::new());
    set.add(at(8, 8));
    assert_holds(&set, &[at(8, 8)].into());
}

/// Add, removal, ranges and the in-place operations across a few buckets against a `BTreeSet`,
/// checking the visible state after every step.
#[test]
fn random_churn_matches_a_set() {
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let mut set = RoaringTreemap::new();
    let mut model = BTreeSet::new();

    for step in 0..3_000 {
        let value = at(rng.next() % 4, rng.next() % 6);
        let mut operand_model = BTreeSet::new();
        let mut operand = RoaringTreemap::new();
        for _ in 0..rng.next() % 5 {
            let v = at(rng.next() % 4, rng.next() % 6);
            operand_model.insert(v);
            operand.add(v);
        }

        match rng.next() % 10 {
            0..=2 => {
                set.add(value);
                model.insert(value);
            },
            3 => assert_eq!(set.remove(value), model.remove(&value)),
            4 => {
                // Ends in the same bucket or in a later one
                let end = value + rng.next() % 4 + (rng.next() % 3) * (1 << 32);
                set.remove_range(value..end);
                model.retain(|v| !(value..end).contains(v));
            },
            5 => {
                set.inplace_and(&operand);
                model = model.intersection(&operand_model).copied().collect();
            },
            6 | 7 => {
                set.inplace_and_not(&operand);
                model = model.difference(&operand_model).copied().collect();
            },
            8 => {
                set.inplace_or(&operand);
                model = model.union(&operand_model).copied().collect();
            },
            _ => if step % 25 == 0 {
                set.clear();
                model.clear();
            }
            else if step % 25 == 1 {
                set.shrink_to_fit();
            }
        }

        assert_holds(&set, &model);
    }
}

/// A clone holds no spare, so dropping its last bucket keeps that bucket's bitmap for the next add.
#[test]
fn clone_of_a_treemap_with_a_spare_keeps_the_bitmap_it_empties() {
    let mut set = RoaringTreemap::new();
    set.add(at(1, 1));
    set.add(at(2, 2));
    set.remove(at(2, 2));

    let mut clone = set.clone();
    assert_holds(&clone, &[at(1, 1)].into());
    clone.remove(at(1, 1));
    assert_holds(&clone, &BTreeSet::new());

    let calls = allocations(|| clone.add(at(3, 3)));

    assert_eq!(calls, 0);
    assert_holds(&clone, &[at(3, 3)].into());
    assert_holds(&set, &[at(1, 1)].into());
}
