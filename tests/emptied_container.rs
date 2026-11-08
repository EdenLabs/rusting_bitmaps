//! A bitmap that empties and refills keeps the allocation of the container it emptied, and no
//! empty container is ever visible to the bitmap's contents.
//!
//! The allocation counter is per thread, so the tests run in parallel without seeing each
//! other's allocations.

use std::collections::BTreeSet;
use std::io::Cursor;

use rusting_bitmaps::RoaringBitmap;

mod common;

use common::{allocations, Rng};

const CYCLES: u32 = 1_000;

/// Everything the bitmap exposes, which must be that of the set `expected`.
fn assert_holds(bitmap: &RoaringBitmap, expected: &BTreeSet<u32>) {
    assert_eq!(bitmap.is_empty(), expected.is_empty());
    assert_eq!(bitmap.len(), expected.len());
    assert_eq!(bitmap.cardinality(), expected.len());
    assert_eq!(bitmap.min(), expected.iter().next().copied());
    assert_eq!(bitmap.max(), expected.iter().next_back().copied());
    assert!(bitmap.iter().eq(expected.iter().copied()));

    for (rank, &value) in expected.iter().enumerate() {
        assert_eq!(bitmap.select(rank as u32), Some(value));
        assert_eq!(bitmap.rank(value), rank + 1);
    }
    assert_eq!(bitmap.select(expected.len() as u32), None);

    // The format has no empty containers, so equal contents serialize to equal bytes
    let values: Vec<u32> = expected.iter().copied().collect();
    let fresh = RoaringBitmap::from_slice(&values);
    let mut bytes = Vec::new();
    let written = bitmap.serialize(&mut bytes).unwrap();
    let mut fresh_bytes = Vec::new();
    fresh.serialize(&mut fresh_bytes).unwrap();

    assert_eq!(written, bitmap.serialized_size());
    assert_eq!(bytes, fresh_bytes);

    let read = RoaringBitmap::deserialize(&mut Cursor::new(&bytes)).unwrap();
    assert!(read.iter().eq(expected.iter().copied()));

    let other = RoaringBitmap::from_slice(&[1, 70_000]);
    assert!(bitmap.subset_of(&bitmap.or(&other)));
    assert_eq!(bitmap.or(&other).len(), expected.union(&[1, 70_000].into()).count());
    assert_eq!(bitmap.and(&other).len(), expected.intersection(&[1, 70_000].into()).count());
    assert_eq!(bitmap.xor(&other).len(), expected.symmetric_difference(&[1, 70_000].into()).count());
    assert_eq!(bitmap.clone().len(), expected.len());
}

#[test]
fn pop_min_cycle_allocates_nothing() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.add(0);
    bitmap.pop_min();

    let calls = allocations(|| {
        for i in 0..CYCLES {
            bitmap.add(i);
            assert_eq!(bitmap.pop_min(), Some(i));
        }
    });

    assert_eq!(calls, 0);
    assert_holds(&bitmap, &BTreeSet::new());
}

#[test]
fn remove_cycle_allocates_nothing() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.add(0);
    bitmap.remove(0);

    let calls = allocations(|| {
        for i in 0..CYCLES {
            bitmap.add(i);
            assert!(bitmap.remove(i));
        }
    });

    assert_eq!(calls, 0);
    assert_holds(&bitmap, &BTreeSet::new());
}

#[test]
fn remove_slice_and_range_cycles_allocate_nothing() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.add_slice(&[1, 2, 3]);
    bitmap.remove_slice(&[1, 2, 3]);

    let calls = allocations(|| {
        for i in 0..CYCLES {
            bitmap.add_slice(&[i, i + 1, i + 2]);
            bitmap.remove_slice(&[i, i + 1, i + 2]);

            bitmap.add(i);
            bitmap.remove_range(i..=i);
        }
    });

    assert_eq!(calls, 0);
    assert_holds(&bitmap, &BTreeSet::new());
}

#[test]
fn clear_cycle_allocates_nothing() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.add_slice(&[1, 2, 3]);
    bitmap.clear();

    let calls = allocations(|| {
        for i in 0..CYCLES {
            bitmap.add_slice(&[i, i + 1, i + 2]);
            bitmap.clear();
        }
    });

    assert_eq!(calls, 0);
    assert_holds(&bitmap, &BTreeSet::new());
}

/// The emptied container is reused for a key other than its own.
#[test]
fn spare_serves_any_key() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.add(5);
    bitmap.pop_min();

    let calls = allocations(|| {
        bitmap.add(3 << 16 | 9);
    });

    assert_eq!(calls, 0);
    assert_holds(&bitmap, &[3 << 16 | 9].into());
    assert_eq!(bitmap.pop_min(), Some(3 << 16 | 9));
    assert_holds(&bitmap, &BTreeSet::new());
}

/// An emptied container never stays in the bitmap: refilling after a cycle holds exactly the
/// new values, and a spare is not taken twice.
#[test]
fn refill_after_cycle_holds_only_new_values() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.add(1);
    bitmap.pop_min();
    assert_holds(&bitmap, &BTreeSet::new());

    bitmap.add(2 << 16);
    bitmap.add(7 << 16);
    bitmap.add(2 << 16 | 1);
    assert_holds(&bitmap, &[2 << 16, 2 << 16 | 1, 7 << 16].into());

    let clone = bitmap.clone();
    bitmap.clear();
    assert_holds(&bitmap, &BTreeSet::new());
    assert_holds(&clone, &[2 << 16, 2 << 16 | 1, 7 << 16].into());

    bitmap.add(40);
    assert_holds(&bitmap, &[40].into());

    bitmap.shrink_to_fit();
    bitmap.pop_min();
    assert_holds(&bitmap, &BTreeSet::new());
}

/// A bitmap holding one run container, which only deserializing builds: key 0, ids 10..=20.
fn run_bitmap() -> RoaringBitmap {
    let bytes = [
        0x3B, 0x30, 0x00, 0x00, // cookie with runs, one container
        0x01,                   // the container is a run
        0x00, 0x00, 0x0A, 0x00, // key 0, cardinality 11
        0x01, 0x00,             // one run
        0x0A, 0x00, 0x0A, 0x00  // starting at 10, 10 more
    ];

    let bitmap = RoaringBitmap::deserialize(&mut Cursor::new(&bytes[..])).unwrap();
    // Not `assert_holds`: a run container serializes as runs where a rebuilt bitmap writes an array
    assert!(bitmap.iter().eq(10..=20));

    bitmap
}

/// A removal that empties a run container leaves nothing behind, whether a range covers the
/// whole run or single values drain it.
#[test]
fn emptied_run_leaves_no_container() {
    let mut bitmap = run_bitmap();
    bitmap.remove_range(..);
    assert_holds(&bitmap, &BTreeSet::new());

    let mut bitmap = run_bitmap();
    bitmap.remove_range(10..=20);
    assert_holds(&bitmap, &BTreeSet::new());

    let mut bitmap = run_bitmap();
    while bitmap.pop_min().is_some() {}
    assert_holds(&bitmap, &BTreeSet::new());

    let mut bitmap = run_bitmap();
    bitmap.remove_slice(&(10..=20).collect::<Vec<_>>());
    assert_holds(&bitmap, &BTreeSet::new());

    bitmap.add(3 << 16);
    assert_holds(&bitmap, &[3 << 16].into());
}

/// A removal that empties a bitset container keeps no spare, and the bitmap holds nothing
/// afterwards.
#[test]
fn emptied_bitset_leaves_no_container() {
    let mut bitmap = RoaringBitmap::from_range(0..60_000);
    bitmap.remove_range(..);
    assert_holds(&bitmap, &BTreeSet::new());

    let mut bitmap = RoaringBitmap::from_slice(&(0..10_000).map(|i| i * 3).collect::<Vec<_>>());
    while bitmap.pop_min().is_some() {}
    assert_holds(&bitmap, &BTreeSet::new());

    bitmap.add(3);
    assert_holds(&bitmap, &[3].into());
}

/// Random removal and insertion across a few keys against a `BTreeSet`, checking the visible
/// state after every step that can empty a container.
#[test]
fn random_churn_matches_a_set() {
    for limit in [1usize, 3, 4096] {
        let mut rng = Rng(0x2545_F491_4F6C_DD1D ^ limit as u64);
        let mut bitmap = RoaringBitmap::with_array_limit(limit);
        let mut set = BTreeSet::new();

        for step in 0..3_000 {
            let value = ((rng.next() % 4) << 16 | (rng.next() % 6)) as u32;

            match rng.next() % 8 {
                0..=2 => {
                    bitmap.add(value);
                    set.insert(value);
                },
                3 => assert_eq!(bitmap.remove(value), set.remove(&value)),
                4 => assert_eq!(bitmap.pop_min(), set.pop_first()),
                5 => {
                    bitmap.remove_slice(&[value, value ^ 1]);
                    set.remove(&value);
                    set.remove(&(value ^ 1));
                },
                6 => {
                    let end = value + 3;
                    bitmap.remove_range(value..end);
                    for v in value..end {
                        set.remove(&v);
                    }
                },
                _ => if step % 50 == 0 {
                    bitmap.clear();
                    set.clear();
                }
            }

            assert_holds(&bitmap, &set);
        }
    }
}

/// A cycle of `work` on `bitmap`, which must leave it empty, allocates nothing once the first
/// has run.
fn assert_cycle_allocates_nothing(bitmap: &mut RoaringBitmap, mut work: impl FnMut(&mut RoaringBitmap)) {
    work(bitmap);
    assert_holds(bitmap, &BTreeSet::new());

    let calls = allocations(|| {
        for _ in 0..CYCLES {
            work(bitmap);
        }
    });

    assert_eq!(calls, 0);
    assert_holds(bitmap, &BTreeSet::new());
}

#[test]
fn inplace_and_not_cycle_allocates_nothing() {
    let other = RoaringBitmap::from_slice(&[7]);

    assert_cycle_allocates_nothing(&mut RoaringBitmap::new(), |bitmap| {
        bitmap.add(7);
        bitmap.inplace_and_not(&other);
    });
}

#[test]
fn inplace_xor_cycle_allocates_nothing() {
    let other = RoaringBitmap::from_slice(&[7]);

    assert_cycle_allocates_nothing(&mut RoaringBitmap::new(), |bitmap| {
        bitmap.add(7);
        bitmap.inplace_xor(&other);
    });
}

/// The container is in both bitmaps and the operation empties it.
#[test]
fn inplace_and_of_a_shared_key_cycle_allocates_nothing() {
    let other = RoaringBitmap::from_slice(&[9]);

    assert_cycle_allocates_nothing(&mut RoaringBitmap::new(), |bitmap| {
        bitmap.add(7);
        bitmap.inplace_and(&other);
    });
}

/// The container's key is in `other` nowhere, before its keys, between them and after them.
#[test]
fn inplace_and_of_other_keys_cycle_allocates_nothing() {
    let after = RoaringBitmap::from_slice(&[5 << 16]);
    assert_cycle_allocates_nothing(&mut RoaringBitmap::new(), |bitmap| {
        bitmap.add(7);
        bitmap.inplace_and(&after);
    });

    // The drained containers are the ones before a shared key
    let between = RoaringBitmap::from_slice(&[5 << 16 | 1]);
    let mut bitmap = RoaringBitmap::new();
    bitmap.add(5 << 16 | 1);
    bitmap.add(3 << 16 | 7);
    bitmap.inplace_and(&between);
    let calls = allocations(|| {
        for _ in 0..CYCLES {
            bitmap.add(3 << 16 | 7);
            bitmap.inplace_and(&between);
        }
    });
    assert_eq!(calls, 0);
    assert_holds(&bitmap, &[5 << 16 | 1].into());

    let before = RoaringBitmap::from_slice(&[1]);
    let mut bitmap = RoaringBitmap::new();
    bitmap.add(1);
    bitmap.add(3 << 16 | 7);
    bitmap.inplace_and(&before);
    let calls = allocations(|| {
        for _ in 0..CYCLES {
            bitmap.add(3 << 16 | 7);
            bitmap.inplace_and(&before);
        }
    });
    assert_eq!(calls, 0);
    assert_holds(&bitmap, &[1].into());

    let none = RoaringBitmap::new();
    assert_cycle_allocates_nothing(&mut RoaringBitmap::new(), |bitmap| {
        bitmap.add(7);
        bitmap.inplace_and(&none);
    });
}

/// Flipping exactly the values a container holds leaves it empty. The flip allocates the
/// vectors it splices in itself, so the add after it is the one counted.
#[test]
fn inplace_not_keeps_the_allocation_of_the_container_it_empties() {
    let mut bitmap = RoaringBitmap::new();
    bitmap.add(7);
    bitmap.inplace_not(7..=7);
    assert_holds(&bitmap, &BTreeSet::new());

    let mut calls = 0;
    for _ in 0..CYCLES {
        calls += allocations(|| bitmap.add_slice(&[10, 11, 12, 13]));
        bitmap.inplace_not(10..14);
        assert_holds(&bitmap, &BTreeSet::new());
    }

    assert_eq!(calls, 0);
}

#[test]
fn copy_from_keeps_the_allocation_it_overwrites() {
    let mut bitmap = RoaringBitmap::from_slice(&[1, 2, 3]);
    bitmap.copy_from(&RoaringBitmap::new());
    assert_holds(&bitmap, &BTreeSet::new());

    let calls = allocations(|| bitmap.add(9));

    assert_eq!(calls, 0);
    assert_holds(&bitmap, &[9].into());
}

/// The bitmap keeps one spare, so a set that empties many containers at once frees the rest.
#[test]
fn inplace_ops_keep_one_spare() {
    let keys: Vec<u32> = (0..8).map(|key| key << 16 | 7).collect();
    let mut bitmap = RoaringBitmap::from_slice(&keys);
    bitmap.inplace_and_not(&RoaringBitmap::from_slice(&keys));
    assert_holds(&bitmap, &BTreeSet::new());

    // The vectors of the bitmap kept their capacity, so what is left is the array of each key but one
    let calls = allocations(|| bitmap.add_slice(&keys));

    assert_eq!(calls, keys.len() - 1);
    assert_holds(&bitmap, &keys.iter().copied().collect());
}

/// A bitset or run container the in-place operations empty is gone, and the bitmap refills as arrays.
#[test]
fn emptied_bitset_and_run_by_inplace_ops_leave_no_container() {
    let dense: Vec<u32> = (0..10_000).map(|i| i * 3).collect();
    let bitset = RoaringBitmap::from_slice(&dense);
    let run = RoaringBitmap::from_range(0..60_000);
    let expected: BTreeSet<u32> = dense.iter().copied().collect();

    for operand in [&bitset, &run] {
        let mut bitmap = bitset.clone();
        bitmap.inplace_and_not(operand);
        assert_holds(&bitmap, &BTreeSet::new());
    }

    // A bitset and a run container, each emptied by `xor` with itself and by `and` with a key
    // and a value it lacks
    for start in [&bitset, &run] {
        let mut bitmap = start.clone();
        bitmap.inplace_xor(start);
        assert_holds(&bitmap, &BTreeSet::new());

        for lacking in [1u32 << 20, 60_000] {
            let mut bitmap = start.clone();
            bitmap.inplace_and(&RoaringBitmap::from_slice(&[lacking]));
            let model: BTreeSet<u32> = start.iter().filter(|&v| v == lacking).collect();
            assert_holds(&bitmap, &model);
        }

        let mut bitmap = start.clone();
        bitmap.inplace_and(&RoaringBitmap::from_slice(&[1 << 20]));
        bitmap.add(3);
        assert_holds(&bitmap, &[3].into());
    }

    let mut bitmap = run.clone();
    bitmap.inplace_and_not(&run);
    assert_holds(&bitmap, &BTreeSet::new());
    bitmap.inplace_or(&bitset);
    assert_holds(&bitmap, &expected);

    let mut bitmap = bitset.clone();
    bitmap.inplace_not(..);
    bitmap.inplace_not(..);
    assert_holds(&bitmap, &expected);
}

/// The in-place operations on bitmaps of several array limits against a `BTreeSet`, checking
/// the visible state after every step.
#[test]
fn random_inplace_ops_match_a_set() {
    for limit in [1usize, 3, 4096] {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ limit as u64);
        let mut bitmap = RoaringBitmap::with_array_limit(limit);
        let mut set = BTreeSet::new();

        for step in 0..3_000 {
            let mut operand_set = BTreeSet::new();
            for _ in 0..rng.next() % 5 {
                operand_set.insert(((rng.next() % 4) << 16 | (rng.next() % 6)) as u32);
            }
            let operand_values: Vec<u32> = operand_set.iter().copied().collect();
            let operand = RoaringBitmap::from_slice(&operand_values);
            let value = ((rng.next() % 4) << 16 | (rng.next() % 6)) as u32;

            match rng.next() % 10 {
                0 | 1 => {
                    bitmap.add(value);
                    set.insert(value);
                },
                2 => {
                    bitmap.inplace_and(&operand);
                    set = set.intersection(&operand_set).copied().collect();
                },
                3 | 4 => {
                    bitmap.inplace_and_not(&operand);
                    set = set.difference(&operand_set).copied().collect();
                },
                5 | 6 => {
                    bitmap.inplace_xor(&operand);
                    set = set.symmetric_difference(&operand_set).copied().collect();
                },
                7 => {
                    bitmap.inplace_or(&operand);
                    set = set.union(&operand_set).copied().collect();
                },
                8 => {
                    let end = value + (rng.next() % 4) as u32;
                    bitmap.inplace_not(value..=end);
                    for v in value..=end {
                        if !set.remove(&v) {
                            set.insert(v);
                        }
                    }
                },
                _ => if step % 7 == 0 {
                    bitmap.copy_from(&operand);
                    set = operand_set;
                }
            }

            assert_holds(&bitmap, &set);
        }
    }
}

/// A clone holds no spare, so removing its last value keeps that container's allocation for the next add.
#[test]
fn clone_of_a_bitmap_with_a_spare_keeps_the_allocation_it_empties() {
    let mut bitmap = RoaringBitmap::from_slice(&[1, 2]);
    bitmap.add(1 << 16);
    bitmap.remove(1 << 16);

    let mut clone = bitmap.clone();
    assert_holds(&clone, &[1, 2].into());
    clone.remove_slice(&[1, 2]);
    assert_holds(&clone, &BTreeSet::new());

    let calls = allocations(|| clone.add(5));

    assert_eq!(calls, 0);
    assert_holds(&clone, &[5].into());
    assert_holds(&bitmap, &[1, 2].into());
}
