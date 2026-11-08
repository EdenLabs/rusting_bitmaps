//! A bitmap that empties and refills keeps the allocation of the container it emptied, and no
//! empty container is ever visible to the bitmap's contents.
//!
//! The allocation counter is per thread, so the tests run in parallel without seeing each
//! other's allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;
use std::io::Cursor;

use rusting_bitmaps::RoaringBitmap;

struct Counting;

thread_local! {
    static CALLS: Cell<usize> = const { Cell::new(0) };
}

fn count_call() {
    // A destroyed thread local only happens while a thread exits, which counts nothing
    let _ = CALLS.try_with(|calls| calls.set(calls.get() + 1));
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_call();
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_call();
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_call();
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// The allocator calls `work` makes on this thread.
fn allocations(work: impl FnOnce()) -> usize {
    let before = CALLS.with(Cell::get);
    work();

    CALLS.with(Cell::get) - before
}

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

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
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
