//! Single-threaded id-allocator churn workloads for `RoaringBitmap`.
//!
//! Usage: `churn <workload> <set> <ops>` prints the ns per op. `churn mem <set>` prints the
//! heap bytes per element. A plain binary rather than criterion so that `perf stat` can count
//! instructions: the instructions per op are the difference between two op counts, which
//! cancels the set construction cost.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use rusting_bitmaps::RoaringBitmap;

/// Live heap bytes, for the bytes per element report.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(new_size, Ordering::Relaxed);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// xorshift64, so the workloads are deterministic and dependency free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        (self.next() >> 11) % n
    }
}

/// Number of ids acquired then released per churn round.
const BATCH: usize = 16;

fn build(set: &str) -> RoaringBitmap {
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);

    match set {
        // 100 ids in one array container
        "small" => {
            let mut bitmap = RoaringBitmap::new();
            while bitmap.len() < 100 {
                bitmap.add(rng.below(4096) as u32);
            }

            bitmap
        },
        // 1M ids, every 32nd value over 32M: ~2048 per container, all arrays
        "sparse" => {
            let data: Vec<u32> = (0..1_000_000u32).map(|i| i * 32).collect();
            RoaringBitmap::from_slice(&data)
        },
        // ~1M ids, a random half of 0..2M: ~32768 per container, all bitsets
        "dense" => {
            let mut data = Vec::new();
            for i in 0..2_000_000u32 {
                if rng.next() & 1 == 0 {
                    data.push(i);
                }
            }

            RoaringBitmap::from_slice(&data)
        },
        // 1M contiguous ids, a fresh allocator's free range
        "range" => RoaringBitmap::from_range(0..1_000_000),
        // exactly the array container limit, 4096 even ids in one container
        "edge" => {
            let data: Vec<u32> = (0..4096u32).map(|i| i * 2).collect();
            RoaringBitmap::from_slice(&data)
        },
        other => panic!("unknown set {other}")
    }
}

/// A sample of present values, for take/contains.
fn sample(bitmap: &RoaringBitmap) -> Vec<u32> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let all: Vec<u32> = bitmap.iter().collect();

    (0..4096).map(|_| all[rng.below(all.len() as u64) as usize]).collect()
}

/// Acquire the lowest `BATCH` ids then release them in shuffled order.
fn run_churn(bitmap: &mut RoaringBitmap, ops: usize, fused: bool) -> u64 {
    let mut rng = Rng(7);
    let mut held = [0u32; BATCH];
    let mut acc = 0u64;

    for _ in 0..ops / BATCH {
        for slot in held.iter_mut() {
            let id = if fused { bitmap.pop_min() } else { pop(bitmap) }.unwrap();
            *slot = id;
            acc += u64::from(id);
        }

        for i in (1..BATCH).rev() {
            held.swap(i, rng.below(i as u64 + 1) as usize);
        }

        for id in held {
            bitmap.add(id);
        }
    }

    acc
}

fn pop(bitmap: &mut RoaringBitmap) -> Option<u32> {
    let id = bitmap.iter().next()?;
    bitmap.remove(id);

    Some(id)
}

fn run_take(bitmap: &mut RoaringBitmap, ops: usize, fused: bool) -> u64 {
    let ids = sample(bitmap);
    let mut acc = 0u64;

    for i in 0..ops {
        let id = ids[i & 4095];
        if fused {
            acc += u64::from(bitmap.remove(id));
        }
        else if bitmap.contains(id) {
            bitmap.remove(id);
            acc += 1;
        }

        bitmap.add(id);
    }

    acc
}

fn run_contains(bitmap: &mut RoaringBitmap, ops: usize) -> u64 {
    let ids = sample(bitmap);
    let mut acc = 0u64;

    for i in 0..ops {
        let id = ids[i & 4095];
        // alternate a likely hit with a likely miss
        let probe = if i & 1 == 0 { id } else { id ^ 1 };
        acc += u64::from(bitmap.contains(probe));
    }

    acc
}

fn run_min(bitmap: &mut RoaringBitmap, ops: usize, fused: bool) -> u64 {
    let mut acc = 0u64;

    for _ in 0..ops {
        acc += u64::from(if fused { bitmap.min() } else { bitmap.iter().next() }.unwrap());
    }

    acc
}

/// Cross the array/bitset threshold back and forth: one add then three more, per op.
fn run_edge(bitmap: &mut RoaringBitmap, ops: usize) -> u64 {
    for i in 0..ops as u32 {
        let new = (i % 4096) * 2 + 1;
        let old = (i % 4096) * 2;

        bitmap.add(new);
        bitmap.remove(new);
        bitmap.remove(old);
        bitmap.add(old);
    }

    bitmap.len() as u64
}

/// Reserve small gaps above everything in the set, as the allocator does for a specific id.
fn run_add_range(bitmap: &mut RoaringBitmap, ops: usize) -> u64 {
    let base = 1u32 << 26;

    for i in 0..ops as u32 {
        let start = base + i * 5;
        bitmap.add_range(start..start + 3);
    }

    bitmap.len() as u64
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args[0] == "mem" {
        let before = LIVE.load(Ordering::Relaxed);
        let mut bitmap = build(&args[1]);
        let built = LIVE.load(Ordering::Relaxed) - before;
        let len = bitmap.len();

        run_churn(&mut bitmap, 160_000, false);
        let churned = LIVE.load(Ordering::Relaxed) - before;

        println!(
            "{} len {} built {:.4} B/elem churned {:.4} B/elem",
            args[1], len, built as f64 / len as f64, churned as f64 / bitmap.len() as f64
        );

        return;
    }

    let ops: usize = args[2].parse().unwrap();
    let mut bitmap = build(&args[1]);

    let start = Instant::now();
    let acc = match args[0].as_str() {
        "churn" => run_churn(&mut bitmap, ops, false),
        "churn_pop_min" => run_churn(&mut bitmap, ops, true),
        "take" => run_take(&mut bitmap, ops, false),
        "take_remove" => run_take(&mut bitmap, ops, true),
        "contains" => run_contains(&mut bitmap, ops),
        "min" => run_min(&mut bitmap, ops, false),
        "min_api" => run_min(&mut bitmap, ops, true),
        "edge" => run_edge(&mut bitmap, ops),
        "add_range" => run_add_range(&mut bitmap, ops),
        other => panic!("unknown workload {other}")
    };
    let elapsed = start.elapsed();
    black_box(acc);

    println!("{} {} {:.2} ns/op", args[0], args[1], elapsed.as_nanos() as f64 / ops as f64);
}
