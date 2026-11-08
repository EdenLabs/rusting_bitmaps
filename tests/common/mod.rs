//! The allocator counter and random numbers the emptied container tests share. The counter is per
//! thread, so tests run in parallel without seeing each other's allocations.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

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
pub fn allocations(work: impl FnOnce()) -> usize {
    let before = CALLS.with(Cell::get);
    work();

    CALLS.with(Cell::get) - before
}

pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
