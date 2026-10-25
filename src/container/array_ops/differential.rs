//! Differential tests for the array set operations.
//!
//! Every operation is checked against a `BTreeSet` reference over a grid of input shapes: lengths
//! on both sides of the vector width, inputs that start at 0 or end at `u16::MAX`, equal, disjoint,
//! interleaved and nested sets, and sets that run out at different times.
//!
//! The SIMD kernels are `cfg(target_feature = "sse4.2")`, so those tests only exist in an SSE4.2
//! build: `RUSTFLAGS="-C target-feature=+sse4.2,+popcnt" cargo test`. The scalar tests run in
//! every build.
//!
//! On x86_64 Linux, inputs and outputs live in a [`Guarded`] allocation whose last element abuts a
//! `PROT_NONE` page, so a read or write past the end faults at the offending instruction instead
//! of silently touching neighbouring heap memory. Elsewhere they are plain vectors, where only
//! Miri checks the bounds, and the results are always checked.

use std::collections::BTreeSet;
use std::ops::Deref;
use std::slice;

use super::scalar;
#[cfg(target_feature = "sse4.2")]
use super::vector;

#[cfg(all(target_os = "linux", target_arch = "x86_64", not(miri)))]
mod guard {
    use std::ptr;

    extern "C" {
        fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
        fn mprotect(addr: *mut u8, len: usize, prot: i32) -> i32;
        fn munmap(addr: *mut u8, len: usize) -> i32;
        fn sysconf(name: i32) -> i64;
    }

    const PROT_NONE: i32 = 0;
    const PROT_READ: i32 = 1;
    const PROT_WRITE: i32 = 2;
    const MAP_PRIVATE: i32 = 0x02;
    const MAP_ANONYMOUS: i32 = 0x20;
    const SC_PAGESIZE: i32 = 30;

    /// `len` u16 elements placed so the element after the last one is on an inaccessible page
    pub struct Guarded {
        map: *mut u8,
        map_len: usize,
        data: *mut u16,
        len: usize,
    }

    impl Guarded {
        pub fn new(len: usize) -> Self {
            unsafe {
                let page = sysconf(SC_PAGESIZE) as usize;
                let bytes = len * 2;
                let data_pages = ((bytes + page - 1) / page).max(1);
                let map_len = (data_pages + 1) * page;

                let map = mmap(
                    ptr::null_mut(),
                    map_len,
                    PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS,
                    -1,
                    0
                );
                assert!(map as isize != -1, "mmap failed");
                assert_eq!(mprotect(map.add(data_pages * page), page, PROT_NONE), 0);

                let data = map.add(data_pages * page - bytes) as *mut u16;

                Guarded { map, map_len, data, len }
            }
        }

        pub fn as_ptr(&self) -> *mut u16 {
            self.data
        }

        pub fn len(&self) -> usize {
            self.len
        }
    }

    impl Drop for Guarded {
        fn drop(&mut self) {
            unsafe { munmap(self.map, self.map_len); }
        }
    }
}

// Without mmap (Miri, other platforms) the bounds are not enforced by hardware; Miri enforces
// them itself and the tests check results everywhere
#[cfg(not(all(target_os = "linux", target_arch = "x86_64", not(miri))))]
mod guard {
    pub struct Guarded {
        // Keeps the allocation alive; all access goes through `data`
        _storage: Vec<u16>,
        data: *mut u16,
        len: usize,
    }

    impl Guarded {
        pub fn new(len: usize) -> Self {
            let mut storage = vec![0u16; len];
            let data = storage.as_mut_ptr();

            Guarded { _storage: storage, data, len }
        }

        pub fn as_ptr(&self) -> *mut u16 {
            self.data
        }

        pub fn len(&self) -> usize {
            self.len
        }
    }
}

use guard::Guarded;

impl Guarded {
    fn from_slice(values: &[u16]) -> Self {
        let g = Guarded::new(values.len());
        unsafe { std::ptr::copy_nonoverlapping(values.as_ptr(), g.as_ptr(), values.len()); }
        g
    }
}

impl Deref for Guarded {
    type Target = [u16];

    fn deref(&self) -> &[u16] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
}

/// The set operations under test
#[derive(Clone, Copy, Debug)]
enum Op {
    Or,
    And,
    AndNot,
    Xor,
}

impl Op {
    fn reference(self, a: &[u16], b: &[u16]) -> Vec<u16> {
        let sa: BTreeSet<u16> = a.iter().copied().collect();
        let sb: BTreeSet<u16> = b.iter().copied().collect();

        match self {
            Op::Or => sa.union(&sb).copied().collect(),
            Op::And => sa.intersection(&sb).copied().collect(),
            Op::AndNot => sa.difference(&sb).copied().collect(),
            Op::Xor => sa.symmetric_difference(&sb).copied().collect(),
        }
    }

    /// The number of elements `out` is documented to need room for with these inputs. On x86_64 Linux
    /// the element after it is on a guard page.
    fn out_capacity(self, a: &[u16], b: &[u16]) -> usize {
        match self {
            Op::Or | Op::Xor => a.len() + b.len(),
            Op::And => a.len().min(b.len()),
            Op::AndNot => a.len(),
        }
    }
}

/// xorshift64*; the sequence only has to be deterministic and well spread
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// How the values of one input are laid out
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// 0, 1, 2, ...
    DenseFromZero,
    /// A run of consecutive values from a random base
    DenseRun,
    /// A run of consecutive values ending at `u16::MAX`
    DenseToMax,
    /// Random values from a universe about twice the length, so two draws overlap heavily
    SmallUniverse,
    /// As `SmallUniverse` with 0 forced in
    SmallUniverseWithZero,
    /// Random values across the whole range
    Sparse,
    /// Random values across the whole range with 0 and `u16::MAX` forced in
    SparseWithEnds,
    /// Values `offset + i * stride` for a small random stride and offset, so two draws interleave
    /// or collide
    Strided,
}

const SHAPES: [Shape; 8] = [
    Shape::DenseFromZero,
    Shape::DenseRun,
    Shape::DenseToMax,
    Shape::SmallUniverse,
    Shape::SmallUniverseWithZero,
    Shape::Sparse,
    Shape::SparseWithEnds,
    Shape::Strided,
];

/// Generate a sorted, duplicate-free input of exactly `len` elements
fn generate(rng: &mut Rng, shape: Shape, len: usize) -> Vec<u16> {
    let mut set = BTreeSet::new();

    let force = |set: &mut BTreeSet<u16>, v: u16| {
        if set.len() < len {
            set.insert(v);
        }
    };

    match shape {
        Shape::DenseFromZero => {
            for v in 0..len {
                force(&mut set, v as u16);
            }
        }
        Shape::DenseRun => {
            let base = rng.below(65536 - len as u64) as u16;
            for v in 0..len {
                force(&mut set, base + v as u16);
            }
        }
        Shape::DenseToMax => {
            for v in 0..len {
                force(&mut set, u16::MAX - v as u16);
            }
        }
        Shape::SmallUniverse | Shape::SmallUniverseWithZero => {
            if let Shape::SmallUniverseWithZero = shape {
                force(&mut set, 0);
            }
            let universe = (len as u64 * 2).max(1);
            while set.len() < len {
                set.insert(rng.below(universe) as u16);
            }
        }
        Shape::Sparse | Shape::SparseWithEnds => {
            if let Shape::SparseWithEnds = shape {
                force(&mut set, 0);
                force(&mut set, u16::MAX);
            }
            while set.len() < len {
                set.insert(rng.below(65536) as u16);
            }
        }
        Shape::Strided => {
            let stride = 1 + rng.below(5);
            let offset = rng.below(stride);
            for i in 0..len as u64 {
                force(&mut set, (offset + i * stride) as u16);
            }
        }
    }

    set.into_iter().collect()
}

/// Lengths around the vector width (8) and its small multiples, plus a few longer ones
const LENGTHS: [usize; 15] = [
    0, 1, 7, 8, 9, 15, 16, 17, 24, 31, 32, 33, 64, 65, 129,
];

/// Relationships between the two inputs beyond two independent draws
#[derive(Clone, Copy, Debug)]
enum Relation {
    Independent,
    Equal,
    /// `b` is the front of `a` (or all of `a` if `b` is longer)
    Prefix,
    /// `b` is the back of `a`
    Suffix,
    /// Every element of `b` is above every element of `a`
    BAbove,
    /// Every element of `a` is above every element of `b`
    AAbove,
    /// `a` holds the even values and `b` the odd values starting at 0, so they never share an
    /// element and alternate where they overlap
    Interleaved,
}

const RELATIONS: [Relation; 7] = [
    Relation::Independent,
    Relation::Equal,
    Relation::Prefix,
    Relation::Suffix,
    Relation::BAbove,
    Relation::AAbove,
    Relation::Interleaved,
];

/// 0, 1, ..., `len - 1`
fn dense_low(len: usize) -> Vec<u16> {
    (0..len as u16).collect()
}

/// A run of `len` consecutive values ending at `u16::MAX`
fn dense_high(len: usize) -> Vec<u16> {
    (0..len as u16).map(|v| u16::MAX - len as u16 + 1 + v).collect()
}

fn build_pair(
    rng: &mut Rng,
    relation: Relation,
    shape_a: Shape,
    shape_b: Shape,
    len_a: usize,
    len_b: usize
) -> (Vec<u16>, Vec<u16>) {
    match relation {
        Relation::Independent => (
            generate(rng, shape_a, len_a),
            generate(rng, shape_b, len_b)
        ),
        Relation::Equal => {
            let a = generate(rng, shape_a, len_a);
            (a.clone(), a)
        }
        Relation::Prefix => {
            let a = generate(rng, shape_a, len_a);
            let b = a[..len_b.min(a.len())].to_vec();
            (a, b)
        }
        Relation::Suffix => {
            let a = generate(rng, shape_a, len_a);
            let b = a[a.len() - len_b.min(a.len())..].to_vec();
            (a, b)
        }
        Relation::BAbove => (dense_low(len_a), dense_high(len_b)),
        Relation::AAbove => (dense_high(len_a), dense_low(len_b)),
        Relation::Interleaved => {
            let a = (0..len_a as u16).map(|v| v * 2).collect();
            let b = (0..len_b as u16).map(|v| v * 2 + 1).collect();
            (a, b)
        }
    }
}

/// Call `check` on every generated pair of inputs, in both argument orders. `check` receives
/// ordinary slices and is responsible for copying them into guarded memory.
fn for_each_case(mut check: impl FnMut(&[u16], &[u16])) {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);

    for &len_a in LENGTHS.iter() {
        for &len_b in LENGTHS.iter() {
            for &relation in RELATIONS.iter() {
                // Only `Independent` draws both inputs, so it gets the wide grid; the other relations
                // derive `b` from `a` (or ignore shapes) and get a shorter list
                let shape_pairs: &[(Shape, Shape)] = match relation {
                    Relation::Independent => &[
                        (Shape::DenseFromZero, Shape::DenseFromZero),
                        (Shape::DenseFromZero, Shape::DenseRun),
                        (Shape::DenseRun, Shape::DenseFromZero),
                        (Shape::DenseRun, Shape::DenseToMax),
                        (Shape::DenseToMax, Shape::SparseWithEnds),
                        (Shape::SmallUniverse, Shape::SmallUniverse),
                        (Shape::SmallUniverseWithZero, Shape::SmallUniverse),
                        (Shape::SmallUniverse, Shape::SmallUniverseWithZero),
                        (Shape::SmallUniverseWithZero, Shape::SmallUniverseWithZero),
                        (Shape::Sparse, Shape::Sparse),
                        (Shape::SparseWithEnds, Shape::Sparse),
                        (Shape::Sparse, Shape::SparseWithEnds),
                        (Shape::SparseWithEnds, Shape::SparseWithEnds),
                        (Shape::Strided, Shape::Strided),
                        (Shape::Strided, Shape::DenseFromZero),
                    ],
                    _ => &[
                        (Shape::DenseFromZero, Shape::DenseFromZero),
                        (Shape::SmallUniverseWithZero, Shape::DenseFromZero),
                        (Shape::SmallUniverse, Shape::DenseFromZero),
                        (Shape::SparseWithEnds, Shape::DenseFromZero),
                        (Shape::Sparse, Shape::DenseFromZero),
                        (Shape::DenseToMax, Shape::DenseFromZero),
                    ],
                };

                for &(shape_a, shape_b) in shape_pairs {
                    let (a, b) = build_pair(&mut rng, relation, shape_a, shape_b, len_a, len_b);

                    check(&a, &b);
                    check(&b, &a);
                }
            }
        }
    }

    // Longer inputs than the grid, where the kernels run many iterations and the inputs run out
    // at arbitrary points relative to each other
    for _ in 0..1500 {
        let len_a = rng.below(301) as usize;
        let len_b = rng.below(301) as usize;
        let shape_a = SHAPES[rng.below(SHAPES.len() as u64) as usize];
        let shape_b = SHAPES[rng.below(SHAPES.len() as u64) as usize];
        let (a, b) = build_pair(&mut rng, Relation::Independent, shape_a, shape_b, len_a, len_b);

        check(&a, &b);
    }
}

fn describe(op: &str, a: &[u16], b: &[u16]) -> String {
    format!("{} a={:?} b={:?}", op, a, b)
}

/// Check `f` against the reference for `op` on every case
fn check_op<F>(op: Op, f: F)
    where F: Fn(&[u16], &[u16], *mut u16) -> usize
{
    for_each_case(|a, b| check_one(op, &f, a, b));
}

fn check_one<F>(op: Op, f: &F, a: &[u16], b: &[u16])
    where F: Fn(&[u16], &[u16], *mut u16) -> usize
{
    let a = Guarded::from_slice(a);
    let b = Guarded::from_slice(b);
    let expected = op.reference(&a, &b);

    let out = Guarded::new(op.out_capacity(&a, &b));
    let count = f(&a, &b, out.as_ptr());

    assert_eq!(count, expected.len(), "{}", describe(&format!("{:?} cardinality", op), &a, &b));
    assert_eq!(&out[..count], &expected[..], "{}", describe(&format!("{:?}", op), &a, &b));
}

fn check_and_cardinality(f: impl Fn(&[u16], &[u16]) -> usize) {
    for_each_case(|a, b| {
        let a = Guarded::from_slice(a);
        let b = Guarded::from_slice(b);
        let expected = Op::And.reference(&a, &b).len();

        assert_eq!(f(&a, &b), expected, "{}", describe("and_cardinality", &a, &b));
    });
}

#[test]
fn or_scalar() {
    check_op(Op::Or, |a, b, out| unsafe { scalar::or(a, b, out) });
}

#[test]
fn and_scalar() {
    check_op(Op::And, |a, b, out| unsafe { scalar::and(a, b, out) });
}

#[test]
fn and_not_scalar() {
    check_op(Op::AndNot, |a, b, out| unsafe { scalar::and_not(a, b, out) });
}

#[test]
fn xor_scalar() {
    check_op(Op::Xor, |a, b, out| unsafe { scalar::xor(a, b, out) });
}

#[test]
fn and_cardinality_scalar() {
    check_and_cardinality(scalar::and_cardinality);
}

#[test]
#[cfg(target_feature = "sse4.2")]
fn or_vector() {
    check_op(Op::Or, |a, b, out| unsafe { vector::or(a, b, out) });
}

#[test]
#[cfg(target_feature = "sse4.2")]
fn and_vector() {
    check_op(Op::And, |a, b, out| unsafe { vector::and(a, b, out) });
}

#[test]
#[cfg(target_feature = "sse4.2")]
fn and_not_vector() {
    check_op(Op::AndNot, |a, b, out| unsafe { vector::and_not(a, b, out) });
}

#[test]
#[cfg(target_feature = "sse4.2")]
fn xor_vector() {
    check_op(Op::Xor, |a, b, out| unsafe { vector::xor(a, b, out) });
}

#[test]
#[cfg(target_feature = "sse4.2")]
fn and_cardinality_vector() {
    check_and_cardinality(vector::and_cardinality);
}

/// The dispatching entry points must reach the right kernel and agree with the reference in
/// whichever build this is
#[test]
fn dispatch_matches_reference() {
    let mut rng = Rng(0x0DDB_1A5E_5BAD_5EED);

    for _ in 0..50 {
        let len_a = rng.below(100) as usize;
        let len_b = rng.below(100) as usize;
        let (a, b) = build_pair(&mut rng, Relation::Independent, Shape::SmallUniverseWithZero, Shape::SmallUniverse, len_a, len_b);

        check_one(Op::Or, &|a, b, out| unsafe { super::or(a, b, out) }, &a, &b);
        check_one(Op::And, &|a, b, out| unsafe { super::and(a, b, out) }, &a, &b);
        check_one(Op::AndNot, &|a, b, out| unsafe { super::and_not(a, b, out) }, &a, &b);
        check_one(Op::Xor, &|a, b, out| unsafe { super::xor(a, b, out) }, &a, &b);
        assert_eq!(super::and_cardinality(&a, &b), Op::And.reference(&a, &b).len());
    }
}

/// `and_not` could read past the end of its inputs when either began with 0: the 0 was skipped by
/// advancing the start index to 1, but the end of the vector loop is a multiple of 8, so that
/// index never reached it
#[test]
fn and_not_inputs_starting_at_zero() {
    let dense: Vec<u16> = (0..40).collect();
    let dense_from_one: Vec<u16> = (1..41).collect();
    let sparse: Vec<u16> = (0..40).map(|v| v * 3).collect();
    let sparse_no_zero: Vec<u16> = (1..41).map(|v| v * 3).collect();

    let cases: [(&[u16], &[u16]); 8] = [
        (&dense, &dense),
        (&dense, &dense_from_one),
        (&dense_from_one, &dense),
        (&dense, &sparse),
        (&sparse, &dense),
        (&sparse, &sparse_no_zero),
        (&sparse_no_zero, &sparse),
        (&dense, &dense[..9]),
    ];

    let f = |a: &[u16], b: &[u16], out: *mut u16| unsafe { super::and_not(a, b, out) };
    for &(a, b) in cases.iter() {
        check_one(Op::AndNot, &f, a, b);
    }
}

/// A full-vector store at the output cursor can overrun an output sized to the shorter input. A
/// vector of `a` that stays put while `b` advances is stored once per block of `b`, so the cursor
/// can sit within 8 elements of the end of the output.
#[test]
fn and_output_sized_to_the_shorter_input() {
    // 6 of the 8 elements of `a` are in the first block of `b`, which has a lower maximum than
    // `a` so `a` is stored again against the second block
    let a: [u16; 8] = [0, 1, 2, 3, 4, 6, 7, 15];
    let b: [u16; 16] = [0, 2, 3, 4, 5, 6, 7, 8, 9, 13, 18, 26, 27, 29, 30, 31];
    // Shifted by one so the same shapes go through the `cmpistrm` loop instead of the zero
    // handling `cmpestrm` loop
    let a_no_zero = a.map(|v| v + 1);
    let b_no_zero = b.map(|v| v + 1);

    let f = |a: &[u16], b: &[u16], out: *mut u16| unsafe { super::and(a, b, out) };
    for &(a, b) in [(&a[..], &b[..]), (&a_no_zero[..], &b_no_zero[..])].iter() {
        check_one(Op::And, &f, a, b);
        check_one(Op::And, &f, b, a);
    }
}
