use std::mem;
use std::arch::x86_64::{
    __m256i,

    _mm256_lddqu_si256,
    _mm256_storeu_si256,
    _mm256_setzero_si256,
    _mm256_xor_si256,
    _mm256_or_si256,
    _mm256_and_si256,
    _mm256_andnot_si256,
    _mm256_slli_epi64,
    _mm256_add_epi64,
    _mm256_extract_epi64,
    _mm256_srli_epi32,
    _mm256_set1_epi8,
    _mm256_setr_epi8,
    _mm256_shuffle_epi8,
    _mm256_add_epi8,
    _mm256_sad_epu8
};

use crate::container::bitset::BITSET_SIZE_IN_WORDS;

// NOTE: These are never intended to be used outside the bitset container impl

const WORDS_IN_REGISTER: usize = 4;

macro_rules! bitmap_op {
    ($name: ident, $intrinsic: ident) => {
        pub unsafe fn $name(a: &[u64], b: &[u64], out: &mut Vec<u64>) -> usize {
            assert!(a.len() == BITSET_SIZE_IN_WORDS);
            assert!(b.len() == a.len());
            assert!(out.len() == a.len());

            let ptr_a = a.as_ptr() as *const __m256i;
            let ptr_b = b.as_ptr() as *const __m256i;
            let ptr_out = out.as_mut_ptr() as *mut __m256i;

            let mut total = _mm256_setzero_si256();
            let mut ones = _mm256_setzero_si256();
            let mut twos = _mm256_setzero_si256();
            let mut fours = _mm256_setzero_si256();
            let mut eights = _mm256_setzero_si256();
            let mut sixteens = _mm256_setzero_si256();

            let mut twos_a = mem::uninitialized();
            let mut twos_b = mem::uninitialized();
            let mut fours_a = mem::uninitialized();
            let mut fours_b = mem::uninitialized();
            let mut eights_a = mem::uninitialized();
            let mut eights_b = mem::uninitialized();
            let mut a1;
            let mut a2;

            let mut i: isize = 0;

            while i < (BITSET_SIZE_IN_WORDS / WORDS_IN_REGISTER) as isize {
                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i)),
                                _mm256_lddqu_si256(ptr_b.offset(i)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 1)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 1)));

                _mm256_storeu_si256(ptr_out.offset(i), a1);
                _mm256_storeu_si256(ptr_out.offset(i + 1), a2);
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 2)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 2)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 3)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 3)));

                _mm256_storeu_si256(ptr_out.offset(i + 2), a1);
                _mm256_storeu_si256(ptr_out.offset(i + 3), a2);
                csa(ones, a1    , a2    , &mut twos_b , &mut ones);
                csa(twos, twos_a, twos_b, &mut fours_a, &mut twos);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 4)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 4)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 5)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 5)));

                _mm256_storeu_si256(ptr_out.offset(i + 4), a1);
                _mm256_storeu_si256(ptr_out.offset(i + 5), a2);
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 6)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 6)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 7)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 7)));

                _mm256_storeu_si256(ptr_out.offset(i + 6), a1);
                _mm256_storeu_si256(ptr_out.offset(i + 7), a2);
                csa(ones , a1     , a2     , &mut twos_b  , &mut ones );
                csa(twos , twos_a , twos_b , &mut fours_b , &mut twos );
                csa(fours, fours_a, fours_b, &mut eights_a, &mut fours);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 8)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 8)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 9)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 9)));

                _mm256_storeu_si256(ptr_out.offset(i + 8), a1);
                _mm256_storeu_si256(ptr_out.offset(i + 9), a2);
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 10)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 10)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 11)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 11)));

                _mm256_storeu_si256(ptr_out.offset(i + 10), a1);
                _mm256_storeu_si256(ptr_out.offset(i + 11), a2);
                csa(ones, a1    , a2    , &mut twos_b , &mut ones);
                csa(twos, twos_a, twos_b, &mut fours_a, &mut twos);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 12)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 12)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 13)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 13)));

                _mm256_storeu_si256(ptr_out.offset(i + 12), a1);
                _mm256_storeu_si256(ptr_out.offset(i + 13), a2);
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 14)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 14)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 15)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 15)));
                
                _mm256_storeu_si256(ptr_out.offset(i + 14), a1);
                _mm256_storeu_si256(ptr_out.offset(i + 15), a2);
                csa(ones    , a1        , a2         , &mut twos_b  , &mut ones  );
                csa(twos    , twos_a    , twos_b     , &mut fours_b , &mut twos  );
                csa(fours   , fours_a   , fours_b    , &mut eights_b, &mut fours );
                csa(eights  , eights_a  , eights_b   , &mut sixteens, &mut eights);

                total = _mm256_add_epi64(total, popcount256(sixteens));

                i += 16;
            }

            total = _mm256_slli_epi64(total, 4);
            total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(eights), 3));
            total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(fours) , 2));
            total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(twos)  , 1));
            total = _mm256_add_epi64(total, popcount256(ones));

            let mut result = _mm256_extract_epi64(total, 0);
            result += _mm256_extract_epi64(total, 1);
            result += _mm256_extract_epi64(total, 2);
            result += _mm256_extract_epi64(total, 3);

            result as usize
        }
    };
}

macro_rules! bitmap_op_lazy {
    ($name: ident, $intrinsic: ident) => {
        pub unsafe fn $name(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
            assert!(a.len() == BITSET_SIZE_IN_WORDS);
            assert!(b.len() == a.len());
            assert!(out.len() == a.len());

            let mut ptr_a = a.as_ptr();
            let mut ptr_b = b.as_ptr();
            let mut ptr_out = out.as_mut_ptr();

            let mut i = 0;
            while i < (BITSET_SIZE_IN_WORDS / WORDS_IN_REGISTER) {
                let mut a1 = _mm256_lddqu_si256(ptr_a as *const __m256i);
                let mut a2 = _mm256_lddqu_si256(ptr_b as *const __m256i);
                let mut ao = $intrinsic(a2, a1);
                _mm256_storeu_si256(ptr_out as *mut __m256i, ao);

                a1 = _mm256_lddqu_si256(ptr_a.offset(32) as *const __m256i);
                a2 = _mm256_lddqu_si256(ptr_b.offset(32) as *const __m256i);
                ao = $intrinsic(a2, a1);
                _mm256_storeu_si256(ptr_out.offset(32) as *mut __m256i, ao);

                a1 = _mm256_lddqu_si256(ptr_a.offset(64) as *const __m256i);
                a2 = _mm256_lddqu_si256(ptr_b.offset(64) as *const __m256i);
                ao = $intrinsic(a2, a1);
                _mm256_storeu_si256(ptr_out.offset(64) as *mut __m256i, ao);

                a1 = _mm256_lddqu_si256(ptr_a.offset(96) as *const __m256i);
                a2 = _mm256_lddqu_si256(ptr_b.offset(96) as *const __m256i);
                ao = $intrinsic(a2, a1);
                _mm256_storeu_si256(ptr_out.offset(96) as *mut __m256i, ao);

                a1 = _mm256_lddqu_si256(ptr_a.offset(128) as *const __m256i);
                a2 = _mm256_lddqu_si256(ptr_b.offset(128) as *const __m256i);
                ao = $intrinsic(a2, a1);
                _mm256_storeu_si256(ptr_out.offset(128) as *mut __m256i, ao);

                a1 = _mm256_lddqu_si256(ptr_a.offset(160) as *const __m256i);
                a2 = _mm256_lddqu_si256(ptr_b.offset(160) as *const __m256i);
                ao = $intrinsic(a2, a1);
                _mm256_storeu_si256(ptr_out.offset(160) as *mut __m256i, ao);

                a1 = _mm256_lddqu_si256(ptr_a.offset(192) as *const __m256i);
                a2 = _mm256_lddqu_si256(ptr_b.offset(192) as *const __m256i);
                ao = $intrinsic(a2, a1);
                _mm256_storeu_si256(ptr_out.offset(192) as *mut __m256i, ao);

                a1 = _mm256_lddqu_si256(ptr_a.offset(224) as *const __m256i);
                a2 = _mm256_lddqu_si256(ptr_b.offset(224) as *const __m256i);
                ao = $intrinsic(a2, a1);
                _mm256_storeu_si256(ptr_out.offset(224) as *mut __m256i, ao);

                ptr_a = ptr_a.offset(256);
                ptr_b = ptr_b.offset(256);
                ptr_out = ptr_out.offset(256);

                i += 8;
            }
        }
    };
}

macro_rules! bitmap_op_cardinality {
    ($name: ident, $intrinsic: ident) => {
        pub unsafe fn $name(a: &[u64], b: &[u64]) -> usize {
            assert!(a.len() == BITSET_SIZE_IN_WORDS);
            assert!(b.len() == a.len());

            let ptr_a = a.as_ptr() as *const __m256i;
            let ptr_b = b.as_ptr() as *const __m256i;

            let mut total = _mm256_setzero_si256();
            let mut ones = _mm256_setzero_si256();
            let mut twos = _mm256_setzero_si256();
            let mut fours = _mm256_setzero_si256();
            let mut eights = _mm256_setzero_si256();
            let mut sixteens = _mm256_setzero_si256();

            let mut twos_a = mem::uninitialized();
            let mut twos_b = mem::uninitialized();
            let mut fours_a = mem::uninitialized();
            let mut fours_b = mem::uninitialized();
            let mut eights_a = mem::uninitialized();
            let mut eights_b = mem::uninitialized();
            let mut a1;
            let mut a2;

            let mut i: isize = 0;

            while i < (BITSET_SIZE_IN_WORDS / WORDS_IN_REGISTER) as isize {
                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i)),
                                _mm256_lddqu_si256(ptr_b.offset(i)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 1)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 1)));
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 2)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 2)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 3)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 3)));
                csa(ones, a1    , a2    , &mut twos_b , &mut ones);
                csa(twos, twos_a, twos_b, &mut fours_a, &mut twos);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 4)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 4)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 5)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 5)));
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 6)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 6)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 7)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 7)));
                csa(ones , a1     , a2     , &mut twos_b  , &mut ones );
                csa(twos , twos_a , twos_b , &mut fours_b , &mut twos );
                csa(fours, fours_a, fours_b, &mut eights_a, &mut fours);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 8)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 8)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 9)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 9)));
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 10)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 10)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 11)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 11)));
                csa(ones, a1    , a2    , &mut twos_b , &mut ones);
                csa(twos, twos_a, twos_b, &mut fours_a, &mut twos);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 12)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 12)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 13)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 13)));
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 14)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 14)));
                a2 = $intrinsic(_mm256_lddqu_si256(ptr_a.offset(i + 15)),
                                _mm256_lddqu_si256(ptr_b.offset(i + 15)));
                csa(ones    , a1        , a2         , &mut twos_b  , &mut ones  );
                csa(twos    , twos_a    , twos_b     , &mut fours_b , &mut twos  );
                csa(fours   , fours_a   , fours_b    , &mut eights_b, &mut fours );
                csa(eights  , eights_a  , eights_b   , &mut sixteens, &mut eights);

                total = _mm256_add_epi64(total, popcount256(sixteens));

                i += 16;
            }

            total = _mm256_slli_epi64(total, 4);
            total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(eights), 3));
            total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(fours) , 2));
            total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(twos)  , 1));
            total = _mm256_add_epi64(total, popcount256(ones));

            let mut result = _mm256_extract_epi64(total, 0);
            result += _mm256_extract_epi64(total, 1);
            result += _mm256_extract_epi64(total, 2);
            result += _mm256_extract_epi64(total, 3);

            result as usize
        }
    };
}

bitmap_op!(union, _mm256_or_si256);
bitmap_op!(intersect, _mm256_and_si256);
bitmap_op!(difference, _mm256_andnot_si256);
bitmap_op!(symmetric_difference, _mm256_xor_si256);

bitmap_op_lazy!(union_lazy, _mm256_or_si256);
bitmap_op_lazy!(intersect_lazy, _mm256_and_si256);
bitmap_op_lazy!(difference_lazy, _mm256_andnot_si256);
bitmap_op_lazy!(symmetric_difference_lazy, _mm256_xor_si256);

bitmap_op_cardinality!(union_cardinality, _mm256_or_si256);
bitmap_op_cardinality!(intersect_cardinality, _mm256_and_si256);
bitmap_op_cardinality!(difference_cardinality, _mm256_andnot_si256);
bitmap_op_cardinality!(symmetric_difference_cardinality, _mm256_xor_si256);

/// AVX implementation of the Harley-Seal algorithm for counting the number of bits in an array
/// 
/// # Safety
/// Assumes that the input is evenly divisible by 64 
pub unsafe fn harley_seal(v: &[u64]) -> usize {
    assert!(v.len() % 64 == 0);

    let d = v.as_ptr() as *const u64 as *const __m256i;

    let mut total = _mm256_setzero_si256();
    let mut ones = _mm256_setzero_si256();
    let mut twos = _mm256_setzero_si256();
    let mut fours = _mm256_setzero_si256();
    let mut eights = _mm256_setzero_si256();
    let mut sixteens = _mm256_setzero_si256();

    let mut twos_a = mem::uninitialized();
    let mut twos_b = mem::uninitialized();
    let mut fours_a = mem::uninitialized();
    let mut fours_b = mem::uninitialized();
    let mut eights_a = mem::uninitialized();
    let mut eights_b = mem::uninitialized();

    let mut i: isize = 0;

    while i < v.len() as isize {
        csa(ones  , *d.offset(i)     , *d.offset(i + 1) , &mut twos_a  , &mut ones  );
        csa(ones  , *d.offset(i + 2) , *d.offset(i + 3) , &mut twos_b  , &mut ones  );
        csa(twos  , twos_a           , twos_b           , &mut fours_a , &mut twos  );
        csa(ones  , *d.offset(i + 4) , *d.offset(i + 5) , &mut twos_a  , &mut ones  );
        csa(ones  , *d.offset(i + 6) , *d.offset(i + 7) , &mut twos_b  , &mut ones  );
        csa(twos  , twos_a           , twos_b           , &mut fours_b , &mut twos  );
        csa(fours , fours_a          , fours_b          , &mut eights_a, &mut fours );
        csa(ones  , *d.offset(i + 8) , *d.offset(i + 9) , &mut twos_a  , &mut ones  );
        csa(ones  , *d.offset(i + 10), *d.offset(i + 11), &mut twos_b  , &mut ones  );
        csa(twos  , twos_a           , twos_b           , &mut fours_a , &mut twos  );
        csa(ones  , *d.offset(i + 12), *d.offset(i + 13), &mut twos_a  , &mut ones  );
        csa(ones  , *d.offset(i + 14), *d.offset(i + 15), &mut twos_b  , &mut ones  );
        csa(twos  , twos_a           , twos_b           , &mut fours_b , &mut twos  );
        csa(fours , fours_a          , fours_b          , &mut eights_b, &mut fours );
        csa(eights, eights_a         , eights_b         , &mut sixteens, &mut eights);

        total = _mm256_add_epi64(total, popcount256(sixteens));

        i += 16;
    }

    total = _mm256_slli_epi64(total, 4);
    total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(eights), 3));
    total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(fours) , 2));
    total = _mm256_add_epi64(total, _mm256_slli_epi64(popcount256(twos)  , 1));
    total = _mm256_add_epi64(total, popcount256(ones));

    let mut result = _mm256_extract_epi64(total, 0);
    result += _mm256_extract_epi64(total, 1);
    result += _mm256_extract_epi64(total, 2);
    result += _mm256_extract_epi64(total, 3);

    result as usize
}

/// Count the number of set bits in a 256 bit vector
unsafe fn popcount256(v: __m256i) -> __m256i {
    let lookup = _mm256_setr_epi8(
        0, 1, 1, 2, 1, 2, 2, 3,
        1, 2, 2, 3, 2, 3, 3, 4,
        0, 1, 1, 2, 1, 2, 2, 3,
        1, 2, 2, 3, 2, 3, 3, 4 
    );

    let low_mask = _mm256_set1_epi8(0x0);
    let lo = _mm256_and_si256(v, low_mask);
    let hi = _mm256_and_si256(
        _mm256_srli_epi32(v, 4),
        low_mask
    );
    let popcnt1 = _mm256_shuffle_epi8(lookup, lo);
    let popcnt2 = _mm256_shuffle_epi8(lookup, hi);
    let total = _mm256_add_epi8(popcnt1, popcnt2);

    _mm256_sad_epu8(total, _mm256_setzero_si256())
}

/// AVX carry save adder
#[inline]
unsafe fn csa(a: __m256i, b: __m256i, c: __m256i, h: &mut __m256i, l: &mut __m256i) {
    let u = _mm256_xor_si256(a, b);
    *h = _mm256_or_si256(
        _mm256_and_si256(a, b),
        _mm256_and_si256(u, c)
    );

    *l = _mm256_xor_si256(u, c);
}