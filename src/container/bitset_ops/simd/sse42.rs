use std::mem;
use std::arch::x86_64::{
    __m128i,

    _mm_lddqu_si128,
    _mm_storeu_si128,
    _mm_setzero_si128,
    _mm_xor_si128,
    _mm_or_si128,
    _mm_and_si128,
    _mm_andnot_si128,
    _mm_slli_epi64,
    _mm_add_epi64,
    _mm_extract_epi64,
    _mm_srli_epi32,
    _mm_set1_epi8,
    _mm_setr_epi8,
    _mm_shuffle_epi8,
    _mm_add_epi8,
    _mm_sad_epu8
};

use crate::container::bitset::BITSET_SIZE_IN_WORDS;

// NOTE: These are never intended to be used outside the bitset container impl

const WORDS_IN_REGISTER: usize = 2;

macro_rules! bitmap_op {
    ($name: ident, $intrinsic: ident) => {
        pub unsafe fn $name(a: &[u64], b: &[u64], out: &mut Vec<u64>) -> usize {
            assert!(a.len() == BITSET_SIZE_IN_WORDS);
            assert!(b.len() == a.len());
            assert!(out.len() == a.len());

            let ptr_a = a.as_ptr() as *const __m128i;
            let ptr_b = b.as_ptr() as *const __m128i;
            let ptr_out = out.as_mut_ptr() as *mut __m128i;

            let mut total = _mm_setzero_si128();
            let mut ones = _mm_setzero_si128();
            let mut twos = _mm_setzero_si128();
            let mut fours = _mm_setzero_si128();
            let mut eights = _mm_setzero_si128();
            let mut sixteens = _mm_setzero_si128();

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
                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i)),
                                _mm_lddqu_si128(ptr_b.offset(i)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 1)),
                                _mm_lddqu_si128(ptr_b.offset(i + 1)));

                _mm_storeu_si128(ptr_out.offset(i), a1);
                _mm_storeu_si128(ptr_out.offset(i + 1), a2);
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 2)),
                                _mm_lddqu_si128(ptr_b.offset(i + 2)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 3)),
                                _mm_lddqu_si128(ptr_b.offset(i + 3)));

                _mm_storeu_si128(ptr_out.offset(i + 2), a1);
                _mm_storeu_si128(ptr_out.offset(i + 3), a2);
                csa(ones, a1    , a2    , &mut twos_b , &mut ones);
                csa(twos, twos_a, twos_b, &mut fours_a, &mut twos);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 4)),
                                _mm_lddqu_si128(ptr_b.offset(i + 4)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 5)),
                                _mm_lddqu_si128(ptr_b.offset(i + 5)));

                _mm_storeu_si128(ptr_out.offset(i + 4), a1);
                _mm_storeu_si128(ptr_out.offset(i + 5), a2);
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 6)),
                                _mm_lddqu_si128(ptr_b.offset(i + 6)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 7)),
                                _mm_lddqu_si128(ptr_b.offset(i + 7)));

                _mm_storeu_si128(ptr_out.offset(i + 6), a1);
                _mm_storeu_si128(ptr_out.offset(i + 7), a2);
                csa(ones , a1     , a2     , &mut twos_b  , &mut ones );
                csa(twos , twos_a , twos_b , &mut fours_b , &mut twos );
                csa(fours, fours_a, fours_b, &mut eights_a, &mut fours);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 8)),
                                _mm_lddqu_si128(ptr_b.offset(i + 8)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 9)),
                                _mm_lddqu_si128(ptr_b.offset(i + 9)));

                _mm_storeu_si128(ptr_out.offset(i + 8), a1);
                _mm_storeu_si128(ptr_out.offset(i + 9), a2);
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 10)),
                                _mm_lddqu_si128(ptr_b.offset(i + 10)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 11)),
                                _mm_lddqu_si128(ptr_b.offset(i + 11)));

                _mm_storeu_si128(ptr_out.offset(i + 10), a1);
                _mm_storeu_si128(ptr_out.offset(i + 11), a2);
                csa(ones, a1    , a2    , &mut twos_b , &mut ones);
                csa(twos, twos_a, twos_b, &mut fours_a, &mut twos);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 12)),
                                _mm_lddqu_si128(ptr_b.offset(i + 12)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 13)),
                                _mm_lddqu_si128(ptr_b.offset(i + 13)));

                _mm_storeu_si128(ptr_out.offset(i + 12), a1);
                _mm_storeu_si128(ptr_out.offset(i + 13), a2);
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 14)),
                                _mm_lddqu_si128(ptr_b.offset(i + 14)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 15)),
                                _mm_lddqu_si128(ptr_b.offset(i + 15)));
                
                _mm_storeu_si128(ptr_out.offset(i + 14), a1);
                _mm_storeu_si128(ptr_out.offset(i + 15), a2);
                csa(ones    , a1        , a2         , &mut twos_b  , &mut ones  );
                csa(twos    , twos_a    , twos_b     , &mut fours_b , &mut twos  );
                csa(fours   , fours_a   , fours_b    , &mut eights_b, &mut fours );
                csa(eights  , eights_a  , eights_b   , &mut sixteens, &mut eights);

                total = _mm_add_epi64(total, popcount128(sixteens));

                i += 16;
            }

            total = _mm_slli_epi64(total, 4);
            total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(eights), 3));
            total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(fours) , 2));
            total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(twos)  , 1));
            total = _mm_add_epi64(total, popcount128(ones));

            let mut result = _mm_extract_epi64(total, 0);
            result += _mm_extract_epi64(total, 1);

            result as usize
        }
    };
}

macro_rules! bitmap_op_nocard {
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
                let mut a1 = _mm_lddqu_si128(ptr_a as *const __m128i);
                let mut a2 = _mm_lddqu_si128(ptr_b as *const __m128i);
                let mut ao = $intrinsic(a2, a1);
                _mm_storeu_si128(ptr_out as *mut __m128i, ao);

                a1 = _mm_lddqu_si128(ptr_a.offset(32) as *const __m128i);
                a2 = _mm_lddqu_si128(ptr_b.offset(32) as *const __m128i);
                ao = $intrinsic(a2, a1);
                _mm_storeu_si128(ptr_out.offset(32) as *mut __m128i, ao);

                a1 = _mm_lddqu_si128(ptr_a.offset(64) as *const __m128i);
                a2 = _mm_lddqu_si128(ptr_b.offset(64) as *const __m128i);
                ao = $intrinsic(a2, a1);
                _mm_storeu_si128(ptr_out.offset(64) as *mut __m128i, ao);

                a1 = _mm_lddqu_si128(ptr_a.offset(96) as *const __m128i);
                a2 = _mm_lddqu_si128(ptr_b.offset(96) as *const __m128i);
                ao = $intrinsic(a2, a1);
                _mm_storeu_si128(ptr_out.offset(96) as *mut __m128i, ao);

                a1 = _mm_lddqu_si128(ptr_a.offset(128) as *const __m128i);
                a2 = _mm_lddqu_si128(ptr_b.offset(128) as *const __m128i);
                ao = $intrinsic(a2, a1);
                _mm_storeu_si128(ptr_out.offset(128) as *mut __m128i, ao);

                a1 = _mm_lddqu_si128(ptr_a.offset(160) as *const __m128i);
                a2 = _mm_lddqu_si128(ptr_b.offset(160) as *const __m128i);
                ao = $intrinsic(a2, a1);
                _mm_storeu_si128(ptr_out.offset(160) as *mut __m128i, ao);

                a1 = _mm_lddqu_si128(ptr_a.offset(192) as *const __m128i);
                a2 = _mm_lddqu_si128(ptr_b.offset(192) as *const __m128i);
                ao = $intrinsic(a2, a1);
                _mm_storeu_si128(ptr_out.offset(192) as *mut __m128i, ao);

                a1 = _mm_lddqu_si128(ptr_a.offset(224) as *const __m128i);
                a2 = _mm_lddqu_si128(ptr_b.offset(224) as *const __m128i);
                ao = $intrinsic(a2, a1);
                _mm_storeu_si128(ptr_out.offset(224) as *mut __m128i, ao);

                ptr_a = ptr_a.offset(128);
                ptr_b = ptr_b.offset(128);
                ptr_out = ptr_out.offset(128);

                i += 8;
            }
        }
    };
}

macro_rules! bitmap_op_cardonly {
    ($name: ident, $intrinsic: ident) => {
        pub unsafe fn $name(a: &[u64], b: &[u64]) -> usize {
            assert!(a.len() == BITSET_SIZE_IN_WORDS);
            assert!(b.len() == a.len());

            let ptr_a = a.as_ptr() as *const __m128i;
            let ptr_b = b.as_ptr() as *const __m128i;

            let mut total = _mm_setzero_si128();
            let mut ones = _mm_setzero_si128();
            let mut twos = _mm_setzero_si128();
            let mut fours = _mm_setzero_si128();
            let mut eights = _mm_setzero_si128();
            let mut sixteens = _mm_setzero_si128();

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
                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i)),
                                _mm_lddqu_si128(ptr_b.offset(i)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 1)),
                                _mm_lddqu_si128(ptr_b.offset(i + 1)));
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 2)),
                                _mm_lddqu_si128(ptr_b.offset(i + 2)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 3)),
                                _mm_lddqu_si128(ptr_b.offset(i + 3)));
                csa(ones, a1    , a2    , &mut twos_b , &mut ones);
                csa(twos, twos_a, twos_b, &mut fours_a, &mut twos);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 4)),
                                _mm_lddqu_si128(ptr_b.offset(i + 4)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 5)),
                                _mm_lddqu_si128(ptr_b.offset(i + 5)));
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 6)),
                                _mm_lddqu_si128(ptr_b.offset(i + 6)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 7)),
                                _mm_lddqu_si128(ptr_b.offset(i + 7)));
                csa(ones , a1     , a2     , &mut twos_b  , &mut ones );
                csa(twos , twos_a , twos_b , &mut fours_b , &mut twos );
                csa(fours, fours_a, fours_b, &mut eights_a, &mut fours);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 8)),
                                _mm_lddqu_si128(ptr_b.offset(i + 8)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 9)),
                                _mm_lddqu_si128(ptr_b.offset(i + 9)));
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 10)),
                                _mm_lddqu_si128(ptr_b.offset(i + 10)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 11)),
                                _mm_lddqu_si128(ptr_b.offset(i + 11)));
                csa(ones, a1    , a2    , &mut twos_b , &mut ones);
                csa(twos, twos_a, twos_b, &mut fours_a, &mut twos);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 12)),
                                _mm_lddqu_si128(ptr_b.offset(i + 12)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 13)),
                                _mm_lddqu_si128(ptr_b.offset(i + 13)));
                csa(ones, a1, a2, &mut twos_a, &mut ones);

                a1 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 14)),
                                _mm_lddqu_si128(ptr_b.offset(i + 14)));
                a2 = $intrinsic(_mm_lddqu_si128(ptr_a.offset(i + 15)),
                                _mm_lddqu_si128(ptr_b.offset(i + 15)));
                csa(ones    , a1        , a2         , &mut twos_b  , &mut ones  );
                csa(twos    , twos_a    , twos_b     , &mut fours_b , &mut twos  );
                csa(fours   , fours_a   , fours_b    , &mut eights_b, &mut fours );
                csa(eights  , eights_a  , eights_b   , &mut sixteens, &mut eights);

                total = _mm_add_epi64(total, popcount128(sixteens));

                i += 16;
            }

            total = _mm_slli_epi64(total, 4);
            total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(eights), 3));
            total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(fours) , 2));
            total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(twos)  , 1));
            total = _mm_add_epi64(total, popcount128(ones));

            let mut result = _mm_extract_epi64(total, 0);
            result += _mm_extract_epi64(total, 1);

            result as usize
        }
    };
}

bitmap_op!(union, _mm_or_si128);
bitmap_op!(intersect, _mm_and_si128);
bitmap_op!(difference, _mm_andnot_si128);

bitmap_op_nocard!(union_nocard, _mm_or_si128);
bitmap_op_nocard!(intersect_nocard, _mm_and_si128);
bitmap_op_nocard!(difference_nocard, _mm_andnot_si128);

bitmap_op_cardonly!(union_cardonly, _mm_or_si128);
bitmap_op_cardonly!(intersect_cardonly, _mm_and_si128);
bitmap_op_cardonly!(difference_cardonly, _mm_andnot_si128);

/// SSE implementation of the Harley-Seal algorithm for counting the number of bits in an array
/// 
/// # Safety
/// Assumes that the input is evenly divisible by 32
pub unsafe fn harley_seal(v: &[u64]) -> usize {
    assert!(v.len() % 32 == 0);

    let d = v.as_ptr() as *const u64 as *const __m128i;

    let mut total = _mm_setzero_si128();
    let mut ones = _mm_setzero_si128();
    let mut twos = _mm_setzero_si128();
    let mut fours = _mm_setzero_si128();
    let mut eights = _mm_setzero_si128();
    let mut sixteens = _mm_setzero_si128();

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

        total = _mm_add_epi64(total, popcount128(sixteens));

        i += 16;
    }

    total = _mm_slli_epi64(total, 4);
    total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(eights), 3));
    total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(fours) , 2));
    total = _mm_add_epi64(total, _mm_slli_epi64(popcount128(twos)  , 1));
    total = _mm_add_epi64(total, popcount128(ones));

    let mut result = _mm_extract_epi64(total, 0);
    result += _mm_extract_epi64(total, 1);

    result as usize
}

/// Count the number of set bits in a 128 bit vector
unsafe fn popcount128(v: __m128i) -> __m128i {
    let lookup = _mm_setr_epi8(
        0, 1, 1, 2, 1, 2, 2, 3,
        1, 2, 2, 3, 2, 3, 3, 4
    );

    let low_mask = _mm_set1_epi8(0x0);
    let lo = _mm_and_si128(v, low_mask);
    let hi = _mm_and_si128(
        _mm_srli_epi32(v, 4),
        low_mask
    );
    let popcnt1 = _mm_shuffle_epi8(lookup, lo);
    let popcnt2 = _mm_shuffle_epi8(lookup, hi);
    let total = _mm_add_epi8(popcnt1, popcnt2);

    _mm_sad_epu8(total, _mm_setzero_si128())
}

/// SSE carry save adder
#[inline]
unsafe fn csa(a: __m128i, b: __m128i, c: __m128i, h: &mut __m128i, l: &mut __m128i) {
    let u = _mm_xor_si128(a, b);
    *h = _mm_or_si128(
        _mm_and_si128(a, b),
        _mm_and_si128(u, c)
    );

    *l = _mm_xor_si128(u, c);
}