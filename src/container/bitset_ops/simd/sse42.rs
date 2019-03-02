use std::mem;
use std::arch::x86_64::{
    __m128i,

    _mm_setzero_si128,
    _mm_xor_si128,
    _mm_or_si128,
    _mm_and_si128,
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

use super::super::scalar_harley_seal;

/// Harley-Seal algorithm for counting the number of bits in an array
pub fn harley_seal(v: &[u64]) -> usize {
    let limit = v.len() >> 5;

    let mut index = 0;
    let mut result = 0;

    // Perform vectorized add
    if limit > 0 {
        unsafe {
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
            while i < limit as isize {
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

            result = _mm_extract_epi64(total, 0);
            result += _mm_extract_epi64(total, 1);
            result += _mm_extract_epi64(total, 2);
            result += _mm_extract_epi64(total, 3);

            index = (i as usize) << 1;
        }
    }
    
    let mut result = result as usize;

    // Finish out with a scalar algorithm
    if index < v.len() {
        result += scalar_harley_seal(&v[index..v.len()]);
    }

    result
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