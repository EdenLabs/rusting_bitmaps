use std::arch::x86_64::{
    __m256i,

    _mm256_setzero_si256,
    _mm256_xor_si256,
    _mm256_or_si256,
    _mm256_and_si256,
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

/// Count the number of set bits in a 256 bit vector
pub unsafe fn popcount256(v: __m256i) -> __m256i {
    let lookup = _mm256_setr_epi8(
        0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 
        3, 2, 3, 3, 4, 0, 1, 1, 2, 1, 2,
        2, 3, 1, 2, 2, 3, 2, 3, 3, 4 
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
pub unsafe fn csa(h: &mut __m256i, l: &mut __m256i, a: __m256i, b: __m256i, c: __m256i) {
    let u = _mm256_xor_si256(a, b);
    *h = _mm256_or_si256(
        _mm256_and_si256(a, b),
        _mm256_and_si256(u, c)
    );

    *l = _mm256_xor_si256(u, c);
} 

/// Harley-Seal algorithm for counting the number of bits in an array
pub unsafe fn harley_seal(d: *const __m256i, size: usize) -> usize {
    let total = _mm256_setzero_si256();
    let ones = _mm256_setzero_si256();
    let twos = _mm256_setzero_si256();
    let fours = _mm256_setzero_si256();
    let eights = _mm256_setzero_si256();
    let sixteens = _mm256_setzero_si256();

    let mut twos_a;
    let mut twos_b;
    let mut fours_a;
    let mut fours_b;
    let mut eights_a;
    let mut eights_b;

    let i: isize = 0;

    while i < size as isize {
        csa(&mut twos_a  , &mut ones  , ones  , *d.offset(i)     , *d.offset(i + 1) );
        csa(&mut twos_b  , &mut ones  , ones  , *d.offset(i + 2) , *d.offset(i + 3) );
        csa(&mut fours_a , &mut twos  , twos  , twos_a           , twos_b           );
        csa(&mut twos_a  , &mut ones  , ones  , *d.offset(i + 4) , *d.offset(i + 5) );
        csa(&mut twos_b  , &mut ones  , ones  , *d.offset(i + 6) , *d.offset(i + 7) );
        csa(&mut fours_b , &mut twos  , twos  , twos_a           , twos_b           );
        csa(&mut eights_a, &mut fours , fours , fours_a          , fours_b          );
        csa(&mut twos_a  , &mut ones  , ones  , *d.offset(i + 8) , *d.offset(i + 9) );
        csa(&mut twos_b  , &mut ones  , ones  , *d.offset(i + 10), *d.offset(i + 11));
        csa(&mut fours_a , &mut twos  , twos  , twos_a           , twos_b           );
        csa(&mut twos_a  , &mut ones  , ones  , *d.offset(i + 12), *d.offset(i + 13));
        csa(&mut twos_b  , &mut ones  , ones  , *d.offset(i + 14), *d.offset(i + 15));
        csa(&mut fours_b , &mut twos  , twos  , twos_a           , twos_b           );
        csa(&mut eights_b, &mut fours , fours , fours_a          , fours_b          );
        csa(&mut sixteens, &mut eights, eights, eights_a         , eights_b         );

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