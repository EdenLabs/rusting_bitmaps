use std::arch::x86_64::{
    __m256i,
    _mm256_min_epu16,
    _mm256_max_epu16,
    _mm256_alignr_epi8
};

pub unsafe fn avx_merge(a: &__m256i, b: &__m256i, min: &mut __m256i, max: &mut __m256i) {
    let mut temp = _mm256_min_epu16(*a, *b);
    *max = _mm256_max_epu16(*a, *b);
    temp = _mm256_alignr_epi8(temp, temp, 2);

    for _i in 0..14 {
        *min = _mm256_min_epu16(temp, *max);
        *max = _mm256_max_epu16(temp, *max);
        temp = _mm256_alignr_epi8(*min, *min, 2);
    }

    *min = _mm256_min_epu16(temp, *max);
    *max = _mm256_max_epu16(temp, *max);
    *min = _mm256_alignr_epi8(*min, *min, 2);
}

/// Compute the union of of two u16 vectors
/// 
/// # Safety
/// A 16 byte alignment for the vecs is assumed
pub unsafe fn union_u16(a: &Vec<u16>, b: &Vec<u16>, target: &mut Vec<u16>) {
    //let range 
}