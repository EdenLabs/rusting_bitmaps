use std::mem;
use std::arch::x86_64::{
    _popcnt32,

    // AVX
    __m256i,
    _mm256_min_epu16,
    _mm256_max_epu16,
    _mm256_alignr_epi8,
    _mm256_lddqu_si256,
    _mm256_set1_epi16,
    _mm256_movemask_epi8,
    _mm256_packs_epi16,
    _mm256_cmpeq_epi16,
    _mm256_setzero_si256,
    _mm256_shuffle_epi8,
    _mm256_storeu_si256
};

use super::super::{
    scalar_symmetric_difference,
    scalar_union
};

use super::{
    VSIZE_256,
    VSIZE_256I,
    UNIQUE_SHUFFLE
};

/// Compute the symmetric difference (`(A \ B) ∪ (B \ A)`) between A and B
pub fn symmetric_difference(a: &[u16], b: &[u16], out: &mut Vec<u16>) {
    assert!(out.len() == 0);

    // Ensure that out has enough space to hold the contents
    if out.capacity() < a.len() + b.len() {
        out.reserve(a.len() + b.len());
    }
    
    if a.len() < VSIZE_256 || b.len() < VSIZE_256 {
        scalar_symmetric_difference(a, b, out);
        return;
    }

    let mut i_a = 0;
    let mut i_b = 0;

    unsafe {
        let len_a = a.len() / VSIZE_256;
        let len_b = b.len() / VSIZE_256;
        
        let mut count = 0;
        
        let v_a = _mm256_lddqu_si256(a.get_unchecked(i_a) as *const u16 as *const __m256i);
        let v_b = _mm256_lddqu_si256(b.get_unchecked(i_b) as *const u16 as *const __m256i);
        
        i_a += 1;
        i_b += 1;
        
        let mut v_min = mem::uninitialized();
        let mut v_max = mem::uninitialized();
        merge(v_a, v_b, &mut v_min, &mut v_max);
        
        let mut last_store = _mm256_set1_epi16(-1);
        count += store_symmetric(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);

        last_store = v_min;

        if i_a < len_a && i_b < len_b {
            let mut v_a = *a.get_unchecked(i_a * VSIZE_256);
            let mut v_b = *b.get_unchecked(i_b * VSIZE_256);
            let mut v;

            loop {
                if v_a < v_b {
                    v = _mm256_lddqu_si256(a.get_unchecked(i_a) as *const u16 as *const __m256i);

                    i_a += VSIZE_256;
                    if i_a >= len_a {
                        break;
                    }

                    v_a = *a.get_unchecked(i_a);
                }
                else {
                    v = _mm256_lddqu_si256(b.get_unchecked(i_b) as *const u16 as *const __m256i);

                    i_b += VSIZE_256;
                    if i_b >= len_b {
                        break;
                    }

                    v_b = *b.get_unchecked(i_b);
                }

                merge(v, v_max, &mut v_min, &mut v_max);
                count += store_symmetric(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);

                last_store = v_min;
            }

            merge(v, v_max, &mut v_min, &mut v_max);
            count += store_symmetric(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);
        }

        out.set_len(count);
    }

    scalar_symmetric_difference(&a[i_a..a.len()], &b[i_b..b.len()], out);
} 

/// Compute the union (`A ∪ B`) of of two u16 vectors
pub fn union(a: &[u16], b: &[u16], out: *mut u16) {
    let len_a = a.len();
    let len_b = b.len();
    
    let ptr_a = a.as_ptr();
    let ptr_b = b.as_ptr();
    
    

    // OLD IMPL
    
    // Ensure that there's enough space in out to fit the result
    let max_len = a.len() + b.len();
    if out.len() < max_len {
        out.reserve(max_len);
    }
    
    // Length is too short to bother with avx, just use the scalar version
    if a.len() < VSIZE_256 || b.len() < VSIZE_256 {
        scalar_union(a, b, out);
        return;
    }

    let mut i_a = 0;
    let mut i_b = 0;

    unsafe {
        let len_a = a.len() / VSIZE_256;
        let len_b = b.len() / VSIZE_256;

        let mut count = 0;

        let v_a = _mm256_lddqu_si256(a.get_unchecked(i_a) as *const u16 as *const __m256i);
        let v_b = _mm256_lddqu_si256(b.get_unchecked(i_b) as *const u16 as *const __m256i);
        let mut v_min = mem::uninitialized();
        let mut v_max = mem::uninitialized();

        i_a += VSIZE_256;
        i_b += VSIZE_256;

        merge(v_a, v_b, &mut v_min, &mut v_max);

        let mut last_store = _mm256_set1_epi16(-1);
        count += store_union(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);
        last_store = v_min;

        if i_a < len_a && i_b < len_b {
            let mut cur_a = *a.get_unchecked(i_a);
            let mut cur_b = *b.get_unchecked(i_b);
            let mut v;

            loop {
                if cur_a <= cur_b {
                    v = _mm256_lddqu_si256(a.get_unchecked(i_a) as *const u16 as *const __m256i);

                    i_a += VSIZE_256;
                    if i_a >= len_a {
                        break;
                    }

                    cur_a = *a.get_unchecked(i_a);
                }
                else {
                    v = _mm256_lddqu_si256(b.get_unchecked(i_b) as *const u16 as *const __m256i);

                    i_b += VSIZE_256;
                    if i_b >= len_b {
                        break;
                    }

                    cur_b = *b.get_unchecked(i_b);
                }

                merge(v, v_max, &mut v_min, &mut v_max);
                
                count += store_union(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);
                last_store = v_min;
            }

            merge(v, v_max, &mut v_min, &mut v_max);
            count += store_union(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);
        }

        out.set_len(count);
    }

    scalar_union(&a[i_a..a.len()], &b[i_b..b.len()], out);
}

unsafe fn merge(a: __m256i, b: __m256i, min: &mut __m256i, max: &mut __m256i) {
    let mut temp = _mm256_min_epu16(a, b);
    *max = _mm256_max_epu16(a, b);
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

unsafe fn store_union(old: __m256i, new: __m256i, output: *mut u16) -> usize {
    let temp = _mm256_alignr_epi8(new, old, VSIZE_256I - 2);
    let mask = _mm256_movemask_epi8(
        _mm256_packs_epi16(
            _mm256_cmpeq_epi16(temp, new),
            _mm256_setzero_si256()
        )
    );

    let num_values = VSIZE_256I - _popcnt32(mask);
    let shuffle = &mut UNIQUE_SHUFFLE[mask as usize] as *mut u8 as *mut __m256i;

    let key = _mm256_lddqu_si256(shuffle);
    let val = _mm256_shuffle_epi8(new, key);
    
    _mm256_storeu_si256(output as *mut __m256i, val);

    num_values as usize
}

unsafe fn store_symmetric(old: __m256i, new: __m256i, output: *mut u16) -> usize {
    let temp_0 = _mm256_alignr_epi8(new, old, VSIZE_256I - 4);
    let temp_1 = _mm256_alignr_epi8(new, old, VSIZE_256I - 2);
    
    let eq_left = _mm256_cmpeq_epi16(temp_0, temp_1);
    let eq_right = _mm256_cmpeq_epi16(temp_0, new);
    let eq_lr = _mm256_cmpeq_epi16(eq_left, eq_right);
    
    let move_mask = _mm256_movemask_epi8(
        _mm256_packs_epi16(eq_lr, _mm256_setzero_si256())
    );
    
    let num_new = VSIZE_256I - _popcnt32(move_mask);
    
    let key = _mm256_lddqu_si256(&UNIQUE_SHUFFLE[move_mask as usize] as *const u8 as *const __m256i);
    let val = _mm256_shuffle_epi8(temp_1, key);
    
    _mm256_storeu_si256(output as *mut __m256i, val);
    
    return num_new as usize;
}