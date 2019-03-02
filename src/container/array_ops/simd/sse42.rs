use std::mem;
use std::arch::x86_64::{
    _popcnt32,

    // SSE
    __m128i,
    _mm_lddqu_si128,
    _mm_shuffle_epi8,
    _mm_storeu_si128,
    _mm_setzero_si128,
    _mm_min_epu16,
    _mm_max_epu16,
    _mm_alignr_epi8,
    _mm_set1_epi16,
    _mm_movemask_epi8,
    _mm_packs_epi16,
    _mm_cmpeq_epi16,
};

use super::super::{
    scalar_symmetric_difference,
    scalar_union
};

use super::{
    UNIQUE_SHUFFLE,
    VSIZE_128,
    VSIZE_128I
};

/// Compute the symmetric difference (`(A \ B) ∪ (B \ A)`) between A and B
pub fn symmetric_difference(a: &[u16], b: &[u16], out: &mut Vec<u16>) {
    assert!(out.len() == 0);

    // Ensure that out has enough space to hold the contents
    if out.capacity() < a.len() + b.len() {
        out.reserve(a.len() + b.len());
    }
    
    if a.len() < VSIZE_128 || b.len() < VSIZE_128 {
        scalar_symmetric_difference(a, b, out);
        return;
    }

    let mut i_a = 0;
    let mut i_b = 0;

    unsafe {
        let len_a = a.len() / VSIZE_128;
        let len_b = b.len() / VSIZE_128;
        
        let mut count = 0;
        
        let v_a = _mm_lddqu_si128(a.get_unchecked(i_a) as *const u16 as *const __m128i);
        let v_b = _mm_lddqu_si128(b.get_unchecked(i_b) as *const u16 as *const __m128i);
        
        i_a += 1;
        i_b += 1;
        
        let mut v_min = mem::uninitialized();
        let mut v_max = mem::uninitialized();
        merge(v_a, v_b, &mut v_min, &mut v_max);
        
        let mut last_store = _mm_set1_epi16(-1);
        count += store_symmetric(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);

        last_store = v_min;

        if i_a < len_a && i_b < len_b {
            let mut v_a = *a.get_unchecked(i_a * VSIZE_128);
            let mut v_b = *b.get_unchecked(i_b * VSIZE_128);
            let mut v;

            loop {
                if v_a < v_b {
                    v = _mm_lddqu_si128(a.get_unchecked(i_a) as *const u16 as *const __m128i);

                    i_a += VSIZE_128;
                    if i_a >= len_a {
                        break;
                    }

                    v_a = *a.get_unchecked(i_a);
                }
                else {
                    v = _mm_lddqu_si128(b.get_unchecked(i_b) as *const u16 as *const __m128i);

                    i_b += VSIZE_128;
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
pub fn union(a: &[u16], b: &[u16], out: &mut Vec<u16>) {
    assert!(out.len() == 0);

    // Ensure that there's enough space in out to fit the result
    let max_len = a.len() + b.len();
    if out.len() < max_len {
        out.reserve(max_len);
    }
    
    // Length is too short to bother with avx, just use the scalar version
    if a.len() < VSIZE_128 || b.len() < VSIZE_128 {
        scalar_union(a, b, out);
        return;
    }

    let mut i_a = 0;
    let mut i_b = 0;

    unsafe {
        let len_a = a.len() / VSIZE_128;
        let len_b = b.len() / VSIZE_128;

        let mut count = 0;

        let v_a = _mm_lddqu_si128(a.get_unchecked(i_a) as *const u16 as *const __m128i);
        let v_b = _mm_lddqu_si128(b.get_unchecked(i_b) as *const u16 as *const __m128i);
        let mut v_min = mem::uninitialized();
        let mut v_max = mem::uninitialized();

        i_a += VSIZE_128;
        i_b += VSIZE_128;

        merge(v_a, v_b, &mut v_min, &mut v_max);

        let mut last_store = _mm_set1_epi16(-1);
        count += store_union(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);
        last_store = v_min;

        if i_a < len_a && i_b < len_b {
            let mut cur_a = *a.get_unchecked(i_a);
            let mut cur_b = *b.get_unchecked(i_b);
            let mut v;

            loop {
                if cur_a <= cur_b {
                    v = _mm_lddqu_si128(a.get_unchecked(i_a) as *const u16 as *const __m128i);

                    i_a += VSIZE_128;
                    if i_a >= len_a {
                        break;
                    }

                    cur_a = *a.get_unchecked(i_a);
                }
                else {
                    v = _mm_lddqu_si128(b.get_unchecked(i_b) as *const u16 as *const __m128i);

                    i_b += VSIZE_128;
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

unsafe fn merge(a: __m128i, b: __m128i, min: &mut __m128i, max: &mut __m128i) {
    let mut temp = _mm_min_epu16(a, b);
    *max = _mm_max_epu16(a, b);
    temp = _mm_alignr_epi8(temp, temp, 2);

    for _i in 0..6 {
        *min = _mm_min_epu16(temp, *max);
        *max = _mm_max_epu16(temp, *max);
        temp = _mm_alignr_epi8(*min, *min, 2);
    }

    *min = _mm_min_epu16(temp, *max);
    *max = _mm_max_epu16(temp, *max);
    *min = _mm_alignr_epi8(*min, *min, 2);
}

unsafe fn store_union(old: __m128i, new: __m128i, output: *mut u16) -> usize {
    let temp = _mm_alignr_epi8(new, old, VSIZE_128I - 2);
    let mask = _mm_movemask_epi8(
        _mm_packs_epi16(
            _mm_cmpeq_epi16(temp, new),
            _mm_setzero_si128()
        )
    );

    let num_values = VSIZE_128I - _popcnt32(mask);
    let shuffle = &mut UNIQUE_SHUFFLE[mask as usize] as *mut u8 as *mut __m128i;

    let key = _mm_lddqu_si128(shuffle);
    let val = _mm_shuffle_epi8(new, key);
    
    _mm_storeu_si128(output as *mut __m128i, val);

    num_values as usize
}

unsafe fn store_symmetric(old: __m128i, new: __m128i, output: *mut u16) -> usize {
    let temp_0 = _mm_alignr_epi8(new, old, VSIZE_128I - 4);
    let temp_1 = _mm_alignr_epi8(new, old, VSIZE_128I - 2);
    
    let eq_left = _mm_cmpeq_epi16(temp_0, temp_1);
    let eq_right = _mm_cmpeq_epi16(temp_0, new);
    let eq_lr = _mm_cmpeq_epi16(eq_left, eq_right);
    
    let move_mask = _mm_movemask_epi8(
        _mm_packs_epi16(eq_lr, _mm_setzero_si128())
    );
    
    let num_new = VSIZE_128I - _popcnt32(move_mask);
    
    let key = _mm_lddqu_si128(&UNIQUE_SHUFFLE[move_mask as usize] as *const u8 as *const __m128i);
    let val = _mm_shuffle_epi8(temp_1, key);
    
    _mm_storeu_si128(output as *mut __m128i, val);
    
    return num_new as usize;
}