use std::mem;

use crate::Aligned;
use crate::simd::*;

use super::scalar;

// TODO: Finish simd abstraction
// TODO: Optimize
// TODO: Cleanup

/// Compute the symmetric difference between `a` and `b` and append the result into `out`
/// 
/// # Returns
/// Returns the number of elements appended to `out`
/// 
/// # Safety
/// - Assumes `out` is aligned to 32 bytes and contains enough space to hold the output
pub unsafe fn xor(a: Aligned<&[u16], 32>, b: Aligned<&[u16], 32>, out: *mut u16) -> usize {
    // Use a scalar algorithm if the length of the two vectors is too short to use simd
    if a.len() < SIZE || b.len() < SIZE {
        return scalar::xor(*a, *b, out);
    }

    let simd_len_a = a.len() / SIZE;
    let simd_len_b = b.len() / SIZE;

    let mut i_a = 0;
    let mut i_b = 0;

    let mut ptr_a = a.as_ptr();
    let mut ptr_b = b.as_ptr();
    let mut count = 0;
    
    let mut v_a = lddqu_si(*ptr_a as *const Register);
    let mut v_b = lddqu_si(*ptr_b as *const Register);
    
    i_a += 1;
    i_b += 1;
    
    let mut min = setzero();
    let mut max = setzero();
    merge(v_a, v_b, &mut min, &mut max);
    
    let mut last_store = set1_epi16(-1);
    count += store_symmetric(last_store, min, out.add(count));

    last_store = min;

    if i_a < simd_len_a && i_b < simd_len_b {
        let mut v = setzero();

        while i_a < simd_len_a && i_b < simd_len_b {
            let mut s_a = *ptr_a.add(i_a * SIZE);
            let mut s_b = *ptr_b.add(i_b * SIZE);

            if s_a < s_b {
                v = lddqu_si((ptr_a as *const Register).add(i_a));
                i_a += 1;
            }
            else {
                v = lddqu_si((ptr_b as *const Register).add(i_b));
                i_b += 1;
            }

            merge(v, max, &mut min, &mut max);
            count += store_symmetric(last_store, min, out.add(count));

            last_store = min;
        }

        merge(v, max, &mut min, &mut max);
        count += store_symmetric(last_store, min, out.add(count));
        last_store = min;
    }

    // TODO: Extend this to be generic over register size
    //       Currently designed for 128bit vectors
    let buffer: [u16; 17] = [0; 17];

    // Extract the last value from `max` and stick in the buffer
    let rem = store_symmetric(last_store, max, buffer.as_mut_ptr());
    let s7 = extract_epi16(max, 7);
    let s6 = extract_epi16(max, 6);

    if s7 != s6 {
        rem += 1;
        buffer[rem] = s7 as u16;
    }

    // Copy the remaining elements into the buffer for processing
    

    scalar_symmetric_difference(&a[i_a..a.len()], &b[i_b..b.len()], out);

    count
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
    if a.len() < SIZE || b.len() < SIZE {
        scalar_union(a, b, out);
        return;
    }

    let mut i_a = 0;
    let mut i_b = 0;

    unsafe {
        let len_a = a.len() / SIZE;
        let len_b = b.len() / SIZE;

        let mut count = 0;

        let v_a = lddqu_si(a.get_unchecked(i_a) as *const u16 as *const simd::Register);
        let v_b = lddqu_si(b.get_unchecked(i_b) as *const u16 as *const simd::Register);
        let mut v_min = mem::uninitialized();
        let mut v_max = mem::uninitialized();

        i_a += SIZE;
        i_b += SIZE;

        merge(v_a, v_b, &mut v_min, &mut v_max);

        let mut last_store = simd::set1_epi16(-1);
        count += store_union(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);
        last_store = v_min;

        if i_a < len_a && i_b < len_b {
            let mut cur_a = *a.get_unchecked(i_a);
            let mut cur_b = *b.get_unchecked(i_b);
            let mut v;

            loop {
                if cur_a <= cur_b {
                    v = simd::lddqu_si(a.get_unchecked(i_a) as *const u16 as *const simd::Register);

                    i_a += SIZE;
                    if i_a >= len_a {
                        break;
                    }

                    cur_a = *a.get_unchecked(i_a);
                }
                else {
                    v = simd::lddqu_si(b.get_unchecked(i_b) as *const u16 as *const simd::Register);

                    i_b += SIZE;
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

unsafe fn merge(a: Register, b: Register, min: &mut Register, max: &mut Register) {
    let mut temp = min_epu16(a, b);
    *max = max_epu16(a, b);
    temp = alignr_epi8(temp, temp, 2);

    for _i in 0..6 {
        *min = min_epu16(temp, *max);
        *max = max_epu16(temp, *max);
        temp = alignr_epi8(*min, *min, 2);
    }

    *min = min_epu16(temp, *max);
    *max = max_epu16(temp, *max);
    *min = alignr_epi8(*min, *min, 2);
}

unsafe fn store_union(old: Register, new: Register, output: *mut u16) -> usize {
    let temp = alignr_epi8(new, old, SIZEI - 2);
    let mask = movemask_epi8(
        packs_epi16(
            cmpeq_epi16(temp, new),
            setzero_si()
        )
    );

    let num_values = SIZEI - _popcnt32(mask);
    let shuffle = &mut UNIQUE_SHUFFLE[mask as usize] as *mut u8 as *mut __m128i;

    let key = lddqu_si(shuffle);
    let val = shuffle_epi8(new, key);
    
    storeu_si(output as *mut __m128i, val);

    num_values as usize
}

unsafe fn store_symmetric(old: Register, new: Register, output: *mut u16) -> usize {
    let temp_0 = alignr_epi8(new, old, SIZEI - 4);
    let temp_1 = alignr_epi8(new, old, SIZEI - 2);
    
    let eq_left = cmpeq_epi16(temp_0, temp_1);
    let eq_right = cmpeq_epi16(temp_0, new);
    let eq_lr = cmpeq_epi16(eq_left, eq_right);
    
    let move_mask = movemask_epi8(
        packs_epi16(eq_lr, setzero_si())
    );
    
    let num_new = SIZEI - _popcnt32(move_mask);
    
    let key = lddqu_si(&UNIQUE_SHUFFLE[move_mask as usize] as *const u8 as *const __m128i);
    let val = shuffle_epi8(temp_1, key);
    
    storeu_si(output as *mut __m128i, val);
    
    return num_new as usize;
}