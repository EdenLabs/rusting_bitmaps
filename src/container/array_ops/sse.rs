use std::mem;

// TODO: Finish simd abstraction
// TODO: Optimize
// TODO: Cleanup

/// Compute the symmetric difference (`(A \ B) ∪ (B \ A)`) between A and B
pub fn xor(a: Aligned<&[u16], 32>, b: Aligned<&[u16], 32>, out: &mut Vec<u16>) {    
    if a.len() < simd::SIZE || b.len() < simd::SIZE {
        scalar::xor(a, b, out);
        return;
    }

    let mut i_a = 0;
    let mut i_b = 0;

    unsafe {
        let len_a = a.len() / VSIZE_128;
        let len_b = b.len() / VSIZE_128;
        
        let mut count = 0;
        
        let v_a = simd::lddqu_si(a.get_unchecked(i_a) as *const u16 as *const simd::Register);
        let v_b = simd::lddqu_si(b.get_unchecked(i_b) as *const u16 as *const simd::Register);
        
        i_a += 1;
        i_b += 1;
        
        let mut v_min = mem::uninitialized();
        let mut v_max = mem::uninitialized();
        merge(v_a, v_b, &mut v_min, &mut v_max);
        
        let mut last_store = simd::set1_epi16(-1);
        count += store_symmetric(last_store, v_min, out.get_unchecked_mut(count) as *mut u16);

        last_store = v_min;

        if i_a < len_a && i_b < len_b {
            let mut v_a = *a.get_unchecked(i_a * VSIZE_128);
            let mut v_b = *b.get_unchecked(i_b * VSIZE_128);
            let mut v;

            loop {
                if v_a < v_b {
                    v = simd::lddqu_si(a.get_unchecked(i_a) as *const u16 as *const simd::Register);

                    i_a += VSIZE_128;
                    if i_a >= len_a {
                        break;
                    }

                    v_a = *a.get_unchecked(i_a);
                }
                else {
                    v = simd::lddqu_si(b.get_unchecked(i_b) as *const u16 as *const simd::Register);

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

        let v_a = _mm_lddqu_si128(a.get_unchecked(i_a) as *const u16 as *const simd::Register);
        let v_b = _mm_lddqu_si128(b.get_unchecked(i_b) as *const u16 as *const simd::Register);
        let mut v_min = mem::uninitialized();
        let mut v_max = mem::uninitialized();

        i_a += VSIZE_128;
        i_b += VSIZE_128;

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

                    i_a += VSIZE_128;
                    if i_a >= len_a {
                        break;
                    }

                    cur_a = *a.get_unchecked(i_a);
                }
                else {
                    v = simd::lddqu_si(b.get_unchecked(i_b) as *const u16 as *const simd::Register);

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

mod simd {
    //! A collection of simd utilities abstracted over the register size.
    //! 
    //! # Safety
    //! A minimum alignment of 32 bytes is assumed for full compatibility

    /// Convenience macro to simplify avx cfg declarations
    macro_rules! cfg_avx {
        ($($t:tt)*) => {
            #[cfg(target_feature = "avx2")]
            $($t)*
        };
    }

    /// Convenience macro to simplify sse cfg declarations
    macro_rules! cfg_sse {
        ($($t:tt)*) => {
            #[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
            $($t)*
        };
    }

    #[allow(unused_imports)]
    use std::arch::x86_64::{
        _popcnt32,
        
        __m256i,
        __m128i,

        _mm256_alignr_epi8,
        _mm256_cmpeq_epi16,
        _mm256_lddqu_si256,
        _mm256_max_epu16,
        _mm256_min_epu16,
        _mm256_movemask_epi8,
        _mm256_packs_epi16,
        _mm256_set1_epi16,
        _mm256_setzero_si256,
        _mm256_shuffle_epi8,
        _mm256_storeu_si256,

        _mm_alignr_epi8,
        _mm_cmpeq_epi16,
        _mm_lddqu_si128,
        _mm_max_epu16,
        _mm_min_epu16,
        _mm_movemask_epi8,
        _mm_packs_epi16,
        _mm_set1_epi16,
        _mm_setzero_si128,
        _mm_shuffle_epi8,
        _mm_storeu_si128
    };

    cfg_avx! {
        pub type Register = __m256i;
    }

    cfg_sse! {
        pub type Register = __m128i;
    }

    cfg_avx! {
        pub const SIZE: usize = 16;
    }

    cfg_sse! {
        pub const SIZE: usize = 8;
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn alignr_epi8(a: Register, b: Register, n: i32) -> Register {
        cfg_avx! { _mm256_alignr_epi8(a, b, n) }
        cfg_sse! { _mm_alignr_epi8(a, b, n) }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn cmpeq_cpi16(a: Register, b: Register) -> Register {
        cfg_avx! { _mm256_cmpeq_epi16(a, b) }
        cfg_sse! { _mm_cmpeq_epi16(a, b) }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn lddqu_si(mem_addr: *const Register) -> Register {
        cfg_avx! { _mm256_lddqu_si256(mem_addr) }
        cfg_sse! { _mm_lddqu_si128(mem_addr) }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn max_epu16(a: Register, b: Register) -> Register {
        cfg_avx! { _mm256_max_epu16(a, b) }
        cfg_sse! { _mm_max_epu16(a, b) }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn min_epu16(a: Register, b: Register) -> Register {
        cfg_avx! { _mm256_min_epu16(a, b) }
        cfg_sse! { _mm_min_epu16(a, b) }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn movemask_epi8(a: Register) -> i32 {
        cfg_avx! { _mm256_movemask_epi8(a) }
        cfg_sse! { _mm_movemask_epi8(a) }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn packs_epi16(a: Register, b: Register) -> Register {
        cfg_avx! { _mm256_packs_epi16(a, b) }
        cfg_sse! { _mm_packs_epi16(a, b) }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn set1_epi16(a: i16) -> Register {
        cfg_avx! { _mm256_set1_epi16(a) }
        cfg_sse! { _mm_set1_epi16(a) }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn setzero() -> Register {
        cfg_avx! { _mm256_setzero_si256() }
        cfg_sse! { _mm_setzero_si128() }
    }

    #[inline(always)]
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn shuffle_epi8(a: Register, b: Register) -> Register {
        cfg_avx! { _mm256_shuffle_epi8(a, b) }
        cfg_sse! { _mm_shuffle_epi8(a, b) }
    }

    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    pub unsafe fn storeu_si(mem_addr: *mut Register, a: Register) {
        cfg_avx! { _mm256_storeu_si256(mem_addr, a) }
        cfg_sse! { _mm_storeu_si128(mem_addr, a) }
    }
}