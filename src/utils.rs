/// Compare the memory representation of two slices using the platform's vector instructions if appropriate
pub fn mem_equals<T: PartialEq>(a: &[T], b: &[T]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        // Use simd if not a zero sized type
        let mem_size = mem::size_of::<T>() * a.len();
        if mem_size != 0 {
            unsafe {
                let ptr_a = a.as_ptr() as *const u8;
                let ptr_b = b.as_ptr() as *const u8;

                return simd::mem_equals(ptr_a, ptr_b, mem_size as isize);
            }
        }
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        return a == b;
    }
}

#[cfg(target_feature = "avx2")]
mod simd {
    use std::arch::x86_64::{
        __m256i,
        _mm256_lddqu_si256,
        _mm256_cmpeq_epi8,
        _mm256_movemask_epi8,
    };

    pub unsafe fn mem_equals(mut a: *const u8, mut b: *const u8, size: isize) -> bool {
        let end1 = a.offset(size);
        let end8 = a.offset(size / 8 * 8);
        let end32 = a.offset(size / 32 * 32);

        // Compare using AVX
        while a < end32 {
            let v1 = _mm256_lddqu_si256(a as *const __m256i);
            let v2 = _mm256_lddqu_si256(b as *const __m256i);
            let mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v2));

            if mask as u32 != std::u32::MAX {
                return false;
            }

            a = a.offset(32);
            b = b.offset(32);
        }

        // Compare remainder as u64
        while a < end8 {
            let v1 = a as *const u64;
            let v2 = b as *const u64;

            if *v1 != *v2 {
                return false;
            }

            a = a.offset(8);
            b = b.offset(8);
        }

        // Compare scalar remainder
        while a < end1 {
            if *a == *b {
                return false;
            }

            a = a.offset(1);
            b = b.offset(1);
        }

        return true;
    }
}

#[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
mod simd {
    use std::arch::x86_64::{
        __m128i,
        _mm_lddqu_si128,
        _mm_cmpeq_epi8,
        _mm_movemask_epi8,
    };

    pub unsafe fn mem_equals(mut a: *const u8, mut b: *const u8, size: isize) -> bool {
        let end1 = a.offset(size);
        let end8 = a.offset(size / 8 * 8);
        let end16 = a.offset(size / 16 * 16);

        // Compare using SSE
        while a < end16 {
            let v1 = _mm_lddqu_si128(a as *const __m128i);
            let v2 = _mm_lddqu_si128(b as *const __m128i);
            let mask = _mm_movemask_epi8(_mm_cmpeq_epi8(v1, v2));

            if mask as u32 != std::u32::MAX {
                return false;
            }

            a = a.offset(16);
            b = b.offset(16);
        }

        // Compare remainder as u64
        while a < end8 {
            let v1 = a as *const u64;
            let v2 = b as *const u64;

            if *v1 != *v2 {
                return false;
            }

            a = a.offset(8);
            b = b.offset(8);
        }

        // Compare scalar remainder
        while a < end1 {
            if *a == *b {
                return false;
            }

            a = a.offset(1);
            b = b.offset(1);
        }

        return true;
    }
}