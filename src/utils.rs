pub fn mem_equals(a: &[u16], b: &[u16]) -> bool {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        return simd::men_equals(a, b);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        return a == b;
    }
}

#[cfg(target_feature = "avx2")]
mod simd {
    pub fn mem_equals(a: &[u16], b: &[u16]) -> bool {
        if a.len() != b.len() {
            return false;
        }

        unsafe {
            let mut i = 0;

            // Compare using AVX
            let end = a.len() / 16;
            while i < end {
                let v1 = _mm256_lddqu_si256(a.get_unchecked(i) as *const u16 as *const __m256i);
                let v2 = _mm256_lddqu_si256(b.get_unchecked(i) as *const u16 as *const __m256i);
                let mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, v2));

                if mask != std::i32::MAX {
                    return false;
                }

                i += 16;
            }

            // Compare remainder as u64
            let end = end + ((a.len() - end) / 4);
            while i < end {
                let v1 = a.get_unchecked(i) as *const u16 as *const u64;
                let v2 = b.get_unchecked(i) as *const u16 as *const u64;

                if *v1 != *v2 {
                    return false;
                }

                i += 4;
            }

            // Compare scalar remainder
            let end = a.len();
            while i < end {
                let v1 = a.get_unchecked(i);
                let v2 = a.get_unchecked(i);

                if *v1 != *v2 {
                    return false;
                }

                i += 1;
            }
        }

        return true;
    }
}

#[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
mod simd {

}