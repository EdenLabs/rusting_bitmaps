mod simd;

// Full
#[inline(always)]
pub unsafe fn union(a: &[u64], b: &[u64], out: &mut Vec<u64>) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::union(a, b, out)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_union(a, b, out)
    }
}

#[inline(always)]
pub unsafe fn intersect(a: &[u64], b: &[u64], out: &mut Vec<u64>) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::intersect(a, b, out)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_intersect(a, b, out)
    }
}

#[inline(always)]
pub unsafe fn difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::difference(a, b, out)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_difference(a, b, out)
    }
}

#[inline(always)]
pub unsafe fn symmetric_difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::symmetric_difference(a, b, out)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_symmetric_difference(a, b, out)
    }
}

// Lazy
#[inline(always)]
pub unsafe fn union_lazy(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::union_lazy(a, b, out)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_union_lazy(a, b, out)
    }
}

#[inline(always)]
pub unsafe fn intersect_lazy(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::intersect_lazy(a, b, out)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_intersect_lazy(a, b, out)
    }
}

#[inline(always)]
pub unsafe fn difference_lazy(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::difference_lazy(a, b, out)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_difference_lazy(a, b, out)
    }
}

#[inline(always)]
pub unsafe fn symmetric_difference_lazy(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::symmetric_difference_lazy(a, b, out)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_symmetric_difference_lazy(a, b, out)
    }
}

// Cardinality
#[inline(always)]
pub unsafe fn union_cardinality(a: &[u64], b: &[u64]) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::union_cardinality(a, b)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_union_cardinality(a, b)
    }
}

#[inline(always)]
pub unsafe fn intersect_cardinality(a: &[u64], b: &[u64]) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::intersect_cardinality(a, b)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_intersect_cardinality(a, b)
    }
}

#[inline(always)]
pub unsafe fn difference_cardinality(a: &[u64], b: &[u64]) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::difference_cardinality(a, b)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_difference_cardinality(a, b)
    }
}

#[inline(always)]
pub unsafe fn symmetric_difference_cardinality(a: &[u64], b: &[u64]) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::symmetric_difference_cardinality(a, b)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_symmetric_difference_cardinality(a, b)
    }
}

/// Harley-Seal algorithm for counting the number of bits in an array
/// 
/// # Safety
/// Assumes that the input is evenly divisible by 64 when using vectorized instructions, scalar has no constraints
pub unsafe fn harley_seal(v: &[u64]) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        assert!(v.len() % 64 == 0);

        simd::harley_seal(v)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_harley_seal(v)
    }
}

// Universal scalar implementations
// 32bit not natively supported but will work albeit slower than a hand optimized version

macro_rules! bitmap_op {
    ($name: ident, $word_a: ident, $word_b: ident, $($op: tt)*) => {
        unsafe fn $name(a: &[u64], b: &[u64], out: &mut Vec<u64>) -> usize {
            let mut i_a = 0;
            let mut i_b = 0;
            let mut cardinality = 0;

            while i_a < a.len() && i_b < b.len() {
                let $word_a = *a.get_unchecked(i_a);
                let $word_b = *b.get_unchecked(i_b);
                let word = $($op)*;

                cardinality += popcount_mul(word);

                out.push(word);
                
                i_a += 1;
                i_b += 1;
            }

            cardinality as usize
        }
    };
}

macro_rules! bitmap_op_lazy {
    ($name: ident, $word_a: ident, $word_b: ident, $($op: tt)*) => {
        unsafe fn $name(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
            let mut i_a = 0;
            let mut i_b = 0;

            while i_a < a.len() && i_b < b.len() {
                let $word_a = *a.get_unchecked(i_a);
                let $word_b = *b.get_unchecked(i_b);

                out.push($($op)*);
                
                i_a += 1;
                i_b += 1;
            }
        }
    };
}

macro_rules! bitmap_op_cardinality {
    ($name: ident, $word_a: ident, $word_b: ident, $($op: tt)*) => {
        unsafe fn $name(a: &[u64], b: &[u64]) -> usize {
            let mut i_a = 0;
            let mut i_b = 0;
            let mut cardinality = 0;

            while i_a < a.len() && i_b < b.len() {
                let $word_a = *a.get_unchecked(i_a);
                let $word_b = *b.get_unchecked(i_b);

                cardinality += popcount_mul($($op)*);

                i_a += 1;
                i_b += 1;
            }

            cardinality as usize
        }
    };
}

bitmap_op!(scalar_union               , word_a, word_b, word_a | word_b            );
bitmap_op!(scalar_intersect           , word_a, word_b, word_a & word_b            );
bitmap_op!(scalar_difference          , word_a, word_b, (word_a & word_b) & !word_b);
bitmap_op!(scalar_symmetric_difference, word_a, word_b, word_a ^ word_b            );

bitmap_op_lazy!(scalar_union_lazy               , word_a, word_b, word_a | word_b            );
bitmap_op_lazy!(scalar_intersect_lazy           , word_a, word_b, word_a & word_b            );
bitmap_op_lazy!(scalar_difference_lazy          , word_a, word_b, (word_a & word_b) & !word_b);
bitmap_op_lazy!(scalar_symmetric_difference_lazy, word_a, word_b, word_a ^ word_b            );

bitmap_op_cardinality!(scalar_union_cardinality               , word_a, word_b, word_a | word_b            );
bitmap_op_cardinality!(scalar_intersect_cardinality           , word_a, word_b, word_a & word_b            );
bitmap_op_cardinality!(scalar_difference_cardinality          , word_a, word_b, (word_a & word_b) & !word_b);
bitmap_op_cardinality!(scalar_symmetric_difference_cardinality, word_a, word_b, word_a ^ word_b            );

fn scalar_harley_seal(v: &[u64]) -> usize {
    unsafe {
        let d = v.as_ptr() as *const u64;
        let limit = v.len() >> 4;

        let mut i: isize = 0;
        let mut result = 0;

        if limit > 0 {
            let mut total = 0;
            let mut ones = 0;
            let mut twos = 0;
            let mut fours = 0;
            let mut eights = 0;
            let mut sixteens = 0;

            let mut twos_a = 0;
            let mut twos_b = 0;
            let mut fours_a = 0;
            let mut fours_b = 0;
            let mut eights_a = 0;
            let mut eights_b = 0;

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

                result += popcount_mul(sixteens);

                i += 16;
            }

            result *= 16;
            result += 8 * popcount_mul(eights);
            result += 4 * popcount_mul(fours);
            result += 2 * popcount_mul(twos);
            result += popcount_mul(ones);
        }

        while i < v.len() as isize {
            result += popcount_mul(*d.offset(i));

            i += 1;
        }

        result as usize
    }
}

/// Count the number of set bits in a 64 bit word
fn popcount_mul(mut x: u64) -> u64 {
    let m1  = 0x5555555555555555;
    let m2  = 0x3333333333333333;
    let m4  = 0x0F0F0F0F0F0F0F0F;
    let h01 = 0x0101010101010101;

    x -= (x >> 1) & m1;
    x = (x & m2) + ((x >> 2) & m2);
    x = (x + (x >> 4)) & m4;

    (x * h01) >> 56
}

/// Carry save adder
#[inline]
fn csa(a: u64, b: u64, c: u64, h: &mut u64, l: &mut u64) {
    let u = a ^ b;
    *h = (a & b) | (u & c);
    *l = u ^ c;
}
