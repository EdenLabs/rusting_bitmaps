mod simd;

// TODO: Add cardinality checks for all ops

#[inline(always)]
pub fn union(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::union(a, b, out);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_union(a, b, out);
    }
}

#[inline(always)]
pub fn intersect(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::intersect(a, b, out);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_intersect(a, b, out);
    }
}

#[inline(always)]
pub fn difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::difference(a, b, out);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_difference(a, b, out);
    }
}

#[inline(always)]
pub fn symmetric_difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::symmetric_difference(a, b, out);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_symmetric_difference(a, b, out);
    }
}

pub fn harley_seal(v: &[u64]) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::harley_seal(v)
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_harley_seal(v)
    }
}

// Universal scalar implementations
// 32bit not natively supported but will work albeit slower than a hand optimized version

fn scalar_union(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    // Handle the cases where A or B is 0
    if a.len() == 0 {
        out.extend_from_slice(b);
    }
    
    if b.len() == 0 {
        out.extend_from_slice(a);
    }
    
    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        
        while i_a < a.len() && i_b < b.len() {
            let word_a = *a.get_unchecked(i_a);
            let word_b = *b.get_unchecked(i_b);
            
            out.push(word_a | word_b);
            
            i_a += 1;
            i_b += 1;
        }
        
        // B finished first, append the rest of A
        if i_a < a.len() {
            out.extend_from_slice(&a[i_a..a.len()]);            
        }
        
        // A finished first, append the rest of B
        if i_b < b.len() {
            out.extend_from_slice(&b[i_b..b.len()]);
        }
    }
}

fn scalar_intersect(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    // One is empty. No possible intersection
    if a.len() == 0 || b.len() == 0 {
        return;
    }
    
    unsafe {
        // Perform the intersection till there's no more elemetns to process
        let mut i_a = 0;
        let mut i_b = 0;
        
        while i_a < a.len() && i_b < b.len() {
            let word_a = *a.get_unchecked(i_a);
            let word_b = *b.get_unchecked(i_b);
            
            out.push(word_a & word_b);
            
            i_a += 1;
            i_b += 1;
        }
    }
}

fn scalar_difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    // A has no elements. No possible difference
    if a.len() == 0 {
        return;
    }
    
    // B has no elements, Difference is all of A
    if b.len() == 0 {
        out.extend_from_slice(a);
        return;
    }
    
    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        
        while i_a < a.len() && i_b < b.len() {
            let word_a = *a.get_unchecked(i_a);
            let word_b = *b.get_unchecked(i_b);
            
            out.push((word_a & word_b) & !word_b);
            
            i_a += 1;
            i_b += 1;
        }
        
        // B finishd first, append the rest of A
        if i_a < a.len() {
            out.extend_from_slice(&a[i_a..a.len()]);
        }
    }
}

fn scalar_symmetric_difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        
        while i_a < a.len() && i_b < b.len() {
            let word_a = *a.get_unchecked(i_a);
            let word_b = *b.get_unchecked(i_b);
            
            out.push(word_a ^ word_b);
            
            i_a += 1;
            i_b += 1;
        }
        
        if i_a < a.len() {
            out.extend_from_slice(&a[i_a..a.len()]);
        }
        
        if i_b < b.len() {
            out.extend_from_slice(&b[i_b..b.len()]);
        }
    }
}


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

/// Count the number of set bits in a 128 bit vector
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
