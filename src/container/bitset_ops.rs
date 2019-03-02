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

// Feature set specific implementations

// AVX2 only
#[cfg(target_feature = "avx2")]
mod simd {
    use std::arch::x86_64::{
        
    };
    
    pub fn union(a: &[u64], b: &[u64], out: &mut Vec<u64>) {

    }

    pub fn intersect(a: &[u64], b: &[u64], out: &mut Vec<u64>) {

    }

    pub fn difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) {

    }

    pub fn symmetric_difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
        
    }
}

// SSE4.2 only (older feature sets unsupported by the algorithms)
#[cfg(all(target_feature = "sse4.2", not(target_feature = "avx2")))]
mod simd {
    pub fn union(a: &[u64], b: &[u64], out: &mut Vec<u64>) {

    }

    pub fn intersect(a: &[u64], b: &[u64], out: &mut Vec<u64>) {

    }

    pub fn difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) {

    }

    pub fn symmetric_difference(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
        
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
