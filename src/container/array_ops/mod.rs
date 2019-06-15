//! This module provides a unified interface for SIMD accelerated operations for array containers.
//! If compiled without vector extensions then these will fall back to a scalar approach

mod avx;
mod sse;
mod scalar;

/// Perform the set union operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to containe the full result
///  - Assumes that `out` is aligned to 32 bytes
pub unsafe fn or(a: Aligned<&[u16], 32>, b: Aligned<&[u16], 32>, out: *mut u16) {
    // Conditionally compile in/out the optimial version of the algorithm
    
    #[cfg(target_feaure = "avx")]
    { avx::or(a, b, out); }

    #[cfg(all(target_feature = "sse4.2", not(target_feature = "avx")))]
    { sse::or(a, b, out); }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx")))]
    { scalar::or(a, b, out); }
}

/// Perform the set intersection operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to containe the full result
///  - Assumes that `out` is aligned to 32 bytes
pub unsafe fn and(a: Aligned<&[u16], 32>, b: Aligned<&[u16], 32>, out: *mut u16) {
    // Conditionally compile in/out the optimial version of the algorithm
    
    #[cfg(target_feaure = "avx")]
    { avx::and(a, b, out); }

    #[cfg(all(target_feature = "sse4.2", not(target_feature = "avx")))]
    { sse::and(a, b, out); }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx")))]
    { scalar::and(a, b, out); }
}

/// Perform the set difference operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to containe the full result
///  - Assumes that `out` is aligned to 32 bytes
pub unsafe fn and_not(a: Aligned<&[u16], 32>, b: Aligned<&[u16], 32>, out: *mut u16) {
    // Conditionally compile in/out the optimial version of the algorithm
    
    #[cfg(target_feaure = "avx")]
    { avx::and_not(a, b, out); }

    #[cfg(all(target_feature = "sse4.2", not(target_feature = "avx")))]
    { sse::and_not(a, b, out); }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx")))]
    { scalar::and_not(a, b, out); }
}

/// Perform the set symmetric difference operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to containe the full result
///  - Assumes that `out` is aligned to 32 bytes
pub unsafe fn xor(a: Aligned<&[u16], 32>, b: Aligned<&[u16], 32>, out: *mut u16) {
    // Conditionally compile in/out the optimial version of the algorithm
    
    #[cfg(target_feaure = "avx")]
    { avx::xor(a, b, out); }

    #[cfg(all(target_feature = "sse4.2", not(target_feature = "avx")))]
    { sse::xor(a, b, out); }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx")))]
    { scalar::xor(a, b, out); }
}

// TODO: Clean this mess up

/// Count the number of elements which are less than the key
/// 
/// # Remarks
/// Assumes that the array is sorted and all elements are unique
pub fn count_less(slice: &[u16], key: u16) -> usize {
    match slice.binary_search(&key) {
        Ok(index) => index,
        Err(index) => index + 1
    }
}

/// Count the number of elements which are greater than the key
/// 
/// # Remarks
/// Assumes that the array is sorted and all elements are unique
pub fn count_greater(slice: &[u16], key: u16) -> usize {
    match slice.binary_search(&key) {
        Ok(index) => slice.len() - index,
        Err(index) => slice.len() - (index + 1)
    }
}

pub fn advance_until(slice: &[u16], index: usize, min: u16) -> usize {
    let mut lower = index as usize;
    if lower >= slice.len() || slice[lower] >= min {
        return lower;
    }

    let mut span_size = 1;
    let mut bound = lower + span_size;

    while bound < slice.len() && slice[bound] < min {
        span_size = span_size << 1;

        bound = lower + span_size;
    }

    let mut upper = {
        if bound < slice.len() {
            bound
        }
        else {
            slice.len() - 1
        }
    };

    if slice[upper] == min {
        return upper;
    }

    if slice[upper] < min {
        return slice.len();
    }

    lower += span_size >> 1;

    while lower + 1 != upper {
        let mid = (lower + upper) >> 1;

        if slice[mid] == min {
            return mid;
        }
        else if slice[mid] < min {
            lower = mid;
        }
        else {
            upper = mid;
        }
    }

    upper
}

pub fn exponential_search<T>(slice: &[T], size: usize, key: T) -> Result<usize, usize>
    where T: Copy + Ord + Eq
{
    //  No values to find or size extends beyond slice length
    if size == 0 || size > slice.len() {
        return Err(0);
    }

    let mut bound = 0;
    while bound < size && slice[bound] < key {
        bound *= 2;
    }

    return slice[(bound / 2)..((bound + 1).min(size))].binary_search(&key);
}

/// Calculate the difference (`A \ B`) between two slices using scalar instructions
///
/// # Assumptions
///  - The contents of `a` and `b` are sorted
fn scalar_difference<T>(a: &[T], b: &[T], out: &mut Vec<T>)
    where T: Copy + Ord + Eq
{
    if a.len() == 0 {
        return;
    }
    
    if b.len() == 0 {
        out.extend_from_slice(b);
    }
    
    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        
        let mut val_a = *a.get_unchecked(i_a);
        let mut val_b = *b.get_unchecked(i_b);
        
        loop {
            if val_a < val_b {
                out.push(val_a);
                
                i_a += 1;
                if i_a >= a.len() {
                    break;
                }
                
                val_a = *a.get_unchecked(i_a);
            }
            else if val_a == val_b {
                i_a += 1;
                i_b += 1;
                
                if i_a >= a.len() {
                    break;
                }
                
                // End of B, Append the remainder of A
                if i_b >= b.len() {
                    out.extend_from_slice(&a[i_a..a.len()]);
                    return;
                }
            }
            else {
                i_b += 1;
                
                // End of B, append remainder of A
                if i_b > b.len() {
                    out.extend_from_slice(&a[i_a..a.len()]);
                    return;
                }
                
                val_b = *b.get_unchecked(i_b);
            }
        }
    }
}

/// Calculate the symmetric difference (`(A \ B) ∪ (B \ A)`) between two slices using scalar instructions
/// 
/// # Assumptions
///  - The contents of `a` and `b` are sorted
fn scalar_symmetric_difference<T>(a: &[T], b: &[T], out: &mut Vec<T>)
    where T: Copy + Ord + Eq
{
    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        
        while i_a < a.len() && i_b < b.len() {
            let v_a = *a.get_unchecked(i_a);
            let v_b = *b.get_unchecked(i_b);
            
            if v_a == v_b {
                i_a += 1;
                i_b += 1;
                continue;
            }
            
            if v_a < v_b {
                out.push(v_a);
                
                i_a += 1;
            }
            else {
                out.push(v_b);
                i_b += 1;
            }
        }
        
        if i_a < a.len() {
            out.extend_from_slice(&a[i_a..a.len()]);
        }
        
        if i_b < b.len() {
            out.extend_from_slice(&b[i_b..b.len()]);
        }
    }
}

