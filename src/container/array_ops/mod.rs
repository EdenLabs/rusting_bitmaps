//! This module provides a unified interface for SIMD accelerated operations for array containers.
//! If compiled without vector extensions then these will fall back to a scalar approach

mod vector;
mod scalar;

/// Perform the set union operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to contain the full result
pub unsafe fn or(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    { vector::or(a, b, out) }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx2")))]
    { scalar::or(a, b, out) }
}

/// Perform the set intersection operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to contain the full result
pub unsafe fn and(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    { vector::and(a, b, out) }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx2")))]
    { scalar::and(a, b, out) }
}

pub fn and_cardinality(a: &[u16], b: &[u16]) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    { vector::and_cardinality(a, b) }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx2")))]
    { scalar::and_cardinality(a, b) }
}

/// Perform the set difference operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to contain the full result
pub unsafe fn and_not(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    { vector::and_not(a, b, out) }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx2")))]
    { scalar::and_not(a, b, out) }
}

/// Perform the set symmetric difference operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to contain the full result
pub unsafe fn xor(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
    #[cfg(any(target_feature = "sse4.2", target_feature = "avx2"))]
    { vector::xor(a, b, out) }
    
    #[cfg(not(any(target_feature = "sse4.2", target_feature = "avx2")))]
    { scalar::xor(a, b, out) }
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
    // TODO: Optimize

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
    // TODO: Optimize

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