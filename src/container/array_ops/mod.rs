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

#[cfg(test)]
mod test {
    use crate::test::*;
    use super::scalar;
    use super::vector;

    /// Create an array container from the given data set
    fn make_container(data: &[u16]) -> Vec<u16> {
        let mut container = Vec::with_capacity(data.len());
        container.extend_from_slice(data);

        container
    }

    fn run_test<F>(data: &[u16], f: F) 
        where F: Fn(&[u16], &[u16], *mut u16) -> usize 
    {
        let a = make_container(&DATA_A);
        let b = make_container(&DATA_B);
        let mut result = Vec::new();

        unsafe {
            result.reserve(a.len() + b.len());
            let card = (f)(&a, &b, result.as_mut_ptr());
            result.set_len(card);
        }

        let len0 = result.len();
        let len1 = data.len();
        assert_eq!(
            len0, 
            len1, 
            "\n\nUnequal cardinality. found {}, expected {}\nResult: {:#?}\nExpected: {:#?}\n\n", 
            len0, 
            len1,
            &result,
            data
        );

        let pass = result.iter()
            .zip(data.iter());
        
        let (failed, found, expected) = {
            let mut out_found = 0;
            let mut out_expected = 0;

            let mut failed = false;
            for (found, expected) in pass {
                if found != expected {
                    failed = true;
                    out_found = *found;
                    out_expected = *expected;
                    break;
                }
            }

            (failed, out_found, out_expected)
        };

        assert!(!failed, "Sets are not equivalent. Found {}, expected {}", found, expected);
    }

    #[test]
    fn or_scalar() {
        run_test(&DATA_OR, |a, b, out| unsafe { scalar::or(a, b, out) } );
    }

    #[test]
    fn and_scalar() {
        run_test(&DATA_AND, |a, b, out| unsafe { scalar::and(a, b, out) } );
    }

    #[test]
    fn and_not_scalar() {
        run_test(&DATA_AND_NOT, |a, b, out| unsafe { scalar::and_not(a, b, out) } );
    }

    #[test]
    fn xor_scalar() {
        run_test(&DATA_XOR, |a, b, out| unsafe { scalar::xor(a, b, out) } );
    }

    #[test]
    fn or_vector() {
        run_test(&DATA_OR, |a, b, out| unsafe { vector::or(a, b, out) } );
    }

    #[test]
    fn and_vector() {
        run_test(&DATA_AND, |a, b, out| unsafe { vector::and(a, b, out) } );
    }

    #[test]
    fn and_not_vector() {
        run_test(&DATA_AND_NOT, |a, b, out| unsafe { vector::and_not(a, b, out) } );
    }

    #[test]
    fn xor_vector() {
        run_test(&DATA_XOR, |a, b, out| unsafe { vector::xor(a, b, out) } );
    }
}