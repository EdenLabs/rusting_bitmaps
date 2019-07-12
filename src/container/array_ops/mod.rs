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
    #[cfg(target_feature = "sse4.2")]
    { vector::or(a, b, out) }
    
    #[cfg(not(target_feature = "sse4.2"))]
    { scalar::or(a, b, out) }
}

/// Perform the set intersection operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to contain the full result
pub unsafe fn and(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
    #[cfg(target_feature = "sse4.2")]
    { vector::and(a, b, out) }
    
    #[cfg(not(target_feature = "sse4.2"))]
    { scalar::and(a, b, out) }
}

pub fn and_cardinality(a: &[u16], b: &[u16]) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
   #[cfg(target_feature = "sse4.2")]
    { vector::and_cardinality(a, b) }
    
    #[cfg(not(target_feature = "sse4.2"))]
    { scalar::and_cardinality(a, b) }
}

/// Perform the set difference operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to contain the full result
pub unsafe fn and_not(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
    #[cfg(target_feature = "sse4.2")]
    { vector::and_not(a, b, out) }
    
    #[cfg(not(target_feature = "sse4.2"))]
    { scalar::and_not(a, b, out) }
}

/// Perform the set symmetric difference operation between `a` and `b` outputting the results into `out`
/// 
/// # Safety
///  - Assumes that `out` has enough space to contain the full result
pub unsafe fn xor(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    // Conditionally compile in/out the optimial version of the algorithm
    #[cfg(target_feature = "sse4.2")]
    { vector::xor(a, b, out) }
    
    #[cfg(not(target_feature = "sse4.2"))]
    { scalar::xor(a, b, out) } // Segfaults
}

// TODO: Clean this mess up

/// Count the number of elements which are less than the key
/// 
/// # Remarks
/// Assumes that the array is sorted and all elements are unique
pub fn count_less(slice: &[u16], key: u16) -> usize {
    if slice.len() == 0 {
        return 0;
    }

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
    if slice.len() == 0 {
        return 0;
    }

    match slice.binary_search(&key) {
        Ok(index) => slice.len() - (index + 1),
        Err(index) => slice.len() - index.saturating_sub(1)
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
        span_size <<= 1;

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

#[cfg(test)]
mod test {
    use crate::test::*;
    use super::scalar;

    #[cfg(target_feature = "sse4.2")]
    use super::vector;

    fn run_test<F>(op: OpType, f: F) 
        where F: Fn(&[u16], &[u16], *mut u16) -> usize 
    {
        let data_a = generate_data(0..65535, 500);
        let data_b = generate_data(0..65535, 500);
        let a = { let mut v = Vec::with_capacity(data_a.len()); v.extend_from_slice(&data_a); v };
        let b = { let mut v = Vec::with_capacity(data_b.len()); v.extend_from_slice(&data_b); v };
        let e = compute_result(&data_a, &data_b, op);

        let mut result = Vec::new();

        unsafe {
            result.reserve(a.len() + b.len());
            let card = (f)(&a, &b, result.as_mut_ptr());
            result.set_len(card);
        }

        let len0 = result.len();
        let len1 = e.len();
        assert_eq!(
            len0, 
            len1, 
            "Unequal cardinality. found {}, expected {}", 
            len0, 
            len1
        );

        let pass = result.iter()
            .zip(e.iter());

        for (found, expected) in pass {
            assert_eq!(*found, *expected);
        }
    }

    #[test]
    fn or_scalar() {
        run_test(OpType::Or, |a, b, out| unsafe { scalar::or(a, b, out) } );
    }

    #[test]
    fn and_scalar() {
        run_test(OpType::And, |a, b, out| unsafe { scalar::and(a, b, out) } );
    }

    #[test]
    fn and_not_scalar() {
        run_test(OpType::AndNot, |a, b, out| unsafe { scalar::and_not(a, b, out) } );
    }

    #[test]
    fn xor_scalar() {
        run_test(OpType::Xor, |a, b, out| unsafe { scalar::xor(a, b, out) } );
    }

    #[test]
    #[cfg(target_feature = "sse4.2")]
    fn or_vector() {
        run_test(OpType::Or, |a, b, out| unsafe { vector::or(a, b, out) } );
    }

    #[test]
    #[cfg(target_feature = "sse4.2")]
    fn and_vector() {
        run_test(OpType::And, |a, b, out| unsafe { vector::and(a, b, out) } );
    }

    #[test]
    #[cfg(target_feature = "sse4.2")]
    fn and_not_vector() {
        run_test(OpType::AndNot, |a, b, out| unsafe { vector::and_not(a, b, out) } );
    }

    #[test]
    #[cfg(target_feature = "sse4.2")]
    fn xor_vector() {
        run_test(OpType::Xor, |a, b, out| unsafe { vector::xor(a, b, out) } );
    }
}