mod simd;

use crate::min;

// TODO: See about moving to aligned loads and having some way to enforce that
// TODO: Implement the cardinality ops for arrays (is this even necessary with pure vecs?)

pub fn union(a: &[u16], b: &[u16], out: &mut Vec<u16>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::union(a, b, out);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_union(a, b, out);
    }
}

pub fn intersect(a: &[u16], b: &[u16], out: &mut Vec<u16>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::intersect(a, b, out);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_intersect(a, b, out);
    }
}

pub fn difference(a: &[u16], b: &[u16], out: &mut Vec<u16>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::difference(a, b, out);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_difference(a, b, out);
    }
}

pub fn symmetric_difference(a: &[u16], b: &[u16], out: &mut Vec<u16>) {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))] {
        simd::symmetric_difference(a, b, out);
    }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))] {
        scalar_symmetric_difference(a, b, out);
    }
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

    return slice[(bound / 2)..min(bound + 1, size)].binary_search(&key);
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

/// Calculate the union (`A ∪ B`) of two slices using scalar instructions
///
/// # Assumptions
///  - The contents of `a` and `b` are sorted
fn scalar_union<T>(a: &[T], b: &[T], out: &mut Vec<T>)
    where T: Copy + Ord + Eq
{
    // Second operand is empty, just copy into out
    if b.len() == 0 {
        out.extend_from_slice(a);
        return;
    }

    // First operand is empty, copy into out
    if a.len() == 0 {
        out.extend_from_slice(b);
        return;
    }

    unsafe {
        // Perform union of both operands and append the result into out
        let mut i_a = 0;
        let mut i_b = 0;
        let mut val_a = *a.get_unchecked(i_a);
        let mut val_b = *b.get_unchecked(i_a);

        loop {
            // B is greater; append A and advance the iterator
            if val_a < val_b {
                out.push(val_a);

                i_a += 1;
                if i_a >= a.len() {
                    break;
                }

                val_a = *a.get_unchecked(i_a);
            }
            // A is greater; append b and advance the iterator
            else if val_b < val_a {
                out.push(val_b);

                i_b += 1;
                if i_b >= b.len() {
                    break;
                }

                val_b = *b.get_unchecked(i_b);
            }
            // A and B are equal; append one and advance the iterators
            else {
                out.push(val_a);

                i_a += 1;
                i_b += 1;

                if i_a >= a.len() {
                    break;
                }

                if i_b >= b.len() {
                    break;
                }

                val_a = *a.get_unchecked(i_a);
                val_b = *b.get_unchecked(i_b);
            }
        }

        if i_a < a.len() {
            out.extend_from_slice(&a[i_a..a.len()]);
        }
        else if i_b < b.len() {
            out.extend_from_slice(&b[i_b..b.len()]);
        }
    }
}

/// Calculate the intersection (`A ∩ B`) of two slices using scalar instructions
///
/// # Assumptions
///  - The contents of `a` and `b` are sorted
fn scalar_intersect<T>(a: &[T], b: &[T], out: &mut Vec<T>)
    where T: Copy + Ord + Eq
{
    if a.len() == 0 || b.len() == 0 {
        return;
    }

    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        let mut v_a = *a.get_unchecked(i_a);
        let mut v_b = *b.get_unchecked(i_b);

        loop {
            while v_a < v_b {
                i_a += 1;
                if i_a >= a.len() {
                    return;
                }

                v_a = *a.get_unchecked(i_a);
            }

            while v_a > v_b {
                i_b += 1;
                if i_b >= b.len() {
                    return;
                }

                v_b = *b.get_unchecked(i_b);
            }

            if v_a == v_b {
                out.push(v_a);

                i_a += 1;
                i_b += 1;

                if i_a > a.len() || i_b >= b.len() {
                    return;
                }
            }
        }
    }
}
