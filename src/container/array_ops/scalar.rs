use std::ptr;

/// Append the contents of a slice to the memory pointed at by `out`
unsafe fn append_slice(slice: &[u16], dst: *mut u16) {
    let src = slice.as_ptr();
    let len = slice.len();

    ptr::copy(src, dst, len);
}

/// Calculate the union of two slices using a scalar algorithm and return the number of elements in the result
///
/// # Safety
/// - Assumes that `out` has enough space for the full contents
/// 
/// # Remarks
/// - Assumes that `a` and `b` are sorted. The result is undefined if violated
pub unsafe fn or(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    // Second operand is empty, just copy into out
    if b.is_empty() {
        append_slice(a, out);
        return a.len();
    }

    // First operand is empty, copy into out
    if a.is_empty() {
        append_slice(b, out);
        return b.len();
    }

    // Perform union of both operands and append the result into out
    let mut i0 = 0;
    let mut i1 = 0;
    let mut count = 0;
    
    while i0 < a.len() && i1 < b.len() {
        let s0 = a[i0];
        let s1 = b[i1];

        // B is greater; append A and advance the iterator
        if s0 < s1 {
            ptr::write(out.add(count), s0);
            i0 += 1;
            count += 1;
        }
        // A is greater; append b and advance the iterator
        else if s1 < s0 {
            ptr::write(out.add(count), s1);
            i1 += 1;
            count += 1;
        }
        // A and B are equal; append one and advance the iterators
        else {
            ptr::write(out.add(count), s0);
            i0 += 1;
            i1 += 1;
            count += 1;
        }
    }

    // Append remainders
    if i0 < a.len() {
        append_slice(&a[i0..], out.add(count));
        count += a.len() - i0;
    }
    
    if i1 < b.len() {
        append_slice(&b[i1..], out.add(count));
        count += b.len() - i1;
    }
        
    count
}

/// Calculate the intersection of two slices using a scalar algorithm and return the number of elements in the result
///
/// # Safety
/// - Assumes that `out` has enough space for the full contents
/// 
/// # Remarks
/// - Assumes that `a` and `b` are sorted. The result is undefined if violated
pub unsafe fn and(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    if a.is_empty() || b.is_empty() {
        return 0;
    }

    let mut i0 = 0;
    let mut i1 = 0;
    let mut count = 0;

    while i0 < a.len() && i1 < b.len() {
        let s0 = a[i0];
        let s1 = b[i1];

        if s0 < s1 {
            i0 += 1;
        }
        else if s1 < s0 {
            i1 += 1;
        }
        else {
            ptr::write(out.add(count), s0);
            i0 += 1;
            i1 += 1;
            count += 1;
        }
    }

    count
}

/// Find the cardinality of the the intersection of two slices using a scalar algorithm
/// 
/// # Remarks
/// - Assumes that `a` and `b` are sorted. The result is undefined if violated
pub fn and_cardinality(a: &[u16], b: &[u16]) -> usize {
    if a.is_empty() || b.is_empty() {
        return 0;
    }

    let mut i0 = 0;
    let mut i1 = 0;
    let mut count = 0;

    while i0 < a.len() && i1 < b.len() {
        let s0 = a[i0];
        let s1 = b[i1];

        if s0 < s1 {
            i0 += 1;
        }
        else if s1 < s0 {
            i1 += 1;
        }
        else {
            i0 += 1;
            i1 += 1;
            count += 1;
        }
    }

    count
}

/// Calculate the difference between two slices using a scalar algorithm and return the number of elements in the result
///
/// # Safety
/// - Assumes that `out` has enough space for the full contents
/// 
/// # Remarks
/// - Assumes that `a` and `b` are sorted. The result is undefined if violated
pub unsafe fn and_not(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    if a.is_empty() {
        return 0;
    }
    
    if b.is_empty() {
        append_slice(a, out);
        return a.len();
    }

    let mut i0 = 0;
    let mut i1 = 0;
    let mut count = 0;

    while i0 < a.len() && i1 < b.len() {
        let s0 = a[i0];
        let s1 = b[i1];

        if s0 < s1 {
            ptr::write(out.add(count), s0);
            i0 += 1;
            count += 1;
        }
        else if s1 < s0 {
            i1 += 1;
        }
        else {
            i0 += 1;
            i1 += 1;
        }
    }

    // B finished first, append the remainder of A
    if i0 < a.len() {
        append_slice(&a[i0..], out.add(count));
        count += a.len() - i0;
    }

    count
}

/// Calculate the difference between two slices using a scalar algorithm and return the number of elements in the result
///
/// # Safety
/// - Assumes that `out` has enough space for the full contents
/// 
/// # Remarks
/// - Assumes that `a` and `b` are sorted. The result is undefined if violated
pub unsafe fn xor(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    let mut i0 = 0;
    let mut i1 = 0;
    let mut count = 0;

    while i0 < a.len() && i1 < b.len() {        
        let s0 = a[i0];
        let s1 = b[i1];
        
        if s0 == s1 {
            i0 += 1;
            i1 += 1;
        }
        else {
            if s0 < s1 {
                ptr::write(out.add(count), s0);
                i0 += 1;   
                count += 1;
            }
            else {
                ptr::write(out.add(count), s1);
                i1 += 1;
                count += 1;
            }
        }
    }

    if i0 < a.len() {
        append_slice(&a[i0..], out.add(count));
        count += a.len() - i0;
    }
    
    if i1 < b.len() {
        append_slice(&b[i1..], out.add(count));
        count += b.len() - i1;
    }

    count
}