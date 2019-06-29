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
    if b.len() == 0 {
        append_slice(a, out);
        
        return a.len();
    }

    // First operand is empty, copy into out
    if a.len() == 0 {
        append_slice(b, out);
        
        return b.len();
    }


    // Perform union of both operands and append the result into out
    let mut ptr_a = a.as_ptr();
    let mut ptr_b = b.as_ptr();
    let mut count = 0;
    
    let ptr_a_end = ptr_a.add(a.len());
    let ptr_b_end = ptr_b.add(b.len());

    loop {
        // B is greater; append A and advance the iterator
        if *ptr_a < *ptr_b {
            *(out.add(count)) = *ptr_a;
            count += 1;

            ptr_a = ptr_a.add(1);
            if ptr_a >= ptr_a_end {
                break;
            }
        }
        // A is greater; append b and advance the iterator
        else if *ptr_b < *ptr_a {
            *(out.add(count)) = *ptr_b;
            count += 1;

            ptr_b = ptr_b.add(1);
            if ptr_b >= ptr_b_end {
                break;
            }
        }
        // A and B are equal; append one and advance the iterators
        else {
            *(out.add(count)) = *ptr_a;
             count += 1;

            ptr_a = ptr_a.add(1);
            ptr_b = ptr_b.add(1);

            if ptr_a >= ptr_a_end {
                break;
            }

            if ptr_b >= ptr_b_end {
                break;
            }
        }
    }

    if ptr_a < ptr_a_end {
        let i = ptr_a.offset_from(a.as_ptr()) as usize;
        
        append_slice(&a[i..], out.add(count));
        count += a.len() - i;
    }
    else if ptr_b < ptr_b_end {
        let i = ptr_b.offset_from(b.as_ptr()) as usize;
       
        append_slice(&b[i..], out.add(count));
        count += b.len() - i;
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
    if a.len() == 0 || b.len() == 0 {
        return 0;
    }

    let mut ptr_a = a.as_ptr();
    let mut ptr_b = b.as_ptr();
    let ptr_a_end = ptr_a.add(a.len());
    let ptr_b_end = ptr_b.add(b.len());

    let mut count = 0;

    'outer: loop {
        while *ptr_a < *ptr_b {
            ptr_a = ptr_a.add(1);

            if ptr_a >= ptr_a_end {
                break 'outer;
            }
        }

        while *ptr_b < *ptr_a {
            ptr_b = ptr_b.add(1);

            if ptr_b >= ptr_b_end {
                break 'outer;
            }
        }

        if *ptr_a == *ptr_b {
            *(out.add(count)) = *ptr_a;
            count += 1;

            ptr_a = ptr_a.add(1);
            ptr_b = ptr_b.add(1);

            if ptr_a >= ptr_a_end || ptr_b >= ptr_b_end {
                break;
            }
        }
    }

    count
}

/// Find the cardinality of the the intersection of two slices using a scalar algorithm
/// 
/// # Remarks
/// - Assumes that `a` and `b` are sorted. The result is undefined if violated
pub fn and_cardinality(a: &[u16], b: &[u16]) -> usize {
    if a.len() == 0 || b.len() == 0 {
        return 0;
    }

    unsafe {
        let mut ptr_a = a.as_ptr();
        let mut ptr_b = b.as_ptr();
        let ptr_a_end = ptr_a.add(a.len());
        let ptr_b_end = ptr_b.add(b.len());

        let mut count = 0;

        'outer: loop {
            while *ptr_a < *ptr_b {
                ptr_a = ptr_a.add(1);

                if ptr_a >= ptr_a_end {
                    break 'outer;
                }
            }

            while *ptr_b < *ptr_a {
                ptr_b = ptr_b.add(1);

                if ptr_b >= ptr_b_end {
                    break 'outer;
                }
            }

            if *ptr_a == *ptr_b {
                count += 1;

                ptr_a = ptr_a.add(1);
                ptr_b = ptr_b.add(1);

                if ptr_a >= ptr_a_end || ptr_b >= ptr_b_end {
                    break;
                }
            }
        }

        count
    }
}

/// Calculate the difference between two slices using a scalar algorithm and return the number of elements in the result
///
/// # Safety
/// - Assumes that `out` has enough space for the full contents
/// 
/// # Remarks
/// - Assumes that `a` and `b` are sorted. The result is undefined if violated
pub unsafe fn and_not(a: &[u16], b: &[u16], out: *mut u16) -> usize {
    if a.len() == 0 {
        return 0;
    }
    
    if b.len() == 0 {
        append_slice(a, out);
        return a.len();
    }
    
    let mut ptr_a = a.as_ptr();
    let mut ptr_b = b.as_ptr();
    let ptr_a_end = ptr_a.add(a.len());
    let ptr_b_end = ptr_b.add(b.len());

    let mut count = 0;

    while ptr_a < ptr_a_end && ptr_b < ptr_b_end {
        if *ptr_a < *ptr_b {
            *(out.add(count)) = *ptr_a;
            count += 1;

            ptr_a = ptr_a.add(1);
        }
        else if *ptr_a == *ptr_b {
            ptr_a = ptr_a.add(1);
            ptr_b = ptr_b.add(1);
        }
        else {
            ptr_b = ptr_b.add(1);   
        }
    }

    // B finished first, append the remainder of A
    if ptr_a < ptr_a_end {
        let i = ptr_a_end.offset_from(ptr_a) as usize;

        append_slice(&a[i..], out.add(count));
        count += a.len() - i;
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
    let mut ptr_a = a.as_ptr();
    let mut ptr_b = b.as_ptr();
    let ptr_a_end = ptr_a.add(a.len());
    let ptr_b_end = ptr_b.add(b.len());
    
    let mut count = 0;

    while ptr_a < ptr_a_end && ptr_b < ptr_b_end {        
        if *ptr_a == *ptr_b {
            ptr_a = ptr_a.add(1);
            ptr_b = ptr_b.add(1);
        }
        else {
            if *ptr_a < *ptr_b {
                *(out.add(count)) = *ptr_a;
                count += 1;

                ptr_a = ptr_a.add(1);   
            }
            else {
                *(out.add(count)) = *ptr_b;
                count += 1;

                ptr_b = ptr_b.add(1);
            }
        }
    }
    
    if ptr_a < ptr_a_end {
        let i = ptr_a_end.offset_from(ptr_a) as usize;
        
        append_slice(&a[i..], out.add(count));
        count += a.len() - i;
    }
    
    if ptr_b < ptr_b_end {
        let i = ptr_b_end.offset_from(ptr_b) as usize;

        append_slice(&b[i..], out.add(count));
        count += b.len() - i;
    }

    count
}

// TODO: Write tests