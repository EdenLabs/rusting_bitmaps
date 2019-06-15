use std::ptr;

/// Append the contents of a slice to the memory pointed at by `out`
fn append_slice(slice: &[u16], dst: *mut u16) {
    let src = slice.as_ptr();
    let len = slice.len();
    
    ptr::copy(src, dst, len);
}

/// Calculate the union of two slices using a scalar algorithm and return the number of elements in the result
///
/// # Safety
/// - Assumes that `out` has enough space for the full contents
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
            *(out.add(count)) = ptr_b;
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
        let i = ptr_a.offset_from(a.as_ptr());
        
        append_slice(&a[i..], out);
        count += a.len() - i;
    }
    else if i_b < b.len() {
        let i = ptr_b.offset_from(b.as_ptr());
       
        append_slice(&b[i..], out);
        count += b.len() - i;
    }
        
    count
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

#[cfg(test)]
mod test {
    use crate::containers::array_ops::scalar;
    
    /// A struct defining a single test case for an operation
    struct TestCase {
        pub name: &'static str,
        pub a: Vec<u16>,
        pub b: Vec<u16>
    }
    
    struct TestResult {
        pub expected: Vec<u16>
    }
    
    /// Get a uniform set of test cases for all possible inputs
    fn cases() -> Vec<TestCase> {
        vec![
            TestCase {
                name: "A and B empty"
                a: Vec::new(),
                b: Vec::new(),
            },
            TestCase {
                name: "A empty",
                a: Vec::new(),
                b: vec![1, 2, 3, 4]
            },
            TestCase {
                name: "B empty",
                a: vec![1, 2, 3, 4],
                b: Vec::new()
            },
            TestCase {
                name: "A and B identical",
                a: vec![1, 2, 3, 4],
                b: vec![1, 2, 3, 4]
            },
            TestCase {
                name: "Disjoint",
                a: vec![1, 2, 3, 4],
                b: vec![6. 7, 2, 7]
            },
            TestCase {
                name: "Variable lengths, partially overlapping",
                a: vec![1, 3, 5, 7, 8, 9],
                b: vec![0, 2, 4, 6]
            }
            // TODO: Resize to make viable for simd
        ]
    }
    
    fn results_or() -> Vec<TestResult> {
        vec! [
            TestResult {
                expected: Vec::new()
            },
            TestResult {
                expected: vec![1, 2, 3, 4]
            },
            TestResult {
                expected: vec![1, 2, 3, 4]
            },
            TestResult {
                expected: vec![1, 2, 3, 4]
            }
            TestResult {
                expected: vec![1, 2, 3, 4, 6, 7, 2, 7]
            }
            TestResult {
                expected: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            }
        ]
    }
    
    /// Run a given test case
    fn run(case: &TestCase, result: &TestResult, target: fn(&[u16], &[u16], *mut u16) -> usize) {
        unsafe {
            let mut out = Vec::with_capacity(result.expected.len());
            let card = (target)(&case.a, &case.b, out.as_mut_ptr());
            
            assert!(card == out.len());
            assert!(out == result.expected);
        }
    }
    
    #[test]
    fn or() {
        let c = cases();
        let r = results_or();
        
        for (case, result) in c.iter().zip(r) {
            run(case, result, scalar::or);
        }
    }
}
