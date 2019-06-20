
macro_rules! bitset_op {
    ($name: ident, $($op:tt)*) => {
        pub unsafe fn $name(a: &[u64], b: &[u64], out: *mut u64) {
            debug_assert!(a.len() == b.len());
            debug_assert!(!out.is_null());
            
            let ptr_a = a.as_ptr();
            let ptr_b = b.as_ptr();
            let len = a.len();

            for i in 0..len {
                *(out.add(i)) = *(ptr_a.add(i)) $($op)* *(ptr_b.add(i));
            }
        }
    };
}

bitset_op!(or, |);

bitset_op!(and, &);

bitset_op!(and_not, &!);

bitset_op!(xor, ^);

pub fn cardinality(bitset: &[u64]) -> usize {
    let mut count = 0;
    for word in bitset.iter() {
        count += word.count_ones();
    }

    count as usize
}