macro_rules! bitset_op {
    ($name: ident, $($op:tt)*) => {
        /// Perform the operation between `a` and `b` and write the result into `out`
        pub fn $name(a: &[u64], b: &[u64], out: &mut [u64]) {
            let pass = a.iter()
                .zip(b.iter())
                .enumerate()
                .map(|(i, (wa, wb))| (i, wa, wb));

            for (i, wa, wb) in pass {
                out[i] = wa $($op)* wb;
            }
        }
    };
}

bitset_op!(or, |);

bitset_op!(and, &);

bitset_op!(and_not, &!);

bitset_op!(xor, ^);

/// Compute the cardinality of the bitset
pub fn cardinality(bitset: &[u64]) -> usize {
    let mut count = 0;
    for word in bitset.iter() {
        count += word.count_ones();
    }

    count as usize
}

/// Compute the cardinality of the intersection of two bitsets
pub fn and_cardinality(a: &[u64], b: &[u64]) -> usize {
    let mut count = 0;
    let pass = a.iter()
        .zip(b.iter());

    for (a, b) in pass {
        count += (a & b).count_ones();
    }

    count as usize
}

#[cfg(test)]
mod test {
    use crate::test::*;
    use crate::container::BitsetContainer;

    /// Create an array container from the given data set
    fn make_container(data: &[u16]) -> BitsetContainer {
        let mut container = BitsetContainer::new();
        container.set_list(data);

        container
    }

    fn run_test<F>(op: OpType, f: F) 
        where F: Fn(&[u64], &[u64], &mut [u64]) 
    {
        let data_a = generate_data(0..1024, 1, 1);
        let data_b = generate_data(0..1024, 1, 1);
        let a = { let mut v = Vec::with_capacity(data_a.len()); v.extend_from_slice(&data_a); v };
        let b = { let mut v = Vec::with_capacity(data_b.len()); v.extend_from_slice(&data_b); v };
        let e = compute_result(&data_a, &data_b, op);

        let mut result = vec![0; a.len()];

        unsafe {
            result.reserve(a.len() + b.len());
            (f)(&a, &b, &mut result);
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
    fn or() {
        run_test(OpType::Or, super::or);
    }

    #[test]
    fn and() {
        run_test(OpType::And, super::and);
    }

    #[test]
    fn and_not() {
        run_test(OpType::AndNot, super::and_not);
    }

    #[test]
    fn xor() {
        run_test(OpType::Xor, super::xor);
    }

}