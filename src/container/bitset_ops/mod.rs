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
    use crate::test::short::*;
    use crate::container::BitsetContainer;

    /// Create an array container from the given data set
    fn make_container(data: &[u16]) -> BitsetContainer {
        let mut container = BitsetContainer::new();
        container.set_list(data);

        container
    }

    fn run_test<F>(data: &[u16], f: F) 
        where F: Fn(&[u64], &[u64], &mut [u64]) 
    {
        let a = make_container(&INPUT_A);
        let b = make_container(&INPUT_B);
        let mut result = BitsetContainer::new();

        (f)(&a, &b, &mut result);

        let len0 = result.cardinality();
        let len1 = data.len();
        assert_eq!(
            len0, 
            len1, 
            "\n\nUnequal cardinality. found {}, expected {}\n\n", 
            len0, 
            len1
        );

        let pass = result.iter()
            .zip(data.iter());
        
        let (failed, found, expected) = {
            let mut out_found = 0;
            let mut out_expected = 0;

            let mut failed = false;
            for (found, expected) in pass {
                if found != *expected {
                    failed = true;
                    out_found = found;
                    out_expected = *expected;
                    break;
                }
            }

            (failed, out_found, out_expected)
        };

        assert!(!failed, "Sets are not equivalent. Found {}, expected {}", found, expected);
    }

    #[test]
    fn or() {
        run_test(&RESULT_OR, super::or);
    }

    #[test]
    fn and() {
        run_test(&RESULT_AND, super::and);
    }

    #[test]
    fn and_not() {
        run_test(&RESULT_AND_NOT, super::and_not);
    }

    #[test]
    fn xor() {
        run_test(&RESULT_XOR, super::xor);
    }

}