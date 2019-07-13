// TODO: Move all these to their respective trait impls
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
