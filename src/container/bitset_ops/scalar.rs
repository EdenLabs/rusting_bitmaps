use crate::container::bitset::BITSET_SIZE_IN_WORDS;

macro_rules! bitset_op {
    ($name: ident, $($op:tt)*) => {
        pub unsafe fn $name(a: *const u64, b: *const u64, out: *mut u64) {
            for i in 0..BITSET_SIZE_IN_WORDS {
                *(out.add(i)) = *(a.add(i)) $($op)* *(b.add(i));
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