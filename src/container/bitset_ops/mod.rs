mod scalar;
mod vector;

macro_rules! bitset_op {
    ($name:ident) => {
        /// Perform the operation between `a` and `b` and write the result into `out`
        /// 
        /// # Safety
        ///  - Assumes that `a`, `b`, and out` are `BITSET_SIZE_WORDS` long
        pub unsafe fn $name(a: &[u64], b: &[u64], out: *mut u64) {
            #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))]
            { return vector::$name(a, b, out); }

            #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))]
            { return scalar::$name(a, b, out); }
        }
    };
}

bitset_op!(or);
bitset_op!(and);
bitset_op!(and_not);
bitset_op!(xor);

/// Compute the cardinality of the bitset
/// 
/// # Safety
/// Assumes that `bitset` is `BITSET_SIZE_IN_WORDS` in length
pub unsafe fn cardinality(bitset: &[u64]) -> usize {
    #[cfg(any(target_feature = "avx2", target_feature = "sse4.2"))]
    { return vector::cardinality(bitset); }

    #[cfg(not(any(target_feature = "avx2", target_feature = "sse4.2")))]
    { return scalar::cardinality(bitset); }
}