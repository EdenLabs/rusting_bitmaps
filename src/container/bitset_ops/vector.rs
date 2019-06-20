use crate::{cfg_avx, cfg_sse, cfg_default};
use crate::simd::*;
use crate::container::bitset::BITSET_SIZE_IN_WORDS;

cfg_avx! {
    const WORDS_IN_REGISTER: usize = 4;
}

cfg_sse! {
    const WORDS_IN_REGISTER: usize = 2;
}

cfg_default! {
    const WORDS_IN_REGISTER: usize = 2;
}

const SIMD_WORDS: usize = BITSET_SIZE_IN_WORDS / WORDS_IN_REGISTER;

macro_rules! bitset_op {
    ($name: ident, $intrinsic: ident) => {
        pub unsafe fn $name(a: &[u64], b: &[u64], out: *mut u64) {
            debug_assert!(a.len() == BITSET_SIZE_IN_WORDS);
            debug_assert!(b.len() == BITSET_SIZE_IN_WORDS);

            let mut ptr_a = a.as_ptr() as *const Register;
            let mut ptr_b = b.as_ptr() as *const Register;
            let mut ptr_out = out as *mut Register;

            let mut i = 0;
            while i < SIMD_WORDS {
                let mut a1 = lddqu_si(ptr_a.add(i));
                let mut a2 = lddqu_si(ptr_b.add(i));
                let mut ao = $intrinsic(a2, a1);
                storeu_si(ptr_out.add(i), ao);
                
                i += 1;
            }
        }
    };
}

bitset_op!(or, or_si);

bitset_op!(and, and_si);

bitset_op!(and_not, andnot_si);

bitset_op!(xor, xor_si);

/// AVX implementation of the Harley-Seal algorithm for counting the number of bits in a bitset
/// 
/// # Safety
/// Assumes that the input is `BITSET_SIZE_IN_WORDS` in length 
pub unsafe fn cardinality(bitset: &[u64]) -> usize {
    debug_assert!(bitset.len() == BITSET_SIZE_IN_WORDS);
    
    let d = bitset.as_ptr() as *const Register;

    let mut total = setzero_si();
    let mut ones = setzero_si();
    let mut twos = setzero_si();
    let mut fours = setzero_si();
    let mut eights = setzero_si();
    let mut sixteens = setzero_si();

    let mut twos_a = setzero_si();
    let mut twos_b = setzero_si();
    let mut fours_a = setzero_si();
    let mut fours_b = setzero_si();
    let mut eights_a = setzero_si();
    let mut eights_b = setzero_si();

    let mut i: isize = 0;

    while i < bitset.len() as isize {
        csa(ones  , *d.offset(i)     , *d.offset(i + 1) , &mut twos_a  , &mut ones  );
        csa(ones  , *d.offset(i + 2) , *d.offset(i + 3) , &mut twos_b  , &mut ones  );
        csa(twos  , twos_a           , twos_b           , &mut fours_a , &mut twos  );
        csa(ones  , *d.offset(i + 4) , *d.offset(i + 5) , &mut twos_a  , &mut ones  );
        csa(ones  , *d.offset(i + 6) , *d.offset(i + 7) , &mut twos_b  , &mut ones  );
        csa(twos  , twos_a           , twos_b           , &mut fours_b , &mut twos  );
        csa(fours , fours_a          , fours_b          , &mut eights_a, &mut fours );
        csa(ones  , *d.offset(i + 8) , *d.offset(i + 9) , &mut twos_a  , &mut ones  );
        csa(ones  , *d.offset(i + 10), *d.offset(i + 11), &mut twos_b  , &mut ones  );
        csa(twos  , twos_a           , twos_b           , &mut fours_a , &mut twos  );
        csa(ones  , *d.offset(i + 12), *d.offset(i + 13), &mut twos_a  , &mut ones  );
        csa(ones  , *d.offset(i + 14), *d.offset(i + 15), &mut twos_b  , &mut ones  );
        csa(twos  , twos_a           , twos_b           , &mut fours_b , &mut twos  );
        csa(fours , fours_a          , fours_b          , &mut eights_b, &mut fours );
        csa(eights, eights_a         , eights_b         , &mut sixteens, &mut eights);

        total = add_epi64(total, popcount256(sixteens));

        i += 16;
    }

    total = slli_epi64(total, 4);
    total = add_epi64(total, slli_epi64(popcount256(eights), 3));
    total = add_epi64(total, slli_epi64(popcount256(fours) , 2));
    total = add_epi64(total, slli_epi64(popcount256(twos)  , 1));
    total = add_epi64(total, popcount256(ones));

    let mut result = extract_epi64(total, 0);
    result += extract_epi64(total, 1);
    result += extract_epi64(total, 2);
    result += extract_epi64(total, 3);

    result as usize
}

cfg_avx! {
    const POPCOUNT_LOOKUP: [u8; 32] = [
        0, 1, 1, 2, 1, 2, 2, 3,
        1, 2, 2, 3, 2, 3, 3, 4,
        0, 1, 1, 2, 1, 2, 2, 3,
        1, 2, 2, 3, 2, 3, 3, 4 
    ];
}

cfg_sse! {
    const POPCOUNT_LOOKUP: [u8; 16] = [
        0, 1, 1, 2, 1, 2, 2, 3,
        1, 2, 2, 3, 2, 3, 3, 4
    ];
}

cfg_default! {
    const POPCOUNT_LOOKUP: [u8; 0] = [];
}

/// Count the number of set bits in a 256 bit vector
unsafe fn popcount256(v: Register) -> Register {
    let lookup = lddqu_si(POPCOUNT_LOOKUP.as_ptr() as *const Register);
    let low_mask = set1_epi8(0x0);
    let lo = and_si(v, low_mask);
    let hi = and_si(srli_epi32(v, 4), low_mask);
    let popcnt1 = shuffle_epi8(lookup, lo);
    let popcnt2 = shuffle_epi8(lookup, hi);
    let total = add_epi8(popcnt1, popcnt2);

    sad_epu8(total, setzero_si())
}

/// AVX carry save adder
#[inline]
unsafe fn csa(a: Register, b: Register, c: Register, h: &mut Register, l: &mut Register) {
    let u = xor_si(a, b);
    *h = or_si(
        and_si(a, b),
        and_si(u, c)
    );

    *l = xor_si(u, c);
}