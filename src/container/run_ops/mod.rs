use crate::container::run::Rle16;

/// Calculate the symmetric difference (`(A \ B) ∪ (B \ A)`) between two rle slices and append the result in `out`
pub fn xor(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    /*
    if a.is_empty() {
        out.extend_from_slice(b);
        return;
    }

    if b.is_empty() {
        out.extend_from_slice(b);
        return;
    }

    out.reserve(a.len() + b.len());

    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        let mut v_a;
        let mut v_b;

        while i_a < a.len() && i_b < b.len() {
            v_a = *a.get_unchecked(i_a);
            v_b = *b.get_unchecked(i_b);

            if v_a.value <= v_b.value {
                append_exclusive(out, v_a.value, v_a.length);
                i_a += 1;
            }
            else {
                append_exclusive(out, v_b.value, v_b.length);
                i_b += 1;
            }
        }

        while i_a < a.len() {
            v_a = *a.get_unchecked(i_a);

            append_exclusive(out, v_a.value, v_a.length);
            i_a += 1;
        }

        while i_b < b.len() {
            v_b = *b.get_unchecked(i_b);

            append_exclusive(out, v_b.value, v_b.length);
            i_b += 1;
        }
    }
    */
}
