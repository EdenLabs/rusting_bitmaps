use crate::utils::max;
use crate::container::run::Rle16;

/// Check if the container is full
/// 
/// # Safety
/// Requires that there be at least one element in the slice
pub unsafe fn is_full(runs: &[Rle16]) -> bool {
    runs.len() == 1 && runs.get_unchecked(0).value == 0 && runs.get_unchecked(0).length == std::u16::MAX
}

/// Calculate the union (`A ∪ B`) of two rle slices and append the result in `out`
pub fn union(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    // Append if one of the slices has no elements
    if a.len() == 0 {
        out.extend_from_slice(b);
        return;
    }
    
    if b.len() == 0 {
        out.extend_from_slice(a);
        return;
    }
    
    unsafe {
        // Append directly if one of the slices is full
        if is_full(a) {
            out.extend_from_slice(a);
            return;
        }
        
        if is_full(b) {
            out.extend_from_slice(b);
            return;
        }
        
        // Perform the union
        let max_capacity = a.len() + b.len();
        if out.capacity() < max_capacity {
            out.reserve(max_capacity - out.capacity());
        }
        
        let mut i_a = 0;
        let mut i_b = 0;
        let mut prev;

        let mut run_a = *a.get_unchecked(i_a);
        let mut run_b = *b.get_unchecked(i_b);
        
        if run_a.value <= run_b.value {
            out.push(run_a);
            
            prev = run_a;
            i_a += 1;
        }
        else {
            out.push(run_b);
            
            prev = run_b;
            i_b += 1;
        }
        
        while i_a < a.len() && i_b < b.len() {
            run_a = *a.get_unchecked(i_a);
            run_b = *b.get_unchecked(i_b);

            let new_run = {
                if run_a.value <= run_b.value {
                    i_a += 1;
                    run_a
                }
                else {
                    i_b += 1;
                    run_b
                }
            };

            append(out, &new_run, &mut prev);
        }

        while i_a < a.len() {
            append(out, a.get_unchecked(i_a), &mut prev);
            i_a += 1;
        }

        while i_b < b.len() {
            append(out, b.get_unchecked(i_b), &mut prev);
            i_b += 1;
        }
    }
}

/// Calculate the intersection (`A ∩ B`) of two r;e slices and append the result in `out`
pub fn intersect(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    // Append if one of the slices has no elements
    if a.len() == 0 {
        out.extend_from_slice(b);
        return;
    }
    
    if b.len() == 0 {
        out.extend_from_slice(a);
        return;
    }

    unsafe {
        // Append directly if one of the slices is full
        if is_full(a) {
            out.extend_from_slice(a);
            return;
        }
        
        if is_full(b) {
            out.extend_from_slice(b);
            return;
        }

        let max_capacity = a.len() + b.len();
        if out.capacity() < max_capacity {
            out.reserve(max_capacity - out.capacity());
        }

        let mut i_a = 0;
        let mut i_b = 0;
        let mut start_a = a.get_unchecked(i_a).value;
        let mut start_b = b.get_unchecked(i_b).value;
        let mut end_a = start_a + a.get_unchecked(i_a).length + 1;
        let mut end_b = start_b + b.get_unchecked(i_b).length + 1;

        while i_a < a.len() && i_b < b.len() {
            if end_a <= start_b {
                i_a += 1;

                if i_a < a.len() {
                    start_a = a.get_unchecked(i_a).value;
                    end_a = start_a + a.get_unchecked(i_a).length + 1;
                }
            }
            else if end_b < start_a {
                i_b += 1;
                
                if i_b < b.len() {
                    start_b = b.get_unchecked(i_b).value;
                    end_b = start_b + b.get_unchecked(i_b).length + 1;
                }
            }
            else {
                let latest_start = max(start_a, start_b);
                
                let earliest_end;
                if end_a == end_b {
                    earliest_end = end_a;
                    i_a += 1;
                    i_b += 1;

                    if i_a < a.len() {
                        start_a = a.get_unchecked(i_a).value;
                        end_a = start_a + a.get_unchecked(i_a).length + 1;
                    }

                    if i_b < b.len() {
                        start_b = b.get_unchecked(i_b).value;
                        end_b = start_b + b.get_unchecked(i_b).length + 1;
                    }
                }
                else if end_a < end_b {
                    earliest_end = end_a;
                    i_a += 1;

                    if i_a < a.len() {
                        start_a = a.get_unchecked(i_a).value;
                        end_a = start_a + a.get_unchecked(i_a).length + 1;
                    }
                }
                else {
                    earliest_end = end_b;
                    i_b += 1;

                    if i_b < b.len() {
                        start_b = b.get_unchecked(i_b).value;
                        end_b = start_b + b.get_unchecked(i_b).length + 1;
                    }
                }

                let run = Rle16::new(latest_start, earliest_end - latest_start - 1);
                out.push(run);
            }
        }
    }
}

/// Calculate the difference (`A \ B`) between two rle slices and append the result in `out`
pub fn difference(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    if a.len() == 0 {
        return;
    }

    if b.len() == 0 {
        out.extend_from_slice(a);
        return;
    }

    let max_capacity = a.len() + b.len();
    if out.capacity() < max_capacity {
        out.reserve(max_capacity - out.capacity());
    }

    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        let mut start_a = a.get_unchecked(i_a).value;
        let mut start_b = b.get_unchecked(i_b).value;
        let mut end_a = start_a + a.get_unchecked(i_a).length + 1;
        let mut end_b = start_b + b.get_unchecked(i_b).length + 1;

        while i_a < a.len() && i_b < b.len() {
            if end_a < start_b {
                let run = Rle16::new(start_a, end_a - start_a - 1);
                out.push(run);
                i_a += 1;

                if i_a < a.len() {
                    start_a = a.get_unchecked(i_a).value;
                    end_a = start_a + a.get_unchecked(i_a).length + 1;
                }
            }
            else if end_b < start_a {
                i_b += 1;
                if i_b < b.len() {
                    start_b = b.get_unchecked(i_b).value;
                    end_b = start_b + b.get_unchecked(i_b).length + 1;
                }
            }
            else {
                if start_a < start_b {
                    let run = Rle16::new(start_a, start_b - start_a - 1);
                    out.push(run);
                }
                
                if end_b < end_a {
                    start_a = end_b;
                }
                else {
                    i_a += 1;
                    if i_a < a.len() {
                        start_a = a.get_unchecked(i_a).value;
                        end_a = start_a + a.get_unchecked(i_a).length + 1;
                    }
                }
            }
        }

        if i_a < a.len() {
            let run = Rle16::new(start_a, end_a - start_a - 1);
            out.push(run);

            i_a += 1;
            if i_a < a.len() {
                out.extend_from_slice(&a[i_a..a.len()]);
            }
        }
    }
}

/// Calculate the symmetric difference (`(A \ B) ∪ (B \ A)`) between two rle slices and append the result in `out`
pub fn symmetric_difference(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    if a.len() == 0 {
        out.extend_from_slice(b);
        return;
    }

    if b.len() == 0 {
        out.extend_from_slice(b);
        return;
    }

    let max_capacity = a.len() + b.len();
    if out.capacity() < max_capacity {
        out.reserve(max_capacity - out.capacity());
    }

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
}

/// Appends a run to `runs` or merges it with `previous_run`
/// 
/// # Notes
/// Expects `runs` to have at least 1 element and `previous_run` to point to that last element. 
pub unsafe fn append(runs: &mut Vec<Rle16>, run: &Rle16, previous_run: &mut Rle16) {
    let prev_end = previous_run.value + previous_run.length;

    // Add a new run
    if run.value > prev_end + 1 {
        runs.push(*run);

        *previous_run = *run;
    }
    // Merge runs
    else {
        let new_end = run.value + run.length + 1;
        if new_end > prev_end {
            previous_run.length = new_end - 1 - previous_run.value;

            let len = runs.len();
            *runs.get_unchecked_mut(len - 1) = *previous_run;
        }
    }
}

/// # Safety
/// Assumes that `runs` has at least one element
pub fn append_exclusive(runs: &mut Vec<Rle16>, start: u16, length: u16) {
    if runs.len() == 0 {
        runs.push(Rle16::new(start, length));
        return;
    }

    let len = runs.len();
    let last_run = &mut runs[len - 1];
    let old_end = last_run.value + last_run.length + 1;

    if start > old_end {
        runs.push(Rle16::new(start, length));
        return;
    }

    if old_end == start {
        last_run.length += length + 1;
        return;
    }

    let new_end = start + length + 1;
    if start == last_run.value {
        if new_end < old_end {
            *last_run = Rle16::new(new_end, old_end - new_end - 1);
            return;
        }
        else if new_end > old_end {
            *last_run = Rle16::new(old_end, new_end - old_end - 1);
            return;
        }
        else {
            runs.pop();
            return;
        }
    }

    last_run.length = start - last_run.value - 1;
    if new_end < old_end {
        let run = Rle16::new(new_end, old_end - new_end - 1);
        runs.push(run);
    }
    else if new_end > old_end {
        let run = Rle16::new(old_end, new_end - old_end - 1);
        runs.push(run);
    }
}

pub fn append_value(runs: &mut Vec<Rle16>, value: u16, prev_rle: &mut Rle16) {
    let prev_end = prev_rle.sum();
    if value > prev_end + 1 {
        let rle = Rle16::new(value, 0);
        runs.push(rle);

        *prev_rle = rle;
        return;
    }
    
    if value == prev_end * 1 {
        prev_rle.length += 1;

        let len = runs.len();
        runs[len - 1] = *prev_rle;
        return;
    }
}