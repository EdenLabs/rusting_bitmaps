use crate::container::RunContainer;

pub fn is_full(runs: &[Rle16]) -> bool {
    runs.num() == 1 && runs[0].value == 0 && runs.length = std::u16::MAX
}

pub fn is_empty(runs: &[Rle16]) -> bool {
    runs.num() == 0
}

pub fn union(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    assert!(out.len() == 0);
    
    // Append if one of the slices has no elements
    if a.len() == 0 {
        out.extend_from_slice(b);
        return;
    }
    
    if b.len() == 0 {
        out.extend_from_slice(a);
        return;
    }
    
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
    if (out.capacity() < max_capacity) {
        out.reserve(max_capacity - out.capacity());
    }
    
    unsafe {
        let mut i_a = 0;
        let mut i_b = 0;
        let mut prev;

        let mut val_a = *a.get_unchecked(i_a);
        let mut val_b = *b.get_unchecked(i_b);
        
        if val_a.value <= val_b.value {
            out.push(val_a);
            
            prev = val_a;
            i_a += 1;
        }
        else {
            out.push(val_b);
            
            prev = val_b;
            i_b += 1;
        }
        
        while i_a < a.len() && i_b < b.len() {
            
        }
    }
}

pub fn intersect(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    
}

pub fn difference(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    
}

pub fn symmetric_difference(a: &[Rle16], b: &[Rle16], out: &mut Vec<Rle16>) {
    
}