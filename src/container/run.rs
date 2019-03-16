use std::slice::{Iter, IterMut};
use std::iter;

use crate::utils;
use crate::container::*;
use crate::container::run_ops;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Rle16 {
    pub value: u16,
    pub length: u16
}

impl Rle16 {
    pub fn new(value: u16, length: u16) -> Self {
        Self {
            value,
            length
        }
    }

    pub fn sum(&self) -> u16 {
        self.value + self.length
    }
}

enum SearchResult {
    ExactMatch(usize),
    PossibleMatch(usize),
    NoMatch
}

#[derive(Clone)]
pub struct RunContainer {
    runs: Vec<Rle16>
}

impl RunContainer {
    pub fn new() -> Self {
        Self {
            runs: Vec::new()
        }
    }
    
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            runs: Vec::with_capacity(capacity)
        }
    }
    
    pub fn shrink_to_fit(&mut self) {
        self.runs.shrink_to_fit()
    }
    
    pub fn reserve(&mut self, additional: usize) {
        self.runs.reserve(additional);
    }
    
    pub fn add(&mut self, value: u16) {
        match self.binary_search(value) {
            SearchResult::ExactMatch(_index) => {
                return;
            },
            SearchResult::PossibleMatch(index) => {
                let v = self.runs[index];
                let offset = value - v.value;
                
                if offset <= v.length {
                    return;
                }

                if offset + 1 == v.length {
                    if index + 1 < self.runs.len() {
                        // Check if necessary to fuse, if so fuse the runs
                        let v1 = self.runs[index + 1];
                        if v1.value == value + 1 {
                            self.runs[index].length = v1.value + v1.length - v.value;
                            self.runs.remove(index + 1);
                            return;
                        }
                    }

                    self.runs[index].length += 1;
                    return;
                }

                if index + 1 < self.runs.len() {
                    // Check if necessary to fuse. If so fuse the runs
                    let v1 = &mut self.runs[index + 1];
                    if v1.value == value + 1 {
                        v1.value = value;
                        v1.length += 1;
                        return;
                    }
                }

                let new_run = Rle16::new(value, 0);
                self.runs.insert(index + 1, new_run);
            },
            SearchResult::NoMatch => {
                // Check if the run needs extended, if so extend it
                if self.runs.len() > 0 {
                    let v0 = &mut self.runs[0];
                    if v0.value == value + 1 {
                        v0.length += 1;
                        v0.value -= 1;
                        return;
                    }
                }

                let new_run = Rle16::new(value, 0);
                self.runs.insert(0, new_run);
            }
        }
    }
    
    pub fn add_range(&mut self, min: u16, max: u16) {
        let runs_min = self.rle_count_less(min);
        let runs_max = self.rle_count_greater(max);

        let common = self.runs.len() - runs_min - runs_max;
        if common == 0 {
            self.runs.insert(
                runs_min,
                Rle16::new(min, max - min)
            );
        }
        else {
            let common_min = self.runs[runs_min].value;
            let common_max = self.runs[runs_min + common - 1].sum();
            let result_min = crate::utils::min(common_min, min);
            let result_max = crate::utils::max(common_max, max);

            self.runs[runs_min] = Rle16::new(result_min, result_max - result_min);
            self.runs.splice((runs_min + 1)..runs_max, iter::empty());
        }
    }
    
    pub fn remove(&mut self, value: u16) -> bool {
        match self.binary_search(value) {
            SearchResult::ExactMatch(index) => {
                let rle = &mut self.runs[index];
                if rle.length == 0 {
                    self.runs.remove(index);
                }
                else {
                    rle.value += 1;
                    rle.length -= 1;
                }

                return true;
            },
            SearchResult::PossibleMatch(prev_index) => {
                let rle = self.runs[prev_index];
                let offset = value - rle.value;
                
                if offset < rle.length {
                    self.runs[prev_index].length = offset - 1;

                    let new_rle = Rle16::new(value + 1, rle.length - offset - 1);
                    self.runs.insert(prev_index + 1, new_rle);

                    return true;
                }
                
                if offset == rle.length {
                    self.runs[prev_index].length -= 1;
                    return true;
                }

                return false;
            },
            SearchResult::NoMatch => {
                return false;
            }
        }
    }
    
    pub fn remove_range(&mut self, min: u16, max: u16) {
        fn result_to_compressed_index(value: SearchResult) -> isize {
            match value {
                SearchResult::ExactMatch(index) => {
                    index as isize
                },
                SearchResult::PossibleMatch(index) => {
                    -(index as isize + 1)
                },
                SearchResult::NoMatch => {
                    -1
                }
            }
        }

        let mut first = result_to_compressed_index(self.find_run(min));
        let mut last = result_to_compressed_index(self.find_run(max));

        if first >= 0 {
            let v_first = self.runs[first as usize];
            if min > v_first.value && max < v_first.sum() {
                // Split into two runs

                // Right interval
                self.runs.insert(first as usize + 1, Rle16::new(max + 1, v_first.sum() - (max + 1)));

                // Left interval
                self.runs[first as usize].length = (min - 1) - v_first.value;
                return;
            }

            if min > v_first.value {
                self.runs[first as usize].length = (min - 1) - v_first.value;
                first += 1;
            }
        }
        else {
            first = -first - 1;
        }

        if last >= 0 {
            let run_max = self.runs[last as usize].sum();
            if run_max > max {
                self.runs[last as usize] = Rle16::new(max + 1, run_max - (max + 1));
                last -= 1;
            }
        }
        else {
            last = (-last - 1) - 1;
        }

        if first <= last {
            self.runs.splice((self.runs.len() - (last as usize + 1))..(-(last - first + 1) as usize), iter::empty());
        }
    }
    
    pub fn contains(&self, value: u16) -> bool {
        match self.binary_search(value) {
            SearchResult::ExactMatch(_index) => {
                true
            },
            SearchResult::PossibleMatch(index) => {
                let v = self.runs[index];

                if value - v.value <= v.length {
                    true
                }
                else {
                    false
                }
            },
            SearchResult::NoMatch => {
                false
            }
        }
    }
    
    pub fn contains_range(&self, min: u16, max: u16) -> bool {
        let mut count = 0;
        let index;

        match self.binary_search(min) {
            SearchResult::ExactMatch(i) => {
                index = i;
            },
            SearchResult::PossibleMatch(i) => {
                let v = self.runs[i];

                if min - v.value > v.length {
                    return false;
                }
                else {
                    index = i;
                }
            },
            SearchResult::NoMatch => {
                return false;
            }
        };

        for run in &self.runs[index..self.runs.len()] {
            let stop = run.value + run.length;
            if run.value >= max {
                break;
            }

            if stop >= max {
                if max > run.value {
                    count += max - run.value;
                }

                break;
            }

            let min_length;
            if stop > min {
                min_length = stop - min;
            }
            else {
                min_length = 0;
            }

            if min_length < run.length {
                count += min_length;
            }
            else {
                count += run.length;
            }
        }

        return count >= max - min - 1;
    }
    
    pub fn cardinality(&self) -> usize {
        unimplemented!()
    }
    
    pub fn is_empty(&self) -> bool {
        self.runs.len() == 0
    }
    
    pub fn is_full(&self) -> bool {
        if self.runs.len() == 0 {
            return false;
        }

        unsafe {
            run_ops::is_full(&self.runs)
        }
    }
    
    pub fn clear(&mut self) {
        self.runs.clear()
    }

    pub fn copy_from(&mut self, other: &RunContainer) {
        self.runs.clear();
        self.runs.copy_from_slice(&other.runs);
    }
    
    pub fn iter(&self) -> Iter<Rle16> {
        self.runs.iter()
    }
    
    pub fn iter_mut(&mut self) -> IterMut<Rle16> {
        self.runs.iter_mut()
    }
    
    pub fn min(&self) -> u16 {
        if self.runs.len() == 0 {
            return 0;
        }

        self.runs[0].value
    }
    
    pub fn max(&self) -> u16 {
        if self.runs.len() == 0 {
            return 0;
        }

        let run = self.runs[self.runs.len() - 1];
        
        run.value + run.length
    }
    
    pub fn rank(&self, value: u32) -> usize {
        let mut sum = 0;
        for run in self.runs.iter() {
            let start = run.value as u32;
            let length = run.length as u32;
            let end = start + length;

            if value < end {
                if value < start {
                    break;
                }
                
                return (sum + (value - start) + 1) as usize;
            }
            else {
                sum += length + 1;
            }
        }

        return sum as usize;
    }

    pub fn select(&self, rank: u32, start_rank: &mut u32, element: &mut u32) -> bool {
        for run in self.runs.iter() {
            let length = run.length as u32;
            let value = run.value as u32;

            if rank <= *start_rank + length {
                *element = value + rank - *start_rank;
                return true;
            }
            else {
                *start_rank += length + 1;
            }
        }

        return false;
    }

    fn binary_search(&self, key: u16) -> SearchResult {
        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low < high {
            let middle = (low + high) >> 1;
            let value = self.runs[middle].value;

            if value < key {
                low = middle + 1;
            }
            else if value > key {
                high = middle - 1;
            }
            else {
                return SearchResult::ExactMatch(middle);
            }
        }

        if low == 0 {
            SearchResult::NoMatch
        }
        else {
            SearchResult::PossibleMatch(low - 1)
        }
    }

    /// Find the run containing `key`
    fn find_run(&self, key: u16) -> SearchResult {
        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low < high {
            let middle = (low + high) >> 1;
            let min = self.runs[middle].value;
            let max = self.runs[middle].sum();
            if key > max {
                low = middle + 1;
            }
            else if key < min {
                high = middle - 1;
            }
            else {
                return SearchResult::ExactMatch(middle);
            }
        }

        if low == 0 {
            SearchResult::NoMatch
        }
        else {
            SearchResult::PossibleMatch(low - 1)
        }
    }

    fn rle_count_less(&self, value: u16) -> usize {
        if self.runs.len() == 0 {
            return 0;
        }

        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low <= high {
            let middle = (low + high) >> 1;
            let min_value = self.runs[middle].value;
            let max_value = min_value + self.runs[middle].length;

            if max_value + 1 < value {
                low = middle + 1;
            }
            else if value < min_value {
                high = middle - 1;
            }
            else {
                return middle;
            }
        }

        return low;
    }

    fn rle_count_greater(&self, value: u16) -> usize {
        if self.runs.len() == 0 {
            return 0;
        }

        let mut low = 0;
        let mut high = self.runs.len() - 1;
        while low <= high {
            let middle = (low + high) >> 1;
            let min_value = self.runs[middle].value;
            let max_value = min_value + self.runs[middle].length;

            if max_value + 1 < value {
                low = middle + 1;
            }
            else if value < min_value {
                high = middle - 1;
            }
            else {
                return middle;
            }
        }

        return self.runs.len() - low;
    }
}

impl From<ArrayContainer> for RunContainer {
    fn from(container: ArrayContainer) -> Self {
        let num_runs = container.num_runs();
        let mut run_container = RunContainer::with_capacity(num_runs);

        let mut prev: isize = -2;
        let mut run_start: isize = -1;
        let cardinality = container.cardinality();

        if cardinality == 0 {
            return run_container;
        }
        
        for value in container.iter() {
            if *value != (prev + 1) as u16 {
                if run_start != -1 {
                    run_container.runs.push(
                        Rle16::new(run_start as u16, prev as u16)
                    );
                }

                run_start = *value as isize;
            }

            prev = *value as isize;
        }

        run_container.runs.push(
            Rle16::new(run_start as u16, prev as u16)
        );

        run_container
    }
}

impl From<BitsetContainer> for RunContainer {
    fn from(container: BitsetContainer) -> Self {
        unimplemented!()
    }
}

impl PartialEq for RunContainer {
    fn eq(&self, other: &RunContainer) -> bool {
        utils::mem_equals(&self.runs, &other.runs)
    }
}

impl Container for RunContainer { }

impl Union<Self> for RunContainer {
    type Output = Self;

    fn union_with(&self, other: &Self, out: &mut Self::Output) {
        run_ops::union(&self.runs, &other.runs, &mut out.runs);
    }
}

impl Union<ArrayContainer> for RunContainer {
    type Output = Self;

    fn union_with(&self, other: &ArrayContainer, out: &mut Self::Output) {
        if self.is_full() {
            out.copy_from(self);
        }

        out.reserve(2 * other.cardinality() + self.runs.len());

        unimplemented!()
    }
}

impl Union<BitsetContainer> for RunContainer {
    type Output = BitsetContainer;

    fn union_with(&self, other: &BitsetContainer, out: &mut Self::Output) {
        out.copy_from(other);

        for rle in self.iter() {
            out.set_range(rle.value as usize, rle.sum() as usize);
        }
    }
}

impl Intersection<Self> for RunContainer {
    fn intersect_with(&self, other: &Self, out: &mut Self) {
        run_ops::intersect(&self.runs, &other.runs, &mut out.runs);
    }
}

impl Intersection<ArrayContainer> for RunContainer {
    fn intersect_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl Intersection<BitsetContainer> for RunContainer {
    fn intersect_with(&self, other: &BitsetContainer, out: &mut BitsetContainer) {
        unimplemented!()
    }
}

impl Difference<Self> for RunContainer {
    fn difference_with(&self, other: &Self, out: &mut Self) {
        run_ops::difference(&self.runs, &other.runs, &mut out.runs);
    }
}

impl Difference<ArrayContainer> for RunContainer {
    fn difference_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl Difference<BitsetContainer> for RunContainer {
    fn difference_with(&self, other: &BitsetContainer, out: &mut BitsetContainer) {
        unimplemented!()
    }
}

impl SymmetricDifference<Self> for RunContainer {
    fn symmetric_difference_with(&self, other: &Self, out: &mut Self) {
        run_ops::symmetric_difference(&self.runs, &other.runs, &mut out.runs);
    }
}

impl SymmetricDifference<ArrayContainer> for RunContainer {
    fn symmetric_difference_with(&self, other: &ArrayContainer, out: &mut ArrayContainer) {
        unimplemented!()
    }
}

impl SymmetricDifference<BitsetContainer> for RunContainer {
    fn symmetric_difference_with(&self, other: &BitsetContainer, out: &mut BitsetContainer) {
        unimplemented!()
    }
}

impl Subset<Self> for RunContainer {
    fn subset_of(&self, other: &Self) -> bool {
        unsafe {
            let mut i_0 = 0;
            let mut i_1 = 0;

            while i_0 < self.runs.len() && i_1 < other.runs.len() {
                let start_0 = self.runs.get_unchecked(i_0).value;
                let start_1 = other.runs.get_unchecked(i_1).value;
                let stop_0 = start_0 + self.runs.get_unchecked(i_0).length;
                let stop_1 = start_1 + other.runs.get_unchecked(i_1).length;

                if start_0 < start_1 {
                    return false;
                }
                else {
                    if stop_0 < stop_1 {
                        i_0 += 1;
                    }
                    else if stop_0 == stop_1 {
                        i_0 += 1;
                        i_1 += 1;
                    }
                    else {
                        i_1 += 1;
                    }
                }
            }

            if i_0 == self.runs.len() {
                return true;
            }
            else {
                return false;
            }
        }
    }
}

impl Subset<ArrayContainer> for RunContainer {
    fn subset_of(&self, other: &ArrayContainer) -> bool {
        unimplemented!()
    }
}

impl Subset<BitsetContainer> for RunContainer {
    fn subset_of(&self, other: &BitsetContainer) -> bool {
        unimplemented!()
    }
}