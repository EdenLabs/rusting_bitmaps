use std::fmt;
use std::ops::{Deref, DerefMut, RangeBounds};

/// A `Vec<u16>` with spare room before its first element
///
/// Removing from the front moves the start of the slice instead of shifting every element
/// down, and an insert near the front takes that room back. The sorted array containers of
/// an id allocator are popped from the front and pushed back near it, so both are constant
/// time there, and any other index shifts the shorter side.
///
/// The slack is the elements `buf[..head]`, kept in the `Vec`'s length so that it is never
/// reallocated or reinterpreted. It adds no capacity: a `push` into a full buffer reclaims it
/// before the `Vec` grows.
///
/// Dereferences to the live elements.
pub struct SlackVec {
    buf: Vec<u16>,

    /// Index into `buf` of the first live element. Zero whenever there are no live elements
    head: usize
}

impl SlackVec {
    #[inline]
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            head: 0
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            head: 0
        }
    }

    /// The number of elements the buffer can hold without reallocating, counting only the
    /// room after the slack
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.capacity() - self.head
    }

    /// Reserve room for `additional` more elements after the last
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.buf.reserve(additional);
    }

    pub fn shrink_to_fit(&mut self) {
        self.close_slack();
        self.buf.shrink_to_fit();
    }

    /// Set the number of live elements
    ///
    /// # Safety
    /// `len` must not exceed `capacity()` and the first `len` elements must be initialized
    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        self.buf.set_len(self.head + len);
    }

    #[inline]
    pub fn as_slice(&self) -> &[u16] {
        &self.buf[self.head..]
    }

    #[inline]
    pub fn clear(&mut self) {
        self.buf.clear();
        self.head = 0;
    }

    pub fn truncate(&mut self, len: usize) {
        if len == 0 {
            self.clear();
        }
        else {
            self.buf.truncate(self.head + len);
        }
    }

    #[inline]
    pub fn push(&mut self, value: u16) {
        self.reclaim_if_full();
        self.buf.push(value);
    }

    /// Insert `value` at `index`, shifting the shorter side
    pub fn insert(&mut self, index: usize, value: u16) {
        let len = self.len();
        assert!(index <= len);

        if self.head > 0 && index < len - index {
            self.head -= 1;

            // The elements before `index` move down into the slack
            self.buf.copy_within(self.head + 1..self.head + 1 + index, self.head);
            self.buf[self.head + index] = value;
        }
        else {
            self.reclaim_if_full();
            self.buf.insert(self.head + index, value);
        }
    }

    /// Remove and return the element at `index`, shifting the shorter side
    pub fn remove(&mut self, index: usize) -> u16 {
        let len = self.len();
        assert!(index < len);

        let value = self.buf[self.head + index];

        if len == 1 {
            self.clear();
        }
        else if index < len - index {
            // The elements before `index` move up over it and the start follows
            self.buf.copy_within(self.head..self.head + index, self.head + 1);
            self.head += 1;
        }
        else {
            self.buf.remove(self.head + index);
        }

        value
    }

    pub fn extend<I: IntoIterator<Item=u16>>(&mut self, values: I) {
        self.buf.extend(values);
    }

    pub fn extend_from_slice(&mut self, values: &[u16]) {
        self.buf.extend_from_slice(values);
    }

    /// Replace the live elements in `range` with `values`
    pub fn splice<R, I>(&mut self, range: R, values: I)
        where R: RangeBounds<usize>, I: IntoIterator<Item=u16>
    {
        use std::ops::Bound::*;

        let start = match range.start_bound() {
            Included(&i) => i,
            Excluded(&i) => i + 1,
            Unbounded => 0
        };
        let end = match range.end_bound() {
            Included(&i) => i + 1,
            Excluded(&i) => i,
            Unbounded => self.len()
        };

        self.buf.splice(self.head + start..self.head + end, values);
    }

    /// Move the live elements to the start of the buffer
    fn close_slack(&mut self) {
        if self.head > 0 {
            let len = self.len();
            self.buf.copy_within(self.head.., 0);
            self.buf.truncate(len);
            self.head = 0;
        }
    }

    /// Take back the slack rather than grow when the buffer is full
    ///
    /// Costs a shift of every element, which is what growing would have cost an insert at
    /// the front, and keeps the capacity, and so the memory, from creeping up.
    #[inline]
    fn reclaim_if_full(&mut self) {
        if self.head > 0 && self.buf.len() == self.buf.capacity() {
            self.close_slack();
        }
    }
}

impl Deref for SlackVec {
    type Target = [u16];

    #[inline]
    fn deref(&self) -> &[u16] {
        &self.buf[self.head..]
    }
}

impl DerefMut for SlackVec {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u16] {
        &mut self.buf[self.head..]
    }
}

impl Clone for SlackVec {
    /// Clones the live elements with no slack
    fn clone(&self) -> Self {
        Self {
            buf: self.as_slice().to_vec(),
            head: 0
        }
    }
}

impl PartialEq for SlackVec {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for SlackVec {}

impl fmt::Debug for SlackVec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list().entries(self.as_slice()).finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Run random inserts and removes against a `Vec` and compare after every step
    #[test]
    fn matches_vec() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move |n: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 11) % n as u64) as usize
        };

        for &capacity in &[0usize, 4, 33, 300] {
            let mut slack = SlackVec::with_capacity(capacity);
            let mut model: Vec<u16> = Vec::new();

            for step in 0..20_000 {
                // Bias toward the ends, where the slack is used
                let index = |len: usize, r: usize| match r % 4 {
                    0 => 0,
                    1 => len,
                    _ => if len == 0 { 0 } else { r % (len + 1) }
                };

                let grow = (step / 700) % 2 == 0;
                let roll = next(10);
                if roll < if grow { 6 } else { 3 } {
                    let at = index(model.len(), next(1 << 20)).min(model.len());
                    let value = next(65_536) as u16;

                    slack.insert(at, value);
                    model.insert(at, value);
                }
                else if !model.is_empty() {
                    let at = index(model.len() - 1, next(1 << 20)).min(model.len() - 1);

                    assert_eq!(slack.remove(at), model.remove(at));
                }

                if step % 7 == 0 {
                    slack.push(step as u16);
                    model.push(step as u16);
                }

                assert_eq!(slack.as_slice(), &model[..]);
                assert_eq!(slack.len(), model.len());
            }

            slack.shrink_to_fit();
            assert_eq!(slack.as_slice(), &model[..]);
            assert_eq!(slack.capacity(), model.len());
        }
    }

    #[test]
    fn pop_front_and_reinsert_do_not_grow() {
        let mut slack = SlackVec::with_capacity(8);
        slack.extend(0..8);
        let capacity = slack.capacity();

        for _ in 0..100 {
            let value = slack.remove(0);
            slack.insert(0, value);
        }

        for round in 0..100u16 {
            // A pop from the front and a push to the back reuse the same room
            slack.remove(0);
            slack.push(100 + round);
        }

        assert_eq!(slack.capacity(), capacity);
        assert_eq!(slack.as_slice(), &(192..200).collect::<Vec<u16>>()[..]);
    }

    #[test]
    fn splice_and_truncate_respect_the_slack() {
        let mut slack = SlackVec::new();
        slack.extend(0..10);
        slack.remove(0);
        slack.remove(0);

        slack.splice(1..3, vec![40, 41, 42]);
        assert_eq!(slack.as_slice(), &[2, 40, 41, 42, 5, 6, 7, 8, 9]);

        slack.truncate(2);
        assert_eq!(slack.as_slice(), &[2, 40]);

        slack.truncate(0);
        assert!(slack.is_empty());
        slack.push(1);
        assert_eq!(slack.as_slice(), &[1]);
    }
}
