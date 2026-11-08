use std::collections::BTreeMap;
use std::collections::btree_map;
use std::mem;
use std::ops::{Bound, RangeBounds};

use crate::{Iter, RoaringBitmap};

/// A Roaring Treemap
///
/// A roaring treemap is a set of 64 bit integers. It is a [`RoaringBitmap`] per distinct value of
/// the upper 32 bits, ordered by that value, so a value costs what it costs in a bitmap and the
/// set of buckets costs one tree node each. The same layout is called a treemap by `roaring-rs`
/// and CRoaring's `Roaring64Map`.
///
/// # How it works
/// A value splits into a 32 bit bucket number, its upper half, and a 32 bit value stored in that
/// bucket's bitmap. A bucket exists only while it holds a value: removing the last value of a
/// bucket, or an operation that leaves a bucket empty, drops the bucket. [`is_empty`](Self::is_empty)
/// and the cost of every walk over the buckets rely on that.
///
/// The treemap keeps the bitmap of one dropped bucket, emptied, and gives it to the next bucket it
/// creates, so a set that empties and refills, like an id free list, does not allocate a bucket's
/// bitmap each time: its two vectors and its first array. It holds at most one, with the
/// capacity its vectors reached, and [`shrink_to_fit`](Self::shrink_to_fit) releases it.
///
/// # Performance Remarks
/// A set whose values sit in few upper halves, such as keys counted up from 0, is one bucket and
/// behaves as the bitmap it holds, plus one tree lookup per call.
///
/// - [`add`](Self::add), [`remove`](Self::remove) and [`contains`](Self::contains) cost one tree
///   lookup, O(log buckets), plus the bitmap's own cost.
/// - [`len`](Self::len) is [`cardinality`](Self::cardinality): it sums [`RoaringBitmap::cardinality`]
///   over the buckets, O(buckets) calls, each O(containers of that bucket). The count is not cached.
/// - [`add_range`](Self::add_range) creates a bucket for every upper half the range crosses and
///   fills it, so its time and memory are O(buckets spanned x 2^16 containers). A range is not a
///   cheap way to name a huge set: a range of the whole universe asks for 2^32 buckets. It is
///   not guarded.
/// - [`remove_range`](Self::remove_range) walks only the buckets the treemap holds within the
///   range, so it is bounded by those and not by the width of the range.
/// - The set operations merge the two bucket lists and run the bitmap operation on the buckets
///   both sets hold, or clone or skip the ones only one holds. They cost O(buckets) plus the
///   bitmap operations. Each bucket lookup is O(log buckets).
///
/// # Examples
/// ```
/// use rusting_bitmaps::RoaringTreemap;
///
/// let mut set = RoaringTreemap::new();
/// set.add(7);
/// set.add(u64::from(u32::MAX) + 1);
/// set.add(u64::MAX);
///
/// assert_eq!(set.len(), 3);
/// assert!(set.contains(u64::MAX));
/// assert_eq!(set.iter().collect::<Vec<_>>(), [7, 1 << 32, u64::MAX]);
/// ```
#[derive(Debug, Default)]
pub struct RoaringTreemap {
    /// The bitmap of each upper half that holds a value. None is empty.
    buckets: BTreeMap<u32, RoaringBitmap>,

    /// The bitmap of a bucket the treemap dropped, emptied, for the next bucket it creates. It
    /// is no bucket and holds no values: only the adds that take it and the operations that
    /// drop a bucket look at it
    spare: Option<RoaringBitmap>
}

/// A clone holds the same values and no spare bucket: a clone of the spare has no allocation,
/// and would keep the clone from taking a real one until an add used it up
impl Clone for RoaringTreemap {
    fn clone(&self) -> Self {
        Self { buckets: self.buckets.clone(), spare: None }
    }
}

impl RoaringTreemap {
    /// Create a new empty roaring treemap
    #[inline]
    pub fn new() -> Self {
        Self { buckets: BTreeMap::new(), spare: None }
    }

    /// Add a value to the treemap
    ///
    /// # Examples
    /// ```
    /// use rusting_bitmaps::RoaringTreemap;
    ///
    /// let mut set = RoaringTreemap::new();
    /// set.add(1 << 40);
    ///
    /// assert!(set.contains(1 << 40));
    /// ```
    #[inline]
    pub fn add(&mut self, value: u64) {
        let (high, low) = split(value);

        bucket(&mut self.buckets, &mut self.spare, high).add(low);
    }

    /// Add a range of values to the treemap. An empty or inverted range adds nothing
    ///
    /// Creates a bucket for every upper half the range crosses, so the time and memory are
    /// O(buckets spanned x 2^16 containers), however few values the range names per bucket. A range
    /// is not a cheap way to name a huge set. Nothing guards a range that spans billions of buckets.
    ///
    /// # Examples
    /// ```
    /// use rusting_bitmaps::RoaringTreemap;
    ///
    /// // Crosses the end of the first bucket
    /// let mut set = RoaringTreemap::new();
    /// set.add_range(u64::from(u32::MAX) - 1..=u64::from(u32::MAX) + 2);
    ///
    /// assert_eq!(set.len(), 4);
    /// ```
    pub fn add_range<R: RangeBounds<u64>>(&mut self, range: R) {
        let (first, last) = match inclusive_bounds(range) {
            Some(bounds) => bounds,
            None => return
        };

        for (high, low) in bucket_ranges(first, last) {
            bucket(&mut self.buckets, &mut self.spare, high).add_range(low);
        }
    }

    /// Create a treemap of the values whose bits are set in `words`, bit `i` of word `w` being
    /// the value `base + w * 64 + i`
    ///
    /// The cost is O(`words`) however few bits are set, plus one tree insert per bucket: a pass
    /// over 1024 words for each 65536 values the words cover. It replaces [`add`](Self::add) per
    /// value only where the words are densely set. Words with no bit set create no bucket.
    ///
    /// # Panics
    /// Panics if `base` is not a multiple of 65536, the values of a container, or if `words`
    /// reach past `u64::MAX`.
    ///
    /// # Examples
    /// ```
    /// use rusting_bitmaps::RoaringTreemap;
    ///
    /// let set = RoaringTreemap::from_lsb0_words(1 << 32, &[0b101, 1 << 63]);
    ///
    /// assert_eq!(set.iter().collect::<Vec<_>>(), [1 << 32, (1 << 32) + 2, (1 << 32) + 127]);
    /// ```
    pub fn from_lsb0_words(base: u64, words: &[u64]) -> Self {
        assert_eq!(base % 65536, 0, "base is not a multiple of 65536");
        assert!(
            base / 65536 + words.len().div_ceil(1024) as u64 <= 1 << 48,
            "words reach past u64::MAX"
        );

        let mut set     = Self::new();
        let mut key     = base / 65536;
        let mut rest    = words;

        while !rest.is_empty() {
            // A bucket holds the containers up to its last key, and the words go on in the next.
            let room            = (65536 - (key & 0xFFFF)) as usize * 1024;
            let (now, later)    = rest.split_at(room.min(rest.len()));
            let bitmap          = RoaringBitmap::from_lsb0_words(key as u16, now);

            if !bitmap.is_empty() {
                set.buckets.insert((key >> 16) as u32, bitmap);
            }

            key  += now.len().div_ceil(1024) as u64;
            rest  = later;
        }

        set
    }

    /// Remove a value from the treemap, returning whether it was present
    ///
    /// # Examples
    /// ```
    /// use rusting_bitmaps::RoaringTreemap;
    ///
    /// let mut set = RoaringTreemap::new();
    /// set.add(1 << 33);
    ///
    /// assert!(set.remove(1 << 33));
    /// assert!(!set.remove(1 << 33));
    /// assert!(set.is_empty());
    /// ```
    pub fn remove(&mut self, value: u64) -> bool {
        let (high, low) = split(value);

        let (removed, emptied) = match self.buckets.get_mut(&high) {
            Some(bucket) => {
                let removed = bucket.remove(low);

                (removed, bucket.is_empty())
            },
            None => return false
        };

        if emptied {
            if let Some(bucket) = self.buckets.remove(&high) {
                keep_as_spare(&mut self.spare, bucket);
            }
        }

        removed
    }

    /// Remove a range of values from the treemap. An empty or inverted range removes nothing
    ///
    /// Walks only the buckets the treemap holds within the range.
    pub fn remove_range<R: RangeBounds<u64>>(&mut self, range: R) {
        let (first, last) = match inclusive_bounds(range) {
            Some(bounds) => bounds,
            None => return
        };

        let (first_high, last_high) = ((first >> 32) as u32, (last >> 32) as u32);

        // A bucket the range empties is dropped as it is found, so no list of them is allocated.
        // Only the first and last bucket of the range can be left holding values
        let mut from = first_high;
        while let Some((&high, bucket)) = self.buckets.range_mut(from..=last_high).next() {
            let start = if high == first_high { first as u32 } else { 0 };
            let end = if high == last_high { last as u32 } else { u32::MAX };

            bucket.remove_range(start..=end);
            if bucket.is_empty() {
                if let Some(bucket) = self.buckets.remove(&high) {
                    keep_as_spare(&mut self.spare, bucket);
                }
            }

            if high == last_high {
                break;
            }

            from = high + 1;
        }
    }

    /// Check if the treemap contains a value
    pub fn contains(&self, value: u64) -> bool {
        let (high, low) = split(value);

        self.buckets.get(&high).is_some_and(|bucket| bucket.contains(low))
    }

    /// Get the length of the treemap
    ///
    /// This is the same as [`cardinality`](Self::cardinality) and costs the same: O(buckets) calls
    /// of [`RoaringBitmap::cardinality`], each O(containers of that bucket), with nothing cached.
    #[inline]
    pub fn len(&self) -> usize {
        self.cardinality()
    }

    /// Get the cardinality of the treemap
    ///
    /// Sums [`RoaringBitmap::cardinality`] over the buckets, so it costs O(buckets) calls of
    /// that, each O(containers of its bucket). Nothing is cached.
    pub fn cardinality(&self) -> usize {
        self.buckets.values().map(RoaringBitmap::cardinality).sum()
    }

    /// Check if the treemap is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Remove every value from the treemap
    ///
    /// Keeps the bitmap of the first bucket for the next one the treemap creates, with the
    /// capacity its vectors reached, until [`shrink_to_fit`](Self::shrink_to_fit).
    pub fn clear(&mut self) {
        // Dropping the iterator drops the buckets after the first, as `BTreeMap::clear` does
        let mut buckets = mem::take(&mut self.buckets).into_iter();
        if let Some((_, bucket)) = buckets.next() {
            keep_as_spare(&mut self.spare, bucket);
        }
    }

    /// Shrink the memory used by the treemap to fit its contents
    ///
    /// Releases the emptied bucket the treemap keeps for the next one it creates, and shrinks
    /// every bucket's bitmap as [`RoaringBitmap::shrink_to_fit`] does.
    pub fn shrink_to_fit(&mut self) {
        self.spare = None;

        for bucket in self.buckets.values_mut() {
            bucket.shrink_to_fit();
        }
    }

    /// And this treemap with `other` (intersect)
    pub fn and(&self, other: &Self) -> Self {
        let mut result = Self::new();

        for (high, bucket) in &self.buckets {
            if let Some(other) = other.buckets.get(high) {
                let both = bucket.and(other);
                if !both.is_empty() {
                    result.buckets.insert(*high, both);
                }
            }
        }

        result
    }

    /// Or this treemap with `other` (union)
    pub fn or(&self, other: &Self) -> Self {
        let mut result = self.clone();
        result.inplace_or(other);

        result
    }

    /// And not this treemap with `other` (difference)
    pub fn and_not(&self, other: &Self) -> Self {
        let mut result = Self::new();

        for (high, bucket) in &self.buckets {
            match other.buckets.get(high) {
                Some(other)
                    => {
                        let rest = bucket.and_not(other);
                        if !rest.is_empty() {
                            result.buckets.insert(*high, rest);
                        }
                    },
                None
                    => {
                        result.buckets.insert(*high, bucket.clone());
                    }
            }
        }

        result
    }

    /// And this treemap with `other` in place. Buckets the result leaves empty are dropped
    pub fn inplace_and(&mut self, other: &Self) {
        let spare = &mut self.spare;

        self.buckets.retain(|high, bucket| {
            let keep = match other.buckets.get(high) {
                Some(other)
                    => {
                        bucket.inplace_and(other);

                        !bucket.is_empty()
                    },
                None
                    => false
            };

            if !keep && spare.is_none() {
                // `retain` does not hand over the bucket it drops
                keep_as_spare(spare, mem::take(bucket));
            }

            keep
        });
    }

    /// Or this treemap with `other` in place
    pub fn inplace_or(&mut self, other: &Self) {
        for (high, other) in &other.buckets {
            match self.buckets.entry(*high) {
                btree_map::Entry::Occupied(bucket) => bucket.into_mut().inplace_or(other),
                btree_map::Entry::Vacant(bucket) => { bucket.insert(other.clone()); }
            }
        }
    }

    /// And not this treemap with `other` in place. Buckets the result leaves empty are dropped
    pub fn inplace_and_not(&mut self, other: &Self) {
        for (high, other) in &other.buckets {
            let emptied = match self.buckets.get_mut(high) {
                Some(bucket)
                    => {
                        bucket.inplace_and_not(other);

                        bucket.is_empty()
                    },
                None
                    => false
            };

            if emptied {
                if let Some(bucket) = self.buckets.remove(high) {
                    keep_as_spare(&mut self.spare, bucket);
                }
            }
        }
    }

    /// Get the cardinality of the intersection with `other` without building it
    pub fn and_cardinality(&self, other: &Self) -> usize {
        self.buckets.iter()
            .filter_map(|(high, bucket)| other.buckets.get(high).map(|other| bucket.and_cardinality(other)))
            .sum()
    }

    /// Iterate the values of the treemap in ascending order
    ///
    /// # Examples
    /// ```
    /// use rusting_bitmaps::RoaringTreemap;
    ///
    /// let mut set = RoaringTreemap::new();
    /// set.add(1 << 33);
    /// set.add(2);
    ///
    /// assert_eq!(set.iter().collect::<Vec<_>>(), [2, 1 << 33]);
    /// ```
    pub fn iter(&self) -> TreemapIter<'_> {
        TreemapIter {
            buckets : self.buckets.iter(),
            high    : 0,
            iter    : None
        }
    }
}

/// An iterator over a roaring treemap
pub struct TreemapIter<'a> {
    /// The buckets not yet iterated
    buckets: btree_map::Iter<'a, u32, RoaringBitmap>,

    /// The upper half of the values in the bucket being iterated, in the upper 32 bits
    high: u64,

    /// The iterator of the bucket being iterated
    iter: Option<Iter<'a>>
}

impl<'a> Iterator for TreemapIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = &mut self.iter {
                if let Some(low) = iter.next() {
                    return Some(self.high | u64::from(low));
                }
            }

            let (high, bucket) = self.buckets.next()?;

            self.high = u64::from(*high) << 32;
            self.iter = Some(bucket.iter());
        }
    }
}

/// The bucket of `high`, created from the spare if there is no such bucket
#[inline]
fn bucket<'a>(
    buckets: &'a mut BTreeMap<u32, RoaringBitmap>,
    spare: &mut Option<RoaringBitmap>,
    high: u32
) -> &'a mut RoaringBitmap {
    buckets.entry(high).or_insert_with(|| spare.take().unwrap_or_default())
}

/// Keep the bitmap of a bucket the treemap is dropping as its spare if it has none, emptied:
/// a bucket can be dropped with its values
#[inline]
fn keep_as_spare(spare: &mut Option<RoaringBitmap>, mut bucket: RoaringBitmap) {
    if spare.is_none() {
        bucket.clear();
        *spare = Some(bucket);
    }
}

/// The upper and lower 32 bits of a value
#[inline]
fn split(value: u64) -> (u32, u32) {
    ((value >> 32) as u32, value as u32)
}

/// The first and last value of a range of `u64` values, or `None` if it holds none. It
/// represents ranges that include `u64::MAX`
fn inclusive_bounds<R: RangeBounds<u64>>(range: R) -> Option<(u64, u64)> {
    let first = match range.start_bound() {
        Bound::Excluded(&bound) => bound.checked_add(1)?,
        Bound::Included(&bound) => bound,
        Bound::Unbounded => 0
    };

    let last = match range.end_bound() {
        Bound::Excluded(&bound) => bound.checked_sub(1)?,
        Bound::Included(&bound) => bound,
        Bound::Unbounded => u64::MAX
    };

    if first <= last { Some((first, last)) } else { None }
}

/// The bucket number and the inclusive range of lower halves inside it, for each bucket the
/// values from `first` to `last` touch
fn bucket_ranges(first: u64, last: u64) -> impl Iterator<Item = (u32, std::ops::RangeInclusive<u32>)> {
    let (first_high, first_low) = split(first);
    let (last_high, last_low) = split(last);

    (first_high..=last_high).map(move |high| {
        let start = if high == first_high { first_low } else { 0 };
        let end = if high == last_high { last_low } else { u32::MAX };

        (high, start..=end)
    })
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use super::*;

    /// The values one bucket either side of each boundary and the ends of the universe
    const EDGES: [u64; 9] = [
        0,
        1,
        u32::MAX as u64 - 1,
        u32::MAX as u64,
        u32::MAX as u64 + 1,
        u32::MAX as u64 + 2,
        (u32::MAX as u64) << 32,
        u64::MAX - 1,
        u64::MAX
    ];

    fn from_values(values: &[u64]) -> RoaringTreemap {
        let mut set = RoaringTreemap::new();
        for value in values {
            set.add(*value);
        }

        set
    }

    /// The first 64 values of a set that is expected to hold fewer. A set a regression made huge fails the
    /// comparison it is in rather than exhausting memory first
    fn to_vec(set: &RoaringTreemap) -> Vec<u64> {
        set.iter().take(64).collect()
    }

    /// Check `set` holds exactly the values of `expected`. The length is compared first and the
    /// values lazily, so a set of the wrong size fails here without being collected
    fn assert_holds<I>(set: &RoaringTreemap, expected: I)
        where I: Iterator<Item = u64> + Clone
    {
        assert_eq!(set.len(), expected.clone().count());
        assert!(set.iter().eq(expected), "the values differ");
    }

    /// Check two sets hold the same values, length first and then lazily
    fn assert_same(set: &RoaringTreemap, expected: &RoaringTreemap) {
        assert_eq!(set.len(), expected.len());
        assert!(set.iter().eq(expected.iter()), "the values differ");
    }

    /// Values at the edges of a bucket are stored, found and iterated in order
    #[test]
    fn treemap_holds_values_either_side_of_a_bucket_boundary() {
        let set = from_values(&EDGES);

        assert_eq!(set.len(), EDGES.len());
        assert_eq!(to_vec(&set), EDGES);
        for value in EDGES {
            assert!(set.contains(value), "{} must be present", value);
        }

        assert!(!set.contains(2));
        assert!(!set.contains(u64::from(u32::MAX) + 3));
        assert!(!set.contains(u64::MAX - 2));
    }

    /// Adding a value twice holds it once
    #[test]
    fn treemap_add_of_a_present_value_changes_nothing() {
        let mut set = from_values(&[5, u64::MAX]);
        set.add(5);
        set.add(u64::MAX);

        assert_eq!(set.len(), 2);
    }

    /// Removing a value reports whether it was there, and the last value of a bucket drops the bucket
    #[test]
    fn treemap_remove_drops_an_emptied_bucket() {
        let mut set = from_values(&[1, 2, 1 << 32, u64::MAX]);
        assert_eq!(set.buckets.len(), 3);

        assert!(set.remove(1 << 32));
        assert!(!set.remove(1 << 32), "a removed value must not be removed twice");
        assert!(!set.remove(1 << 33), "a value in no bucket must not be removed");
        assert_eq!(set.buckets.len(), 2, "the emptied bucket must go");

        assert!(set.remove(1));
        assert_eq!(set.buckets.len(), 2, "a bucket with a value left must stay");
        assert!(set.remove(2));
        assert!(set.remove(u64::MAX));
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert_eq!(set.iter().next(), None);
    }

    /// A range that spans buckets adds every value in it and no other
    #[test]
    fn treemap_add_range_spans_buckets() {
        let mut set = RoaringTreemap::new();
        set.add_range(u64::from(u32::MAX) - 2..u64::from(u32::MAX) + 3);

        assert_eq!(to_vec(&set), [4_294_967_293, 4_294_967_294, 4_294_967_295, 4_294_967_296, 4_294_967_297]);
        assert_eq!(set.buckets.len(), 2);

        // Inclusive and exclusive ends, and the end of the universe
        let mut set = RoaringTreemap::new();
        set.add_range(u64::MAX - 2..=u64::MAX);
        assert_eq!(to_vec(&set), [u64::MAX - 2, u64::MAX - 1, u64::MAX]);

        let mut set = RoaringTreemap::new();
        set.add_range(u64::MAX - 2..u64::MAX);
        assert_eq!(to_vec(&set), [u64::MAX - 2, u64::MAX - 1]);

        let mut set = RoaringTreemap::new();
        set.add_range((Bound::Excluded(u64::MAX - 1), Bound::Unbounded));
        assert_eq!(to_vec(&set), [u64::MAX]);

        // A range that spans more than two buckets fills the ones between
        let mut set = RoaringTreemap::new();
        set.add_range((1u64 << 32) - 1..=(3u64 << 32));
        assert_eq!(set.buckets.len(), 4);
        assert_eq!(set.len(), 1 + (2usize << 32) + 1);
    }

    /// An empty or inverted range adds and removes nothing
    #[test]
    fn treemap_empty_and_inverted_ranges_do_nothing() {
        let mut set = from_values(&[10]);
        set.add_range(5..5);
        set.add_range(9..4);
        set.add_range((Bound::Excluded(u64::MAX), Bound::Unbounded));
        set.add_range(..0);
        set.remove_range(5..5);
        set.remove_range(20..10);

        assert_eq!(to_vec(&set), [10]);
    }

    /// Removing a range that spans buckets removes its values and drops the buckets it empties
    #[test]
    fn treemap_remove_range_spans_buckets() {
        let mut set = RoaringTreemap::new();
        set.add_range(0..=(2u64 << 32) + 4);
        set.add(u64::MAX);

        set.remove_range(u64::from(u32::MAX) - 1..(2u64 << 32));
        assert!(set.contains(u64::from(u32::MAX) - 2));
        assert!(!set.contains(u64::from(u32::MAX) - 1));
        assert!(!set.contains(u64::from(u32::MAX) + 1));
        assert!(!set.contains((2u64 << 32) - 1));
        assert!(set.contains(2u64 << 32), "the end of an exclusive range must stay");
        assert_eq!(set.buckets.len(), 3, "buckets 0, 2 and the top one remain");

        // A range over a whole bucket drops it, and one over the top of the universe works
        set.remove_range(2u64 << 32..=(2u64 << 32) + 4);
        assert_eq!(set.buckets.len(), 2);
        set.remove_range(u64::MAX - 1..=u64::MAX);
        assert!(!set.contains(u64::MAX));
        assert_eq!(set.buckets.len(), 1);

        set.remove_range(..);
        assert!(set.is_empty());
    }

    /// And keeps the common values, drops the buckets that end up empty, and matches a plain set
    #[test]
    fn treemap_and_drops_empty_buckets() {
        let a = from_values(&[1, 2, 1 << 32, (1 << 32) + 1, u64::MAX]);
        let b = from_values(&[2, (1 << 32) + 1, 3 << 32, u64::MAX - 1]);

        let both = a.and(&b);
        assert_eq!(to_vec(&both), [2, (1 << 32) + 1]);
        assert_eq!(both.buckets.len(), 2);

        // Buckets that share no value leave none
        let c = from_values(&[1 << 32, u64::MAX]);
        let d = from_values(&[(1 << 32) + 1, u64::MAX - 1]);
        let none = c.and(&d);
        assert!(none.is_empty());
        assert_eq!(none.buckets.len(), 0, "a bucket with no common value must not be kept");

        let mut c = c;
        c.inplace_and(&d);
        assert!(c.is_empty(), "an in place and must drop the buckets it empties");
        assert_eq!(c.buckets.len(), 0);

        let mut a = a;
        a.inplace_and(&b);
        assert_eq!(to_vec(&a), [2, (1 << 32) + 1]);
        assert_eq!(a.buckets.len(), 2);
        assert_eq!(a.and_cardinality(&b), 2);
    }

    /// Or holds the values of both and and_not the values of the first alone
    #[test]
    fn treemap_or_and_and_not_cross_buckets() {
        let a = from_values(&[1, 1 << 32, u64::MAX]);
        let b = from_values(&[2, 1 << 32, 7 << 32]);

        assert_eq!(to_vec(&a.or(&b)), [1, 2, 1 << 32, 7 << 32, u64::MAX]);

        let rest = a.and_not(&b);
        assert_eq!(to_vec(&rest), [1, u64::MAX]);
        assert_eq!(rest.buckets.len(), 2, "the bucket the difference emptied must go");

        let mut or = a.clone();
        or.inplace_or(&b);
        assert_eq!(or.len(), 5);

        let mut rest = a.clone();
        rest.inplace_and_not(&b);
        assert_eq!(to_vec(&rest), [1, u64::MAX]);
        assert_eq!(rest.buckets.len(), 2);

        // Removing everything leaves nothing, not empty buckets
        let mut all = a.clone();
        all.inplace_and_not(&a);
        assert!(all.is_empty());
        assert_eq!(all.buckets.len(), 0);

        // An empty operand
        let empty = RoaringTreemap::new();
        assert_eq!(to_vec(&a.and_not(&empty)), to_vec(&a));
        assert_eq!(to_vec(&a.or(&empty)), to_vec(&a));
        assert!(a.and(&empty).is_empty());
        assert_eq!(a.and_cardinality(&empty), 0);
    }

    /// A clone is independent of its source
    #[test]
    fn treemap_clone_is_independent() {
        let mut a = from_values(&[1, 1 << 32]);
        let b = a.clone();

        a.add(2);
        a.remove(1 << 32);

        assert_eq!(to_vec(&b), [1, 1 << 32]);
        assert_eq!(to_vec(&a), [1, 2]);
    }

    /// Clearing empties the treemap and leaves it usable
    #[test]
    fn treemap_clear_empties_the_buckets() {
        let mut set = from_values(&EDGES);
        set.clear();

        assert!(set.is_empty());
        assert_eq!(set.buckets.len(), 0);

        set.add(3);
        assert_eq!(to_vec(&set), [3]);
    }

    /// Random operations agree with a `BTreeSet`, with values concentrated at bucket boundaries
    #[test]
    fn treemap_matches_a_plain_set_under_random_operations() {
        let mut state = 0x0206_0807_0405_0409u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;

            state
        };

        // A value in one of six buckets, near the start of it or within 300 of its end, so buckets
        // are shared and emptied often and short ranges cross a bucket boundary
        let value = |rng: &mut dyn FnMut() -> u64| {
            let high = [0u64, 1, 2, 3, u32::MAX as u64 - 1, u32::MAX as u64][(rng() % 6) as usize];
            let low = if rng() % 2 == 0 { rng() % 70_000 } else { u32::MAX as u64 - rng() % 300 };

            (high << 32) | low
        };

        let mut set = RoaringTreemap::new();
        let mut model = BTreeSet::new();
        let mut other = RoaringTreemap::new();
        let mut other_model = BTreeSet::new();

        for step in 0..4_000 {
            let v = value(&mut next);
            match next() % 8 {
                0 | 1 | 2 => { set.add(v); model.insert(v); },
                3 | 4 => assert_eq!(set.remove(v), model.remove(&v)),
                5 => { other.add(v); other_model.insert(v); },
                6 => {
                    let end = v.saturating_add(next() % 300);
                    set.add_range(v..end);
                    model.extend(v..end);
                },
                _ => {
                    let end = v.saturating_add(next() % 300);
                    set.remove_range(v..end);
                    for x in v..end {
                        model.remove(&x);
                    }
                }
            }

            if step % 500 == 499 {
                assert_holds(&set, model.iter().copied());

                let and = set.and(&other);
                let or = set.or(&other);
                let not = set.and_not(&other);
                assert_holds(&and, model.intersection(&other_model).copied());
                assert_holds(&or, model.union(&other_model).copied());
                assert_holds(&not, model.difference(&other_model).copied());
                assert_eq!(set.and_cardinality(&other), and.len());

                for result in [&and, &or, &not] {
                    assert!(result.buckets.values().all(|bucket| !bucket.is_empty()), "no bucket may be empty");
                }

                let mut x = set.clone();
                x.inplace_and(&other);
                assert_same(&x, &and);
                let mut x = set.clone();
                x.inplace_or(&other);
                assert_same(&x, &or);
                let mut x = set.clone();
                x.inplace_and_not(&other);
                assert_same(&x, &not);
                for x in [&x] {
                    assert!(x.buckets.values().all(|bucket| !bucket.is_empty()), "no bucket may be empty");
                }
            }
        }
    }

    /// Deterministic words with about `density` of 64 bits set
    fn words_with_density(seed: u64, count: usize, density: u32) -> Vec<u64> {
        let mut state = seed;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        (0..count)
            .map(|_| (0..density).fold(0, |word, _| word | 1 << (next() % 64)))
            .collect()
    }

    /// The set `add` builds from the same words
    fn from_words_by_add(base: u64, words: &[u64]) -> RoaringTreemap {
        let mut set = RoaringTreemap::new();
        for (index, word) in words.iter().enumerate() {
            for bit in 0..64 {
                if word >> bit & 1 == 1 {
                    set.add(base + index as u64 * 64 + bit);
                }
            }
        }

        set
    }

    /// Check `set` is `expected` and behaves as it under the set operations
    fn assert_equivalent(set: &RoaringTreemap, expected: &RoaringTreemap) {
        assert_same(set, expected);
        assert_eq!(set.cardinality(), expected.cardinality());
        assert!(set.buckets.values().all(|bucket| !bucket.is_empty()), "no bucket may be empty");
        for value in expected.iter().step_by(11) {
            assert!(set.contains(value));
        }

        let other = from_values(&[0, 5, 1 << 32, (1 << 32) + 70_000, 3 << 32, (3 << 32) + 131_072 + 64]);
        assert_same(&set.or(&other), &expected.or(&other));
        assert_same(&set.and(&other), &expected.and(&other));
        assert_same(&set.and_not(&other), &expected.and_not(&other));
        assert_same(&other.and_not(set), &other.and_not(expected));
        assert_eq!(set.and_cardinality(&other), expected.and_cardinality(&other));
    }

    /// A base above 2^32 puts the values in a later bucket
    #[test]
    fn treemap_from_lsb0_words_above_the_first_bucket() {
        for base in [1 << 32, (5 << 32) + 3 * 65_536, u64::MAX - 65_535] {
            let words = words_with_density(base, 1024, 3);
            let set = RoaringTreemap::from_lsb0_words(base, &words);

            assert_equivalent(&set, &from_words_by_add(base, &words));
            assert_eq!(set.iter().next().unwrap() >> 32, base >> 32);
        }
    }

    /// A last chunk shorter than 1024 words adds only its bits
    #[test]
    fn treemap_from_lsb0_words_partial_last_chunk() {
        for count in [1, 63, 1023, 1025, 2048, 2049, 3000] {
            let words = words_with_density(count as u64, count, 6);
            let set = RoaringTreemap::from_lsb0_words(7 * 65_536, &words);

            assert_equivalent(&set, &from_words_by_add(7 * 65_536, &words));
            assert!(set.iter().last().unwrap() < 7 * 65_536 + count as u64 * 64);
        }
    }

    /// A run of words that crosses from one bucket into the next
    #[test]
    fn treemap_from_lsb0_words_crosses_a_bucket() {
        let base = (1 << 32) - 65_536;
        let words = words_with_density(9, 2048 + 5, 4);
        let set = RoaringTreemap::from_lsb0_words(base, &words);

        assert_eq!(set.buckets.len(), 2);
        assert_equivalent(&set, &from_words_by_add(base, &words));
    }

    /// The words reach the last value of the universe
    #[test]
    fn treemap_from_lsb0_words_at_the_end_of_the_universe() {
        let mut words = vec![0u64; 1024];
        words[0] = 1;
        words[1023] = 1 << 63;

        let set = RoaringTreemap::from_lsb0_words(u64::MAX - 65_535, &words);

        assert_eq!(to_vec(&set), [u64::MAX - 65_535, u64::MAX]);
    }

    /// Empty words and words with no bit set create no bucket
    #[test]
    fn treemap_from_lsb0_words_without_bits_creates_nothing() {
        assert!(RoaringTreemap::from_lsb0_words(1 << 40, &[]).is_empty());

        let set = RoaringTreemap::from_lsb0_words(1 << 40, &[0; 3000]);
        assert!(set.is_empty());
        assert_eq!(set.buckets.len(), 0);

        // A chunk of zeros between two with bits makes no container
        let mut words = vec![0u64; 3 * 1024];
        words[0] = 1;
        words[2 * 1024] = 1;
        let set = RoaringTreemap::from_lsb0_words(1 << 40, &words);
        assert_eq!(to_vec(&set), [1 << 40, (1 << 40) + 2 * 65_536]);
    }

    /// Full chunks match `add_range`
    #[test]
    fn treemap_from_lsb0_words_full() {
        let set = RoaringTreemap::from_lsb0_words(3 << 32, &vec![u64::MAX; 3 * 1024 + 2]);

        let mut expected = RoaringTreemap::new();
        expected.add_range(3 << 32..(3 << 32) + 3 * 65_536 + 128);

        assert_equivalent(&set, &expected);
        assert_eq!(set.len(), 3 * 65_536 + 128);
    }

    #[test]
    #[should_panic(expected = "multiple of 65536")]
    fn treemap_from_lsb0_words_rejects_an_unaligned_base() {
        RoaringTreemap::from_lsb0_words(65_537, &[1]);
    }

    #[test]
    #[should_panic(expected = "past u64::MAX")]
    fn treemap_from_lsb0_words_rejects_words_past_the_universe() {
        RoaringTreemap::from_lsb0_words(u64::MAX - 65_535, &[1; 1025]);
    }
}
