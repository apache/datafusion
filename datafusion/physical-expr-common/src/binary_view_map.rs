// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`ArrowBytesViewMap`] and [`ArrowBytesViewSet`] for storing maps/sets of values from
//! `StringViewArray`/`BinaryViewArray`.
use crate::binary_map::OutputType;
use arrow::array::cast::AsArray;
use arrow::array::{Array, ArrayRef, BinaryViewArray, ByteView};
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::{BinaryViewType, ByteViewType, DataType, StringViewType};
use arrow::util::bit_util::unset_bit;
use datafusion_common::hash_utils::HashValue;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::utils::proxy::HashTableAllocExt;
use std::mem::size_of;
use std::sync::Arc;

/// HashSet optimized for storing string or binary values that can produce that
/// the final set as a `GenericBinaryViewArray` with minimal copies.
#[derive(Debug)]
pub struct ArrowBytesViewSet(ArrowBytesViewMap);

impl ArrowBytesViewSet {
    pub fn new(output_type: OutputType) -> Self {
        Self(ArrowBytesViewMap::new(output_type))
    }

    /// Inserts each value from `values` into the set
    pub fn insert(&mut self, values: &ArrayRef) {
        self.0.insert_if_new(values, |_idx| {});
    }

    /// Return the contents of this map and replace it with a new empty map with
    /// the same output type
    pub fn take(&mut self) -> Self {
        let mut new_self = Self::new(self.0.output_type);
        std::mem::swap(self, &mut new_self);
        new_self
    }

    /// Converts this set into a `StringViewArray` or `BinaryViewArray`
    /// containing each distinct value that was interned.
    /// This is done without copying the values.
    pub fn into_state(self) -> ArrayRef {
        self.0.into_state()
    }

    /// Returns the total number of distinct values (including nulls) seen so far
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// returns the total number of distinct values (not including nulls) seen so far
    pub fn non_null_len(&self) -> usize {
        self.0.non_null_len()
    }

    /// Return the total size, in bytes, of memory used to store the data in
    /// this set, not including `self`
    pub fn size(&self) -> usize {
        self.0.size()
    }
}

/// Optimized map for storing Arrow "byte view" types (`StringView`, `BinaryView`)
/// values that can produce the set of keys on
/// output as `GenericBinaryViewArray` without copies.
///
/// Each distinct value is assigned a sequential index (its position in
/// insertion order). This index serves as the implicit payload — callers
/// like `GroupValuesBytesView` use it directly as the group index.
///
/// # Description
///
/// This is a specialized HashMap with the following properties:
///
/// 1. Optimized for storing and emitting Arrow byte types  (e.g.
///    `StringViewArray` / `BinaryViewArray`) very efficiently by minimizing copying of
///    the string values themselves, both when inserting and when emitting the
///    final array.
///
/// 2. Retains the insertion order of entries in the final array. The values are
///    in the same order as they were inserted.
///
/// This map is used by the special `COUNT DISTINCT` aggregate function to
/// store the distinct values, and by the `GROUP BY` operator to store
/// group values when they are a single string array.
/// Max size of the in-progress buffer before flushing to completed buffers
const BYTE_VIEW_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;

pub struct ArrowBytesViewMap {
    /// Should the output be StringView or BinaryView?
    output_type: OutputType,
    /// Underlying hash set for each distinct value.
    /// Stores `(index into views, hash)` pairs.
    map: hashbrown::hash_table::HashTable<(usize, u64)>,
    /// Total size of the map in bytes
    map_size: usize,

    /// Views for all stored values (in insertion order)
    views: Vec<u128>,
    /// In-progress buffer for out-of-line string data
    in_progress: Vec<u8>,
    /// Completed buffers containing string data
    completed: Vec<Buffer>,

    /// random state used to generate hashes
    random_state: RandomState,
    /// Index into `views` for the null entry, if any.
    /// The null buffer is reconstructed from this in `into_state()`.
    null: Option<usize>,
}

/// The size, in number of entries, of the initial hash table
const INITIAL_MAP_CAPACITY: usize = 512;

impl ArrowBytesViewMap {
    pub fn new(output_type: OutputType) -> Self {
        Self {
            output_type,
            map: hashbrown::hash_table::HashTable::with_capacity(INITIAL_MAP_CAPACITY),
            map_size: 0,
            views: Vec::new(),
            in_progress: Vec::new(),
            completed: Vec::new(),
            random_state: RandomState::default(),
            null: None,
        }
    }

    /// Return the contents of this map and replace it with a new empty map with
    /// the same output type
    pub fn take(&mut self) -> Self {
        let mut new_self = Self::new(self.output_type);
        std::mem::swap(self, &mut new_self);
        new_self
    }

    /// Inserts each value from `values` into the map.
    ///
    /// For each value, `observe_fn` is called with the value's index in
    /// insertion order (its position in `views`). New values get the next
    /// sequential index; existing values get their previously assigned index.
    pub fn insert_if_new<OP>(&mut self, values: &ArrayRef, observe_fn: OP)
    where
        OP: FnMut(usize),
    {
        // Sanity check array type
        match self.output_type {
            OutputType::BinaryView => {
                assert!(matches!(values.data_type(), DataType::BinaryView));
                self.insert_if_new_inner::<OP, BinaryViewType>(values, observe_fn)
            }
            OutputType::Utf8View => {
                assert!(matches!(values.data_type(), DataType::Utf8View));
                self.insert_if_new_inner::<OP, StringViewType>(values, observe_fn)
            }
            _ => unreachable!("Utf8/Binary should use `ArrowBytesSet`"),
        };
    }

    /// Generic version of [`Self::insert_if_new`] that handles `ByteViewType`
    /// (both StringView and BinaryView)
    ///
    /// Note this is the only function that is generic on [`ByteViewType`], which
    /// avoids having to template the entire structure, making the code
    /// simpler to understand and reducing code bloat due to duplication.
    ///
    /// Dispatches to a const-generic inner loop specialized on the presence
    /// of nulls and out-of-line buffers, eliminating per-row branches.
    fn insert_if_new_inner<OP, B>(&mut self, values: &ArrayRef, observe_fn: OP)
    where
        OP: FnMut(usize),
        B: ByteViewType,
    {
        let values = values.as_byte_view::<B>();
        let input_views = values.views();
        let input_buffers = values.data_buffers();
        let nulls = values.nulls();

        match (nulls.is_some(), !input_buffers.is_empty()) {
            (false, false) => self.insert_loop::<OP, false, false>(
                input_views,
                input_buffers,
                nulls,
                observe_fn,
            ),
            (false, true) => self.insert_loop::<OP, false, true>(
                input_views,
                input_buffers,
                nulls,
                observe_fn,
            ),
            (true, false) => self.insert_loop::<OP, true, false>(
                input_views,
                input_buffers,
                nulls,
                observe_fn,
            ),
            (true, true) => self.insert_loop::<OP, true, true>(
                input_views,
                input_buffers,
                nulls,
                observe_fn,
            ),
        }
    }

    /// Inner loop for [`Self::insert_if_new_inner`], specialized via const generics.
    ///
    /// `HAS_NULLS`: whether the input has any null values (skip null checks if false)
    /// `HAS_BUFFERS`: whether the input has out-of-line buffers (all inline if false)
    #[inline(never)]
    fn insert_loop<OP, const HAS_NULLS: bool, const HAS_BUFFERS: bool>(
        &mut self,
        input_views: &[u128],
        input_buffers: &[Buffer],
        nulls: Option<&NullBuffer>,
        mut observe_fn: OP,
    ) where
        OP: FnMut(usize),
    {
        for (i, &view_u128) in input_views.iter().enumerate() {
            // handle null value — skipped entirely when HAS_NULLS is false
            if HAS_NULLS && nulls.unwrap().is_null(i) {
                let idx = if let Some(null_index) = self.null {
                    null_index
                } else {
                    let null_index = self.views.len();
                    self.views.push(0);
                    self.null = Some(null_index);
                    null_index
                };
                observe_fn(idx);
                continue;
            }

            // Extract length from the view (first 4 bytes of u128 in little-endian)
            let len = view_u128 as u32;

            // Compute hash and resolve non-inline bytes in a single pass.
            // When HAS_BUFFERS is false, all values are inline (<=12 bytes)
            // and we hash the u128 view directly with no pointer chase.
            let (hash, value_bytes) = if !HAS_BUFFERS || len <= 12 {
                (view_u128.hash_one(&self.random_state), None)
            } else {
                let byte_view = ByteView::from(view_u128);
                let buf_idx = byte_view.buffer_index as usize;
                let offset = byte_view.offset as usize;
                let bytes = &input_buffers[buf_idx][offset..offset + len as usize];
                (bytes.hash_one(&self.random_state), Some(bytes))
            };

            // Check if value already exists
            let maybe_idx = {
                let views = &self.views;
                let completed = &self.completed;
                let in_progress = &self.in_progress;

                self.map
                    .find(hash, |&(idx, stored_hash)| {
                        if stored_hash != hash {
                            return false;
                        }

                        let stored_view = views[idx];

                        // When HAS_BUFFERS is false, all values are inline
                        if !HAS_BUFFERS || len <= 12 {
                            return stored_view == view_u128;
                        }

                        // For larger strings: first compare the 4-byte prefix
                        let stored_prefix = (stored_view >> 32) as u32;
                        let input_prefix = (view_u128 >> 32) as u32;
                        if stored_prefix != input_prefix {
                            return false;
                        }

                        // Prefix matched - compare full bytes
                        let byte_view = ByteView::from(stored_view);
                        let stored_len = byte_view.length as usize;
                        let buffer_index = byte_view.buffer_index as usize;
                        let offset = byte_view.offset as usize;

                        let stored_value = if buffer_index < completed.len() {
                            &completed[buffer_index].as_slice()
                                [offset..offset + stored_len]
                        } else {
                            &in_progress[offset..offset + stored_len]
                        };
                        stored_value == value_bytes.unwrap()
                    })
                    .map(|&(idx, _)| idx)
            };

            let idx = if let Some(idx) = maybe_idx {
                idx
            } else {
                let new_idx = self.views.len();

                let new_view = if !HAS_BUFFERS || len <= 12 {
                    // Inline: the input view IS the data, reuse it directly
                    view_u128
                } else {
                    // Copy bytes to our buffer and rewrite the view's
                    // buffer_index + offset while keeping length + prefix
                    let value = value_bytes.unwrap();
                    self.store_non_inline(value, view_u128)
                };

                self.views.push(new_view);

                self.map.insert_accounted(
                    (new_idx, hash),
                    |&(_, h)| h,
                    &mut self.map_size,
                );
                new_idx
            };
            observe_fn(idx);
        }
    }

    /// Converts this set into a `StringViewArray`, or `BinaryViewArray`,
    /// containing each distinct value
    /// that was inserted. This is done without copying the values.
    ///
    /// The values are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn into_state(mut self) -> ArrayRef {
        // Flush any remaining in-progress buffer
        if !self.in_progress.is_empty() {
            let flushed = std::mem::take(&mut self.in_progress);
            self.completed.push(Buffer::from_vec(flushed));
        }

        // Reconstruct null buffer from the null index if present.
        // At most one null exists: start all-valid, unset the single null bit.
        let null_buffer = self.null.map(|null_index| {
            let byte_len = (self.views.len() + 7) / 8;
            let mut buf = MutableBuffer::new(byte_len).with_bitset(byte_len, true);
            unset_bit(buf.as_slice_mut(), null_index);
            // SAFETY: we unset exactly one bit
            unsafe {
                NullBuffer::new_unchecked(
                    BooleanBuffer::new(buf.into(), 0, self.views.len()),
                    1,
                )
            }
        });

        let views = ScalarBuffer::from(self.views);
        let array =
            unsafe { BinaryViewArray::new_unchecked(views, self.completed, null_buffer) };

        match self.output_type {
            OutputType::BinaryView => Arc::new(array),
            OutputType::Utf8View => {
                // SAFETY: all input was valid utf8
                let array = unsafe { array.to_string_view_unchecked() };
                Arc::new(array)
            }
            _ => unreachable!("Utf8/Binary should use `ArrowBytesMap`"),
        }
    }

    /// Store a non-inline value (>12 bytes) in our buffers and return
    /// a view with the original length+prefix but our buffer_index+offset.
    #[inline]
    fn store_non_inline(&mut self, value: &[u8], input_view: u128) -> u128 {
        let len = value.len();
        // Flush if the in-progress buffer would overflow
        if self.in_progress.len() + len > BYTE_VIEW_MAX_BLOCK_SIZE {
            let flushed = std::mem::replace(
                &mut self.in_progress,
                Vec::with_capacity(BYTE_VIEW_MAX_BLOCK_SIZE),
            );
            self.completed.push(Buffer::from_vec(flushed));
        }

        let buffer_index = self.completed.len() as u32;
        let offset = self.in_progress.len() as u32;
        self.in_progress.extend_from_slice(value);

        // Reuse length + prefix from the input view, replace buffer location
        ByteView::from(input_view)
            .with_buffer_index(buffer_index)
            .with_offset(offset)
            .as_u128()
    }

    /// Total number of entries (including null, if present)
    pub fn len(&self) -> usize {
        self.non_null_len() + self.null.map(|_| 1).unwrap_or(0)
    }

    /// Is the set empty?
    pub fn is_empty(&self) -> bool {
        self.map.is_empty() && self.null.is_none()
    }

    /// Number of non null entries
    pub fn non_null_len(&self) -> usize {
        self.map.len()
    }

    /// Return the total size, in bytes, of memory used to store the data in
    /// this set, not including `self`
    pub fn size(&self) -> usize {
        let views_size = self.views.len() * size_of::<u128>();
        let in_progress_size = self.in_progress.capacity();
        let completed_size: usize = self.completed.iter().map(|b| b.len()).sum();

        self.map_size + views_size + in_progress_size + completed_size
    }
}

impl std::fmt::Debug for ArrowBytesViewMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowBytesViewMap")
            .field("map", &"<map>")
            .field("map_size", &self.map_size)
            .field("views_len", &self.views.len())
            .field("completed_buffers", &self.completed.len())
            .field("random_state", &self.random_state)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{GenericByteViewArray, StringViewArray};
    use datafusion_common::HashMap;

    use super::*;

    // asserts that the set contains the expected strings, in the same order
    fn assert_set(set: ArrowBytesViewSet, expected: &[Option<&str>]) {
        let strings = set.into_state();
        let strings = strings.as_string_view();
        let state = strings.into_iter().collect::<Vec<_>>();
        assert_eq!(state, expected);
    }

    #[test]
    fn string_view_set_empty() {
        let mut set = ArrowBytesViewSet::new(OutputType::Utf8View);
        let array: ArrayRef = Arc::new(StringViewArray::new_null(0));
        set.insert(&array);
        assert_eq!(set.len(), 0);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[]);
    }

    #[test]
    fn string_view_set_one_null() {
        let mut set = ArrowBytesViewSet::new(OutputType::Utf8View);
        let array: ArrayRef = Arc::new(StringViewArray::new_null(1));
        set.insert(&array);
        assert_eq!(set.len(), 1);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[None]);
    }

    #[test]
    fn string_view_set_many_null() {
        let mut set = ArrowBytesViewSet::new(OutputType::Utf8View);
        let array: ArrayRef = Arc::new(StringViewArray::new_null(11));
        set.insert(&array);
        assert_eq!(set.len(), 1);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[None]);
    }

    #[test]
    fn test_string_view_set_basic() {
        // basic test for mixed small and large string values
        let values = GenericByteViewArray::from(vec![
            Some("a"),
            Some("b"),
            Some("CXCCCCCCCCAABB"), // 14 bytes
            Some(""),
            Some("cbcxx"), // 5 bytes
            None,
            Some("AAAAAAAA"),     // 8 bytes
            Some("BBBBBQBBBAAA"), // 12 bytes
            Some("a"),
            Some("cbcxx"),
            Some("b"),
            Some("cbcxx"),
            Some(""),
            None,
            Some("BBBBBQBBBAAA"),
            Some("BBBBBQBBBAAA"),
            Some("AAAAAAAA"),
            Some("CXCCCCCCCCAABB"),
        ]);

        let mut set = ArrowBytesViewSet::new(OutputType::Utf8View);
        let array: ArrayRef = Arc::new(values);
        set.insert(&array);
        // values must appear in the order they were inserted
        assert_set(
            set,
            &[
                Some("a"),
                Some("b"),
                Some("CXCCCCCCCCAABB"),
                Some(""),
                Some("cbcxx"),
                None,
                Some("AAAAAAAA"),
                Some("BBBBBQBBBAAA"),
            ],
        );
    }

    #[test]
    fn test_string_set_non_utf8() {
        // basic test for mixed small and large string values
        let values = GenericByteViewArray::from(vec![
            Some("a"),
            Some("✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥"),
            Some("🔥"),
            Some("✨✨✨"),
            Some("foobarbaz"),
            Some("🔥"),
            Some("✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥"),
        ]);

        let mut set = ArrowBytesViewSet::new(OutputType::Utf8View);
        let array: ArrayRef = Arc::new(values);
        set.insert(&array);
        // strings must appear in the order they were inserted
        assert_set(
            set,
            &[
                Some("a"),
                Some("✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥"),
                Some("🔥"),
                Some("✨✨✨"),
                Some("foobarbaz"),
            ],
        );
    }

    // Test use of binary output type
    #[test]
    fn test_binary_set() {
        let v: Vec<Option<&[u8]>> = vec![
            Some(b"a"),
            Some(b"CXCCCCCCCCCCCCC"),
            None,
            Some(b"CXCCCCCCCCCCCCC"),
        ];
        let values: ArrayRef = Arc::new(BinaryViewArray::from(v));

        let expected: Vec<Option<&[u8]>> =
            vec![Some(b"a"), Some(b"CXCCCCCCCCCCCCC"), None];
        let expected: ArrayRef = Arc::new(GenericByteViewArray::from(expected));

        let mut set = ArrowBytesViewSet::new(OutputType::BinaryView);
        set.insert(&values);
        assert_eq!(&set.into_state(), &expected);
    }

    // inserting strings into the set does not increase reported memory
    #[test]
    fn test_string_set_memory_usage() {
        let strings1 = StringViewArray::from(vec![
            Some("a"),
            Some("b"),
            Some("CXCCCCCCCCCCC"), // 13 bytes
            Some("AAAAAAAA"),      // 8 bytes
            Some("BBBBBQBBB"),     // 9 bytes
        ]);
        let total_strings1_len = strings1
            .iter()
            .map(|s| s.map(|s| s.len()).unwrap_or(0))
            .sum::<usize>();
        let values1: ArrayRef = Arc::new(StringViewArray::from(strings1));

        // Much larger strings in strings2
        let strings2 = StringViewArray::from(vec![
            "FOO".repeat(1000),
            "BAR larger than 12 bytes.".repeat(100_000),
            "more unique.".repeat(1000),
            "more unique2.".repeat(1000),
            "FOO".repeat(3000),
        ]);
        let total_strings2_len = strings2
            .iter()
            .map(|s| s.map(|s| s.len()).unwrap_or(0))
            .sum::<usize>();
        let values2: ArrayRef = Arc::new(StringViewArray::from(strings2));

        let mut set = ArrowBytesViewSet::new(OutputType::Utf8View);
        let size_empty = set.size();

        set.insert(&values1);
        let size_after_values1 = set.size();
        assert!(size_empty < size_after_values1);
        assert!(
            size_after_values1 > total_strings1_len,
            "expect {size_after_values1} to be more than {total_strings1_len}"
        );
        assert!(size_after_values1 < total_strings1_len + total_strings2_len);

        // inserting the same strings should not affect the size
        set.insert(&values1);
        assert_eq!(set.size(), size_after_values1);
        assert_eq!(set.len(), 5);

        // inserting the large strings should increase the reported size
        set.insert(&values2);
        let size_after_values2 = set.size();
        assert!(size_after_values2 > size_after_values1);

        assert_eq!(set.len(), 10);
    }

    /// Wraps an [`ArrowBytesViewMap`], validating its invariants
    struct TestMap {
        map: ArrowBytesViewMap,
        // stores distinct strings seen, in order
        strings: Vec<Option<String>>,
        // map strings to index in strings
        indexes: HashMap<Option<String>, usize>,
    }

    impl TestMap {
        /// creates a map with TestPayloads for the given strings and then
        /// validates the payloads
        fn new() -> Self {
            Self {
                map: ArrowBytesViewMap::new(OutputType::Utf8View),
                strings: vec![],
                indexes: HashMap::new(),
            }
        }

        /// Inserts strings into the map
        fn insert(&mut self, strings: &[Option<&str>]) {
            let string_array = StringViewArray::from(strings.to_vec());
            let arr: ArrayRef = Arc::new(string_array);

            let mut actual_seen_indexes = vec![];
            // update self with new values, keeping track of newly added values
            for str in strings {
                let str = str.map(|s| s.to_string());
                let index = self.indexes.get(&str).cloned().unwrap_or_else(|| {
                    let index = self.strings.len();
                    self.strings.push(str.clone());
                    self.indexes.insert(str, index);
                    index
                });
                actual_seen_indexes.push(index);
            }

            // insert the values into the map, recording what we did
            let mut seen_indexes = vec![];
            self.map.insert_if_new(&arr, |idx| {
                seen_indexes.push(idx);
            });

            assert_eq!(actual_seen_indexes, seen_indexes);
        }

        /// Call `self.map.into_array()` validating that the strings are in the same
        /// order as they were inserted
        fn into_array(self) -> ArrayRef {
            let Self {
                map,
                strings,
                indexes: _,
            } = self;

            let arr = map.into_state();
            let expected: ArrayRef = Arc::new(StringViewArray::from(strings));
            assert_eq!(&arr, &expected);
            arr
        }
    }

    #[test]
    fn test_map() {
        let input = vec![
            // Note mix of short/long strings
            Some("A"),
            Some("bcdefghijklmnop1234567"),
            Some("X"),
            Some("Y"),
            None,
            Some("qrstuvqxyzhjwya"),
            Some("✨🔥"),
            Some("🔥"),
            Some("🔥🔥🔥🔥🔥🔥"),
        ];

        let mut test_map = TestMap::new();
        test_map.insert(&input);
        test_map.insert(&input); // put it in twice
        let expected_output: ArrayRef = Arc::new(StringViewArray::from(input));
        assert_eq!(&test_map.into_array(), &expected_output);
    }
}
