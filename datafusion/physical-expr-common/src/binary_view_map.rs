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
use arrow::array::NullBufferBuilder;
use arrow::array::cast::AsArray;
use arrow::array::{Array, ArrayRef, BinaryViewArray, ByteView, make_view};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::{BinaryViewType, ByteViewType, DataType, StringViewType};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::proxy::{HashTableAllocExt, VecAllocExt};
use std::fmt::Debug;
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
/// Each distinct value is assigned a sequential index (starting from 0) which
/// serves as both the position in the output array and the group index for
/// `GROUP BY` operations.
///
/// This map is used by the special `COUNT DISTINCT` aggregate function to
/// store the distinct values, and by the `GROUP BY` operator to store
/// group values when they are a single string array.
/// Max size of the in-progress buffer before flushing to completed buffers
const BYTE_VIEW_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;

pub struct ArrowBytesViewMap {
    /// Should the output be StringView or BinaryView?
    output_type: OutputType,
    /// Underlying hash set for each distinct value
    map: hashbrown::hash_table::HashTable<Entry>,
    /// Total size of the map in bytes
    map_size: usize,

    /// Views for all stored values (in insertion order)
    views: Vec<u128>,
    /// In-progress buffer for out-of-line string data
    in_progress: Vec<u8>,
    /// Completed buffers containing string data
    completed: Vec<Buffer>,
    /// Tracks null values (true = null)
    nulls: NullBufferBuilder,

    /// random state used to generate hashes
    random_state: RandomState,
    /// buffer that stores hash values (reused across batches to save allocations)
    hashes_buffer: Vec<u64>,
    /// Index of the null value in the views array, if any
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
            nulls: NullBufferBuilder::new(0),
            random_state: RandomState::default(),
            hashes_buffer: vec![],
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
    /// For each value in `values`, calls `observe_fn` with the entry's index
    /// (sequential, starting from 0). This index is the same for both new and
    /// previously seen values, serving as the group index for `GROUP BY`.
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
    /// avoids having to template the entire structure,  making the code
    /// simpler and understand and reducing code bloat due to duplication.
    ///
    /// See comments on `insert_if_new` for more details
    fn insert_if_new_inner<OP, B>(&mut self, values: &ArrayRef, mut observe_fn: OP)
    where
        OP: FnMut(usize),
        B: ByteViewType,
    {
        // step 1: compute hashes
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(values.len(), 0);
        create_hashes([values], &self.random_state, batch_hashes)
            // hash is supported for all types and create_hashes only
            // returns errors for unsupported types
            .unwrap();

        // step 2: insert each value into the set, if not already present
        let values = values.as_byte_view::<B>();

        // Get raw views buffer for direct comparison
        let input_views = values.views();

        // Ensure lengths are equivalent
        assert_eq!(values.len(), self.hashes_buffer.len());

        for i in 0..values.len() {
            let view_u128 = input_views[i];
            let hash = self.hashes_buffer[i];

            // handle null value via validity bitmap check
            if values.is_null(i) {
                let idx = if let Some(null_index) = self.null {
                    null_index
                } else {
                    let null_index = self.views.len();
                    self.views.push(0);
                    self.nulls.append_null();
                    self.null = Some(null_index);
                    null_index
                };
                observe_fn(idx);
                continue;
            }

            // Extract length from the view (first 4 bytes of u128 in little-endian)
            let len = view_u128 as u32;

            // Check if value already exists
            let existing_idx = {
                // Borrow fields for comparison closure
                let views = &self.views;
                let completed = &self.completed;
                let in_progress = &self.in_progress;

                self.map
                    .find(hash, |header| {
                        if header.hash != hash {
                            return false;
                        }

                        let stored_view = views[header.view_idx];

                        // Fast path: inline strings can be compared directly
                        if len <= 12 {
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
                        let input_value: &[u8] = values.value(i).as_ref();
                        stored_value == input_value
                    })
                    .map(|entry| entry.view_idx)
            };

            let idx = if let Some(idx) = existing_idx {
                idx
            } else {
                // no existing value, insert new entry
                let value: &[u8] = values.value(i).as_ref();
                let view_idx = self.views.len();
                self.append_value(value);
                let new_header = Entry { view_idx, hash };

                self.map
                    .insert_accounted(new_header, |h| h.hash, &mut self.map_size);
                view_idx
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

        // Build null buffer if we have any nulls
        let null_buffer = self.nulls.finish();

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

    /// Append a value to our buffers and return the view pointing to it
    fn append_value(&mut self, value: &[u8]) -> u128 {
        let len = value.len();
        let view = if len <= 12 {
            make_view(value, 0, 0)
        } else {
            // Ensure buffer is big enough
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

            make_view(value, buffer_index, offset)
        };

        self.views.push(view);
        self.nulls.append_non_null();
        view
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
        let nulls_size = self.nulls.allocated_size();

        self.map_size
            + views_size
            + in_progress_size
            + completed_size
            + nulls_size
            + self.hashes_buffer.allocated_size()
    }
}

impl Debug for ArrowBytesViewMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowBytesMap")
            .field("map", &"<map>")
            .field("map_size", &self.map_size)
            .field("views_len", &self.views.len())
            .field("completed_buffers", &self.completed.len())
            .field("random_state", &self.random_state)
            .field("hashes_buffer", &self.hashes_buffer)
            .finish()
    }
}

/// Entry in the hash table -- see [`ArrowBytesViewMap`] for more details
///
/// Only stores the view index and hash. The view_idx also serves as the
/// entry's group index since entries are inserted sequentially.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry {
    /// Index into the `views` Vec in [`ArrowBytesViewMap`].
    /// Also serves as the group index for GROUP BY operations.
    view_idx: usize,
    hash: u64,
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
        // values mut appear be in the order they were inserted
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
        // strings mut appear be in the order they were inserted
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
        fn new() -> Self {
            Self {
                map: ArrowBytesViewMap::new(OutputType::Utf8View),
                strings: vec![],
                indexes: HashMap::new(),
            }
        }

        /// Inserts strings into the map and validates the returned indexes
        fn insert(&mut self, strings: &[Option<&str>]) {
            let string_array = StringViewArray::from(strings.to_vec());
            let arr: ArrayRef = Arc::new(string_array);

            let mut expected_indexes = vec![];
            // update self with new values, keeping track of expected indexes
            for str in strings {
                let str = str.map(|s| s.to_string());
                let index = self.indexes.get(&str).cloned().unwrap_or_else(|| {
                    let index = self.strings.len();
                    self.strings.push(str.clone());
                    self.indexes.insert(str, index);
                    index
                });
                expected_indexes.push(index);
            }

            // insert the values into the map, recording returned indexes
            let mut seen_indexes = vec![];
            self.map.insert_if_new(&arr, |idx| {
                seen_indexes.push(idx);
            });

            assert_eq!(expected_indexes, seen_indexes);
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

    #[test]
    fn test_entry_size() {
        // Entry should be 16 bytes: view_idx (usize) + hash (u64)
        // Previously it also stored a full u128 view and a payload,
        // which made each entry 32+ bytes.
        assert_eq!(size_of::<Entry>(), 16);
    }
}
