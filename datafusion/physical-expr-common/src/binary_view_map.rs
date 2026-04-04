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
use arrow::array::{Array, ArrayRef, BinaryViewArray, ByteView};
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
pub struct ArrowBytesViewSet(ArrowBytesViewMap<()>);

impl ArrowBytesViewSet {
    pub fn new(output_type: OutputType) -> Self {
        Self(ArrowBytesViewMap::new(output_type))
    }

    /// Inserts each value from `values` into the set
    pub fn insert(&mut self, values: &ArrayRef) {
        fn make_payload_fn(_value: Option<&[u8]>) {}
        fn observe_payload_fn(_payload: ()) {}
        self.0
            .insert_if_new(values, make_payload_fn, observe_payload_fn);
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
/// Equivalent to `HashSet<String, V>` but with better performance if you need
/// to emit the keys as an Arrow `StringViewArray` / `BinaryViewArray`. For other
/// purposes it is the same as a `HashMap<String, V>`
///
/// # Generic Arguments
///
/// * `V`: payload type
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
/// Note this structure can be used as a `HashSet` by specifying the value type
/// as `()`, as is done by [`ArrowBytesViewSet`].
///
/// This map is used by the special `COUNT DISTINCT` aggregate function to
/// store the distinct values, and by the `GROUP BY` operator to store
/// group values when they are a single string array.
pub struct ArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// Should the output be StringView or BinaryView?
    output_type: OutputType,
    /// Underlying hash set for each distinct value
    map: hashbrown::hash_table::HashTable<Entry<V>>,
    /// Total size of the map in bytes
    map_size: usize,

    /// Views for all stored values (in insertion order).
    /// For non-inline values (>12 bytes), the `buffer_index` field in each view
    /// points into `buffers`.
    views: Vec<u128>,
    /// Buffers containing out-of-line string data. May include:
    /// - Buffers adopted zero-copy from input arrays (high utilization)
    /// - Compacted buffers with only referenced data (low utilization)
    buffers: Vec<Buffer>,
    /// Tracks null values (true = null)
    nulls: NullBufferBuilder,

    /// random state used to generate hashes
    random_state: RandomState,
    /// buffer that stores hash values (reused across batches to save allocations)
    hashes_buffer: Vec<u64>,
    /// view indices of pending non-inline entries (reused across batches)
    pending_view_indices: Vec<usize>,
    /// `(payload, null_index)` for the 'null' value, if any
    /// NOTE null_index is the logical index in the final array, not the index
    /// in the buffer
    null: Option<(V, usize)>,
}

/// The size, in number of entries, of the initial hash table
const INITIAL_MAP_CAPACITY: usize = 512;

impl<V> ArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    pub fn new(output_type: OutputType) -> Self {
        Self {
            output_type,
            map: hashbrown::hash_table::HashTable::with_capacity(INITIAL_MAP_CAPACITY),
            map_size: 0,
            views: Vec::new(),
            buffers: Vec::new(),
            nulls: NullBufferBuilder::new(0),
            random_state: RandomState::default(),
            hashes_buffer: vec![],
            pending_view_indices: Vec::new(),
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

    /// Inserts each value from `values` into the map, invoking `payload_fn` for
    /// each value if *not* already present, deferring the allocation of the
    /// payload until it is needed.
    ///
    /// Note that this is different than a normal map that would replace the
    /// existing entry
    ///
    /// # Arguments:
    ///
    /// `values`: array whose values are inserted
    ///
    /// `make_payload_fn`:  invoked for each value that is not already present
    /// to create the payload, in order of the values in `values`
    ///
    /// `observe_payload_fn`: invoked once, for each value in `values`, that was
    /// already present in the map, with corresponding payload value.
    ///
    /// # Returns
    ///
    /// The payload value for the entry, either the existing value or
    /// the newly inserted value
    ///
    /// # Safety:
    ///
    /// Note that `make_payload_fn` and `observe_payload_fn` are only invoked
    /// with valid values from `values`, not for the `NULL` value.
    pub fn insert_if_new<MP, OP>(
        &mut self,
        values: &ArrayRef,
        make_payload_fn: MP,
        observe_payload_fn: OP,
    ) where
        MP: FnMut(Option<&[u8]>) -> V,
        OP: FnMut(V),
    {
        // Sanity check array type
        match self.output_type {
            OutputType::BinaryView => {
                assert!(matches!(values.data_type(), DataType::BinaryView));
                self.insert_if_new_inner::<MP, OP, BinaryViewType>(
                    values,
                    make_payload_fn,
                    observe_payload_fn,
                )
            }
            OutputType::Utf8View => {
                assert!(matches!(values.data_type(), DataType::Utf8View));
                self.insert_if_new_inner::<MP, OP, StringViewType>(
                    values,
                    make_payload_fn,
                    observe_payload_fn,
                )
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
    fn insert_if_new_inner<MP, OP, B>(
        &mut self,
        values: &ArrayRef,
        mut make_payload_fn: MP,
        mut observe_payload_fn: OP,
    ) where
        MP: FnMut(Option<&[u8]>) -> V,
        OP: FnMut(V),
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
        let input_buffers = values.data_buffers();

        // Ensure lengths are equivalent
        assert_eq!(values.len(), self.hashes_buffer.len());

        // Lazily register input buffers so that new values added during the
        // loop can be read by subsequent rows in the same batch for comparison.
        // After the loop we decide whether to keep the adopted buffers (high
        // utilization) or copy just the referenced bytes into in_progress.
        let mut buffer_offset: Option<u32> = None;
        let mut batch_referenced_bytes: usize = 0;
        self.pending_view_indices.clear();

        for i in 0..values.len() {
            // Safety: i is in range 0..values.len()
            let view_u128 = unsafe { *input_views.get_unchecked(i) };
            let hash = unsafe { *self.hashes_buffer.get_unchecked(i) };

            // handle null value via validity bitmap check
            if values.is_null(i) {
                let payload = if let Some(&(payload, _offset)) = self.null.as_ref() {
                    payload
                } else {
                    let payload = make_payload_fn(None);
                    let null_index = self.views.len();
                    self.views.push(0);
                    self.nulls.append_null();
                    self.null = Some((payload, null_index));
                    payload
                };
                observe_payload_fn(payload);
                continue;
            }

            // Compare length + prefix in a single u64 comparison.
            // View layout (little-endian): len(4B) | prefix(4B) | buffer_index(4B) | offset(4B)
            let input_len_prefix = view_u128 as u64;
            let len = view_u128 as u32;

            // Check if value already exists
            let maybe_payload = {
                let buffers = &self.buffers;
                let views = &self.views;

                self.map
                    .find(hash, |header| {
                        if header.hash != hash {
                            return false;
                        }

                        // Safety: view_idx was valid when inserted
                        let stored_view =
                            unsafe { *views.get_unchecked(header.view_idx as usize) };

                        // Fast path: inline strings can be compared directly
                        if len <= 12 {
                            return stored_view == view_u128;
                        }

                        // Compare length + 4-byte prefix in one u64 comparison
                        let stored_len_prefix = stored_view as u64;
                        if stored_len_prefix != input_len_prefix {
                            return false;
                        }

                        // Prefix matched - compare full bytes
                        let byte_view = ByteView::from(stored_view);
                        let stored_len = byte_view.length as usize;
                        let buffer_index = byte_view.buffer_index as usize;
                        let offset = byte_view.offset as usize;

                        let stored_value = unsafe {
                            buffers
                                .get_unchecked(buffer_index)
                                .as_slice()
                                .get_unchecked(offset..offset + stored_len)
                        };
                        // Safety: i is in range and not null
                        let input_value: &[u8] =
                            unsafe { values.value_unchecked(i).as_ref() };
                        stored_value == input_value
                    })
                    .map(|entry| entry.payload)
            };

            let payload = if let Some(payload) = maybe_payload {
                payload
            } else {
                // no existing value, make a new one
                // Safety: i is in range and not null
                let value: &[u8] = unsafe { values.value_unchecked(i).as_ref() };
                let payload = make_payload_fn(Some(value));

                let new_view = if len <= 12 {
                    view_u128
                } else {
                    // Register input buffers into completed on first use so
                    // subsequent rows in the same batch can compare against them.
                    let offset = *buffer_offset.get_or_insert_with(|| {
                        let off = self.buffers.len() as u32;
                        self.buffers
                            .extend(input_buffers.iter().cloned());
                        off
                    });
                    batch_referenced_bytes += len as usize;
                    self.pending_view_indices.push(self.views.len());

                    let byte_view = ByteView::from(view_u128);
                    ByteView {
                        buffer_index: byte_view.buffer_index + offset,
                        ..byte_view
                    }
                    .into()
                };

                let view_idx = self.views.len() as u32;
                self.views.push(new_view);
                self.nulls.append_non_null();

                let new_header = Entry {
                    view_idx,
                    hash,
                    payload,
                };

                self.map
                    .insert_accounted(new_header, |h| h.hash, &mut self.map_size);
                payload
            };
            observe_payload_fn(payload);
        }

        // Batch finalization: decide whether to keep adopted buffers or copy.
        let Some(adopted_offset) = buffer_offset else {
            return; // no new non-inline values
        };

        let input_buffers_size: usize =
            input_buffers.iter().map(|b| b.len()).sum();

        if batch_referenced_bytes * 2 >= input_buffers_size {
            // High utilization (>= 50%): keep the adopted buffers as-is.
            return;
        }

        // Low utilization: copy only referenced bytes into a new compact buffer,
        // rewrite the pending views, then drop adopted buffers.
        // Hash table entries store view_idx so they see the update automatically.
        let mut compact = Vec::with_capacity(batch_referenced_bytes);
        for &view_idx in &self.pending_view_indices {
            let view = unsafe { self.views.get_unchecked_mut(view_idx) };
            let byte_view = ByteView::from(*view);
            let buf_idx = byte_view.buffer_index as usize;
            let offset = byte_view.offset as usize;
            let length = byte_view.length as usize;

            let value = unsafe {
                self.buffers
                    .get_unchecked(buf_idx)
                    .as_slice()
                    .get_unchecked(offset..offset + length)
            };

            let new_offset = compact.len() as u32;
            compact.extend_from_slice(value);

            *view = ByteView {
                length: byte_view.length,
                prefix: byte_view.prefix,
                buffer_index: adopted_offset,
                offset: new_offset,
            }
            .into();
        }

        // Replace adopted buffers with the single compact buffer
        self.buffers.truncate(adopted_offset as usize);
        self.buffers.push(Buffer::from_vec(compact));
    }

    /// Converts this set into a `StringViewArray`, or `BinaryViewArray`,
    /// containing each distinct value
    /// that was inserted. This is done without copying the values.
    ///
    /// The values are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn into_state(mut self) -> ArrayRef {
        // Build null buffer if we have any nulls
        let null_buffer = self.nulls.finish();

        let views = ScalarBuffer::from(self.views);
        let array =
            unsafe { BinaryViewArray::new_unchecked(views, self.buffers, null_buffer) };

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
        let buffers_size: usize = self.buffers.iter().map(|b| b.len()).sum();
        let nulls_size = self.nulls.allocated_size();

        self.map_size
            + views_size
            + buffers_size
            + nulls_size
            + self.hashes_buffer.allocated_size()
    }
}

impl<V> Debug for ArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowBytesMap")
            .field("map", &"<map>")
            .field("map_size", &self.map_size)
            .field("views_len", &self.views.len())
            .field("buffers", &self.buffers.len())
            .field("random_state", &self.random_state)
            .field("hashes_buffer", &self.hashes_buffer)
            .finish()
    }
}

/// Entry in the hash table -- see [`ArrowBytesViewMap`] for more details
///
/// Stores an index into `ArrowBytesViewMap::views` rather than a copy of
/// the view itself. This means compaction only needs to rewrite `views`
/// — hash-table entries follow automatically.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// Index into [`ArrowBytesViewMap::views`].
    view_idx: u32,

    hash: u64,

    /// value stored by the entry
    payload: V,
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

    #[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
    struct TestPayload {
        // store the string value to check against input
        index: usize, // store the index of the string (each new string gets the next sequential input)
    }

    /// Wraps an [`ArrowBytesViewMap`], validating its invariants
    struct TestMap {
        map: ArrowBytesViewMap<TestPayload>,
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

            let mut next_index = self.indexes.len();
            let mut actual_new_strings = vec![];
            let mut actual_seen_indexes = vec![];
            // update self with new values, keeping track of newly added values
            for str in strings {
                let str = str.map(|s| s.to_string());
                let index = self.indexes.get(&str).cloned().unwrap_or_else(|| {
                    actual_new_strings.push(str.clone());
                    let index = self.strings.len();
                    self.strings.push(str.clone());
                    self.indexes.insert(str, index);
                    index
                });
                actual_seen_indexes.push(index);
            }

            // insert the values into the map, recording what we did
            let mut seen_new_strings = vec![];
            let mut seen_indexes = vec![];
            self.map.insert_if_new(
                &arr,
                |s| {
                    let value = s
                        .map(|s| String::from_utf8(s.to_vec()).expect("Non utf8 string"));
                    let index = next_index;
                    next_index += 1;
                    seen_new_strings.push(value);
                    TestPayload { index }
                },
                |payload| {
                    seen_indexes.push(payload.index);
                },
            );

            assert_eq!(actual_seen_indexes, seen_indexes);
            assert_eq!(actual_new_strings, seen_new_strings);
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
