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

//! [`ArrowBytesMap`] and [`ArrowBytesSet`] for storing maps/sets of values from
//! StringArray / LargeStringArray / BinaryArray / LargeBinaryArray.
use ahash::RandomState;
use arrow::array::cast::AsArray;
use arrow::array::{
    Array, ArrayRef, BinaryViewArray, BooleanBufferBuilder, GenericByteViewArray,
    StringViewArray,
};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::{BinaryViewType, ByteViewType, DataType, StringViewType};
use arrow_data::ByteView;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::proxy::{RawTableAllocExt, VecAllocExt};
use std::fmt::Debug;
use std::sync::Arc;

/// Should the output be a String or Binary?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputType {
    /// `StringArray` or `LargeStringArray`
    Utf8View,
    /// `BinaryArray` or `LargeBinaryArray`
    BinaryView,
}

/// HashSet optimized for storing string or binary values that can produce that
/// the final set as a GenericStringArray with minimal copies.
#[derive(Debug)]
pub struct ArrowBytesViewSet(ArrowBytesViewMap<()>);

impl ArrowBytesViewSet {
    pub fn new(output_type: OutputType) -> Self {
        Self(ArrowBytesViewMap::new(output_type))
    }

    /// Return the contents of this set and replace it with a new empty
    /// set with the same output type
    pub(super) fn take(&mut self) -> Self {
        Self(self.0.take())
    }

    /// Inserts each value from `values` into the set
    pub fn insert(&mut self, values: &ArrayRef) {
        fn make_payload_fn(_value: Option<&[u8]>) {}
        fn observe_payload_fn(_payload: ()) {}
        self.0
            .insert_if_new(values, make_payload_fn, observe_payload_fn);
    }

    /// Converts this set into a `StringArray`/`LargeStringArray` or
    /// `BinaryArray`/`LargeBinaryArray` containing each distinct value that
    /// was interned. This is done without copying the values.
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
/// Equivalent to `HashSet<String, V>` but with better performance for arrow
/// data.
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
/// `StringViewArray` / `BinaryViewArray`) very efficiently by minimizing copying of
/// the string values themselves, both when inserting and when emitting the
/// final array.
///
///
/// 2. Retains the insertion order of entries in the final array. The values are
/// in the same order as they were inserted.
///
/// Note this structure can be used as a `HashSet` by specifying the value type
/// as `()`, as is done by [`ArrowByteViewSet`].
///
/// This map is used by the special `COUNT DISTINCT` aggregate function to
/// store the distinct values, and by the `GROUP BY` operator to store
/// group values when they are a single string array.

pub struct ArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// Should the output be String or Binary?
    output_type: OutputType,
    /// Underlying hash set for each distinct value
    map: hashbrown::raw::RawTable<Entry<V>>,
    /// Total size of the map in bytes
    map_size: usize,
    view_buffers: Vec<Buffer>,
    views: Vec<u128>,
    /// random state used to generate hashes
    random_state: RandomState,
    /// buffer that stores hash values (reused across batches to save allocations)
    hashes_buffer: Vec<u64>,
    /// `(payload, null_index)` for the 'null' value, if any
    /// NOTE null_index is the logical index in the final array, not the index
    /// in the buffer
    null: Option<(V, usize)>,

    // the length of the input array. Used to determine if we want to gc the output array
    // to avoid holding the view_buffers too long.
    input_len: usize,
}

/// The size, in number of entries, of the initial hash table
const INITIAL_MAP_CAPACITY: usize = 128;

impl<V> ArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    pub fn new(output_type: OutputType) -> Self {
        Self {
            output_type,
            map: hashbrown::raw::RawTable::with_capacity(INITIAL_MAP_CAPACITY),
            map_size: 0,
            views: Vec::new(),
            view_buffers: Vec::new(),
            random_state: RandomState::new(),
            hashes_buffer: vec![],
            null: None,
            input_len: 0,
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
        // Sanity array type
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
        create_hashes(&[values.clone()], &self.random_state, batch_hashes)
            // hash is supported for all types and create_hashes only
            // returns errors for unsupported types
            .unwrap();

        // step 2: insert each value into the set, if not already present
        let values = values.as_byte_view::<B>();

        self.input_len += values.len();

        let buffer_offset = self.view_buffers.len();
        self.view_buffers.extend_from_slice(values.data_buffers());

        // Ensure lengths are equivalent
        assert_eq!(values.len(), batch_hashes.len());

        for (view_idx, (value, &hash)) in
            values.iter().zip(batch_hashes.iter()).enumerate()
        {
            // handle null value
            let Some(value) = value else {
                let payload = if let Some(&(payload, _offset)) = self.null.as_ref() {
                    payload
                } else {
                    let payload = make_payload_fn(None);
                    let null_index = self.views.len();
                    self.views.push(0);
                    self.null = Some((payload, null_index));
                    payload
                };
                observe_payload_fn(payload);
                continue;
            };

            // get the value as bytes
            let value: &[u8] = value.as_ref();

            let entry = self.map.get_mut(hash, |header| {
                let v = unsafe { self.views.get_unchecked(header.view_idx) };

                let len = *v as u32;
                if len as usize != value.len() {
                    return false;
                }

                // We should probably change arrow-rs to provide a
                // GenericByteViewArray::value_from_view() method
                let b = if len <= 12 {
                    unsafe {
                        GenericByteViewArray::<BinaryViewType>::inline_value(
                            v,
                            len as usize,
                        )
                    }
                } else {
                    let view = ByteView::from(*v);

                    let data = unsafe {
                        self.view_buffers.get_unchecked(view.buffer_index as usize)
                    };
                    let offset = view.offset as usize;
                    unsafe { data.get_unchecked(offset..offset + len as usize) }
                };
                b == value
            });

            let payload = if let Some(entry) = entry {
                entry.payload
            } else {
                // no existing value, make a new one.
                let payload = make_payload_fn(Some(value));
                let new_header = Entry { view_idx, payload };

                let view = if value.len() <= 12 {
                    unsafe { *values.views().get_unchecked(view_idx) }
                } else {
                    let v = unsafe { *values.views().get_unchecked(view_idx) };
                    let mut v = ByteView::from(v);
                    v.buffer_index += buffer_offset as u32;
                    v.as_u128()
                };
                self.views.push(view);
                self.map
                    .insert_accounted(new_header, |_| hash, &mut self.map_size);
                payload
            };
            observe_payload_fn(payload);
        }
    }

    /// Converts this set into a `StringViewArray`, or `BinaryViewArray`,
    /// containing each distinct value
    /// that was inserted. This is done without copying the values.
    ///
    /// The values are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn into_state(self) -> ArrayRef {
        // Only make a `NullBuffer` if there was a null value
        let nulls = self.null.map(|(_payload, null_index)| {
            let num_values = self.views.len();
            single_null_buffer(num_values, null_index)
        });

        let len = self.views.len();
        let b = ScalarBuffer::new(Buffer::from_vec(self.views), 0, len);
        match self.output_type {
            OutputType::BinaryView => {
                // SAFETY: the offsets were constructed correctly
                let mut array = unsafe {
                    BinaryViewArray::new_unchecked(b, self.view_buffers, nulls)
                };
                if array.len() < (self.input_len / 2) {
                    // arrow gc by default will deduplicate strings, it should not do that.
                    // todo: file a ticket to change it.
                    array = array.gc();
                }
                Arc::new(array)
            }
            OutputType::Utf8View => {
                // SAFETY:
                // 1. the offsets were constructed safely
                //
                // 2. we asserted the input arrays were all the correct type and
                // thus since all the values that went in were valid (e.g. utf8)
                // so are all the values that come out
                let mut array = unsafe {
                    StringViewArray::new_unchecked(b, self.view_buffers, nulls)
                };
                if array.len() < (self.input_len / 2) {
                    array = array.gc();
                }
                Arc::new(array)
            }
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
        // view buffers are from upstream string view, not technically from us.
        self.map_size + self.views.allocated_size() + self.hashes_buffer.allocated_size()
    }
}

/// Returns a `NullBuffer` with a single null value at the given index
fn single_null_buffer(num_values: usize, null_index: usize) -> NullBuffer {
    let mut bool_builder = BooleanBufferBuilder::new(num_values);
    bool_builder.append_n(num_values, true);
    bool_builder.set_bit(null_index, false);
    NullBuffer::from(bool_builder.finish())
}

impl<V> Debug for ArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowBytesMap")
            .field("map", &"<map>")
            .field("map_size", &self.map_size)
            .field("views", &self.views)
            .field("random_state", &self.random_state)
            .field("hashes_buffer", &self.hashes_buffer)
            .finish()
    }
}

/// Entry in the hash table -- see [`ArrowViewBytesMap`] for more details
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// The idx into the views array
    view_idx: usize,

    /// value stored by the entry
    payload: V,
}

#[cfg(test)]
mod tests {
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
}
