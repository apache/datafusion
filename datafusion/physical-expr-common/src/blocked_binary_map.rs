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

//! [`BlockedArrowBytesMap`] and [`BlockedArrowBytesSet`] for storing maps/sets of values from
//! StringArray / LargeStringArray / BinaryArray / LargeBinaryArray.

use arrow::array::{
    Array, ArrayRef, GenericBinaryArray, GenericStringArray, NullBufferBuilder,
    OffsetSizeTrait,
    cast::AsArray,
    types::{ByteArrayType, GenericBinaryType, GenericStringType},
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::proxy::{HashTableAllocExt, VecAllocExt};
use std::any::type_name;
use std::fmt::Debug;
use std::mem::{size_of, swap};
use std::ops::Range;
use std::sync::Arc;
use datafusion_expr_common::blocked_helpers::{Block, BlockedBytesBufferBuilder, BlockedOffsetBufferBuilder};
use datafusion_expr_common::groups_accumulator::BlocksIndex;
use crate::binary_map::OutputType;

/// HashSet optimized for storing string or binary values that can produce that
/// the final set as a GenericStringArray with minimal copies.
#[derive(Debug)]
pub struct BlockedArrowBytesSet<O: OffsetSizeTrait>(BlockedArrowBytesMap<O, ()>);

impl<O: OffsetSizeTrait> BlockedArrowBytesSet<O> {
    pub fn new(output_type: OutputType, block_size: usize) -> Self {
        Self(BlockedArrowBytesMap::new(output_type, block_size))
    }

    /// Return the contents of this set and replace it with a new empty
    /// set with the same output type
    pub fn take(&mut self) -> Self {
        Self(self.0.take())
    }

    /// Inserts each value from `values` into the set
    pub fn insert(&mut self, values: &ArrayRef) {
        fn make_payload_fn(_value: Option<&[u8]>) {}
        fn observe_payload_fn(_payload: (), _index: BlocksIndex) {}
        self.0
            .insert_if_new(values, make_payload_fn, observe_payload_fn);
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

/// Optimized map for storing Arrow "bytes" types (`String`, `LargeString`,
/// `Binary`, and `LargeBinary`) values that can produce the set of keys on
/// output as `GenericBinaryArray` without copies.
///
/// Equivalent to `HashSet<String, V>` but with better performance if you need
/// to emit the keys as an Arrow `StringArray` / `BinaryArray`. For other
/// purposes it is the same as a `HashMap<String, V>`
///
/// # Generic Arguments
///
/// * `O`: OffsetSize (String/LargeString)
/// * `V`: payload type
///
/// # Description
///
/// This is a specialized HashMap with the following properties:
///
/// 1. Optimized for storing and emitting Arrow byte types  (e.g.
///    `StringArray` / `BinaryArray`) very efficiently by minimizing copying of
///    the string values themselves, both when inserting and when emitting the
///    final array.
///
///
/// 2. Retains the insertion order of entries in the final array. The values are
///    in the same order as they were inserted.
///
/// Note this structure can be used as a `HashSet` by specifying the value type
/// as `()`, as is done by [`BlockedArrowBytesSet`].
///
/// This map is used by the special `COUNT DISTINCT` aggregate function to
/// store the distinct values, and by the `GROUP BY` operator to store
/// group values when they are a single string array.
///
/// # Example
///
/// The following diagram shows how the map would store the four strings
/// "Foo", NULL, "Bar", "TheQuickBrownFox":
///
/// * `hashtable` stores entries for each distinct string that has been
///   inserted. The entries contain the payload as well as information about the
///   value (either an offset or the actual bytes, see `Entry` docs for more
///   details)
///
/// * `offsets` stores offsets into `buffer` for each distinct string value,
///   following the same convention as the offsets in a `StringArray` or
///   `LargeStringArray`.
///
/// * `buffer` stores the actual byte data
///
/// * `null`: stores the index and payload of the null value, in this case the
///   second value (index 1)
///
/// ```text
/// ┌───────────────────────────────────┐    ┌─────┐    ┌────┐
/// │                ...                │    │  0  │    │FooB│
/// │ ┌──────────────────────────────┐  │    │  0  │    │arTh│
/// │ │      <Entry for "Bar">       │  │    │  3  │    │eQui│
/// │ │            len: 3            │  │    │  3  │    │ckBr│
/// │ │   offset_or_inline: "Bar"    │  │    │  6  │    │ownF│
/// │ │         payload:...          │  │    │     │    │ox  │
/// │ └──────────────────────────────┘  │    │     │    │    │
/// │                ...                │    └─────┘    └────┘
/// │ ┌──────────────────────────────┐  │
/// │ │<Entry for "TheQuickBrownFox">│  │    offsets    buffer
/// │ │           len: 16            │  │
/// │ │     offset_or_inline: 6      │  │    ┌───────────────┐
/// │ │         payload: ...         │  │    │    Some(1)    │
/// │ └──────────────────────────────┘  │    │ payload: ...  │
/// │                ...                │    └───────────────┘
/// └───────────────────────────────────┘
///                                              null
///               HashTable
/// ```
///
/// # Entry Format
///
/// Entries stored in a [`BlockedArrowBytesMap`] represents a value that is either
/// stored inline or in the buffer
///
/// This helps the case where there are many short (less than 8 bytes) strings
/// that are the same (e.g. "MA", "CA", "NY", "TX", etc)
///
/// ```text
///                                                                ┌──────────────────┐
///                                                  ─ ─ ─ ─ ─ ─ ─▶│...               │
///                                                 │              │TheQuickBrownFox  │
///                                                                │...               │
///                                                 │              │                  │
///                                                                └──────────────────┘
///                                                 │               buffer of u8
///
///                                                 │
///                        ┌────────────────┬───────────────┬───────────────┐
///  Storing               │                │ starting byte │  length, in   │
///  "TheQuickBrownFox"    │   hash value   │   offset in   │  bytes (not   │
///  (long string)         │                │    buffer     │  characters)  │
///                        └────────────────┴───────────────┴───────────────┘
///                              8 bytes          8 bytes       4 or 8
///
///
///                         ┌───────────────┬─┬─┬─┬─┬─┬─┬─┬─┬───────────────┐
/// Storing "foobar"        │               │ │ │ │ │ │ │ │ │  length, in   │
/// (short string)          │  hash value   │?│?│f│o│o│b│a│r│  bytes (not   │
///                         │               │ │ │ │ │ │ │ │ │  characters)  │
///                         └───────────────┴─┴─┴─┴─┴─┴─┴─┴─┴───────────────┘
///                              8 bytes         8 bytes        4 or 8
/// ```
pub struct BlockedArrowBytesMap<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// Should the output be String or Binary?
    output_type: OutputType,
    /// Underlying hash set for each distinct value
    map: hashbrown::hash_table::HashTable<Entry<O, V>>,
    // /// Total size of the map in bytes
    // map_size: usize,
    /// In progress buffer containing all values
    buffer: BlockedBytesBufferBuilder,
    /// Offsets into `buffer` for each distinct  value. These offsets as used
    /// directly to create the final `GenericBinaryArray`. The `i`th string is
    /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    /// are stored as a zero length string.
    offsets: BlockedOffsetBufferBuilder<true, O>,
    /// random state used to generate hashes
    random_state: RandomState,
    /// buffer that stores hash values (reused across batches to save allocations)
    hashes_buffer: Vec<u64>,
    /// `(payload, null_index)` for the 'null' value, if any
    /// NOTE null_index is the logical index in the final array, not the index
    /// in the buffer
    null: Option<(V, BlocksIndex)>,
    block_size: usize,
}

/// The size, in number of entries, of the initial hash table
const INITIAL_MAP_CAPACITY: usize = 128;
/// The initial size, in bytes, of the string data
pub const INITIAL_BUFFER_CAPACITY: usize = 8 * 1024;
impl<O: OffsetSizeTrait, V> BlockedArrowBytesMap<O, V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    pub fn new(output_type: OutputType, block_size: usize) -> Self {
        let mut buffer = BlockedBytesBufferBuilder::new();
        buffer.reserve_bytes_in_current_block(INITIAL_BUFFER_CAPACITY);
        Self {
            output_type,
            block_size,
            map: hashbrown::hash_table::HashTable::with_capacity(INITIAL_MAP_CAPACITY),
            //  Vec::with_capacity(INITIAL_BUFFER_CAPACITY)
            buffer,
            offsets: BlockedOffsetBufferBuilder::new(block_size),
            random_state: RandomState::default(),
            hashes_buffer: vec![],
            null: None,
        }
    }

    /// Return the contents of this map and replace it with a new empty map with
    /// the same output type
    pub fn take(&mut self) -> Self {
        let mut new_self = Self::new(self.output_type, self.block_size);
        swap(self, &mut new_self);
        new_self
    }

    fn new_result(&self, offsets: Vec<O>, values: Vec<u8>, nulls: Option<NullBuffer>) -> ArrayRef {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let values = Buffer::from_vec(values);
        match self.output_type {
            OutputType::Binary => {
                // SAFETY: the offsets were constructed correctly
                Arc::new(unsafe {
                    GenericBinaryArray::new_unchecked(offsets, values, nulls)
                })
            }
            OutputType::Utf8 => {
                // SAFETY:
                // 1. the offsets were constructed safely
                //
                // 2. we asserted the input arrays were all the correct type and
                // thus since all the values that went in were valid (e.g. utf8)
                // so are all the values that come out
                Arc::new(unsafe {
                    GenericStringArray::new_unchecked(offsets, values, nulls)
                })
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
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
    /// `observe_payload_fn`: invoked once, for each value in `values`, with the
    /// corresponding payload value and the current position of the value in the
    /// output blocks. The position is kept up to date by `take_block` / `take_n`,
    /// the payload is not, so callers that need the group index must use the position.
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
        OP: FnMut(V, BlocksIndex),
    {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                assert!(matches!(
                    values.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.insert_if_new_inner::<MP, OP, GenericBinaryType<O>>(
                    values,
                    make_payload_fn,
                    observe_payload_fn,
                )
            }
            OutputType::Utf8 => {
                assert!(matches!(
                    values.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.insert_if_new_inner::<MP, OP, GenericStringType<O>>(
                    values,
                    make_payload_fn,
                    observe_payload_fn,
                )
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        };
    }

    /// Generic version of [`Self::insert_if_new`] that handles `ByteArrayType`
    /// (both String and Binary)
    ///
    /// Note this is the only function that is generic on [`ByteArrayType`], which
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
        OP: FnMut(V, BlocksIndex),
        B: ByteArrayType,
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
        let values = values.as_bytes::<B>();

        // Ensure lengths are equivalent
        assert_eq!(values.len(), batch_hashes.len());

        let num_blocks = self.buffer.num_blocks();

        for (value, &hash) in values.iter().zip(batch_hashes.iter()) {
            // handle null value
            let Some(value) = value else {
                let (payload, null_index) = if let Some(&existing) = self.null.as_ref() {
                    existing
                } else {
                    let payload = make_payload_fn(None);

                    let null_index = BlocksIndex::from_index_in_fixed_block_size(self.offsets.len(), self.block_size);
                    // nulls need a zero length in the offset buffer
                    let should_start_new_block = self.offsets.push_length(0);

                    if should_start_new_block {
                        self.buffer.start_new_block();
                    }
                    self.null = Some((payload, null_index));
                    (payload, null_index)
                };
                observe_payload_fn(payload, null_index);
                continue;
            };

            // get the value as bytes
            let value: &[u8] = value.as_ref();
            let value_len = O::usize_as(value.len());

            // value is "small"
            let (payload, index) = if value.len() <= SHORT_VALUE_LEN {
                let inline = value.iter().fold(0usize, |acc, &x| (acc << 8) | x as usize);

                // is value is already present in the set?
                let entry = self.map.find_mut(hash, |header| {
                    // compare value if hashes match
                    if header.hash != hash || header.len != value_len {
                        return false;
                    }
                    // value is stored inline so no need to consult buffer
                    // (this is the "small string optimization")
                    inline == header.offset_or_inline
                });

                if let Some(entry) = entry {
                    (entry.payload, entry.index)
                }
                // if no existing entry, make a new one
                else {
                    // Put the small values into buffer and offsets so it appears
                    // the output array, but store the actual bytes inline for
                    // comparison
                    self.buffer.extend_from_slice(value);
                    let index = BlocksIndex::from_index_in_fixed_block_size(self.offsets.len(), self.block_size);
                    let should_start_new_block = self.offsets.push_length(value.len());

                    if should_start_new_block {
                        self.buffer.start_new_block();
                    }
                    let payload = make_payload_fn(Some(value));
                    let new_header = Entry {
                        hash,
                        index,
                        len: value_len,
                        offset_or_inline: inline,
                        payload,
                    };
                    self.map.insert_unique(
                        new_header.hash,
                        new_header,
                        |header| header.hash,
                    );
                    (payload, index)
                }
            }
            // value is not "small"
            else {
                // Check if the value is already present in the set
                let entry = self.map.find_mut(hash, |header| {
                    // compare value if hashes match
                    if header.hash != hash {
                        return false;
                    }
                    // Need to compare the bytes in the buffer
                    // SAFETY: buffer is only appended to, and we correctly inserted values and offsets
                    let existing_value =
                        unsafe { self.buffer.block(header.index.block_index()).get_unchecked(header.range()) };
                    value == existing_value
                });

                if let Some(entry) = entry {
                    (entry.payload, entry.index)
                }
                // if no existing entry, make a new one
                else {
                    // Put the small values into buffer and offsets so it
                    // appears the output array, and store that offset
                    // so the bytes can be compared if needed
                    let offset = self.buffer.current_block_len(); // offset of start for data
                    self.buffer.extend_from_slice(value);
                    let index = BlocksIndex::from_index_in_fixed_block_size(self.offsets.len(), self.block_size);
                    let should_start_new_block = self.offsets.push_length(value.len());

                    if should_start_new_block {
                        self.buffer.start_new_block();
                    }

                    let payload = make_payload_fn(Some(value));
                    let new_header = Entry {
                        hash,
                        index,
                        len: value_len,
                        offset_or_inline: offset,
                        payload,
                    };
                    self.map.insert_unique(
                        new_header.hash,
                        new_header,
                        |header| header.hash,
                    );
                    (payload, index)
                }
            };
            observe_payload_fn(payload, index);
        }

        for block_index in num_blocks.saturating_sub(1)..self.buffer.num_blocks() {
            let modified_block = self.buffer.block(block_index);

            // Check for overflow in offsets (if more data was sent than can be represented)
            assert!(
                O::from_usize(modified_block.len()).is_some(),
                "Put {} bytes in buffer, more than can be represented by a {}",
                modified_block.len(),
                type_name::<O>()
            )
        }
    }

    /// Converts this set into a `StringArray`, `LargeStringArray`,
    /// `BinaryArray`, or `LargeBinaryArray` containing each distinct value
    /// that was inserted. This is done without copying the values.
    ///
    /// The values are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn take_all(&mut self) -> Vec<ArrayRef> {
        let offsets = self.offsets.take_all();
        // The bytes builder can not tell a trailing block of empty values (or just the null)
        // from an unused one, so take as many blocks as the offsets have
        let mut values = self.buffer.take_all();
        values.resize_with(offsets.len(), Vec::new);
        self.map = hashbrown::hash_table::HashTable::with_capacity(INITIAL_MAP_CAPACITY);
        // self.map_size = 0;

        let mut blocks = Vec::with_capacity(offsets.len());
        let mut into_iter = offsets.into_iter().zip(values.into_iter());

        if let Some((_, null_index)) = self.null.take() {
            if null_index.block_index() > 0 {
                for (offsets, values) in into_iter.by_ref().take(null_index.block_index()) {
                    blocks.push(
                        self.new_result(offsets, values, None)
                    );
                }
            }

            let (offsets, values) = into_iter.next().expect("must have since null exists");
            let num_values = offsets.len() - 1;
            blocks.push(
                self.new_result(offsets, values, Some(single_null_buffer(num_values, null_index.index_in_block())))
            );
        }

        for (offsets, values) in into_iter {
            blocks.push(
                self.new_result(offsets, values, None)
            );
        }

        blocks
    }

    /// Converts this set into a `StringArray`, `LargeStringArray`,
    /// `BinaryArray`, or `LargeBinaryArray` containing each distinct value
    /// that was inserted. This is done without copying the values.
    ///
    /// The values are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn take_block(&mut self) -> Option<ArrayRef> {
        let offsets = self.offsets.take_block()?;
        // The block may hold no bytes at all when every value in it is empty or the null
        let values = self.buffer.take_first_block();

        // Only make a `NullBuffer` if there was a null value
        let nulls = if self
          .null
          .as_ref()
          .is_some_and(|(_, block_index)| block_index.block_index() == 0)
        {
            self.null
              .take()
              .map(|(_payload, null_index)| {
                  let num_values = offsets.len() - 1;
                  single_null_buffer(num_values, null_index.index_in_block())
              })
        } else {
            if let Some((_, block_index)) = self.null.as_mut() {
                *block_index = block_index.prev_block();
            }
            None
        };

        if self.offsets.len() == 0 {
            self.map.clear();
            // self.map_size = 0;
            assert_eq!(self.null, None);
        } else {
            // Remove the element from the map that corresponds to the current emitted block
            self.map.retain(|entry| {
                if let Some(new_block_index) = entry.index.prev_block_checked() {
                    entry.index = new_block_index;
                    true
                } else {
                    false
                }
            });
        }

        Some(self.new_result(offsets, values, nulls))
    }

    /// Converts this set into a `StringArray`, `LargeStringArray`,
    /// `BinaryArray`, or `LargeBinaryArray` containing each distinct value
    /// that was inserted. This is done without copying the values.
    ///
    /// The values are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn take_n(&mut self, n: usize) -> ArrayRef {
        let offsets = self.offsets.take_n_fixed(n);
        let values = self.buffer.take_n(
            offsets[offsets.len() - 1].as_usize(),
            self
              .offsets
              .blocks_iter()
              .map(|block| block[block.len() - 1].as_usize()),
        );

        // Only make a `NullBuffer` if there was a null value
        let nulls = if self
          .null
          .as_ref()
          .is_some_and(|(_, block_index)| block_index.block_index() == 0 && block_index.index_in_block() < n)
        {
            self.null
              .take()
              .map(|(_payload, null_index)| {
                  let num_values = offsets.len() - 1;
                  single_null_buffer(num_values, null_index.index_in_block())
              })
        } else {
            if let Some((_, block_index)) = self.null.as_mut() {
                *block_index = block_index.sub_flat(n, self.block_size);
            }
            None
        };

        // Remove the element from the map that corresponds to the current emitted block
        self.map.retain(|entry| {
            if entry.index.block_index() > 0 || entry.index.index_in_block() >= n {
                entry.index = entry.index.sub_flat(n, self.block_size);
                // if not inline, need to update the offset to be in the new block or inside the block
                if entry.len.as_usize() > SHORT_VALUE_LEN {
                    entry.offset_or_inline = self.offsets[entry.index].as_usize();
                }
                true
            } else {
                false
            }
        });

        self.new_result(offsets, values, nulls)
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
          self.map.capacity() * size_of::<Entry<O, V>>()
            + self.buffer.allocated_size()
            + self.offsets.allocated_size()
            + VecAllocExt::allocated_size(&self.hashes_buffer)
    }
}

/// Returns a `NullBuffer` with a single null value at the given index
fn single_null_buffer(num_values: usize, null_index: usize) -> NullBuffer {
    let mut null_builder = NullBufferBuilder::new(num_values);
    null_builder.append_n_non_nulls(null_index);
    null_builder.append_null();
    null_builder.append_n_non_nulls(num_values - null_index - 1);
    // SAFETY: inner builder must be constructed
    null_builder.finish().unwrap()
}

impl<O: OffsetSizeTrait, V> Debug for BlockedArrowBytesMap<O, V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowBytesMap")
            .field("map", &"<map>")
            .field("buffer", &self.buffer)
            .field("random_state", &self.random_state)
            .field("hashes_buffer", &self.hashes_buffer)
            .finish()
    }
}

/// Maximum size of a value that can be inlined in the hash table
const SHORT_VALUE_LEN: usize = size_of::<usize>();

/// Entry in the hash table -- see [`BlockedArrowBytesMap`] for more details
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// hash of the value (stored to avoid recomputing it in hash table check)
    hash: u64,

    index: BlocksIndex,
    /// if len =< [`SHORT_VALUE_LEN`]: the data inlined
    /// if len > [`SHORT_VALUE_LEN`], the offset of where the data starts
    offset_or_inline: usize,
    /// length of the value, in bytes (use O here so we use only i32 for
    /// strings, rather 64 bit usize)
    len: O,
    /// value stored by the entry
    payload: V,
}

impl<O, V> Entry<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// returns self.offset..self.offset + self.len
    #[inline(always)]
    fn range(&self) -> Range<usize> {
        self.offset_or_inline..self.offset_or_inline + self.len.as_usize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    type Map = BlockedArrowBytesMap<i32, ()>;

    /// Inserts `values` and returns the position reported for every row
    fn positions(map: &mut Map, values: &[Option<&str>]) -> Vec<usize> {
        let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        let block_size = map.block_size;
        let mut out = vec![];
        map.insert_if_new(&array, |_| (), |(), index| {
            out.push(index.into_index_in_fixed_block_size(block_size))
        });
        out
    }

    fn strings(array: &ArrayRef) -> Vec<Option<String>> {
        array
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    fn owned(values: &[Option<&str>]) -> Vec<Option<String>> {
        values.iter().map(|v| v.map(str::to_string)).collect()
    }

    #[test]
    fn positions_stay_correct_after_take_block_and_take_n() {
        // mix of inline (<= 8 bytes, including exactly 8), long and null values
        let values = [
            Some("a"),
            Some("exactly8"),
            None,
            Some("a long value here"),
            Some(""),
            Some("b"),
            Some("another long value"),
        ];
        let mut map = Map::new(OutputType::Utf8, 3);
        assert_eq!(positions(&mut map, &values), [0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(map.len(), 7);

        // seen values keep their position, new ones get the next
        assert_eq!(positions(&mut map, &[Some("b"), None, Some("c")]), [5, 2, 7]);

        // emit the first block, everything shifts down by a block
        let block = map.take_block().unwrap();
        assert_eq!(strings(&block), owned(&values[..3]));
        assert_eq!(map.len(), 5);
        assert_eq!(
            positions(&mut map, &[Some("a long value here"), Some("exactly8"), None, Some("c"), Some("b")]),
            [0, 5, 6, 4, 2]
        );

        // emit the first 2, the rest shifts down by 2
        let block = map.take_n(2);
        assert_eq!(strings(&block), owned(&[Some("a long value here"), Some("")]));
        assert_eq!(map.len(), 5);
        assert_eq!(
            positions(&mut map, &[Some("b"), Some("another long value"), Some("c"), Some("exactly8"), None]),
            [0, 1, 2, 3, 4]
        );

        let blocks = map.take_all();
        let all: Vec<Option<String>> = blocks.iter().flat_map(strings).collect();
        assert_eq!(
            all,
            owned(&[Some("b"), Some("another long value"), Some("c"), Some("exactly8"), None])
        );
        assert!(map.is_empty());
        assert!(map.take_block().is_none());
    }

    #[test]
    fn last_block_with_only_a_null_is_emitted() {
        let mut map = Map::new(OutputType::Utf8, 2);
        assert_eq!(positions(&mut map, &[Some("x"), Some("y"), None]), [0, 1, 2]);

        let blocks = map.take_all();
        assert_eq!(blocks.len(), 2);
        assert_eq!(strings(&blocks[1]), vec![None]);

        // a last block that holds no bytes at all, only the null or only an empty value
        let mut map = Map::new(OutputType::Utf8, 2);
        positions(&mut map, &[Some("a"), Some("b"), None]);
        assert_eq!(strings(&map.take_block().unwrap()), owned(&[Some("a"), Some("b")]));
        assert_eq!(strings(&map.take_block().unwrap()), vec![None]);
        assert!(map.take_block().is_none());

        let mut map = Map::new(OutputType::Utf8, 2);
        positions(&mut map, &[Some("a"), Some("b"), Some("")]);
        assert_eq!(strings(&map.take_block().unwrap()), owned(&[Some("a"), Some("b")]));
        assert_eq!(strings(&map.take_block().unwrap()), owned(&[Some("")]));
        assert!(map.take_block().is_none());
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use arrow::array::{BinaryArray, LargeBinaryArray, StringArray};
//     use std::collections::HashMap;
//
//     #[test]
//     fn string_set_empty() {
//         let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
//         let array: ArrayRef = Arc::new(StringArray::new_null(0));
//         set.insert(&array);
//         assert_eq!(set.len(), 0);
//         assert_eq!(set.non_null_len(), 0);
//         assert_set(set, &[]);
//     }
//
//     #[test]
//     fn string_set_one_null() {
//         let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
//         let array: ArrayRef = Arc::new(StringArray::new_null(1));
//         set.insert(&array);
//         assert_eq!(set.len(), 1);
//         assert_eq!(set.non_null_len(), 0);
//         assert_set(set, &[None]);
//     }
//
//     #[test]
//     fn string_set_many_null() {
//         let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
//         let array: ArrayRef = Arc::new(StringArray::new_null(11));
//         set.insert(&array);
//         assert_eq!(set.len(), 1);
//         assert_eq!(set.non_null_len(), 0);
//         assert_set(set, &[None]);
//     }
//
//     #[test]
//     fn string_set_basic_i32() {
//         test_string_set_basic::<i32>();
//     }
//
//     #[test]
//     fn string_set_basic_i64() {
//         test_string_set_basic::<i64>();
//     }
//
//     fn test_string_set_basic<O: OffsetSizeTrait>() {
//         // basic test for mixed small and large string values
//         let values = GenericStringArray::<O>::from(vec![
//             Some("a"),
//             Some("b"),
//             Some("CXCCCCCCCC"), // 10 bytes
//             Some(""),
//             Some("cbcxx"), // 5 bytes
//             None,
//             Some("AAAAAAAA"),  // 8 bytes
//             Some("BBBBBQBBB"), // 9 bytes
//             Some("a"),
//             Some("cbcxx"),
//             Some("b"),
//             Some("cbcxx"),
//             Some(""),
//             None,
//             Some("BBBBBQBBB"),
//             Some("BBBBBQBBB"),
//             Some("AAAAAAAA"),
//             Some("CXCCCCCCCC"),
//         ]);
//
//         let mut set = ArrowBytesSet::<O>::new(OutputType::Utf8);
//         let array: ArrayRef = Arc::new(values);
//         set.insert(&array);
//         // values mut appear be in the order they were inserted
//         assert_set(
//             set,
//             &[
//                 Some("a"),
//                 Some("b"),
//                 Some("CXCCCCCCCC"),
//                 Some(""),
//                 Some("cbcxx"),
//                 None,
//                 Some("AAAAAAAA"),
//                 Some("BBBBBQBBB"),
//             ],
//         );
//     }
//
//     #[test]
//     fn string_set_non_utf8_32() {
//         test_string_set_non_utf8::<i32>();
//     }
//
//     #[test]
//     fn string_set_non_utf8_64() {
//         test_string_set_non_utf8::<i64>();
//     }
//
//     fn test_string_set_non_utf8<O: OffsetSizeTrait>() {
//         // basic test for mixed small and large string values
//         let values = GenericStringArray::<O>::from(vec![
//             Some("a"),
//             Some("✨🔥"),
//             Some("🔥"),
//             Some("✨✨✨"),
//             Some("foobarbaz"),
//             Some("🔥"),
//             Some("✨🔥"),
//         ]);
//
//         let mut set = ArrowBytesSet::<O>::new(OutputType::Utf8);
//         let array: ArrayRef = Arc::new(values);
//         set.insert(&array);
//         // strings mut appear be in the order they were inserted
//         assert_set(
//             set,
//             &[
//                 Some("a"),
//                 Some("✨🔥"),
//                 Some("🔥"),
//                 Some("✨✨✨"),
//                 Some("foobarbaz"),
//             ],
//         );
//     }
//
//     // asserts that the set contains the expected strings, in the same order
//     fn assert_set<O: OffsetSizeTrait>(set: ArrowBytesSet<O>, expected: &[Option<&str>]) {
//         let strings = set.into_state();
//         let strings = strings.as_string::<O>();
//         let state = strings.into_iter().collect::<Vec<_>>();
//         assert_eq!(state, expected);
//     }
//
//     // Test use of binary output type
//     #[test]
//     fn test_binary_set() {
//         let values: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
//             Some(b"a"),
//             Some(b"CXCCCCCCCC"),
//             None,
//             Some(b"CXCCCCCCCC"),
//         ]));
//
//         let expected: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
//             Some(b"a"),
//             Some(b"CXCCCCCCCC"),
//             None,
//         ]));
//
//         let mut set = ArrowBytesSet::<i32>::new(OutputType::Binary);
//         set.insert(&values);
//         assert_eq!(&set.into_state(), &expected);
//     }
//
//     // Test use of binary output type
//     #[test]
//     fn test_large_binary_set() {
//         let values: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(vec![
//             Some(b"a"),
//             Some(b"CXCCCCCCCC"),
//             None,
//             Some(b"CXCCCCCCCC"),
//         ]));
//
//         let expected: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(vec![
//             Some(b"a"),
//             Some(b"CXCCCCCCCC"),
//             None,
//         ]));
//
//         let mut set = ArrowBytesSet::<i64>::new(OutputType::Binary);
//         set.insert(&values);
//         assert_eq!(&set.into_state(), &expected);
//     }
//
//     #[test]
//     #[should_panic(
//         expected = "matches!(values.data_type(), DataType::Utf8 | DataType::LargeUtf8)"
//     )]
//     fn test_mismatched_types() {
//         // inserting binary into a set that expects strings should panic
//         let values: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(vec![Some(b"a")]));
//
//         let mut set = ArrowBytesSet::<i64>::new(OutputType::Utf8);
//         set.insert(&values);
//     }
//
//     #[test]
//     #[should_panic(expected = "byte array")]
//     fn test_mismatched_sizes() {
//         // inserting large strings into a set that expects small should panic
//         let values: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(vec![Some(b"a")]));
//
//         let mut set = ArrowBytesSet::<i32>::new(OutputType::Binary);
//         set.insert(&values);
//     }
//
//     // put more than 2GB in a string set and expect it to panic
//     #[test]
//     #[should_panic(
//         expected = "Put 2147483648 bytes in buffer, more than can be represented by a i32"
//     )]
//     fn test_string_overflow() {
//         let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
//         for value in ["a", "b", "c"] {
//             // 1GB strings, so 3rd is over 2GB and should panic
//             let arr: ArrayRef =
//                 Arc::new(StringArray::from_iter_values([value.repeat(1 << 30)]));
//             set.insert(&arr);
//         }
//     }
//
//     // inserting strings into the set does not increase reported memory
//     #[test]
//     fn test_string_set_memory_usage() {
//         let strings1 = GenericStringArray::<i32>::from(vec![
//             Some("a"),
//             Some("b"),
//             Some("CXCCCCCCCC"), // 10 bytes
//             Some("AAAAAAAA"),   // 8 bytes
//             Some("BBBBBQBBB"),  // 9 bytes
//         ]);
//         let total_strings1_len = strings1
//             .iter()
//             .map(|s| s.map(|s| s.len()).unwrap_or(0))
//             .sum::<usize>();
//         let values1: ArrayRef = Arc::new(GenericStringArray::<i32>::from(strings1));
//
//         // Much larger strings in strings2
//         let strings2 = GenericStringArray::<i32>::from(vec![
//             "FOO".repeat(1000),
//             "BAR".repeat(2000),
//             "BAZ".repeat(3000),
//         ]);
//         let total_strings2_len = strings2
//             .iter()
//             .map(|s| s.map(|s| s.len()).unwrap_or(0))
//             .sum::<usize>();
//         let values2: ArrayRef = Arc::new(GenericStringArray::<i32>::from(strings2));
//
//         let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
//         let size_empty = set.size();
//
//         set.insert(&values1);
//         let size_after_values1 = set.size();
//         assert!(size_empty < size_after_values1);
//         assert!(
//             size_after_values1 > total_strings1_len,
//             "expect {size_after_values1} to be more than {total_strings1_len}"
//         );
//         assert!(size_after_values1 < total_strings1_len + total_strings2_len);
//
//         // inserting the same strings should not affect the size
//         set.insert(&values1);
//         assert_eq!(set.size(), size_after_values1);
//
//         // inserting the large strings should increase the reported size
//         set.insert(&values2);
//         let size_after_values2 = set.size();
//         assert!(size_after_values2 > size_after_values1);
//         assert!(size_after_values2 > total_strings1_len + total_strings2_len);
//     }
//
//     #[test]
//     fn test_map() {
//         let input = vec![
//             // Note mix of short/long strings
//             Some("A"),
//             Some("bcdefghijklmnop"),
//             Some("X"),
//             Some("Y"),
//             None,
//             Some("qrstuvqxyzhjwya"),
//             Some("✨🔥"),
//             Some("🔥"),
//             Some("🔥🔥🔥🔥🔥🔥"),
//         ];
//
//         let mut test_map = TestMap::new();
//         test_map.insert(&input);
//         test_map.insert(&input); // put it in twice
//         let expected_output: ArrayRef = Arc::new(StringArray::from(input));
//         assert_eq!(&test_map.into_array(), &expected_output);
//     }
//
//     #[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
//     struct TestPayload {
//         // store the string value to check against input
//         index: usize, // store the index of the string (each new string gets the next sequential input)
//     }
//
//     /// Wraps an [`BlockedArrowBytesMap`], validating its invariants
//     struct TestMap {
//         map: BlockedArrowBytesMap<i32, TestPayload>,
//         // stores distinct strings seen, in order
//         strings: Vec<Option<String>>,
//         // map strings to index in strings
//         indexes: HashMap<Option<String>, usize>,
//     }
//
//     impl Debug for TestMap {
//         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//             f.debug_struct("TestMap")
//                 .field("map", &"...")
//                 .field("strings", &self.strings)
//                 .field("indexes", &self.indexes)
//                 .finish()
//         }
//     }
//
//     impl TestMap {
//         /// creates a map with TestPayloads for the given strings and then
//         /// validates the payloads
//         fn new() -> Self {
//             Self {
//                 map: BlockedArrowBytesMap::new(OutputType::Utf8),
//                 strings: vec![],
//                 indexes: HashMap::new(),
//             }
//         }
//
//         /// Inserts strings into the map
//         fn insert(&mut self, strings: &[Option<&str>]) {
//             let string_array = StringArray::from(strings.to_vec());
//             let arr: ArrayRef = Arc::new(string_array);
//
//             let mut next_index = self.indexes.len();
//             let mut actual_new_strings = vec![];
//             let mut actual_seen_indexes = vec![];
//             // update self with new values, keeping track of newly added values
//             for str in strings {
//                 let str = str.map(|s| s.to_string());
//                 let index = self.indexes.get(&str).cloned().unwrap_or_else(|| {
//                     actual_new_strings.push(str.clone());
//                     let index = self.strings.len();
//                     self.strings.push(str.clone());
//                     self.indexes.insert(str, index);
//                     index
//                 });
//                 actual_seen_indexes.push(index);
//             }
//
//             // insert the values into the map, recording what we did
//             let mut seen_new_strings = vec![];
//             let mut seen_indexes = vec![];
//             self.map.insert_if_new(
//                 &arr,
//                 |s| {
//                     let value = s
//                         .map(|s| String::from_utf8(s.to_vec()).expect("Non utf8 string"));
//                     let index = next_index;
//                     next_index += 1;
//                     seen_new_strings.push(value);
//                     TestPayload { index }
//                 },
//                 |payload| {
//                     seen_indexes.push(payload.index);
//                 },
//             );
//
//             assert_eq!(actual_seen_indexes, seen_indexes);
//             assert_eq!(actual_new_strings, seen_new_strings);
//         }
//
//         /// Call `self.map.into_array()` validating that the strings are in the same
//         /// order as they were inserted
//         fn into_array(self) -> ArrayRef {
//             let Self {
//                 map,
//                 strings,
//                 indexes: _,
//             } = self;
//
//             let arr = map.into_state();
//             let expected: ArrayRef = Arc::new(StringArray::from(strings));
//             assert_eq!(&arr, &expected);
//             arr
//         }
//     }
// }
