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

//! [`BlockedArrowBytesViewMap`] and [`BlockedArrowBytesViewSet`] for storing maps/sets of values from
//! `StringViewArray`/`BinaryViewArray`.

use crate::binary_map::OutputType;
use arrow::array::cast::AsArray;
use arrow::array::{
    Array, ArrayRef, BinaryViewArray, BooleanBufferBuilder, ByteView, NullBufferBuilder,
    make_view,
};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::{BinaryViewType, ByteViewType, DataType, StringViewType};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::proxy::{HashTableAllocExt, VecAllocExt};
use datafusion_expr_common::blocked_helpers::{
    BlockedBytesBufferBuilder, BlockedNullsBuilder, CopyItemBlockedVecBuilder,
};
use datafusion_expr_common::groups_accumulator::BlocksIndex;
use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;

/// HashSet optimized for storing string or binary values that can produce that
/// the final set as a `GenericBinaryViewArray` with minimal copies.
#[derive(Debug)]
pub struct BlockedArrowBytesViewSet(BlockedArrowBytesViewMap<()>);

impl BlockedArrowBytesViewSet {
    pub fn new(output_type: OutputType, block_size: usize) -> Self {
        Self(BlockedArrowBytesViewMap::new(output_type, block_size))
    }

    /// Inserts each value from `values` into the set
    pub fn insert(&mut self, values: &ArrayRef) {
        fn make_payload_fn(_value: Option<&[u8]>) {}
        fn observe_payload_fn(_payload: (), _index: BlocksIndex) {}
        self.0
            .insert_if_new(values, make_payload_fn, observe_payload_fn);
    }

    /// Return the contents of this map and replace it with a new empty map with
    /// the same output type
    pub fn take(&mut self) -> Self {
        let mut new_self = Self::new(self.0.output_type, self.0.block_size);
        std::mem::swap(self, &mut new_self);
        new_self
    }

    /// Converts this set into a `StringViewArray` or `BinaryViewArray`
    /// containing each distinct value that was interned.
    /// This is done without copying the values.
    pub fn take_block(&mut self) -> Option<ArrayRef> {
        self.0.take_block()
    }

    pub fn take_all(&mut self) -> Vec<ArrayRef> {
        self.0.take_all()
    }

    pub fn take_n(&mut self, n: usize) -> ArrayRef {
        self.0.take_n(n)
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
/// as `()`, as is done by [`BlockedArrowBytesViewSet`].
///
/// This map is used by the special `COUNT DISTINCT` aggregate function to
/// store the distinct values, and by the `GROUP BY` operator to store
/// group values when they are a single string array.
/// Max size of the in-progress buffer before flushing to completed buffers
const BYTE_VIEW_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;

pub struct BlockedArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// Should the output be StringView or BinaryView?
    output_type: OutputType,
    /// Underlying hash set for each distinct value
    map: hashbrown::hash_table::HashTable<Entry<V>>,
    /// Total size of the map in bytes
    map_size: usize,

    /// Views for all stored values (in insertion order)
    views: CopyItemBlockedVecBuilder<true, u128>,
    buffer: BlockedBytesBufferBuilder,
    num_buffer_blocks_per_block: Vec<usize>,
    /// First buffer block index of every views block, parallel to
    /// `num_buffer_blocks_per_block`. Looked up by `Entry::index` instead of
    /// storing it per entry, which keeps `Entry` at 32 bytes (2 per cache line).
    block_starts: Vec<usize>,

    /// random state used to generate hashes
    random_state: RandomState,
    /// buffer that stores hash values (reused across batches to save allocations)
    hashes_buffer: Vec<u64>,
    /// `(payload, null_index)` for the 'null' value, if any
    /// NOTE null_index is the logical index in the final array, not the index
    /// in the buffer
    null: Option<(V, BlocksIndex)>,

    block_size: usize,
    current_start_block_index: usize,
}

/// The size, in number of entries, of the initial hash table
const INITIAL_MAP_CAPACITY: usize = 512;

impl<V> BlockedArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    pub fn new(output_type: OutputType, block_size: usize) -> Self {
        let map = hashbrown::hash_table::HashTable::with_capacity(INITIAL_MAP_CAPACITY);
        let map_size = map.capacity() * size_of::<Entry<V>>();

        Self {
            output_type,
            map,
            map_size,
            views: CopyItemBlockedVecBuilder::new(block_size),
            buffer: BlockedBytesBufferBuilder::new(),
            // 1 empty block
            num_buffer_blocks_per_block: vec![1],
            block_starts: vec![0],
            random_state: RandomState::default(),
            hashes_buffer: vec![],
            null: None,
            block_size,
            current_start_block_index: 0,
        }
    }

    /// Return the contents of this map and replace it with a new empty map with
    /// the same output type
    pub fn take(&mut self) -> Self {
        let mut new_self = Self::new(self.output_type, self.block_size);
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
        OP: FnMut(V, BlocksIndex),
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
        }
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
        OP: FnMut(V, BlocksIndex),
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

        let block_size = self.block_size;
        for i in 0..values.len() {
            let view_u128 = input_views[i];
            let hash = self.hashes_buffer[i];

            // handle null value via validity bitmap check
            if values.is_null(i) {
                let (payload, null_index) = if let Some(&existing) = self.null.as_ref() {
                    existing
                } else {
                    let payload = make_payload_fn(None);
                    let null_index = self.next_index();
                    let should_start_new_block = self.views.push(0);

                    if should_start_new_block {
                        self.start_new_block();
                    }
                    self.null = Some((payload, null_index));
                    (payload, null_index)
                };
                observe_payload_fn(payload, null_index);
                continue;
            }

            // Extract length from the view (first 4 bytes of u128 in little-endian)
            let len = view_u128 as u32;

            // Check if value already exists
            let maybe_payload = {
                self.map
                    .find(hash, |header| {
                        if header.hash != hash {
                            return false;
                        }

                        // Fast path: inline strings can be compared directly
                        if len <= 12 {
                            return header.view == view_u128;
                        }

                        // For larger strings: first compare the 4-byte prefix
                        let stored_prefix = (header.view >> 32) as u32;
                        let input_prefix = (view_u128 >> 32) as u32;
                        if stored_prefix != input_prefix {
                            return false;
                        }

                        // Prefix matched - compare full bytes
                        let byte_view = ByteView::from(header.view);
                        let stored_len = byte_view.length as usize;
                        let buffer_index = byte_view.buffer_index as usize;
                        let offset = byte_view.offset as usize;

                        let start_block_index =
                            self.block_starts[header.index.block_index(block_size)];
                        let block = self.buffer.block(start_block_index + buffer_index);

                        let stored_value = &block[offset..offset + stored_len];
                        let input_value: &[u8] = values.value(i).as_ref();
                        stored_value == input_value
                    })
                    .map(|entry| (entry.payload, entry.index))
            };

            let (payload, index) = if let Some(existing) = maybe_payload {
                existing
            } else {
                // no existing value, make a new one
                let index = self.next_index();
                let (new_view, payload) = if len <= 12 {
                    // Inline path: bytes are already packed in view_u128.
                    // The inline ByteView format is [len:u32 LE][data:12 bytes zero-padded],
                    // so extracting bytes from the u128 avoids a round-trip through
                    // values.value(i) (which reads the views buffer and returns the same slice).
                    let view_bytes = view_u128.to_le_bytes();
                    let value = &view_bytes[4..4 + len as usize];
                    let payload = make_payload_fn(Some(value));
                    // For inline strings, the stored view is identical to the input view:
                    // make_view(value, 0, 0) produces the same u128 as view_u128.
                    //
                    // SAFETY: view_u128 was a valid view, and the enclosing `len <= 12`
                    // ensures it is inline
                    let new_view = unsafe { self.append_inline_view(view_u128) };
                    (new_view, payload)
                } else {
                    let value: &[u8] = values.value(i).as_ref();
                    let payload = make_payload_fn(Some(value));
                    let new_view = self.append_value(value);
                    (new_view, payload)
                };

                let new_header = Entry {
                    index,
                    view: new_view,
                    hash,
                    payload,
                };

                self.map
                    .insert_accounted(new_header, |h| h.hash, &mut self.map_size);
                (payload, index)
            };
            observe_payload_fn(payload, index);
        }
    }

    /// The position the next new value gets
    fn next_index(&self) -> BlocksIndex {
        BlocksIndex::from_index_in_fixed_block_size(self.views.len(), self.block_size)
    }

    fn build_null_buffer_with_null_at_index(buffer_len: usize, null_index: usize) -> NullBuffer {
        let mut nulls = BooleanBufferBuilder::new(buffer_len);
        nulls.append_n(buffer_len, true);
        nulls.set_bit(null_index, false);
        // SAFETY: sage as we just set a single bit as null
        let nulls = unsafe { NullBuffer::new_unchecked(nulls.finish(), 1) };
        nulls
    }

    /// Converts this set into a `StringViewArray`, or `BinaryViewArray`,
    /// containing each distinct value
    /// that was inserted. This is done without copying the values.
    ///
    /// The values are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn take_block(&mut self) -> Option<ArrayRef> {
        let views = self.views.take_block_finished()?;
        let null_buffer = match self.null.as_mut() {
            Some((_payload, null_index)) if null_index.is_in_block_0(self.block_size) => {
                Some(Self::build_null_buffer_with_null_at_index(views.len(), null_index.index_in_block(self.block_size)))
            }
            Some((_payload, null_index)) => {
                *null_index = null_index.prev_block(self.block_size);
                None
            }
            None => None,
        };

        if null_buffer.is_some() {
            self.null = None;
        }

        // The buffer blocks of a views block may be empty when all its values are inline
        let num_blocks = self
            .num_buffer_blocks_per_block
            .remove(0);
        let buffers = (0..num_blocks)
            .map(|_| Buffer::from(self.buffer.take_first_block()))
            .collect::<Vec<_>>();
        if self.num_buffer_blocks_per_block.is_empty() {
            self.num_buffer_blocks_per_block.push(1);
        }
        // The views block being written to may already span several buffer blocks
        self.current_start_block_index = self.buffer.num_blocks()
            - self
                .num_buffer_blocks_per_block
                .last()
                .expect("always has the current block");
        // The taken buffer blocks shift every remaining block index down
        self.block_starts.remove(0);
        for start in &mut self.block_starts {
            *start -= num_blocks;
        }
        if self.block_starts.is_empty() {
            self.block_starts.push(self.current_start_block_index);
        }

        if self.views.is_empty() {
            self.map.clear();
        } else {
            self.map.retain(|entry| {
                if let Some(index) = entry.index.prev_block_checked(self.block_size) {
                    entry.index = index;
                    true
                } else {
                    false
                }
            });
        }

        Some(self.build_output(views, buffers, null_buffer))
    }

    /// Take every block, the map is empty afterwards
    pub fn take_all(&mut self) -> Vec<ArrayRef> {
        // Build null buffer if we have any nulls
        let views_blocks = self.views.take_all();
        let number_of_buffers_per_block = std::mem::replace(
            &mut self.num_buffer_blocks_per_block,
            vec![1],
        );
        self.block_starts = vec![0];

        self.map.clear();

        let (null_block_index, null_index_in_block) = match self.null.take() {
            Some((_payload, null_index)) => {
                (null_index.block_index(self.block_size), null_index.index_in_block(self.block_size))
            }
            None => (usize::MAX, usize::MAX),
        };

        let mut output_blocks = Vec::with_capacity(views_blocks.len());

        for (index, (views, number_of_buffers)) in views_blocks.into_iter()
            .zip(number_of_buffers_per_block.into_iter()).enumerate()
        {
            let block_buffers = (0..number_of_buffers).map(|_| self.buffer.take_first_block()).collect::<Vec<_>>();
            assert_eq!(block_buffers.len(), number_of_buffers);

            let null_buffer = if null_block_index == index {
                Some(Self::build_null_buffer_with_null_at_index(views.len(), null_index_in_block))
            } else {
                None
            };

            let views = views.into_scalar_buffer();
            output_blocks.push(self.build_output(views, block_buffers, null_buffer));
        }

        self.current_start_block_index = 0;

        output_blocks
    }

    /// Take the first `n` values, `n` must be less than the block size
    ///
    /// The remaining values keep their order so their positions shift down by `n`,
    /// nothing is copied: the taken views and bytes are a prefix of the builders and
    /// are handed out as is, the remaining bytes are only re-blocked so that every
    /// views block still owns whole buffer blocks
    pub fn take_n(&mut self, n: usize) -> ArrayRef {
        assert!(
            n < self.block_size,
            "n ({n}) must be less than the block size ({})",
            self.block_size
        );
        assert!(n <= self.views.len(), "n ({n}) must be <= len ({})", self.views.len());
        let block_size = self.block_size;

        // Byte blocks are appended in value order, so the taken values own a prefix of the
        // bytes ending where the last non inline taken value ends, in block `b` of the
        // first views block whose buffer blocks start at 0
        let taken_views = self.views.take_n_fixed(n);
        let (b, end) = taken_views
            .as_slice()
            .iter()
            .rev()
            .find(|view| (**view as u32) > 12)
            .map(|view| {
                let view = ByteView::from(*view);
                (view.buffer_index as usize, (view.offset + view.length) as usize)
            })
            .unwrap_or((0, 0));

        let old_block_lens: Vec<usize> =
            (0..self.buffer.num_blocks()).map(|i| self.buffer.block(i).len()).collect();
        let taken_bytes = old_block_lens[..b].iter().sum::<usize>() + end;
        let remaining_bytes = self.buffer.len() - taken_bytes;

        // Old block boundaries that fall in the remaining bytes, relative to their start
        let mut old_boundaries = old_block_lens[b..]
            .iter()
            .scan(0usize, |acc, len| {
                *acc += len;
                Some(*acc - end)
            })
            .filter(|&boundary| boundary > 0 && boundary < remaining_bytes)
            .peekable();

        // New layout: a buffer block starts at every new views block and at every old
        // boundary that is kept, views are rewritten to point into it
        let mut starts: Vec<usize> = vec![];
        let mut per_views_block: Vec<usize> = Vec::new();
        let mut pos = 0usize;
        let mut first_block_of_current = 0usize;
        for (k, view) in self.views.as_mut_slice().iter_mut().enumerate() {
            if k % block_size == 0 {
                first_block_of_current = starts.len();
                starts.push(pos);
                per_views_block.push(1);
            }
            let len = *view as u32;
            if len <= 12 {
                continue;
            }
            while old_boundaries.peek().is_some_and(|&boundary| boundary <= pos) {
                let boundary = old_boundaries.next().unwrap();
                if boundary == pos && starts.last() != Some(&pos) {
                    starts.push(pos);
                    *per_views_block.last_mut().unwrap() += 1;
                }
            }
            let mut byte_view = ByteView::from(*view);
            byte_view.buffer_index = (starts.len() - 1 - first_block_of_current) as u32;
            byte_view.offset = (pos - starts[starts.len() - 1]) as u32;
            *view = byte_view.as_u128();
            pos += len as usize;
        }
        debug_assert_eq!(pos, remaining_bytes);
        // The views block being written to always owns a buffer block, even when empty
        if self.views.len().is_multiple_of(block_size) {
            starts.push(pos);
            per_views_block.push(1);
        }
        let sizes: Vec<usize> = starts
            .iter()
            .zip(starts.iter().skip(1).chain(std::iter::once(&remaining_bytes)))
            .map(|(start, next)| next - start)
            .collect();

        let mut buffers: Vec<Buffer> = (0..b).map(|_| self.buffer.take_first_block()).collect();
        let last = self.buffer.take_n(end, sizes.into_iter());
        if !last.is_empty() {
            buffers.push(last);
        }

        // First buffer block of every new views block
        let block_firsts: Vec<usize> = std::iter::once(0)
            .chain(per_views_block.iter().scan(0, |acc, c| {
                *acc += c;
                Some(*acc)
            }))
            .collect();
        self.block_starts = block_firsts[..per_views_block.len()].iter().copied().collect();
        self.num_buffer_blocks_per_block = per_views_block;
        self.current_start_block_index = self.buffer.num_blocks()
            - self.num_buffer_blocks_per_block.last().expect("always has the current block");

        let views = &self.views;
        self.map.retain(|entry| {
            if entry.index.lt_flat(n, block_size) {
                return false;
            }
            entry.index = entry.index.sub_flat(n, block_size);
            entry.view = views[entry.index];
            true
        });

        let null_buffer = match self.null {
            Some((_payload, null_index)) if null_index.lt_flat(n, block_size) => {
                self.null = None;
                Some(Self::build_null_buffer_with_null_at_index(
                    n,
                    null_index.into_index_in_fixed_block_size(block_size),
                ))
            }
            Some((payload, null_index)) => {
                self.null = Some((payload, null_index.sub_flat(n, block_size)));
                None
            }
            None => None,
        };

        self.build_output(taken_views.into_scalar_buffer(), buffers, null_buffer)
    }

    fn build_output(&self, views: ScalarBuffer<u128>, buffers: Vec<Buffer>, null_buffer: Option<NullBuffer>) -> ArrayRef {
        let array =
          unsafe { BinaryViewArray::new_unchecked(views, buffers, null_buffer) };

        match self.output_type {
            OutputType::BinaryView => Arc::new(array),
            OutputType::Utf8View => {
                let array = unsafe { array.to_string_view_unchecked() };
                Arc::new(array)
            }
            _ => unreachable!("Utf8/Binary should use `ArrowBytesMap`"),
        }
    }

    unsafe fn append_inline_view(&mut self, view: u128) -> u128 {
        let should_start_new_buffer = self.views.push(view);

        if should_start_new_buffer {
            self.start_new_block();
        }
        view
    }

    /// Append a value to our buffers and return the view pointing to it
    fn append_value(&mut self, value: &[u8]) -> u128 {
        let len = value.len();
        let view = if len <= 12 {
            make_view(value, 0, 0)
        } else {
            // Ensure buffer is big enough
            if self.buffer.current_block_len() + len > BYTE_VIEW_MAX_BLOCK_SIZE {
                self.buffer.start_new_block();
                let count = self.num_buffer_blocks_per_block.last_mut().unwrap();
                *count += 1;
                self.buffer
                    .reserve_bytes_in_current_block(BYTE_VIEW_MAX_BLOCK_SIZE);
            }

            let buffer_index =
                (self.num_buffer_blocks_per_block.last().unwrap() - 1) as u32;
            let offset = self.buffer.current_block_len() as u32;
            self.buffer.extend_from_slice(value);

            make_view(value, buffer_index, offset)
        };

        let should_start_new_block = self.views.push(view);

        if should_start_new_block {
            self.start_new_block();
        }
        view
    }

    fn start_new_block(&mut self) {
        self.num_buffer_blocks_per_block.push(1);
        self.buffer.start_new_block();
        self.current_start_block_index = self.buffer.num_blocks() - 1;
        self.block_starts.push(self.current_start_block_index);
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
    /// this set, not including `self` or input-array buffers.
    pub fn size(&self) -> usize {
        // All fields below own their allocations. Count retained capacity rather
        // than used length because this value drives memory accounting.
        self.map_size
            + self.num_buffer_blocks_per_block.allocated_size()
            + self.block_starts.allocated_size()
            + self.views.allocated_size()
            + self.buffer.allocated_size()
            + self.hashes_buffer.allocated_size()
    }
}

impl<V> Debug for BlockedArrowBytesViewMap<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowBytesMap")
            .field("map", &"<map>")
            .field("map_size", &self.map_size)
            .field("views_len", &self.views.len())
            .field("completed_buffers", &(self.buffer.num_blocks() - 1))
            .field("random_state", &self.random_state)
            .field("hashes_buffer", &self.hashes_buffer)
            .finish()
    }
}

/// Entry in the hash table -- see [`BlockedArrowBytesViewMap`] for more details
///
/// Stores the view pointing to our internal buffers, eliminating the need
/// for a separate builder index. For inline strings (<=12 bytes), the view
/// contains the entire value. For out-of-line strings, the view contains
/// buffer_index and offset pointing directly to our storage.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry<V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// The u128 view pointing to our internal buffers. For inline strings,
    /// this contains the complete value. For larger strings, this contains
    /// the buffer_index/offset into our completed/in_progress buffers.
    view: u128,

    /// Position of the value in the output blocks
    index: BlocksIndex,

    hash: u64,

    /// value stored by the entry
    payload: V,
}
//
// #[cfg(test)]
// mod tests {
//     use arrow::array::{GenericByteViewArray, StringViewArray};
//     use datafusion_common::HashMap;
//
//     use super::*;
//
//     // asserts that the set contains the expected strings, in the same order
//     fn assert_set(set: BlockedArrowBytesViewSet, expected: &[Option<&str>]) {
//         let strings = set.into_state();
//         let strings = strings.as_string_view();
//         let state = strings.into_iter().collect::<Vec<_>>();
//         assert_eq!(state, expected);
//     }
//
//     #[test]
//     fn string_view_set_empty() {
//         let mut set = BlockedArrowBytesViewSet::new(OutputType::Utf8View);
//         let array: ArrayRef = Arc::new(StringViewArray::new_null(0));
//         set.insert(&array);
//         assert_eq!(set.len(), 0);
//         assert_eq!(set.non_null_len(), 0);
//         assert_set(set, &[]);
//     }
//
//     #[test]
//     fn string_view_set_one_null() {
//         let mut set = BlockedArrowBytesViewSet::new(OutputType::Utf8View);
//         let array: ArrayRef = Arc::new(StringViewArray::new_null(1));
//         set.insert(&array);
//         assert_eq!(set.len(), 1);
//         assert_eq!(set.non_null_len(), 0);
//         assert_set(set, &[None]);
//     }
//
//     #[test]
//     fn string_view_set_many_null() {
//         let mut set = BlockedArrowBytesViewSet::new(OutputType::Utf8View);
//         let array: ArrayRef = Arc::new(StringViewArray::new_null(11));
//         set.insert(&array);
//         assert_eq!(set.len(), 1);
//         assert_eq!(set.non_null_len(), 0);
//         assert_set(set, &[None]);
//     }
//
//     #[test]
//     fn test_string_view_set_basic() {
//         // basic test for mixed small and large string values
//         let values = GenericByteViewArray::from(vec![
//             Some("a"),
//             Some("b"),
//             Some("CXCCCCCCCCAABB"), // 14 bytes
//             Some(""),
//             Some("cbcxx"), // 5 bytes
//             None,
//             Some("AAAAAAAA"),     // 8 bytes
//             Some("BBBBBQBBBAAA"), // 12 bytes
//             Some("a"),
//             Some("cbcxx"),
//             Some("b"),
//             Some("cbcxx"),
//             Some(""),
//             None,
//             Some("BBBBBQBBBAAA"),
//             Some("BBBBBQBBBAAA"),
//             Some("AAAAAAAA"),
//             Some("CXCCCCCCCCAABB"),
//         ]);
//
//         let mut set = BlockedArrowBytesViewSet::new(OutputType::Utf8View);
//         let array: ArrayRef = Arc::new(values);
//         set.insert(&array);
//         // values mut appear be in the order they were inserted
//         assert_set(
//             set,
//             &[
//                 Some("a"),
//                 Some("b"),
//                 Some("CXCCCCCCCCAABB"),
//                 Some(""),
//                 Some("cbcxx"),
//                 None,
//                 Some("AAAAAAAA"),
//                 Some("BBBBBQBBBAAA"),
//             ],
//         );
//     }
//
//     #[test]
//     fn test_string_set_non_utf8() {
//         // basic test for mixed small and large string values
//         let values = GenericByteViewArray::from(vec![
//             Some("a"),
//             Some("✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥"),
//             Some("🔥"),
//             Some("✨✨✨"),
//             Some("foobarbaz"),
//             Some("🔥"),
//             Some("✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥"),
//         ]);
//
//         let mut set = BlockedArrowBytesViewSet::new(OutputType::Utf8View);
//         let array: ArrayRef = Arc::new(values);
//         set.insert(&array);
//         // strings mut appear be in the order they were inserted
//         assert_set(
//             set,
//             &[
//                 Some("a"),
//                 Some("✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥✨🔥"),
//                 Some("🔥"),
//                 Some("✨✨✨"),
//                 Some("foobarbaz"),
//             ],
//         );
//     }
//
//     // Test use of binary output type
//     #[test]
//     fn test_binary_set() {
//         let v: Vec<Option<&[u8]>> = vec![
//             Some(b"a"),
//             Some(b"CXCCCCCCCCCCCCC"),
//             None,
//             Some(b"CXCCCCCCCCCCCCC"),
//         ];
//         let values: ArrayRef = Arc::new(BinaryViewArray::from(v));
//
//         let expected: Vec<Option<&[u8]>> =
//             vec![Some(b"a"), Some(b"CXCCCCCCCCCCCCC"), None];
//         let expected: ArrayRef = Arc::new(GenericByteViewArray::from(expected));
//
//         let mut set = BlockedArrowBytesViewSet::new(OutputType::BinaryView);
//         set.insert(&values);
//         assert_eq!(&set.into_state(), &expected);
//     }
//
//     // inserting strings into the set does not increase reported memory
//     #[test]
//     fn test_string_set_memory_usage() {
//         let strings1 = StringViewArray::from(vec![
//             Some("a"),
//             Some("b"),
//             Some("CXCCCCCCCCCCC"), // 13 bytes
//             Some("AAAAAAAA"),      // 8 bytes
//             Some("BBBBBQBBB"),     // 9 bytes
//         ]);
//         let total_strings1_len = strings1
//             .iter()
//             .map(|s| s.map(|s| s.len()).unwrap_or(0))
//             .sum::<usize>();
//         let values1: ArrayRef = Arc::new(StringViewArray::from(strings1));
//
//         // Much larger strings in strings2
//         let strings2 = StringViewArray::from(vec![
//             "FOO".repeat(1000),
//             "BAR larger than 12 bytes.".repeat(100_000),
//             "more unique.".repeat(1000),
//             "more unique2.".repeat(1000),
//             "FOO".repeat(3000),
//         ]);
//         let total_strings2_len = strings2
//             .iter()
//             .map(|s| s.map(|s| s.len()).unwrap_or(0))
//             .sum::<usize>();
//         let values2: ArrayRef = Arc::new(StringViewArray::from(strings2));
//
//         let mut set = BlockedArrowBytesViewSet::new(OutputType::Utf8View);
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
//         assert_eq!(set.len(), 5);
//
//         // inserting the large strings should increase the reported size
//         set.insert(&values2);
//         let size_after_values2 = set.size();
//         assert!(size_after_values2 > size_after_values1);
//
//         assert_eq!(set.len(), 10);
//     }
//
//     #[test]
//     fn test_size_counts_initial_hash_table_capacity() {
//         let map = BlockedArrowBytesViewMap::<()>::new(OutputType::Utf8View);
//
//         assert_eq!(map.size(), map.map.capacity() * size_of::<Entry<()>>());
//     }
//
//     #[test]
//     fn test_size_counts_retained_buffer_capacities() {
//         let first = "a".repeat(BYTE_VIEW_MAX_BLOCK_SIZE / 2 + 1);
//         let second = "b".repeat(BYTE_VIEW_MAX_BLOCK_SIZE / 2 + 1);
//         let third = "c".repeat(BYTE_VIEW_MAX_BLOCK_SIZE / 2 + 1);
//         let values: ArrayRef = Arc::new(StringViewArray::from(vec![
//             first.as_str(),
//             second.as_str(),
//             third.as_str(),
//         ]));
//
//         let mut map = BlockedArrowBytesViewMap::new(OutputType::Utf8View);
//         map.insert_if_new(&values, |_| (), |_| {});
//
//         // Make unused vector capacity explicit; the completed buffers were created
//         // by the map's flush path.
//         map.views.shrink_to_fit();
//         map.views.reserve_exact(1);
//         map.completed.shrink_to_fit();
//         map.completed.reserve_exact(1);
//
//         // The map owns these allocations; `values` and its Arrow buffers remain external.
//         assert!(map.views.capacity() > map.views.len());
//         assert!(map.completed.capacity() > map.completed.len());
//         assert!(
//             map.completed
//                 .iter()
//                 .any(|buffer| buffer.capacity() > buffer.len())
//         );
//
//         let expected_size = map.map_size
//             + map.views.allocated_size()
//             + map.buffer.allocated_size()
//             + map.completed.allocated_size()
//             + map.completed.iter().map(Buffer::capacity).sum::<usize>()
//             + map.nulls.allocated_size()
//             + map.hashes_buffer.allocated_size();
//         assert_eq!(map.size(), expected_size);
//
//         // Verify the retained-capacity delta independently from the production formula.
//         let legacy_size = map.map_size
//             + map.views.len() * size_of::<u128>()
//             + map.buffer.capacity()
//             + map.completed.iter().map(Buffer::len).sum::<usize>()
//             + map.nulls.allocated_size()
//             + map.hashes_buffer.allocated_size();
//         let retained_capacity_delta = (map.views.capacity() - map.views.len())
//             * size_of::<u128>()
//             + map.completed.capacity() * size_of::<Buffer>()
//             + map
//                 .completed
//                 .iter()
//                 .map(|buffer| buffer.capacity() - buffer.len())
//                 .sum::<usize>();
//         assert_eq!(map.size() - legacy_size, retained_capacity_delta);
//
//         let size_after_insert = map.size();
//         map.insert_if_new(&values, |_| (), |_| {});
//         assert_eq!(map.size(), size_after_insert);
//     }
//
//     #[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
//     struct TestPayload {
//         // store the string value to check against input
//         index: usize, // store the index of the string (each new string gets the next sequential input)
//     }
//
//     /// Wraps an [`BlockedArrowBytesViewMap`], validating its invariants
//     struct TestMap {
//         map: BlockedArrowBytesViewMap<TestPayload>,
//         // stores distinct strings seen, in order
//         strings: Vec<Option<String>>,
//         // map strings to index in strings
//         indexes: HashMap<Option<String>, usize>,
//     }
//
//     impl TestMap {
//         /// creates a map with TestPayloads for the given strings and then
//         /// validates the payloads
//         fn new() -> Self {
//             Self {
//                 map: BlockedArrowBytesViewMap::new(OutputType::Utf8View),
//                 strings: vec![],
//                 indexes: HashMap::new(),
//             }
//         }
//
//         /// Inserts strings into the map
//         fn insert(&mut self, strings: &[Option<&str>]) {
//             let string_array = StringViewArray::from(strings.to_vec());
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
//             let expected: ArrayRef = Arc::new(StringViewArray::from(strings));
//             assert_eq!(&arr, &expected);
//             arr
//         }
//     }
//
//     #[test]
//     fn test_map() {
//         let input = vec![
//             // Note mix of short/long strings
//             Some("A"),
//             Some("bcdefghijklmnop1234567"),
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
//         let expected_output: ArrayRef = Arc::new(StringViewArray::from(input));
//         assert_eq!(&test_map.into_array(), &expected_output);
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringViewArray;

    type Map = BlockedArrowBytesViewMap<()>;

    /// Inserts `values` and returns the position reported for every row
    fn positions(map: &mut Map, values: &[Option<&str>]) -> Vec<usize> {
        let array: ArrayRef = Arc::new(StringViewArray::from(values.to_vec()));
        let block_size = map.block_size;
        let mut out = vec![];
        map.insert_if_new(
            &array,
            |_| (),
            |(), index| out.push(index.into_index_in_fixed_block_size(block_size)),
        );
        out
    }

    fn strings(array: &ArrayRef) -> Vec<Option<String>> {
        array
            .as_string_view()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    fn owned(values: &[Option<&str>]) -> Vec<Option<String>> {
        values.iter().map(|v| v.map(str::to_string)).collect()
    }

    #[test]
    fn positions_stay_correct_after_take_block_and_take_n() {
        // mix of inline (<= 12 bytes, including exactly 12), long and null values
        let values = [
            Some("a"),
            Some("exactly12byt"),
            None,
            Some("a long value that is not inline"),
            Some(""),
            Some("b"),
            Some("another long value that is not inline"),
        ];
        let mut map = Map::new(OutputType::Utf8View, 3);
        assert_eq!(positions(&mut map, &values), [0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(map.len(), 7);

        // seen values keep their position, new ones get the next
        assert_eq!(
            positions(&mut map, &[Some("b"), None, Some("c")]),
            [5, 2, 7]
        );

        // emit the first block, everything shifts down by a block
        let block = map.take_block().unwrap();
        assert_eq!(strings(&block), owned(&values[..3]));
        assert_eq!(map.len(), 5);
        assert_eq!(
            positions(
                &mut map,
                &[
                    Some("a long value that is not inline"),
                    Some("exactly12byt"),
                    None,
                    Some("c"),
                    Some("b"),
                ]
            ),
            [0, 5, 6, 4, 2]
        );

        // emit the first 2, the rest shifts down by 2
        let block = map.take_n(2);
        assert_eq!(
            strings(&block),
            owned(&[Some("a long value that is not inline"), Some("")])
        );
        assert_eq!(map.len(), 5);
        assert_eq!(
            positions(
                &mut map,
                &[
                    Some("b"),
                    Some("another long value that is not inline"),
                    Some("c"),
                    Some("exactly12byt"),
                    None,
                ]
            ),
            [0, 1, 2, 3, 4]
        );

        let blocks = map.take_all();
        let all: Vec<Option<String>> = blocks.iter().flat_map(strings).collect();
        assert_eq!(
            all,
            owned(&[
                Some("b"),
                Some("another long value that is not inline"),
                Some("c"),
                Some("exactly12byt"),
                None,
            ])
        );
        assert!(map.is_empty());
        assert!(map.take_block().is_none());
        assert!(map.take_all().is_empty());
    }

    #[test]
    fn blocks_without_bytes_and_null_only_last_block() {
        // only inline values, the byte buffers stay empty
        let mut map = Map::new(OutputType::Utf8View, 2);
        assert_eq!(
            positions(&mut map, &[Some("x"), Some("y"), None]),
            [0, 1, 2]
        );
        let blocks = map.take_all();
        assert_eq!(blocks.len(), 2);
        assert_eq!(strings(&blocks[0]), owned(&[Some("x"), Some("y")]));
        assert_eq!(strings(&blocks[1]), vec![None]);

        // a null seen again after its block was emitted is a new group
        let mut map = Map::new(OutputType::Utf8View, 2);
        positions(&mut map, &[None, Some("x")]);
        assert_eq!(
            strings(&map.take_block().unwrap()),
            owned(&[None, Some("x")])
        );
        assert_eq!(positions(&mut map, &[None, Some("x")]), [0, 1]);
    }

    #[test]
    fn long_value_completing_a_block_is_found_again() {
        // with a block size of 2 every second value completes a views block, make those
        // the long ones so their bytes live in the block that was just completed
        let values: Vec<String> = (0..10)
            .map(|i| {
                if i % 2 == 1 {
                    format!("long value that is not inline {i}")
                } else {
                    format!("s{i}")
                }
            })
            .collect();
        let values: Vec<Option<&str>> = values.iter().map(|v| Some(v.as_str())).collect();
        let mut map = Map::new(OutputType::Utf8View, 2);
        assert_eq!(positions(&mut map, &values), (0..10).collect::<Vec<_>>());
        // every value must be found again, the long ones through their buffer block
        assert_eq!(positions(&mut map, &values), (0..10).collect::<Vec<_>>());
        assert_eq!(map.len(), 10);

        let blocks = map.take_all();
        let all: Vec<Option<String>> = blocks.iter().flat_map(strings).collect();
        assert_eq!(all, owned(&values));
    }

    #[test]
    fn long_values_spill_into_many_buffer_blocks() {
        let big = "z".repeat(BYTE_VIEW_MAX_BLOCK_SIZE / 2 + 1);
        let values: Vec<Option<&str>> = vec![
            Some(big.as_str()),
            Some("small"),
            Some("another long value that is not inline"),
            Some(big.as_str()),
        ];
        let mut map = Map::new(OutputType::Utf8View, 4);
        assert_eq!(positions(&mut map, &values), [0, 1, 2, 0]);

        // the same big value again must still be found in its buffer block
        let mut more = values.clone();
        more.push(Some("y"));
        assert_eq!(positions(&mut map, &more), [0, 1, 2, 0, 3]);

        let blocks = map.take_all();
        assert_eq!(blocks.len(), 1);
        assert_eq!(
            strings(&blocks[0]),
            owned(
                &values[..3]
                    .iter()
                    .copied()
                    .chain([Some("y")])
                    .collect::<Vec<_>>()
            )
        );
    }

    #[test]
    fn take_n_only_take_n() {
        // every distinct value the map has seen and not emitted, in order, is the model
        let big = "z".repeat(BYTE_VIEW_MAX_BLOCK_SIZE / 2 + 1);
        let value = |i: usize| -> Option<String> {
            match i % 7 {
                0 => None,
                1 => Some(String::new()),
                2 => Some(format!("in{i}")),
                3 => Some(format!("{big}{i}")),
                _ => Some(format!("long value that is not inline {i}")),
            }
        };
        for block_size in [1, 2, 3, 5, 8] {
            let mut map = Map::new(OutputType::Utf8View, block_size);
            let mut model: Vec<Option<String>> = vec![];
            let mut next = 0usize;
            let mut step = 0usize;
            while next < 60 {
                step += 1;
                // insert a batch that repeats some old values and adds new ones
                let batch: Vec<Option<String>> = (next.saturating_sub(4)..next + 1 + step % 6)
                    .map(value)
                    .collect();
                next += 1 + step % 6;
                let refs: Vec<Option<&str>> = batch.iter().map(|v| v.as_deref()).collect();
                let got = positions(&mut map, &refs);
                for (v, pos) in batch.iter().zip(got) {
                    let expected = match model.iter().position(|m| m == v) {
                        Some(pos) => pos,
                        None => {
                            model.push(v.clone());
                            model.len() - 1
                        }
                    };
                    assert_eq!(pos, expected, "block_size {block_size} step {step} value {v:?}");
                }
                assert_eq!(map.len(), model.len());

                match step % 3 {
                    0 => {
                        let n = (step * 7 % block_size).min(model.len());
                        let taken = map.take_n(n);
                        assert_eq!(strings(&taken), model.drain(..n).collect::<Vec<_>>());
                    }
                    1 if model.len() >= block_size => {
                        let taken = map.take_block().unwrap();
                        assert_eq!(strings(&taken), model.drain(..block_size).collect::<Vec<_>>());
                    }
                    _ => {}
                }
                assert_eq!(map.len(), model.len());
            }
            let all: Vec<Option<String>> = map.take_all().iter().flat_map(strings).collect();
            assert_eq!(all, model);
        }
    }
}
