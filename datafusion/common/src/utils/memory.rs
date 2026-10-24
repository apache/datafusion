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

//! This module provides a function to estimate the memory size of a HashTable prior to allocation

use crate::error::_exec_datafusion_err;
use crate::{HashMap, HashSet, Result};
use arrow::array::types::{
    ByteArrayType, ByteViewType, Int8Type, Int16Type, Int32Type, Int64Type,
    RunEndIndexType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::array::{
    Array, ArrayRef, AsArray, GenericByteArray, GenericByteViewArray, GenericListArray,
    GenericListViewArray, RunArray,
};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::downcast_primitive_array;
use arrow::record_batch::RecordBatch;
use std::mem::size_of;
use std::num::NonZero;
use std::sync::Arc;

/// Maximum number of distinct buffer IDs retained inline before promotion to
/// a heap-allocated map. Sixteen keeps small buffer sets allocation-free while
/// limiting linear lookup and inline storage to 16 entries.
/// This is a performance heuristic, not a semantic limit.
const INLINE_BUFFER_IDS: usize = 16;

/// Estimates the memory size required for a hash table prior to allocation.
///
/// # Parameters
/// - `num_elements`: The number of elements expected in the hash table.
/// - `fixed_size`: A fixed overhead size associated with the collection
///   (e.g., HashSet or HashTable).
/// - `T`: The type of elements stored in the hash table.
///
/// # Details
/// This function calculates the estimated memory size by considering:
/// - An overestimation of buckets to keep approximately 1/8 of them empty.
/// - The total memory size is computed as:
///   - The size of each entry (`T`) multiplied by the estimated number of
///     buckets.
///   - One byte overhead for each bucket.
///   - The fixed size overhead of the collection.
/// - If the estimation overflows, we return a [`crate::error::DataFusionError`]
///
/// # Examples
/// ---
///
/// ## From within a struct
///
/// ```rust
/// # use datafusion_common::utils::memory::estimate_memory_size;
/// # use datafusion_common::Result;
///
/// struct MyStruct<T> {
///     values: Vec<T>,
///     other_data: usize,
/// }
///
/// impl<T> MyStruct<T> {
///     fn size(&self) -> Result<usize> {
///         let num_elements = self.values.len();
///         let fixed_size =
///             std::mem::size_of_val(self) + std::mem::size_of_val(&self.values);
///
///         estimate_memory_size::<T>(num_elements, fixed_size)
///     }
/// }
/// ```
/// ---
/// ## With a simple collection
///
/// ```rust
/// # use datafusion_common::utils::memory::estimate_memory_size;
/// # use std::collections::HashMap;
///
/// let num_rows = 100;
/// let fixed_size = std::mem::size_of::<HashMap<u64, u64>>();
/// let estimated_hashtable_size =
///     estimate_memory_size::<(u64, u64)>(num_rows, fixed_size)
///         .expect("Size estimation failed");
/// ```
pub fn estimate_memory_size<T>(num_elements: usize, fixed_size: usize) -> Result<usize> {
    // For the majority of cases hashbrown overestimates the bucket quantity
    // to keep ~1/8 of them empty. We take this factor into account by
    // multiplying the number of elements with a fixed ratio of 8/7 (~1.14).
    // This formula leads to over-allocation for small tables (< 8 elements)
    // but should be fine overall.
    num_elements
        .checked_mul(8)
        .and_then(|overestimate| {
            let estimated_buckets = (overestimate / 7).next_power_of_two();
            // + size of entry * number of buckets
            // + 1 byte for each bucket
            // + fixed size of collection (HashSet/HashTable)
            size_of::<T>()
                .checked_mul(estimated_buckets)?
                .checked_add(estimated_buckets)?
                .checked_add(fixed_size)
        })
        .ok_or_else(|| {
            _exec_datafusion_err!("usize overflow while estimating the number of buckets")
        })
}

/// Calculate total used memory of this batch.
///
/// This function is used to estimate the physical memory usage of the `RecordBatch`.
/// It only counts the memory of large data `Buffer`s, and ignores metadata like
/// types and pointers.
/// The implementation will add up all unique `Buffer`'s memory
/// size, due to:
/// - The data pointer inside `Buffer` are memory regions returned by global memory
///   allocator, those regions can't have overlap.
/// - The actual used range of `ArrayRef`s inside `RecordBatch` can have overlap
///   or reuse the same `Buffer`. For example: taking a slice from `Array`.
///
/// Example:
/// For a `RecordBatch` with two columns: `col1` and `col2`, two columns are pointing
/// to a sub-region of the same buffer.
///
/// ```text
/// {xxxxxxxxxxxxxxxxxxx} <--- buffer
///       ^    ^  ^    ^
///       |    |  |    |
/// col1->{    }  |    |
/// col2--------->{    }
/// ```
///
/// In the above case, `get_record_batch_memory_size` will return the size of
/// the buffer, instead of the sum of `col1` and `col2`'s actual memory size.
///
/// Note: [`RecordBatch::get_array_memory_size`] double counts the buffer
/// memory size if multiple arrays within the batch are sharing the same
/// `Buffer`, while this function counts each `Buffer` exactly once.
pub fn get_record_batch_memory_size(batch: &RecordBatch) -> usize {
    RecordBatchMemoryCounter::new().count_batch(batch)
}

/// Tracks the memory used by a sequence of [`RecordBatch`]es that may share
/// underlying buffers, counting each buffer exactly once.
///
/// Use this instead of [`get_record_batch_memory_size`] to account for the
/// total memory of a sequence of batches, e.g. when buffering the batches of
/// an input stream. Such batches can share buffers (for example, operators
/// like aggregates emit one large batch as multiple zero-copy slices), and
/// calling [`get_record_batch_memory_size`] per batch counts the shared
/// buffers once per batch, while this counter counts them exactly once. A
/// batch's buffers are kept alive by the batch even when only a sub-range is
/// referenced, so counting unique buffers in full reflects the memory the
/// batches actually retain.
///
/// This counter only adds. Use [`RecordBatchMemoryTracker`] when batches are
/// dropped one at a time and their memory must be released.
#[derive(Debug, Default)]
pub struct RecordBatchMemoryCounter {
    /// Start addresses of `Buffer`s that have already been counted (instead of
    /// actual used data region's pointer represented by current `Array`)
    counted_buffers: BufferIdSet,
    /// Array objects already counted by [`Self::count_batch_with_array_overhead`]
    counted_arrays: HashSet<usize>,
    /// Total memory of all counted allocations
    memory_usage: usize,
}

impl RecordBatchMemoryCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Count `batch`, returning the memory used by its buffers that have not
    /// been counted before.
    pub fn count_batch(&mut self, batch: &RecordBatch) -> usize {
        let previous_memory_usage = self.memory_usage;

        for array in batch.columns() {
            self.count_array_memory_size(array.as_ref());
        }

        self.memory_usage - previous_memory_usage
    }

    /// Count `array`, returning the memory used by its buffers that have not
    /// been counted before.
    pub fn count_array(&mut self, array: &dyn Array) -> usize {
        let previous_memory_usage = self.memory_usage;
        self.count_array_memory_size(array);
        self.memory_usage - previous_memory_usage
    }

    /// Counts unique buffers and Array objects retained by `batch`.
    ///
    /// This is useful for accounting a sequence of batches at an operator
    /// boundary. It counts buffers once, including buffers shared by multiple
    /// batches. Array-object overhead is deduplicated recursively when a shared
    /// child has the same `ArrayRef` identity; Arrow constructors that rebuild
    /// child array objects may conservatively count their object overhead again.
    pub fn count_batch_with_array_overhead(&mut self, batch: &RecordBatch) -> usize {
        let mut total_size = self.count_batch(batch);
        let mut array_overhead = 0;

        for array in batch.columns() {
            array_overhead += count_unique_array_object_memory_size(array, &mut |id| {
                self.counted_arrays.insert(id)
            });
        }

        total_size += array_overhead;
        self.memory_usage += array_overhead;
        total_size
    }

    /// Total memory of all counted allocations.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// Count the memory usage of `array` and its children recursively.
    fn count_array_memory_size(&mut self, array: &dyn Array) {
        visit_array_buffers(array, self);
    }
}

impl BufferVisitor for RecordBatchMemoryCounter {
    fn visit(&mut self, buffer: &Buffer) {
        if self.counted_buffers.insert(buffer.data_ptr().addr()) {
            self.memory_usage += buffer.capacity();
        }
    }
}

/// Like [`RecordBatchMemoryCounter`], but batches can also be released.
///
/// Operators that retain batches incrementally and drop them later (sort,
/// window, sort-merge join, TopK) count a batch when they retain it and
/// [`Self::uncount_batch`] it when they drop it. The tracker keeps a reference
/// count per buffer: a buffer's capacity is added when its count goes from 0
/// to 1 and subtracted when it returns to 0.
///
/// **Contract**: a batch must be uncounted *before* its buffers are dropped.
/// The tracker identifies buffers by their data-pointer address and does not
/// keep them alive.
///
/// ```text
/// ───────────────────────┬────────────────────────────
/// step                   │ memory_usage()
/// ───────────────────────┼────────────────────────────
/// count_batch(slice1)    │ 4 MB (parent buffer counted)
/// count_batch(slice2)    │ 4 MB (already counted)
/// uncount_batch(slice1)  │ 4 MB (slice2 still uses it)
/// uncount_batch(slice2)  │ 0
/// ───────────────────────┴────────────────────────────
/// ```
#[derive(Debug, Default)]
pub struct RecordBatchMemoryTracker {
    /// Reference count of each retained buffer, keyed by its start address
    counted_buffers: BufferIdMap,
    /// Reference count of each Array object counted by
    /// [`Self::count_batch_with_array_overhead`]
    counted_arrays: HashMap<usize, u32>,
    /// Total memory of all retained allocations
    memory_usage: usize,
}

impl RecordBatchMemoryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Count `batch`, returning the memory used by its buffers that were not
    /// already retained.
    pub fn count_batch(&mut self, batch: &RecordBatch) -> usize {
        let previous_memory_usage = self.memory_usage;
        for array in batch.columns() {
            self.count_array_memory_size(array.as_ref());
        }
        self.memory_usage - previous_memory_usage
    }

    /// Count `array`, returning the memory used by its buffers that were not
    /// already retained.
    pub fn count_array(&mut self, array: &dyn Array) -> usize {
        let previous_memory_usage = self.memory_usage;
        self.count_array_memory_size(array);
        self.memory_usage - previous_memory_usage
    }

    /// Like [`RecordBatchMemoryCounter::count_batch_with_array_overhead`].
    pub fn count_batch_with_array_overhead(&mut self, batch: &RecordBatch) -> usize {
        let mut total_size = self.count_batch(batch);
        let mut array_overhead = 0;

        for array in batch.columns() {
            array_overhead += count_unique_array_object_memory_size(array, &mut |id| {
                let count = self.counted_arrays.entry(id).or_insert(0);
                *count += 1;
                *count == 1
            });
        }

        total_size += array_overhead;
        self.memory_usage += array_overhead;
        total_size
    }

    /// Release the buffers of `batch`, returning the bytes freed: the sizes of
    /// buffers no other counted batch retains. Buffers that were never
    /// counted are ignored.
    pub fn uncount_batch(&mut self, batch: &RecordBatch) -> usize {
        let previous_memory_usage = self.memory_usage;
        for array in batch.columns() {
            self.uncount_array_memory_size(array.as_ref());
        }
        previous_memory_usage - self.memory_usage
    }

    /// Release the buffers of `array`, returning the bytes freed.
    ///
    /// See [`Self::uncount_batch`] for semantics.
    pub fn uncount_array(&mut self, array: &dyn Array) -> usize {
        let previous_memory_usage = self.memory_usage;
        self.uncount_array_memory_size(array);
        previous_memory_usage - self.memory_usage
    }

    /// Inverse of [`Self::count_batch_with_array_overhead`].
    pub fn uncount_batch_with_array_overhead(&mut self, batch: &RecordBatch) -> usize {
        let mut total_released = self.uncount_batch(batch);
        let mut array_overhead = 0;

        for array in batch.columns() {
            // Releases the overhead of each array object no other counted
            // batch still holds
            array_overhead +=
                count_unique_array_object_memory_size(array, &mut |id| match self
                    .counted_arrays
                    .get_mut(&id)
                {
                    Some(count) if *count > 1 => {
                        *count -= 1;
                        false
                    }
                    Some(_) => {
                        self.counted_arrays.remove(&id);
                        true
                    }
                    None => false,
                });
        }

        total_released += array_overhead;
        self.memory_usage -= array_overhead;
        total_released
    }

    /// Total memory of all retained allocations.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    fn count_array_memory_size(&mut self, array: &dyn Array) {
        visit_array_buffers(array, self);
    }

    fn uncount_array_memory_size(&mut self, array: &dyn Array) {
        visit_array_buffers(array, &mut Uncount(self));
    }
}

/// Counting visitor: retains each buffer once more.
impl BufferVisitor for RecordBatchMemoryTracker {
    fn visit(&mut self, buffer: &Buffer) {
        if self.counted_buffers.increment(buffer.data_ptr().addr()) {
            self.memory_usage += buffer.capacity();
        }
    }
}

/// Releasing visitor for [`RecordBatchMemoryTracker::uncount_array_memory_size`].
struct Uncount<'a>(&'a mut RecordBatchMemoryTracker);

impl BufferVisitor for Uncount<'_> {
    fn visit(&mut self, buffer: &Buffer) {
        if self.0.counted_buffers.decrement(buffer.data_ptr().addr()) {
            self.0.memory_usage -= buffer.capacity();
        }
    }
}

/// Receives each buffer found by [`visit_array_buffers`].
trait BufferVisitor {
    fn visit(&mut self, buffer: &Buffer);
}

/// Calls `visitor` on every buffer of `array` and its children, recursively.
fn visit_array_buffers(array: &dyn Array, visitor: &mut impl BufferVisitor) {
    if let Some(nulls) = array.nulls() {
        visitor.visit(nulls.buffer());
    }

    downcast_primitive_array! {
        array => visitor.visit(array.values().inner()),
        DataType::Null => {}
        DataType::Boolean => visitor.visit(array.as_boolean().values().inner()),
        DataType::Binary => visit_byte_array_buffers(array.as_binary::<i32>(), visitor),
        DataType::LargeBinary => visit_byte_array_buffers(array.as_binary::<i64>(), visitor),
        DataType::Utf8 => visit_byte_array_buffers(array.as_string::<i32>(), visitor),
        DataType::LargeUtf8 => visit_byte_array_buffers(array.as_string::<i64>(), visitor),
        DataType::BinaryView => {
            visit_byte_view_array_buffers(array.as_binary_view(), visitor)
        }
        DataType::Utf8View => visit_byte_view_array_buffers(array.as_string_view(), visitor),
        DataType::FixedSizeBinary(_) => visitor.visit(array.as_fixed_size_binary().values()),
        DataType::List(_) => visit_list_array_buffers(array.as_list::<i32>(), visitor),
        DataType::LargeList(_) => visit_list_array_buffers(array.as_list::<i64>(), visitor),
        DataType::ListView(_) => {
            visit_list_view_array_buffers(array.as_list_view::<i32>(), visitor)
        }
        DataType::LargeListView(_) => {
            visit_list_view_array_buffers(array.as_list_view::<i64>(), visitor)
        }
        DataType::FixedSizeList(_, _) => {
            visit_array_buffers(array.as_fixed_size_list().values().as_ref(), visitor)
        }
        DataType::Struct(_) => {
            for child in array.as_struct().columns() {
                visit_array_buffers(child.as_ref(), visitor);
            }
        }
        DataType::Union(_, _) => {
            let array = array.as_union();
            visitor.visit(array.type_ids().inner());
            if let Some(offsets) = array.offsets() {
                visitor.visit(offsets.inner());
            }
            for (type_id, _) in array.fields().iter() {
                visit_array_buffers(array.child(type_id).as_ref(), visitor);
            }
        }
        DataType::Dictionary(_, _) => {
            let array = array.as_any_dictionary();
            visit_array_buffers(array.keys(), visitor);
            visit_array_buffers(array.values().as_ref(), visitor);
        }
        DataType::Map(_, _) => {
            let array = array.as_map();
            visitor.visit(array.offsets().inner().inner());
            visit_array_buffers(array.entries(), visitor);
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => visit_run_array_buffers::<Int16Type>(array, visitor),
            DataType::Int32 => visit_run_array_buffers::<Int32Type>(array, visitor),
            DataType::Int64 => visit_run_array_buffers::<Int64Type>(array, visitor),
            _ => visit_array_data_buffers(&array.to_data(), visitor),
        },
        _ => visit_array_data_buffers(&array.to_data(), visitor),
    }
}

fn visit_byte_array_buffers<T: ByteArrayType>(
    array: &GenericByteArray<T>,
    visitor: &mut impl BufferVisitor,
) {
    visitor.visit(array.offsets().inner().inner());
    visitor.visit(array.values());
}

fn visit_byte_view_array_buffers<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    visitor: &mut impl BufferVisitor,
) {
    visitor.visit(array.views().inner());
    for buffer in array.data_buffers().iter() {
        visitor.visit(buffer);
    }
}

fn visit_list_array_buffers<O: arrow::array::OffsetSizeTrait>(
    array: &GenericListArray<O>,
    visitor: &mut impl BufferVisitor,
) {
    visitor.visit(array.offsets().inner().inner());
    visit_array_buffers(array.values().as_ref(), visitor);
}

fn visit_list_view_array_buffers<O: arrow::array::OffsetSizeTrait>(
    array: &GenericListViewArray<O>,
    visitor: &mut impl BufferVisitor,
) {
    visitor.visit(array.offsets().inner());
    visitor.visit(array.sizes().inner());
    visit_array_buffers(array.values().as_ref(), visitor);
}

fn visit_run_array_buffers<R: RunEndIndexType>(
    array: &dyn Array,
    visitor: &mut impl BufferVisitor,
) {
    if let Some(array) = array.as_any().downcast_ref::<RunArray<R>>() {
        visitor.visit(array.run_ends().inner().inner());
        visit_array_buffers(array.values().as_ref(), visitor);
    } else {
        visit_array_data_buffers(&array.to_data(), visitor);
    }
}

fn visit_array_data_buffers(
    array_data: &arrow::array::ArrayData,
    visitor: &mut impl BufferVisitor,
) {
    for buffer in array_data.buffers() {
        visitor.visit(buffer);
    }
    if let Some(nulls) = array_data.nulls() {
        visitor.visit(nulls.buffer());
    }
    for child in array_data.child_data() {
        visit_array_data_buffers(child, visitor);
    }
}

/// Counts the Array object memory of `array` and its children whose identity
/// `first_seen` reports as new; children of an already seen array are skipped.
fn count_unique_array_object_memory_size(
    array: &ArrayRef,
    first_seen: &mut impl FnMut(usize) -> bool,
) -> usize {
    let array_ptr = Arc::as_ptr(array).cast::<()>() as usize;
    if !first_seen(array_ptr) {
        return 0;
    }

    let children = array_children(array);
    let children_overhead: usize = children
        .iter()
        .map(|child| child.get_array_memory_size() - child.get_buffer_memory_size())
        .sum();
    let own_overhead = array.get_array_memory_size()
        - array.get_buffer_memory_size()
        - children_overhead;

    own_overhead
        + children
            .into_iter()
            .map(|child| count_unique_array_object_memory_size(child, first_seen))
            .sum::<usize>()
}

/// Returns the `ArrayRef` children whose object allocations may be shared.
fn array_children(array: &ArrayRef) -> Vec<&ArrayRef> {
    match array.data_type() {
        DataType::Struct(_) => array.as_struct().columns().iter().collect(),
        DataType::List(_) => vec![array.as_list::<i32>().values()],
        DataType::LargeList(_) => vec![array.as_list::<i64>().values()],
        DataType::ListView(_) => vec![array.as_list_view::<i32>().values()],
        DataType::LargeListView(_) => vec![array.as_list_view::<i64>().values()],
        DataType::FixedSizeList(_, _) => vec![array.as_fixed_size_list().values()],
        DataType::Map(_, _) => {
            let map = array.as_map();
            vec![map.keys(), map.values()]
        }
        DataType::Union(_, _) => {
            let union = array.as_union();
            union
                .fields()
                .iter()
                .map(|(type_id, _)| union.child(type_id))
                .collect()
        }
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => vec![array.as_dictionary::<Int8Type>().values()],
            DataType::Int16 => vec![array.as_dictionary::<Int16Type>().values()],
            DataType::Int32 => vec![array.as_dictionary::<Int32Type>().values()],
            DataType::Int64 => vec![array.as_dictionary::<Int64Type>().values()],
            DataType::UInt8 => vec![array.as_dictionary::<UInt8Type>().values()],
            DataType::UInt16 => vec![array.as_dictionary::<UInt16Type>().values()],
            DataType::UInt32 => vec![array.as_dictionary::<UInt32Type>().values()],
            DataType::UInt64 => vec![array.as_dictionary::<UInt64Type>().values()],
            _ => unreachable!("invalid dictionary key type: {key_type}"),
        },
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<RunArray<Int16Type>>()
                .map(|array| vec![array.values()])
                .unwrap_or_default(),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<RunArray<Int32Type>>()
                .map(|array| vec![array.values()])
                .unwrap_or_default(),
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<RunArray<Int64Type>>()
                .map(|array| vec![array.values()])
                .unwrap_or_default(),
            _ => vec![],
        },
        _ => vec![],
    }
}

/// Tracks a small number of buffers inline, avoiding a heap allocation for
/// typical batches, and promotes to a hash set when more buffers are seen.
#[derive(Debug)]
struct BufferIdSet {
    inline: [Option<NonZero<usize>>; INLINE_BUFFER_IDS],
    len: usize,
    overflow: Option<HashSet<NonZero<usize>>>,
}

impl Default for BufferIdSet {
    fn default() -> Self {
        Self {
            inline: [None; INLINE_BUFFER_IDS],
            len: 0,
            overflow: None,
        }
    }
}

impl BufferIdSet {
    fn insert(&mut self, buffer_id: NonZero<usize>) -> bool {
        if let Some(overflow) = &mut self.overflow {
            return overflow.insert(buffer_id);
        }

        if self.inline[..self.len].contains(&Some(buffer_id)) {
            return false;
        }

        if self.len < INLINE_BUFFER_IDS {
            self.inline[self.len] = Some(buffer_id);
            self.len += 1;
            return true;
        }

        let mut overflow = HashSet::with_capacity(INLINE_BUFFER_IDS + 1);
        overflow.extend(self.inline.iter().flatten().copied());
        let inserted = overflow.insert(buffer_id);
        self.overflow = Some(overflow);
        inserted
    }
}

/// Reference-counted buffer tracker with an inline fast path.
///
/// Tracks buffer addresses with their reference counts. The first
/// [`INLINE_BUFFER_IDS`] distinct buffers are stored in a fixed-size inline
/// array to avoid heap allocation for typical batches. When more buffers are
/// encountered, the tracker promotes to a heap-allocated `HashMap`.
///
/// The inline path stores addresses and counts in **separate** arrays so that
/// the hot `position()` scan over addresses touches a contiguous
/// `[Option<NonZero<usize>>; 16]` (128 bytes, two cache lines) without
/// interleaved count words, matching the layout of the previous insert-only
/// `BufferIdSet`.
#[derive(Debug)]
struct BufferIdMap {
    /// Inline storage: buffer addresses (None = empty slot).
    ids: [Option<NonZero<usize>>; INLINE_BUFFER_IDS],
    /// Inline storage: per-slot reference counts, parallel to `ids`.
    counts: [u32; INLINE_BUFFER_IDS],
    /// Number of occupied inline slots.
    len: usize,
    /// Overflow storage used when more than `INLINE_BUFFER_IDS` distinct
    /// buffers are tracked.
    overflow: Option<HashMap<NonZero<usize>, u32>>,
}

impl Default for BufferIdMap {
    fn default() -> Self {
        Self {
            ids: [None; INLINE_BUFFER_IDS],
            counts: [0; INLINE_BUFFER_IDS],
            len: 0,
            overflow: None,
        }
    }
}

impl BufferIdMap {
    /// Increment the reference count for `buffer_id`. Returns `true` when the
    /// count went from 0 to 1 (i.e., the buffer is newly tracked).
    #[inline(always)]
    fn increment(&mut self, buffer_id: NonZero<usize>) -> bool {
        if let Some(overflow) = &mut self.overflow {
            let count = overflow.entry(buffer_id).or_insert(0);
            *count += 1;
            return *count == 1;
        }

        // Search inline slots.
        if let Some(i) = self.ids[..self.len]
            .iter()
            .position(|id| *id == Some(buffer_id))
        {
            self.counts[i] += 1;
            return false; // Already tracked, count > 1 now.
        }

        // New buffer: try inline first.
        if self.len < INLINE_BUFFER_IDS {
            self.ids[self.len] = Some(buffer_id);
            self.counts[self.len] = 1;
            self.len += 1;
            return true;
        }

        // Promote to overflow.
        let mut overflow = HashMap::with_capacity(INLINE_BUFFER_IDS + 1);
        for i in 0..self.len {
            if let Some(id) = self.ids[i] {
                overflow.insert(id, self.counts[i]);
            }
        }
        overflow.insert(buffer_id, 1);
        self.overflow = Some(overflow);
        true
    }

    /// Decrement the reference count for `buffer_id`. Returns `true` when the
    /// count reached zero (i.e., the buffer is no longer tracked). Returns
    /// `false` if the buffer was not tracked or its count is still positive.
    fn decrement(&mut self, buffer_id: NonZero<usize>) -> bool {
        if let Some(overflow) = &mut self.overflow {
            if let Some(count) = overflow.get_mut(&buffer_id) {
                *count -= 1;
                if *count == 0 {
                    overflow.remove(&buffer_id);
                    return true;
                }
            }
            return false;
        }

        // Search inline slots.
        let target = Some(buffer_id);
        for i in 0..self.len {
            if self.ids[i] == target {
                self.counts[i] -= 1;
                if self.counts[i] == 0 {
                    // Swap-remove: move last entry here.
                    self.len -= 1;
                    if i < self.len {
                        self.ids[i] = self.ids[self.len];
                        self.counts[i] = self.counts[self.len];
                    }
                    self.ids[self.len] = None;
                    return true;
                }
                return false;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, mem::size_of};

    use super::estimate_memory_size;

    #[test]
    fn test_estimate_memory() {
        // size (bytes): 48
        let fixed_size = size_of::<HashSet<u32>>();

        // estimated buckets: 16 = (8 * 8 / 7).next_power_of_two()
        let num_elements = 8;
        // size (bytes): 128 = 16 * 4 + 16 + 48
        let estimated = estimate_memory_size::<u32>(num_elements, fixed_size).unwrap();
        assert_eq!(estimated, 128);

        // estimated buckets: 64 = (40 * 8 / 7).next_power_of_two()
        let num_elements = 40;
        // size (bytes): 368 = 64 * 4 + 64 + 48
        let estimated = estimate_memory_size::<u32>(num_elements, fixed_size).unwrap();
        assert_eq!(estimated, 368);
    }

    #[test]
    fn test_estimate_memory_overflow() {
        let num_elements = usize::MAX;
        let fixed_size = size_of::<HashSet<u32>>();
        let estimated = estimate_memory_size::<u32>(num_elements, fixed_size);

        assert!(estimated.is_err());
    }
}

#[cfg(test)]
mod record_batch_tests {
    use super::*;
    use arrow::array::{
        ArrayData, ArrayRef, BinaryViewArray, BooleanArray, DictionaryArray,
        Float64Array, Int16Array, Int32Array, Int64Array, LargeListViewArray, ListArray,
        ListViewArray, MapArray, RunArray, StringArray, StringViewArray, StructArray,
        UnionArray, new_null_array,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{
        DataType, Field, Fields, Int16Type, Int32Type, Int64Type, Schema, UnionFields,
        UnionMode,
    };
    use std::sync::Arc;

    fn array_data_memory_size(array: &dyn Array) -> usize {
        fn count(
            array_data: &ArrayData,
            counted_buffers: &mut HashSet<NonZero<usize>>,
            total_size: &mut usize,
        ) {
            for buffer in array_data.buffers() {
                if counted_buffers.insert(buffer.data_ptr().addr()) {
                    *total_size += buffer.capacity();
                }
            }
            if let Some(nulls) = array_data.nulls() {
                let buffer = nulls.inner().inner();
                if counted_buffers.insert(buffer.data_ptr().addr()) {
                    *total_size += buffer.capacity();
                }
            }
            for child in array_data.child_data() {
                count(child, counted_buffers, total_size);
            }
        }

        let mut total_size = 0;
        count(&array.to_data(), &mut HashSet::default(), &mut total_size);
        total_size
    }

    fn assert_array_memory_size_matches(array: &dyn Array) {
        let counted = RecordBatchMemoryCounter::new().count_array(array);
        assert_eq!(counted, array_data_memory_size(array));
        // The tracker counts the same, and uncounting releases everything.
        let mut tracker = RecordBatchMemoryTracker::new();
        assert_eq!(tracker.count_array(array), counted);
        assert_eq!(tracker.uncount_array(array), counted);
        assert_eq!(tracker.memory_usage(), 0);
    }

    #[test]
    fn test_count_array_counts_shared_buffers_once() {
        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let size = array_data_memory_size(&array);

        let mut counter = RecordBatchMemoryCounter::new();
        assert_eq!(counter.count_array(&array), size);
        // A slice shares the buffer that is already counted
        assert_eq!(counter.count_array(&array.slice(1, 2)), 0);
        assert_eq!(counter.memory_usage(), size);
    }

    #[test]
    fn test_get_record_batch_memory_size() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int32, true),
            Field::new("float64", DataType::Float64, false),
        ]));

        let int_array =
            Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let float64_array = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(int_array), Arc::new(float64_array)],
        )
        .unwrap();

        let size = get_record_batch_memory_size(&batch);
        assert_eq!(size, 60);
    }

    #[test]
    fn test_get_record_batch_memory_size_with_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int32, true),
            Field::new("float64", DataType::Float64, false),
        ]));

        let int_array = Int32Array::from(vec![None, Some(2), Some(3)]);
        let float64_array = Float64Array::from(vec![1.0, 2.0, 3.0]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(int_array), Arc::new(float64_array)],
        )
        .unwrap();

        let size = get_record_batch_memory_size(&batch);
        assert_eq!(size, 100);
    }

    #[test]
    fn test_get_record_batch_memory_size_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ints",
            DataType::Int32,
            false,
        )]));

        let int_array: Int32Array = Int32Array::from(vec![] as Vec<i32>);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(int_array)]).unwrap();

        let size = get_record_batch_memory_size(&batch);
        assert_eq!(size, 0, "Empty batch should have 0 memory size");
    }

    #[test]
    fn test_get_record_batch_memory_size_shared_buffer() {
        let original = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let slice1 = original.slice(0, 3);
        let slice2 = original.slice(2, 3);

        let schema_origin = Arc::new(Schema::new(vec![Field::new(
            "origin_col",
            DataType::Int32,
            false,
        )]));
        let batch_origin =
            RecordBatch::try_new(schema_origin, vec![Arc::new(original)]).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("slice1", DataType::Int32, false),
            Field::new("slice2", DataType::Int32, false),
        ]));

        let batch_sliced =
            RecordBatch::try_new(schema, vec![Arc::new(slice1), Arc::new(slice2)])
                .unwrap();

        let size_origin = get_record_batch_memory_size(&batch_origin);
        let size_sliced = get_record_batch_memory_size(&batch_sliced);

        assert_eq!(size_origin, size_sliced);
    }

    #[test]
    fn test_record_batch_memory_counter_array_overhead_shared_across_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int32, false),
            Field::new("floats", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(Float64Array::from(vec![1., 2., 3., 4., 5., 6.])),
            ],
        )
        .unwrap();

        let mut counter = RecordBatchMemoryCounter::new();
        assert_eq!(
            counter.count_batch_with_array_overhead(&batch),
            batch.get_array_memory_size()
        );
        assert_eq!(counter.count_batch_with_array_overhead(&batch), 0);
        assert_eq!(counter.memory_usage(), batch.get_array_memory_size());
    }

    #[test]
    fn test_record_batch_memory_counter_deduplicates_shared_nested_array_overhead() {
        let shared_child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let fields =
            Fields::from(vec![Arc::new(Field::new("value", DataType::Int32, false))]);
        let first = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::clone(&shared_child)],
            None,
        )) as _;
        let second = Arc::new(StructArray::new(
            fields,
            vec![Arc::clone(&shared_child)],
            None,
        )) as _;
        let batch =
            RecordBatch::try_from_iter(vec![("first", first), ("second", second)])
                .unwrap();

        let mut counter = RecordBatchMemoryCounter::new();
        counter.count_batch_with_array_overhead(&batch);

        assert_eq!(
            counter.memory_usage(),
            batch.get_array_memory_size() - shared_child.get_array_memory_size()
        );
    }

    fn assert_recursive_shared_child_memory(
        name: &str,
        first: ArrayRef,
        second: ArrayRef,
        shared_memory: usize,
    ) {
        let first_memory = first.get_array_memory_size();
        let second_memory = second.get_array_memory_size();
        let first_batch = RecordBatch::try_from_iter(vec![(name, first)]).unwrap();
        let second_batch = RecordBatch::try_from_iter(vec![(name, second)]).unwrap();
        let mut counter = RecordBatchMemoryCounter::new();

        assert_eq!(
            counter.count_batch_with_array_overhead(&first_batch),
            first_memory,
            "{name}: first batch"
        );
        assert_eq!(
            counter.count_batch_with_array_overhead(&second_batch),
            second_memory - shared_memory,
            "{name}: shared child"
        );
    }

    #[test]
    fn test_record_batch_memory_counter_deduplicates_shared_list_child() {
        let shared_child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, false));

        let first = Arc::new(ListArray::new(
            Arc::clone(&list_field),
            OffsetBuffer::new(vec![0, 3].into()),
            Arc::clone(&shared_child),
            None,
        ));
        let second = Arc::new(ListArray::new(
            list_field,
            OffsetBuffer::new(vec![0, 3].into()),
            Arc::clone(&shared_child),
            None,
        ));

        assert_recursive_shared_child_memory(
            "list",
            first,
            second,
            shared_child.get_array_memory_size(),
        );
    }

    fn map_with_shared_children(
        fields: &Fields,
        shared_key: &ArrayRef,
        shared_value: &ArrayRef,
    ) -> ArrayRef {
        Arc::new(
            MapArray::try_new(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(fields.clone()),
                    false,
                )),
                OffsetBuffer::new(vec![0, 3].into()),
                StructArray::new(
                    fields.clone(),
                    vec![Arc::clone(shared_key), Arc::clone(shared_value)],
                    None,
                ),
                None,
                false,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_record_batch_memory_counter_deduplicates_shared_map_children() {
        let shared_key: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
        let shared_value: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let fields = Fields::from(vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, false)),
        ]);

        let first = map_with_shared_children(&fields, &shared_key, &shared_value);
        let second = map_with_shared_children(&fields, &shared_key, &shared_value);

        assert_recursive_shared_child_memory(
            "map",
            first,
            second,
            shared_key.get_array_memory_size() + shared_value.get_array_memory_size(),
        );
    }

    #[test]
    fn test_record_batch_memory_counter_deduplicates_union_slice_child_overhead() {
        let shared_child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let fields: UnionFields =
            std::iter::once((0, Arc::new(Field::new("value", DataType::Int32, false))))
                .collect();
        let first = Arc::new(
            UnionArray::try_new(
                fields,
                vec![0, 0, 0].into(),
                Some(vec![0, 1, 2].into()),
                vec![Arc::clone(&shared_child)],
            )
            .unwrap(),
        ) as ArrayRef;
        let second = Arc::new(first.as_union().slice(1, 2)) as ArrayRef;
        let first_batch =
            RecordBatch::try_from_iter(vec![("union", Arc::clone(&first))]).unwrap();
        let second_batch =
            RecordBatch::try_from_iter(vec![("union", Arc::clone(&second))]).unwrap();
        let child_overhead =
            shared_child.get_array_memory_size() - shared_child.get_buffer_memory_size();
        let second_parent_overhead = second.get_array_memory_size()
            - second.get_buffer_memory_size()
            - child_overhead;
        let mut counter = RecordBatchMemoryCounter::new();

        assert_eq!(
            counter.count_batch_with_array_overhead(&first_batch),
            first.get_array_memory_size()
        );
        assert_eq!(
            counter.count_batch_with_array_overhead(&second_batch),
            second_parent_overhead
        );
    }

    #[test]
    fn test_record_batch_memory_counter_deduplicates_run_end_slice_child_overhead() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let first = Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![1, 2, 3]),
                values.as_ref(),
            )
            .unwrap(),
        ) as ArrayRef;
        let second = Arc::new(
            first
                .as_any()
                .downcast_ref::<RunArray<Int32Type>>()
                .unwrap()
                .slice(1, 2),
        ) as ArrayRef;
        let first_batch =
            RecordBatch::try_from_iter(vec![("run", Arc::clone(&first))]).unwrap();
        let second_batch =
            RecordBatch::try_from_iter(vec![("run", Arc::clone(&second))]).unwrap();
        let child_overhead =
            values.get_array_memory_size() - values.get_buffer_memory_size();
        let second_parent_overhead = second.get_array_memory_size()
            - second.get_buffer_memory_size()
            - child_overhead;
        let mut counter = RecordBatchMemoryCounter::new();

        assert_eq!(
            counter.count_batch_with_array_overhead(&first_batch),
            first.get_array_memory_size()
        );
        assert_eq!(
            counter.count_batch_with_array_overhead(&second_batch),
            second_parent_overhead
        );
    }

    #[test]
    fn test_record_batch_memory_counter_deduplicates_shared_dictionary_child() {
        let shared_child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let make_dictionary = || {
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 1, 2]),
                    Arc::clone(&shared_child),
                )
                .unwrap(),
            ) as ArrayRef
        };

        assert_recursive_shared_child_memory(
            "dictionary",
            make_dictionary(),
            make_dictionary(),
            shared_child.get_array_memory_size(),
        );
    }

    #[test]
    fn test_record_batch_memory_counter_buffer_shared_across_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ints",
            DataType::Int32,
            false,
        )]));

        let int_array = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(int_array)]).unwrap();
        let slices = [batch.slice(0, 2), batch.slice(2, 2), batch.slice(4, 2)];

        // Counting each slice individually counts the shared buffer once per slice
        let summed: usize = slices.iter().map(get_record_batch_memory_size).sum();
        assert_eq!(summed, 3 * get_record_batch_memory_size(&batch));

        // A counter shared across the batches counts it exactly once
        let mut counter = RecordBatchMemoryCounter::new();
        let deduped: usize = slices.iter().map(|slice| counter.count_batch(slice)).sum();
        assert_eq!(deduped, get_record_batch_memory_size(&batch));
        assert_eq!(counter.memory_usage(), get_record_batch_memory_size(&batch));
    }

    #[test]
    fn test_record_batch_memory_counter_promotes_buffer_set() {
        let fields = (0..=INLINE_BUFFER_IDS)
            .map(|index| Field::new(format!("col_{index}"), DataType::Int32, false))
            .collect::<Vec<_>>();
        let columns = (0..=INLINE_BUFFER_IDS)
            .map(|value| Arc::new(Int32Array::from(vec![value as i32])) as _)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        let mut counter = RecordBatchMemoryCounter::new();
        assert_eq!(
            counter.count_batch(&batch),
            (INLINE_BUFFER_IDS + 1) * size_of::<i32>()
        );
        assert!(counter.counted_buffers.overflow.is_some());
        assert_eq!(counter.count_batch(&batch), 0);
    }

    #[test]
    fn test_array_memory_size_matches_array_data_layouts() {
        let list_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let struct_fields = vec![Field::new("value", DataType::Int32, true)].into();
        let union_fields = UnionFields::try_new(
            vec![0],
            vec![Field::new("value", DataType::Int32, true)],
        )
        .unwrap();
        let map_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        ));
        let data_types = vec![
            DataType::Boolean,
            DataType::Int32,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(4),
            DataType::BinaryView,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::List(Arc::clone(&list_field)),
            DataType::LargeList(Arc::clone(&list_field)),
            DataType::ListView(Arc::clone(&list_field)),
            DataType::LargeListView(Arc::clone(&list_field)),
            DataType::FixedSizeList(Arc::clone(&list_field), 2),
            DataType::Struct(struct_fields),
            DataType::Union(union_fields, UnionMode::Dense),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Map(map_entries, false),
        ];

        for data_type in data_types {
            let array = new_null_array(&data_type, 3);
            assert_array_memory_size_matches(array.as_ref());
        }

        // Exercise the view-specific buffers with concrete, non-empty values.
        let view_arrays = [
            Arc::new(BinaryViewArray::from_iter_values([
                b"short".as_slice(),
                b"a payload longer than twelve bytes".as_slice(),
            ])) as ArrayRef,
            Arc::new(StringViewArray::from_iter_values([
                "short",
                "a payload longer than twelve bytes",
            ])) as ArrayRef,
            Arc::new(ListViewArray::from_iter_primitive::<Int32Type, _, _>([
                Some(vec![Some(1), Some(2)]),
                None,
                Some(vec![Some(3)]),
            ])) as ArrayRef,
            Arc::new(LargeListViewArray::from_iter_primitive::<Int32Type, _, _>(
                [Some(vec![Some(1), Some(2)]), None, Some(vec![Some(3)])],
            )) as ArrayRef,
        ];

        for array in view_arrays {
            assert_array_memory_size_matches(array.as_ref());
        }

        let run_values = StringArray::from(vec!["alpha", "beta"]);
        let run_arrays = [
            Arc::new(
                RunArray::<Int16Type>::try_new(
                    &Int16Array::from(vec![2_i16, 5]),
                    &run_values,
                )
                .unwrap(),
            ) as ArrayRef,
            Arc::new(
                RunArray::<Int32Type>::try_new(
                    &Int32Array::from(vec![2_i32, 5]),
                    &run_values,
                )
                .unwrap(),
            ) as ArrayRef,
            Arc::new(
                RunArray::<Int64Type>::try_new(
                    &Int64Array::from(vec![2_i64, 5]),
                    &run_values,
                )
                .unwrap(),
            ) as ArrayRef,
        ];

        for array in run_arrays {
            assert_array_memory_size_matches(array.as_ref());
        }
    }

    #[test]
    fn test_get_record_batch_memory_size_nested_array() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "nested_int",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                false,
            ),
            Field::new(
                "nested_int2",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                false,
            ),
        ]));

        let int_list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
        ]);

        let int_list_array2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(4), Some(5), Some(6)]),
        ]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(int_list_array), Arc::new(int_list_array2)],
        )
        .unwrap();

        let size = get_record_batch_memory_size(&batch);
        assert_eq!(size, 8208);
    }

    // ---- RecordBatchMemoryTracker ----

    #[test]
    fn test_uncount_two_slices_example_from_issue() {
        let array = Int32Array::from(vec![0; 1_000_000]); // 4 MB
        let schema =
            Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let slice1 = batch.slice(0, 500_000);
        let slice2 = batch.slice(500_000, 500_000);
        let buffer_size = get_record_batch_memory_size(&batch);

        let mut tracker = RecordBatchMemoryTracker::new();
        assert_eq!(tracker.count_batch(&slice1), buffer_size);
        assert_eq!(tracker.count_batch(&slice2), 0);
        // slice2 still holds the buffer
        assert_eq!(tracker.uncount_batch(&slice1), 0);
        assert_eq!(tracker.memory_usage(), buffer_size);
        assert_eq!(tracker.uncount_batch(&slice2), buffer_size);
        assert_eq!(tracker.memory_usage(), 0);
    }

    /// Uncounting one of two arrays that share buffers keeps exactly what the
    /// other one still holds.
    #[test]
    fn test_uncount_keeps_buffers_shared_with_counted_arrays() {
        let string_view = Arc::new(StringViewArray::from_iter_values([
            "short",
            "a payload longer than twelve bytes that spills",
            "another long payload that also spills to data buffers",
        ])) as ArrayRef;
        let dictionary_values: ArrayRef =
            Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let dictionary = |keys: Vec<i32>| {
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(keys),
                    Arc::clone(&dictionary_values),
                )
                .unwrap(),
            ) as ArrayRef
        };
        let list_values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let list = |offsets: Vec<i32>| {
            Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(DataType::Int32, false)),
                OffsetBuffer::new(offsets.into()),
                Arc::clone(&list_values),
                None,
            )) as ArrayRef
        };

        let cases = [
            (
                "view data buffers",
                string_view.slice(0, 1),
                string_view.slice(1, 2),
            ),
            (
                "dictionary values",
                dictionary(vec![0, 1]),
                dictionary(vec![2]),
            ),
            ("list child", list(vec![0, 2]), list(vec![0, 1, 4])),
        ];
        for (name, first, second) in cases {
            let mut tracker = RecordBatchMemoryTracker::new();
            tracker.count_array(first.as_ref());
            tracker.count_array(second.as_ref());
            tracker.uncount_array(first.as_ref());
            assert_eq!(
                tracker.memory_usage(),
                RecordBatchMemoryCounter::new().count_array(second.as_ref()),
                "{name}"
            );
            tracker.uncount_array(second.as_ref());
            assert_eq!(tracker.memory_usage(), 0, "{name}");
        }
    }

    #[test]
    fn test_uncount_with_overflow_promotion() {
        let arrays: Vec<ArrayRef> = (0..INLINE_BUFFER_IDS + 4)
            .map(|i| Arc::new(Int32Array::from(vec![i as i32])) as ArrayRef)
            .collect();

        let mut tracker = RecordBatchMemoryTracker::new();
        let counted: usize = arrays.iter().map(|a| tracker.count_array(a)).sum();
        assert!(tracker.counted_buffers.overflow.is_some());
        let released: usize = arrays.iter().rev().map(|a| tracker.uncount_array(a)).sum();
        assert_eq!(released, counted);
        assert_eq!(tracker.memory_usage(), 0);
    }

    #[test]
    fn test_uncount_untracked_array_releases_nothing() {
        let array = Int32Array::from(vec![1, 2, 3]);
        let mut tracker = RecordBatchMemoryTracker::new();
        assert_eq!(tracker.uncount_array(&array), 0);

        tracker.count_array(&array);
        tracker.uncount_array(&array);
        assert_eq!(tracker.uncount_array(&array), 0);
        assert_eq!(tracker.memory_usage(), 0);
    }

    /// Array overhead is released only when the last batch holding a shared
    /// `ArrayRef` is uncounted.
    #[test]
    fn test_uncount_array_overhead_while_still_held() {
        let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let b1 = RecordBatch::try_new(schema, vec![col]).unwrap();
        let b2 = b1.clone();

        let mut tracker = RecordBatchMemoryTracker::new();
        let full = tracker.count_batch_with_array_overhead(&b1);
        assert_eq!(tracker.count_batch_with_array_overhead(&b2), 0);
        assert_eq!(tracker.uncount_batch_with_array_overhead(&b1), 0);
        assert_eq!(tracker.memory_usage(), full);
        assert_eq!(tracker.uncount_batch_with_array_overhead(&b2), full);
    }

    /// A random sequence of counts and uncounts, checked against counting the
    /// arrays still held from scratch.
    #[test]
    fn test_randomized_count_uncount_sequence() {
        let int32 = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let string = Arc::new(StringArray::from(vec!["a", "bc", "def"])) as ArrayRef;
        let string_view = Arc::new(StringViewArray::from_iter_values([
            "short",
            "another long string that also spills to data buffers for sharing",
        ])) as ArrayRef;
        let dictionary = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1, 2]),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            )
            .unwrap(),
        ) as ArrayRef;
        let arrays = [
            int32.slice(0, 2),
            int32.slice(1, 2),
            Arc::clone(&string),
            string.slice(1, 2),
            string_view.slice(0, 1),
            string_view.slice(1, 1),
            dictionary.slice(0, 2),
            dictionary,
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
        ];

        let mut seed: u64 = 42;
        let mut next_rand = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            (seed >> 33) as usize
        };
        let mut tracker = RecordBatchMemoryTracker::new();
        let mut held = vec![0usize; arrays.len()];
        for op in 0..300 {
            let idx = next_rand() % arrays.len();
            if held[idx] > 0 && next_rand() % 2 == 0 {
                tracker.uncount_array(arrays[idx].as_ref());
                held[idx] -= 1;
            } else {
                tracker.count_array(arrays[idx].as_ref());
                held[idx] += 1;
            }

            let mut expected = RecordBatchMemoryCounter::new();
            for (array, &count) in arrays.iter().zip(&held) {
                if count > 0 {
                    expected.count_array(array.as_ref());
                }
            }
            assert_eq!(tracker.memory_usage(), expected.memory_usage(), "op {op}");
        }
    }
}
