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
use crate::{HashSet, Result};
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
/// a [`HashSet`]. Sixteen keeps small buffer sets allocation-free while
/// limiting linear lookup and inline storage to 16 pointer-sized entries.
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
            array_overhead +=
                count_unique_array_object_memory_size(array, &mut self.counted_arrays);
        }

        total_size += array_overhead;
        self.memory_usage += array_overhead;
        total_size
    }

    /// Total memory of all counted allocations.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    fn count_buffer_memory_size(&mut self, buffer: &Buffer) {
        if self.counted_buffers.insert(buffer.data_ptr().addr()) {
            self.memory_usage += buffer.capacity();
        }
    }

    /// Count the memory usage of `array` and its children recursively.
    fn count_array_memory_size(&mut self, array: &dyn Array) {
        if let Some(nulls) = array.nulls() {
            self.count_buffer_memory_size(nulls.buffer());
        }

        downcast_primitive_array! {
            array => self.count_buffer_memory_size(array.values().inner()),
            DataType::Null => {}
            DataType::Boolean => {
                self.count_buffer_memory_size(array.as_boolean().values().inner());
            }
            DataType::Binary => {
                self.count_byte_array_memory_size(array.as_binary::<i32>());
            }
            DataType::LargeBinary => {
                self.count_byte_array_memory_size(array.as_binary::<i64>());
            }
            DataType::Utf8 => {
                self.count_byte_array_memory_size(array.as_string::<i32>());
            }
            DataType::LargeUtf8 => {
                self.count_byte_array_memory_size(array.as_string::<i64>());
            }
            DataType::BinaryView => {
                self.count_byte_view_array_memory_size(array.as_binary_view());
            }
            DataType::Utf8View => {
                self.count_byte_view_array_memory_size(array.as_string_view());
            }
            DataType::FixedSizeBinary(_) => {
                self.count_buffer_memory_size(array.as_fixed_size_binary().values());
            }
            DataType::List(_) => {
                self.count_list_array_memory_size(array.as_list::<i32>());
            }
            DataType::LargeList(_) => {
                self.count_list_array_memory_size(array.as_list::<i64>());
            }
            DataType::ListView(_) => {
                self.count_list_view_array_memory_size(array.as_list_view::<i32>());
            }
            DataType::LargeListView(_) => {
                self.count_list_view_array_memory_size(array.as_list_view::<i64>());
            }
            DataType::FixedSizeList(_, _) => {
                self.count_array_memory_size(
                    array.as_fixed_size_list().values().as_ref(),
                );
            }
            DataType::Struct(_) => {
                for child in array.as_struct().columns() {
                    self.count_array_memory_size(child.as_ref());
                }
            }
            DataType::Union(_, _) => {
                let array = array.as_union();
                self.count_buffer_memory_size(array.type_ids().inner());
                if let Some(offsets) = array.offsets() {
                    self.count_buffer_memory_size(offsets.inner());
                }
                for (type_id, _) in array.fields().iter() {
                    self.count_array_memory_size(array.child(type_id).as_ref());
                }
            }
            DataType::Dictionary(_, _) => {
                let array = array.as_any_dictionary();
                self.count_array_memory_size(array.keys());
                self.count_array_memory_size(array.values().as_ref());
            }
            DataType::Map(_, _) => {
                let array = array.as_map();
                self.count_buffer_memory_size(array.offsets().inner().inner());
                self.count_array_memory_size(array.entries());
            }
            DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
                DataType::Int16 => self.count_run_array_memory_size::<Int16Type>(array),
                DataType::Int32 => self.count_run_array_memory_size::<Int32Type>(array),
                DataType::Int64 => self.count_run_array_memory_size::<Int64Type>(array),
                // Arrow only permits Int16, Int32, and Int64 run-end indexes. A
                // custom Array implementation may still expose malformed data;
                // retain correct accounting for it without panicking.
                _ => self.count_array_data_memory_size(&array.to_data()),
            },
            // All currently supported non-primitive layouts are handled above.
            // The Arrow macro requires a final arm for primitive variants that
            // its nested dispatch has already consumed. Keep a safe generic
            // fallback for custom or future Array implementations.
            _ => self.count_array_data_memory_size(&array.to_data()),
        }
    }

    fn count_byte_array_memory_size<T: ByteArrayType>(
        &mut self,
        array: &GenericByteArray<T>,
    ) {
        self.count_buffer_memory_size(array.offsets().inner().inner());
        self.count_buffer_memory_size(array.values());
    }

    fn count_byte_view_array_memory_size<T: ByteViewType>(
        &mut self,
        array: &GenericByteViewArray<T>,
    ) {
        self.count_buffer_memory_size(array.views().inner());
        for buffer in array.data_buffers() {
            self.count_buffer_memory_size(buffer);
        }
    }

    fn count_list_array_memory_size<O: arrow::array::OffsetSizeTrait>(
        &mut self,
        array: &GenericListArray<O>,
    ) {
        self.count_buffer_memory_size(array.offsets().inner().inner());
        self.count_array_memory_size(array.values().as_ref());
    }

    fn count_list_view_array_memory_size<O: arrow::array::OffsetSizeTrait>(
        &mut self,
        array: &GenericListViewArray<O>,
    ) {
        self.count_buffer_memory_size(array.offsets().inner());
        self.count_buffer_memory_size(array.sizes().inner());
        self.count_array_memory_size(array.values().as_ref());
    }

    fn count_run_array_memory_size<R: RunEndIndexType>(&mut self, array: &dyn Array) {
        if let Some(array) = array.as_any().downcast_ref::<RunArray<R>>() {
            self.count_buffer_memory_size(array.run_ends().inner().inner());
            self.count_array_memory_size(array.values().as_ref());
        } else {
            // The DataType and concrete array implementation disagree. Use the
            // generic representation rather than panic while accounting memory.
            self.count_array_data_memory_size(&array.to_data());
        }
    }

    fn count_array_data_memory_size(&mut self, array_data: &arrow::array::ArrayData) {
        for buffer in array_data.buffers() {
            self.count_buffer_memory_size(buffer);
        }
        if let Some(nulls) = array_data.nulls() {
            self.count_buffer_memory_size(nulls.buffer());
        }
        for child in array_data.child_data() {
            self.count_array_data_memory_size(child);
        }
    }
}

/// Counts the unique Array object memory retained by `array` and its children.
fn count_unique_array_object_memory_size(
    array: &ArrayRef,
    counted_arrays: &mut HashSet<usize>,
) -> usize {
    let array_ptr = Arc::as_ptr(array).cast::<()>() as usize;
    if !counted_arrays.insert(array_ptr) {
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
            .map(|child| count_unique_array_object_memory_size(child, counted_arrays))
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
        ArrayData, ArrayRef, BinaryViewArray, DictionaryArray, Float64Array, Int16Array,
        Int32Array, Int64Array, LargeListViewArray, ListArray, ListViewArray, MapArray,
        RunArray, StringArray, StringViewArray, StructArray, UnionArray, new_null_array,
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
        let mut counter = RecordBatchMemoryCounter::new();
        counter.count_array_memory_size(array);
        assert_eq!(counter.memory_usage(), array_data_memory_size(array));
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
}
