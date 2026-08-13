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
use arrow::array::types::ByteArrayType;
use arrow::array::{Array, AsArray, downcast_run_array};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::downcast_primitive_array;
use arrow::record_batch::RecordBatch;
use std::mem::size_of;
use std::num::NonZero;

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
/// {xxxxxxxxxxxxxxxxxxx} <--- buffer
///       ^    ^  ^    ^
///       |    |  |    |
/// col1->{    }  |    |
/// col2--------->{    }
///
/// In the above case, `get_record_batch_memory_size` will return the size of
/// the buffer, instead of the sum of `col1` and `col2`'s actual memory size.
///
/// Note: Current `RecordBatch`.get_array_memory_size()` will double count the
/// buffer memory size if multiple arrays within the batch are sharing the same
/// `Buffer`. This method provides temporary fix until the issue is resolved:
/// <https://github.com/apache/arrow-rs/issues/6439>
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
    /// Total memory of all unique buffers counted so far
    memory_usage: usize,
}

impl RecordBatchMemoryCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Count `batch`, returning the memory used by its buffers that have not
    /// been counted before.
    pub fn count_batch(&mut self, batch: &RecordBatch) -> usize {
        let mut total_size = 0;

        for array in batch.columns() {
            count_array_memory_size(
                array.as_ref(),
                &mut self.counted_buffers,
                &mut total_size,
            );
        }

        self.memory_usage += total_size;
        total_size
    }

    /// Total memory of the unique buffers of all batches counted so far.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
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

fn count_buffer_memory_size(
    buffer: &Buffer,
    counted_buffers: &mut BufferIdSet,
    total_size: &mut usize,
) {
    if counted_buffers.insert(buffer.data_ptr().addr()) {
        *total_size += buffer.capacity();
    }
}

/// Count the memory usage of `array` and its children recursively.
fn count_array_memory_size(
    array: &dyn Array,
    counted_buffers: &mut BufferIdSet,
    total_size: &mut usize,
) {
    if let Some(nulls) = array.nulls() {
        count_buffer_memory_size(nulls.buffer(), counted_buffers, total_size);
    }

    downcast_primitive_array! {
        array => count_buffer_memory_size(
            array.values().inner(),
            counted_buffers,
            total_size,
        ),
        DataType::Null => {}
        DataType::Boolean => count_buffer_memory_size(
            array.as_boolean().values().inner(),
            counted_buffers,
            total_size,
        ),
        DataType::Binary => count_byte_array_memory_size(
            array.as_binary::<i32>(),
            counted_buffers,
            total_size,
        ),
        DataType::LargeBinary => count_byte_array_memory_size(
            array.as_binary::<i64>(),
            counted_buffers,
            total_size,
        ),
        DataType::Utf8 => count_byte_array_memory_size(
            array.as_string::<i32>(),
            counted_buffers,
            total_size,
        ),
        DataType::LargeUtf8 => count_byte_array_memory_size(
            array.as_string::<i64>(),
            counted_buffers,
            total_size,
        ),
        DataType::BinaryView => {
            let array = array.as_binary_view();
            count_buffer_memory_size(array.views().inner(), counted_buffers, total_size);
            for buffer in array.data_buffers() {
                count_buffer_memory_size(buffer, counted_buffers, total_size);
            }
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            count_buffer_memory_size(array.views().inner(), counted_buffers, total_size);
            for buffer in array.data_buffers() {
                count_buffer_memory_size(buffer, counted_buffers, total_size);
            }
        }
        DataType::FixedSizeBinary(_) => count_buffer_memory_size(
            array.as_fixed_size_binary().values(),
            counted_buffers,
            total_size,
        ),
        DataType::List(_) => count_list_array_memory_size(
            array.as_list::<i32>(),
            counted_buffers,
            total_size,
        ),
        DataType::LargeList(_) => count_list_array_memory_size(
            array.as_list::<i64>(),
            counted_buffers,
            total_size,
        ),
        DataType::ListView(_) => {
            let array = array.as_list_view::<i32>();
            count_buffer_memory_size(array.offsets().inner(), counted_buffers, total_size);
            count_buffer_memory_size(array.sizes().inner(), counted_buffers, total_size);
            count_array_memory_size(array.values().as_ref(), counted_buffers, total_size);
        }
        DataType::LargeListView(_) => {
            let array = array.as_list_view::<i64>();
            count_buffer_memory_size(array.offsets().inner(), counted_buffers, total_size);
            count_buffer_memory_size(array.sizes().inner(), counted_buffers, total_size);
            count_array_memory_size(array.values().as_ref(), counted_buffers, total_size);
        }
        DataType::FixedSizeList(_, _) => count_array_memory_size(
            array.as_fixed_size_list().values().as_ref(),
            counted_buffers,
            total_size,
        ),
        DataType::Struct(_) => {
            for child in array.as_struct().columns() {
                count_array_memory_size(child.as_ref(), counted_buffers, total_size);
            }
        }
        DataType::Union(_, _) => {
            let array = array.as_union();
            count_buffer_memory_size(array.type_ids().inner(), counted_buffers, total_size);
            if let Some(offsets) = array.offsets() {
                count_buffer_memory_size(offsets.inner(), counted_buffers, total_size);
            }
            for (type_id, _) in array.fields().iter() {
                count_array_memory_size(
                    array.child(type_id).as_ref(),
                    counted_buffers,
                    total_size,
                );
            }
        }
        DataType::Dictionary(_, _) => {
            let array = array.as_any_dictionary();
            count_array_memory_size(array.keys(), counted_buffers, total_size);
            count_array_memory_size(array.values().as_ref(), counted_buffers, total_size);
        }
        DataType::Map(_, _) => {
            let array = array.as_map();
            count_buffer_memory_size(
                array.offsets().inner().inner(),
                counted_buffers,
                total_size,
            );
            count_array_memory_size(array.entries(), counted_buffers, total_size);
        }
        DataType::RunEndEncoded(_, _) => downcast_run_array! {
            array => {
                count_buffer_memory_size(
                    array.run_ends().inner().inner(),
                    counted_buffers,
                    total_size,
                );
                count_array_memory_size(
                    array.values().as_ref(),
                    counted_buffers,
                    total_size,
                );
            },
            _ => unreachable!(),
        }
        _ => unreachable!("unsupported array type: {}", array.data_type()),
    }
}

fn count_byte_array_memory_size<T: ByteArrayType>(
    array: &arrow::array::GenericByteArray<T>,
    counted_buffers: &mut BufferIdSet,
    total_size: &mut usize,
) {
    count_buffer_memory_size(
        array.offsets().inner().inner(),
        counted_buffers,
        total_size,
    );
    count_buffer_memory_size(array.values(), counted_buffers, total_size);
}

fn count_list_array_memory_size<O: arrow::array::OffsetSizeTrait>(
    array: &arrow::array::GenericListArray<O>,
    counted_buffers: &mut BufferIdSet,
    total_size: &mut usize,
) {
    count_buffer_memory_size(
        array.offsets().inner().inner(),
        counted_buffers,
        total_size,
    );
    count_array_memory_size(array.values().as_ref(), counted_buffers, total_size);
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
        ArrayData, ArrayRef, Float64Array, Int16Array, Int32Array, Int64Array, ListArray,
        RunArray, StringArray, new_null_array,
    };
    use arrow::datatypes::{
        DataType, Field, Int16Type, Int32Type, Int64Type, Schema, UnionFields, UnionMode,
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
            let mut total_size = 0;
            count_array_memory_size(
                array.as_ref(),
                &mut BufferIdSet::default(),
                &mut total_size,
            );
            assert_eq!(
                total_size,
                array_data_memory_size(array.as_ref()),
                "{data_type}"
            );
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
            let mut total_size = 0;
            count_array_memory_size(
                array.as_ref(),
                &mut BufferIdSet::default(),
                &mut total_size,
            );
            assert_eq!(total_size, array_data_memory_size(array.as_ref()));
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
