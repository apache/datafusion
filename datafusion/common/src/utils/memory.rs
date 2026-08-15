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
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow::array::{Array, ArrayData, ArrayRef, AsArray, RunArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::mem::size_of;
use std::num::NonZero;
use std::sync::Arc;

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
    counted_buffers: HashSet<NonZero<usize>>,
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
        let mut total_size = 0;

        for array in batch.columns() {
            let array_data = array.to_data();
            count_array_data_memory_size(
                &array_data,
                &mut self.counted_buffers,
                &mut total_size,
            );
        }

        self.memory_usage += total_size;
        total_size
    }

    /// Counts unique buffers and Array objects retained by `batch`.
    ///
    /// This is useful for accounting a sequence of batches at an operator
    /// boundary. It counts buffers once, and also avoids double-counting a
    /// top-level Arrow array shared by multiple batches.
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
}

/// Counts the unique Array object memory retained by `array` and its children.
fn count_unique_array_object_memory_size(
    array: &ArrayRef,
    counted_arrays: &mut HashSet<usize>,
) -> usize {
    let array_ptr = Arc::as_ptr(array) as *const () as usize;
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
        DataType::Union(_, _) => array
            .as_union()
            .fields()
            .iter()
            .map(|(type_id, _)| array.as_union().child(type_id))
            .collect(),
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
            DataType::Int16 => vec![
                array
                    .as_any()
                    .downcast_ref::<RunArray<Int16Type>>()
                    .expect("run-end array data type must match its run-end field")
                    .values(),
            ],
            DataType::Int32 => vec![
                array
                    .as_any()
                    .downcast_ref::<RunArray<Int32Type>>()
                    .expect("run-end array data type must match its run-end field")
                    .values(),
            ],
            DataType::Int64 => vec![
                array
                    .as_any()
                    .downcast_ref::<RunArray<Int64Type>>()
                    .expect("run-end array data type must match its run-end field")
                    .values(),
            ],
            _ => unreachable!("invalid run-end type: {run_ends}"),
        },
        _ => vec![],
    }
}

/// Count the memory usage of `array_data` and its children recursively.
fn count_array_data_memory_size(
    array_data: &ArrayData,
    counted_buffers: &mut HashSet<NonZero<usize>>,
    total_size: &mut usize,
) {
    // Count memory usage for `array_data`
    for buffer in array_data.buffers() {
        if counted_buffers.insert(buffer.data_ptr().addr()) {
            *total_size += buffer.capacity();
        } // Otherwise the buffer's memory is already counted
    }

    if let Some(null_buffer) = array_data.nulls()
        && counted_buffers.insert(null_buffer.inner().inner().data_ptr().addr())
    {
        *total_size += null_buffer.inner().inner().capacity();
    }

    // Count all children `ArrayData` recursively
    for child in array_data.child_data() {
        count_array_data_memory_size(child, counted_buffers, total_size);
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
        ArrayRef, DictionaryArray, Float64Array, Int32Array, ListArray, MapArray,
        RunArray, StructArray, UnionArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema, UnionFields};
    use std::sync::Arc;

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
    fn test_record_batch_memory_counter_deduplicates_shared_union_child() {
        let shared_child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let fields: UnionFields =
            [(0, Arc::new(Field::new("value", DataType::Int32, false)))]
                .into_iter()
                .collect();
        let make_union = || {
            Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    vec![0, 0, 0].into(),
                    None,
                    vec![Arc::clone(&shared_child)],
                )
                .unwrap(),
            ) as ArrayRef
        };

        assert_recursive_shared_child_memory(
            "union",
            make_union(),
            make_union(),
            shared_child.get_buffer_memory_size(),
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
    fn test_record_batch_memory_counter_deduplicates_shared_run_end_encoded_child() {
        let shared_child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let make_run_array = || {
            Arc::new(
                RunArray::<Int32Type>::try_new(
                    &Int32Array::from(vec![1, 2, 3]),
                    shared_child.as_ref(),
                )
                .unwrap(),
            ) as ArrayRef
        };

        assert_recursive_shared_child_memory(
            "run_end_encoded",
            make_run_array(),
            make_run_array(),
            shared_child.get_buffer_memory_size(),
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
