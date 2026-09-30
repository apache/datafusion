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

use super::*;
use arrow::array::{AsArray, GenericListArray, OffsetSizeTrait};
use datafusion_common::utils::offset_span;

pub(super) const MAX_COMPACT_BUILD_BYTES: usize = 64 * 1024 * 1024;
const TARGET_BUILD_BATCH_BYTES: usize = 8 * 1024 * 1024;
const TARGET_BUILD_BATCH_ROWS: usize = 8192;
const BUILD_ROW_DIRECTORY_STRIDE: usize = 1024;

/// Keep the compact path for small logical inputs, even when their slices pin
/// much larger allocations. Unsupported estimates only disable this optimization.
pub(super) fn should_preserve_batches(
    batches: &[RecordBatch],
    input_bytes: usize,
) -> bool {
    if input_bytes <= MAX_COMPACT_BUILD_BYTES {
        return false;
    }
    let mut copy_bytes = 0usize;
    for batch in batches {
        let Ok(bytes) = estimate_batch_concat_allocation(batch) else {
            return true;
        };
        copy_bytes = copy_bytes.saturating_add(bytes);
        if copy_bytes > MAX_COMPACT_BUILD_BYTES {
            return true;
        }
    }
    false
}

fn estimate_batch_concat_allocation(batch: &RecordBatch) -> Result<usize> {
    batch.columns().iter().try_fold(0usize, |bytes, array| {
        Ok(bytes.saturating_add(estimate_compact_copy_size(array.as_ref())?))
    })
}

/// Estimate visible children for the layout decision, not memory admission.
/// `concat_build_batches` still reserves its existing conservative estimate.
/// Arrow's generic slice estimate includes unsliced List and Map children,
/// whereas concat copies only the child ranges covered by their offsets.
fn estimate_compact_copy_size(array: &dyn Array) -> Result<usize> {
    let bytes = match array.data_type() {
        DataType::List(_) => estimate_list_copy_size(array.as_list::<i32>())?,
        DataType::LargeList(_) => estimate_list_copy_size(array.as_list::<i64>())?,
        DataType::Map(_, _) => {
            let map = array.as_map();
            let (start, len) = offset_span(map.offsets());
            let entries = map.entries().slice(start, len);
            (map.len() + 1)
                .saturating_mul(size_of::<i32>())
                .saturating_add(estimate_compact_copy_size(&entries)?)
        }
        DataType::Struct(_) => array.as_struct().columns().iter().try_fold(
            0usize,
            |bytes, child| -> Result<usize> {
                Ok(bytes.saturating_add(estimate_compact_copy_size(child.as_ref())?))
            },
        )?,
        DataType::FixedSizeList(_, _) => {
            estimate_compact_copy_size(array.as_fixed_size_list().values().as_ref())?
        }
        _ => return estimate_concat_allocation(array),
    };
    Ok(bytes.saturating_add(array.nulls().map_or(0, |_| array.len().div_ceil(8))))
}

fn estimate_list_copy_size<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
) -> Result<usize> {
    let (start, len) = offset_span(list.offsets());
    let values = list.values().slice(start, len);
    Ok((list.len() + 1)
        .saturating_mul(size_of::<O>())
        .saturating_add(estimate_compact_copy_size(values.as_ref())?))
}

/// Batch-local keys and a sparse directory over logical hash-table row indices.
pub(in crate::joins::hash_join) struct MultiBatchBuildData {
    batches: Vec<RecordBatch>,
    values: Vec<Vec<ArrayRef>>,
    batch_offsets: Vec<usize>,
    row_directory: Vec<usize>,
}

impl MultiBatchBuildData {
    pub(super) fn try_new(
        batches: Vec<RecordBatch>,
        on_left: &[PhysicalExprRef],
        reservation: &MemoryReservation,
        metrics: &BuildProbeJoinMetrics,
    ) -> Result<Self> {
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let directory_len = rows.div_ceil(BUILD_ROW_DIRECTORY_STRIDE);
        let metadata_size = (batches.len() + 1 + directory_len) * size_of::<usize>()
            + batches.capacity() * size_of::<RecordBatch>()
            + batches.len()
                * (size_of::<Vec<ArrayRef>>() + on_left.len() * size_of::<ArrayRef>());
        reservation.try_grow(metadata_size)?;
        metrics.build_mem_used.add(metadata_size);

        // Fallible iterator collection can grow geometrically. Allocate the
        // capacities admitted above, including the common single-key case.
        let mut values = Vec::with_capacity(batches.len());
        for batch in &batches {
            let mut keys = Vec::with_capacity(on_left.len());
            for expr in on_left {
                keys.push(expr.evaluate(batch)?.into_array_of_size(batch.num_rows())?);
            }
            values.push(keys);
        }
        let mut batch_offsets = Vec::with_capacity(batches.len() + 1);
        batch_offsets.push(0);
        for batch in &batches {
            batch_offsets.push(batch_offsets.last().unwrap() + batch.num_rows());
        }
        let mut row_directory = Vec::with_capacity(directory_len);
        let mut batch_index = 0;
        for row in (0..rows).step_by(BUILD_ROW_DIRECTORY_STRIDE) {
            while batch_offsets[batch_index + 1] <= row {
                batch_index += 1;
            }
            row_directory.push(batch_index);
        }
        Ok(Self {
            batches,
            values,
            batch_offsets,
            row_directory,
        })
    }

    pub(in crate::joins::hash_join) fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub(in crate::joins::hash_join) fn values(&self) -> &[Vec<ArrayRef>] {
        &self.values
    }

    pub(in crate::joins::hash_join) fn num_rows(&self) -> usize {
        *self.batch_offsets.last().unwrap()
    }

    pub(in crate::joins::hash_join) fn gather_indices(
        &self,
        indices: &UInt64Array,
    ) -> Vec<(usize, usize)> {
        indices
            .iter()
            .map(|index| {
                index.map_or((0, 0), |row| {
                    let row = row as usize;
                    let mut batch = self.row_directory[row / BUILD_ROW_DIRECTORY_STRIDE];
                    while self.batch_offsets[batch + 1] <= row {
                        batch += 1;
                    }
                    (batch + 1, row - self.batch_offsets[batch])
                })
            })
            .collect()
    }

    /// Only small key arrays are copied for IN-list pushdown, never the payload.
    pub(super) fn try_inlist_values(
        &self,
        max_size: usize,
        reservation: &MemoryReservation,
        metrics: &BuildProbeJoinMetrics,
    ) -> Result<Option<ArrayRef>> {
        let max_size = max_size.min(MAX_COMPACT_BUILD_BYTES);
        let num_keys = self.values[0].len();
        let mut copy_size = num_keys * (self.num_rows().div_ceil(8) + 64);
        for array in self.values.iter().flatten() {
            let Ok(bytes) = estimate_concat_allocation(array.as_ref()) else {
                return Ok(None);
            };
            copy_size = copy_size.saturating_add(bytes);
            if copy_size > max_size {
                return Ok(None);
            }
        }
        if reservation.try_grow(copy_size).is_err() {
            return Ok(None);
        }
        // Concatenation can fail for otherwise valid batch-local keys, e.g.
        // when the union of dictionaries exceeds their key type's capacity.
        // Membership pushdown is optional, so keep the hash predicate then.
        let result = (|| -> Result<Option<(ArrayRef, usize)>> {
            let keys = (0..num_keys)
                .map(|key| {
                    let arrays = self
                        .values
                        .iter()
                        .map(|values| values[key].as_ref())
                        .collect::<Vec<_>>();
                    Ok(arrow::compute::concat(&arrays)?)
                })
                .collect::<Result<Vec<ArrayRef>>>()?;
            let Some(inlist) = build_struct_inlist_values(&keys)? else {
                return Ok(None);
            };
            let mut counter = RecordBatchMemoryCounter::new();
            for batch in &self.batches {
                counter.count_batch(batch);
            }
            for values in self.values.iter().flatten() {
                counter.count_array(values.as_ref());
            }
            let retained = counter.count_array(inlist.as_ref());
            Ok((retained <= max_size && retained <= copy_size)
                .then_some((inlist, retained)))
        })()
        .unwrap_or(None);
        if let Some((inlist, retained)) = result {
            reservation.shrink(copy_size - retained);
            metrics.build_mem_used.add(retained);
            Ok(Some(inlist))
        } else {
            reservation.shrink(copy_size);
            Ok(None)
        }
    }
}

/// Coalesce independent flat inputs a bounded group at a time. Shared buffers
/// stay intact: replacing one slice must not release another slice's charge.
pub(super) fn coalesce_build_batches(
    schema: &SchemaRef,
    mut batches: Vec<RecordBatch>,
    mut input_bytes: usize,
    reservation: &mut MemoryReservation,
    metrics: &BuildProbeJoinMetrics,
) -> Result<Vec<RecordBatch>> {
    if batches.iter().any(|batch| batch.num_rows() == 0) {
        batches.retain(|batch| batch.num_rows() != 0);
        // Empty slices can pin whole allocations, but buffers shared with a
        // remaining batch must keep their charge.
        let mut counter = RecordBatchMemoryCounter::new();
        for batch in &batches {
            counter.count_batch(batch);
        }
        let retained = counter.memory_usage();
        let released = input_bytes - retained;
        reservation.shrink(released);
        metrics.build_mem_used.sub(released);
        input_bytes = retained;
    }
    let independent_bytes = batches
        .iter()
        .map(get_record_batch_memory_size)
        .sum::<usize>();
    if independent_bytes != input_bytes
        || schema.fields().iter().any(|field| {
            matches!(
                field.data_type(),
                DataType::Dictionary(_, _)
                    | DataType::Union(_, _)
                    | DataType::RunEndEncoded(_, _)
            ) || field.data_type().is_nested()
        })
    {
        return Ok(batches);
    }

    let mut output = Vec::new();
    let mut pending = Vec::new();
    let mut pending_reserved_bytes = 0usize;
    let mut pending_copy_bytes = 0usize;
    let mut pending_rows = 0usize;
    for batch in batches {
        let reserved_bytes = get_record_batch_memory_size(&batch);
        let copy_bytes = estimate_batch_concat_allocation(&batch).unwrap_or(usize::MAX);
        let rows = batch.num_rows();
        if !pending.is_empty()
            && (pending_copy_bytes.saturating_add(copy_bytes) > TARGET_BUILD_BATCH_BYTES
                || pending_rows.saturating_add(rows) > TARGET_BUILD_BATCH_ROWS)
        {
            output.push(coalesce_build_group(
                schema,
                std::mem::take(&mut pending),
                pending_reserved_bytes,
                pending_copy_bytes,
                reservation,
                metrics,
            )?);
            pending_reserved_bytes = 0;
            pending_copy_bytes = 0;
            pending_rows = 0;
        }
        pending_reserved_bytes += reserved_bytes;
        pending_copy_bytes = pending_copy_bytes.saturating_add(copy_bytes);
        pending_rows += rows;
        pending.push(batch);
    }
    if !pending.is_empty() {
        output.push(coalesce_build_group(
            schema,
            pending,
            pending_reserved_bytes,
            pending_copy_bytes,
            reservation,
            metrics,
        )?);
    }
    Ok(output)
}

fn coalesce_build_group(
    schema: &SchemaRef,
    mut batches: Vec<RecordBatch>,
    reserved_bytes: usize,
    copy_bytes: usize,
    reservation: &mut MemoryReservation,
    metrics: &BuildProbeJoinMetrics,
) -> Result<RecordBatch> {
    if batches.len() == 1
        && copy_bytes <= TARGET_BUILD_BATCH_BYTES
        && reserved_bytes > copy_bytes.saturating_mul(2)
        && !schema.fields().iter().any(|field| {
            matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView)
        })
    {
        // Arrow's single-input concat is zero-copy. A second, empty slice
        // forces a bounded copy without allocating another input buffer.
        batches.push(batches[0].slice(0, 0));
    }
    concat_build_batches(schema, batches, false, reserved_bytes, reservation, metrics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        FixedSizeListArray, Int64Array, LargeListArray, ListArray, MapArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow_schema::Field;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };

    fn primitive_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1; rows]))])
            .unwrap()
    }

    fn input_bytes(batches: &[RecordBatch]) -> usize {
        let mut counter = RecordBatchMemoryCounter::new();
        for batch in batches {
            counter.count_batch(batch);
        }
        counter.memory_usage()
    }

    #[test]
    fn key_metadata_capacities_match_reservation() -> Result<()> {
        // Non-power-of-two counts expose spare capacity from fallible collect.
        for (batch_count, key_count) in [(1, 1), (3, 1), (5, 3)] {
            let batches = (0..batch_count)
                .map(|_| primitive_batch(1025))
                .collect::<Vec<_>>();
            let keys = (0..key_count)
                .map(|_| Arc::new(Column::new("a", 0)) as PhysicalExprRef)
                .collect::<Vec<_>>();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
            let reservation = MemoryConsumer::new("test").register(&pool);
            let metrics = BuildProbeJoinMetrics::new(0, &ExecutionPlanMetricsSet::new());
            let data =
                MultiBatchBuildData::try_new(batches, &keys, &reservation, &metrics)?;
            let metadata_bytes = data.batches.capacity() * size_of::<RecordBatch>()
                + data.values.capacity() * size_of::<Vec<ArrayRef>>()
                + data
                    .values
                    .iter()
                    .map(|keys| keys.capacity())
                    .sum::<usize>()
                    * size_of::<ArrayRef>()
                + (data.batch_offsets.capacity() + data.row_directory.capacity())
                    * size_of::<usize>();
            assert_eq!(reservation.size(), metadata_bytes);
            assert_eq!(metrics.build_mem_used.value(), metadata_bytes);
            drop(data);
            drop(reservation);
            assert_eq!(pool.reserved(), 0);
        }
        Ok(())
    }

    #[test]
    fn small_slices_of_large_shared_input_stay_compact() -> Result<()> {
        let parent = primitive_batch((MAX_COMPACT_BUILD_BYTES + 8) / 8);
        let batches = vec![parent.slice(0, 1), parent.slice(1, 1)];
        drop(parent);
        let bytes = input_bytes(&batches);
        assert!(bytes > MAX_COMPACT_BUILD_BYTES);
        assert!(!should_preserve_batches(&batches, bytes));

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes + 128));
        let mut reservation = MemoryConsumer::new("test").register(&pool);
        reservation.try_grow(bytes)?;
        let metrics = BuildProbeJoinMetrics::new(0, &ExecutionPlanMetricsSet::new());
        metrics.build_mem_used.add(bytes);
        let compact = concat_build_batches(
            &batches[0].schema(),
            batches,
            false,
            bytes,
            &mut reservation,
            &metrics,
        )?;
        assert_eq!(compact.num_rows(), 2);
        assert_eq!(get_record_batch_memory_size(&compact), 16);
        assert_eq!(reservation.size(), 16);
        Ok(())
    }

    #[test]
    fn large_logical_input_keeps_batches() {
        let parent = primitive_batch((MAX_COMPACT_BUILD_BYTES + 8) / 8);
        let batches = vec![parent.clone(), parent];
        assert!(should_preserve_batches(&batches, input_bytes(&batches)));
        let small = vec![primitive_batch(2)];
        assert!(!should_preserve_batches(&small, input_bytes(&small)));
    }

    #[test]
    fn small_nested_slices_of_large_shared_input_stay_compact() -> Result<()> {
        let len = (MAX_COMPACT_BUILD_BYTES + 8) / 8;
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1; len]));
        let item = Arc::new(Field::new_list_field(DataType::Int64, false));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 1, 2, len as i32].into()),
            Arc::clone(&values),
            None,
        ));
        let large_list: ArrayRef = Arc::new(LargeListArray::new(
            item,
            OffsetBuffer::new(vec![0, 1, 2, len as i64].into()),
            Arc::clone(&values),
            None,
        ));
        let entries = StructArray::new(
            vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ]
            .into(),
            vec![Arc::clone(&values), values],
            None,
        );
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 1, 2, len as i32].into()),
            entries,
            None,
            false,
        ));
        let structure: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("list", list.data_type().clone(), false)].into(),
            vec![Arc::clone(&list)],
            None,
        ));
        let fixed_size_list: ArrayRef = Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new_list_field(list.data_type().clone(), false)),
            1,
            Arc::clone(&list),
            None,
        ));

        for array in [list, large_list, map, structure, fixed_size_list] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "nested",
                array.data_type().clone(),
                false,
            )]));
            let parent = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
            let batches = vec![parent.slice(0, 1), parent.slice(1, 1)];
            drop(parent);
            let bytes = input_bytes(&batches);
            assert!(bytes > MAX_COMPACT_BUILD_BYTES);
            assert!(!should_preserve_batches(&batches, bytes));

            // Admission remains deliberately more conservative than the layout
            // estimate, as on the original contiguous build path.
            let admission_bytes = batches.iter().try_fold(0usize, |bytes, batch| {
                Ok::<_, datafusion_common::DataFusionError>(
                    bytes + estimate_concat_allocation(batch.column(0).as_ref())?,
                )
            })?;
            assert!(admission_bytes > MAX_COMPACT_BUILD_BYTES);
            let pool: Arc<dyn MemoryPool> =
                Arc::new(GreedyMemoryPool::new(bytes + admission_bytes + 1024));
            let mut reservation = MemoryConsumer::new("test").register(&pool);
            reservation.try_grow(bytes)?;
            let metrics = BuildProbeJoinMetrics::new(0, &ExecutionPlanMetricsSet::new());
            metrics.build_mem_used.add(bytes);
            let compact = concat_build_batches(
                &schema,
                batches,
                false,
                bytes,
                &mut reservation,
                &metrics,
            )?;
            assert_eq!(compact.num_rows(), 2);
            assert!(get_record_batch_memory_size(&compact) < 1024);
            assert_eq!(reservation.size(), get_record_batch_memory_size(&compact));
        }
        Ok(())
    }

    #[test]
    fn coalesce_independent_slices_uses_copy_size() -> Result<()> {
        let batches = (0..2)
            .map(|_| primitive_batch(2 * 1024 * 1024).slice(0, 4096))
            .collect::<Vec<_>>();
        let bytes = input_bytes(&batches);
        assert_eq!(bytes, 32 * 1024 * 1024);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(bytes + TARGET_BUILD_BATCH_BYTES));
        let mut reservation = MemoryConsumer::new("test").register(&pool);
        reservation.try_grow(bytes)?;
        let metrics = BuildProbeJoinMetrics::new(0, &ExecutionPlanMetricsSet::new());
        metrics.build_mem_used.add(bytes);
        let output = coalesce_build_batches(
            &batches[0].schema(),
            batches,
            bytes,
            &mut reservation,
            &metrics,
        )?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 8192);
        assert_eq!(input_bytes(&output), 8192 * 8);
        assert_eq!(reservation.size(), input_bytes(&output));
        Ok(())
    }

    #[test]
    fn coalesce_repackages_wasteful_singleton() -> Result<()> {
        let batches = vec![primitive_batch(2 * 1024 * 1024).slice(0, 4096)];
        let bytes = input_bytes(&batches);
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(bytes + TARGET_BUILD_BATCH_BYTES));
        let mut reservation = MemoryConsumer::new("test").register(&pool);
        reservation.try_grow(bytes)?;
        let metrics = BuildProbeJoinMetrics::new(0, &ExecutionPlanMetricsSet::new());
        metrics.build_mem_used.add(bytes);
        let output = coalesce_build_batches(
            &batches[0].schema(),
            batches,
            bytes,
            &mut reservation,
            &metrics,
        )?;
        assert_eq!(output.len(), 1);
        assert_eq!(input_bytes(&output), 4096 * 8);
        assert_eq!(reservation.size(), input_bytes(&output));
        Ok(())
    }

    #[test]
    fn coalesce_releases_buffers_retained_only_by_empty_batches() -> Result<()> {
        let parent = primitive_batch(16);
        let batches = vec![
            primitive_batch(2 * 1024 * 1024).slice(0, 0),
            parent.slice(0, 8),
            parent.slice(8, 8),
            parent.slice(0, 0),
        ];
        drop(parent);
        let bytes = input_bytes(&batches);
        assert_eq!(bytes, 16 * 1024 * 1024 + 128);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let mut reservation = MemoryConsumer::new("test").register(&pool);
        reservation.try_grow(bytes)?;
        let metrics = BuildProbeJoinMetrics::new(0, &ExecutionPlanMetricsSet::new());
        metrics.build_mem_used.add(bytes);
        let output = coalesce_build_batches(
            &batches[0].schema(),
            batches,
            bytes,
            &mut reservation,
            &metrics,
        )?;
        assert_eq!(output.len(), 2);
        assert!(output.iter().all(|batch| batch.num_rows() == 8));
        assert_eq!(input_bytes(&output), 128);
        assert_eq!(reservation.size(), 128);
        assert_eq!(metrics.build_mem_used.value(), 128);
        Ok(())
    }
}
