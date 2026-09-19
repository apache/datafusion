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

use crate::spill::get_record_batch_memory_size;
use arrow::array::ArrayRef;
use arrow::compute::interleave;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, assert_or_internal_err};
use datafusion_execution::memory_pool::MemoryReservation;
use log::warn;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
struct BatchCursor {
    /// The index into BatchBuilder::batches
    batch_idx: usize,
    /// The row index within the given batch
    row_idx: usize,
}

impl BatchCursor {
    /// A cursor whose batch has been released. `push_row` must not be called
    /// for the stream until `push_batch` installs a new cursor.
    const RELEASED: Self = Self {
        batch_idx: usize::MAX,
        row_idx: 0,
    };
}

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
#[derive(Debug)]
pub struct BatchBuilder {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Maintain a list of [`RecordBatch`] and their corresponding stream
    batches: Vec<(usize, RecordBatch)>,

    /// Accounts for memory used by buffered batches.
    ///
    /// May include pre-reserved bytes (from `sort_spill_reservation_bytes`)
    /// that were transferred via [`MemoryReservation::take()`] to prevent
    /// starvation when concurrent sort partitions compete for pool memory.
    reservation: MemoryReservation,

    /// Tracks the actual memory used by buffered batches (not including
    /// pre-reserved bytes). This allows [`Self::push_batch`] to skip pool
    /// allocation requests when the pre-reserved bytes cover the batch.
    batches_mem_used: usize,

    /// The initial reservation size at construction time. When the reservation
    /// is pre-loaded with `sort_spill_reservation_bytes` (via `take()`), this
    /// records that amount so we never shrink below it, maintaining the
    /// anti-starvation guarantee throughout the merge.
    initial_reservation: usize,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<BatchCursor>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_idx, row_idx)`
    indices: Vec<(usize, usize)>,
}

impl BatchBuilder {
    /// Create a new [`BatchBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(
        schema: SchemaRef,
        stream_count: usize,
        batch_size: usize,
        reservation: MemoryReservation,
    ) -> Self {
        let initial_reservation = reservation.size();
        Self {
            schema,
            batches: Vec::with_capacity(stream_count * 2),
            cursors: vec![BatchCursor::RELEASED; stream_count],
            indices: Vec::with_capacity(batch_size),
            reservation,
            batches_mem_used: 0,
            initial_reservation,
        }
    }

    /// Append a new batch in `stream_idx`
    pub fn push_batch(&mut self, stream_idx: usize, batch: RecordBatch) -> Result<()> {
        let size = get_record_batch_memory_size(&batch);
        self.batches_mem_used += size;
        // Only request additional memory from the pool when actual batch
        // usage exceeds the current reservation (which may include
        // pre-reserved bytes from sort_spill_reservation_bytes).
        try_grow_reservation_to_at_least(&mut self.reservation, self.batches_mem_used)?;
        let batch_idx = self.batches.len();
        self.batches.push((stream_idx, batch));
        self.cursors[stream_idx] = BatchCursor {
            batch_idx,
            row_idx: 0,
        };
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        debug_assert!(
            self.batches
                .get(self.cursors[stream_idx].batch_idx)
                .is_some_and(|(_, batch)| {
                    self.cursors[stream_idx].row_idx < batch.num_rows()
                }),
            "push_row on stream {stream_idx} with no live batch"
        );
        let cursor = &mut self.cursors[stream_idx];
        let row_idx = cursor.row_idx;
        cursor.row_idx += 1;
        self.indices.push((cursor.batch_idx, row_idx));
    }

    /// Returns the number of in-progress rows in this [`BatchBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`BatchBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Returns the schema of this [`BatchBuilder`]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Release fully consumed batches after a merge drains at an input boundary.
    /// Keeping their dictionaries can otherwise enlarge the next output even
    /// though none of its rows refer to those batches.
    pub(super) fn discard_consumed_batches(&mut self) -> Result<()> {
        assert_or_internal_err!(
            self.indices.is_empty(),
            "pending merge rows must be emitted before discarding source batches"
        );
        self.retain_cursor_batches();
        // Bypassed spill merges only update their local accounting here; their
        // real pool reservation remains attached to the outer merge stream.
        self.release_unused_memory();
        Ok(())
    }

    /// Whether replacing an exhausted input would exceed the allowance for
    /// retained source batches and materializing output together. This preserves
    /// the caller's existing source/output estimate; cursor, read-ahead and IPC
    /// allocations still depend on the merge's heuristic workspace reservation.
    ///
    /// This check runs only at input boundaries. A tight allowance falls back
    /// to flushing at every boundary with pending rows, as before. An empty
    /// builder skips flushing, so this policy cannot emit empty batches or
    /// stall progress while waiting for a larger allowance.
    pub(super) fn should_flush_before_input(
        &self,
        next_batch_bytes: usize,
        memory_limit: usize,
        batch_size: usize,
    ) -> Result<bool> {
        if self.is_empty() {
            return Ok(false);
        }

        // Rows selected from each source batch form one contiguous range, even
        // though the merged indices interleave those ranges.
        let mut ranges = self
            .schema
            .fields()
            .iter()
            .any(|field| {
                matches!(
                    field.data_type(),
                    DataType::Utf8
                        | DataType::Binary
                        | DataType::LargeUtf8
                        | DataType::LargeBinary
                )
            })
            .then(|| vec![None; self.batches.len()]);
        if let Some(ranges) = &mut ranges {
            for &(batch, row) in &self.indices {
                let range = ranges[batch].get_or_insert(row..row);
                range.end = row + 1;
            }
        }

        let mut remaining_rows = 0usize;
        for (batch_idx, (stream_idx, batch)) in self.batches.iter().enumerate() {
            let cursor = &self.cursors[*stream_idx];
            // Released cursors have no matching batch and contribute no rows.
            if cursor.batch_idx == batch_idx && cursor.row_idx < batch.num_rows() {
                remaining_rows =
                    remaining_rows.saturating_add(batch.num_rows() - cursor.row_idx);
                if let Some(ranges) = &mut ranges {
                    let range =
                        ranges[batch_idx].get_or_insert(cursor.row_idx..cursor.row_idx);
                    range.end = batch.num_rows();
                }
            }
        }
        // A primitive column bounds the number of rows in the next input even
        // when other columns are variable-width. Use the largest individual
        // width, since columns may share their backing buffers.
        let minimum_row_bytes = self
            .schema
            .fields()
            .iter()
            .filter_map(|field| field.data_type().primitive_width())
            .max();
        let future_rows = minimum_row_bytes.map_or(batch_size, |width| {
            self.len()
                .saturating_add(remaining_rows)
                .saturating_add(next_batch_bytes / width)
                .min(batch_size)
        });

        let mut output_bytes = 0usize;
        let mut next_batch_may_contribute_values = false;
        for (column, field) in self.schema.fields().iter().enumerate() {
            let data_type = field.data_type();
            let column_bytes = if let Some(width) = data_type.primitive_width() {
                padded_buffer_size(future_rows.saturating_mul(width))
            } else {
                match data_type {
                    DataType::Null => 0,
                    DataType::Boolean => padded_buffer_size(future_rows.div_ceil(8)),
                    DataType::Utf8
                    | DataType::Binary
                    | DataType::LargeUtf8
                    | DataType::LargeBinary => {
                        next_batch_may_contribute_values = true;
                        let offset_width =
                            if matches!(data_type, DataType::Utf8 | DataType::Binary) {
                                4
                            } else {
                                8
                            };
                        let mut values_bytes = 0usize;
                        let ranges = ranges.as_ref().expect("byte arrays have ranges");
                        for ((_, batch), range) in self.batches.iter().zip(ranges) {
                            if let Some(range) = range {
                                let data = batch.column(column).to_data();
                                let slice = data.slice(range.start, range.len());
                                // Remove the source's offsets and validity so
                                // the output accounts for those buffers once.
                                let validity = if slice.nulls().is_some() {
                                    range.len().div_ceil(8)
                                } else {
                                    0
                                };
                                values_bytes = values_bytes.saturating_add(
                                    slice.get_slice_memory_size()?
                                        - (range.len() + 1) * offset_width
                                        - validity,
                                );
                            }
                        }
                        padded_buffer_size(values_bytes).saturating_add(
                            padded_buffer_size(
                                future_rows
                                    .saturating_add(1)
                                    .saturating_mul(offset_width),
                            ),
                        )
                    }
                    _ => {
                        // Nested arrays, dictionaries and views can retain or
                        // concatenate buffers with no selected rows. Count the
                        // entire input buffers, including the next input, rather
                        // than estimating them from an average row width.
                        next_batch_may_contribute_values = true;
                        self.batches.iter().fold(0usize, |bytes, (_, batch)| {
                            bytes.saturating_add(
                                batch.column(column).get_buffer_memory_size(),
                            )
                        })
                    }
                }
            };
            output_bytes = output_bytes.saturating_add(column_bytes);
            if field.is_nullable()
                || self
                    .batches
                    .iter()
                    .any(|(_, batch)| batch.column(column).nulls().is_some())
            {
                output_bytes = output_bytes
                    .saturating_add(padded_buffer_size(future_rows.div_ceil(8)));
            }
        }
        if next_batch_may_contribute_values {
            // The replacement's variable-width values are not available yet.
            // Include their full maximum rather than their average row width.
            output_bytes = output_bytes.saturating_add(next_batch_bytes);
        }

        Ok(self
            .batches_mem_used
            .saturating_add(next_batch_bytes)
            .saturating_add(output_bytes)
            > memory_limit)
    }

    fn release_unused_memory(&mut self) {
        // Keep the initial grant to avoid re-admission between output batches.
        let target = self.batches_mem_used.max(self.initial_reservation);
        if self.reservation.size() > target {
            self.reservation.shrink(self.reservation.size() - target);
        }
    }

    /// Try to interleave all columns using the given index slice.
    fn try_interleave_columns(
        &self,
        indices: &[(usize, usize)],
    ) -> Result<Vec<ArrayRef>> {
        (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.column(column_idx).as_ref())
                    .collect();
                // Arrow 58.1.0+ returns OffsetOverflowError directly from
                // interleave, allowing retry_interleave to shrink the batch.
                interleave(&arrays, indices).map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Builds a record batch from the first `rows_to_emit` buffered rows.
    fn finish_record_batch(
        &mut self,
        rows_to_emit: usize,
        columns: Vec<ArrayRef>,
    ) -> Result<RecordBatch> {
        // Remove consumed indices, keeping any remaining for the next call.
        self.indices.drain(..rows_to_emit);

        if self.indices.is_empty() {
            self.retain_cursor_batches();
        } else {
            self.retain_live_batches();
        }

        // Release excess memory back to the pool, but never shrink below
        // initial_reservation to maintain the anti-starvation guarantee
        // for the merge phase.
        self.release_unused_memory();

        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(Into::into)
    }

    fn retain_cursor_batches(&mut self) {
        // New cursors are only created once the previous cursor for the stream
        // is finished. This means all remaining rows from all but the last batch
        // for each stream have been yielded to the newly created record batch
        //
        // We can therefore drop all but the last live cursor batch for each stream
        let mut batch_idx = 0;
        let mut retained = 0;
        self.batches.retain(|(stream_idx, batch)| {
            let stream_cursor = &mut self.cursors[*stream_idx];
            let is_cursor_batch = stream_cursor.batch_idx == batch_idx;
            let retain = is_cursor_batch && stream_cursor.row_idx < batch.num_rows();
            batch_idx += 1;

            if retain {
                stream_cursor.batch_idx = retained;
                retained += 1;
            } else {
                if is_cursor_batch {
                    *stream_cursor = BatchCursor::RELEASED;
                }
                self.batches_mem_used -= get_record_batch_memory_size(batch);
            }
            retain
        });
    }

    fn retain_live_batches(&mut self) {
        let mut retain_batch = vec![false; self.batches.len()];
        for (batch_idx, _) in &self.indices {
            retain_batch[*batch_idx] = true;
        }

        for cursor in &self.cursors {
            if self
                .batches
                .get(cursor.batch_idx)
                .is_some_and(|(_, batch)| cursor.row_idx < batch.num_rows())
            {
                retain_batch[cursor.batch_idx] = true;
            }
        }

        let mut batch_idx = 0;
        let mut retained = 0;
        let mut remap = vec![usize::MAX; self.batches.len()];
        self.batches.retain(|(_, batch)| {
            let retain = retain_batch[batch_idx];
            if retain {
                remap[batch_idx] = retained;
                retained += 1;
            } else {
                self.batches_mem_used -= get_record_batch_memory_size(batch);
            }
            batch_idx += 1;
            retain
        });

        for (batch_idx, _) in &mut self.indices {
            *batch_idx = remap[*batch_idx];
        }
        for cursor in &mut self.cursors {
            if let Some(new_idx) = remap.get(cursor.batch_idx) {
                // `usize::MAX` means the cursor's batch was released.
                cursor.batch_idx = *new_idx;
            }
        }
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output.
    /// If an offset overflow occurs (e.g. string/list offsets exceed i32::MAX),
    /// retries with progressively fewer rows until it succeeds.
    ///
    /// Returns `None` if no pending rows
    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        let (rows_to_emit, columns) =
            retry_interleave(self.indices.len(), self.indices.len(), |rows_to_emit| {
                self.try_interleave_columns(&self.indices[..rows_to_emit])
            })?;

        Ok(Some(self.finish_record_batch(rows_to_emit, columns)?))
    }
}

/// Arrow rounds newly allocated buffers up to a multiple of 64 bytes.
fn padded_buffer_size(bytes: usize) -> usize {
    bytes.checked_add(63).map_or(usize::MAX, |size| size & !63)
}

/// Try to grow `reservation` so it covers at least `needed` bytes.
///
/// When a reservation has been pre-loaded with bytes (e.g. via
/// [`MemoryReservation::take()`]), this avoids redundant pool
/// allocations: if the reservation already covers `needed`, this is
/// a no-op; otherwise only the deficit is requested from the pool.
pub(crate) fn try_grow_reservation_to_at_least(
    reservation: &mut MemoryReservation,
    needed: usize,
) -> Result<()> {
    if needed > reservation.size() {
        reservation.try_grow(needed - reservation.size())?;
    }
    Ok(())
}

/// Returns true if the error is an Arrow offset overflow.
fn is_offset_overflow(e: &DataFusionError) -> bool {
    matches!(
        e,
        DataFusionError::ArrowError(boxed, _)
            if matches!(boxed.as_ref(), ArrowError::OffsetOverflowError(_))
    )
}

#[cfg(test)]
fn offset_overflow_error() -> DataFusionError {
    DataFusionError::ArrowError(Box::new(ArrowError::OffsetOverflowError(0)), None)
}

fn retry_interleave<T, F>(
    mut rows_to_emit: usize,
    total_rows: usize,
    mut interleave: F,
) -> Result<(usize, T)>
where
    F: FnMut(usize) -> Result<T>,
{
    loop {
        match interleave(rows_to_emit) {
            Ok(value) => return Ok((rows_to_emit, value)),
            // Only offset overflow is recoverable by emitting fewer rows.
            Err(e) if is_offset_overflow(&e) => {
                rows_to_emit /= 2;
                if rows_to_emit == 0 {
                    return Err(e);
                }
                warn!(
                    "Interleave offset overflow with {total_rows} rows, retrying with {rows_to_emit}"
                );
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayDataBuilder, Int32Array, Int64Array, ListArray, StringArray,
    };
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_execution::memory_pool::{
        MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };

    fn overflow_list_batch() -> RecordBatch {
        let values_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        // SAFETY: This intentionally constructs an invalid child length so
        // Arrow's interleave hits offset overflow before touching child data.
        let list = ListArray::from(unsafe {
            ArrayDataBuilder::new(DataType::List(Arc::clone(&values_field)))
                .len(1)
                .add_buffer(Buffer::from_slice_ref([0_i32, i32::MAX]))
                .add_child_data(Int32Array::from(Vec::<i32>::new()).to_data())
                .build_unchecked()
        });
        let schema = Arc::new(Schema::new(vec![Field::new(
            "list_col",
            DataType::List(values_field),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap()
    }

    fn reservation() -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        MemoryConsumer::new("test").register(&pool)
    }

    fn int_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn push_n_rows(builder: &mut BatchBuilder, stream_idx: usize, n: usize) {
        for _ in 0..n {
            builder.push_row(stream_idx);
        }
    }

    fn emit_n_rows(builder: &mut BatchBuilder, n: usize) -> RecordBatch {
        let columns = builder
            .try_interleave_columns(&builder.indices[..n])
            .unwrap();
        builder.finish_record_batch(n, columns).unwrap()
    }

    fn assert_int_output(batch: &RecordBatch, expected: &[i32]) {
        let actual = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_partial_emit_releases_unreferenced_and_retains_live_batches() {
        let batch0 = int_batch(vec![10, 11]);
        let batch1 = int_batch(vec![20, 21]);
        let batch2 = int_batch(vec![30, 31]);
        let batch1_size = get_record_batch_memory_size(&batch1);
        let batch2_size = get_record_batch_memory_size(&batch2);
        let schema = batch0.schema();
        let mut builder = BatchBuilder::new(Arc::clone(&schema), 3, 6, reservation());

        builder.push_batch(0, batch0).unwrap();
        push_n_rows(&mut builder, 0, 2);
        builder.push_batch(1, batch1).unwrap();
        push_n_rows(&mut builder, 1, 2);
        // Keep one stream empty so an unloaded cursor cannot retain consumed batches.
        builder.push_batch(0, batch2).unwrap();

        let output = emit_n_rows(&mut builder, 2);
        assert_int_output(&output, &[10, 11]);

        assert_eq!(builder.len(), 2);
        assert_eq!(builder.batches.len(), 2);
        assert_eq!(builder.batches_mem_used, batch1_size + batch2_size);
        assert_eq!(builder.reservation.size(), batch1_size + batch2_size);

        push_n_rows(&mut builder, 0, 2);
        let output = emit_n_rows(&mut builder, 4);
        assert_int_output(&output, &[20, 21, 30, 31]);

        assert!(builder.is_empty());
        assert!(builder.batches.is_empty());
        assert_eq!(builder.batches_mem_used, 0);
        assert_eq!(builder.reservation.size(), 0);
    }

    #[test]
    fn test_released_cursor_accepts_new_batch_for_stream() {
        let batch0 = int_batch(vec![10]);
        let batch1 = int_batch(vec![20]);
        let schema = batch0.schema();
        let mut builder = BatchBuilder::new(Arc::clone(&schema), 1, 1, reservation());

        builder.push_batch(0, batch0).unwrap();
        builder.push_row(0);
        let output = emit_n_rows(&mut builder, 1);
        assert_int_output(&output, &[10]);
        assert!(builder.batches.is_empty());

        builder.push_batch(0, batch1).unwrap();
        builder.push_row(0);
        let output = emit_n_rows(&mut builder, 1);
        assert_int_output(&output, &[20]);
        assert!(builder.batches.is_empty());
        assert_eq!(builder.batches_mem_used, 0);
        assert_eq!(builder.reservation.size(), 0);
    }

    #[test]
    fn test_merge_budget_includes_future_replacement_rows() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let mut builder = BatchBuilder::new(
            Arc::clone(&schema),
            2,
            8192,
            MemoryConsumer::new("test").register(&pool),
        );
        for stream in 0..2 {
            builder.push_batch(
                stream,
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from_iter_values(0..8))],
                )?,
            )?;
        }
        for _ in 0..8 {
            builder.push_row(0);
        }
        // Current output (64 bytes), both inputs (128), and replacement (64)
        // fit. Future output also includes the live input and replacement.
        assert!(builder.should_flush_before_input(64, 256, 8192)?);
        Ok(())
    }

    #[test]
    fn test_merge_budget_includes_unselected_wide_values() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let mut builder = BatchBuilder::new(
            Arc::clone(&schema),
            2,
            3,
            MemoryConsumer::new("test").register(&pool),
        );
        builder.push_batch(
            0,
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec!["a"]))],
            )?,
        )?;
        let wide_value = "z".repeat(4096);
        builder.push_batch(
            1,
            RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["b", wide_value.as_str()]))],
            )?,
        )?;
        builder.push_row(0);
        // Only a small value is pending, but the other live batch can add its
        // large value before the next input boundary. Average widths or only
        // the current pending slice would miss this materialization cost.
        assert!(builder.should_flush_before_input(
            64,
            builder.batches_mem_used + 64 + 256,
            3,
        )?);
        Ok(())
    }

    #[test]
    fn test_retry_interleave_halves_rows_until_success() {
        let mut attempts = Vec::new();

        let (rows_to_emit, result) = retry_interleave(4, 4, |rows_to_emit| {
            attempts.push(rows_to_emit);
            if rows_to_emit > 1 {
                Err(offset_overflow_error())
            } else {
                Ok("ok")
            }
        })
        .unwrap();

        assert_eq!(rows_to_emit, 1);
        assert_eq!(result, "ok");
        assert_eq!(attempts, vec![4, 2, 1]);
    }

    #[test]
    fn test_is_offset_overflow_matches_arrow_error() {
        assert!(is_offset_overflow(&offset_overflow_error()));
    }

    #[test]
    fn test_retry_interleave_does_not_retry_non_offset_errors() {
        let mut attempts = Vec::new();

        let error = retry_interleave(4, 4, |rows_to_emit| {
            attempts.push(rows_to_emit);
            Err::<(), _>(DataFusionError::Execution("boom".into()))
        })
        .unwrap_err();

        assert_eq!(attempts, vec![4]);
        assert!(matches!(error, DataFusionError::Execution(msg) if msg == "boom"));
    }

    #[test]
    fn test_try_interleave_columns_surfaces_arrow_offset_overflow() {
        let batch = overflow_list_batch();
        let schema = batch.schema();
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("test").register(&pool);
        let mut builder = BatchBuilder::new(schema, 1, 2, reservation);
        builder.push_batch(0, batch).unwrap();

        let error = builder
            .try_interleave_columns(&[(0, 0), (0, 0)])
            .unwrap_err();

        assert!(is_offset_overflow(&error));
    }
}
