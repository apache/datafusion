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
use arrow::array::types::{
    BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type,
};
use arrow::array::{Array, ArrayRef, GenericByteArray};
use arrow::compute::interleave;
use arrow::datatypes::{ArrowNativeType, DataType, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use log::{debug, warn};
use std::mem::size_of;
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

    /// Accounts for the memory budget needed while constructing an output batch.
    output_construction_reservation: MemoryReservation,

    /// Tracks the actual memory used by buffered batches (not including
    /// pre-reserved bytes). This allows [`Self::push_batch`] to skip pool
    /// allocation requests when the pre-reserved bytes cover the batch.
    batches_mem_used: usize,

    /// The initial reservation size at construction time. When the reservation
    /// is pre-loaded with `sort_spill_reservation_bytes` (via `take()`), this
    /// records that amount so we never shrink below it, maintaining the
    /// anti-starvation guarantee throughout the merge.
    initial_reservation: usize,

    /// Output-construction workspace to retain across output batches.
    ///
    /// Spill merges can opt into retaining the workspace admitted before the
    /// merge starts so another consumer cannot take it between stream polls.
    initial_output_construction_reservation: usize,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<BatchCursor>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_idx, row_idx)`
    indices: Vec<(usize, usize)>,

    /// Optional target memory size for output batches.
    target_batch_bytes: Option<usize>,
}

impl BatchBuilder {
    /// Create a new [`BatchBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(
        schema: SchemaRef,
        stream_count: usize,
        batch_size: usize,
        reservation: MemoryReservation,
        output_construction_reservation: Option<MemoryReservation>,
        target_batch_bytes: Option<usize>,
    ) -> Self {
        let initial_reservation = reservation.size();
        let target_batch_bytes =
            target_batch_bytes.filter(|_| schema_supports_byte_target(&schema));
        let output_construction_reservation = if target_batch_bytes.is_some() {
            output_construction_reservation.unwrap_or_else(|| reservation.new_empty())
        } else {
            reservation.new_empty()
        };
        Self {
            schema,
            batches: Vec::with_capacity(stream_count * 2),
            cursors: vec![BatchCursor::RELEASED; stream_count],
            indices: Vec::with_capacity(batch_size),
            reservation,
            output_construction_reservation,
            batches_mem_used: 0,
            initial_reservation,
            initial_output_construction_reservation: 0,
            target_batch_bytes,
        }
    }

    /// Retain any output-construction reservation supplied at creation time
    /// across successful output batches.
    pub(crate) fn retain_output_construction_reservation(mut self) -> Self {
        self.initial_output_construction_reservation =
            self.output_construction_reservation.size();
        self
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
        let batch = match RecordBatch::try_new(Arc::clone(&self.schema), columns) {
            Ok(batch) => batch,
            Err(e) => {
                self.output_construction_reservation.free();
                return Err(e.into());
            }
        };
        #[cfg(debug_assertions)]
        if self.target_batch_bytes.is_some() {
            let actual_size = get_record_batch_memory_size(&batch);
            // A target-enabled output reaches this point only after its estimate
            // succeeded and the reservation was grown before interleave. Every
            // supported estimate upper-bounds Arrow's output buffer capacities,
            // so actual <= estimated <= the reservation already acquired.
            debug_assert!(
                actual_size <= self.output_construction_reservation.size(),
                "sort output used {actual_size} bytes after reserving {} bytes",
                self.output_construction_reservation.size()
            );
        }

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
        let target = self.batches_mem_used.max(self.initial_reservation);
        if self.reservation.size() > target {
            self.reservation.shrink(self.reservation.size() - target);
        }
        if self.output_construction_reservation.size()
            > self.initial_output_construction_reservation
        {
            self.output_construction_reservation.shrink(
                self.output_construction_reservation.size()
                    - self.initial_output_construction_reservation,
            );
        }

        Ok(batch)
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

        let Some(target_batch_bytes) = self.target_batch_bytes else {
            let (rows_to_emit, columns) =
                retry_interleave(self.indices.len(), |rows_to_emit| {
                    self.try_interleave_columns(&self.indices[..rows_to_emit])
                })?;

            return Ok(Some(self.finish_record_batch(rows_to_emit, columns)?));
        };
        let Some((mut rows_to_emit, mut estimated_bytes)) =
            self.prefix_under_target(target_batch_bytes)
        else {
            return Err(self.fail_prefix_estimation());
        };

        loop {
            match try_grow_reservation_to_at_least(
                &mut self.output_construction_reservation,
                estimated_bytes,
            ) {
                Ok(()) => break,
                Err(_) if rows_to_emit > 1 => {
                    let failed_bytes = estimated_bytes;
                    rows_to_emit /= 2;
                    estimated_bytes = self
                        .estimated_prefix_bytes(rows_to_emit)
                        .expect("a smaller prefix has the same supported arrays");
                    debug!(
                        "Could not reserve {failed_bytes} bytes for sort output, retrying with {rows_to_emit} rows requiring {estimated_bytes} bytes"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        let (rows_to_emit, columns) =
            match retry_interleave(rows_to_emit, |rows_to_emit| {
                self.try_interleave_columns(&self.indices[..rows_to_emit])
            }) {
                Ok(value) => value,
                Err(e) => {
                    self.output_construction_reservation.free();
                    return Err(e);
                }
            };

        Ok(Some(self.finish_record_batch(rows_to_emit, columns)?))
    }

    /// Release output memory and classify a failed target-enabled prefix estimate.
    ///
    /// The checked arithmetic in the estimator is the only expected failure
    /// after schema validation. A byte-array downcast failure instead means the
    /// builder's schema and buffered arrays are internally inconsistent.
    fn fail_prefix_estimation(&mut self) -> DataFusionError {
        let type_mismatch =
            self.batches
                .iter()
                .enumerate()
                .find_map(|(batch_idx, (_, batch))| {
                    self.schema.fields.iter().enumerate().find_map(
                        |(column_idx, field)| {
                            let array = batch.column(column_idx);
                            let matches_expected_type = match field.data_type() {
                                DataType::Binary => array
                                    .as_any()
                                    .downcast_ref::<GenericByteArray<BinaryType>>()
                                    .is_some(),
                                DataType::LargeBinary => array
                                    .as_any()
                                    .downcast_ref::<GenericByteArray<LargeBinaryType>>()
                                    .is_some(),
                                DataType::Utf8 => array
                                    .as_any()
                                    .downcast_ref::<GenericByteArray<Utf8Type>>()
                                    .is_some(),
                                DataType::LargeUtf8 => array
                                    .as_any()
                                    .downcast_ref::<GenericByteArray<LargeUtf8Type>>()
                                    .is_some(),
                                _ => true,
                            };
                            (!matches_expected_type).then_some((
                                batch_idx,
                                column_idx,
                                field.data_type().clone(),
                                array.data_type().clone(),
                            ))
                        },
                    )
                });
        self.output_construction_reservation.free();

        if let Some((batch_idx, column_idx, expected, actual)) = type_mismatch {
            DataFusionError::Internal(format!(
                "Sort output size estimate expected {expected} at batch {batch_idx}, column {column_idx}, but found {actual}"
            ))
        } else {
            DataFusionError::ResourcesExhausted(
                "Sort output size estimate overflow".to_string(),
            )
        }
    }

    fn prefix_under_target(&self, target_batch_bytes: usize) -> Option<(usize, usize)> {
        let has_byte_arrays = self.schema.fields.iter().any(|field| {
            matches!(
                field.data_type(),
                DataType::Binary
                    | DataType::LargeBinary
                    | DataType::Utf8
                    | DataType::LargeUtf8
            )
        });
        if has_byte_arrays {
            return self.variable_width_prefix_under_target(target_batch_bytes);
        }

        let mut estimated_bytes = self.estimated_prefix_bytes(self.indices.len())?;
        let rows_to_emit =
            if self.indices.len() <= 1 || estimated_bytes <= target_batch_bytes {
                self.indices.len()
            } else {
                self.largest_prefix_under_target(target_batch_bytes)?
            };
        if rows_to_emit != self.indices.len() {
            estimated_bytes = self.estimated_prefix_bytes(rows_to_emit)?;
        }
        Some((rows_to_emit, estimated_bytes))
    }

    fn largest_prefix_under_target(&self, target_batch_bytes: usize) -> Option<usize> {
        if self.estimated_prefix_bytes(1)? > target_batch_bytes {
            return Some(1);
        }

        let mut low = 1;
        let mut high = self.indices.len();
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if self.estimated_prefix_bytes(mid)? <= target_batch_bytes {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Some(low)
    }

    fn variable_width_prefix_under_target(
        &self,
        target_batch_bytes: usize,
    ) -> Option<(usize, usize)> {
        let mut byte_array_values = vec![0usize; self.schema.fields.len()];
        let has_nulls = (0..self.schema.fields.len())
            .map(|column_idx| {
                self.batches
                    .iter()
                    .any(|(_, batch)| batch.column(column_idx).null_count() > 0)
            })
            .collect::<Vec<_>>();
        let mut previous_bytes = 0usize;

        // Every supported estimate is monotonically non-decreasing with the
        // prefix length, so the first prefix over the target identifies the
        // same largest prefix as a binary search over full-prefix estimates.
        for rows_to_emit in 1..=self.indices.len() {
            let (batch_idx, row_idx) = self.indices[rows_to_emit - 1];
            let bitmap_bytes = bitmap_buffer_bytes(rows_to_emit)?;
            let mut total = 0usize;
            for (column_idx, field) in self.schema.fields.iter().enumerate() {
                let validity_bytes = if has_nulls[column_idx] {
                    bitmap_bytes
                } else {
                    0
                };
                let column_bytes = match field.data_type() {
                    DataType::Binary => self
                        .incremental_byte_array_memory_upper_bound::<BinaryType>(
                            column_idx,
                            rows_to_emit,
                            batch_idx,
                            row_idx,
                            &mut byte_array_values[column_idx],
                            validity_bytes,
                        )?,
                    DataType::LargeBinary => self
                        .incremental_byte_array_memory_upper_bound::<LargeBinaryType>(
                            column_idx,
                            rows_to_emit,
                            batch_idx,
                            row_idx,
                            &mut byte_array_values[column_idx],
                            validity_bytes,
                        )?,
                    DataType::Utf8 => self
                        .incremental_byte_array_memory_upper_bound::<Utf8Type>(
                            column_idx,
                            rows_to_emit,
                            batch_idx,
                            row_idx,
                            &mut byte_array_values[column_idx],
                            validity_bytes,
                        )?,
                    DataType::LargeUtf8 => self
                        .incremental_byte_array_memory_upper_bound::<LargeUtf8Type>(
                            column_idx,
                            rows_to_emit,
                            batch_idx,
                            row_idx,
                            &mut byte_array_values[column_idx],
                            validity_bytes,
                        )?,
                    _ => self.column_prefix_memory_upper_bound_with_validity(
                        column_idx,
                        rows_to_emit,
                        validity_bytes,
                    )?,
                };
                total = total.checked_add(column_bytes)?;
            }

            if total > target_batch_bytes {
                return if rows_to_emit == 1 {
                    Some((1, total))
                } else {
                    Some((rows_to_emit - 1, previous_bytes))
                };
            }
            previous_bytes = total;
        }

        Some((self.indices.len(), previous_bytes))
    }

    fn estimated_prefix_bytes(&self, rows_to_emit: usize) -> Option<usize> {
        let mut total = 0usize;
        for column_idx in 0..self.schema.fields.len() {
            total = total.checked_add(
                self.column_prefix_memory_upper_bound(column_idx, rows_to_emit)?,
            )?;
        }
        Some(total)
    }

    fn column_prefix_memory_upper_bound(
        &self,
        column_idx: usize,
        rows_to_emit: usize,
    ) -> Option<usize> {
        let validity_bytes = self.validity_buffer_bytes(column_idx, rows_to_emit)?;
        self.column_prefix_memory_upper_bound_with_validity(
            column_idx,
            rows_to_emit,
            validity_bytes,
        )
    }

    fn column_prefix_memory_upper_bound_with_validity(
        &self,
        column_idx: usize,
        rows_to_emit: usize,
        validity_bytes: usize,
    ) -> Option<usize> {
        let data_type = self.schema.field(column_idx).data_type();

        match data_type {
            DataType::Null => Some(0),
            DataType::Boolean => {
                bitmap_buffer_bytes(rows_to_emit)?.checked_add(validity_bytes)
            }
            DataType::Binary => self.byte_array_prefix_memory_upper_bound::<BinaryType>(
                column_idx,
                rows_to_emit,
                validity_bytes,
            ),
            DataType::LargeBinary => self
                .byte_array_prefix_memory_upper_bound::<LargeBinaryType>(
                    column_idx,
                    rows_to_emit,
                    validity_bytes,
                ),
            DataType::Utf8 => self.byte_array_prefix_memory_upper_bound::<Utf8Type>(
                column_idx,
                rows_to_emit,
                validity_bytes,
            ),
            DataType::LargeUtf8 => self
                .byte_array_prefix_memory_upper_bound::<LargeUtf8Type>(
                    column_idx,
                    rows_to_emit,
                    validity_bytes,
                ),
            DataType::FixedSizeBinary(width) => usize::try_from(*width)
                .ok()
                .and_then(|width| {
                    rows_to_emit
                        .checked_mul(width)
                        .and_then(aligned_buffer_bytes)
                })
                .and_then(|values| validity_bytes.checked_add(values)),
            _ => fixed_width(data_type).and_then(|width| {
                rows_to_emit
                    .checked_mul(width)
                    .and_then(|values| validity_bytes.checked_add(values))
            }),
        }
    }

    /// Arrow allocates an output validity buffer when any input array for this
    /// column contains nulls, even if the selected rows are all valid.
    fn validity_buffer_bytes(
        &self,
        column_idx: usize,
        rows_to_emit: usize,
    ) -> Option<usize> {
        if self
            .batches
            .iter()
            .any(|(_, batch)| batch.column(column_idx).null_count() > 0)
        {
            bitmap_buffer_bytes(rows_to_emit)
        } else {
            Some(0)
        }
    }

    fn byte_array_prefix_memory_upper_bound<T: ByteArrayType>(
        &self,
        column_idx: usize,
        rows_to_emit: usize,
        validity_bytes: usize,
    ) -> Option<usize> {
        let mut values_len = 0usize;
        for (batch_idx, row_idx) in &self.indices[..rows_to_emit] {
            values_len = values_len.checked_add(
                self.byte_array_value_length::<T>(column_idx, *batch_idx, *row_idx)?,
            )?;
        }

        Self::byte_array_memory_upper_bound::<T>(rows_to_emit, values_len, validity_bytes)
    }

    fn incremental_byte_array_memory_upper_bound<T: ByteArrayType>(
        &self,
        column_idx: usize,
        rows_to_emit: usize,
        batch_idx: usize,
        row_idx: usize,
        values_len: &mut usize,
        validity_bytes: usize,
    ) -> Option<usize> {
        *values_len = values_len.checked_add(
            self.byte_array_value_length::<T>(column_idx, batch_idx, row_idx)?,
        )?;
        Self::byte_array_memory_upper_bound::<T>(
            rows_to_emit,
            *values_len,
            validity_bytes,
        )
    }

    fn byte_array_value_length<T: ByteArrayType>(
        &self,
        column_idx: usize,
        batch_idx: usize,
        row_idx: usize,
    ) -> Option<usize> {
        let array = self.batches[batch_idx].1.column(column_idx);
        let array = array.as_any().downcast_ref::<GenericByteArray<T>>()?;
        Some(array.value_length(row_idx).as_usize())
    }

    fn byte_array_memory_upper_bound<T: ByteArrayType>(
        rows_to_emit: usize,
        values_len: usize,
        validity_bytes: usize,
    ) -> Option<usize> {
        validity_bytes
            .checked_add(
                rows_to_emit
                    .checked_add(1)?
                    .checked_mul(size_of::<T::Offset>())?,
            )?
            .checked_add(values_len)
    }
}

pub(super) fn schema_supports_byte_target(schema: &SchemaRef) -> bool {
    schema
        .fields()
        .iter()
        .all(|field| data_type_supports_byte_target(field.data_type()))
}

/// Return the output-construction target for a spilled run.
///
/// Spill metadata records the largest output-construction footprint inferred
/// from the serialized input batches. For a wholly fixed-width schema, also
/// reserve enough to emit the configured row batch size without an unnecessary
/// byte cap.
pub(super) fn spill_output_batch_targets(
    schema: &SchemaRef,
    max_batch_rows: usize,
    max_record_batch_memory: usize,
) -> Option<(usize, usize)> {
    if !schema_supports_byte_target(schema) {
        return None;
    }

    let preferred_target = fixed_width_schema_batch_bytes(schema, max_batch_rows)
        .map_or(max_record_batch_memory, |fixed_width_target| {
            max_record_batch_memory.max(fixed_width_target)
        });
    Some((max_record_batch_memory, preferred_target))
}

/// Adjust compact spill accounting to match the buffers Arrow interleave
/// allocates for the same batch. Variable-width and ordinary fixed-width
/// values use exact-capacity `Vec`s; bitmaps and fixed-size binary values use
/// 64-byte-aligned buffers.
pub(crate) fn interleave_memory_size_from_sliced(
    batch: &RecordBatch,
    sliced_size: usize,
) -> Option<usize> {
    if !schema_supports_byte_target(&batch.schema()) {
        return None;
    }

    let rows = batch.num_rows();
    batch
        .columns()
        .iter()
        .try_fold(sliced_size, |total, array| {
            let logical_values = match array.data_type() {
                DataType::Boolean => validity_bytes(rows),
                DataType::FixedSizeBinary(width) if *width >= 0 => {
                    rows.checked_mul(usize::try_from(*width).ok()?)?
                }
                _ => 0,
            };
            let aligned_values = match array.data_type() {
                DataType::Boolean => bitmap_buffer_bytes(rows)?,
                DataType::FixedSizeBinary(_) => aligned_buffer_bytes(logical_values)?,
                _ => logical_values,
            };
            let value_padding = aligned_values.checked_sub(logical_values)?;
            let validity_padding = if !matches!(array.data_type(), DataType::Null)
                && array.null_count() > 0
            {
                bitmap_buffer_bytes(rows)?.checked_sub(validity_bytes(rows))?
            } else {
                0
            };

            total
                .checked_add(value_padding)?
                .checked_add(validity_padding)
        })
}

fn fixed_width_schema_batch_bytes(schema: &SchemaRef, rows: usize) -> Option<usize> {
    schema.fields().iter().try_fold(0usize, |total, field| {
        if matches!(field.data_type(), DataType::Null) {
            return Some(total);
        }
        let values = match field.data_type() {
            DataType::Boolean => bitmap_buffer_bytes(rows)?,
            DataType::FixedSizeBinary(width) if *width >= 0 => rows
                .checked_mul(usize::try_from(*width).ok()?)
                .and_then(aligned_buffer_bytes)?,
            data_type => rows.checked_mul(fixed_width(data_type)?)?,
        };
        let validity = if field.is_nullable() {
            bitmap_buffer_bytes(rows)?
        } else {
            0
        };
        total.checked_add(values)?.checked_add(validity)
    })
}

fn data_type_supports_byte_target(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
    ) || matches!(data_type, DataType::FixedSizeBinary(width) if *width >= 0)
        || fixed_width(data_type).is_some()
}

fn validity_bytes(rows: usize) -> usize {
    rows.div_ceil(8)
}

fn bitmap_buffer_bytes(rows: usize) -> Option<usize> {
    aligned_buffer_bytes(validity_bytes(rows))
}

fn aligned_buffer_bytes(bytes: usize) -> Option<usize> {
    bytes.checked_add(63)?.checked_div(64)?.checked_mul(64)
}

fn fixed_width(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_) => Some(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => Some(8),
        DataType::Decimal32(_, _) => Some(4),
        DataType::Decimal64(_, _) => Some(8),
        DataType::Decimal128(_, _) => Some(16),
        DataType::Decimal256(_, _) => Some(32),
        _ => None,
    }
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
                let failed_rows = rows_to_emit;
                rows_to_emit /= 2;
                if rows_to_emit == 0 {
                    return Err(e);
                }
                warn!(
                    "Interleave offset overflow with {failed_rows} rows, retrying with {rows_to_emit}"
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
        Array, ArrayDataBuilder, BinaryArray, BooleanArray, FixedSizeBinaryArray,
        Int32Array, Int64Array, ListArray, NullArray, StringArray, StringViewArray,
        StructArray,
    };
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
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

    fn assert_bool_output(batch: &RecordBatch, expected: &[Option<bool>]) {
        let actual = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(actual.len(), expected.len());
        for (idx, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(!actual.is_null(idx), "row {idx} should be valid");
                    assert_eq!(actual.value(idx), *expected, "row {idx}");
                }
                None => assert!(actual.is_null(idx), "row {idx} should be null"),
            }
        }
    }

    fn fixed_size_binary_batch(rows: usize) -> RecordBatch {
        let values = (0..rows)
            .map(|idx| {
                if idx % 2 == 0 {
                    Some(vec![(idx % 251) as u8])
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let fixed =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 1)
                .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "f",
            DataType::FixedSizeBinary(1),
            true,
        )]));
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(fixed)]).unwrap()
    }

    fn unsupported_mixed_batch() -> RecordBatch {
        let supported = BinaryArray::from_vec(vec![
            b"wide-value-1".as_slice(),
            b"wide-value-2".as_slice(),
        ]);
        let nested_view = StringViewArray::from(vec!["nested-1", "nested-2"]);
        let struct_fields: Fields =
            vec![Arc::new(Field::new("nested", DataType::Utf8View, false))].into();
        let nested = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(nested_view) as _],
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Binary, false),
            Field::new("s", DataType::Struct(struct_fields), false),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(supported), Arc::new(nested)]).unwrap()
    }

    fn assert_fixed_size_binary_output(
        batch: &RecordBatch,
        first_input_row: usize,
        expected_rows: usize,
    ) {
        let actual = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(actual.len(), expected_rows);
        for idx in 0..expected_rows {
            let input_row = first_input_row + idx;
            if input_row.is_multiple_of(2) {
                assert!(!actual.is_null(idx), "row {input_row} should be valid");
                assert_eq!(actual.value(idx), &[(input_row % 251) as u8]);
            } else {
                assert!(actual.is_null(idx), "row {input_row} should be null");
            }
        }
    }

    #[test]
    fn test_partial_emit_releases_unreferenced_and_retains_live_batches() {
        let batch0 = int_batch(vec![10, 11]);
        let batch1 = int_batch(vec![20, 21]);
        let batch2 = int_batch(vec![30, 31]);
        let batch1_size = get_record_batch_memory_size(&batch1);
        let batch2_size = get_record_batch_memory_size(&batch2);
        let schema = batch0.schema();
        let mut builder =
            BatchBuilder::new(Arc::clone(&schema), 3, 6, reservation(), None, None);

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
        let mut builder =
            BatchBuilder::new(Arc::clone(&schema), 1, 1, reservation(), None, None);

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
    fn test_retry_interleave_halves_rows_until_success() {
        let mut attempts = Vec::new();

        let (rows_to_emit, result) = retry_interleave(4, |rows_to_emit| {
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

        let error = retry_interleave(4, |rows_to_emit| {
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
        let mut builder = BatchBuilder::new(schema, 1, 2, reservation, None, None);
        builder.push_batch(0, batch).unwrap();

        let error = builder
            .try_interleave_columns(&[(0, 0), (0, 0)])
            .unwrap_err();

        assert!(is_offset_overflow(&error));
    }

    #[test]
    fn test_byte_target_emits_largest_supported_prefix() {
        let batch = int_batch(vec![1, 2, 3, 4]);
        let schema = batch.schema();
        let target = 2 * size_of::<i32>();
        let mut builder = BatchBuilder::new(
            Arc::clone(&schema),
            1,
            4,
            reservation(),
            None,
            Some(target),
        );
        builder.push_batch(0, batch).unwrap();
        push_n_rows(&mut builder, 0, 4);

        let output = builder.build_record_batch().unwrap().unwrap();

        assert_int_output(&output, &[1, 2]);
        assert_eq!(builder.len(), 2);
        assert_eq!(builder.output_construction_reservation.size(), 0);
    }

    #[test]
    fn test_forward_prefix_scan_matches_full_estimator() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("i", DataType::Int32, false),
        ]));
        let batch0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("longer")])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("xy"), Some("end")])),
                Arc::new(Int32Array::from(vec![3, 4])),
            ],
        )
        .unwrap();
        let mut builder =
            BatchBuilder::new(schema, 2, 4, reservation(), None, Some(usize::MAX));
        builder.push_batch(0, batch0).unwrap();
        push_n_rows(&mut builder, 0, 2);
        builder.push_batch(1, batch1).unwrap();
        push_n_rows(&mut builder, 1, 2);
        let estimates = (1..=4)
            .map(|rows| builder.estimated_prefix_bytes(rows).unwrap())
            .collect::<Vec<_>>();

        for target in [
            0,
            estimates[0],
            estimates[1] - 1,
            estimates[1],
            estimates[2],
            estimates[3],
            usize::MAX,
        ] {
            let expected_rows = estimates
                .iter()
                .rposition(|bytes| *bytes <= target)
                .map_or(1, |index| index + 1);
            assert_eq!(
                builder.prefix_under_target(target),
                Some((expected_rows, estimates[expected_rows - 1])),
                "target {target}"
            );
        }
    }

    #[test]
    fn test_byte_target_estimate_covers_null_and_byte_array_buffers() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Null, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(NullArray::new(2)),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        let mut builder =
            BatchBuilder::new(schema, 1, 2, reservation(), None, Some(usize::MAX));
        builder.push_batch(0, batch).unwrap();
        push_n_rows(&mut builder, 0, 2);
        let estimated_bytes = builder.estimated_prefix_bytes(2).unwrap();

        let output = builder.build_record_batch().unwrap().unwrap();

        assert_eq!(output.num_rows(), 2);
        assert!(get_record_batch_memory_size(&output) <= estimated_bytes);
    }

    #[test]
    fn test_prefix_estimation_failure_reports_overflow_and_releases_reservation() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        output_reservation.try_grow(8).unwrap();
        let mut builder = BatchBuilder::new(
            schema,
            1,
            1,
            reservation(),
            Some(output_reservation),
            Some(8),
        );

        let error = builder.fail_prefix_estimation();

        assert!(matches!(
            error,
            DataFusionError::ResourcesExhausted(message)
                if message == "Sort output size estimate overflow"
        ));
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_prefix_estimation_type_mismatch_fails_before_interleave() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let binary_schema =
            Arc::new(Schema::new(vec![Field::new("s", DataType::Binary, false)]));
        let binary_batch = RecordBatch::try_new(
            binary_schema,
            vec![Arc::new(BinaryArray::from_vec(vec![b"value".as_slice()]))],
        )
        .unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        output_reservation.try_grow(64).unwrap();
        let mut builder = BatchBuilder::new(
            schema,
            1,
            1,
            reservation(),
            Some(output_reservation),
            Some(64),
        );
        // Construct an internally inconsistent state that RecordBatch validation
        // normally prevents, so the estimator's downcast failure is reachable.
        builder.batches.push((0, binary_batch));
        builder.indices.push((0, 0));

        let error = builder.build_record_batch().unwrap_err();

        assert!(matches!(
            error,
            DataFusionError::Internal(message)
                if message.contains("expected Utf8") && message.contains("found Binary")
        ));
        assert_eq!(builder.indices, vec![(0, 0)]);
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_spill_output_target_aligns_validity_bitmap() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let rows = 1000;
        let spill_bytes = rows * size_of::<i32>() + bitmap_buffer_bytes(rows).unwrap();

        assert_eq!(
            spill_output_batch_targets(&schema, rows, spill_bytes),
            Some((spill_bytes, spill_bytes))
        );
    }

    #[test]
    fn test_interleave_memory_size_aligns_spill_buffers() {
        let rows = 1000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("b", DataType::Boolean, false),
            Field::new("f", DataType::FixedSizeBinary(3), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter(
                    (0..rows).map(|idx| (idx != 1).then_some(idx as i32)),
                )),
                Arc::new(BooleanArray::from(vec![true; rows])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter((0..rows).map(|_| [1_u8, 2, 3]))
                        .unwrap(),
                ),
            ],
        )
        .unwrap();
        let compact_size = rows * size_of::<i32>()
            + validity_bytes(rows)
            + validity_bytes(rows)
            + rows * 3;
        let expected_size = rows * size_of::<i32>()
            + bitmap_buffer_bytes(rows).unwrap()
            + bitmap_buffer_bytes(rows).unwrap()
            + aligned_buffer_bytes(rows * 3).unwrap();

        assert_eq!(
            interleave_memory_size_from_sliced(&batch, compact_size),
            Some(expected_size)
        );
    }

    #[test]
    fn test_spill_output_target_keeps_fixed_width_row_batch_size() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));

        assert_eq!(
            spill_output_batch_targets(&schema, 100, size_of::<i64>()),
            Some((size_of::<i64>(), 100 * size_of::<i64>()))
        );
    }

    #[test]
    fn test_fixed_width_batch_bytes_aligns_boolean_and_fixed_size_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("null", DataType::Null, true),
            Field::new("bool", DataType::Boolean, true),
            Field::new("bytes", DataType::FixedSizeBinary(3), true),
        ]));
        let rows = 65;

        assert_eq!(
            fixed_width_schema_batch_bytes(&schema, rows),
            Some(
                bitmap_buffer_bytes(rows).unwrap() * 3
                    + aligned_buffer_bytes(rows * 3).unwrap()
            )
        );
    }

    #[test]
    fn test_byte_target_non_null_int64_uses_actual_eight_byte_estimate() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let make_builder = |pool_size| {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
            let output_reservation = MemoryConsumer::new("output").register(&pool);
            let mut builder = BatchBuilder::new(
                Arc::clone(&schema),
                1,
                1,
                reservation(),
                Some(output_reservation),
                Some(size_of::<i64>()),
            );
            builder.push_batch(0, batch.clone()).unwrap();
            builder.push_row(0);
            (builder, pool)
        };

        let (mut builder, pool) = make_builder(size_of::<i64>() - 1);
        let error = builder.build_record_batch().unwrap_err();
        assert!(error.to_string().contains("Resources exhausted"));
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);

        let (mut builder, pool) = make_builder(size_of::<i64>());

        assert_eq!(builder.estimated_prefix_bytes(1), Some(size_of::<i64>()));
        let output = builder.build_record_batch().unwrap().unwrap();

        assert_eq!(get_record_batch_memory_size(&output), size_of::<i64>());
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_byte_target_validity_follows_all_input_arrays() {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let output_budget = size_of::<i64>() + bitmap_buffer_bytes(1).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(output_budget));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        let mut builder = BatchBuilder::new(
            schema,
            1,
            1,
            reservation(),
            Some(output_reservation),
            Some(output_budget),
        );
        builder.push_batch(0, batch).unwrap();
        // Only select the valid row. Arrow still allocates validity because the
        // input array contains a null in another row.
        builder.push_row(0);

        assert_eq!(builder.estimated_prefix_bytes(1), Some(output_budget));
        let output = builder.build_record_batch().unwrap().unwrap();

        let actual_size = get_record_batch_memory_size(&output);
        assert!(actual_size > size_of::<i64>());
        assert!(actual_size <= output_budget);
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_byte_target_can_retain_admitted_output_workspace() {
        let batch = int_batch(vec![1, 2]);
        let schema = batch.schema();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        output_reservation.try_grow(128).unwrap();
        let mut builder = BatchBuilder::new(
            schema,
            1,
            2,
            reservation(),
            Some(output_reservation),
            Some(128),
        )
        .retain_output_construction_reservation();
        builder.push_batch(0, batch).unwrap();
        push_n_rows(&mut builder, 0, 2);

        let output = builder.build_record_batch().unwrap().unwrap();

        assert_eq!(output.num_rows(), 2);
        assert_eq!(builder.output_construction_reservation.size(), 128);
        assert_eq!(pool.reserved(), 128);
        drop(builder);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_byte_target_boolean_and_nullable_estimate_covers_aligned_bitmaps() {
        let bools = BooleanArray::from(vec![Some(true), None]);
        let ints = Int32Array::from(vec![Some(10), None]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(bools), Arc::new(ints)],
        )
        .unwrap();
        let output_budget = bitmap_buffer_bytes(2).unwrap() * 3 + 2 * size_of::<i32>();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(output_budget));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        let mut builder = BatchBuilder::new(
            schema,
            1,
            2,
            reservation(),
            Some(output_reservation),
            Some(output_budget),
        );
        builder.push_batch(0, batch).unwrap();
        push_n_rows(&mut builder, 0, 2);

        let output = builder.build_record_batch().unwrap().unwrap();

        assert_eq!(output.num_rows(), 2);
        assert!(builder.is_empty());
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_byte_target_boolean_equal_target_keeps_largest_prefix() {
        let expected = (0..513)
            .map(|idx| {
                if idx % 5 == 0 {
                    None
                } else {
                    Some(idx % 2 == 0)
                }
            })
            .collect::<Vec<_>>();
        let bools = BooleanArray::from(expected.clone());
        let schema =
            Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, true)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(bools)]).unwrap();
        let target = bitmap_buffer_bytes(512).unwrap() * 2;
        let mut builder =
            BatchBuilder::new(schema, 1, 513, reservation(), None, Some(target));
        builder.push_batch(0, batch).unwrap();
        push_n_rows(&mut builder, 0, 513);

        let output = builder.build_record_batch().unwrap().unwrap();

        assert_eq!(output.num_rows(), 512);
        assert_bool_output(&output, &expected[..512]);
        assert_eq!(builder.len(), 1);
        assert_eq!(builder.output_construction_reservation.size(), 0);

        let output = builder.build_record_batch().unwrap().unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_bool_output(&output, &expected[512..]);
        assert!(builder.build_record_batch().unwrap().is_none());
    }

    #[test]
    fn test_byte_target_fixed_size_binary_estimate_covers_aligned_values() {
        let batch = fixed_size_binary_batch(511);
        let schema = batch.schema();
        let output_budget =
            bitmap_buffer_bytes(511).unwrap() + aligned_buffer_bytes(511).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(output_budget));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        let mut builder = BatchBuilder::new(
            schema,
            1,
            511,
            reservation(),
            Some(output_reservation),
            Some(output_budget),
        );
        builder.push_batch(0, batch).unwrap();
        push_n_rows(&mut builder, 0, 511);
        assert_eq!(builder.estimated_prefix_bytes(511), Some(output_budget));

        let output = builder.build_record_batch().unwrap().unwrap();

        assert_eq!(output.num_rows(), 511);
        assert_fixed_size_binary_output(&output, 0, 511);
        assert_eq!(get_record_batch_memory_size(&output), output_budget);
        assert!(builder.is_empty());
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_byte_target_fixed_size_binary_tight_pool_splits_before_allocating() {
        let batch = fixed_size_binary_batch(511);
        let schema = batch.schema();
        let target = bitmap_buffer_bytes(511).unwrap() + 511;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(target));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        let mut builder = BatchBuilder::new(
            schema,
            1,
            511,
            reservation(),
            Some(output_reservation),
            Some(target),
        );
        builder.push_batch(0, batch).unwrap();
        push_n_rows(&mut builder, 0, 511);

        let mut emitted = 0;
        while let Some(output) = builder.build_record_batch().unwrap() {
            let rows = output.num_rows();
            assert!(rows > 0);
            assert!(rows < 511, "575 bytes cannot hold the full 511-row batch");
            assert!(get_record_batch_memory_size(&output) <= target);
            assert_fixed_size_binary_output(&output, emitted, rows);
            emitted += rows;
        }

        assert_eq!(emitted, 511);
        assert!(builder.is_empty());
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_byte_target_falls_back_for_unsupported_mixed_schema() {
        let batch = unsupported_mixed_batch();
        let schema = batch.schema();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        output_reservation.try_grow(1).unwrap();
        let mut builder = BatchBuilder::new(
            schema,
            1,
            2,
            reservation(),
            Some(output_reservation),
            Some(1),
        );
        assert_eq!(builder.target_batch_bytes, None);
        assert_eq!(pool.reserved(), 0);
        builder.push_batch(0, batch).unwrap();
        push_n_rows(&mut builder, 0, 2);

        let output = builder.build_record_batch().unwrap().unwrap();

        assert_eq!(output.num_rows(), 2);
        assert!(builder.is_empty());
        assert_eq!(builder.output_construction_reservation.size(), 0);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn test_unsupported_schema_stays_unaccounted_after_first_batch() {
        let first = unsupported_mixed_batch();
        let second = unsupported_mixed_batch();
        let schema = first.schema();
        let pool_capacity = get_record_batch_memory_size(&first);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_capacity));
        let output_reservation = MemoryConsumer::new("output").register(&pool);
        output_reservation.try_grow(pool_capacity).unwrap();
        let mut builder = BatchBuilder::new(
            schema,
            1,
            2,
            reservation(),
            Some(output_reservation),
            Some(1),
        );

        builder.push_batch(0, first).unwrap();
        push_n_rows(&mut builder, 0, 2);
        let output = builder.build_record_batch().unwrap().unwrap();
        let output_size = get_record_batch_memory_size(&output);
        assert!(output_size <= pool_capacity);
        assert_eq!(pool.reserved(), 0);

        let contender = MemoryConsumer::new("contender").register(&pool);
        contender.try_grow(pool_capacity).unwrap();
        builder.push_batch(0, second).unwrap();
        push_n_rows(&mut builder, 0, 2);
        let output = builder.build_record_batch().unwrap().unwrap();

        assert_eq!(get_record_batch_memory_size(&output), output_size);
        assert_eq!(pool.reserved(), contender.size());
    }
}
