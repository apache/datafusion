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
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use log::warn;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

#[derive(Debug, Copy, Clone, Default)]
struct BatchCursor {
    /// The index into BatchBuilder::batches
    batch_idx: usize,
    /// The row index within the given batch
    row_idx: usize,
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
            cursors: vec![BatchCursor::default(); stream_count],
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
    ) -> Result<Vec<Arc<dyn arrow::array::Array>>> {
        (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = self
                    .batches
                    .iter()
                    .map(|(_, batch)| batch.column(column_idx).as_ref())
                    .collect();
                // Arrow's interleave panics on i32 offset overflow with
                // `.expect("overflow")`. Catch that panic so the caller
                // can retry with fewer rows.
                match catch_unwind(AssertUnwindSafe(|| interleave(&arrays, indices))) {
                    Ok(result) => Ok(result?),
                    Err(panic_payload) => {
                        if is_overflow_panic(&panic_payload) {
                            Err(DataFusionError::ArrowError(
                                Box::new(ArrowError::OffsetOverflowError(0)),
                                None,
                            ))
                        } else {
                            std::panic::resume_unwind(panic_payload);
                        }
                    }
                }
            })
            .collect::<Result<Vec<_>>>()
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

        // Try interleaving all indices. On offset overflow, halve and retry.
        let mut end = self.indices.len();
        let columns = loop {
            match self.try_interleave_columns(&self.indices[..end]) {
                Ok(cols) => break cols,
                Err(e) if is_offset_overflow(&e) => {
                    end /= 2;
                    if end == 0 {
                        return Err(e);
                    }
                    warn!(
                        "Interleave offset overflow with {} rows, retrying with {}",
                        self.indices.len(),
                        end
                    );
                }
                Err(e) => return Err(e),
            }
        };

        // Remove consumed indices, keeping any remaining for the next call.
        self.indices.drain(..end);

        // Only clean up fully-consumed batches when all indices are drained,
        // because remaining indices may still reference earlier batches.
        if self.indices.is_empty() {
            // New cursors are only created once the previous cursor for the stream
            // is finished. This means all remaining rows from all but the last batch
            // for each stream have been yielded to the newly created record batch
            //
            // We can therefore drop all but the last batch for each stream
            let mut batch_idx = 0;
            let mut retained = 0;
            self.batches.retain(|(stream_idx, batch)| {
                let stream_cursor = &mut self.cursors[*stream_idx];
                let retain = stream_cursor.batch_idx == batch_idx;
                batch_idx += 1;

                if retain {
                    stream_cursor.batch_idx = retained;
                    retained += 1;
                } else {
                    self.batches_mem_used -= get_record_batch_memory_size(batch);
                }
                retain
            });
        }

        // Release excess memory back to the pool, but never shrink below
        // initial_reservation to maintain the anti-starvation guarantee
        // for the merge phase.
        let target = self.batches_mem_used.max(self.initial_reservation);
        if self.reservation.size() > target {
            self.reservation.shrink(self.reservation.size() - target);
        }

        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            columns,
        )?))
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

/// Returns true if a caught panic payload looks like an Arrow offset overflow.
fn is_overflow_panic(payload: &Box<dyn std::any::Any + Send>) -> bool {
    if let Some(msg) = payload.downcast_ref::<&str>() {
        return msg.contains("overflow");
    }
    if let Some(msg) = payload.downcast_ref::<String>() {
        return msg.contains("overflow");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::memory_pool::{
        MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };

    /// Test that interleaving string columns whose combined byte length
    /// exceeds i32::MAX does not panic. Arrow's `interleave` panics with
    /// `.expect("overflow")` in this case; `BatchBuilder` catches the
    /// panic and retries with fewer rows until the output fits in i32
    /// offsets.
    #[test]
    fn test_interleave_overflow_is_caught() {
        // Each string is ~768 MB. Three rows total → ~2.3 GB > i32::MAX.
        let big_str: String = "x".repeat(768 * 1024 * 1024);

        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));

        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("test").register(&pool);
        let mut builder = BatchBuilder::new(
            Arc::clone(&schema),
            /* stream_count */ 3,
            /* batch_size */ 16,
            reservation,
        );

        // Push one batch per stream, each containing one large string.
        for stream_idx in 0..3 {
            let array = StringArray::from(vec![big_str.as_str()]);
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap();
            builder.push_batch(stream_idx, batch).unwrap();
            builder.push_row(stream_idx);
        }

        // 3 rows total; interleaving all 3 would overflow i32 offsets.
        // The retry loop should halve until it succeeds.
        let batch = builder.build_record_batch().unwrap().unwrap();
        assert!(batch.num_rows() > 0);
        assert!(batch.num_rows() < 3);

        // Drain remaining rows.
        let batch2 = builder.build_record_batch().unwrap().unwrap();
        assert!(batch2.num_rows() > 0);
        assert_eq!(batch.num_rows() + batch2.num_rows(), 3);
    }
}
