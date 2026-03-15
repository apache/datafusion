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

use crate::SendableRecordBatchStream;
use crate::sorts::cursor::{ArrayValues, CursorArray, RowValues};
use crate::{PhysicalExpr, PhysicalSortExpr};
use arrow::array::{Array, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_ord::sort::lexsort_to_indices;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::stream::{Fuse, StreamExt};
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

/// A [`Stream`](futures::Stream) that has multiple partitions that can
/// be polled separately but not concurrently
///
/// Used by sort preserving merge to decouple the cursor merging logic from
/// the source of the cursors, the intention being to allow preserving
/// any row encoding performed for intermediate sorts
pub trait PartitionedStream: std::fmt::Debug + Send {
    type Output;

    /// Returns the number of partitions
    fn partitions(&self) -> usize;

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>>;
}

/// A new type wrapper around a set of fused [`SendableRecordBatchStream`]
/// that implements debug, and skips over empty [`RecordBatch`]
struct FusedStreams(Vec<Fuse<SendableRecordBatchStream>>);

impl std::fmt::Debug for FusedStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedStreams")
            .field("num_streams", &self.0.len())
            .finish()
    }
}

impl FusedStreams {
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match ready!(self.0[stream_idx].poll_next_unpin(cx)) {
                Some(Ok(b)) if b.num_rows() == 0 => continue,
                r => return Poll::Ready(r),
            }
        }
    }
}

/// A pair of `Arc<Rows>` that can be reused
#[derive(Debug)]
struct ReusableRows {
    // inner[stream_idx] holds a two Arcs:
    // at start of a new poll
    // .0 is the rows from the previous poll (at start),
    // .1 is the one that is being written to
    // at end of a poll, .0 will be swapped with .1,
    inner: Vec<[Option<Arc<Rows>>; 2]>,
}

impl ReusableRows {
    // return a Rows for writing,
    // does not clone if the existing rows can be reused
    fn take_next(&mut self, stream_idx: usize) -> Result<Rows> {
        Arc::try_unwrap(self.inner[stream_idx][1].take().unwrap()).map_err(|_| {
            internal_datafusion_err!(
                "Rows from RowCursorStream is still in use by consumer"
            )
        })
    }
    // save the Rows
    fn save(&mut self, stream_idx: usize, rows: &Arc<Rows>) {
        self.inner[stream_idx][1] = Some(Arc::clone(rows));
        // swap the current with the previous one, so that the next poll can reuse the Rows from the previous poll
        let [a, b] = &mut self.inner[stream_idx];
        mem::swap(a, b);
    }
}

/// A [`PartitionedStream`] that wraps a set of [`SendableRecordBatchStream`]
/// and computes [`RowValues`] based on the provided [`PhysicalSortExpr`]
/// Note: the stream returns an error if the consumer buffers more than one RowValues (i.e. holds on to two RowValues
/// from the same partition at the same time).
#[derive(Debug)]
pub struct RowCursorStream {
    /// Converter to convert output of physical expressions
    converter: RowConverter,
    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,
    /// Input streams
    streams: FusedStreams,
    /// Tracks the memory used by `converter`
    reservation: MemoryReservation,
    /// Allocated rows for each partition, we keep two to allow for buffering one
    /// in the consumer of the stream
    rows: ReusableRows,
}

impl RowCursorStream {
    pub fn try_new(
        schema: &Schema,
        expressions: &LexOrdering,
        streams: Vec<SendableRecordBatchStream>,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        let sort_fields = expressions
            .iter()
            .map(|expr| {
                let data_type = expr.expr.data_type(schema)?;
                Ok(SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()?;

        let streams: Vec<_> = streams.into_iter().map(|s| s.fuse()).collect();
        let converter = RowConverter::new(sort_fields)?;
        let mut rows = Vec::with_capacity(streams.len());
        for _ in &streams {
            // Initialize each stream with an empty Rows
            rows.push([
                Some(Arc::new(converter.empty_rows(0, 0))),
                Some(Arc::new(converter.empty_rows(0, 0))),
            ]);
        }
        Ok(Self {
            converter,
            reservation,
            column_expressions: expressions.iter().map(|x| Arc::clone(&x.expr)).collect(),
            streams: FusedStreams(streams),
            rows: ReusableRows { inner: rows },
        })
    }

    fn convert_batch(
        &mut self,
        batch: &RecordBatch,
        stream_idx: usize,
    ) -> Result<RowValues> {
        let cols = evaluate_expressions_to_arrays(&self.column_expressions, batch)?;

        // At this point, ownership should of this Rows should be unique
        let mut rows = self.rows.take_next(stream_idx)?;

        rows.clear();

        self.converter.append(&mut rows, &cols)?;
        self.reservation.try_resize(self.converter.size())?;

        let rows = Arc::new(rows);

        self.rows.save(stream_idx, &rows);

        // track the memory in the newly created Rows.
        let rows_reservation = self.reservation.new_empty();
        rows_reservation.try_grow(rows.size())?;
        Ok(RowValues::new(rows, rows_reservation))
    }
}

impl PartitionedStream for RowCursorStream {
    type Output = Result<(RowValues, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch, stream_idx)?;
                Ok((cursor, batch))
            })
        }))
    }
}

/// Specialized stream for sorts on single primitive columns
pub struct FieldCursorStream<T: CursorArray> {
    /// The physical expressions to sort by
    sort: PhysicalSortExpr,
    /// Input streams
    streams: FusedStreams,
    /// Create new reservations for each array
    reservation: MemoryReservation,
    phantom: PhantomData<fn(T) -> T>,
}

impl<T: CursorArray> std::fmt::Debug for FieldCursorStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveCursorStream")
            .field("num_streams", &self.streams)
            .finish()
    }
}

impl<T: CursorArray> FieldCursorStream<T> {
    pub fn new(
        sort: PhysicalSortExpr,
        streams: Vec<SendableRecordBatchStream>,
        reservation: MemoryReservation,
    ) -> Self {
        let streams = streams.into_iter().map(|s| s.fuse()).collect();
        Self {
            sort,
            streams: FusedStreams(streams),
            reservation,
            phantom: Default::default(),
        }
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<ArrayValues<T::Values>> {
        let value = self.sort.expr.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;
        let size_in_mem = array.get_buffer_memory_size();
        let array = array.as_any().downcast_ref::<T>().expect("field values");
        let array_reservation = self.reservation.new_empty();
        array_reservation.try_grow(size_in_mem)?;
        Ok(ArrayValues::new(
            self.sort.options,
            array,
            array_reservation,
        ))
    }
}

impl<T: CursorArray> PartitionedStream for FieldCursorStream<T> {
    type Output = Result<(ArrayValues<T::Values>, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch)?;
                Ok((cursor, batch))
            })
        }))
    }
}

/// A lazy, memory-efficient sort iterator used as a fallback during aggregate
/// spill when there is not enough memory for an eager sort (which requires ~2x
/// peak memory to hold both the unsorted and sorted copies simultaneously).
///
/// On the first call to `next()`, a sorted index array (`UInt32Array`) is
/// computed via `lexsort_to_indices`. Subsequent calls yield chunks of
/// `batch_size` rows by `take`-ing from the original batch using slices of
/// this index array. Each `take` copies data for the chunk (not zero-copy),
/// but only one chunk is live at a time since the caller consumes it before
/// requesting the next. Once all rows have been yielded, the original batch
/// and index array are dropped to free memory.
///
/// The caller must reserve `sizeof(batch) + sizeof(one chunk)` for this iterator,
/// and free the reservation once the iterator is depleted.
pub(crate) struct IncrementalSortIterator {
    batch: RecordBatch,
    expressions: LexOrdering,
    batch_size: usize,
    indices: Option<UInt32Array>,
    cursor: usize,
}

impl IncrementalSortIterator {
    pub(crate) fn new(
        batch: RecordBatch,
        expressions: LexOrdering,
        batch_size: usize,
    ) -> Self {
        Self {
            batch,
            expressions,
            batch_size,
            cursor: 0,
            indices: None,
        }
    }
}

impl Iterator for IncrementalSortIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.batch.num_rows() {
            return None;
        }

        match self.indices.as_ref() {
            None => {
                let sort_columns = match self
                    .expressions
                    .iter()
                    .map(|expr| expr.evaluate_to_sort_column(&self.batch))
                    .collect::<Result<Vec<_>>>()
                {
                    Ok(cols) => cols,
                    Err(e) => return Some(Err(e)),
                };

                let indices = match lexsort_to_indices(&sort_columns, None) {
                    Ok(indices) => indices,
                    Err(e) => return Some(Err(e.into())),
                };
                self.indices = Some(indices);

                // Call again, this time it will hit the Some(indices) branch and return the first batch
                self.next()
            }
            Some(indices) => {
                let batch_size = self.batch_size.min(self.batch.num_rows() - self.cursor);

                // Perform the take to produce the next batch
                let new_batch_indices = indices.slice(self.cursor, batch_size);
                let new_batch = match take_record_batch(&self.batch, &new_batch_indices) {
                    Ok(batch) => batch,
                    Err(e) => return Some(Err(e.into())),
                };

                self.cursor += batch_size;

                // If this is the last batch, we can release the memory
                if self.cursor >= self.batch.num_rows() {
                    let schema = self.batch.schema();
                    let _ = mem::replace(&mut self.batch, RecordBatch::new_empty(schema));
                    self.indices = None;
                }

                // Return the new batch
                Some(Ok(new_batch))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_rows = self.batch.num_rows();
        let batch_size = self.batch_size;
        let num_batches = num_rows.div_ceil(batch_size);
        (num_batches, Some(num_batches))
    }
}

impl FusedIterator for IncrementalSortIterator {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion_common::DataFusionError;
    use datafusion_physical_expr::expressions::col;

    /// Verifies that `take_record_batch` in `IncrementalSortIterator` actually
    /// copies the data into a new allocation rather than returning a zero-copy
    /// slice of the original batch. If the output arrays were slices, their
    /// underlying buffer length would match the original array's length; a true
    /// copy will have a buffer sized to fit only the chunk.
    #[test]
    fn incremental_sort_iterator_copies_data() -> Result<()> {
        let original_len = 10;
        let batch_size = 3;

        // Build a batch with a single Int32 column of descending values
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let col_a: Int32Array = Int32Array::from(vec![0; original_len]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col_a)])?;

        // Sort ascending on column "a"
        let expressions = LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
            "a",
            &batch.schema(),
        )?)])
        .unwrap();

        let mut total_rows = 0;
        IncrementalSortIterator::new(batch.clone(), expressions, batch_size).try_for_each(
            |result| {
                let chunk = result?;
                total_rows += chunk.num_rows();

                // Every output column must be a fresh allocation whose length
                // equals the chunk size, NOT the original array length.
                chunk.columns().iter().zip(batch.columns()).for_each(|(arr, original_arr)| {
                    let (_, scalar_buf, _) = arr.as_primitive::<Int32Type>().clone().into_parts();
                    let (_, original_scalar_buf, _) = original_arr.as_primitive::<Int32Type>().clone().into_parts();

                    assert_ne!(scalar_buf.inner().data_ptr(), original_scalar_buf.inner().data_ptr(), "Expected a copy of the data for each chunk, but got a slice that shares the same buffer as the original array");
                });

                Result::<_, DataFusionError>::Ok(())
            },
        )?;

        assert_eq!(total_rows, original_len);
        Ok(())
    }
}
