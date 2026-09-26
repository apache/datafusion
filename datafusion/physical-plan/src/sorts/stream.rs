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

use crate::sorts::cursor::{ArrayValues, CursorArray, RowValues};
use crate::{EmptyRecordBatchStream, SendableRecordBatchStream};
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
            let poll_result = self.0[stream_idx].poll_next_unpin(cx);
            match &poll_result {
                Poll::Pending => return Poll::Pending,
                // Skip empty batches
                Poll::Ready(Some(Ok(b))) if b.num_rows() == 0 => {}
                Poll::Ready(Some(Ok(_))) => return poll_result,
                Poll::Ready(None) | Poll::Ready(Some(Err(_))) => {
                    let stream_schema = self.0[stream_idx].get_ref().schema();

                    // Replace the stream with an empty stream, so we can drop memory usage
                    let empty_stream: SendableRecordBatchStream =
                        Box::pin(EmptyRecordBatchStream::new(stream_schema));
                    self.0[stream_idx] = empty_stream.fuse();

                    return poll_result;
                }
            }
        }
    }
}

/// An `Arc<Rows>` that can be reused.
///
/// Owns the reservation covering every retained buffer for as long as it is
/// retained, so the cache is visible to the pool rather than held off-book.
/// A retained buffer keeps its capacity, so the cost is the high-water mark of
/// each stream, not the size of the batch currently in flight.
#[derive(Debug)]
struct ReusableRows {
    inner: Vec<Option<Arc<Rows>>>,
    reservation: MemoryReservation,
}

impl ReusableRows {
    // return a Rows for writing,
    // does not clone if the existing rows can be reused
    fn take_next(&mut self, stream_idx: usize, converter: &RowConverter) -> Result<Rows> {
        match self.inner[stream_idx].take() {
            Some(rows) => Arc::try_unwrap(rows).map_err(|_| {
                internal_datafusion_err!(
                    "Rows from RowCursorStream is still in use by consumer"
                )
            }),
            // Nothing retained yet, or already released, so start over.
            None => Ok(converter.empty_rows(0, 0)),
        }
    }

    /// Account for a freshly built buffer, and retain it for reuse.
    ///
    /// The reservation is mandatory rather than best-effort. The buffer is live in the
    /// cursor whether or not this slot keeps a handle to it, so declining to reserve
    /// would hide it from the pool instead of avoiding it, and it frees nothing at this
    /// point either, since the cursor holds the same `Arc`. Retention on top of the
    /// reservation is free for the same reason. A pool that cannot cover the buffer
    /// fails the query here, as it did before the buffer was cached at all.
    fn save(&mut self, stream_idx: usize, rows: &Arc<Rows>) -> Result<()> {
        self.inner[stream_idx] = Some(Arc::clone(rows));
        let retained = self.retained_size();
        debug_assert!(retained >= self.reservation.size());
        if let Err(e) = self.reservation.try_resize(retained) {
            self.inner[stream_idx] = None;
            return Err(e);
        }
        Ok(())
    }

    // drop whatever a finished stream was holding
    fn release(&mut self, stream_idx: usize) {
        debug_assert!(self.reservation.size() >= self.retained_size());
        if let Some(rows) = self.inner[stream_idx].take() {
            self.reservation.shrink(rows.size());
        }
    }

    fn retained_size(&self) -> usize {
        self.inner.iter().flatten().map(|rows| rows.size()).sum()
    }
}

/// A [`PartitionedStream`] that wraps a set of [`SendableRecordBatchStream`]
/// and computes [`RowValues`] based on the provided [`PhysicalSortExpr`]
/// Note: the stream returns an error if the consumer buffers even one RowValues (i.e. holds on to one RowValues
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
    /// Reused `Rows` allocation for each partition. The consumer must not
    /// buffer the `RowValues` returned for a partition, since the old
    /// `Arc<Rows>` must be dropped before that partition can be polled again.
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
        let stream_count = streams.len();
        let converter = RowConverter::new(sort_fields)?;
        let rows_reservation = reservation.new_empty();
        Ok(Self {
            converter,
            reservation,
            column_expressions: expressions.iter().map(|x| Arc::clone(&x.expr)).collect(),
            streams: FusedStreams(streams),
            rows: ReusableRows {
                inner: vec![None; stream_count],
                reservation: rows_reservation,
            },
        })
    }

    fn convert_batch(
        &mut self,
        batch: &RecordBatch,
        stream_idx: usize,
    ) -> Result<RowValues> {
        let cols = evaluate_expressions_to_arrays(&self.column_expressions, batch)?;

        // At this point, ownership should of this Rows should be unique
        let mut rows = self.rows.take_next(stream_idx, &self.converter)?;

        rows.clear();

        self.converter.append(&mut rows, &cols)?;
        self.reservation.try_resize(self.converter.size())?;

        let rows = Arc::new(rows);

        self.rows.save(stream_idx, &rows)?;

        // `self.rows` now holds the reservation for this buffer unconditionally, and
        // holds it for at least as long as the cursor does. `take_next` cannot reclaim
        // the slot while the consumer still owns the `Arc`, so the cursor is handed an
        // empty reservation rather than accounting for the same bytes a second time.
        Ok(RowValues::new(rows, self.reservation.new_empty()))
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
        let polled = ready!(self.streams.poll_next(cx, stream_idx));
        if polled.is_none() {
            // The stream is finished, so its retained buffer will never be reused.
            self.rows.release(stream_idx);
        }
        Poll::Ready(polled.map(|r| {
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

    // Not implementing ExactSizeIterator since in case of an error we stop and don't emit any more
    // so the length would be wrong
    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_rows = self.batch.num_rows().saturating_sub(self.cursor);
        let batch_size = self.batch_size;
        let num_batches = num_rows.div_ceil(batch_size);
        (num_batches, Some(num_batches))
    }
}

impl FusedIterator for IncrementalSortIterator {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryStream;
    use arrow::array::{AsArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use arrow_schema::SchemaRef;
    use datafusion_common::DataFusionError;
    use datafusion_execution::RecordBatchStream;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use datafusion_physical_expr::expressions::col;
    use futures::Stream;
    use std::pin::Pin;

    fn create_incremental_sort_iter_on(
        input_batch_len: usize,
        output_batch_size: usize,
    ) -> Result<(IncrementalSortIterator, RecordBatch)> {
        // Build a batch with a single Int32 column of descending values
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let col_a: Int32Array =
            Int32Array::from_iter_values((0..input_batch_len as i32).rev());
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col_a)])?;

        // Sort ascending on column "a"
        let expressions = LexOrdering::new(vec![PhysicalSortExpr::new_default(col(
            "a",
            &batch.schema(),
        )?)])
        .unwrap();

        let iter =
            IncrementalSortIterator::new(batch.clone(), expressions, output_batch_size);

        Ok((iter, batch))
    }

    /// Verifies that `take_record_batch` in `IncrementalSortIterator` actually
    /// copies the data into a new allocation rather than returning a zero-copy
    /// slice of the original batch. If the output arrays were slices, their
    /// underlying buffer length would match the original array's length; a true
    /// copy will have a buffer sized to fit only the chunk.
    #[test]
    fn incremental_sort_iterator_copies_data() -> Result<()> {
        let original_len = 10;
        let batch_size = 3;

        let (mut iter, batch) =
            create_incremental_sort_iter_on(original_len, batch_size)?;

        let mut total_rows = 0;
        iter.try_for_each(
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

    #[test]
    fn test_fused_stream_drop_finished_streams() {
        #[derive(Clone)]
        struct SingleItemManualStream {
            // Held only so its `Arc` strong count reveals when the stream is dropped.
            #[expect(dead_code)]
            hold_ref: Arc<()>,
            record_batch: RecordBatch,
            should_finish: bool,
        }

        impl Stream for SingleItemManualStream {
            type Item = Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if !self.should_finish {
                    self.should_finish = true;
                    return Poll::Ready(Some(Ok(self.record_batch.clone())));
                }

                Poll::Ready(None)
            }
        }

        impl RecordBatchStream for SingleItemManualStream {
            fn schema(&self) -> SchemaRef {
                self.record_batch.schema()
            }
        }

        let hold_ref = Arc::new(());
        let record_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();

        let stream_1 = SingleItemManualStream {
            hold_ref: Arc::clone(&hold_ref),
            should_finish: false,
            record_batch: record_batch.clone(),
        };
        let stream_2 = stream_1.clone();

        let stream_1: SendableRecordBatchStream = Box::pin(stream_1);
        let stream_2: SendableRecordBatchStream = Box::pin(stream_2);

        let mut fused_stream = FusedStreams(vec![stream_1.fuse(), stream_2.fuse()]);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // The original plus one clone held by each of the two streams.
        assert_eq!(Arc::strong_count(&hold_ref), 3);

        // First fetch from stream 0 yields its single batch.
        // the stream is not finished yet, so nothing is dropped.
        let poll = fused_stream.poll_next(&mut cx, 0);
        assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));
        assert_eq!(Arc::strong_count(&hold_ref), 3);

        // Second fetch from stream 0 returns `None`, so it is replaced with an
        // empty stream and dropped, releasing its `hold_ref` clone.
        // running 3 times to make sure the stream is fused correctly
        for _ in 0..3 {
            let poll = fused_stream.poll_next(&mut cx, 0);
            assert!(matches!(poll, Poll::Ready(None)));
            assert_eq!(Arc::strong_count(&hold_ref), 2);
        }

        // First fetch from stream 1 yields its single batch
        // the stream is not finished yet, so nothing is dropped.
        let poll = fused_stream.poll_next(&mut cx, 1);
        assert!(matches!(poll, Poll::Ready(Some(Ok(_)))));
        assert_eq!(Arc::strong_count(&hold_ref), 2);

        // Second fetch from stream 1 returns `None`, so it is replaced with an
        // empty stream and dropped, releasing its `hold_ref` clone.
        // running 3 times to make sure the stream is fused correctly
        for _ in 0..3 {
            let poll = fused_stream.poll_next(&mut cx, 1);
            assert!(matches!(poll, Poll::Ready(None)));
            assert_eq!(Arc::strong_count(&hold_ref), 1);
        }
    }

    fn assert_iterator_size_hint(iter: &IncrementalSortIterator, expected_len: usize) {
        assert_eq!(iter.size_hint(), (expected_len, Some(expected_len)));
    }

    #[test]
    fn incremental_sort_iterator_report_correct_len() -> Result<()> {
        let original_len = 10;
        let batch_size = 3;

        let (mut iterator, _) =
            create_incremental_sort_iter_on(original_len, batch_size)?;

        assert_iterator_size_hint(&iterator, 4);

        let batch = iterator.next().unwrap()?;
        assert_eq!(batch.num_rows(), batch_size);

        assert_iterator_size_hint(&iterator, 3);

        let batch = iterator.next().unwrap()?;
        assert_eq!(batch.num_rows(), batch_size);

        assert_iterator_size_hint(&iterator, 2);

        let batch = iterator.next().unwrap()?;
        assert_eq!(batch.num_rows(), batch_size);

        assert_iterator_size_hint(&iterator, 1);

        let batch = iterator.next().unwrap()?;
        // left over
        assert_eq!(batch.num_rows(), 1);

        assert_iterator_size_hint(&iterator, 0);

        assert!(iterator.next().is_none());

        Ok(())
    }

    /// Drives a `RowCursorStream` over several partitions and reports the pool
    /// reservation, so the tests below can watch it move.
    struct RowCursorHarness {
        stream: RowCursorStream,
        pool: Arc<dyn MemoryPool>,
    }

    impl RowCursorHarness {
        /// `batches_per_partition` batches of `rows` rows each, per partition.
        /// The sort key spans two columns so this takes the `Rows` path rather
        /// than the specialized single-column `FieldCursorStream`.
        fn new(
            partitions: usize,
            batches_per_partition: usize,
            rows: usize,
            str_width: usize,
        ) -> Result<Self> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
            ]));

            let make_batch = |seq: usize| {
                let base = (seq * rows) as i32;
                let a = Int32Array::from_iter_values((0..rows as i32).map(|i| base + i));
                let b = StringArray::from_iter_values(
                    (0..rows).map(|_| "x".repeat(str_width)),
                );
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(a), Arc::new(b)])
                    .unwrap()
            };

            let streams: Vec<SendableRecordBatchStream> = (0..partitions)
                .map(|_| {
                    let batches: Vec<RecordBatch> =
                        (0..batches_per_partition).map(make_batch).collect();
                    Box::pin(
                        MemoryStream::try_new(batches, Arc::clone(&schema), None)
                            .unwrap(),
                    ) as SendableRecordBatchStream
                })
                .collect();

            let expressions = LexOrdering::new(vec![
                PhysicalSortExpr::new_default(col("a", &schema)?),
                PhysicalSortExpr::new_default(col("b", &schema)?),
            ])
            .unwrap();

            // Generous limit: this measures what is reserved, not what is refused.
            let pool: Arc<dyn MemoryPool> =
                Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
            let reservation = MemoryConsumer::new("test").register(&pool);
            let stream =
                RowCursorStream::try_new(&schema, &expressions, streams, reservation)?;

            Ok(Self { stream, pool })
        }

        fn reserved(&self) -> usize {
            self.pool.reserved()
        }

        /// Polls `stream_idx`, handing back the cursor so the caller controls when
        /// it is dropped. `None` once the partition is finished.
        fn poll_cursor(&mut self, stream_idx: usize) -> Result<Option<RowValues>> {
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            match self.stream.poll_next(&mut cx, stream_idx) {
                Poll::Ready(Some(Ok((values, _batch)))) => Ok(Some(values)),
                Poll::Ready(Some(Err(e))) => Err(e),
                Poll::Ready(None) => Ok(None),
                Poll::Pending => unreachable!("MemoryStream is never pending"),
            }
        }

        /// Polls `stream_idx` and immediately drops the returned cursor, which is
        /// what the merge does once a cursor is exhausted. Returns `false` once
        /// the partition is finished.
        fn poll_and_drop_cursor(&mut self, stream_idx: usize) -> Result<bool> {
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            match self.stream.poll_next(&mut cx, stream_idx) {
                Poll::Ready(Some(Ok((values, _batch)))) => {
                    drop(values);
                    Ok(true)
                }
                Poll::Ready(Some(Err(e))) => Err(e),
                Poll::Ready(None) => Ok(false),
                Poll::Pending => unreachable!("MemoryStream is never pending"),
            }
        }
    }

    // `RowCursorStream` keeps one `Rows` buffer per partition alive between
    // polls so the allocation can be reused. That buffer is real memory, and it
    // outlives the cursor handed to the consumer: the merge drops a cursor as
    // soon as it is exhausted, but the buffer behind it stays cached.
    //
    // So dropping the cursor must not change what the pool reports. If it does,
    // the bytes still held by the cache have gone off the books and the pool is
    // under-counting a live allocation.
    #[test]
    fn dropping_a_cursor_does_not_unaccount_its_retained_buffer() -> Result<()> {
        let mut harness = RowCursorHarness::new(1, 4, 512, 64)?;

        let cursor = harness.poll_cursor(0)?.expect("first batch");
        let with_cursor_alive = harness.reserved();

        drop(cursor);
        let after_drop = harness.reserved();

        assert_eq!(
            with_cursor_alive, after_drop,
            "the cached `Rows` buffer outlives the cursor, so dropping the cursor \
             must not release its bytes (before {with_cursor_alive}, after {after_drop})"
        );

        // And the buffer is genuinely on the books, not merely unchanged at zero.
        let baseline = RowCursorHarness::new(1, 4, 512, 64)?.reserved();
        assert!(
            after_drop > baseline,
            "retained buffer should be reserved (baseline {baseline}, now {after_drop})"
        );

        Ok(())
    }

    // The retained bytes are per partition, so what the pool reports has to
    // scale with partition count rather than staying flat at the size of
    // whichever batch happens to be in flight.
    #[test]
    fn retained_row_buffer_reservation_scales_with_partitions() -> Result<()> {
        let measure = |partitions: usize| -> Result<usize> {
            let mut harness = RowCursorHarness::new(partitions, 4, 512, 64)?;
            for stream_idx in 0..partitions {
                assert!(harness.poll_and_drop_cursor(stream_idx)?);
            }
            Ok(harness.reserved())
        };

        let few = measure(2)?;
        let many = measure(16)?;

        // 8x the partitions. The converter reservation is shared and does not
        // scale, so this is deliberately loose - the point is that it grows with
        // the number of cached buffers, not that it grows by an exact factor.
        assert!(
            many > few * 4,
            "reservation should scale with the number of retained buffers, \
             but 2 partitions reserved {few} and 16 reserved {many}"
        );

        Ok(())
    }

    // The flip side: a partition that will never be polled again must hand its
    // buffer back. Otherwise the reservation only ever grows and a long merge
    // holds every partition's high-water mark until the whole stream drops.
    #[test]
    fn exhausted_partitions_release_their_retained_buffers() -> Result<()> {
        let partitions = 8;
        let batches_per_partition = 3;
        let mut harness =
            RowCursorHarness::new(partitions, batches_per_partition, 512, 64)?;

        let baseline = harness.reserved();

        for stream_idx in 0..partitions {
            for _ in 0..batches_per_partition {
                assert!(harness.poll_and_drop_cursor(stream_idx)?);
            }
        }
        let peak = harness.reserved();
        assert!(
            peak > baseline,
            "expected the cached buffers to be reserved"
        );

        // Poll each partition once more so it reports exhaustion.
        for stream_idx in 0..partitions {
            assert!(!harness.poll_and_drop_cursor(stream_idx)?);
        }

        let after_release = harness.reserved();
        assert!(
            after_release < peak,
            "exhausted partitions should release their buffers \
             (peak {peak}, after {after_release})"
        );

        Ok(())
    }
}
