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

use crate::physical_plan::sorts::builder::SortOrder;
use crate::physical_plan::sorts::cursor::{Cursor, FieldArray, FieldCursor, RowCursor};
use crate::physical_plan::SendableRecordBatchStream;
use crate::physical_plan::{PhysicalExpr, PhysicalSortExpr};
use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use futures::stream::{Fuse, StreamExt};
use parking_lot::Mutex;
use uuid::Uuid;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// A fallible [`PartitionedStream`] of [`Cursor`] and [`RecordBatch`]
pub(crate) type BatchCursorStream<C> =
    Box<dyn PartitionedStream<Output = Result<(C, RecordBatch)>>>;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BatchOffset(pub usize); // offset into a batch, used when a cursor is sliced

/// A fallible [`PartitionedStream`] of [`Cursor`] and a batch identifier (Uuid)
pub(crate) type CursorStream<C> =
    Box<dyn PartitionedStream<Output = Result<(C, Uuid, BatchOffset)>>>;

/// A stream of yielded [`SortOrder`]s, with the corresponding [`Cursor`]s and [`Uuid`]s
pub(crate) type MergeStream<C> = std::pin::Pin<
    Box<
        dyn Send
            + futures::Stream<
                Item = Result<(Vec<(C, Uuid, BatchOffset)>, Vec<SortOrder>)>,
            >,
    >,
>;

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

/// A newtype wrapper around a set of fused [`SendableRecordBatchStream`]
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

/// A [`PartitionedStream`] that wraps a set of [`SendableRecordBatchStream`]
/// and computes [`RowCursor`] based on the provided [`PhysicalSortExpr`]
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
}

impl RowCursorStream {
    pub fn try_new(
        schema: &Schema,
        expressions: &[PhysicalSortExpr],
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

        let streams = streams.into_iter().map(|s| s.fuse()).collect();
        let converter = RowConverter::new(sort_fields)?;
        Ok(Self {
            converter,
            reservation,
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            streams: FusedStreams(streams),
        })
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<RowCursor> {
        let cols = self
            .column_expressions
            .iter()
            .map(|expr| Ok(expr.evaluate(batch)?.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;

        let rows = self.converter.convert_columns(&cols)?;
        self.reservation.try_resize(self.converter.size())?;

        // track the memory in the newly created Rows.
        let mut rows_reservation = self.reservation.new_empty();
        rows_reservation.try_grow(rows.size())?;
        Ok(RowCursor::new(rows, rows_reservation))
    }
}

impl PartitionedStream for RowCursorStream {
    type Output = Result<(RowCursor, RecordBatch)>;

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

/// Specialized stream for sorts on single primitive columns
pub struct FieldCursorStream<T: FieldArray> {
    /// The physical expressions to sort by
    sort: PhysicalSortExpr,
    /// Input streams
    streams: FusedStreams,
    phantom: PhantomData<fn(T) -> T>,
}

impl<T: FieldArray> std::fmt::Debug for FieldCursorStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveCursorStream")
            .field("num_streams", &self.streams)
            .finish()
    }
}

impl<T: FieldArray> FieldCursorStream<T> {
    pub fn new(sort: PhysicalSortExpr, streams: Vec<SendableRecordBatchStream>) -> Self {
        let streams = streams.into_iter().map(|s| s.fuse()).collect();
        Self {
            sort,
            streams: FusedStreams(streams),
            phantom: Default::default(),
        }
    }

    fn convert_batch(&mut self, batch: &RecordBatch) -> Result<FieldCursor<T::Values>> {
        let value = self.sort.expr.evaluate(batch)?;
        let array = value.into_array(batch.num_rows());
        let array = array.as_any().downcast_ref::<T>().expect("field values");
        Ok(FieldCursor::new(self.sort.options, array))
    }
}

impl<T: FieldArray> PartitionedStream for FieldCursorStream<T> {
    type Output = Result<(FieldCursor<T::Values>, RecordBatch)>;

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

/// A wrapper around [`CursorStream<C>`] that implements [`PartitionedStream`]
/// and provides polling of a subset of the streams.
pub struct OffsetCursorStream<C: Cursor> {
    streams: Arc<Mutex<BatchTrackingStream<C>>>,
    offset: usize,
    limit: usize,
}

impl<C: Cursor> OffsetCursorStream<C> {
    pub fn new(
        streams: Arc<Mutex<BatchTrackingStream<C>>>,
        offset: usize,
        limit: usize,
    ) -> Self {
        Self {
            streams,
            offset,
            limit,
        }
    }
}

impl<C: Cursor> PartitionedStream for OffsetCursorStream<C> {
    type Output = Result<(C, Uuid, BatchOffset)>;

    fn partitions(&self) -> usize {
        self.limit - self.offset
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        let stream_abs_idx = stream_idx + self.offset;
        if stream_abs_idx >= self.limit {
            return Poll::Ready(Some(Err(DataFusionError::Internal(format!(
                "Invalid stream index {} for offset {} and limit {}",
                stream_idx, self.offset, self.limit
            )))));
        }
        Poll::Ready(ready!(self.streams.lock().poll_next(cx, stream_abs_idx)))
    }
}

impl<C: Cursor> std::fmt::Debug for OffsetCursorStream<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OffsetCursorStream").finish()
    }
}

pub struct BatchTrackingStream<C: Cursor> {
    /// Write once, read many [`RecordBatch`]s
    batches: HashMap<Uuid, Arc<RecordBatch>>,
    /// Input streams yielding [`Cursor`]s and [`RecordBatch`]es
    streams: BatchCursorStream<C>,
    /// Accounts for memory used by buffered batches
    reservation: MemoryReservation,
}

impl<C: Cursor> BatchTrackingStream<C> {
    pub fn new(streams: BatchCursorStream<C>, reservation: MemoryReservation) -> Self {
        Self {
            batches: HashMap::new(),
            streams,
            reservation,
        }
    }

    pub fn get_batches(&self, batch_ids: &[Uuid]) -> Vec<Arc<RecordBatch>> {
        batch_ids.iter().map(|id| self.batches[id].clone()).collect()
    }

    pub fn remove_batches(&mut self, batch_ids: &[Uuid]) {
        for id in batch_ids {
            self.batches.remove(id);
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<(C, Uuid, BatchOffset)>>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|(cursor, batch)| {
                self.reservation.try_grow(batch.get_array_memory_size())?;
                let batch_id = Uuid::new_v4();
                self.batches.insert(batch_id, Arc::new(batch));
                Ok((cursor, batch_id, BatchOffset(0_usize)))
            })
        }))
    }
}

/// A newtype wrapper around a set of fused [`MergeStream`]
/// that implements debug, and skips over empty inner poll results
struct FusedMergeStreams<C>(Vec<Fuse<MergeStream<C>>>);

impl<C: Cursor> FusedMergeStreams<C> {
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Result<(Vec<(C, Uuid, BatchOffset)>, Vec<SortOrder>)>>>
    {
        loop {
            match ready!(self.0[stream_idx].poll_next_unpin(cx)) {
                Some(Ok((_, sort_order))) if sort_order.len() == 0 => continue,
                r => return Poll::Ready(r),
            }
        }
    }
}

pub struct YieldedCursorStream<C: Cursor> {
    // inner polled batch cursors, per stream_idx, which are partially yielded
    cursors: Vec<Option<VecDeque<(C, Uuid, BatchOffset)>>>,
    /// Streams being polled
    streams: FusedMergeStreams<C>,
}

impl<C: Cursor + std::marker::Send> YieldedCursorStream<C> {
    pub fn new(streams: Vec<MergeStream<C>>) -> Self {
        let stream_cnt = streams.len();
        Self {
            cursors: (0..stream_cnt).map(|_| None).collect(),
            streams: FusedMergeStreams(streams.into_iter().map(|s| s.fuse()).collect()),
        }
    }

    fn incr_next_batch(
        &mut self,
        stream_idx: usize,
    ) -> Option<(C, Uuid, BatchOffset)> {
        self.cursors[stream_idx]
            .as_mut()
            .map(|queue| queue.pop_front())
            .flatten()
    }

    // TODO: in order to handle sort_order, we need to either:
    // parse further
    // or concat the cursors
    fn try_parse_batches(
        &mut self,
        stream_idx: usize,
        cursors: Vec<(C, Uuid, BatchOffset)>,
        sort_order: Vec<SortOrder>,
    ) -> Result<()> {
        let mut cursors_per_batch: HashMap<(Uuid, BatchOffset), C> =
            HashMap::with_capacity(cursors.len());
        for (cursor, batch_id, batch_offset) in cursors {
            cursors_per_batch.insert((batch_id, batch_offset), cursor);
        }

        let mut parsed_cursors: Vec<(C, Uuid, BatchOffset)> =
            Vec::with_capacity(sort_order.len());
        let (mut prev_batch_id, mut prev_row_idx, mut prev_batch_offset) = sort_order[0];
        let mut len = 0;

        for (batch_id, row_idx, batch_offset) in sort_order.iter() {
            if prev_batch_id == *batch_id && batch_offset.0 == prev_batch_offset.0 {
                len += 1;
                continue;
            } else {
                // parse cursor
                if let Some(cursor) =
                    cursors_per_batch.get(&(prev_batch_id, prev_batch_offset))
                {
                    let parsed_cursor = cursor.slice(prev_row_idx, len)?;
                    parsed_cursors.push((
                        parsed_cursor,
                        prev_batch_id,
                        BatchOffset(prev_row_idx + prev_batch_offset.0),
                    ));
                } else {
                    unreachable!("cursor not found");
                }

                prev_batch_id = *batch_id;
                prev_row_idx = *row_idx;
                prev_batch_offset = *batch_offset;
                len = 1;
            }
        }
        if let Some(cursor) = cursors_per_batch.get(&(prev_batch_id, prev_batch_offset)) {
            let parsed_cursor = cursor.slice(prev_row_idx, len)?;
            parsed_cursors.push((
                parsed_cursor,
                prev_batch_id,
                BatchOffset(prev_row_idx + prev_batch_offset.0),
            ));
        }

        self.cursors[stream_idx] = Some(VecDeque::from(parsed_cursors));
        return Ok(());
    }
}

impl<C: Cursor + std::marker::Send> PartitionedStream for YieldedCursorStream<C> {
    type Output = Result<(C, Uuid, BatchOffset)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<Self::Output>> {
        match self.incr_next_batch(stream_idx) {
            None => match ready!(self.streams.poll_next(cx, stream_idx)) {
                None => Poll::Ready(None),
                Some(Err(e)) => Poll::Ready(Some(Err(e))),
                Some(Ok((cursors, sort_order))) => {
                    self.try_parse_batches(stream_idx, cursors, sort_order)?;
                    Poll::Ready((Ok(self.incr_next_batch(stream_idx))).transpose())
                }
            },
            Some(r) => Poll::Ready(Some(Ok(r))),
        }
    }
}

impl<C: Cursor + std::marker::Send> std::fmt::Debug for YieldedCursorStream<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YieldedCursorStream")
            .field("num_partitions", &self.partitions())
            .finish()
    }
}
