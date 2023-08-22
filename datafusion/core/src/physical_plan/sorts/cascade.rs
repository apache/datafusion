use crate::physical_plan::metrics::BaselineMetrics;
use crate::physical_plan::sorts::builder::SortOrder;
use crate::physical_plan::sorts::cursor::Cursor;
use crate::physical_plan::sorts::merge::SortPreservingMergeStream;
use crate::physical_plan::sorts::stream::{
    CursorStream, MergeStream, OffsetCursorStream, YieldedCursorStream,
};
use crate::physical_plan::RecordBatchStream;

use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub(crate) struct SortPreservingCascadeStream<C: Cursor> {
    /// If the stream has encountered an error, or fetch is reached
    aborted: bool,

    /// used to record execution metrics
    metrics: BaselineMetrics,

    /// The cascading stream
    cascade: MergeStream<C>,

    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,
}

impl<C: Cursor + Unpin + Send + 'static> SortPreservingCascadeStream<C> {
    pub(crate) fn new(
        streams: CursorStream<C>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
    ) -> Self {
        let stream_count = streams.partitions();

        // TODO: since we already use a mutex here,
        // we can have (for the same relative cost) a mutex for a single holder of record_batches
        // which yields a batch_idx tracker.
        // In this way, we can do slicing followed by concating of Cursors yielded from each merge,
        // without needing to also yield sliced-then-concated batches (which are expensive to concat).
        //
        // Refer to YieldedCursorStream for where the concat would happen (TODO).
        let streams = Arc::new(parking_lot::Mutex::new(streams));

        let max_streams_per_merge = 2; // TODO: change this to 10, once we have tested with 2 (to force more cascade levels)
        let mut divided_streams: VecDeque<MergeStream<C>> =
            VecDeque::with_capacity(stream_count / max_streams_per_merge + 1);

        // build leaves
        for stream_offset in (0..stream_count).step_by(max_streams_per_merge) {
            let limit =
                std::cmp::min(stream_offset + max_streams_per_merge, stream_count);

            // OffsetCursorStream enables the ability to share the same RowCursorStream across multiple leafnode merges.
            let streams =
                OffsetCursorStream::new(Arc::clone(&streams), stream_offset, limit);

            divided_streams.push_back(Box::pin(SortPreservingMergeStream::new(
                Box::new(streams),
                metrics.clone(),
                batch_size,
                None, // fetch, the LIMIT, is applied to the final merge
                reservation.new_empty(),
            )));
        }

        // build rest of tree
        let mut next_level: VecDeque<MergeStream<C>> =
            VecDeque::with_capacity(divided_streams.len() / max_streams_per_merge + 1);
        while divided_streams.len() > 1 || !next_level.is_empty() {
            let fan_in: Vec<MergeStream<C>> = divided_streams
                .drain(0..std::cmp::min(max_streams_per_merge, divided_streams.len()))
                .collect();

            next_level.push_back(Box::pin(SortPreservingMergeStream::new(
                Box::new(YieldedCursorStream::new(fan_in)),
                metrics.clone(),
                batch_size,
                if divided_streams.is_empty() && next_level.is_empty() {
                    fetch
                } else {
                    None
                }, // fetch, the LIMIT, is applied to the final merge
                reservation.new_empty(),
            )));
            // in order to maintain sort-preserving streams, don't mix the merge tree levels.
            if divided_streams.is_empty() {
                divided_streams = next_level.drain(..).collect();
            }
        }

        Self {
            aborted: false,
            cascade: divided_streams
                .remove(0)
                .expect("must have a root merge stream"),
            schema,
            metrics,
        }
    }

    fn build_record_batch(
        &mut self,
        batches: Vec<RecordBatch>,
        sort_order: Vec<SortOrder>,
    ) -> Result<RecordBatch> {
        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = batches
                    .iter()
                    .map(|batch| batch.column(column_idx).as_ref())
                    .collect();
                Ok(interleave(&arrays, sort_order.as_slice())?)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        match futures::ready!(self.cascade.as_mut().poll_next(cx)) {
            None => Poll::Ready(None),
            Some(Err(e)) => {
                self.aborted = true;
                Poll::Ready(Some(Err(e)))
            }
            Some(Ok((batches, _, sort_order))) => {
                match self.build_record_batch(batches, sort_order) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(e) => {
                        self.aborted = true;
                        Poll::Ready(Some(Err(e)))
                    }
                }
            }
        }
    }
}

impl<C: Cursor + Unpin + Send + 'static> Stream for SortPreservingCascadeStream<C> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.metrics.record_poll(poll)
    }
}

impl<C: Cursor + Unpin + Send + 'static> RecordBatchStream
    for SortPreservingCascadeStream<C>
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
