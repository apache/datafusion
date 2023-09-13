use crate::physical_plan::metrics::BaselineMetrics;
use crate::physical_plan::sorts::builder::SortOrder;
use crate::physical_plan::sorts::cursor::Cursor;
use crate::physical_plan::sorts::merge::SortPreservingMergeStream;
use crate::physical_plan::sorts::stream::{
    BatchCursorStream, BatchId, BatchTrackingStream, MergeStream, OffsetCursorStream,
    YieldedCursorStream,
};
use crate::physical_plan::stream::ReceiverStream;
use crate::physical_plan::RecordBatchStream;

use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::{Stream, StreamExt};
use std::collections::{HashMap, VecDeque};
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

    /// Batches are collected on first yield from the RowCursorStream
    /// Subsequent merges in cascade all refer to the [`BatchId`]s
    record_batch_collector: Arc<parking_lot::Mutex<BatchTrackingStream<C>>>,
}

impl<C: Cursor + Unpin + Send + 'static> SortPreservingCascadeStream<C> {
    pub(crate) fn new(
        streams: BatchCursorStream<C>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
    ) -> Self {
        let stream_count = streams.partitions();

        let streams = Arc::new(parking_lot::Mutex::new(BatchTrackingStream::new(
            streams,
            reservation.new_empty(),
        )));

        let max_streams_per_merge = 10;
        let mut divided_streams: VecDeque<MergeStream<C>> =
            VecDeque::with_capacity(stream_count / max_streams_per_merge + 1);

        // build leaves
        for stream_offset in (0..stream_count).step_by(max_streams_per_merge) {
            let limit =
                std::cmp::min(stream_offset + max_streams_per_merge, stream_count);

            // OffsetCursorStream enables the ability to share the same RowCursorStream across multiple leafnode merges.
            let streams =
                OffsetCursorStream::new(Arc::clone(&streams), stream_offset, limit);

            divided_streams.push_back(spawn_buffered_merge(
                Box::pin(SortPreservingMergeStream::new(
                    Box::new(streams),
                    metrics.clone(),
                    batch_size,
                    None, // fetch, the LIMIT, is applied to the final merge
                )),
                schema.clone(),
                2,
            ));
        }

        // build rest of tree
        let mut next_level: VecDeque<MergeStream<C>> =
            VecDeque::with_capacity(divided_streams.len() / max_streams_per_merge + 1);
        while divided_streams.len() > 1 || !next_level.is_empty() {
            let fan_in: Vec<MergeStream<C>> = divided_streams
                .drain(0..std::cmp::min(max_streams_per_merge, divided_streams.len()))
                .collect();

            next_level.push_back(spawn_buffered_merge(
                Box::pin(SortPreservingMergeStream::new(
                    Box::new(YieldedCursorStream::new(fan_in)),
                    metrics.clone(),
                    batch_size,
                    if divided_streams.is_empty() && next_level.is_empty() {
                        fetch
                    } else {
                        None
                    }, // fetch, the LIMIT, is applied to the final merge
                )),
                schema.clone(),
                2,
            ));
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
            record_batch_collector: streams,
        }
    }

    fn build_record_batch(&mut self, sort_order: Vec<SortOrder>) -> Result<RecordBatch> {
        let mut batches_needed = Vec::with_capacity(sort_order.len());
        let mut batches_seen: HashMap<BatchId, (usize, usize)> =
            HashMap::with_capacity(sort_order.len()); // (batch_idx, rows_sorted)
        let mut sort_order_offset_adjusted = Vec::with_capacity(sort_order.len());
        let mut batch_idx: usize = 0;

        for (batch_id, row_idx, offset) in sort_order.iter() {
            let batch_idx = match batches_seen.get(batch_id) {
                Some((batch_idx, _)) => *batch_idx,
                None => {
                    batches_needed.push(*batch_id);
                    let batch_now = batch_idx;
                    batch_idx += 1;
                    batch_now
                }
            };
            sort_order_offset_adjusted.push((batch_idx, *row_idx + offset.0));
            batches_seen.insert(*batch_id, (batch_idx, *row_idx + offset.0 + 1));
        }

        let batch_collecter = self.record_batch_collector.lock();
        let batches = batch_collecter.get_batches(batches_needed.as_slice());
        drop(batch_collecter);

        let batches_to_remove = batches
            .iter()
            .zip(batches_needed.into_iter())
            .filter_map(|(batch, batch_id)| {
                if batch.num_rows() == batches_seen[&batch_id].1 {
                    Some(batch_id)
                } else {
                    None
                }
            }).collect::<Vec<_>>();

        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = batches
                    .iter()
                    .map(|batch| batch.column(column_idx).as_ref())
                    .collect();
                Ok(interleave(&arrays, sort_order_offset_adjusted.as_slice())?)
            })
            .collect::<Result<Vec<_>>>()?;

        let mut batch_collecter = self.record_batch_collector.lock();
        batch_collecter.remove_batches(batches_to_remove.as_slice());
        drop(batch_collecter);

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
            Some(Ok((_, sort_order))) => {
                match self.build_record_batch(sort_order) {
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

fn spawn_buffered_merge<C: Send + Sync + 'static>(
    mut input: MergeStream<C>,
    schema: SchemaRef,
    buffer: usize,
) -> MergeStream<C> {
    // Use tokio only if running from a multi-thread tokio context
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
        {
            let mut builder = ReceiverStream::builder(schema, buffer);

            let sender = builder.tx();

            builder.spawn(async move {
                while let Some(item) = input.next().await {
                    if sender.send(item).await.is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            });

            builder.build()
        }
        _ => input,
    }
}
