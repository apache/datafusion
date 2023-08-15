use crate::physical_plan::metrics::BaselineMetrics;
use crate::physical_plan::sorts::builder::SortOrder;
use crate::physical_plan::sorts::cursor::Cursor;
use crate::physical_plan::sorts::merge::{CursorStream, SortPreservingMergeStream};
use crate::physical_plan::RecordBatchStream;

use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

type MergeStream<C> = Pin<
    Box<dyn Send + Stream<Item = Result<(Vec<RecordBatch>, Vec<C>, Vec<SortOrder>)>>>,
>;

pub(crate) struct SortPreservingCascadeStream<C: Cursor> {
    /// If the stream has encountered an error
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
        let root: MergeStream<C> = Box::pin(SortPreservingMergeStream::new(
            streams,
            metrics.clone(),
            batch_size,
            fetch,
            reservation,
        ));

        // TODO (followup commit): build the cascade tree here
        let cascade = root;
        Self {
            aborted: false,
            cascade,
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
