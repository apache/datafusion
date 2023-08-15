use crate::physical_plan::metrics::BaselineMetrics;
use crate::physical_plan::sorts::cursor::Cursor;
use crate::physical_plan::sorts::merge::{CursorStream, SortPreservingMergeStream};
use crate::physical_plan::RecordBatchStream;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// TODO:
// this will be changed to dyn PartitionedStream<Output = Result<(Vec<RecordBatch>, Vec<C>, Vec<SortOrder>)>>
// and the final interleave will be done here, after root node returns
type MergeStream = Pin<Box<dyn Send + Stream<Item = Result<RecordBatch>>>>;

pub(crate) struct SortPreservingCascadeStream {
    /// If the stream has encountered an error
    aborted: bool,

    /// used to record execution metrics
    metrics: BaselineMetrics,

    /// The cascading stream
    cascade: MergeStream,

    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,
}

impl SortPreservingCascadeStream {
    pub(crate) fn new<C: Cursor + Unpin + Send + 'static>(
        streams: CursorStream<C>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
    ) -> Self {
        let root: MergeStream = Box::pin(SortPreservingMergeStream::new(
            streams,
            Arc::clone(&schema),
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
            Some(Ok(inner_result)) => Poll::Ready(Some(Ok(inner_result))),
        }
    }
}

impl Stream for SortPreservingCascadeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.metrics.record_poll(poll)
    }
}

impl RecordBatchStream for SortPreservingCascadeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
