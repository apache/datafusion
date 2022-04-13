use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;

use crate::ArrowResult;

pub mod execution;
pub mod repartition;

/// A push-based interface used by the scheduler to drive query execution
///
/// A pipeline processes data from one or more input partitions, producing output
/// to one or more output partitions. As a [`Pipeline`] may drawn on input from
/// more than one upstream [`Pipeline`], input partitions are identified by both
/// a child index, and a partition index, whereas output partitions are only
/// identified by a partition index.
///
pub trait Pipeline: Send + Sync + std::fmt::Debug {
    /// Push a [`RecordBatch`] to the given input partition
    fn push(&self, input: RecordBatch, child: usize, partition: usize);

    /// Mark an input partition as exhausted
    fn close(&self, child: usize, partition: usize);

    /// Returns the number of output partitions
    fn output_partitions(&self) -> usize;

    /// Attempt to pull out the next value of the given output partition, registering the
    /// current task for wakeup if the value is not yet available, and returning `None`
    /// if the output partition is exhausted and will never yield any further values
    ///
    /// # Return value
    ///
    /// There are several possible return values:
    ///
    /// - `Poll::Pending` indicates that this partition's next value is not ready yet.
    /// Implementations should use the waker provided by `cx` to notify the scheduler when
    /// progress may be able to be made
    ///
    /// - `Poll::Ready(Some(Ok(val)))` returns the next value from this output partition,
    /// the output partition should be polled again as it may have further values. The returned
    /// value will be routed to the next pipeline in the query
    ///
    /// - `Poll::Ready(Some(Err(e)))` returns an error that will be routed to the query's output
    /// and the query execution aborted.
    ///
    /// - `Poll::Ready(None)` indicates that this partition is exhausted and will not produce any
    /// further values.
    ///
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<ArrowResult<RecordBatch>>>;
}
