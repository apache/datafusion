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

use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;

use crate::error::Result;

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
/// This is not intended as an eventual replacement for the physical plan representation
/// within DataFusion, [`ExecutionPlan`], but rather a generic interface that
/// parts of the physical plan are "compiled" into by the scheduler.
///
/// # Eager vs Lazy Execution
///
/// Whether computation is eagerly done on push, or lazily done on pull, is
/// intentionally left as an implementation detail of the [`Pipeline`]
///
/// This allows flexibility to support the following different patterns, and potentially more:
///
/// An eager, push-based pipeline, that processes a batch synchronously in [`Pipeline::push`]
/// and immediately wakes the corresponding output partition.
///
/// A parallel, push-based pipeline, that enqueues the processing of a batch to the rayon
/// thread pool in [`Pipeline::push`], and wakes the corresponding output partition when
/// the job completes. Order and non-order preserving variants are possible
///
/// A merge pipeline which combines data from one or more input partitions into one or
/// more output partitions. [`Pipeline::push`] adds data to an input buffer, and wakes
/// any output partitions that may now be able to make progress. This may be none if
/// the operator is waiting on data from a different input partition
///
/// An aggregation pipeline which combines data from one or more input partitions into
/// a single output partition. [`Pipeline::push`] would eagerly update the computed
/// aggregates, and the final [`Pipeline::close`] trigger flushing these to the output.
/// It would also be possible to flush once the partial aggregates reach a certain size
///
/// A partition-aware aggregation pipeline, which functions similarly to the above, but
/// computes aggregations per input partition, before combining these prior to flush.
///
/// An async input pipeline, which has no inputs, and wakes the output partition
/// whenever new data is available
///
/// A JIT compiled sequence of synchronous operators, that perform multiple operations
/// from the physical plan as a single [`Pipeline`]. Parallelized implementations
/// are also possible
///
pub trait Pipeline: Send + Sync + std::fmt::Debug {
    /// Push a [`RecordBatch`] to the given input partition
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()>;

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
    ) -> Poll<Option<Result<RecordBatch>>>;
}
