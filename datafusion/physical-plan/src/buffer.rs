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

//! [`BufferExec`] decouples production and consumption on messages by buffering the input in the
//! background up to a certain capacity.

use crate::execution_plan::{CardinalityEffect, SchedulingType};
use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::projection::ProjectionExec;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SortOrderPushdownResult,
    check_if_same_properties,
};
use arrow::array::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, Statistics, internal_err, plan_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr_common::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use futures::{Stream, StreamExt, TryStreamExt};
use pin_project_lite::pin_project;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// WARNING: EXPERIMENTAL
///
/// Decouples production and consumption of record batches with an internal queue per partition,
/// eagerly filling up the capacity of the queues even before any message is requested.
///
/// ```text
///             ┌───────────────────────────┐
///             │        BufferExec         │
///             │                           │
///             │┌────── Partition 0 ──────┐│
///             ││            ┌────┐ ┌────┐││       ┌────┐
/// ──background poll────────▶│    │ │    ├┼┼───────▶    │
///             ││            └────┘ └────┘││       └────┘
///             │└─────────────────────────┘│
///             │┌────── Partition 1 ──────┐│
///             ││     ┌────┐ ┌────┐ ┌────┐││       ┌────┐
/// ──background poll─▶│    │ │    │ │    ├┼┼───────▶    │
///             ││     └────┘ └────┘ └────┘││       └────┘
///             │└─────────────────────────┘│
///             │                           │
///             │           ...             │
///             │                           │
///             │┌────── Partition N ──────┐│
///             ││                   ┌────┐││       ┌────┐
/// ──background poll───────────────▶│    ├┼┼───────▶    │
///             ││                   └────┘││       └────┘
///             │└─────────────────────────┘│
///             └───────────────────────────┘
/// ```
///
/// The capacity is provided in bytes, and for each buffered record batch it will take into account
/// the size reported by [RecordBatch::get_array_memory_size].
///
/// If a single record batch exceeds the maximum capacity set in the `capacity` argument, it's still
/// allowed to pass in order to not deadlock the buffer.
///
/// This is useful for operators that conditionally start polling one of their children only after
/// other child has finished, allowing to perform some early work and accumulating batches in
/// memory so that they can be served immediately when requested.
#[derive(Debug, Clone)]
pub struct BufferExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    capacity: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl BufferExec {
    /// Builds a new [BufferExec] with the provided capacity in bytes.
    pub fn new(input: Arc<dyn ExecutionPlan>, capacity: usize) -> Self {
        let properties = PlanProperties::clone(input.properties())
            .with_scheduling_type(SchedulingType::Cooperative);

        Self {
            input,
            properties: Arc::new(properties),
            capacity,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Returns the input [ExecutionPlan] of this [BufferExec].
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Returns the per-partition capacity in bytes for this [BufferExec].
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(self)
        }
    }
}

impl DisplayAs for BufferExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "BufferExec: capacity={}", self.capacity)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "target_batch_size={}", self.capacity)
            }
        }
    }
}

impl ExecutionPlan for BufferExec {
    fn name(&self) -> &str {
        "BufferExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        if children.len() != 1 {
            return plan_err!("BufferExec can only have one child");
        }
        Ok(Arc::new(Self::new(children.swap_remove(0), self.capacity)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mem_reservation = MemoryConsumer::new(format!("BufferExec[{partition}]"))
            .register(context.memory_pool());
        let in_stream = self.input.execute(partition, context)?;

        // Set up the metrics for the stream.
        let curr_mem_in = Arc::new(AtomicUsize::new(0));
        let curr_mem_out = Arc::clone(&curr_mem_in);
        let mut max_mem_in = 0;
        let max_mem = MetricBuilder::new(&self.metrics).gauge("max_mem_used", partition);

        let curr_queued_in = Arc::new(AtomicUsize::new(0));
        let curr_queued_out = Arc::clone(&curr_queued_in);
        let mut max_queued_in = 0;
        let max_queued = MetricBuilder::new(&self.metrics).gauge("max_queued", partition);

        // Capture metrics when an element is queued on the stream.
        let in_stream = in_stream.inspect_ok(move |v| {
            let size = v.get_array_memory_size();
            let curr_size = curr_mem_in.fetch_add(size, Ordering::Relaxed) + size;
            if curr_size > max_mem_in {
                max_mem_in = curr_size;
                max_mem.set(max_mem_in);
            }

            let curr_queued = curr_queued_in.fetch_add(1, Ordering::Relaxed) + 1;
            if curr_queued > max_queued_in {
                max_queued_in = curr_queued;
                max_queued.set(max_queued_in);
            }
        });
        // Buffer the input.
        let out_stream =
            MemoryBufferedStream::new(in_stream, self.capacity, mem_reservation);
        // Update in the metrics that when an element gets out, some memory gets freed.
        let out_stream = out_stream.inspect_ok(move |v| {
            curr_mem_out.fetch_sub(v.get_array_memory_size(), Ordering::Relaxed);
            curr_queued_out.fetch_sub(1, Ordering::Relaxed);
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            out_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        match self.input.try_swapping_with_projection(projection)? {
            Some(new_input) => Ok(Some(
                Arc::new(self.clone()).with_new_children(vec![new_input])?,
            )),
            None => Ok(None),
        }
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        // CoalesceBatchesExec is transparent for sort ordering - it preserves order
        // Delegate to the child and wrap with a new CoalesceBatchesExec
        self.input.try_pushdown_sort(order)?.try_map(|new_input| {
            Ok(Arc::new(Self::new(new_input, self.capacity)) as Arc<dyn ExecutionPlan>)
        })
    }
}

/// Represents anything that occupies a capacity in a [MemoryBufferedStream].
pub trait SizedMessage {
    fn size(&self) -> usize;
}

impl SizedMessage for RecordBatch {
    fn size(&self) -> usize {
        self.get_array_memory_size()
    }
}

pin_project! {
/// Decouples production and consumption of messages in a stream with an internal queue, eagerly
/// filling it up to the specified maximum capacity even before any message is requested.
///
/// Allows each message to have a different size, which is taken into account for determining if
/// the queue is full or not.
pub struct MemoryBufferedStream<T: SizedMessage> {
    task: SpawnedTask<()>,
    batch_rx: UnboundedReceiver<Result<(T, OwnedSemaphorePermit)>>,
    memory_reservation: Arc<MemoryReservation>,
}}

impl<T: Send + SizedMessage + 'static> MemoryBufferedStream<T> {
    /// Builds a new [MemoryBufferedStream] with the provided capacity and event handler.
    ///
    /// This immediately spawns a Tokio task that will start consumption of the input stream.
    pub fn new(
        mut input: impl Stream<Item = Result<T>> + Unpin + Send + 'static,
        capacity: usize,
        memory_reservation: MemoryReservation,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(capacity));
        let (batch_tx, batch_rx) = tokio::sync::mpsc::unbounded_channel();

        let memory_reservation = Arc::new(memory_reservation);
        let memory_reservation_clone = Arc::clone(&memory_reservation);
        let task = SpawnedTask::spawn(async move {
            loop {
                // Select on both the input stream and the channel being closed.
                // By down this, we abort polling the input as soon as the consumer channel is
                // closed. Otherwise, we would need to wait for a full new message to be available
                // in order to consider aborting the stream
                let item_or_err = tokio::select! {
                    biased;
                    _ = batch_tx.closed() => break,
                    item_or_err = input.next() => {
                        let Some(item_or_err) = item_or_err else {
                            break; // stream finished
                        };
                        item_or_err
                    }
                };

                let item = match item_or_err {
                    Ok(batch) => batch,
                    Err(err) => {
                        let _ = batch_tx.send(Err(err)); // If there's an error it means the channel was closed, which is fine.
                        break;
                    }
                };

                let size = item.size();
                if let Err(err) = memory_reservation.try_grow(size) {
                    let _ = batch_tx.send(Err(err)); // If there's an error it means the channel was closed, which is fine.
                    break;
                }

                // We need to cap the minimum between amount of permits and the actual size of the
                // message. If at any point we try to acquire more permits than the capacity of the
                // semaphore, the stream will deadlock.
                let capped_size = size.min(capacity) as u32;

                let semaphore = Arc::clone(&semaphore);
                let Ok(permit) = semaphore.acquire_many_owned(capped_size).await else {
                    let _ = batch_tx.send(internal_err!("Closed semaphore in MemoryBufferedStream. This is a bug in DataFusion, please report it!"));
                    break;
                };

                if batch_tx.send(Ok((item, permit))).is_err() {
                    break; // stream was closed
                };
            }
        });

        Self {
            task,
            batch_rx,
            memory_reservation: memory_reservation_clone,
        }
    }

    /// Returns the number of queued messages.
    pub fn messages_queued(&self) -> usize {
        self.batch_rx.len()
    }
}

impl<T: SizedMessage> Stream for MemoryBufferedStream<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_project = self.project();
        match self_project.batch_rx.poll_recv(cx) {
            Poll::Ready(Some(Ok((item, _semaphore_permit)))) => {
                self_project.memory_reservation.shrink(item.size());
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.batch_rx.is_closed() {
            let len = self.batch_rx.len();
            (len, Some(len))
        } else {
            (self.batch_rx.len(), None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{DataFusionError, assert_contains};
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryPool, UnboundedMemoryPool,
    };
    use std::error::Error;
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn buffers_only_some_messages() -> Result<(), Box<dyn Error>> {
        let input = futures::stream::iter([1, 2, 3, 4]).map(Ok);
        let (_, res) = memory_pool_and_reservation();

        let buffered = MemoryBufferedStream::new(input, 4, res);
        wait_for_buffering().await;
        assert_eq!(buffered.messages_queued(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn yields_all_messages() -> Result<(), Box<dyn Error>> {
        let input = futures::stream::iter([1, 2, 3, 4]).map(Ok);
        let (_, res) = memory_pool_and_reservation();

        let mut buffered = MemoryBufferedStream::new(input, 10, res);
        wait_for_buffering().await;
        assert_eq!(buffered.messages_queued(), 4);

        pull_ok_msg(&mut buffered).await?;
        pull_ok_msg(&mut buffered).await?;
        pull_ok_msg(&mut buffered).await?;
        pull_ok_msg(&mut buffered).await?;
        finished(&mut buffered).await?;
        Ok(())
    }

    #[tokio::test]
    async fn yields_first_msg_even_if_big() -> Result<(), Box<dyn Error>> {
        let input = futures::stream::iter([25, 1, 2, 3]).map(Ok);
        let (_, res) = memory_pool_and_reservation();

        let mut buffered = MemoryBufferedStream::new(input, 10, res);
        wait_for_buffering().await;
        assert_eq!(buffered.messages_queued(), 1);
        pull_ok_msg(&mut buffered).await?;
        Ok(())
    }

    #[tokio::test]
    async fn memory_pool_kills_stream() -> Result<(), Box<dyn Error>> {
        let input = futures::stream::iter([1, 2, 3, 4]).map(Ok);
        let (_, res) = bounded_memory_pool_and_reservation(7);

        let mut buffered = MemoryBufferedStream::new(input, 10, res);
        wait_for_buffering().await;

        pull_ok_msg(&mut buffered).await?;
        pull_ok_msg(&mut buffered).await?;
        pull_ok_msg(&mut buffered).await?;
        let msg = pull_err_msg(&mut buffered).await?;

        assert_contains!(msg.to_string(), "Failed to allocate additional 4.0 B");
        Ok(())
    }

    #[tokio::test]
    async fn memory_pool_does_not_kill_stream() -> Result<(), Box<dyn Error>> {
        let input = futures::stream::iter([1, 2, 3, 4]).map(Ok);
        let (_, res) = bounded_memory_pool_and_reservation(7);

        let mut buffered = MemoryBufferedStream::new(input, 3, res);
        wait_for_buffering().await;
        pull_ok_msg(&mut buffered).await?;

        wait_for_buffering().await;
        pull_ok_msg(&mut buffered).await?;

        wait_for_buffering().await;
        pull_ok_msg(&mut buffered).await?;

        wait_for_buffering().await;
        pull_ok_msg(&mut buffered).await?;

        wait_for_buffering().await;
        finished(&mut buffered).await?;
        Ok(())
    }

    #[tokio::test]
    async fn messages_pass_even_if_all_exceed_limit() -> Result<(), Box<dyn Error>> {
        let input = futures::stream::iter([3, 3, 3, 3]).map(Ok);
        let (_, res) = memory_pool_and_reservation();

        let mut buffered = MemoryBufferedStream::new(input, 2, res);
        wait_for_buffering().await;
        assert_eq!(buffered.messages_queued(), 1);
        pull_ok_msg(&mut buffered).await?;

        wait_for_buffering().await;
        assert_eq!(buffered.messages_queued(), 1);
        pull_ok_msg(&mut buffered).await?;

        wait_for_buffering().await;
        assert_eq!(buffered.messages_queued(), 1);
        pull_ok_msg(&mut buffered).await?;

        wait_for_buffering().await;
        assert_eq!(buffered.messages_queued(), 1);
        pull_ok_msg(&mut buffered).await?;

        wait_for_buffering().await;
        finished(&mut buffered).await?;
        Ok(())
    }

    #[tokio::test]
    async fn errors_get_propagated() -> Result<(), Box<dyn Error>> {
        let input = futures::stream::iter([1, 2, 3, 4]).map(|v| {
            if v == 3 {
                return internal_err!("Error on 3");
            }
            Ok(v)
        });
        let (_, res) = memory_pool_and_reservation();

        let mut buffered = MemoryBufferedStream::new(input, 10, res);
        wait_for_buffering().await;

        pull_ok_msg(&mut buffered).await?;
        pull_ok_msg(&mut buffered).await?;
        pull_err_msg(&mut buffered).await?;

        Ok(())
    }

    #[tokio::test]
    async fn memory_gets_released_if_stream_drops() -> Result<(), Box<dyn Error>> {
        let input = futures::stream::iter([1, 2, 3, 4]).map(Ok);
        let (pool, res) = memory_pool_and_reservation();

        let mut buffered = MemoryBufferedStream::new(input, 10, res);
        wait_for_buffering().await;
        assert_eq!(buffered.messages_queued(), 4);
        assert_eq!(pool.reserved(), 10);

        pull_ok_msg(&mut buffered).await?;
        assert_eq!(buffered.messages_queued(), 3);
        assert_eq!(pool.reserved(), 9);

        pull_ok_msg(&mut buffered).await?;
        assert_eq!(buffered.messages_queued(), 2);
        assert_eq!(pool.reserved(), 7);

        drop(buffered);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    fn memory_pool_and_reservation() -> (Arc<dyn MemoryPool>, MemoryReservation) {
        let pool = Arc::new(UnboundedMemoryPool::default()) as _;
        let reservation = MemoryConsumer::new("test").register(&pool);
        (pool, reservation)
    }

    fn bounded_memory_pool_and_reservation(
        size: usize,
    ) -> (Arc<dyn MemoryPool>, MemoryReservation) {
        let pool = Arc::new(GreedyMemoryPool::new(size)) as _;
        let reservation = MemoryConsumer::new("test").register(&pool);
        (pool, reservation)
    }

    async fn wait_for_buffering() {
        // We do not have control over the spawned task, so the best we can do is to yield some
        // cycles to the tokio runtime and let the task make progress on its own.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    async fn pull_ok_msg<T: SizedMessage>(
        buffered: &mut MemoryBufferedStream<T>,
    ) -> Result<T, Box<dyn Error>> {
        Ok(timeout(Duration::from_millis(1), buffered.next())
            .await?
            .unwrap_or_else(|| internal_err!("Stream should not have finished"))?)
    }

    async fn pull_err_msg<T: SizedMessage + Debug>(
        buffered: &mut MemoryBufferedStream<T>,
    ) -> Result<DataFusionError, Box<dyn Error>> {
        Ok(timeout(Duration::from_millis(1), buffered.next())
            .await?
            .map(|v| match v {
                Ok(v) => internal_err!(
                    "Stream should not have failed, but succeeded with {v:?}"
                ),
                Err(err) => Ok(err),
            })
            .unwrap_or_else(|| internal_err!("Stream should not have finished"))?)
    }

    async fn finished<T: SizedMessage>(
        buffered: &mut MemoryBufferedStream<T>,
    ) -> Result<(), Box<dyn Error>> {
        match timeout(Duration::from_millis(1), buffered.next())
            .await?
            .is_none()
        {
            true => Ok(()),
            false => internal_err!("Stream should have finished")?,
        }
    }

    impl SizedMessage for usize {
        fn size(&self) -> usize {
            *self
        }
    }
}
