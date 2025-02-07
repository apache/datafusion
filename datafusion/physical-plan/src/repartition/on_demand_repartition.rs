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

//! This file implements the [`OnDemandRepartitionExec`]  operator, which maps N input
//! partitions to M output partitions based on a partitioning scheme, optionally
//! maintaining the order of the input rows in the output. The operator is similar to the [`RepartitionExec`]
//! operator, but it doesn't distribute the data to the output streams until the downstreams request the data.
//!
//! [`RepartitionExec`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/repartition/struct.RepartitionExec.html

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, vec};

use super::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use super::{
    DisplayAs, ExecutionPlanProperties, MaybeBatch, RecordBatchStream,
    RepartitionExecBase, SendableRecordBatchStream,
};
use crate::common::SharedMemoryReservation;
use crate::execution_plan::CardinalityEffect;
use crate::metrics::{self, BaselineMetrics, MetricBuilder};
use crate::projection::{all_columns, make_with_child, ProjectionExec};
use crate::repartition::distributor_channels::{
    DistributionReceiver, DistributionSender,
};
use crate::repartition::RepartitionExecStateBuilder;
use crate::sorts::streaming_merge::StreamingMergeBuilder;
use crate::stream::RecordBatchStreamAdapter;
use crate::{DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, Statistics};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_channel::{Receiver, Sender};

use datafusion_common::{internal_datafusion_err, Result};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::TaskContext;

use datafusion_common::HashMap;
use futures::stream::Stream;
use futures::{ready, FutureExt, StreamExt, TryStreamExt};
use log::trace;
use parking_lot::Mutex;

type PartitionChannels = (Vec<Sender<usize>>, Vec<Receiver<usize>>);

/// The OnDemandRepartitionExec operator repartitions the input data based on a push-based model.
/// It is similar to the RepartitionExec operator, but it doesn't distribute the data to the output
/// partitions until the output partitions request the data.
///
/// When polling, the operator sends the output partition number to the one partition channel, then the prefetch buffer will distribute the data based on the order of the partition number.
/// Each input steams has a prefetch buffer(channel) to distribute the data to the output partitions.
///
/// The following diagram illustrates the data flow of the OnDemandRepartitionExec operator with 3 output partitions for the input stream 1:
/// ```text
///         /\                     /\                     /\
///         ││                     ││                     ││
///         ││                     ││                     ││
///         ││                     ││                     ││
/// ┌───────┴┴────────┐    ┌───────┴┴────────┐    ┌───────┴┴────────┐
/// │     Stream      │    │     Stream      │    │     Stream      │
/// │       (1)       │    │       (2)       │    │       (3)       │
/// └────────┬────────┘    └───────┬─────────┘    └────────┬────────┘
///          │                     │                       │    / \
///          │                     │                       │    | |
///          │                     │                       │    | |
///          └────────────────┐    │    ┌──────────────────┘    | |
///                           │    │    │                       | |
///                           ▼    ▼    ▼                       | |
///                       ┌─────────────────┐                   | |
///  Send the partition   │ partion channel │                   | |
///  number when polling  │                 │                   | |
///                       └────────┬────────┘                   | |
///                                │                            | |
///                                │                            | |
///                                │  Get the partition number  | |
///                                ▼  then send data            | |
///                       ┌─────────────────┐                   | |
///                       │ Prefetch Buffer │───────────────────┘ |
///                       │       (1)       │─────────────────────┘
///                       └─────────────────┘ Distribute data to the output partitions
///
/// ```text

#[derive(Debug, Clone)]
pub struct OnDemandRepartitionExec {
    base: RepartitionExecBase,
    /// Channel to send partition number to the downstream task
    partition_channels: Arc<tokio::sync::OnceCell<Mutex<PartitionChannels>>>,
}

impl OnDemandRepartitionExec {
    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.base.input
    }

    /// Partitioning scheme to use
    pub fn partitioning(&self) -> &Partitioning {
        &self.base.cache.partitioning
    }

    /// Get preserve_order flag of the RepartitionExecutor
    /// `true` means `SortPreservingRepartitionExec`, `false` means `OnDemandRepartitionExec`
    pub fn preserve_order(&self) -> bool {
        self.base.preserve_order
    }

    /// Specify if this reparititoning operation should preserve the order of
    /// rows from its input when producing output. Preserving order is more
    /// expensive at runtime, so should only be set if the output of this
    /// operator can take advantage of it.
    ///
    /// If the input is not ordered, or has only one partition, this is a no op,
    /// and the node remains a `OnDemandRepartitionExec`.
    pub fn with_preserve_order(mut self) -> Self {
        self.base = self.base.with_preserve_order();
        self
    }

    /// Get name used to display this Exec
    pub fn name(&self) -> &str {
        "OnDemandRepartitionExec"
    }
}

impl DisplayAs for OnDemandRepartitionExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "{}: partitioning={}, input_partitions={}",
                    self.name(),
                    self.partitioning(),
                    self.base.input.output_partitioning().partition_count()
                )?;

                if self.base.preserve_order {
                    write!(f, ", preserve_order=true")?;
                }

                if let Some(sort_exprs) = self.base.sort_exprs() {
                    write!(f, ", sort_exprs={}", sort_exprs.clone())?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for OnDemandRepartitionExec {
    fn name(&self) -> &'static str {
        "OnDemandRepartitionExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.base.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.base.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut repartition = OnDemandRepartitionExec::try_new(
            children.swap_remove(0),
            self.partitioning().clone(),
        )?;
        if self.base.preserve_order {
            repartition = repartition.with_preserve_order();
        }
        Ok(Arc::new(repartition))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        RepartitionExecBase::maintains_input_order_helper(
            self.input(),
            self.base.preserve_order,
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start {}::execute for partition: {}",
            self.name(),
            partition
        );

        let lazy_state = Arc::clone(&self.base.state);
        let partition_channels = Arc::clone(&self.partition_channels);
        let input = Arc::clone(&self.base.input);
        let partitioning = self.partitioning().clone();
        let metrics = self.base.metrics.clone();
        let preserve_order = self.base.preserve_order;
        let name = self.name().to_owned();
        let schema = self.schema();
        let schema_captured = Arc::clone(&schema);

        // Get existing ordering to use for merging
        let sort_exprs = self.base.sort_exprs().cloned().unwrap_or_default();

        let stream = futures::stream::once(async move {
            let num_input_partitions = input.output_partitioning().partition_count();
            let input_captured = Arc::clone(&input);
            let metrics_captured = metrics.clone();
            let name_captured = name.clone();
            let context_captured = Arc::clone(&context);
            let partition_channels = partition_channels
                .get_or_init(|| async move {
                    let (txs, rxs) = if preserve_order {
                        (0..num_input_partitions)
                            .map(|_| async_channel::unbounded())
                            .unzip::<_, _, Vec<_>, Vec<_>>()
                    } else {
                        let (tx, rx) = async_channel::unbounded();
                        (vec![tx], vec![rx])
                    };
                    Mutex::new((txs, rxs))
                })
                .await;
            let (partition_txs, partition_rxs) = {
                let channel = partition_channels.lock();
                (channel.0.clone(), channel.1.clone())
            };

            let state = lazy_state
                .get_or_init(|| async move {
                    Mutex::new(
                        RepartitionExecStateBuilder::new()
                            .enable_pull_based(true)
                            .partition_receivers(partition_rxs.clone())
                            .build(
                                input_captured,
                                partitioning.clone(),
                                metrics_captured,
                                preserve_order,
                                name_captured,
                                context_captured,
                            ),
                    )
                })
                .await;

            // lock scope
            let (mut rx, reservation, abort_helper) = {
                // lock mutexes
                let mut state = state.lock();

                // now return stream for the specified *output* partition which will
                // read from the channel
                let (_tx, rx, reservation) = state
                    .channels
                    .remove(&partition)
                    .expect("partition not used yet");

                (rx, reservation, Arc::clone(&state.abort_helper))
            };

            trace!(
                "Before returning stream in {}::execute for partition: {}",
                name,
                partition
            );

            if preserve_order {
                // Store streams from all the input partitions:
                let input_streams = rx
                    .into_iter()
                    .enumerate()
                    .map(|(i, receiver)| {
                        // sender should be partition-wise
                        Box::pin(OnDemandPerPartitionStream {
                            schema: Arc::clone(&schema_captured),
                            receiver,
                            _drop_helper: Arc::clone(&abort_helper),
                            reservation: Arc::clone(&reservation),
                            sender: partition_txs[i].clone(),
                            partition,
                            is_requested: false,
                        }) as SendableRecordBatchStream
                    })
                    .collect::<Vec<_>>();
                // Note that receiver size (`rx.len()`) and `num_input_partitions` are same.

                // Merge streams (while preserving ordering) coming from
                // input partitions to this partition:
                let fetch = None;
                let merge_reservation =
                    MemoryConsumer::new(format!("{}[Merge {partition}]", name))
                        .register(context.memory_pool());
                StreamingMergeBuilder::new()
                    .with_streams(input_streams)
                    .with_schema(schema_captured)
                    .with_expressions(&sort_exprs)
                    .with_metrics(BaselineMetrics::new(&metrics, partition))
                    .with_batch_size(context.session_config().batch_size())
                    .with_fetch(fetch)
                    .with_reservation(merge_reservation)
                    .build()
            } else {
                Ok(Box::pin(OnDemandRepartitionStream {
                    num_input_partitions,
                    num_input_partitions_processed: 0,
                    schema: input.schema(),
                    input: rx.swap_remove(0),
                    _drop_helper: abort_helper,
                    reservation,
                    sender: partition_txs[0].clone(),
                    partition,
                    is_requested: false,
                }) as SendableRecordBatchStream)
            }
        })
        .try_flatten();
        let stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.base.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.base.input.statistics()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If the projection does not narrow the schema, we should not try to push it down.
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }

        // If pushdown is not beneficial or applicable, break it.
        if projection.benefits_from_input_partitioning()[0]
            || !all_columns(projection.expr())
        {
            return Ok(None);
        }

        let new_projection = make_with_child(projection, self.input())?;

        Ok(Some(Arc::new(OnDemandRepartitionExec::try_new(
            new_projection,
            self.partitioning().clone(),
        )?)))
    }
}

impl OnDemandRepartitionExec {
    /// Create a new RepartitionExec, that produces output `partitioning`, and
    /// does not preserve the order of the input (see [`Self::with_preserve_order`]
    /// for more details)
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Self> {
        let preserve_order = false;
        let cache = RepartitionExecBase::compute_properties(
            &input,
            partitioning.clone(),
            preserve_order,
        );
        Ok(OnDemandRepartitionExec {
            base: RepartitionExecBase {
                input,
                state: Default::default(),
                metrics: ExecutionPlanMetricsSet::new(),
                preserve_order,
                cache,
            },
            partition_channels: Default::default(),
        })
    }
    // Executes the input plan and poll stream into the buffer, records fetch_time and buffer_time metrics
    async fn process_input(
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        buffer_tx: Sender<RecordBatch>,
        context: Arc<TaskContext>,
        fetch_time: metrics::Time,
        send_buffer_time: metrics::Time,
    ) -> Result<()> {
        let timer = fetch_time.timer();
        let mut stream = input.execute(partition, context).map_err(|e| {
            internal_datafusion_err!(
                "Error executing input partition {} for on demand repartitioning: {}",
                partition,
                e
            )
        })?;
        timer.done();

        loop {
            let timer = fetch_time.timer();
            let batch = stream.next().await;
            timer.done();

            let Some(batch) = batch else {
                break;
            };
            let timer = send_buffer_time.timer();
            // Feed the buffer with batch, since the buffer channel has limited capacity
            // The process waits here until one is consumed
            buffer_tx.send(batch?).await.map_err(|e| {
                internal_datafusion_err!(
                    "Error sending batch to buffer channel for partition {}: {}",
                    partition,
                    e
                )
            })?;
            timer.done();
        }

        Ok(())
    }

    /// Pulls data from the specified input plan, feeding it to the
    /// output partitions based on the desired partitioning
    ///
    /// txs hold the output sending channels for each output partition
    pub(crate) async fn pull_from_input(
        input: Arc<dyn ExecutionPlan>,
        input_partition: usize,
        mut output_channels: HashMap<
            usize,
            (DistributionSender<MaybeBatch>, SharedMemoryReservation),
        >,
        partitioning: Partitioning,
        output_partition_rx: Receiver<usize>,
        metrics: OnDemandRepartitionMetrics,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        // initialize buffer channel so that we can pre-fetch from input
        let (buffer_tx, buffer_rx) = async_channel::bounded::<RecordBatch>(2);
        // execute the child operator in a separate task
        // that pushes batches into buffer channel with limited capacity
        let processing_task = SpawnedTask::spawn(Self::process_input(
            Arc::clone(&input),
            input_partition,
            buffer_tx,
            Arc::clone(&context),
            metrics.fetch_time.clone(),
            metrics.send_buffer_time.clone(),
        ));

        let mut batches_until_yield = partitioning.partition_count();
        // When the input is done, break the loop
        while !output_channels.is_empty() {
            // Fetch the batch from the buffer, ideally this should reduce the time gap between the requester and the input stream
            let batch = match buffer_rx.recv().await {
                Ok(batch) => batch,
                _ => break,
            };

            // Wait until a partition is requested, then get the output partition information
            let partition = output_partition_rx.recv().await.map_err(|e| {
                internal_datafusion_err!(
                    "Error receiving partition number from output partition: {}",
                    e
                )
            })?;

            let size = batch.get_array_memory_size();

            let timer = metrics.send_time[partition].timer();
            // if there is still a receiver, send to it
            if let Some((tx, reservation)) = output_channels.get_mut(&partition) {
                reservation.lock().try_grow(size)?;

                if tx.send(Some(Ok(batch))).await.is_err() {
                    // If the other end has hung up, it was an early shutdown (e.g. LIMIT)
                    reservation.lock().shrink(size);
                    output_channels.remove(&partition);
                }
            }
            timer.done();

            // If the input stream is endless, we may spin forever and
            // never yield back to tokio.  See
            // https://github.com/apache/datafusion/issues/5278.
            //
            // However, yielding on every batch causes a bottleneck
            // when running with multiple cores. See
            // https://github.com/apache/datafusion/issues/6290
            //
            // Thus, heuristically yield after producing num_partition
            // batches
            if batches_until_yield == 0 {
                tokio::task::yield_now().await;
                batches_until_yield = partitioning.partition_count();
            } else {
                batches_until_yield -= 1;
            }
        }

        processing_task.join().await.map_err(|e| {
            internal_datafusion_err!("Error waiting for processing task to finish: {}", e)
        })??;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OnDemandRepartitionMetrics {
    /// Time in nanos to execute child operator and fetch batches
    fetch_time: metrics::Time,
    /// Time in nanos for sending resulting batches to buffer channels.
    send_buffer_time: metrics::Time,
    /// Time in nanos for sending resulting batches to channels.
    ///
    /// One metric per output partition.
    send_time: Vec<metrics::Time>,
}

impl OnDemandRepartitionMetrics {
    pub fn new(
        input_partition: usize,
        num_output_partitions: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        // Time in nanos to execute child operator and fetch batches
        let fetch_time =
            MetricBuilder::new(metrics).subset_time("fetch_time", input_partition);

        // Time in nanos for sending resulting batches to channels
        let send_time = (0..num_output_partitions)
            .map(|output_partition| {
                let label =
                    metrics::Label::new("outputPartition", output_partition.to_string());
                MetricBuilder::new(metrics)
                    .with_label(label)
                    .subset_time("send_time", input_partition)
            })
            .collect();

        // Time in nanos for sending resulting batches to buffer channels
        let send_buffer_time =
            MetricBuilder::new(metrics).subset_time("send_buffer_time", input_partition);
        Self {
            fetch_time,
            send_time,
            send_buffer_time,
        }
    }
}

/// This struct converts a receiver to a stream.
/// Receiver receives data on an SPSC channel.
struct OnDemandPerPartitionStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    receiver: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    _drop_helper: Arc<Vec<SpawnedTask<()>>>,

    /// Memory reservation.
    reservation: SharedMemoryReservation,

    /// Sender to send partititon number to the receiver
    sender: Sender<usize>,

    /// Partition number
    partition: usize,

    /// Sender State
    is_requested: bool,
}

impl Stream for OnDemandPerPartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if !self.is_requested && !self.sender.is_closed() {
            self.sender.try_send(self.partition).map_err(|e| {
                internal_datafusion_err!(
                    "Error sending partition number to the receiver for partition {}: {}",
                    self.partition,
                    e
                )
            })?;
            self.is_requested = true;
        }

        let result = ready!(self.receiver.recv().poll_unpin(cx));
        self.is_requested = false;

        match result {
            Some(Some(batch_result)) => {
                if let Ok(batch) = &batch_result {
                    self.reservation
                        .lock()
                        .shrink(batch.get_array_memory_size());
                }
                Poll::Ready(Some(batch_result))
            }
            _ => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for OnDemandPerPartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct OnDemandRepartitionStream {
    /// Number of input partitions that will be sending batches to this output channel
    num_input_partitions: usize,

    /// Number of input partitions that have finished sending batches to this output channel
    num_input_partitions_processed: usize,

    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    input: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    _drop_helper: Arc<Vec<SpawnedTask<()>>>,

    /// Memory reservation.
    reservation: SharedMemoryReservation,

    /// Sender for the output partition
    sender: Sender<usize>,

    /// Partition number
    partition: usize,

    /// Sender state
    is_requested: bool,
}

impl Stream for OnDemandRepartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            // Send partition number to input partitions
            if !self.is_requested && !self.sender.is_closed() {
                self.sender.try_send(self.partition).map_err(|e| {
                    internal_datafusion_err!(
                        "Error sending partition number to the receiver for partition {}: {}",
                        self.partition,
                        e
                    )
                })?;
                self.is_requested = true;
            }

            let result = ready!(self.input.recv().poll_unpin(cx));
            self.is_requested = false;

            match result {
                Some(Some(v)) => {
                    if let Ok(batch) = &v {
                        self.reservation
                            .lock()
                            .shrink(batch.get_array_memory_size());
                    }
                    return Poll::Ready(Some(v));
                }
                Some(None) => {
                    self.num_input_partitions_processed += 1;

                    if self.num_input_partitions == self.num_input_partitions_processed {
                        // all input partitions have finished sending batches
                        return Poll::Ready(None);
                    } else {
                        // other partitions still have data to send
                        continue;
                    }
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for OnDemandRepartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::{
        collect,
        memory::MemorySourceConfig,
        source::DataSourceExec,
        test::{
            assert_is_pending,
            exec::{
                assert_strong_count_converges_to_zero, BarrierExec, BlockingExec,
                ErrorExec, MockExec,
            },
        },
    };

    use arrow::array::{ArrayRef, StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::cast::as_string_array;
    use datafusion_common::{assert_batches_sorted_eq, exec_err};
    use tokio::task::JoinSet;

    use arrow_schema::SortOptions;

    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::union::UnionExec;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

    /// Asserts that the plan is as expected
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$PLAN`: the plan to optimized
    ///
    macro_rules! assert_plan {
        ($EXPECTED_PLAN_LINES: expr,  $PLAN: expr) => {
            let physical_plan = $PLAN;
            let formatted = crate::displayable(&physical_plan).indent(true).to_string();
            let actual: Vec<&str> = formatted.trim().lines().collect();

            let expected_plan_lines: Vec<&str> = $EXPECTED_PLAN_LINES
                .iter().map(|s| *s).collect();

            assert_eq!(
                expected_plan_lines, actual,
                "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );
        };
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    async fn repartition(
        schema: &SchemaRef,
        input_partitions: Vec<Vec<RecordBatch>>,
        partitioning: Partitioning,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let task_ctx = Arc::new(TaskContext::default());
        // create physical plan
        let exec = MemorySourceConfig::try_new_exec(
            &input_partitions,
            Arc::clone(schema),
            None,
        )?;
        let exec = OnDemandRepartitionExec::try_new(exec, partitioning)?;

        // execute and collect results
        let mut output_partitions = vec![];
        for i in 0..exec.partitioning().partition_count() {
            // execute this *output* partition and collect all batches
            let mut stream = exec.execute(i, Arc::clone(&task_ctx))?;
            let mut batches = vec![];
            while let Some(result) = stream.next().await {
                batches.push(result?);
            }
            output_partitions.push(batches);
        }
        Ok(output_partitions)
    }

    #[tokio::test]
    async fn many_to_one_on_demand() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 1 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::OnDemand(1)).await?;

        assert_eq!(1, output_partitions.len());
        assert_eq!(150, output_partitions[0].len());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_on_demand() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        let output_partitions =
            repartition(&schema, partitions, Partitioning::OnDemand(8)).await?;

        let total_rows: usize = output_partitions
            .iter()
            .map(|x| x.iter().map(|x| x.num_rows()).sum::<usize>())
            .sum();

        assert_eq!(8, output_partitions.len());
        assert_eq!(total_rows, 8 * 50 * 3);

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_on_demand_with_coalesce() -> Result<()> {
        let schema = test_schema();
        let partition: Vec<RecordBatch> = create_vec_batches(2);
        let partitions = vec![partition.clone(), partition.clone()];
        let input =
            MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&schema), None)?;
        let exec =
            OnDemandRepartitionExec::try_new(input, Partitioning::OnDemand(3)).unwrap();

        let coalesce_exec =
            CoalescePartitionsExec::new(Arc::new(exec) as Arc<dyn ExecutionPlan>);

        let expected_plan = [
            "CoalescePartitionsExec",
            "  OnDemandRepartitionExec: partitioning=OnDemand(3), input_partitions=2",
            "    DataSourceExec: partitions=2, partition_sizes=[2, 2]",
        ];
        assert_plan!(expected_plan, coalesce_exec.clone());

        // execute the plan
        let task_ctx = Arc::new(TaskContext::default());
        let stream = coalesce_exec.execute(0, task_ctx)?;
        let batches = crate::common::collect(stream).await?;

        #[rustfmt::skip]
        let expected = vec![
            "+----+",
            "| c0 |",
            "+----+",
            "| 1  |",
            "| 1  |",
            "| 1  |",
            "| 1  |",
            "| 2  |",
            "| 2  |",
            "| 2  |",
            "| 2  |",
            "| 3  |",
            "| 3  |",
            "| 3  |",
            "| 3  |",
            "| 4  |",
            "| 4  |",
            "| 4  |",
            "| 4  |",
            "| 5  |",
            "| 5  |",
            "| 5  |",
            "| 5  |",
            "| 6  |",
            "| 6  |",
            "| 6  |",
            "| 6  |",
            "| 7  |",
            "| 7  |",
            "| 7  |",
            "| 7  |",
            "| 8  |",
            "| 8  |",
            "| 8  |",
            "| 8  |",
            "+----+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn error_for_input_exec() {
        // This generates an error on a call to execute. The error
        // should be returned and no results produced.

        let task_ctx = Arc::new(TaskContext::default());
        let input = ErrorExec::new();
        let partitioning = Partitioning::OnDemand(1);
        let exec =
            OnDemandRepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        // Note: this should pass (the stream can be created) but the
        // error when the input is executed should get passed back
        let output_stream = exec.execute(0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string.contains("ErrorExec, unsurprisingly, errored in partition 0"),
            "actual: {result_string}"
        );
    }

    #[tokio::test]
    async fn repartition_with_error_in_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        // input stream returns one good batch and then one error. The
        // error should be returned.
        let err = exec_err!("bad data error");

        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch), err], schema);
        let partitioning = Partitioning::OnDemand(1);
        let exec =
            OnDemandRepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        // Note: this should pass (the stream can be created) but the
        // error when the input is executed should get passed back
        let output_stream = exec.execute(0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string.contains("bad data error"),
            "actual: {result_string}"
        );
    }

    #[tokio::test]
    async fn repartition_with_delayed_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let batch1 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["frob", "baz"])) as ArrayRef,
        )])
        .unwrap();

        // The mock exec doesn't return immediately (instead it
        // requires the input to wait at least once)
        let schema = batch1.schema();
        let expected_batches = vec![batch1.clone(), batch2.clone()];
        let input = MockExec::new(vec![Ok(batch1), Ok(batch2)], schema);
        let partitioning = Partitioning::OnDemand(1);

        let exec =
            OnDemandRepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        let expected = vec![
            "+------------------+",
            "| my_awesome_field |",
            "+------------------+",
            "| foo              |",
            "| bar              |",
            "| frob             |",
            "| baz              |",
            "+------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &expected_batches);

        let output_stream = exec.execute(0, task_ctx).unwrap();
        let batches = crate::common::collect(output_stream).await.unwrap();

        assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn hash_repartition_avoid_empty_batch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let batch = RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(StringArray::from(vec!["foo"])) as ArrayRef,
        )])
        .unwrap();
        let partitioning = Partitioning::OnDemand(2);
        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch)], schema);
        let exec =
            OnDemandRepartitionExec::try_new(Arc::new(input), partitioning).unwrap();
        let output_stream0 = exec.execute(0, Arc::clone(&task_ctx)).unwrap();
        let batch0 = crate::common::collect(output_stream0).await.unwrap();
        let output_stream1 = exec.execute(1, Arc::clone(&task_ctx)).unwrap();
        let batch1 = crate::common::collect(output_stream1).await.unwrap();
        assert!(batch0.is_empty() || batch1.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn on_demand_repartition_with_dropping_output_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let partitioning = Partitioning::OnDemand(2);

        // We first collect the results without dropping the output stream.
        let input = Arc::new(make_barrier_exec());
        let exec = OnDemandRepartitionExec::try_new(
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            partitioning.clone(),
        )
        .unwrap();
        let output_stream1 = exec.execute(1, Arc::clone(&task_ctx)).unwrap();
        let mut background_task = JoinSet::new();
        background_task.spawn(async move {
            input.wait().await;
        });
        let batches_without_drop = crate::common::collect(output_stream1).await.unwrap();

        // run some checks on the result
        let items_vec = str_batches_to_vec(&batches_without_drop);
        let items_set: HashSet<&str> = items_vec.iter().copied().collect();
        assert_eq!(items_vec.len(), items_set.len());
        let source_str_set: HashSet<&str> =
            ["foo", "bar", "frob", "baz", "goo", "gar", "grob", "gaz"]
                .iter()
                .copied()
                .collect();
        assert_eq!(items_set.difference(&source_str_set).count(), 0);

        // Now do the same but dropping the stream before waiting for the barrier
        let input = Arc::new(make_barrier_exec());
        let exec = OnDemandRepartitionExec::try_new(
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            partitioning,
        )
        .unwrap();
        let output_stream0 = exec.execute(0, Arc::clone(&task_ctx)).unwrap();
        let output_stream1 = exec.execute(1, Arc::clone(&task_ctx)).unwrap();
        // now, purposely drop output stream 0
        // *before* any outputs are produced
        drop(output_stream0);
        let mut background_task = JoinSet::new();
        background_task.spawn(async move {
            input.wait().await;
        });
        let batches_with_drop = crate::common::collect(output_stream1).await.unwrap();

        assert_eq!(batches_without_drop, batches_with_drop);
    }

    fn str_batches_to_vec(batches: &[RecordBatch]) -> Vec<&str> {
        batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.columns().len(), 1);
                let string_array = as_string_array(batch.column(0))
                    .expect("Unexpected type for repartitoned batch");

                string_array
                    .iter()
                    .map(|v| v.expect("Unexpected null"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    /// Create a BarrierExec that returns two partitions of two batches each
    fn make_barrier_exec() -> BarrierExec {
        let batch1 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["frob", "baz"])) as ArrayRef,
        )])
        .unwrap();

        let batch3 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["goo", "gar"])) as ArrayRef,
        )])
        .unwrap();

        let batch4 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["grob", "gaz"])) as ArrayRef,
        )])
        .unwrap();

        // The barrier exec waits to be pinged
        // requires the input to wait at least once)
        let schema = batch1.schema();
        BarrierExec::new(vec![vec![batch1, batch2], vec![batch3, batch4]], schema)
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let repartition_exec = Arc::new(OnDemandRepartitionExec::try_new(
            blocking_exec,
            Partitioning::UnknownPartitioning(1),
        )?);

        let fut = collect(repartition_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    /// Create vector batches
    fn create_vec_batches(n: usize) -> Vec<RecordBatch> {
        let batch = create_batch();
        (0..n).map(|_| batch.clone()).collect()
    }

    /// Create batch
    fn create_batch() -> RecordBatch {
        let schema = test_schema();
        RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_preserve_order() -> Result<()> {
        let schema = test_schema();
        let sort_exprs = sort_exprs(&schema);
        let source1 = sorted_memory_exec(&schema, sort_exprs.clone());
        let source2 = sorted_memory_exec(&schema, sort_exprs);
        // output has multiple partitions, and is sorted
        let union = UnionExec::new(vec![source1, source2]);
        let exec =
            OnDemandRepartitionExec::try_new(Arc::new(union), Partitioning::OnDemand(10))
                .unwrap()
                .with_preserve_order();

        // Repartition should preserve order
        let expected_plan = [
            "OnDemandRepartitionExec: partitioning=OnDemand(10), input_partitions=2, preserve_order=true, sort_exprs=c0@0 ASC",
            "  UnionExec",
            "    DataSourceExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC",
            "    DataSourceExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC",
        ];
        assert_plan!(expected_plan, exec);
        Ok(())
    }

    #[tokio::test]
    async fn test_preserve_order_with_coalesce() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "my_awesome_field",
            DataType::UInt32,
            false,
        )]));
        let options = SortOptions::default();
        let sort_exprs = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("my_awesome_field", &schema).unwrap(),
            options,
        }]);

        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(UInt32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        )])?;

        let source = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(
                &[vec![batch.clone()]],
                Arc::clone(&schema),
                None,
            )
            .unwrap()
            .try_with_sort_information(vec![sort_exprs])
            .unwrap(),
        )));

        // output has multiple partitions, and is sorted
        let union = UnionExec::new(vec![Arc::<DataSourceExec>::clone(&source), source]);
        let repartition_exec =
            OnDemandRepartitionExec::try_new(Arc::new(union), Partitioning::OnDemand(5))
                .unwrap()
                .with_preserve_order();

        let coalesce_exec = CoalescePartitionsExec::new(
            Arc::new(repartition_exec) as Arc<dyn ExecutionPlan>
        );

        // Repartition should preserve order
        let expected_plan = [
            "CoalescePartitionsExec",
            "  OnDemandRepartitionExec: partitioning=OnDemand(5), input_partitions=2, preserve_order=true, sort_exprs=my_awesome_field@0 ASC",
            "    UnionExec",
            "      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=my_awesome_field@0 ASC",
            "      DataSourceExec: partitions=1, partition_sizes=[1], output_ordering=my_awesome_field@0 ASC",

        ];
        assert_plan!(expected_plan, coalesce_exec.clone());

        let task_ctx = Arc::new(TaskContext::default());
        let stream = coalesce_exec.execute(0, task_ctx)?;
        let expected_batches = crate::common::collect(stream).await?;

        let expected = vec![
            "+------------------+",
            "| my_awesome_field |",
            "+------------------+",
            "| 1                |",
            "| 1                |",
            "| 2                |",
            "| 2                |",
            "| 3                |",
            "| 3                |",
            "| 4                |",
            "| 4                |",
            "+------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &expected_batches);
        Ok(())
    }

    #[tokio::test]
    async fn test_preserve_order_one_partition() -> Result<()> {
        let schema = test_schema();
        let sort_exprs = sort_exprs(&schema);
        let source = sorted_memory_exec(&schema, sort_exprs);
        // output is sorted, but has only a single partition, so no need to sort
        let exec = OnDemandRepartitionExec::try_new(source, Partitioning::OnDemand(10))
            .unwrap()
            .with_preserve_order();

        // Repartition should not preserve order
        let expected_plan = [
            "OnDemandRepartitionExec: partitioning=OnDemand(10), input_partitions=1",
            "  DataSourceExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC",
        ];
        assert_plan!(expected_plan, exec);
        Ok(())
    }

    #[tokio::test]
    async fn test_preserve_order_input_not_sorted() -> Result<()> {
        let schema = test_schema();
        let source1 = memory_exec(&schema);
        let source2 = memory_exec(&schema);
        // output has multiple partitions, but is not sorted
        let union = UnionExec::new(vec![source1, source2]);
        let exec =
            OnDemandRepartitionExec::try_new(Arc::new(union), Partitioning::OnDemand(10))
                .unwrap()
                .with_preserve_order();

        // Repartition should not preserve order, as there is no order to preserve
        let expected_plan = [
            "OnDemandRepartitionExec: partitioning=OnDemand(10), input_partitions=2",
            "  UnionExec",
            "    DataSourceExec: partitions=1, partition_sizes=[0]",
            "    DataSourceExec: partitions=1, partition_sizes=[0]",
        ];
        assert_plan!(expected_plan, exec);
        Ok(())
    }

    fn sort_exprs(schema: &Schema) -> LexOrdering {
        let options = SortOptions::default();
        LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("c0", schema).unwrap(),
            options,
        }])
    }

    fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(schema), None).unwrap()
    }

    fn sorted_memory_exec(
        schema: &SchemaRef,
        sort_exprs: LexOrdering,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![]], Arc::clone(schema), None)
                .unwrap()
                .try_with_sort_information(vec![sort_exprs])
                .unwrap(),
        )))
    }
}
