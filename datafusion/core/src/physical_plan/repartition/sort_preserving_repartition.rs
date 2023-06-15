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

//! The repartition operator maps N input partitions to M output partitions based on a
//! partitioning scheme.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, vec};

use crate::physical_plan::hash_utils::create_hashes;
use crate::physical_plan::repartition::distributor_channels::{
    channels, DistributionReceiver, DistributionSender,
};
use crate::physical_plan::{
    DisplayFormatType, EquivalenceProperties, ExecutionPlan, Partitioning, Statistics,
};
use arrow::array::{ArrayRef, UInt64Builder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryConsumer;
use log::trace;

use super::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use super::{RecordBatchStream, SendableRecordBatchStream};

use crate::physical_plan::common::{
    AbortOnDropMany, AbortOnDropSingle, SharedMemoryReservation,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use futures::stream::Stream;
use futures::{FutureExt, StreamExt};
use hashbrown::HashMap;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

type MaybeBatch = Option<Result<RecordBatch>>;

/// Inner state of [`SPRepartitionExec`].
#[derive(Debug)]
struct SPRepartitionExecState {
    /// Channels for sending batches from input partitions to output partitions.
    /// Key is the partition number.
    channels: HashMap<
        usize,
        (
            DistributionSender<MaybeBatch>,
            DistributionReceiver<MaybeBatch>,
            SharedMemoryReservation,
        ),
    >,

    /// Helper that ensures that that background job is killed once it is no longer needed.
    abort_helper: Arc<AbortOnDropMany<()>>,
}

/// A utility that can be used to partition batches based on [`Partitioning`]
pub struct BatchPartitioner {
    state: BatchPartitionerState,
    timer: metrics::Time,
}

enum BatchPartitionerState {
    Hash {
        random_state: ahash::RandomState,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        num_partitions: usize,
        hash_buffer: Vec<u64>,
    },
    RoundRobin {
        num_partitions: usize,
        next_idx: usize,
    },
}

impl BatchPartitioner {
    /// Create a new [`BatchPartitioner`] with the provided [`Partitioning`]
    ///
    /// The time spent repartitioning will be recorded to `timer`
    pub fn try_new(partitioning: Partitioning, timer: metrics::Time) -> Result<Self> {
        let state = match partitioning {
            Partitioning::RoundRobinBatch(num_partitions) => {
                BatchPartitionerState::RoundRobin {
                    num_partitions,
                    next_idx: 0,
                }
            }
            Partitioning::Hash(exprs, num_partitions) => BatchPartitionerState::Hash {
                exprs,
                num_partitions,
                // Use fixed random hash
                random_state: ahash::RandomState::with_seeds(0, 0, 0, 0),
                hash_buffer: vec![],
            },
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported repartitioning scheme {other:?}"
                )))
            }
        };

        Ok(Self { state, timer })
    }

    /// Partition the provided [`RecordBatch`] into one or more partitioned [`RecordBatch`]
    /// based on the [`Partitioning`] specified on construction
    ///
    /// `f` will be called for each partitioned [`RecordBatch`] with the corresponding
    /// partition index. Any error returned by `f` will be immediately returned by this
    /// function without attempting to publish further [`RecordBatch`]
    ///
    /// The time spent repartitioning, not including time spent in `f` will be recorded
    /// to the [`metrics::Time`] provided on construction
    pub fn partition<F>(&mut self, batch: RecordBatch, mut f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        self.partition_iter(batch)?.try_for_each(|res| match res {
            Ok((partition, batch)) => f(partition, batch),
            Err(e) => Err(e),
        })
    }

    /// Actual implementation of [`partition`](Self::partition).
    ///
    /// The reason this was pulled out is that we need to have a variant of `partition` that works w/ sync functions,
    /// and one that works w/ async. Using an iterator as an intermediate representation was the best way to achieve
    /// this (so we don't need to clone the entire implementation).
    fn partition_iter(
        &mut self,
        batch: RecordBatch,
    ) -> Result<impl Iterator<Item = Result<(usize, RecordBatch)>> + Send + '_> {
        let it: Box<dyn Iterator<Item = Result<(usize, RecordBatch)>> + Send> =
            match &mut self.state {
                BatchPartitionerState::RoundRobin {
                    num_partitions,
                    next_idx,
                } => {
                    let idx = *next_idx;
                    *next_idx = (*next_idx + 1) % *num_partitions;
                    Box::new(std::iter::once(Ok((idx, batch))))
                }
                BatchPartitionerState::Hash {
                    random_state,
                    exprs,
                    num_partitions: partitions,
                    hash_buffer,
                } => {
                    let timer = self.timer.timer();

                    let arrays = exprs
                        .iter()
                        .map(|expr| {
                            Ok(expr.evaluate(&batch)?.into_array(batch.num_rows()))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    hash_buffer.clear();
                    hash_buffer.resize(batch.num_rows(), 0);

                    create_hashes(&arrays, random_state, hash_buffer)?;

                    let mut indices: Vec<_> = (0..*partitions)
                        .map(|_| UInt64Builder::with_capacity(batch.num_rows()))
                        .collect();

                    for (index, hash) in hash_buffer.iter().enumerate() {
                        indices[(*hash % *partitions as u64) as usize]
                            .append_value(index as u64);
                    }

                    let it = indices
                        .into_iter()
                        .enumerate()
                        .filter_map(|(partition, mut indices)| {
                            let indices = indices.finish();
                            (!indices.is_empty()).then_some((partition, indices))
                        })
                        .map(move |(partition, indices)| {
                            // Produce batches based on indices
                            let columns = batch
                                .columns()
                                .iter()
                                .map(|c| {
                                    arrow::compute::take(c.as_ref(), &indices, None)
                                        .map_err(DataFusionError::ArrowError)
                                })
                                .collect::<Result<Vec<ArrayRef>>>()?;

                            let batch =
                                RecordBatch::try_new(batch.schema(), columns).unwrap();

                            // bind timer so it drops w/ this iterator
                            let _ = &timer;

                            Ok((partition, batch))
                        });

                    Box::new(it)
                }
            };

        Ok(it)
    }

    // return the number of output partitions
    fn num_partitions(&self) -> usize {
        match self.state {
            BatchPartitionerState::RoundRobin { num_partitions, .. } => num_partitions,
            BatchPartitionerState::Hash { num_partitions, .. } => num_partitions,
        }
    }
}

/// The repartition operator maps N input partitions to M output partitions based on a
/// partitioning scheme. No guarantees are made about the order of the resulting partitions.
#[derive(Debug)]
pub struct SPRepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,

    /// SortExpr
    sort_expr: Vec<PhysicalSortExpr>,

    /// Partitioning scheme to use
    partitioning: Partitioning,

    /// Inner state that is initialized when the first output stream is created.
    state: Arc<Mutex<SPRepartitionExecState>>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone)]
struct SPRepartitionMetrics {
    /// Time in nanos to execute child operator and fetch batches
    fetch_time: metrics::Time,
    /// Time in nanos to perform repartitioning
    repart_time: metrics::Time,
    /// Time in nanos for sending resulting batches to channels
    send_time: metrics::Time,
}

impl SPRepartitionMetrics {
    pub fn new(
        output_partition: usize,
        input_partition: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let label = metrics::Label::new("inputPartition", input_partition.to_string());

        // Time in nanos to execute child operator and fetch batches
        let fetch_time = MetricBuilder::new(metrics)
            .with_label(label.clone())
            .subset_time("fetch_time", output_partition);

        // Time in nanos to perform repartitioning
        let repart_time = MetricBuilder::new(metrics)
            .with_label(label.clone())
            .subset_time("repart_time", output_partition);

        // Time in nanos for sending resulting batches to channels
        let send_time = MetricBuilder::new(metrics)
            .with_label(label)
            .subset_time("send_time", output_partition);

        Self {
            fetch_time,
            repart_time,
            send_time,
        }
    }
}

impl SPRepartitionExec {
    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Partitioning scheme to use
    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }
}

impl ExecutionPlan for SPRepartitionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SPRepartitionExec::try_new(
            self.sort_expr.clone(),
            children[0].clone(),
            self.partitioning.clone(),
        )?))
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        if self.maintains_input_order()[0] {
            self.input().output_ordering()
        } else {
            None
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // We preserve ordering when input partitioning is 1
        vec![self.input().output_partitioning().partition_count() <= 1]
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input.equivalence_properties()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start SPRepartitionExec::execute for partition: {}",
            partition
        );
        // lock mutexes
        let mut state = self.state.lock();

        let num_input_partitions = self.input.output_partitioning().partition_count();
        let num_output_partitions = self.partitioning.partition_count();

        // if this is the first partition to be invoked then we need to set up initial state
        if state.channels.is_empty() {
            // create one channel per *output* partition
            // note we use a custom channel that ensures there is always data for each receiver
            // but limits the amount of buffering if required.
            let (txs, rxs) = channels(num_output_partitions);
            for (partition, (tx, rx)) in txs.into_iter().zip(rxs).enumerate() {
                let reservation = Arc::new(Mutex::new(
                    MemoryConsumer::new(format!("SPRepartitionExec[{partition}]"))
                        .register(context.memory_pool()),
                ));
                state.channels.insert(partition, (tx, rx, reservation));
            }

            // launch one async task per *input* partition
            let mut join_handles = Vec::with_capacity(num_input_partitions);
            for i in 0..num_input_partitions {
                let txs: HashMap<_, _> = state
                    .channels
                    .iter()
                    .map(|(partition, (tx, _rx, reservation))| {
                        (*partition, (tx.clone(), Arc::clone(reservation)))
                    })
                    .collect();

                let r_metrics = SPRepartitionMetrics::new(i, partition, &self.metrics);

                let input_task: JoinHandle<Result<()>> =
                    tokio::spawn(Self::pull_from_input(
                        self.input.clone(),
                        i,
                        txs.clone(),
                        self.partitioning.clone(),
                        r_metrics,
                        context.clone(),
                    ));

                // In a separate task, wait for each input to be done
                // (and pass along any errors, including panic!s)
                let join_handle = tokio::spawn(Self::wait_for_task(
                    AbortOnDropSingle::new(input_task),
                    txs.into_iter()
                        .map(|(partition, (tx, _reservation))| (partition, tx))
                        .collect(),
                ));
                join_handles.push(join_handle);
            }

            state.abort_helper = Arc::new(AbortOnDropMany(join_handles))
        }

        trace!(
            "Before returning stream in SPRepartitionExec::execute for partition: {}",
            partition
        );

        // now return stream for the specified *output* partition which will
        // read from the channel
        let (_tx, rx, reservation) = state
            .channels
            .remove(&partition)
            .expect("partition not used yet");
        Ok(Box::pin(RepartitionStream {
            num_input_partitions,
            num_input_partitions_processed: 0,
            schema: self.input.schema(),
            input: rx,
            drop_helper: Arc::clone(&state.abort_helper),
            reservation,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "SPRepartitionExec: partitioning={:?}, input_partitions={}",
                    self.partitioning,
                    self.input.output_partitioning().partition_count()
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

impl SPRepartitionExec {
    /// Create a new SPRepartitionExec
    pub fn try_new(
        sort_expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Self> {
        Ok(SPRepartitionExec {
            sort_expr,
            input,
            partitioning,
            state: Arc::new(Mutex::new(SPRepartitionExecState {
                channels: HashMap::new(),
                abort_helper: Arc::new(AbortOnDropMany::<()>(vec![])),
            })),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Pulls data from the specified input plan, feeding it to the
    /// output partitions based on the desired partitioning
    ///
    /// i is the input partition index
    ///
    /// txs hold the output sending channels for each output partition
    async fn pull_from_input(
        input: Arc<dyn ExecutionPlan>,
        i: usize,
        mut txs: HashMap<
            usize,
            (DistributionSender<MaybeBatch>, SharedMemoryReservation),
        >,
        partitioning: Partitioning,
        r_metrics: SPRepartitionMetrics,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let mut partitioner =
            BatchPartitioner::try_new(partitioning, r_metrics.repart_time.clone())?;

        // execute the child operator
        let timer = r_metrics.fetch_time.timer();
        let mut stream = input.execute(i, context)?;
        timer.done();

        // While there are still outputs to send to, keep
        // pulling inputs
        let mut batches_until_yield = partitioner.num_partitions();
        while !txs.is_empty() {
            // fetch the next batch
            let timer = r_metrics.fetch_time.timer();
            let result = stream.next().await;
            timer.done();

            // Input is done
            let batch = match result {
                Some(result) => result?,
                None => break,
            };

            for res in partitioner.partition_iter(batch)? {
                let (partition, batch) = res?;
                let size = batch.get_array_memory_size();

                let timer = r_metrics.send_time.timer();
                // if there is still a receiver, send to it
                if let Some((tx, reservation)) = txs.get_mut(&partition) {
                    reservation.lock().try_grow(size)?;

                    if tx.send(Some(Ok(batch))).await.is_err() {
                        // If the other end has hung up, it was an early shutdown (e.g. LIMIT)
                        reservation.lock().shrink(size);
                        txs.remove(&partition);
                    }
                }
                timer.done();
            }

            // If the input stream is endless, we may spin forever and
            // never yield back to tokio.  See
            // https://github.com/apache/arrow-datafusion/issues/5278.
            //
            // However, yielding on every batch causes a bottleneck
            // when running with multiple cores. See
            // https://github.com/apache/arrow-datafusion/issues/6290
            //
            // Thus, heuristically yield after producing num_partition
            // batches
            //
            // In round robin this is ideal as each input will get a
            // new batch. In hash partitioning it may yield too often
            // on uneven distributions even if some partition can not
            // make progress, but parallelism is going to be limited
            // in that case anyways
            if batches_until_yield == 0 {
                tokio::task::yield_now().await;
                batches_until_yield = partitioner.num_partitions();
            } else {
                batches_until_yield -= 1;
            }
        }

        Ok(())
    }

    /// Waits for `input_task` which is consuming one of the inputs to
    /// complete. Upon each successful completion, sends a `None` to
    /// each of the output tx channels to signal one of the inputs is
    /// complete. Upon error, propagates the errors to all output tx
    /// channels.
    async fn wait_for_task(
        input_task: AbortOnDropSingle<Result<()>>,
        txs: HashMap<usize, DistributionSender<Option<Result<RecordBatch>>>>,
    ) {
        // wait for completion, and propagate error
        // note we ignore errors on send (.ok) as that means the receiver has already shutdown.
        match input_task.await {
            // Error in joining task
            Err(e) => {
                let e = Arc::new(e);

                for (_, tx) in txs {
                    let err = Err(DataFusionError::Context(
                        "Join Error".to_string(),
                        Box::new(DataFusionError::External(Box::new(Arc::clone(&e)))),
                    ));
                    tx.send(Some(err)).await.ok();
                }
            }
            // Error from running input task
            Ok(Err(e)) => {
                let e = Arc::new(e);

                for (_, tx) in txs {
                    // wrap it because need to send error to all output partitions
                    let err = Err(DataFusionError::External(Box::new(e.clone())));
                    tx.send(Some(err)).await.ok();
                }
            }
            // Input task completed successfully
            Ok(Ok(())) => {
                // notify each output partition that this input partition has no more data
                for (_, tx) in txs {
                    tx.send(None).await.ok();
                }
            }
        }
    }
}

struct RepartitionStream {
    /// Number of input partitions that will be sending batches to this output channel
    num_input_partitions: usize,

    /// Number of input partitions that have finished sending batches to this output channel
    num_input_partitions_processed: usize,

    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    input: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    #[allow(dead_code)]
    drop_helper: Arc<AbortOnDropMany<()>>,

    /// Memory reservation.
    reservation: SharedMemoryReservation,
}

impl Stream for RepartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.input.recv().poll_unpin(cx) {
                Poll::Ready(Some(Some(v))) => {
                    if let Ok(batch) = &v {
                        self.reservation
                            .lock()
                            .shrink(batch.get_array_memory_size());
                    }

                    return Poll::Ready(Some(v));
                }
                Poll::Ready(Some(None)) => {
                    self.num_input_partitions_processed += 1;

                    if self.num_input_partitions == self.num_input_partitions_processed {
                        // all input partitions have finished sending batches
                        return Poll::Ready(None);
                    } else {
                        // other partitions still have data to send
                        continue;
                    }
                }
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl RecordBatchStream for RepartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
