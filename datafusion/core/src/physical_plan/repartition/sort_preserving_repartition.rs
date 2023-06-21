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

//! This file implements an order-preserving repartitioning operator
//! mapping N input partitions to M output partitions.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, vec};

use super::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use super::SendableRecordBatchStream;

use crate::physical_plan::common::{
    transpose, AbortOnDropMany, AbortOnDropSingle, SharedMemoryReservation,
};
use crate::physical_plan::metrics::BaselineMetrics;
use crate::physical_plan::repartition::distributor_channels::{
    partition_aware_channels, DistributionReceiver, DistributionSender,
};
use crate::physical_plan::repartition::{
    pull_from_input, wait_for_task, MaybeBatch, RepartitionMetrics,
};
use crate::physical_plan::sorts::streaming_merge;
use crate::physical_plan::{
    DisplayFormatType, EquivalenceProperties, ExecutionPlan, Partitioning,
    RecordBatchStream, Statistics,
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalSortExpr;

use futures::{FutureExt, Stream};
use hashbrown::HashMap;
use log::trace;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

type InputPartitionsToCurrentPartitionSender = Vec<DistributionSender<MaybeBatch>>;
type InputPartitionsToCurrentPartitionReceiver = Vec<DistributionReceiver<MaybeBatch>>;

/// Inner state of [`SortPreservingRepartitionExec`].
#[derive(Debug)]
struct SortPreservingRepartitionExecState {
    /// Channels for sending batches from input partitions to output partitions.
    /// Key is the partition number.
    channels: HashMap<
        usize,
        (
            InputPartitionsToCurrentPartitionSender,
            InputPartitionsToCurrentPartitionReceiver,
            SharedMemoryReservation,
        ),
    >,

    /// Helper that ensures that that background job is killed once it is no longer needed.
    abort_helper: Arc<AbortOnDropMany<()>>,
}

/// The repartition operator maps N input partitions to M output partitions based on a
/// partitioning scheme. Any input ordering is preserved.
#[derive(Debug)]
pub struct SortPreservingRepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,

    /// Partitioning scheme to use
    partitioning: Partitioning,

    /// Inner state that is initialized when the first output stream is created.
    state: Arc<Mutex<SortPreservingRepartitionExecState>>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl SortPreservingRepartitionExec {
    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Partitioning scheme to use
    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }
}

impl ExecutionPlan for SortPreservingRepartitionExec {
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
        Ok(Arc::new(SortPreservingRepartitionExec::try_new(
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
        // Indicate that input ordering is preserved:
        self.input().output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
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
            "Start SortPreservingRepartitionExec::execute for partition: {}",
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
            let (txs, rxs) =
                partition_aware_channels(num_input_partitions, num_output_partitions);
            // Take transpose of senders and receivers. `state.channels` keeps track of entries per output partition
            let txs = transpose(txs);
            let rxs = transpose(rxs);
            for (partition, (tx, rx)) in txs.into_iter().zip(rxs).enumerate() {
                let reservation = Arc::new(Mutex::new(
                    MemoryConsumer::new(format!(
                        "SortPreservingRepartitionExec[{partition}]"
                    ))
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
                        (*partition, (tx[i].clone(), Arc::clone(reservation)))
                    })
                    .collect();
                let r_metrics = RepartitionMetrics::new(i, partition, &self.metrics);

                let input_task: JoinHandle<Result<()>> = tokio::spawn(pull_from_input(
                    self.input.clone(),
                    i,
                    txs.clone(),
                    self.partitioning.clone(),
                    r_metrics,
                    context.clone(),
                ));

                // In a separate task, wait for each input to be done
                // (and pass along any errors, including panic!s)
                let join_handle = tokio::spawn(wait_for_task(
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
            "Before returning stream in SortPreservingRepartitionExec::execute for partition: {}",
            partition
        );

        // now return stream for the specified *output* partition which will
        // read from the channels
        let (_tx, rx, reservation) = state
            .channels
            .remove(&partition)
            .expect("partition not used yet");

        // Store streams from all the input partitions:
        let input_streams = rx
            .into_iter()
            .map(|receiver| {
                Box::pin(PerPartitionStream {
                    schema: self.schema(),
                    receiver,
                    drop_helper: Arc::clone(&state.abort_helper),
                    reservation: reservation.clone(),
                }) as SendableRecordBatchStream
            })
            .collect::<Vec<_>>();
        // Note that receiver size (`rx.len()`) and `num_input_partitions` are same.

        // Get existing ordering:
        let sort_exprs = self.input.output_ordering().unwrap_or(&[]);
        // Merge streams (while preserving ordering) coming from input partitions to this partition:
        streaming_merge(
            input_streams,
            self.schema(),
            sort_exprs,
            BaselineMetrics::new(&self.metrics, partition),
            context.session_config().batch_size(),
        )
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
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SortPreservingRepartitionExec: partitioning={:?}, input_partitions={}",
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

impl SortPreservingRepartitionExec {
    /// Create a new `SortPreservingRepartitionExec`.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Self> {
        Ok(SortPreservingRepartitionExec {
            input,
            partitioning,
            state: Arc::new(Mutex::new(SortPreservingRepartitionExecState {
                channels: HashMap::new(),
                abort_helper: Arc::new(AbortOnDropMany::<()>(vec![])),
            })),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// This struct converts a receiver to a stream.
/// Receiver receives data on an SPSC channel.
struct PerPartitionStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    receiver: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    #[allow(dead_code)]
    drop_helper: Arc<AbortOnDropMany<()>>,

    /// Memory reservation.
    reservation: SharedMemoryReservation,
}

impl Stream for PerPartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.receiver.recv().poll_unpin(cx) {
            Poll::Ready(Some(Some(v))) => {
                if let Ok(batch) = &v {
                    self.reservation
                        .lock()
                        .shrink(batch.get_array_memory_size());
                }
                Poll::Ready(Some(v))
            }
            Poll::Ready(Some(None)) => {
                // Input partition has finished sending batches
                Poll::Ready(None)
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for PerPartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
