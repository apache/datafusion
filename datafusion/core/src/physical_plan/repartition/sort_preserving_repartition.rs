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
//! partitioning scheme. If its input is ordered, ordering is preserved during repartitioning

use std::sync::Arc;
use std::{any::Any, vec};

use crate::physical_plan::repartition::distributor_channels::{
    channels, DistributionSender,
};
use crate::physical_plan::{
    DisplayFormatType, EquivalenceProperties, ExecutionPlan, Partitioning, Statistics,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryConsumer;
use log::trace;

use super::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use super::SendableRecordBatchStream;

use crate::physical_plan::common::{
    AbortOnDropMany, AbortOnDropSingle, SharedMemoryReservation,
};
use crate::physical_plan::metrics::BaselineMetrics;
use crate::physical_plan::repartition::{
    BatchPartitioner, RepartitionExecState, RepartitionMetrics, RepartitionStream,
};
use crate::physical_plan::sorts::streaming_merge;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalSortExpr;
use futures::StreamExt;
use hashbrown::HashMap;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

type MaybeBatch = Option<Result<RecordBatch>>;

/// The repartition operator maps N input partitions to M output partitions based on a
/// partitioning scheme. If input is ordered, output is also ordered
#[derive(Debug)]
pub struct SortPreservingRepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,

    /// SortExpr
    sort_expr: Vec<PhysicalSortExpr>,

    /// Partitioning scheme to use
    partitioning: Partitioning,

    /// Inner state that is initialized when the first output stream is created.
    state: Arc<Mutex<RepartitionExecState>>,

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

            let txs: HashMap<_, _> = state
                .channels
                .iter()
                .map(|(partition, (tx, _rx, reservation))| {
                    (*partition, (tx.clone(), Arc::clone(reservation)))
                })
                .collect();

            let r_metrics = RepartitionMetrics::new(partition, partition, &self.metrics);

            let input_task: JoinHandle<Result<()>> =
                tokio::spawn(Self::pull_from_inputs(
                    self.input.clone(),
                    txs.clone(),
                    self.partitioning.clone(),
                    r_metrics,
                    context,
                    self.sort_expr.clone(),
                    partition,
                    self.schema(),
                    self.metrics.clone(),
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

            state.abort_helper = Arc::new(AbortOnDropMany(join_handles))
        }

        trace!(
            "Before returning stream in SortPreservingRepartitionExec::execute for partition: {}",
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
    /// Create a new `SortPreservingRepartitionExec`
    pub fn try_new(
        sort_expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Self> {
        Ok(SortPreservingRepartitionExec {
            sort_expr,
            input,
            partitioning,
            state: Arc::new(Mutex::new(RepartitionExecState {
                channels: HashMap::new(),
                abort_helper: Arc::new(AbortOnDropMany::<()>(vec![])),
            })),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Pulls data from the specified input plan (all partitions of the input),
    /// feeds sort preserved merge stream to the
    /// output partitions based on the desired partitioning
    ///
    /// txs hold the output sending channels for each output partition
    #[allow(clippy::too_many_arguments)]
    async fn pull_from_inputs(
        input: Arc<dyn ExecutionPlan>,
        mut txs: HashMap<
            usize,
            (DistributionSender<MaybeBatch>, SharedMemoryReservation),
        >,
        partitioning: Partitioning,
        r_metrics: RepartitionMetrics,
        context: Arc<TaskContext>,
        sort_exprs: Vec<PhysicalSortExpr>,
        partition: usize,
        schema: SchemaRef,
        metrics: ExecutionPlanMetricsSet,
    ) -> Result<()> {
        let mut partitioner =
            BatchPartitioner::try_new(partitioning, r_metrics.repart_time.clone())?;

        // execute the child operator
        let timer = r_metrics.fetch_time.timer();
        let mut input_streams = vec![];
        for i in 0..input.output_partitioning().partition_count() {
            let stream = input.execute(i, context.clone())?;
            input_streams.push(stream);
        }
        let mut stream = streaming_merge(
            input_streams,
            schema,
            &sort_exprs,
            BaselineMetrics::new(&metrics, partition),
            context.session_config().batch_size(),
        )?;
        // let mut stream = input.execute(i, context)?;
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
