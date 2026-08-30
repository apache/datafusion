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

//! [`ReuseExec`]: execute a subplan once and distribute it to several consumers.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::execution_plan::{CardinalityEffect, EvaluationType, SchedulingType};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties, RecordBatchStream, ReplaceChildrenOptions,
    SendableRecordBatchStream, Statistics, validate_child_count,
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::PhysicalExpr;

use futures::{Stream, StreamExt};
use parking_lot::Mutex;

/// Executes its input **once** and distributes the result to every consumer
/// that shares this operator.
///
/// A plan is a tree, so a subplan appearing in two places is executed twice.
/// When the same `Arc<ReuseExec>` is installed at both places, the first
/// consumer to call [`ExecutionPlan::execute`] starts the input, and each batch
/// it produces is handed to every consumer.
///
/// # Retention
///
/// Batches are not cached wholesale. Each batch is released as soon as all
/// consumers have read it, so consumers that keep pace with each other cost
/// roughly one batch of retention apiece.
///
/// A consumer that attaches late is the case that costs memory: everything
/// produced before it attaches must be held for it. That is what happens under
/// [`ScalarSubqueryExec`], which runs the subquery to completion before
/// executing the main input, so the whole subplan output is retained. Bounding
/// the buffer instead would deadlock there — the producer would block on a
/// consumer that cannot start until the producer has finished.
///
/// # Sharing
///
/// Sharing is by `Arc` identity: two separately constructed `ReuseExec`s over
/// equal inputs share nothing. Rewriting a plan through
/// [`ExecutionPlan::with_new_children`] rebuilds the operator and drops the
/// sharing — the result stays correct, it just recomputes.
///
/// [`ScalarSubqueryExec`]: crate::scalar_subquery::ScalarSubqueryExec
#[derive(Debug)]
pub struct ReuseExec {
    /// The subplan to execute once.
    input: Arc<dyn ExecutionPlan>,
    /// How many plan sites share this operator. Used to know when a batch has
    /// been seen by everyone and can be dropped.
    consumers: usize,
    /// Created by whichever consumer executes first.
    state: Mutex<Option<Arc<ReuseState>>>,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ReuseExec {
    /// Create a [`ReuseExec`] over `input` shared by `consumers` plan sites.
    pub fn new(input: Arc<dyn ExecutionPlan>, consumers: usize) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            consumers,
            state: Mutex::new(None),
            cache: Arc::new(cache),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The subplan being reused.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Number of plan sites sharing this operator.
    pub fn consumers(&self) -> usize {
        self.consumers
    }

    /// Partitioning, ordering and emission all pass through: batches are
    /// forwarded as they are produced, in order, per partition.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        )
        .with_evaluation_type(EvaluationType::Eager)
        .with_scheduling_type(SchedulingType::Cooperative)
    }

    /// Start the input on first use; later callers join the running execution.
    fn shared_state(&self, context: &Arc<TaskContext>) -> Result<Arc<ReuseState>> {
        let mut guard = self.state.lock();
        if let Some(state) = guard.as_ref() {
            return Ok(Arc::clone(state));
        }

        let partition_count = self.input.output_partitioning().partition_count();
        let mut logs = Vec::with_capacity(partition_count);
        let mut tasks = Vec::with_capacity(partition_count);

        for partition in 0..partition_count {
            let reservation =
                MemoryConsumer::new(format!("ReuseExec[{partition}]"))
                    .register(context.memory_pool());
            let log = Arc::new(PartitionLog::new(self.consumers, reservation));
            let stream = self.input.execute(partition, Arc::clone(context))?;
            tasks.push(SpawnedTask::spawn(pull_from_input(
                Arc::clone(&log),
                stream,
            )));
            logs.push(log);
        }

        let state = Arc::new(ReuseState {
            logs,
            _tasks: tasks,
        });
        *guard = Some(Arc::clone(&state));
        Ok(state)
    }
}

/// The running execution of the input, shared by all consumers.
#[derive(Debug)]
struct ReuseState {
    logs: Vec<Arc<PartitionLog>>,
    /// Producer tasks; aborted when the last consumer drops the state.
    _tasks: Vec<SpawnedTask<()>>,
}

/// An append-only log of one partition's batches, read concurrently by every
/// consumer at its own pace.
#[derive(Debug)]
struct PartitionLog {
    inner: Mutex<LogState>,
    reservation: MemoryReservation,
}

#[derive(Debug)]
struct LogState {
    /// Produced batches. A slot becomes `None` once every consumer has read it.
    batches: Vec<Option<RecordBatch>>,
    /// How many consumers have yet to read each slot.
    unread: Vec<usize>,
    /// Consumers still reading. New batches start with this many readers.
    live: usize,
    finished: bool,
    error: Option<Arc<DataFusionError>>,
    /// Consumers parked waiting for the producer.
    wakers: Vec<Waker>,
}

impl PartitionLog {
    fn new(consumers: usize, reservation: MemoryReservation) -> Self {
        Self {
            inner: Mutex::new(LogState {
                batches: Vec::new(),
                unread: Vec::new(),
                live: consumers,
                finished: false,
                error: None,
                wakers: Vec::new(),
            }),
            reservation,
        }
    }

    /// Append a batch for all live consumers. Returns `Err` if the buffer could
    /// not be accounted for, which stops the producer.
    fn push(&self, batch: RecordBatch) -> Result<()> {
        let size = batch.get_array_memory_size();
        if let Err(e) = self.reservation.try_grow(size) {
            self.fail(e);
            return internal_err!("ReuseExec: memory reservation failed");
        }
        let mut state = self.inner.lock();
        let live = state.live;
        state.batches.push(Some(batch));
        state.unread.push(live);
        // Nobody left to read it; release straight away.
        if live == 0 {
            let last = state.batches.len() - 1;
            state.batches[last] = None;
            self.reservation.shrink(size);
        }
        state.wake_all();
        Ok(())
    }

    fn fail(&self, error: DataFusionError) {
        let mut state = self.inner.lock();
        if state.error.is_none() {
            state.error = Some(Arc::new(error));
        }
        state.finished = true;
        state.wake_all();
    }

    fn finish(&self) {
        let mut state = self.inner.lock();
        state.finished = true;
        state.wake_all();
    }
}

impl LogState {
    fn wake_all(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// Drive one input partition into its log.
async fn pull_from_input(log: Arc<PartitionLog>, mut stream: SendableRecordBatchStream) {
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => {
                if log.push(batch).is_err() {
                    return;
                }
            }
            Err(e) => {
                log.fail(e);
                return;
            }
        }
    }
    log.finish();
}

/// One consumer's view of a partition log.
struct ReuseStream {
    log: Arc<PartitionLog>,
    /// Keeps the producer tasks alive while any consumer is reading.
    _state: Arc<ReuseState>,
    schema: SchemaRef,
    cursor: usize,
    done: bool,
}

impl Stream for ReuseStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        let mut state = self.log.inner.lock();

        if let Some(error) = &state.error {
            let error = Arc::clone(error);
            drop(state);
            self.done = true;
            return Poll::Ready(Some(Err(DataFusionError::Shared(error))));
        }

        if self.cursor < state.batches.len() {
            let index = self.cursor;
            let batch = state.batches[index]
                .clone()
                .expect("batch released while a consumer still needed it");
            state.unread[index] = state.unread[index].saturating_sub(1);
            if state.unread[index] == 0 {
                state.batches[index] = None;
                self.log.reservation.shrink(batch.get_array_memory_size());
            }
            drop(state);
            self.cursor += 1;
            return Poll::Ready(Some(Ok(batch)));
        }

        if state.finished {
            drop(state);
            self.done = true;
            return Poll::Ready(None);
        }

        state.wakers.push(cx.waker().clone());
        Poll::Ready(Some(Ok(RecordBatch::new_empty(Arc::clone(&self.schema)))))
    }
}

impl RecordBatchStream for ReuseStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for ReuseStream {
    fn drop(&mut self) {
        // Give up this consumer's claim so retained batches can be released
        // even when the stream is abandoned early (a LIMIT upstream, say).
        let mut state = self.log.inner.lock();
        state.live = state.live.saturating_sub(1);
        let mut freed = 0;
        for index in self.cursor..state.batches.len() {
            state.unread[index] = state.unread[index].saturating_sub(1);
            if state.unread[index] == 0 {
                if let Some(batch) = state.batches[index].take() {
                    freed += batch.get_array_memory_size();
                }
            }
        }
        drop(state);
        if freed > 0 {
            self.log.reservation.shrink(freed);
        }
    }
}

impl DisplayAs for ReuseExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ReuseExec: consumers={}", self.consumers)
            }
            DisplayFormatType::TreeRender => write!(f, "ReuseExec"),
        }
    }
}

impl ExecutionPlan for ReuseExec {
    fn name(&self) -> &'static str {
        "ReuseExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        let input = children.swap_remove(0);
        // A rebuilt operator starts a fresh execution, so the sharing the
        // optimizer established is lost here. That costs a recomputation, not
        // correctness.
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                input,
                consumers: self.consumers,
                state: Mutex::new(None),
                cache: Arc::clone(&self.cache),
                metrics: ExecutionPlanMetricsSet::new(),
            })),
            ChildrenPropertiesMode::Recompute => {
                Ok(Arc::new(Self::new(input, self.consumers)))
            }
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partition_count = self.input.output_partitioning().partition_count();
        if partition >= partition_count {
            return internal_err!(
                "ReuseExec invalid partition {partition} (expected less than {partition_count})"
            );
        }

        let state = self.shared_state(&context)?;
        let log = Arc::clone(&state.logs[partition]);
        Ok(Box::pin(ReuseStream {
            log,
            _state: state,
            schema: self.schema(),
            cursor: 0,
            done: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    /// Distributing changes when rows appear, not which rows or how many.
    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[0]))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}
