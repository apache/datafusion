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

//! [`AdaptiveHashJoinExec`], a hash join that picks its mode at runtime

use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::utils::memory::RecordBatchMemoryCounter;
use datafusion_common::{DataFusionError, Result, Statistics, internal_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;

use super::HashJoinExec;
use crate::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use crate::joins::PartitionMode;
use crate::joins::utils::OnceAsync;
use crate::metrics::MetricsSet;
use crate::repartition::RepartitionExec;
use crate::statistics::StatisticsArgs;
use crate::stream::RecordBatchStreamAdapter;
use crate::streaming::{PartitionStream, StreamingTableExec};
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_stream_partitioned, validate_child_count,
};

/// A hash join that picks its mode at runtime from the exact size of its
/// build side.
///
/// A [`HashJoinExec`] reads its whole build side before it probes anything,
/// so this adds no wait of its own. It wraps a `Partitioned` join whose probe
/// side is hash repartitioned, and keeps that join's properties and its dynamic
/// filter, which the optimizer has already pushed into the probe side. On first
/// execution it collects the build side, up to `max_build_rows` rows and
/// `max_build_bytes` bytes. If it all fits, it runs the wrapped join in
/// `CollectLeft` mode on the probe input as it is, without repartitioning it.
/// Otherwise it runs it in `Partitioned` mode, as planned: the rest of the
/// build side streams on behind the batches collected so far.
///
/// ```text
///        planned                       at runtime, small build side
///
///    HashJoinExec(Partitioned)          HashJoinExec(CollectLeft)
///      /                 \                /                \
/// Repartition       Repartition      collected          probe input
///      |                 |           build side
/// build input       probe input
/// ```
///
/// In `CollectLeft` mode the output is not hash partitioned. If an ancestor
/// relies on that (`required`), the output is repartitioned instead of the
/// probe input, which pays off when the join is selective.
///
/// Created by the `AdaptiveHashJoin` physical optimizer rule, see
/// `datafusion.optimizer.enable_adaptive_hash_join`.
#[derive(Debug)]
pub struct AdaptiveHashJoinExec {
    inputs: Arc<Inputs>,
    /// Runtime state, shared by all partitions
    state: Arc<State>,
}

/// What creating the join to run needs, kept apart from the runtime state
/// that holds the future creating it.
#[derive(Debug, Clone)]
struct Inputs {
    /// The join chosen by the planner. Its children stay the planned ones,
    /// including the hash repartitions that `build` and `probe` skip.
    join: Arc<HashJoinExec>,
    /// The build-side input, without the hash repartition the planner added,
    /// if any
    build: Arc<dyn ExecutionPlan>,
    /// The probe-side input, without the hash repartition the planner added
    probe: Arc<dyn ExecutionPlan>,
    max_build_rows: usize,
    max_build_bytes: usize,
    /// Whether an ancestor relies on the output being hash partitioned
    required: bool,
}

/// The runtime state of an [`AdaptiveHashJoinExec`].
#[derive(Debug, Default)]
struct State {
    /// The join to run, created on first execution
    join: OnceAsync<Prepared>,
    abandoned: Mutex<Abandoned>,
    decision: OnceLock<Decision>,
}

#[derive(Debug)]
struct Prepared {
    join: Arc<dyn ExecutionPlan>,
    decision: Decision,
}

#[derive(Debug, Clone, Copy)]
struct Decision {
    mode: PartitionMode,
    /// The number of build-side rows, if they were all collected
    build_rows: Option<usize>,
}

impl AdaptiveHashJoinExec {
    /// Returns the probe-side input of `join` below its hash repartition, if
    /// `join` can be wrapped: it runs in `Partitioned` mode, is not null
    /// aware, and hash repartitions its probe side.
    pub fn probe_input(join: &HashJoinExec) -> Option<&Arc<dyn ExecutionPlan>> {
        if *join.partition_mode() != PartitionMode::Partitioned || join.null_aware {
            return None;
        }
        hash_repartition_input(join.right())
    }

    /// Wraps `join`, which [`Self::probe_input`] must accept.
    pub fn try_new(
        join: &HashJoinExec,
        max_build_rows: usize,
        max_build_bytes: usize,
        required: bool,
    ) -> Result<Self> {
        let Some(probe) = Self::probe_input(join) else {
            return internal_err!(
                "AdaptiveHashJoinExec requires a partitioned hash join that hash repartitions its probe side"
            );
        };
        let build = hash_repartition_input(join.left()).unwrap_or_else(|| join.left());
        let inputs = Inputs {
            build: Arc::clone(build),
            probe: Arc::clone(probe),
            join: Arc::new(join.builder().build()?),
            max_build_rows,
            max_build_bytes,
            required,
        };
        Ok(Self::new(inputs))
    }

    fn new(inputs: Inputs) -> Self {
        Self {
            inputs: Arc::new(inputs),
            state: Default::default(),
        }
    }

    /// Whether the planner repartitioned the build side. If not, it relies
    /// on the build input being hash partitioned already, and so does this
    /// join in `Partitioned` mode.
    pub fn repartitions_build(&self) -> bool {
        hash_repartition_input(self.inputs.join.left()).is_some()
    }
}

/// Collects the build side, up to the thresholds, and creates the join to run.
async fn prepare(inputs: Arc<Inputs>, context: Arc<TaskContext>) -> Result<Prepared> {
    let collected = collect_up_to(
        &inputs.build,
        &context,
        inputs.max_build_rows,
        inputs.max_build_bytes,
    )
    .await?;
    let decision = Decision {
        // Collection stops at the first threshold the build side exceeds, so
        // it only completes when the build side fits in a single partition
        mode: if collected.complete {
            PartitionMode::CollectLeft
        } else {
            PartitionMode::Partitioned
        },
        build_rows: collected.complete.then_some(collected.rows),
    };

    let join = &inputs.join;
    let output_partitioning = &join.properties().partitioning;
    let output_partitions = output_partitioning.partition_count();
    let (build, probe) = if decision.mode == PartitionMode::CollectLeft {
        let build = collected.into_single_partition()?;
        let mut probe = Arc::clone(&inputs.probe);
        if !inputs.required
            && probe.output_partitioning().partition_count() != output_partitions
        {
            probe = Arc::new(RepartitionExec::try_new(
                probe,
                Partitioning::RoundRobinBatch(output_partitions),
            )?);
        }
        (build, probe)
    } else {
        let mut build = collected.into_partitions()?;
        if hash_repartition_input(join.left()).is_some() {
            let partitioning = join.left().output_partitioning().clone();
            build = Arc::new(RepartitionExec::try_new(build, partitioning)?);
        }
        let partitioning = join.right().output_partitioning().clone();
        let probe = RepartitionExec::try_new(Arc::clone(&inputs.probe), partitioning)?;
        (build, Arc::new(probe) as _)
    };

    let mut join: Arc<dyn ExecutionPlan> = Arc::new(
        join.builder()
            .with_new_children(vec![build, probe])?
            .recompute_properties()
            .with_partition_mode(decision.mode)
            .build()?,
    );
    if inputs.required && decision.mode == PartitionMode::CollectLeft {
        join = Arc::new(RepartitionExec::try_new(join, output_partitioning.clone())?);
    }
    Ok(Prepared { join, decision })
}

/// Returns the input of `plan` if it is a hash repartition.
fn hash_repartition_input(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<&Arc<dyn ExecutionPlan>> {
    let repartition = plan.downcast_ref::<RepartitionExec>()?;
    (matches!(repartition.partitioning(), Partitioning::Hash(..))
        && !repartition.preserve_order())
    .then(|| repartition.input())
}

/// Partitions whose stream was dropped before it started the join to run.
///
/// In `Partitioned` mode a hash join waits for every partition to report its
/// build side before it publishes its dynamic filter, and a partition whose
/// stream is dropped reports itself as canceled. A parent that skips probing,
/// for example because its own build side is empty, may never poll some of
/// our partitions, so their streams of the join to run are still created and
/// dropped, as soon as that join exists.
#[derive(Debug, Default)]
struct Abandoned {
    join: Option<Arc<dyn ExecutionPlan>>,
    partitions: Vec<(usize, Arc<TaskContext>)>,
}

impl State {
    /// Records that the join to run exists, and cancels the partitions
    /// abandoned before that.
    fn join_created(&self, join: &Arc<dyn ExecutionPlan>) {
        let partitions = {
            let mut abandoned = self.abandoned.lock();
            abandoned.join.get_or_insert_with(|| Arc::clone(join));
            std::mem::take(&mut abandoned.partitions)
        };
        for (partition, context) in partitions {
            drop(join.execute(partition, context));
        }
    }

    fn abandon(&self, partition: usize, context: Arc<TaskContext>) {
        let join = {
            let mut abandoned = self.abandoned.lock();
            match &abandoned.join {
                Some(join) => Arc::clone(join),
                None => return abandoned.partitions.push((partition, context)),
            }
        };
        drop(join.execute(partition, context));
    }
}

/// Cancels its partition of the join to run if dropped before `started`.
struct PartitionGuard {
    state: Arc<State>,
    partition: usize,
    context: Arc<TaskContext>,
    started: bool,
}

impl Drop for PartitionGuard {
    fn drop(&mut self) {
        if !self.started {
            self.state
                .abandon(self.partition, Arc::clone(&self.context));
        }
    }
}

/// A build-side batch, and the memory first counted for it.
type CountedBatch = (RecordBatch, usize);

/// The build side collected so far.
struct Collected {
    schema: SchemaRef,
    /// The batches of each partition, and the rest of its stream if it did
    /// not finish
    partitions: Vec<(Vec<CountedBatch>, Option<SendableRecordBatchStream>)>,
    /// Accounts for all the batches
    reservation: MemoryReservation,
    rows: usize,
    /// Whether all partitions finished within the thresholds
    complete: bool,
}

impl Collected {
    /// Hands the batches over as a single partition. Only for a complete
    /// build side, which has no rest of any stream.
    fn into_single_partition(self) -> Result<Arc<dyn ExecutionPlan>> {
        let batches = self.partitions.into_iter().flat_map(|(batches, rest)| {
            debug_assert!(rest.is_none(), "build side not collected completely");
            batches
        });
        let partition = CollectedPartition::new(
            Arc::clone(&self.schema),
            batches.collect(),
            None,
            self.reservation,
        );
        build_exec(self.schema, vec![Arc::new(partition)])
    }

    /// Hands the batches of each partition over, followed by the rest of its
    /// stream.
    fn into_partitions(self) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = self
            .partitions
            .into_iter()
            .map(|(batches, rest)| {
                let size = batches.iter().map(|(_, size)| size).sum();
                let reservation = self.reservation.split(size);
                let partition = CollectedPartition::new(
                    Arc::clone(&self.schema),
                    batches,
                    rest,
                    reservation,
                );
                Arc::new(partition) as _
            })
            .collect();
        build_exec(self.schema, partitions)
    }
}

fn build_exec(
    schema: SchemaRef,
    partitions: Vec<Arc<dyn PartitionStream>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let exec = StreamingTableExec::try_new(schema, partitions, None, [], false, None)?;
    Ok(Arc::new(exec))
}

/// Counts what has been collected across all partitions.
struct Counter {
    memory: RecordBatchMemoryCounter,
    rows: usize,
    /// Set once collection must stop
    full: bool,
}

/// Runs all partitions of `plan` in parallel until they finish, or have
/// produced more than `max_rows` rows or `max_bytes` bytes in total, or the
/// memory pool cannot hold more.
async fn collect_up_to(
    plan: &Arc<dyn ExecutionPlan>,
    context: &Arc<TaskContext>,
    max_rows: usize,
    max_bytes: usize,
) -> Result<Collected> {
    let reservation = Arc::new(
        MemoryConsumer::new("AdaptiveHashJoinExec").register(context.memory_pool()),
    );
    let counter = Arc::new(Mutex::new(Counter {
        memory: RecordBatchMemoryCounter::new(),
        rows: 0,
        full: false,
    }));
    let streams = execute_stream_partitioned(Arc::clone(plan), Arc::clone(context))?;
    let tasks: Vec<_> = streams
        .into_iter()
        .map(|mut stream| {
            let reservation = Arc::clone(&reservation);
            let counter = Arc::clone(&counter);
            SpawnedTask::spawn(async move {
                let mut batches = vec![];
                while !counter.lock().full {
                    let Some(batch) = stream.next().await.transpose()? else {
                        return Ok::<_, DataFusionError>((batches, None));
                    };
                    let mut counter = counter.lock();
                    let mut size = counter.memory.count_batch(&batch);
                    counter.rows += batch.num_rows();
                    if reservation.try_grow(size).is_err() {
                        size = 0;
                        counter.full = true;
                    }
                    if counter.rows > max_rows
                        || counter.memory.memory_usage() > max_bytes
                    {
                        counter.full = true;
                    }
                    drop(counter);
                    batches.push((batch, size));
                }
                Ok((batches, Some(stream)))
            })
        })
        .collect();
    let mut partitions = Vec::with_capacity(tasks.len());
    for task in tasks {
        partitions.push(
            task.join_unwind()
                .await
                .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??,
        );
    }
    let Ok(reservation) = Arc::try_unwrap(reservation) else {
        return internal_err!("build-side reservation is still shared");
    };
    let rows = counter.lock().rows;
    Ok(Collected {
        schema: plan.schema(),
        complete: partitions.iter().all(|(_, rest)| rest.is_none()),
        partitions,
        reservation,
        rows,
    })
}

/// One partition of a collected build side: its batches, then the rest of its
/// stream. It hands them over once, and releases each batch, and its memory,
/// as soon as the join takes it.
struct CollectedPartition {
    schema: SchemaRef,
    contents: Mutex<Option<CollectedContents>>,
}

type CollectedContents = (
    Vec<CountedBatch>,
    Option<SendableRecordBatchStream>,
    MemoryReservation,
);

impl CollectedPartition {
    fn new(
        schema: SchemaRef,
        batches: Vec<CountedBatch>,
        rest: Option<SendableRecordBatchStream>,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            schema,
            contents: Mutex::new(Some((batches, rest, reservation))),
        }
    }
}

impl fmt::Debug for CollectedPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectedPartition").finish_non_exhaustive()
    }
}

impl PartitionStream for CollectedPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let Some((batches, rest, reservation)) = self.contents.lock().take() else {
            let error = internal_err!("collected build-side partition executed twice");
            return Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::once(async { error }),
            ));
        };
        let batches = futures::stream::iter(batches).map(move |(batch, size)| {
            reservation.shrink(size);
            Ok(batch)
        });
        match rest {
            None => Box::pin(RecordBatchStreamAdapter::new(schema, batches)),
            Some(rest) => {
                Box::pin(RecordBatchStreamAdapter::new(schema, batches.chain(rest)))
            }
        }
    }
}

impl DisplayAs for AdaptiveHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let inputs = &self.inputs;
        let decision = self.state.decision.get();
        let mode =
            decision.map_or_else(|| "pending".to_string(), |d| format!("{:?}", d.mode));
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AdaptiveHashJoinExec: mode={mode}")?;
                if let Some(rows) = decision.and_then(|d| d.build_rows) {
                    write!(f, ", build_rows={rows}")?;
                }
                if inputs.required
                    && decision.is_some_and(|d| d.mode == PartitionMode::CollectLeft)
                {
                    write!(f, ", output_repartitioned=true")?;
                }
                let on = inputs
                    .join
                    .on()
                    .iter()
                    .map(|(l, r)| format!("({l}, {r})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    ", join_type={:?}, on=[{on}], max_build_rows={}",
                    inputs.join.join_type(),
                    inputs.max_build_rows
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "mode={mode}")?;
                inputs.join.fmt_as(t, f)
            }
        }
    }
}

impl ExecutionPlan for AdaptiveHashJoinExec {
    fn name(&self) -> &'static str {
        "AdaptiveHashJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inputs.join.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inputs.build, &self.inputs.probe]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inputs.join.apply_expressions(f)
    }

    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.inputs.join.dynamic_expressions_produced()
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        let probe = children.pop().unwrap();
        let build = children.pop().unwrap();
        Ok(Arc::new(Self::new(Inputs {
            build,
            probe,
            ..Inputs::clone(&self.inputs)
        })))
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

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let join = Arc::new(self.inputs.join.builder().reset_state().build()?);
        Ok(Arc::new(Self::new(Inputs {
            join,
            ..Inputs::clone(&self.inputs)
        })))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut prepared = self
            .state
            .join
            .try_once(|| Ok(prepare(Arc::clone(&self.inputs), Arc::clone(&context))))?;
        let guard = PartitionGuard {
            state: Arc::clone(&self.state),
            partition,
            context: Arc::clone(&context),
            started: false,
        };
        let stream = futures::stream::once(async move {
            // Move the whole guard in, not just the field set below
            let mut guard = guard;
            let prepared = std::future::poll_fn(|cx| prepared.get_shared(cx)).await?;
            let _ = guard.state.decision.set(prepared.decision);
            guard.state.join_created(&prepared.join);
            guard.started = true;
            prepared.join.execute(partition, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inputs.join.metrics()
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}
