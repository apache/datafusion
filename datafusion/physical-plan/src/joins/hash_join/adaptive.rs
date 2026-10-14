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

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayData, RecordBatch};
use arrow::datatypes::SchemaRef;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, Statistics, internal_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;

use super::HashJoinExec;
use crate::execution_plan::{
    Boundedness, ChildrenPropertiesMode, EmissionType, ReplaceChildrenOptions,
};
use crate::joins::PartitionMode;
use crate::joins::utils::{OnceAsync, OnceFut};
use crate::memory::MemoryStream;
use crate::metrics::MetricsSet;
use crate::repartition::RepartitionExec;
use crate::statistics::StatisticsArgs;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
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
    /// The join chosen by the planner
    join: Arc<HashJoinExec>,
    /// The build-side input, without the hash repartition the planner added
    build: Arc<dyn ExecutionPlan>,
    /// The partitioning of that repartition, if there was one
    build_partitioning: Option<Partitioning>,
    /// The probe-side input, without the hash repartition the planner added
    probe: Arc<dyn ExecutionPlan>,
    probe_partitioning: Partitioning,
    max_build_rows: usize,
    max_build_bytes: usize,
    /// Whether an ancestor relies on the output being hash partitioned
    required: bool,
    /// Runtime state, shared by all partitions
    state: Arc<State>,
}

/// The runtime state of an [`AdaptiveHashJoinExec`].
#[derive(Debug, Default)]
struct State {
    /// The join to run, created on first execution
    join: OnceAsync<Arc<dyn ExecutionPlan>>,
    abandoned: Mutex<Abandoned>,
    decision: OnceLock<Decision>,
}

#[derive(Debug)]
struct Decision {
    mode: PartitionMode,
    /// The number of build-side rows, if they were all collected
    build_rows: Option<usize>,
    output_repartitioned: bool,
}

impl AdaptiveHashJoinExec {
    /// Wraps `join`, which must run in `Partitioned` mode and hash repartition
    /// its probe side.
    pub fn try_new(
        join: &HashJoinExec,
        max_build_rows: usize,
        max_build_bytes: usize,
        required: bool,
    ) -> Result<Self> {
        if *join.partition_mode() != PartitionMode::Partitioned {
            return internal_err!("AdaptiveHashJoinExec requires a partitioned join");
        }
        let Some(probe) = hash_repartition_input(join.right()) else {
            return internal_err!(
                "AdaptiveHashJoinExec requires a hash repartitioned probe side"
            );
        };
        let (build, build_partitioning) = match hash_repartition_input(join.left()) {
            Some(input) => (input, Some(join.left().output_partitioning().clone())),
            None => (join.left(), None),
        };
        Ok(Self {
            build: Arc::clone(build),
            build_partitioning,
            probe: Arc::clone(probe),
            probe_partitioning: join.right().output_partitioning().clone(),
            join: Arc::new(join.builder().build()?),
            max_build_rows,
            max_build_bytes,
            required,
            state: Default::default(),
        })
    }

    /// The wrapped join
    pub fn join(&self) -> &HashJoinExec {
        &self.join
    }

    /// Whether the planner repartitioned the build side. If not, it relies
    /// on the build input being hash partitioned already, and so does this
    /// join in `Partitioned` mode.
    pub fn repartitions_build(&self) -> bool {
        self.build_partitioning.is_some()
    }

    /// Collects the build side, up to the thresholds, and creates the plan to
    /// run.
    async fn prepare(
        self: Arc<Self>,
        context: Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let collected = collect_up_to(&self.build, &context, self.max_build_rows).await?;
        let rows: usize = collected
            .partitions
            .iter()
            .flat_map(|(batches, _)| batches)
            .map(RecordBatch::num_rows)
            .sum();
        let complete = collected.partitions.iter().all(|(_, rest)| rest.is_none());
        let broadcast = complete
            && rows <= self.max_build_rows
            && memory_size(collected.partitions.iter().flat_map(|(batches, _)| batches))
                <= self.max_build_bytes;
        // The join to run accounts for its own memory
        drop(collected.reservation);

        let schema = self.build.schema();
        let output_partitioning = &self.join.properties().partitioning;
        let output_partitions = output_partitioning.partition_count();
        let (mode, build, probe) = if broadcast {
            let batches = collected
                .partitions
                .into_iter()
                .flat_map(|(batches, _)| batches)
                .collect();
            let properties = PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            let build =
                CollectedBuildExec::new(schema, vec![(batches, None)], properties);
            let mut probe = Arc::clone(&self.probe);
            if !self.required
                && probe.output_partitioning().partition_count() != output_partitions
            {
                probe = Arc::new(RepartitionExec::try_new(
                    probe,
                    Partitioning::RoundRobinBatch(output_partitions),
                )?);
            }
            (PartitionMode::CollectLeft, Arc::new(build) as _, probe)
        } else {
            let properties = PlanProperties::clone(self.build.properties());
            let build = CollectedBuildExec::new(schema, collected.partitions, properties);
            let build: Arc<dyn ExecutionPlan> = match &self.build_partitioning {
                Some(partitioning) => Arc::new(RepartitionExec::try_new(
                    Arc::new(build),
                    partitioning.clone(),
                )?),
                None => Arc::new(build),
            };
            let probe = RepartitionExec::try_new(
                Arc::clone(&self.probe),
                self.probe_partitioning.clone(),
            )?;
            (PartitionMode::Partitioned, build, Arc::new(probe) as _)
        };

        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(
            self.join
                .builder()
                .with_new_children(vec![build, probe])?
                .recompute_properties()
                .with_partition_mode(mode)
                .build()?,
        );
        let output_repartitioned = self.required && mode == PartitionMode::CollectLeft;
        if output_repartitioned {
            plan = Arc::new(RepartitionExec::try_new(plan, output_partitioning.clone())?);
        }
        let _ = self.state.decision.set(Decision {
            mode,
            build_rows: complete.then_some(rows),
            output_repartitioned,
        });
        Ok(plan)
    }

    fn with_children(
        &self,
        build: Arc<dyn ExecutionPlan>,
        probe: Arc<dyn ExecutionPlan>,
        join: Arc<HashJoinExec>,
    ) -> Self {
        Self {
            join,
            build,
            build_partitioning: self.build_partitioning.clone(),
            probe,
            probe_partitioning: self.probe_partitioning.clone(),
            max_build_rows: self.max_build_rows,
            max_build_bytes: self.max_build_bytes,
            required: self.required,
            state: Default::default(),
        }
    }
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

impl Abandoned {
    /// Records that the join to run exists, and cancels the partitions
    /// abandoned before that.
    fn set_join(&mut self, join: &Arc<dyn ExecutionPlan>) {
        if self.join.is_none() {
            self.join = Some(Arc::clone(join));
        }
        for (partition, context) in self.partitions.drain(..) {
            drop(join.execute(partition, context));
        }
    }

    fn abandon(&mut self, partition: usize, context: Arc<TaskContext>) {
        match &self.join {
            Some(join) => drop(join.execute(partition, context)),
            None => self.partitions.push((partition, context)),
        }
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
            let context = Arc::clone(&self.context);
            self.state.abandoned.lock().abandon(self.partition, context);
        }
    }
}

/// A partition's collected batches, and the rest of its stream if it did not
/// finish.
type CollectedPartition = (Vec<RecordBatch>, Option<SendableRecordBatchStream>);

struct Collected {
    partitions: Vec<CollectedPartition>,
    /// Accounts for the collected batches
    reservation: Arc<Mutex<MemoryReservation>>,
}

/// Runs all partitions of `plan` in parallel until they finish or have
/// produced more than `max_rows` rows in total, or the memory pool cannot hold
/// more.
async fn collect_up_to(
    plan: &Arc<dyn ExecutionPlan>,
    context: &Arc<TaskContext>,
    max_rows: usize,
) -> Result<Collected> {
    let reservation = Arc::new(Mutex::new(
        MemoryConsumer::new("AdaptiveHashJoinExec").register(context.memory_pool()),
    ));
    let rows = Arc::new(AtomicUsize::new(0));
    let full = Arc::new(AtomicBool::new(false));
    let mut tasks = vec![];
    for partition in 0..plan.output_partitioning().partition_count() {
        let mut stream = plan.execute(partition, Arc::clone(context))?;
        let rows = Arc::clone(&rows);
        let full = Arc::clone(&full);
        let reservation = Arc::clone(&reservation);
        tasks.push(SpawnedTask::spawn(async move {
            let mut batches = vec![];
            while !full.load(Ordering::Relaxed) {
                let Some(batch) = stream.next().await.transpose()? else {
                    return Ok((batches, None));
                };
                let size = batch.get_array_memory_size();
                let total_rows = rows.fetch_add(batch.num_rows(), Ordering::Relaxed)
                    + batch.num_rows();
                // Batches can share buffers, which `size` counts again, so the
                // byte threshold is only checked once collection is complete
                if reservation.lock().try_grow(size).is_err() || total_rows > max_rows {
                    full.store(true, Ordering::Relaxed);
                }
                batches.push(batch);
            }
            Ok::<_, DataFusionError>((batches, Some(stream)))
        }));
    }
    let mut partitions = Vec::with_capacity(tasks.len());
    for task in tasks {
        partitions.push(
            task.join_unwind()
                .await
                .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??,
        );
    }
    Ok(Collected {
        partitions,
        reservation,
    })
}

/// Memory held by `batches`, counting buffers they share only once.
fn memory_size<'a>(batches: impl Iterator<Item = &'a RecordBatch>) -> usize {
    fn add(data: &ArrayData, seen: &mut HashSet<usize>, size: &mut usize) {
        let nulls = data.nulls().map(|nulls| nulls.inner().inner());
        for buffer in data.buffers().iter().chain(nulls) {
            if seen.insert(buffer.data_ptr().as_ptr() as usize) {
                *size += buffer.capacity();
            }
        }
        for child in data.child_data() {
            add(child, seen, size);
        }
    }
    let mut seen = HashSet::new();
    let mut size = 0;
    for batch in batches {
        for column in batch.columns() {
            add(&column.to_data(), &mut seen, &mut size);
        }
    }
    size
}

impl DisplayAs for AdaptiveHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let decision = self.state.decision.get();
        let mode =
            decision.map_or_else(|| "pending".to_string(), |d| format!("{:?}", d.mode));
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AdaptiveHashJoinExec: mode={mode}")?;
                if let Some(rows) = decision.and_then(|d| d.build_rows) {
                    write!(f, ", build_rows={rows}")?;
                }
                if decision.is_some_and(|d| d.output_repartitioned) {
                    write!(f, ", output_repartitioned=true")?;
                }
                let on = self
                    .join
                    .on()
                    .iter()
                    .map(|(l, r)| format!("({l}, {r})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    ", join_type={:?}, on=[{on}], max_build_rows={}",
                    self.join.join_type(),
                    self.max_build_rows
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "mode={mode}")?;
                self.join.fmt_as(t, f)
            }
        }
    }
}

impl ExecutionPlan for AdaptiveHashJoinExec {
    fn name(&self) -> &'static str {
        "AdaptiveHashJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.join.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.build, &self.probe]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.join.apply_expressions(f)
    }

    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.join.dynamic_expressions_produced()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!("AdaptiveHashJoinExec expects two children");
        }
        let probe = children.pop().unwrap();
        let build = children.pop().unwrap();
        Ok(Arc::new(self.with_children(
            build,
            probe,
            Arc::clone(&self.join),
        )))
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
        let join = Arc::new(self.join.builder().reset_state().build()?);
        Ok(Arc::new(self.with_children(
            Arc::clone(&self.build),
            Arc::clone(&self.probe),
            join,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let this = Arc::new(self.with_shared_state());
        let prepare_context = Arc::clone(&context);
        let mut join: OnceFut<Arc<dyn ExecutionPlan>> = self
            .state
            .join
            .try_once(move || Ok(Arc::clone(&this).prepare(prepare_context)))?;
        let guard = PartitionGuard {
            state: Arc::clone(&self.state),
            partition,
            context: Arc::clone(&context),
            started: false,
        };
        let stream = futures::stream::once(async move {
            // Move the whole guard in, not just the field set below
            let mut guard = guard;
            let join = std::future::poll_fn(|cx| join.get_shared(cx)).await?;
            guard.state.abandoned.lock().set_join(&join);
            guard.started = true;
            join.execute(partition, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.join.metrics()
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

impl AdaptiveHashJoinExec {
    /// A copy that shares the runtime state, for executing a partition.
    fn with_shared_state(&self) -> Self {
        Self {
            join: Arc::clone(&self.join),
            build: Arc::clone(&self.build),
            build_partitioning: self.build_partitioning.clone(),
            probe: Arc::clone(&self.probe),
            probe_partitioning: self.probe_partitioning.clone(),
            max_build_rows: self.max_build_rows,
            max_build_bytes: self.max_build_bytes,
            required: self.required,
            state: Arc::clone(&self.state),
        }
    }
}

/// The batches a build side has produced so far, followed by the rest of its
/// streams, handed over to the join to run.
///
/// Each partition can be executed only once: its batches move into the
/// stream so the memory is released as soon as the join drops them.
struct CollectedBuildExec {
    schema: SchemaRef,
    partitions: Vec<Mutex<Option<CollectedPartition>>>,
    /// Rows collected so far
    num_rows: usize,
    complete: bool,
    properties: Arc<PlanProperties>,
}

impl CollectedBuildExec {
    fn new(
        schema: SchemaRef,
        partitions: Vec<CollectedPartition>,
        properties: PlanProperties,
    ) -> Self {
        Self {
            schema,
            num_rows: partitions
                .iter()
                .flat_map(|(batches, _)| batches)
                .map(RecordBatch::num_rows)
                .sum(),
            complete: partitions.iter().all(|(_, rest)| rest.is_none()),
            partitions: partitions
                .into_iter()
                .map(|p| Mutex::new(Some(p)))
                .collect(),
            properties: Arc::new(properties),
        }
    }
}

impl fmt::Debug for CollectedBuildExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectedBuildExec")
            .field("num_rows", &self.num_rows)
            .field("complete", &self.complete)
            .finish()
    }
}

impl DisplayAs for CollectedBuildExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CollectedBuildExec: rows={}{}, partitions={}",
            if self.complete { "" } else { ">" },
            self.num_rows,
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for CollectedBuildExec {
    fn name(&self) -> &'static str {
        "CollectedBuildExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
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

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some((batches, rest)) = self.partitions[partition].lock().take() else {
            return internal_err!(
                "CollectedBuildExec partition {partition} executed twice"
            );
        };
        let collected = MemoryStream::try_new(batches, Arc::clone(&self.schema), None)?;
        Ok(match rest {
            None => Box::pin(collected),
            Some(rest) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                collected.chain(rest),
            )),
        })
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let num_rows = if self.complete {
            Precision::Exact(self.num_rows)
        } else {
            Precision::Absent
        };
        Ok(Arc::new(
            Statistics::new_unknown(&self.schema).with_num_rows(num_rows),
        ))
    }
}
