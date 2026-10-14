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

//! Prototype of adaptive query execution (AQE) for hash joins.
//!
//! The physical optimizer picks the mode of each hash join from *estimated*
//! statistics. It collects the build side into one shared hash table
//! (`PartitionMode::CollectLeft`) only when the estimate is below
//! `hash_join_single_partition_threshold`, and otherwise hash-repartitions both
//! inputs (`PartitionMode::Partitioned`). Estimates for filtered scans and for
//! join or aggregate outputs are rough, so large queries often repartition a
//! huge probe side to join against a build side that turns out to be tiny.
//!
//! [`make_adaptive`] replaces these joins with an [`AdaptiveHashJoinExec`]. It
//! runs the build side first, as a hash join does anyway, and picks the mode
//! from its exact size, so the probe side is only repartitioned when the build
//! side turns out to be large.
//!
//! The rest of the plan is unchanged and executes in the same order: a join's
//! build side completes before the build sides below it on the probe side
//! start, so dynamic filters still reach them.

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::array::{Array, ArrayData};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, JoinType, Result, Statistics, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, Partitioning, PhysicalExpr,
};
use datafusion::physical_plan::execution_plan::{
    Boundedness, ChildrenPropertiesMode, EmissionType, ReplaceChildrenOptions,
    replace_children_if_necessary,
};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    StatisticsArgs, StatisticsContext,
};
use datafusion_common_runtime::SpawnedTask;
use futures::{StreamExt, TryStreamExt};
use tokio::sync::OnceCell;

/// Options for [`AdaptiveHashJoinExec`].
#[derive(Debug, Clone)]
pub struct AdaptiveOptions {
    /// Use `CollectLeft` when the build side has at most this many rows.
    pub broadcast_max_rows: usize,
    /// ... or this many rows for joins that track which build-side rows
    /// matched (`Left`, `LeftSemi`, `LeftAnti`, `LeftMark` and `Full`). With
    /// a single hash table, every probe thread updates one shared bitmap, so
    /// these only pay off for small build sides.
    pub broadcast_max_rows_tracking: usize,
    /// ... and at most this many bytes.
    pub broadcast_max_bytes: usize,
    /// ... and, if non-zero and the probe side has a row count estimate, at
    /// most this fraction of it. Collecting the build side saves
    /// repartitioning both sides, but builds a single hash table in one
    /// thread, which only pays off when the probe side is much larger.
    pub broadcast_max_probe_ratio: f64,
}

impl Default for AdaptiveOptions {
    fn default() -> Self {
        Self {
            broadcast_max_rows: 4 * 1024 * 1024,
            broadcast_max_rows_tracking: 128 * 1024,
            broadcast_max_bytes: 1024 * 1024 * 1024,
            broadcast_max_probe_ratio: 0.05,
        }
    }
}

/// Replaces each `Partitioned` hash join of an optimized `plan` that
/// repartitions its probe side with an [`AdaptiveHashJoinExec`].
///
/// A join whose probe side is already hash partitioned keeps its mode: with
/// nothing to save on the probe side, collecting the build side would only
/// lose the parallel build.
pub fn make_adaptive(
    plan: Arc<dyn ExecutionPlan>,
    options: &AdaptiveOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    rewrite(plan, false, options)
}

/// `required` is whether an ancestor relies on `plan` being hash partitioned,
/// with no repartition in between.
fn rewrite(
    plan: Arc<dyn ExecutionPlan>,
    required: bool,
    options: &AdaptiveOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>()
        && *join.partition_mode() == PartitionMode::Partitioned
        && !join.null_aware
        && hash_repartition_input(join.right()).is_some()
    {
        let join = AdaptiveHashJoinExec::try_new(join, required, options)?;
        return Ok(Arc::new(join));
    }

    // A node that is key partitioned without repartitioning got that from
    // its children, or only from the probe side for a `CollectLeft` join.
    let inherits = required
        && plan.downcast_ref::<RepartitionExec>().is_none()
        && matches!(
            plan.output_partitioning(),
            Partitioning::Hash(..) | Partitioning::Range(..)
        );
    let collect_left = plan
        .downcast_ref::<HashJoinExec>()
        .is_some_and(|join| *join.partition_mode() == PartitionMode::CollectLeft);
    let requirements = plan.input_distribution_requirements();
    let co_partitioned = requirements.is_co_partitioned();
    let children: Vec<_> = plan.children().into_iter().cloned().collect();
    let mut new_children = Vec::with_capacity(children.len());
    for (i, (child, distribution)) in children
        .into_iter()
        .zip(requirements.into_per_child())
        .enumerate()
    {
        let child_required = co_partitioned
            || is_key_distribution(&distribution)
            || (inherits && !(collect_left && i == 0));
        new_children.push(rewrite(child, child_required, options)?);
    }
    replace_children_if_necessary(plan, new_children)
}

#[expect(
    deprecated,
    reason = "HashPartitioned is still accepted during the KeyPartitioned migration"
)]
fn is_key_distribution(distribution: &Distribution) -> bool {
    matches!(
        distribution,
        Distribution::KeyPartitioned(_) | Distribution::HashPartitioned(_)
    )
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

/// A hash join that picks its mode at runtime from the exact size of its
/// build side.
///
/// It wraps a `Partitioned` [`HashJoinExec`] whose probe side is hash
/// repartitioned, and keeps its properties and its dynamic filter, which the
/// optimizer has already pushed into the probe side. On first execution it
/// collects the build side, up to [`AdaptiveOptions::broadcast_max_rows`]. If
/// it all fits, it runs the wrapped join in `CollectLeft` mode on the probe
/// input as it is. Otherwise it runs it in `Partitioned` mode: the rest of the
/// build side streams on behind the collected batches, and both sides are
/// repartitioned as the static plan does.
///
/// In `CollectLeft` mode the output is not hash partitioned, so if an
/// ancestor relies on that the output is repartitioned instead of the probe
/// side, which pays off when the join is selective.
#[derive(Debug)]
pub struct AdaptiveHashJoinExec {
    /// The join chosen by the static planner
    join: Arc<HashJoinExec>,
    /// The build-side input, without the hash repartition the planner added
    build: Arc<dyn ExecutionPlan>,
    /// The partitioning of that repartition, if any
    build_partitioning: Option<Partitioning>,
    /// The probe-side input, without the hash repartition the planner added
    probe: Arc<dyn ExecutionPlan>,
    probe_partitioning: Partitioning,
    /// Estimated number of rows on the probe side
    probe_rows: Option<usize>,
    /// Whether an ancestor relies on the output being hash partitioned
    required: bool,
    options: AdaptiveOptions,
    /// The join to run, created on first execution
    inner: Arc<OnceCell<Arc<dyn ExecutionPlan>>>,
    abandoned: Arc<Mutex<Abandoned>>,
    decision: Arc<OnceLock<String>>,
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
    abandoned: Arc<Mutex<Abandoned>>,
    partition: usize,
    context: Arc<TaskContext>,
    started: bool,
}

impl Drop for PartitionGuard {
    fn drop(&mut self) {
        if !self.started {
            let context = Arc::clone(&self.context);
            self.abandoned
                .lock()
                .unwrap()
                .abandon(self.partition, context);
        }
    }
}

impl AdaptiveHashJoinExec {
    fn try_new(
        join: &HashJoinExec,
        required: bool,
        options: &AdaptiveOptions,
    ) -> Result<Self> {
        let probe = hash_repartition_input(join.right())
            .expect("probe side is hash repartitioned");
        let probe_partitioning = join.right().output_partitioning().clone();
        // If the planner relies on the build side already being hash
        // partitioned, keep relying on it rather than repartitioning, so that
        // each partition can start probing as soon as its own build is done.
        let (build, build_partitioning) = match hash_repartition_input(join.left()) {
            Some(input) => (input, Some(join.left().output_partitioning().clone())),
            None => (join.left(), None),
        };
        let build = rewrite(Arc::clone(build), build_partitioning.is_none(), options)?;
        let probe_rows = StatisticsContext::new()
            .compute(probe.as_ref(), &StatisticsArgs::new())?
            .num_rows
            .get_value()
            .copied();
        let probe = rewrite(Arc::clone(probe), false, options)?;
        Ok(Self {
            join: Arc::new(join.builder().build()?),
            build,
            build_partitioning,
            probe,
            probe_partitioning,
            probe_rows,
            required,
            options: options.clone(),
            inner: Arc::new(OnceCell::new()),
            abandoned: Default::default(),
            decision: Arc::new(OnceLock::new()),
        })
    }

    /// The largest build side, in rows, to run in `CollectLeft` mode.
    fn broadcast_max_rows(&self) -> usize {
        let options = &self.options;
        let mut max_rows = if tracks_build_side(*self.join.join_type()) {
            options.broadcast_max_rows_tracking
        } else {
            options.broadcast_max_rows
        };
        if let Some(probe_rows) = self.probe_rows
            && options.broadcast_max_probe_ratio > 0.0
        {
            let limit = probe_rows as f64 * options.broadcast_max_probe_ratio;
            max_rows = max_rows.min(limit as usize);
        }
        max_rows
    }

    /// Collects the build side, up to the broadcast threshold, and creates
    /// the plan to run.
    async fn prepare(&self, context: Arc<TaskContext>) -> Result<Arc<dyn ExecutionPlan>> {
        let start = Instant::now();
        let schema = self.build.schema();
        let max_rows = self.broadcast_max_rows();
        let partitions = collect_up_to(&self.build, context, max_rows).await?;
        let rows: usize = partitions
            .iter()
            .flat_map(|(batches, _)| batches)
            .map(RecordBatch::num_rows)
            .sum();
        let complete = partitions.iter().all(|(_, rest)| rest.is_none());
        let bytes = memory_size(partitions.iter().flat_map(|(batches, _)| batches));
        let output_partitioning = &self.join.properties().partitioning;
        let output_partitions = output_partitioning.partition_count();

        let broadcast =
            complete && rows <= max_rows && bytes <= self.options.broadcast_max_bytes;
        let (mode, build, probe) = if broadcast {
            let batches = partitions.into_iter().flat_map(|(batches, _)| batches);
            let properties = PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            let stage = AdaptiveStageExec::new(
                schema,
                vec![(batches.collect(), None)],
                properties,
            );
            let mut probe = Arc::clone(&self.probe);
            if !self.required
                && probe.output_partitioning().partition_count() != output_partitions
            {
                probe = Arc::new(RepartitionExec::try_new(
                    probe,
                    Partitioning::RoundRobinBatch(output_partitions),
                )?);
            }
            (PartitionMode::CollectLeft, Arc::new(stage) as _, probe)
        } else {
            // The rest of the build side streams on, as in a plain
            // partitioned join
            let properties = PlanProperties::clone(self.build.properties());
            let stage = AdaptiveStageExec::new(schema, partitions, properties);
            let build: Arc<dyn ExecutionPlan> = match &self.build_partitioning {
                Some(partitioning) => Arc::new(RepartitionExec::try_new(
                    Arc::new(stage),
                    partitioning.clone(),
                )?),
                None => Arc::new(stage),
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
        let repartition_output = self.required && mode == PartitionMode::CollectLeft;
        if repartition_output {
            plan = Arc::new(RepartitionExec::try_new(plan, output_partitioning.clone())?);
        }
        let _ = self.decision.set(format!(
            "mode={mode:?}{}, build_rows={}{rows}, max_rows={max_rows}, probe_rows_estimate={}, decided_after={:.1}ms",
            if repartition_output {
                " (output repartitioned)"
            } else {
                ""
            },
            if complete { "" } else { ">" },
            self.probe_rows
                .map_or_else(|| "?".to_string(), |rows| rows.to_string()),
            start.elapsed().as_secs_f64() * 1000.0
        ));
        Ok(plan)
    }

    /// A copy with the same inputs that shares the plan to run, so all
    /// partitions run the same one.
    fn shared(&self) -> Self {
        Self {
            join: Arc::clone(&self.join),
            build: Arc::clone(&self.build),
            build_partitioning: self.build_partitioning.clone(),
            probe: Arc::clone(&self.probe),
            probe_partitioning: self.probe_partitioning.clone(),
            probe_rows: self.probe_rows,
            required: self.required,
            options: self.options.clone(),
            inner: Arc::clone(&self.inner),
            abandoned: Arc::clone(&self.abandoned),
            decision: Arc::clone(&self.decision),
        }
    }
}

/// Whether the join tracks which build-side rows matched, see
/// [`AdaptiveOptions::broadcast_max_rows_tracking`].
fn tracks_build_side(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark
            | JoinType::Full
    )
}

/// A partition's collected batches, and the rest of its stream if it did not
/// finish.
type CollectedPartition = (Vec<RecordBatch>, Option<SendableRecordBatchStream>);

/// Runs all partitions of `plan` in parallel until they finish or have
/// produced more than `max_rows` rows in total.
async fn collect_up_to(
    plan: &Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    max_rows: usize,
) -> Result<Vec<CollectedPartition>> {
    let rows = Arc::new(AtomicUsize::new(0));
    let mut tasks = vec![];
    for partition in 0..plan.output_partitioning().partition_count() {
        let mut stream = plan.execute(partition, Arc::clone(&context))?;
        let rows = Arc::clone(&rows);
        tasks.push(SpawnedTask::spawn(async move {
            let mut batches = vec![];
            while rows.load(Ordering::Relaxed) <= max_rows {
                let Some(batch) = stream.next().await.transpose()? else {
                    return Ok((batches, None));
                };
                rows.fetch_add(batch.num_rows(), Ordering::Relaxed);
                batches.push(batch);
            }
            Ok::<_, DataFusionError>((batches, Some(stream)))
        }));
    }
    let mut partitions = Vec::with_capacity(tasks.len());
    for task in tasks {
        partitions.push(task.join_unwind().await.map_err(|e| {
            DataFusionError::Execution(format!("build side task failed: {e}"))
        })??);
    }
    Ok(partitions)
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
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let on = self
            .join
            .on()
            .iter()
            .map(|(l, r)| format!("({l}, {r})"))
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "AdaptiveHashJoinExec: {}, join_type={:?}, on=[{on}]",
            self.decision.get().map_or("mode=pending", String::as_str),
            self.join.join_type(),
        )
    }
}

impl ExecutionPlan for AdaptiveHashJoinExec {
    fn name(&self) -> &str {
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
        Ok(Arc::new(Self {
            join: Arc::clone(&self.join),
            build,
            build_partitioning: self.build_partitioning.clone(),
            probe,
            probe_partitioning: self.probe_partitioning.clone(),
            probe_rows: self.probe_rows,
            required: self.required,
            options: self.options.clone(),
            inner: Arc::new(OnceCell::new()),
            abandoned: Default::default(),
            decision: Arc::new(OnceLock::new()),
        }))
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
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let this = self.shared();
        let guard = PartitionGuard {
            abandoned: Arc::clone(&self.abandoned),
            partition,
            context: Arc::clone(&context),
            started: false,
        };
        let stream = futures::stream::once(async move {
            // Move the whole guard in, not just the field set below
            let mut guard = guard;
            let inner = this
                .inner
                .get_or_try_init(|| this.prepare(Arc::clone(&context)))
                .await?;
            this.abandoned.lock().unwrap().set_join(inner);
            guard.started = true;
            inner.execute(partition, context)
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

/// The batches a build side has produced so far, followed by the rest of its
/// streams, handed over to the consumer.
///
/// Each partition can be executed only once: its batches move into the
/// stream so the memory is released as soon as the consumer drops them.
struct AdaptiveStageExec {
    schema: SchemaRef,
    partitions: Vec<Mutex<Option<CollectedPartition>>>,
    /// Rows collected so far
    num_rows: usize,
    complete: bool,
    properties: Arc<PlanProperties>,
}

impl AdaptiveStageExec {
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

impl fmt::Debug for AdaptiveStageExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdaptiveStageExec")
            .field("num_rows", &self.num_rows)
            .finish()
    }
}

impl DisplayAs for AdaptiveStageExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AdaptiveStageExec: rows={}{}, partitions={}",
            if self.complete { "" } else { ">" },
            self.num_rows,
            self.partitions.len()
        )
    }
}

impl ExecutionPlan for AdaptiveStageExec {
    fn name(&self) -> &str {
        "AdaptiveStageExec"
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
        let Some((batches, rest)) = self.partitions[partition].lock().unwrap().take()
        else {
            return internal_err!(
                "AdaptiveStageExec partition {partition} executed twice"
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

    use super::*;

    /// A context that never picks `CollectLeft` statically, with parquet
    /// tables so that hash joins push dynamic filters into their probe side:
    /// `dim` (1000 keys), `fact` (100K rows over 2000 keys) and `tiny` (1 row).
    async fn context(dir: &Path) -> Result<SessionContext> {
        let mut config = SessionConfig::new().with_target_partitions(8);
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        let ctx = SessionContext::new_with_config(config);
        let tables = [
            ("dim", "SELECT value AS k FROM generate_series(1, 1000)"),
            (
                "fact",
                "SELECT value % 2000 AS k, value AS v FROM generate_series(1, 100000)",
            ),
            ("tiny", "SELECT 7 AS k"),
        ];
        for (name, query) in tables {
            let path = dir.join(format!("{name}.parquet"));
            let path = path.to_str().unwrap();
            ctx.sql(&format!("COPY ({query}) TO '{path}' STORED AS PARQUET"))
                .await?
                .collect()
                .await?;
            ctx.register_parquet(name, path, ParquetReadOptions::default())
                .await?;
        }
        Ok(ctx)
    }

    /// Runs `sql`, made adaptive with `options` if given, and returns its
    /// results and the plan after execution.
    async fn run(
        ctx: &SessionContext,
        sql: &str,
        options: Option<&AdaptiveOptions>,
    ) -> Result<(String, String)> {
        let mut plan = ctx.sql(sql).await?.create_physical_plan().await?;
        if let Some(options) = options {
            plan = make_adaptive(plan, options)?;
        }
        let batches = tokio::time::timeout(
            Duration::from_secs(30),
            collect(Arc::clone(&plan), ctx.task_ctx()),
        )
        .await
        .expect("query hung")?;
        let results = pretty_format_batches(&batches)?.to_string();
        Ok((
            results,
            displayable(plan.as_ref()).indent(false).to_string(),
        ))
    }

    const JOIN: &str = "SELECT count(*), sum(f.v) FROM dim d JOIN fact f ON d.k = f.k";

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn small_build_side_is_collected() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = context(dir.path()).await?;
        let (expected, _) = run(&ctx, JOIN, None).await?;
        let (results, plan) = run(&ctx, JOIN, Some(&AdaptiveOptions::default())).await?;
        assert_eq!(results, expected);
        assert!(
            plan.contains("AdaptiveHashJoinExec: mode=CollectLeft, build_rows=1000,"),
            "{plan}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_build_side_stays_partitioned() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = context(dir.path()).await?;
        let options = AdaptiveOptions {
            broadcast_max_rows: 100,
            ..Default::default()
        };
        let (expected, _) = run(&ctx, JOIN, None).await?;
        let (results, plan) = run(&ctx, JOIN, Some(&options)).await?;
        assert_eq!(results, expected);
        // Collection stops once the threshold is exceeded
        assert!(
            plan.contains("AdaptiveHashJoinExec: mode=Partitioned, build_rows=>"),
            "{plan}"
        );
        Ok(())
    }

    /// The outer partitioned join has a one-row build side, so all but one of
    /// its partitions finish without polling the adaptive join below. That
    /// join still has to cancel those partitions of its own partitioned join,
    /// whose dynamic filter waits for every partition to report.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unpolled_partitions_do_not_hang() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = context(dir.path()).await?;
        let sql = "SELECT count(*) FROM tiny t \
            JOIN (SELECT f.k FROM dim d JOIN fact f ON d.k = f.k) j ON t.k = j.k";
        let options = AdaptiveOptions {
            broadcast_max_rows: 0,
            ..Default::default()
        };
        let (expected, _) = run(&ctx, sql, None).await?;
        let (results, plan) = run(&ctx, sql, Some(&options)).await?;
        assert_eq!(results, expected);
        assert!(
            plan.contains("AdaptiveHashJoinExec: mode=Partitioned"),
            "{plan}"
        );
        Ok(())
    }
}
