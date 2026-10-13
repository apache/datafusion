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

//! Execution plans for `WITH x AS MATERIALIZED (...)`.
//!
//! [`MaterializedCteExec`] owns the CTE body and the continuation (the rest of
//! the query). Every [`MaterializedCteScanExec`] in the continuation shares one
//! [`MaterializedCteBuffer`] with it. The first consumer to execute runs every
//! partition of the body to completion and buffers the output; every scan then
//! replays that buffer.
//!
//! The buffered output is not kept in the plan. It is kept in the
//! [`RunState`] of the [`TaskContext`], so each run of a plan has its own
//! output, and two runs of one plan that overlap do not share it. All
//! `execute` calls of one run must get the same [`TaskContext`]. With a
//! different [`TaskContext`] for each call, each call computes the body again.
//!
//! The output of a run is released when no scan can read it again: when each
//! partition of each bound scan was executed in the run, and each of these
//! executions has finished or was dropped. See `RunOutput`. Until then, the
//! [`RunState`] holds the output, so a scan partition that is never executed
//! keeps it until the run ends. A scan partition that is executed again after
//! the release computes the body again, as a second run of the plan with the
//! same [`TaskContext`] does.
//!
//! The plan nodes keep no state of a run, so the default
//! [`ExecutionPlan::reset_state`] is correct for them: it returns a new node
//! and the old node stays usable.
//!
//! `RecursiveQueryExec` executes its recursive term one time for each
//! iteration, with the same [`TaskContext`]. It gives each iteration new
//! outputs for the CTEs that the recursive term declares. See
//! `with_new_materialized_cte_outputs`. Its recursive term can also read a
//! CTE that is declared outside it, one time for each iteration. It holds the
//! output of such a CTE until its last iteration. See
//! `hold_materialized_cte_outputs`.
//!
//! The body is fully materialized before any scan yields a row. This is a
//! pipeline break, but it cannot deadlock when two consumers of the same CTE
//! are read at different rates (for example the build and probe side of one
//! hash join), which a bounded fan-out channel can.
//!
//! Buffered batches are accounted in the memory pool. When a reservation
//! fails, the rest of that partition is written to a spill file, and the scans
//! read the in-memory prefix and then the spill files, so row order within a
//! partition is kept.
//!
//! A scan reads a spill file one batch at a time, without read-ahead. After
//! the body is buffered, memory for one decoded batch per concurrent reader is
//! reserved. If the pool cannot grant it, in-memory batches are moved to spill
//! files until it can. An in-memory batch that is larger than the batches on
//! disk is written in pieces, so that the move does not raise the memory that
//! each reader needs.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, Statistics, internal_err};
use datafusion_common_runtime::JoinSet;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{RunState, SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;

use crate::coop::cooperative;
use crate::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SchedulingType,
};
use crate::filter_pushdown::{
    ChildFilterDescription, ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::joins::utils::{OnceAsync, OnceFut};
use crate::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, SpillMetrics,
};
use crate::spill::spill_manager::SpillManager;
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ReplaceChildrenOptions,
};

/// The buffered output of one body partition: an in-memory prefix followed by
/// spill files with the remaining batches, in order.
struct BufferedPartition {
    batches: Vec<RecordBatch>,
    spill_files: Vec<Arc<dyn SpillFile>>,
    /// The memory size of the largest batch in `spill_files` when decoded.
    max_spilled_batch_size: usize,
    /// Holds `batches`. Released when the output is dropped.
    reservation: MemoryReservation,
}

/// A plan to move the in-memory batches `split..` of a [`BufferedPartition`]
/// to a spill file. See [`BufferedPartition::plan_spill`].
struct SpillPlan {
    split: usize,
    /// A batch larger than this is written in pieces of about this size.
    piece_size: usize,
    /// The memory that the move releases, less the memory that it adds to
    /// the replay of the spill files.
    gain: usize,
}

impl BufferedPartition {
    /// Plan a move of in-memory batches from the end of `batches` to a spill
    /// file, to gain at least `bytes`.
    ///
    /// A spilled batch that is larger than every batch already on disk makes
    /// each scan reserve more memory to decode it. `added_replay_memory`
    /// returns that increase for a new `max_spilled_batch_size`. To keep it
    /// small, a batch larger than `piece_size` is written in pieces.
    ///
    /// Returns `None` when no move releases more memory than it adds.
    fn plan_spill(
        &self,
        bytes: usize,
        piece_size: usize,
        added_replay_memory: impl Fn(usize) -> usize,
    ) -> Option<SpillPlan> {
        let mut best: Option<SpillPlan> = None;
        let mut released = 0;
        let mut max_size = self.max_spilled_batch_size;
        let mut added = 0;
        for (split, batch) in self.batches.iter().enumerate().rev() {
            let size = batch.get_array_memory_size();
            released += size;
            // A batch of one row cannot be written in pieces.
            let spilled_size = match batch.num_rows() {
                0 | 1 => size,
                _ => size.min(piece_size),
            };
            if spilled_size > max_size {
                max_size = spilled_size;
                added = added_replay_memory(max_size);
            }
            let gain = released.saturating_sub(added);
            if gain > best.as_ref().map_or(0, |b| b.gain) {
                best = Some(SpillPlan {
                    split,
                    piece_size,
                    gain,
                });
            }
            if gain >= bytes {
                break;
            }
        }
        best
    }

    /// Move the in-memory batches of `plan` to a new spill file. The new file
    /// is read before the existing spill files, so the order of the partition
    /// is kept.
    fn spill(&mut self, plan: &SpillPlan, spill_manager: &SpillManager) -> Result<()> {
        let suffix = self.batches.split_off(plan.split);
        let mut file = spill_manager.create_in_progress_file("MaterializedCte")?;
        let mut released = 0;
        for batch in &suffix {
            let size = batch.get_array_memory_size();
            released += size;
            let rows = batch.num_rows();
            // Rows per piece, so that a piece is about `piece_size`.
            let piece_rows = rows.div_ceil(size.div_ceil(plan.piece_size).max(1)).max(1);
            for offset in (0..rows.max(1)).step_by(piece_rows) {
                let piece = batch.slice(offset, piece_rows.min(rows - offset));
                let size = file.append_batch(&piece)?;
                self.max_spilled_batch_size = self.max_spilled_batch_size.max(size);
            }
        }
        if let Some(file) = file.finish()? {
            self.spill_files.insert(0, file);
        }
        self.reservation.shrink(released);
        Ok(())
    }
}

/// The materialized output of a CTE body.
struct MaterializedOutput {
    partitions: Vec<BufferedPartition>,
    spill_manager: SpillManager,
    /// Memory for the batches that the scans decode from the spill files.
    /// Released when the output is dropped.
    _replay_reservation: MemoryReservation,
}

/// The memory that the scans need to read the spill files of the body
/// partitions. `max_spilled_batch_sizes` has one entry per body partition.
///
/// Partition `p` of a scan with `n` partitions reads the body partitions
/// `p, p + n, ...` one after the other, with at most one decoded batch in
/// memory. All scan partitions can read at the same time.
fn replay_memory(max_spilled_batch_sizes: &[usize], scan_partitions: &[usize]) -> usize {
    scan_partitions
        .iter()
        .map(|&n| {
            (0..n)
                .map(|p| {
                    max_spilled_batch_sizes
                        .iter()
                        .skip(p)
                        .step_by(n)
                        .max()
                        .copied()
                        .unwrap_or(0)
                })
                .sum::<usize>()
        })
        .sum()
}

/// Grow `reservation` to the memory the scans need to replay `partitions`.
/// If the pool cannot grant it, move in-memory batches to spill files until
/// it can.
///
/// A move can also raise the memory that the replay needs, because a scan
/// reserves memory for the largest batch that it reads from disk. Each step
/// makes the move that gains the most memory, and a batch that is larger than
/// the batches on disk is written in pieces. If no move gains memory, the
/// allocation error is returned.
fn reserve_replay_memory(
    partitions: &mut [BufferedPartition],
    scan_partitions: &[usize],
    spill_manager: &SpillManager,
    reservation: &mut MemoryReservation,
) -> Result<()> {
    loop {
        let sizes: Vec<_> = partitions
            .iter()
            .map(|p| p.max_spilled_batch_size)
            .collect();
        let required = replay_memory(&sizes, scan_partitions);
        let Err(e) = reservation.try_resize(required) else {
            return Ok(());
        };
        // `required` is not zero, so at least one partition has a spill file.
        let largest = sizes.iter().copied().max().unwrap_or(0).max(1);
        let plan = partitions
            .iter()
            .enumerate()
            .filter_map(|(i, partition)| {
                let piece_size = match sizes[i] {
                    0 => largest,
                    size => size,
                };
                let added_replay_memory = |size| {
                    let mut sizes = sizes.clone();
                    sizes[i] = size;
                    replay_memory(&sizes, scan_partitions).saturating_sub(required)
                };
                let plan =
                    partition.plan_spill(required, piece_size, added_replay_memory)?;
                Some((i, plan))
            })
            .max_by_key(|(_, plan)| plan.gain);
        let Some((i, plan)) = plan else {
            return Err(e);
        };
        partitions[i].spill(&plan, spill_manager)?;
    }
}

/// The output of one materialized CTE in one run of a plan, and the count of
/// its readers.
///
/// The output is released when `not_executed` and `users` are both zero.
/// Then each partition of each bound scan was executed, and each of these
/// executions has finished or was dropped, so a plan in which each scan
/// partition is executed one time cannot read the output again.
///
/// The count is of the partitions that can still be executed, not of the
/// streams that are open. Thus the output is not released between two scans
/// that are executed one after the other. A partition that is never executed
/// (for example because the query was cancelled) keeps the output until the
/// run ends, which is the time of release without this count.
///
/// The count does not know that a partition is executed more than one time.
/// When a partition is executed after the release, a new [`RunOutput`] is
/// created and the body is computed again. The scan never reads an empty
/// output. An operator that executes a scan more than one time in a run must
/// hold the output. See [`hold_materialized_cte_outputs`].
struct RunOutput {
    once: Arc<OnceAsync<MaterializedOutput>>,
    /// For each bound scan, whether each of its partitions was executed.
    executed: Vec<Vec<bool>>,
    /// The number of partitions in `executed` that were not executed.
    not_executed: usize,
    /// The number of [`MaterializedCteOutputUse`]s: the scan streams that
    /// are open, and the holds.
    users: usize,
}

impl RunOutput {
    fn new() -> Self {
        Self {
            once: Arc::default(),
            executed: vec![],
            not_executed: 0,
            users: 0,
        }
    }

    /// Add the scans in `scan_partitions` that are not yet counted.
    fn add_scans(&mut self, scan_partitions: &[usize]) {
        for &partitions in scan_partitions.iter().skip(self.executed.len()) {
            self.executed.push(vec![false; partitions]);
            self.not_executed += partitions;
        }
    }
}

/// The outputs of the materialized CTEs in one run of a plan, by CTE id.
///
/// The [`RunState`] of the [`TaskContext`] holds it. An output is removed
/// when its last reader has finished (see [`RunOutput`]), or else it lives
/// until the last [`TaskContext`] of the run is dropped.
#[derive(Default)]
struct MaterializedCteOutputs {
    /// The outputs of the enclosing execution. `None` for a whole run.
    parent: Option<Arc<MaterializedCteOutputs>>,
    /// The CTEs that have their own output here when `parent` is set. The
    /// output of every other CTE is the output in `parent`.
    renewed: HashSet<u64>,
    outputs: Mutex<HashMap<u64, RunOutput>>,
}

impl MaterializedCteOutputs {
    /// Register a reader of the output of the CTE `id`, and return the
    /// output. The output is created, empty, if there is none.
    ///
    /// `scan` is the index and the partition of the scan that is executed.
    /// `None` registers a hold: a reader that does not execute a scan.
    /// The output is not released before the returned
    /// [`MaterializedCteOutputUse`] is dropped.
    fn start_use(
        self: &Arc<Self>,
        id: u64,
        scan_partitions: &[usize],
        scan: Option<(usize, usize)>,
    ) -> (Arc<OnceAsync<MaterializedOutput>>, MaterializedCteOutputUse) {
        if let Some(parent) = &self.parent
            && !self.renewed.contains(&id)
        {
            return parent.start_use(id, scan_partitions, scan);
        }
        let mut outputs = self.outputs.lock();
        let output = outputs.entry(id).or_insert_with(RunOutput::new);
        output.add_scans(scan_partitions);
        output.users += 1;
        if let Some((scan, partition)) = scan
            && let Some(executed) = output
                .executed
                .get_mut(scan)
                .and_then(|scan| scan.get_mut(partition))
            && !*executed
        {
            *executed = true;
            output.not_executed -= 1;
        }
        let in_use = MaterializedCteOutputUse {
            outputs: Arc::clone(self),
            id,
        };
        (Arc::clone(&output.once), in_use)
    }
}

/// One reader of the output of a materialized CTE in a run: a scan stream
/// that is open, or a hold. To drop it ends the read, and releases the output
/// if no reader remains and none can start. See [`RunOutput`].
pub(crate) struct MaterializedCteOutputUse {
    outputs: Arc<MaterializedCteOutputs>,
    id: u64,
}

impl Drop for MaterializedCteOutputUse {
    fn drop(&mut self) {
        let released = {
            let mut outputs = self.outputs.outputs.lock();
            // An output is removed only when it has no users, so this is the
            // output that `start_use` returned.
            let Some(output) = outputs.get_mut(&self.id) else {
                return;
            };
            output.users -= 1;
            if output.users == 0 && output.not_executed == 0 {
                outputs.remove(&self.id)
            } else {
                None
            }
        };
        // Drop the output without the lock. If the body has not completed,
        // this drops the streams of the body, which can be the last users of
        // other outputs in the same map.
        drop(released);
    }
}

/// Return the ids of the materialized CTEs that `plan` declares.
pub(crate) fn materialized_cte_ids(plan: &Arc<dyn ExecutionPlan>) -> HashSet<u64> {
    let mut ids = HashSet::new();
    plan.apply(|node| {
        if let Some(cte) = node.downcast_ref::<MaterializedCteExec>() {
            ids.insert(cte.buffer.id);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("the closure does not fail");
    ids
}

/// Return a copy of `context` in which the materialized CTEs `ids` have no
/// output, so that they are computed again. All other CTEs keep the output
/// that they have in `context`.
///
/// `RecursiveQueryExec` calls this for each iteration with the CTEs that its
/// recursive term declares. A CTE that is declared outside the recursive term
/// and read inside it is not in `ids`, so it is computed one time for the
/// whole query.
pub(crate) fn with_new_materialized_cte_outputs(
    context: &TaskContext,
    ids: &HashSet<u64>,
) -> TaskContext {
    let run_state = context.run_state();
    let outputs = MaterializedCteOutputs {
        parent: Some(run_state.get_or_init()),
        renewed: ids.clone(),
        outputs: Mutex::default(),
    };
    let run_state = RunState::new_child(Arc::clone(run_state), Arc::new(outputs));
    context.clone().with_run_state(Arc::new(run_state))
}

/// Hold the outputs of the materialized CTEs that `plan` reads and that are
/// not in `declared`, until the returned values are dropped.
///
/// `RecursiveQueryExec` calls this with its recursive term, and the CTEs that
/// the recursive term declares. It executes the recursive term one time for
/// each iteration, so the scans of a CTE that is declared outside it are
/// executed more than one time in a run. Without the hold, the output would
/// be released after an iteration, and the next iteration would compute the
/// body again.
pub(crate) fn hold_materialized_cte_outputs(
    plan: &Arc<dyn ExecutionPlan>,
    declared: &HashSet<u64>,
    context: &TaskContext,
) -> Vec<MaterializedCteOutputUse> {
    let mut buffers = HashMap::new();
    plan.apply(|node| {
        if let Some(scan) = node.downcast_ref::<MaterializedCteScanExec>()
            && let Some(buffer) = &scan.buffer
            && !declared.contains(&buffer.id)
        {
            buffers.insert(buffer.id, Arc::clone(buffer));
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("the closure does not fail");
    if buffers.is_empty() {
        return vec![];
    }
    let outputs = context.run_state().get_or_init::<MaterializedCteOutputs>();
    buffers
        .values()
        .map(|buffer| {
            let scan_partitions = buffer.scan_partitions.lock().clone();
            outputs.start_use(buffer.id, &scan_partitions, None).1
        })
        .collect()
}

/// The plan-time state shared by one [`MaterializedCteExec`] and all its
/// [`MaterializedCteScanExec`]s. It holds no output: the output of a run is in
/// the [`RunState`] of the [`TaskContext`] of that run.
pub struct MaterializedCteBuffer {
    id: u64,
    name: String,
    /// The body to run. [`MaterializedCteExec`] updates it every time it is
    /// rebuilt (for example by a physical optimizer rule), so it is the body of
    /// the final plan. A scan can run the body before the owning
    /// `MaterializedCteExec` executes, for example from a scalar subquery.
    body: Mutex<Option<Arc<dyn ExecutionPlan>>>,
    /// The partition count of every scan bound to this buffer.
    scan_partitions: Mutex<Vec<usize>>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for MaterializedCteBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializedCteBuffer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl MaterializedCteBuffer {
    pub fn new(id: u64, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            body: Mutex::new(None),
            scan_partitions: Mutex::new(vec![]),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The id that binds scans to this buffer.
    pub fn id(&self) -> u64 {
        self.id
    }

    fn set_body(&self, body: Arc<dyn ExecutionPlan>) {
        *self.body.lock() = Some(body);
    }

    /// Return a future that resolves once the body has been fully buffered.
    /// The body runs at most once for each run, whichever consumer asks
    /// first. The run is identified by the [`RunState`] of `context`.
    ///
    /// `scan` is the index and the partition of the scan that reads the
    /// output, or `None` for a hold. The caller must keep the returned
    /// [`MaterializedCteOutputUse`] until it has read the output. See
    /// [`MaterializedCteOutputs::start_use`].
    fn materialize(
        self: &Arc<Self>,
        context: Arc<TaskContext>,
        scan: Option<(usize, usize)>,
    ) -> Result<(OnceFut<MaterializedOutput>, MaterializedCteOutputUse)> {
        let Some(body) = self.body.lock().clone() else {
            return internal_err!("MaterializedCte {} has no body", self.name);
        };
        let scan_partitions = self.scan_partitions.lock().clone();
        let (once, in_use) = context
            .run_state()
            .get_or_init::<MaterializedCteOutputs>()
            .start_use(self.id, &scan_partitions, scan);
        let name = self.name.clone();
        let metrics = self.metrics.clone();
        let output = once.try_once(move || {
            Ok(buffer_body(name, body, scan_partitions, context, metrics))
        })?;
        Ok((output, in_use))
    }
}

async fn buffer_body(
    name: String,
    body: Arc<dyn ExecutionPlan>,
    scan_partitions: Vec<usize>,
    context: Arc<TaskContext>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<MaterializedOutput> {
    let spill_manager = SpillManager::new(
        context.runtime_env(),
        SpillMetrics::new(&metrics, 0),
        body.schema(),
    )
    .with_compression_type(context.session_config().spill_compression());
    let buffered_rows = MetricBuilder::new(&metrics).global_counter("buffered_rows");

    let mut join_set = JoinSet::new();
    for partition in 0..body.output_partitioning().partition_count() {
        let stream = body.execute(partition, Arc::clone(&context))?;
        let reservation =
            MemoryConsumer::new(format!("MaterializedCte[{name}][{partition}]"))
                .with_can_spill(true)
                .register(context.memory_pool());
        let spill_manager = spill_manager.clone();
        let buffered_rows = buffered_rows.clone();
        join_set.spawn(async move {
            let result =
                buffer_partition(stream, reservation, &spill_manager, &buffered_rows)
                    .await;
            (partition, result)
        });
    }

    let mut partitions = Vec::with_capacity(join_set.len());
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok((partition, result)) => partitions.push((partition, result?)),
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => return internal_err!("MaterializedCte task failed: {e}"),
        }
    }
    partitions.sort_by_key(|(partition, _)| *partition);
    let mut partitions: Vec<_> = partitions.into_iter().map(|(_, p)| p).collect();

    let mut replay_reservation =
        MemoryConsumer::new(format!("MaterializedCte[{name}] replay"))
            .register(context.memory_pool());
    reserve_replay_memory(
        &mut partitions,
        &scan_partitions,
        &spill_manager,
        &mut replay_reservation,
    )?;
    Ok(MaterializedOutput {
        partitions,
        spill_manager,
        _replay_reservation: replay_reservation,
    })
}

async fn buffer_partition(
    mut stream: SendableRecordBatchStream,
    reservation: MemoryReservation,
    spill_manager: &SpillManager,
    buffered_rows: &crate::metrics::Count,
) -> Result<BufferedPartition> {
    let mut batches = vec![];
    let mut spill = None;
    let mut max_spilled_batch_size = 0;
    while let Some(batch) = stream.next().await.transpose()? {
        buffered_rows.add(batch.num_rows());
        // Once a partition spills, every later batch goes to the same file,
        // so replay keeps the order of the partition.
        if spill.is_none() && reservation.try_grow(batch.get_array_memory_size()).is_ok()
        {
            batches.push(batch);
            continue;
        }
        if spill.is_none() {
            spill = Some(spill_manager.create_in_progress_file("MaterializedCte")?);
        }
        if let Some(file) = spill.as_mut() {
            let size = file.append_batch(&batch)?;
            max_spilled_batch_size = max_spilled_batch_size.max(size);
        }
    }
    let spill_file = match spill {
        Some(mut file) => file.finish()?,
        None => None,
    };
    Ok(BufferedPartition {
        batches,
        spill_files: spill_file.into_iter().collect(),
        max_spilled_batch_size,
        reservation,
    })
}

/// Stream the buffered partitions `partition, partition + n, ...` of `output`,
/// where `n` is the number of output partitions of the scan.
fn replay(
    output: Arc<MaterializedOutput>,
    partition: usize,
    output_partitions: usize,
) -> Result<ReplayStream> {
    let mut streams = vec![];
    for buffered in output
        .partitions
        .iter()
        .skip(partition)
        .step_by(output_partitions)
    {
        let schema = Arc::clone(output.spill_manager.schema());
        let batches = buffered.batches.clone();
        streams.push(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        )) as SendableRecordBatchStream);
        // Read without read-ahead, so that the decoded batch fits in the
        // replay reservation.
        for file in &buffered.spill_files {
            streams.push(output.spill_manager.read_spill_as_stream_unbuffered(
                Arc::clone(file),
                Some(buffered.max_spilled_batch_size),
            )?);
        }
    }
    Ok(ReplayStream {
        stream: futures::stream::iter(streams).flatten().boxed(),
        _output: output,
    })
}

/// The replay of one scan partition.
///
/// Holds the [`MaterializedOutput`], and with it the memory reservations of
/// the buffered batches and of the spill reads, until the replay is dropped.
/// The scan can outlive its plan and the [`TaskContext`] of its run, for
/// example when both are dropped after `execute`, so the output must not be
/// released earlier.
struct ReplayStream {
    stream: BoxStream<'static, Result<RecordBatch>>,
    _output: Arc<MaterializedOutput>,
}

impl Stream for ReplayStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

/// The stream of one scan partition. It is a reader of the output from the
/// call of `execute` until it ends, returns an error, or is dropped.
struct ScanStream {
    /// `None` after the end of the stream. Declared before `in_use`, so that
    /// it drops its reference to the output first.
    stream: Option<BoxStream<'static, Result<RecordBatch>>>,
    in_use: Option<MaterializedCteOutputUse>,
}

impl Stream for ScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };
        let item = futures::ready!(stream.poll_next_unpin(cx));
        if !matches!(item, Some(Ok(_))) {
            // The scan does not read the output again. Do not wait for the
            // consumer to drop the stream.
            self.stream = None;
            self.in_use = None;
        }
        Poll::Ready(item)
    }
}

/// Computes a CTE body once and runs the continuation, whose
/// [`MaterializedCteScanExec`]s read the buffered body output.
///
/// Children: `[body, continuation]`. The output is the output of the
/// continuation.
#[derive(Debug)]
pub struct MaterializedCteExec {
    body: Arc<dyn ExecutionPlan>,
    continuation: Arc<dyn ExecutionPlan>,
    buffer: Arc<MaterializedCteBuffer>,
    cache: Arc<PlanProperties>,
}

impl MaterializedCteExec {
    pub fn new(
        body: Arc<dyn ExecutionPlan>,
        continuation: Arc<dyn ExecutionPlan>,
        buffer: Arc<MaterializedCteBuffer>,
    ) -> Self {
        buffer.set_body(Arc::clone(&body));
        let cache = Arc::clone(continuation.properties());
        Self {
            body,
            continuation,
            buffer,
            cache,
        }
    }

    pub fn buffer(&self) -> &Arc<MaterializedCteBuffer> {
        &self.buffer
    }
}

impl DisplayAs for MaterializedCteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MaterializedCteExec: name={}", self.buffer.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.buffer.name),
        }
    }
}

impl ExecutionPlan for MaterializedCteExec {
    fn name(&self) -> &'static str {
        "MaterializedCteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.body, &self.continuation]
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
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!("MaterializedCteExec takes 2 children");
        }
        let continuation = children.pop().unwrap();
        let body = children.pop().unwrap();
        Ok(Arc::new(Self::new(
            body,
            continuation,
            Arc::clone(&self.buffer),
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true, false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The body runs lazily, when the first scan polls it.
        self.buffer.set_body(Arc::clone(&self.body));
        self.continuation.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.buffer.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::Skip, ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[1]))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    /// The output is the output of the continuation, so parent filters go to
    /// the continuation only. The body is shared by every scan and must not
    /// be filtered for one consumer.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        Ok(FilterDescription::new()
            .with_child(ChildFilterDescription::all_unsupported(&parent_filters))
            .with_child(ChildFilterDescription::from_child(
                &parent_filters,
                &self.continuation,
            )?))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // The body always reports `No`, so a filter is handled when the
        // continuation handles it.
        Ok(FilterPushdownPropagation::if_any(child_pushdown_result))
    }
}

/// Reads the buffered output of a [`MaterializedCteExec`] body.
#[derive(Debug)]
pub struct MaterializedCteScanExec {
    id: u64,
    name: String,
    schema: SchemaRef,
    buffer: Option<Arc<MaterializedCteBuffer>>,
    /// The index of this scan in the scans that are bound to `buffer`.
    scan_index: usize,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl MaterializedCteScanExec {
    /// Create a scan that is not yet bound to a buffer. See [`Self::bind`].
    pub fn new(
        id: u64,
        name: impl Into<String>,
        schema: SchemaRef,
        partitions: usize,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions.max(1)),
            EmissionType::Final,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        Self {
            id,
            name: name.into(),
            schema,
            buffer: None,
            scan_index: 0,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        }
    }

    /// Bind this scan to the buffer of its [`MaterializedCteExec`].
    pub fn bind(&self, buffer: Arc<MaterializedCteBuffer>) -> Self {
        let scan_index = {
            let mut scan_partitions = buffer.scan_partitions.lock();
            scan_partitions.push(self.cache.partitioning.partition_count());
            scan_partitions.len() - 1
        };
        Self {
            id: self.id,
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            buffer: Some(buffer),
            scan_index,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::clone(&self.cache),
        }
    }

    /// The id of the [`MaterializedCteBuffer`] this scan reads.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Whether [`Self::bind`] has been called on this scan.
    pub fn is_bound(&self) -> bool {
        self.buffer.is_some()
    }
}

impl DisplayAs for MaterializedCteScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MaterializedCteScanExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.name),
        }
    }
}

impl ExecutionPlan for MaterializedCteScanExec {
    fn name(&self) -> &'static str {
        "MaterializedCteScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
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
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
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
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(buffer) = &self.buffer else {
            return internal_err!(
                "MaterializedCteScanExec {} is not bound to its CTE",
                self.name
            );
        };
        let (mut output, in_use) =
            buffer.materialize(context, Some((self.scan_index, partition)))?;
        let output_partitions = self.cache.partitioning.partition_count();
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let stream = futures::stream::once(async move {
            let output = std::future::poll_fn(|cx| output.get_shared(cx)).await?;
            replay(output, partition, output_partitions)
        })
        .try_flatten()
        .inspect_ok(move |batch| baseline.record_output(batch.num_rows()));
        let stream = ScanStream {
            stream: Some(stream.boxed()),
            in_use: Some(in_use),
        };
        // The replay of in-memory batches never returns `Pending`, so it must
        // consume the Tokio budget explicitly.
        Ok(Box::pin(cooperative(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The statistics are unknown: a leaf cannot ask the body for its
    /// statistics through [`StatisticsArgs`].
    ///
    /// This is a known limitation, and it can make a plan worse than the
    /// plan of the inlined CTE. For example, `JoinSelection` does not know
    /// that a scan is small, and can put the other, larger input of a hash
    /// join on the build side.
    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::collect;
    use crate::empty::EmptyExec;
    use crate::execution_plan::reset_plan_states;
    use crate::filter_pushdown::PushedDown;
    use crate::test::TestMemoryExec;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::expressions::{col, lit};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn parent_filters_go_to_the_continuation_only() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let body = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let continuation = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let exec = MaterializedCteExec::new(
            body,
            continuation,
            Arc::new(MaterializedCteBuffer::new(0, "c")),
        );
        let filter = Arc::new(datafusion_physical_expr::expressions::BinaryExpr::new(
            col("a", &schema)?,
            datafusion_expr::Operator::Eq,
            lit(1i32),
        )) as Arc<dyn PhysicalExpr>;

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Pre,
            vec![filter],
            &ConfigOptions::default(),
        )?;
        let parent_filters = description.parent_filters();
        assert_eq!(parent_filters.len(), 2);
        assert!(matches!(parent_filters[0][0].discriminant, PushedDown::No));
        assert!(matches!(parent_filters[1][0].discriminant, PushedDown::Yes));
        Ok(())
    }

    /// The in-memory prefix takes all the memory that the pool can grant.
    /// Some of it is then moved to disk, so that the two scans have memory to
    /// decode the spill files, and both scans still return every row in order.
    #[tokio::test]
    async fn spill_replay_memory_is_reserved() -> Result<()> {
        let memory_limit = 20_000;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batches = (0..10)
            .map(|i| {
                let values = Int32Array::from_iter_values(i * 1000..(i + 1) * 1000);
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let body = TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let exec = MaterializedCteExec::new(
            body,
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Arc::clone(&buffer),
        );
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        let (mut once, hold) = buffer.materialize(Arc::clone(&context), None)?;
        let output = std::future::poll_fn(|cx| once.get_shared(cx)).await?;
        let partition = &output.partitions[0];
        assert!(
            partition.spill_files.len() > 1,
            "the prefix was not spilled"
        );
        let spill_paths = spill_paths(&output);
        assert!(partition.max_spilled_batch_size > 0);
        // The pool holds the in-memory prefix and one decoded batch per scan.
        let reserved =
            partition.reservation.size() + 2 * partition.max_spilled_batch_size;
        assert_eq!(context.memory_pool().reserved(), reserved);
        assert!(reserved <= memory_limit);

        // The replay streams keep the reservations after the plan and the
        // context of the run are dropped.
        let mut streams = scans
            .iter()
            .map(|scan| scan.execute(0, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;
        let pool = Arc::clone(context.memory_pool());
        drop((once, output, scans, exec, buffer, context));
        // Once every stream has returned a batch, only the streams hold the
        // buffered output.
        let mut firsts = vec![];
        for stream in &mut streams {
            firsts.push(stream.next().await.transpose()?.unwrap());
        }
        drop(hold);
        assert_eq!(pool.reserved(), reserved);
        assert!(spill_paths.iter().all(|path| path.exists()));

        for (stream, first) in streams.into_iter().zip(firsts) {
            let mut batches = vec![first];
            batches.extend(collect(stream).await?);
            let values: Vec<i32> = batches
                .iter()
                .flat_map(|b| {
                    let array = b.column(0).as_any().downcast_ref::<Int32Array>();
                    array.unwrap().values().to_vec()
                })
                .collect();
            assert_eq!(values, (0..10_000).collect::<Vec<_>>());
        }
        assert_eq!(pool.reserved(), 0);
        assert!(!spill_paths.iter().any(|path| path.exists()));
        Ok(())
    }

    /// The first batch is larger than the batches on disk. To move it to disk
    /// whole would raise the memory that each scan needs to decode a batch
    /// above the memory limit, so it is written in pieces.
    #[tokio::test]
    async fn spill_of_large_in_memory_batch_does_not_raise_replay_memory() -> Result<()> {
        let memory_limit = 17_500;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = |range: std::ops::Range<i32>| {
            let values = Int32Array::from_iter_values(range);
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
        };
        let mut batches = vec![batch(0..4000)?];
        for i in 0..10 {
            batches.push(batch(4000 + i * 250..4000 + (i + 1) * 250)?);
        }
        let small_batch_size = batches[1].get_array_memory_size();
        let body = TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let _exec = MaterializedCteExec::new(
            body,
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Arc::clone(&buffer),
        );
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        // The hold keeps the output between the two scans.
        let (mut once, _hold) = buffer.materialize(Arc::clone(&context), None)?;
        let output = std::future::poll_fn(|cx| once.get_shared(cx)).await?;
        let partition = &output.partitions[0];
        assert!(partition.batches.is_empty(), "the large batch is in memory");
        assert!(partition.max_spilled_batch_size <= small_batch_size);
        assert!(context.memory_pool().reserved() <= memory_limit);

        for scan in &scans {
            let batches = collect(scan.execute(0, Arc::clone(&context))?).await?;
            let values: Vec<i32> = batches
                .iter()
                .flat_map(|b| {
                    let array = b.column(0).as_any().downcast_ref::<Int32Array>();
                    array.unwrap().values().to_vec()
                })
                .collect();
            assert_eq!(values, (0..6500).collect::<Vec<_>>());
        }
        Ok(())
    }

    /// Counts the `execute` calls of `input`.
    #[derive(Debug)]
    struct CountingExec {
        input: Arc<dyn ExecutionPlan>,
        executions: Arc<AtomicUsize>,
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "CountingExec")
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                input: children.swap_remove(0),
                executions: Arc::clone(&self.executions),
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.executions.fetch_add(1, Ordering::Relaxed);
            self.input.execute(partition, context)
        }
    }

    /// A plan `MaterializedCteExec(body, scan)` whose body has the rows
    /// `0..rows` in batches of one row, and the count of body executions.
    fn counting_plan(rows: i32) -> Result<(Arc<dyn ExecutionPlan>, Arc<AtomicUsize>)> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batches = (0..rows)
            .map(|i| {
                let values = Int32Array::from(vec![i]);
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let executions = Arc::new(AtomicUsize::new(0));
        let body = Arc::new(CountingExec {
            input: TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?,
            executions: Arc::clone(&executions),
        });
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", schema, 1);
        let scan = Arc::new(scan.bind(Arc::clone(&buffer)));
        let plan = Arc::new(MaterializedCteExec::new(body, scan, buffer));
        Ok((plan, executions))
    }

    /// The paths of the spill files of `output`.
    fn spill_paths(output: &MaterializedOutput) -> Vec<std::path::PathBuf> {
        output
            .partitions
            .iter()
            .flat_map(|partition| &partition.spill_files)
            .map(|file| file.path().expect("a local file").to_path_buf())
            .collect()
    }

    fn values(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                let array = b.column(0).as_any().downcast_ref::<Int32Array>();
                array.unwrap().values().to_vec()
            })
            .collect()
    }

    /// Two runs of one plan overlap. Each run has its own `TaskContext`, so
    /// each run computes the body and holds its own output.
    #[tokio::test]
    async fn overlapping_runs_do_not_share_the_output() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let context_1 = Arc::new(TaskContext::default());
        let context_2 = Arc::new(TaskContext::default());

        let mut run_1 = plan.execute(0, Arc::clone(&context_1))?;
        let first = run_1.next().await.transpose()?.unwrap();
        assert_eq!(executions.load(Ordering::Relaxed), 1);
        assert!(context_1.memory_pool().reserved() > 0);
        assert_eq!(context_2.memory_pool().reserved(), 0);

        // The second run starts and completes while the first run is open.
        let run_2 = collect(plan.execute(0, Arc::clone(&context_2))?).await?;
        assert_eq!(values(&run_2), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        // The end of the second run releases its output only.
        assert_eq!(context_2.memory_pool().reserved(), 0);
        assert!(context_1.memory_pool().reserved() > 0);

        // One more scan in the first run, while that run is open, replays
        // the output of that run.
        let again = collect(plan.execute(0, Arc::clone(&context_1))?).await?;
        assert_eq!(values(&again), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        assert!(context_1.memory_pool().reserved() > 0);

        let mut run_1_batches = vec![first];
        run_1_batches.extend(collect(run_1).await?);
        assert_eq!(values(&run_1_batches), vec![0, 1, 2]);
        assert_eq!(context_1.memory_pool().reserved(), 0);
        Ok(())
    }

    /// `reset_state` returns a new plan and does not change the old plan or
    /// the output of a run.
    #[tokio::test]
    async fn reset_state_leaves_the_old_plan_usable() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let context = Arc::new(TaskContext::default());

        let mut stream = plan.execute(0, Arc::clone(&context))?;
        let first = stream.next().await.transpose()?.unwrap();
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        let reset = reset_plan_states(Arc::clone(&plan))?;
        assert!(!Arc::ptr_eq(&plan, &reset));

        // A scan of the old plan that starts after the reset, in the run
        // that is open, replays the output of that run.
        let old = collect(plan.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&old), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        // The new plan in the same run also replays that output.
        let new = collect(reset.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&new), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        let mut batches = vec![first];
        batches.extend(collect(stream).await?);
        assert_eq!(values(&batches), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        // The new plan in a new run computes the body again.
        let new = collect(reset.execute(0, Arc::new(TaskContext::default()))?).await?;
        assert_eq!(values(&new), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        Ok(())
    }

    /// A context from `with_new_materialized_cte_outputs` computes the given
    /// CTEs again and keeps the output of the other CTEs.
    #[tokio::test]
    async fn new_outputs_are_only_for_the_given_ctes() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let context = Arc::new(TaskContext::default());
        // A stream that is open keeps the output of its context.
        let _open = plan.execute(0, Arc::clone(&context))?;
        collect(plan.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        // The CTE has the id 0, so it keeps its output.
        let other = with_new_materialized_cte_outputs(&context, &HashSet::from([1]));
        collect(plan.execute(0, Arc::new(other))?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        let ids = materialized_cte_ids(&plan);
        assert_eq!(ids, HashSet::from([0]));
        let renewed = Arc::new(with_new_materialized_cte_outputs(&context, &ids));
        let _open_renewed = plan.execute(0, Arc::clone(&renewed))?;
        collect(plan.execute(0, Arc::clone(&renewed))?).await?;
        collect(plan.execute(0, Arc::clone(&renewed))?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 2);

        // A nested context reads the CTEs that it does not renew from the
        // enclosing context.
        let nested = with_new_materialized_cte_outputs(&renewed, &HashSet::new());
        collect(plan.execute(0, Arc::new(nested))?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 2);

        // The first context still has its output.
        collect(plan.execute(0, context)?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        Ok(())
    }

    /// A plan with a body of two partitions (the rows `0..rows` in batches of
    /// 100 rows, one half in each partition), and two scans of two partitions
    /// each. Also returns the count of body executions.
    #[expect(clippy::type_complexity)]
    fn two_scans_plan(
        rows: i32,
    ) -> Result<(
        Arc<MaterializedCteExec>,
        [Arc<MaterializedCteScanExec>; 2],
        Arc<AtomicUsize>,
    )> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let partition = |range: std::ops::Range<i32>| {
            range
                .step_by(100)
                .map(|i| {
                    let values = Int32Array::from_iter_values(i..i + 100);
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        };
        let partitions = [partition(0..rows / 2)?, partition(rows / 2..rows)?];
        let executions = Arc::new(AtomicUsize::new(0));
        let body = Arc::new(CountingExec {
            input: TestMemoryExec::try_new_exec(&partitions, Arc::clone(&schema), None)?,
            executions: Arc::clone(&executions),
        });
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 2);
        let scans = [
            Arc::new(scan.bind(Arc::clone(&buffer))),
            Arc::new(scan.bind(Arc::clone(&buffer))),
        ];
        let exec = Arc::new(MaterializedCteExec::new(
            body,
            Arc::new(EmptyExec::new(schema)),
            buffer,
        ));
        Ok((exec, scans, executions))
    }

    /// The memory reservations and the spill files are released when the
    /// last partition of the last scan finishes. The plan, the context of the
    /// run and the finished streams are still alive.
    #[tokio::test]
    async fn output_is_released_when_the_last_scan_partition_finishes() -> Result<()> {
        let (exec, scans, executions) = two_scans_plan(10_000)?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(20_000, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let pool = Arc::clone(context.memory_pool());

        // Read the first three scan partitions one after the other. The
        // output is not released between them, because the other partitions
        // can still be executed.
        let mut paths = vec![];
        for (scan, partition) in [(0, 0), (1, 1), (0, 1)] {
            let mut stream = scans[scan].execute(partition, Arc::clone(&context))?;
            let mut batches = vec![];
            while let Some(batch) = stream.next().await.transpose()? {
                batches.push(batch);
            }
            let start = partition as i32 * 5000;
            assert_eq!(values(&batches), (start..start + 5000).collect::<Vec<_>>());
            // The stream has ended but is not dropped.
            assert!(pool.reserved() > 0);
            if paths.is_empty() {
                let (mut once, _hold) =
                    exec.buffer.materialize(Arc::clone(&context), None)?;
                let output = std::future::poll_fn(|cx| once.get_shared(cx)).await?;
                paths = spill_paths(&output);
                assert!(!paths.is_empty(), "the body did not spill");
            }
            assert!(paths.iter().all(|path| path.exists()));
        }

        let mut last = scans[1].execute(0, Arc::clone(&context))?;
        let mut batches = vec![];
        while let Some(batch) = last.next().await.transpose()? {
            // The last reader is open.
            assert!(pool.reserved() > 0);
            batches.push(batch);
        }
        assert_eq!(values(&batches), (0..5000).collect::<Vec<_>>());
        // The stream has ended. The stream, the plan and the context of the
        // run are alive.
        assert_eq!(pool.reserved(), 0);
        assert!(!paths.iter().any(|path| path.exists()));
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        drop((last, exec, context));
        Ok(())
    }

    /// A scan stream that is dropped before it is polled, or before its end,
    /// does not keep the output.
    #[tokio::test]
    async fn output_is_released_when_scan_streams_are_dropped_early() -> Result<()> {
        let (_exec, scans, executions) = two_scans_plan(1000)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());

        // Not polled.
        let unpolled = scans[0].execute(0, Arc::clone(&context))?;
        // Dropped after one batch, as below a `LIMIT`.
        let mut limited = scans[0].execute(1, Arc::clone(&context))?;
        limited.next().await.transpose()?.unwrap();
        let mut partial = scans[1].execute(0, Arc::clone(&context))?;
        partial.next().await.transpose()?.unwrap();
        let full = collect(scans[1].execute(1, Arc::clone(&context))?).await?;
        assert_eq!(values(&full), (500..1000).collect::<Vec<_>>());

        // Each scan partition was executed, and three streams are open.
        assert!(pool.reserved() > 0);
        drop(unpolled);
        drop(limited);
        assert!(pool.reserved() > 0);
        drop(partial);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        Ok(())
    }

    /// When every scan stream is dropped before the body completes, the body
    /// is cancelled and its memory is released.
    #[tokio::test]
    async fn body_is_cancelled_when_all_scan_streams_are_dropped() -> Result<()> {
        let (_exec, scans, _) = two_scans_plan(1000)?;
        let context = Arc::new(TaskContext::default());
        let streams = [(0, 0), (0, 1), (1, 0), (1, 1)].map(|(scan, partition)| {
            scans[scan].execute(partition, Arc::clone(&context))
        });
        drop(streams);
        let outputs = context.run_state().get_or_init::<MaterializedCteOutputs>();
        assert!(outputs.outputs.lock().is_empty());
        assert_eq!(context.memory_pool().reserved(), 0);
        Ok(())
    }

    /// A scan partition that is never executed keeps the output until the
    /// context of the run is dropped. The other scans read correct rows.
    #[tokio::test]
    async fn partition_that_is_not_executed_keeps_the_output() -> Result<()> {
        let (_exec, scans, executions) = two_scans_plan(1000)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());

        for (scan, partition) in [(0, 0), (0, 1), (1, 0)] {
            let batches =
                collect(scans[scan].execute(partition, Arc::clone(&context))?).await?;
            let start = partition as i32 * 500;
            assert_eq!(values(&batches), (start..start + 500).collect::<Vec<_>>());
        }
        // Partition 1 of the second scan can still be executed.
        assert!(pool.reserved() > 0);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        drop(context);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A scan partition that is executed after the release of the output
    /// computes the body again. It does not read an empty output.
    #[tokio::test]
    async fn scan_after_the_release_computes_the_body_again() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());

        let first = collect(plan.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&first), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 1);
        assert_eq!(pool.reserved(), 0);

        // The same scan partition, in the same run.
        let mut late = plan.execute(0, Arc::clone(&context))?;
        let mut batches = vec![late.next().await.transpose()?.unwrap()];
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        assert!(pool.reserved() > 0);
        batches.extend(collect(late).await?);
        assert_eq!(values(&batches), vec![0, 1, 2]);
        // The new output is released in the same way.
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A hold keeps the output for scans that are executed again, as the
    /// recursive term of a recursive query is.
    #[tokio::test]
    async fn hold_keeps_the_output_between_executions() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let cte = plan.downcast_ref::<MaterializedCteExec>().unwrap();
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());

        // The continuation reads the CTE and does not declare it.
        let holds =
            hold_materialized_cte_outputs(&cte.continuation, &HashSet::new(), &context);
        assert_eq!(holds.len(), 1);
        // The plan declares the CTE, so there is nothing to hold.
        let ids = materialized_cte_ids(&plan);
        assert!(hold_materialized_cte_outputs(&plan, &ids, &context).is_empty());

        for _ in 0..3 {
            let batches = collect(plan.execute(0, Arc::clone(&context))?).await?;
            assert_eq!(values(&batches), vec![0, 1, 2]);
            assert_eq!(executions.load(Ordering::Relaxed), 1);
            assert!(pool.reserved() > 0);
        }
        drop(holds);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }
}
