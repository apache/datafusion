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

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, Statistics, internal_err};
use datafusion_common_runtime::JoinSet;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, SpillFile, TaskContext};
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
    /// Holds `batches`. Released when the buffer is dropped.
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
    /// Released when the buffer is dropped.
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

/// State shared by one [`MaterializedCteExec`] and all its
/// [`MaterializedCteScanExec`]s.
pub struct MaterializedCteBuffer {
    id: u64,
    name: String,
    /// The body to run. [`MaterializedCteExec`] updates it every time it is
    /// rebuilt (for example by a physical optimizer rule), so it is the body of
    /// the final plan. A scan can run the body before the owning
    /// `MaterializedCteExec` executes, for example from a scalar subquery.
    body: Mutex<Option<Arc<dyn ExecutionPlan>>>,
    output: Mutex<Arc<OnceAsync<MaterializedOutput>>>,
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
            output: Mutex::new(Arc::default()),
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

    fn reset(&self) {
        *self.output.lock() = Arc::default();
    }

    /// Return a future that resolves once the body has been fully buffered.
    /// The body runs at most once, whichever consumer asks first.
    fn materialize(
        self: &Arc<Self>,
        context: Arc<TaskContext>,
    ) -> Result<OnceFut<MaterializedOutput>> {
        let Some(body) = self.body.lock().clone() else {
            return internal_err!("MaterializedCte {} has no body", self.name);
        };
        let once = Arc::clone(&self.output.lock());
        let name = self.name.clone();
        let scan_partitions = self.scan_partitions.lock().clone();
        let metrics = self.metrics.clone();
        once.try_once(move || {
            Ok(buffer_body(name, body, scan_partitions, context, metrics))
        })
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
/// The scan can outlive its plan, for example when the plan is dropped after
/// `execute`, so the output must not be released earlier.
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

    /// Clears the shared buffer in place and returns `self`.
    ///
    /// This is an exception to the `reset_state` contract, which expects a
    /// new node and leaves the old node unchanged. `RecursiveQueryExec` resets
    /// the recursive term before each iteration, and a `MATERIALIZED` CTE in
    /// that term must be computed again. A new buffer would also need new
    /// scans, and a scan in a scalar subquery is not below this node, so this
    /// node cannot rebind it. `ScalarSubqueryExec::reset_state` does the same.
    ///
    /// Two overlapping runs of one plan share this buffer. Nothing in
    /// DataFusion does that today. See
    /// <https://github.com/apache/datafusion/issues/26229>.
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        self.buffer.reset();
        Ok(self)
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
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        }
    }

    /// Bind this scan to the buffer of its [`MaterializedCteExec`].
    pub fn bind(&self, buffer: Arc<MaterializedCteBuffer>) -> Self {
        buffer
            .scan_partitions
            .lock()
            .push(self.cache.partitioning.partition_count());
        Self {
            id: self.id,
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            buffer: Some(buffer),
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
        let mut output = buffer.materialize(context)?;
        let output_partitions = self.cache.partitioning.partition_count();
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let stream = futures::stream::once(async move {
            let output = std::future::poll_fn(|cx| output.get_shared(cx)).await?;
            replay(output, partition, output_partitions)
        })
        .try_flatten()
        .inspect_ok(move |batch| baseline.record_output(batch.num_rows()));
        // The replay of in-memory batches never returns `Pending`, so it must
        // consume the Tokio budget explicitly.
        Ok(Box::pin(cooperative(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            Box::pin(stream),
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
    use crate::filter_pushdown::PushedDown;
    use crate::test::TestMemoryExec;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::expressions::{col, lit};

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

        let mut once = buffer.materialize(Arc::clone(&context))?;
        let output = std::future::poll_fn(|cx| once.get_shared(cx)).await?;
        let partition = &output.partitions[0];
        assert!(
            partition.spill_files.len() > 1,
            "the prefix was not spilled"
        );
        assert!(partition.max_spilled_batch_size > 0);
        // The pool holds the in-memory prefix and one decoded batch per scan.
        let reserved =
            partition.reservation.size() + 2 * partition.max_spilled_batch_size;
        assert_eq!(context.memory_pool().reserved(), reserved);
        assert!(reserved <= memory_limit);

        // The replay streams keep the reservations after the plan is dropped.
        let mut streams = scans
            .iter()
            .map(|scan| scan.execute(0, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;
        drop((once, output, scans, exec, buffer));
        // Once every stream has returned a batch, only the streams hold the
        // buffered output.
        let mut firsts = vec![];
        for stream in &mut streams {
            firsts.push(stream.next().await.transpose()?.unwrap());
        }
        assert_eq!(context.memory_pool().reserved(), reserved);

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
        assert_eq!(context.memory_pool().reserved(), 0);
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

        let mut once = buffer.materialize(Arc::clone(&context))?;
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
}
