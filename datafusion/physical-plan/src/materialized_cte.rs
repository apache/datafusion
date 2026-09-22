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
//! files until it can.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, Statistics, internal_err};
use datafusion_common_runtime::JoinSet;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use futures::{StreamExt, TryStreamExt};
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

impl BufferedPartition {
    /// Move in-memory batches from the end of `batches` to a new spill file,
    /// until at least `bytes` are released or no in-memory batch is left.
    /// The new file is read before the existing spill files, so the order of
    /// the partition is kept.
    fn spill_in_memory_suffix(
        &mut self,
        bytes: usize,
        spill_manager: &SpillManager,
    ) -> Result<()> {
        let mut split = self.batches.len();
        let mut released = 0;
        while split > 0 && released < bytes {
            split -= 1;
            released += self.batches[split].get_array_memory_size();
        }
        let suffix = self.batches.split_off(split);
        let mut file = spill_manager.create_in_progress_file("MaterializedCte")?;
        for batch in &suffix {
            let size = file.append_batch(batch)?;
            self.max_spilled_batch_size = self.max_spilled_batch_size.max(size);
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

/// The memory that the scans need to read the spill files of `partitions`.
///
/// Partition `p` of a scan with `n` partitions reads the body partitions
/// `p, p + n, ...` one after the other, with at most one decoded batch in
/// memory. All scan partitions can read at the same time.
fn replay_memory(partitions: &[BufferedPartition], scan_partitions: &[usize]) -> usize {
    scan_partitions
        .iter()
        .map(|&n| {
            (0..n)
                .map(|p| {
                    partitions
                        .iter()
                        .skip(p)
                        .step_by(n)
                        .map(|b| b.max_spilled_batch_size)
                        .max()
                        .unwrap_or(0)
                })
                .sum::<usize>()
        })
        .sum()
}

/// Grow `reservation` to the memory the scans need to replay `partitions`.
/// If the pool cannot grant it, move in-memory batches to spill files, from
/// the partition that holds the most memory first, until it can.
fn reserve_replay_memory(
    partitions: &mut [BufferedPartition],
    scan_partitions: &[usize],
    spill_manager: &SpillManager,
    reservation: &mut MemoryReservation,
) -> Result<()> {
    loop {
        let required = replay_memory(partitions, scan_partitions);
        let Err(e) = reservation.try_resize(required) else {
            return Ok(());
        };
        let Some(partition) = partitions
            .iter_mut()
            .filter(|p| !p.batches.is_empty())
            .max_by_key(|p| p.reservation.size())
        else {
            return Err(e);
        };
        partition.spill_in_memory_suffix(
            required.saturating_sub(reservation.size()),
            spill_manager,
        )?;
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
    output: &MaterializedOutput,
    partition: usize,
    output_partitions: usize,
) -> Result<Vec<SendableRecordBatchStream>> {
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
    Ok(streams)
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
            let streams = replay(&output, partition, output_partitions)?;
            Ok::<_, datafusion_common::DataFusionError>(
                futures::stream::iter(streams).flatten(),
            )
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

        let mut output = buffer.materialize(Arc::clone(&context))?;
        let output = std::future::poll_fn(|cx| output.get_shared(cx)).await?;
        let partition = &output.partitions[0];
        assert!(
            partition.spill_files.len() > 1,
            "the prefix was not spilled"
        );
        assert!(partition.max_spilled_batch_size > 0);
        // The pool holds the in-memory prefix and one decoded batch per scan.
        assert_eq!(
            context.memory_pool().reserved(),
            partition.reservation.size() + 2 * partition.max_spilled_batch_size
        );
        assert!(context.memory_pool().reserved() <= memory_limit);

        for scan in scans {
            let batches = collect(scan.execute(0, Arc::clone(&context))?).await?;
            let values: Vec<i32> = batches
                .iter()
                .flat_map(|b| {
                    let array = b.column(0).as_any().downcast_ref::<Int32Array>();
                    array.unwrap().values().to_vec()
                })
                .collect();
            assert_eq!(values, (0..10_000).collect::<Vec<_>>());
        }
        Ok(())
    }
}
