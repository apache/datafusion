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
//! read the in-memory prefix and then the spill file, so row order within a
//! partition is kept.

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
/// an optional spill file with the remaining batches.
struct BufferedPartition {
    batches: Vec<RecordBatch>,
    spill_file: Option<Arc<dyn SpillFile>>,
    /// Released when the buffer is dropped.
    _reservation: MemoryReservation,
}

/// The materialized output of a CTE body.
struct MaterializedOutput {
    partitions: Vec<BufferedPartition>,
    spill_manager: SpillManager,
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
        let metrics = self.metrics.clone();
        once.try_once(move || Ok(buffer_body(name, body, context, metrics)))
    }
}

async fn buffer_body(
    name: String,
    body: Arc<dyn ExecutionPlan>,
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
    Ok(MaterializedOutput {
        partitions: partitions.into_iter().map(|(_, p)| p).collect(),
        spill_manager,
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
            file.append_batch(&batch)?;
        }
    }
    let spill_file = match spill {
        Some(mut file) => file.finish()?,
        None => None,
    };
    Ok(BufferedPartition {
        batches,
        spill_file,
        _reservation: reservation,
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
        if let Some(file) = &buffered.spill_file {
            streams.push(
                output
                    .spill_manager
                    .read_spill_as_stream(Arc::clone(file), None)?,
            );
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
    use crate::empty::EmptyExec;
    use crate::filter_pushdown::PushedDown;
    use arrow::datatypes::{DataType, Field, Schema};
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
}
