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

//! [`NestedLoopJoinExec`]: joins without equijoin (equality predicates).

use std::fmt::Formatter;
use std::ops::{BitOr, ControlFlow};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use super::utils::{
    asymmetric_join_output_partitioning, need_produce_result_in_final,
    reorder_output_after_swap, swap_join_projection,
};
use crate::common::can_project;
use crate::execution_plan::{EmissionType, boundedness_from_children};
use crate::joins::SharedBitmapBuilder;
use crate::joins::utils::{
    BuildProbeJoinMetrics, ColumnIndex, JoinFilter, OnceAsync, OnceFut,
    build_join_schema, check_join_is_valid, estimate_join_statistics,
    need_produce_right_in_final,
};
use crate::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricType, MetricsSet, RatioMetrics,
    Time,
};
use crate::projection::{
    EmbeddedProjection, JoinData, ProjectionExec, try_embed_projection,
    try_pushdown_through_join_with_column_indices,
};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties, RecordBatchStream, ReplaceChildrenOptions,
    SendableRecordBatchStream, validate_child_count,
};

use arrow::array::{
    Array, BooleanArray, BooleanBufferBuilder, RecordBatchOptions, UInt32Array,
    UInt64Array, new_null_array,
};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{
    BatchCoalescer, concat_batches, filter, filter_record_batch, not, take,
};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    JoinSide, NullEquality, Result, ScalarValue, Statistics, arrow_err,
    assert_eq_or_internal_err, exec_err, internal_datafusion_err, internal_err,
    project_schema, unwrap_or_internal_err,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SpillFile, TaskContext};
use datafusion_expr::JoinType;
use datafusion_physical_expr::equivalence::{
    ProjectionMapping, join_equivalence_properties,
};

use datafusion_physical_expr::projection::{ProjectionRef, combine_projections};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use log::debug;
use parking_lot::Mutex;

use crate::metrics::SpillMetrics;
use crate::spill::replayable_spill_input::ReplayableStreamSource;
use crate::spill::spill_manager::SpillManager;

#[expect(rustdoc::private_intra_doc_links)]
/// NestedLoopJoinExec is a build-probe join operator designed for joins that
/// do not have equijoin keys in their `ON` clause.
///
/// # Execution Flow
///
/// ```text
///                                                Incoming right batch
///                Left Side Buffered Batches
///                       ┌───────────┐              ┌───────────────┐
///                       │ ┌───────┐ │              │               │
///                       │ │       │ │              │               │
///  Current Left Row ───▶│ ├───────├─┤──────────┐   │               │
///                       │ │       │ │          │   └───────────────┘
///                       │ │       │ │          │           │
///                       │ │       │ │          │           │
///                       │ └───────┘ │          │           │
///                       │ ┌───────┐ │          │           │
///                       │ │       │ │          │     ┌─────┘
///                       │ │       │ │          │     │
///                       │ │       │ │          │     │
///                       │ │       │ │          │     │
///                       │ │       │ │          │     │
///                       │ └───────┘ │          ▼     ▼
///                       │   ......  │  ┌──────────────────────┐
///                       │           │  │X (Cartesian Product) │
///                       │           │  └──────────┬───────────┘
///                       └───────────┘             │
///                                                 │
///                                                 ▼
///                                      ┌───────┬───────────────┐
///                                      │       │               │
///                                      │       │               │
///                                      │       │               │
///                                      └───────┴───────────────┘
///                                        Intermediate Batch
///                                  (For join predicate evaluation)
/// ```
///
/// The execution follows a two-phase design:
///
/// ## 1. Buffering Left Input
/// - The operator eagerly buffers all left-side input batches into memory,
///   util a memory limit is reached.
///   Currently, an out-of-memory error will be thrown if all the left-side input batches
///   cannot fit into memory at once.
///   In the future, it's possible to make this case finish execution. (see
///   'Memory-limited Execution' section)
/// - The rationale for buffering the left side is that scanning the right side
///   can be expensive (e.g., decoding Parquet files), so buffering more left
///   rows reduces the number of right-side scan passes required.
///
/// ## 2. Probing Right Input
/// - Right-side input is streamed batch by batch.
/// - For each right-side batch:
///   - It evaluates the join filter against the full buffered left input.
///     This results in a Cartesian product between the right batch and each
///     left row -- with the join predicate/filter applied -- for each inner
///     loop iteration.
///   - Matched results are accumulated into an output buffer. (see more in
///     `Output Buffering Strategy` section)
/// - This process continues until all right-side input is consumed.
///
/// # Producing unmatched build-side data
/// - For special join types like left/full joins, it's required to also output
///   unmatched pairs. During execution, bitmaps are kept for both left and right
///   sides of the input; they'll be handled by dedicated states in `NLJStream`.
/// - The final output of the left side unmatched rows is handled by a single
///   partition for simplicity, since it only counts a small portion of the
///   execution time. (e.g. if probe side has 10k rows, the final output of
///   unmatched build side only roughly counts for 1/10k of the total time)
///
/// # Output Buffering Strategy
/// The operator uses an intermediate output buffer to accumulate results. Once
/// the output threshold is reached (currently set to the same value as
/// `batch_size` in the configuration), the results will be eagerly output.
///
/// # Extra Notes
/// - The operator always considers the **left** side as the build (buffered) side.
///   Therefore, the physical optimizer should assign the smaller input to the left.
/// - The design try to minimize the intermediate data size to approximately
///   1 batch, for better cache locality and memory efficiency.
///
/// # Memory-limited Execution
/// When the memory budget is exceeded during left-side buffering, the operator
/// falls back to a multi-pass strategy:
/// 1. Buffer as many left rows as fit in memory (one "chunk")
/// 2. On the first pass, the right side is both processed and spilled to disk
/// 3. For each subsequent left chunk, the right side is re-read from the spill file
///
/// The fallback is triggered automatically when the initial in-memory load
/// fails with `ResourcesExhausted` and disk spilling is available. Each
/// output partition independently re-executes the left child and manages
/// its own spill state.
///
/// All join types are supported. For RIGHT/FULL/RIGHT SEMI/RIGHT ANTI/
/// RIGHT MARK joins, a global right-side bitmap (indexed by right batch
/// sequence number) accumulates matches across all left chunks. After the
/// last left chunk is processed, the right side is replayed one more time
/// to emit unmatched right rows using the accumulated bitmap.
///
/// Tracking issue: <https://github.com/apache/datafusion/issues/15760>
///
/// # Clone / Shared State
/// Note this structure includes a [`OnceAsync`] that is used to coordinate the
/// loading of the left side with the processing in each output stream.
/// Therefore it can not be [`Clone`]
#[derive(Debug)]
pub struct NestedLoopJoinExec {
    /// left side
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// right side
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Filters which are applied while finding matching rows
    pub(crate) filter: Option<JoinFilter>,
    /// How the join is performed
    pub(crate) join_type: JoinType,
    /// The full concatenated schema of left and right children should be distinct from
    /// the output schema of the operator
    join_schema: SchemaRef,
    /// Future that consumes left input and buffers it in memory
    ///
    /// This structure is *shared* across all output streams.
    ///
    /// Each output stream waits on the `OnceAsync` to signal the completion of
    /// the build(left) side data, and buffer them all for later joining.
    build_side_data: OnceAsync<LeftLoad>,
    /// Coordinator that, in the memory-limited fallback path, shares
    /// per-chunk `JoinLeftData` (visited bitmap + probe-thread counter)
    /// across all right-side output partitions. This makes the fallback
    /// path's left-side tracking consistent with the single-pass path
    /// (where `collect_left_input(..., probe_threads_count)` initializes
    /// the counter to `right_partition_count`).
    fallback_coordinator: Arc<FallbackCoordinator>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Projection to apply to the output of the join
    projection: Option<ProjectionRef>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
}

/// Helps to build [`NestedLoopJoinExec`].
pub struct NestedLoopJoinExecBuilder {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    filter: Option<JoinFilter>,
    projection: Option<ProjectionRef>,
}

impl NestedLoopJoinExecBuilder {
    /// Make a new [`NestedLoopJoinExecBuilder`].
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            filter: None,
            projection: None,
        }
    }

    /// Set projection from the vector.
    pub fn with_projection(self, projection: Option<Vec<usize>>) -> Self {
        self.with_projection_ref(projection.map(Into::into))
    }

    /// Set projection from the shared reference.
    pub fn with_projection_ref(mut self, projection: Option<ProjectionRef>) -> Self {
        self.projection = projection;
        self
    }

    /// Set optional filter.
    pub fn with_filter(mut self, filter: Option<JoinFilter>) -> Self {
        self.filter = filter;
        self
    }

    /// Build resulting execution plan.
    pub fn build(self) -> Result<NestedLoopJoinExec> {
        let Self {
            left,
            right,
            join_type,
            filter,
            projection,
        } = self;

        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, &join_type);
        let join_schema = Arc::new(join_schema);
        let cache = NestedLoopJoinExec::compute_properties(
            &left,
            &right,
            &join_schema,
            join_type,
            projection.as_deref(),
        )?;
        let right_partition_count = right.output_partitioning().partition_count().max(1);
        let with_visited_bitmap = need_produce_result_in_final(join_type);
        Ok(NestedLoopJoinExec {
            left,
            right,
            filter,
            join_type,
            join_schema,
            build_side_data: Default::default(),
            fallback_coordinator: Arc::new(FallbackCoordinator::new(
                right_partition_count,
                with_visited_bitmap,
            )),
            column_indices,
            projection,
            metrics: Default::default(),
            cache: Arc::new(cache),
        })
    }
}

impl From<&NestedLoopJoinExec> for NestedLoopJoinExecBuilder {
    fn from(exec: &NestedLoopJoinExec) -> Self {
        Self {
            left: Arc::clone(exec.left()),
            right: Arc::clone(exec.right()),
            join_type: exec.join_type,
            filter: exec.filter.clone(),
            projection: exec.projection.clone(),
        }
    }
}

impl NestedLoopJoinExec {
    /// Try to create a new [`NestedLoopJoinExec`]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        NestedLoopJoinExecBuilder::new(left, right, *join_type)
            .with_projection(projection)
            .with_filter(filter)
            .build()
    }

    /// left side
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right side
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Filters applied before join output
    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    pub fn projection(&self) -> &Option<ProjectionRef> {
        &self.projection
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        join_type: JoinType,
        projection: Option<&[usize]>,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let mut eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            Arc::clone(schema),
            &Self::maintains_input_order(join_type),
            None,
            // No on columns in nested loop join
            &[],
        )?;

        let mut output_partitioning =
            asymmetric_join_output_partitioning(left, right, &join_type)?;

        let emission_type = if left.boundedness().is_unbounded() {
            EmissionType::Final
        } else if right.pipeline_behavior() == EmissionType::Incremental {
            match join_type {
                // If we only need to generate matched rows from the probe side,
                // we can emit rows incrementally.
                JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::Right
                | JoinType::RightAnti
                | JoinType::RightMark => EmissionType::Incremental,
                // If we need to generate unmatched rows from the *build side*,
                // we need to emit them at the end.
                JoinType::Left
                | JoinType::LeftAnti
                | JoinType::LeftMark
                | JoinType::Full => EmissionType::Both,
            }
        } else {
            right.pipeline_behavior()
        };

        if let Some(projection) = projection {
            // construct a map from the input expressions to the output expression of the Projection
            let projection_mapping = ProjectionMapping::from_indices(projection, schema)?;
            let out_schema = project_schema(schema, Some(&projection))?;
            output_partitioning =
                output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            emission_type,
            boundedness_from_children([left, right]),
        ))
    }

    /// This join implementation does not preserve the input order of either side.
    fn maintains_input_order(_join_type: JoinType) -> Vec<bool> {
        vec![false, false]
    }

    pub fn contains_projection(&self) -> bool {
        self.projection.is_some()
    }

    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        let projection = projection.map(Into::into);
        // check if the projection is valid
        can_project(&self.schema(), projection.as_deref())?;
        let projection =
            combine_projections(projection.as_ref(), self.projection.as_ref())?;
        NestedLoopJoinExecBuilder::from(self)
            .with_projection_ref(projection)
            .build()
    }

    /// Returns a new `ExecutionPlan` that runs NestedLoopsJoins with the left
    /// and right inputs swapped.
    ///
    /// # Notes:
    ///
    /// This function should be called BEFORE inserting any repartitioning
    /// operators on the join's children. Check [`super::HashJoinExec::swap_inputs`]
    /// for more details.
    pub fn swap_inputs(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let left = self.left();
        let right = self.right();
        let new_join = NestedLoopJoinExecBuilder::new(
            Arc::clone(right),
            Arc::clone(left),
            self.join_type().swap(),
        )
        .with_filter(self.filter().map(JoinFilter::swap))
        .with_projection(swap_join_projection(
            left.schema().fields().len(),
            right.schema().fields().len(),
            self.projection.as_deref(),
            self.join_type(),
        ))
        .build()?;

        // For Semi/Anti joins, swap result will produce same output schema,
        // no need to wrap them into additional projection
        let plan: Arc<dyn ExecutionPlan> = if matches!(
            self.join_type(),
            JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::LeftMark
                | JoinType::RightMark
        ) || self.projection.is_some()
        {
            Arc::new(new_join)
        } else {
            reorder_output_after_swap(
                Arc::new(new_join),
                &self.left().schema(),
                &self.right().schema(),
            )?
        };

        Ok(plan)
    }
}

impl DisplayAs for NestedLoopJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                let display_projections = if self.contains_projection() {
                    format!(
                        ", projection=[{}]",
                        self.projection
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|index| format!(
                                "{}@{}",
                                self.join_schema.fields().get(*index).unwrap().name(),
                                index
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_string()
                };
                write!(
                    f,
                    "NestedLoopJoinExec: join_type={:?}{}{}",
                    self.join_type, display_filter, display_projections
                )
            }
            DisplayFormatType::TreeRender => {
                if *self.join_type() != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl ExecutionPlan for NestedLoopJoinExec {
    fn name(&self) -> &'static str {
        "NestedLoopJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> crate::InputDistributionRequirements {
        crate::InputDistributionRequirements::new(vec![
            Distribution::SinglePartition,
            Distribution::UnspecifiedDistribution,
        ])
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn crate::PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // Apply to join filter expressions if present
        crate::apply_expression_roots(
            self.filter.iter().map(|filter| filter.expression()),
            f,
        )
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => {
                let left = children.swap_remove(0);
                let right = children.swap_remove(0);
                // The coordinator seeds each chunk's probe-thread counter from
                // the right child's partition count, so it must be rebuilt for
                // the new child rather than cloned from `self`.
                let fallback_coordinator = Arc::new(FallbackCoordinator::new(
                    right.output_partitioning().partition_count().max(1),
                    need_produce_result_in_final(self.join_type),
                ));
                Ok(Arc::new(Self {
                    left,
                    right,
                    metrics: ExecutionPlanMetricsSet::new(),
                    build_side_data: Default::default(),
                    fallback_coordinator,
                    cache: Arc::clone(&self.cache),
                    filter: self.filter.clone(),
                    join_type: self.join_type,
                    join_schema: Arc::clone(&self.join_schema),
                    column_indices: self.column_indices.clone(),
                    projection: self.projection.clone(),
                }))
            }
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(
                NestedLoopJoinExecBuilder::new(
                    Arc::clone(&children[0]),
                    Arc::clone(&children[1]),
                    self.join_type,
                )
                .with_filter(self.filter.clone())
                .with_projection_ref(self.projection.clone())
                .build()?,
            )),
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
        // Rebuild rather than `replace_children(.., Keep)`: the fallback
        // coordinator's `right_partition_count` must match the *new* right
        // child, since it seeds each chunk's probe-thread counter.
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
        assert_eq_or_internal_err!(
            self.left.output_partitioning().partition_count(),
            1,
            "Invalid NestedLoopJoinExec, the output partition count of the left child must be 1,\
                 consider using CoalescePartitionsExec or the EnforceDistribution rule"
        );

        let metrics = NestedLoopJoinMetrics::new(&self.metrics, partition);
        let batch_size = context.session_config().batch_size();

        // update column indices to reflect the projection
        let column_indices_after_projection = match self.projection.as_ref() {
            Some(projection) => projection
                .iter()
                .map(|i| self.column_indices[*i].clone())
                .collect(),
            None => self.column_indices.clone(),
        };

        let right_partition_count = self.right().output_partitioning().partition_count();

        // Always try to buffer all left data in memory via OnceFut. If it does not fit, the load
        // spills the left side during the same pass and the stream runs in memory-limited mode.
        let load_reservation =
            MemoryConsumer::new(format!("NestedLoopJoinLoad[{partition}]"))
                .register(context.memory_pool());

        // Determine if memory-limited mode is possible.
        // Conditions:
        // 1. Disk manager supports temp files (needed for spilling).
        // 2. Join types whose final emission reads the visited-left bitmap
        //    (LEFT, LEFT SEMI, LEFT ANTI, LEFT MARK, FULL) need that bitmap
        //    complete across every probe partition. The memory-limited path
        //    shares each chunk's `JoinLeftData` (visited bitmap plus
        //    probe-thread counter) across all right-side partitions through
        //    [`FallbackCoordinator`], seeding the counter with
        //    `right_partition_count`, so left-side tracking matches the
        //    single-pass path and every partition emits from the same bitmap.
        //
        //    That coordination assumes all right partitions run in the same
        //    process. A distributed engine executes each partition as an
        //    independent task with its own coordinator, so the shared
        //    probe-thread counter would never reach zero and the fallback
        //    would stall. Such engines set
        //    `enable_nlj_coordinated_fallback = false` to opt out for the
        //    affected join types (left-emitting joins over a multi-partition
        //    right side), which then fail with resource exhaustion under
        //    memory pressure instead of deadlocking. Single-partition and
        //    non-left-emitting joins are always safe and keep the fallback.
        let coordinated_fallback_disabled = !context
            .session_config()
            .options()
            .execution
            .enable_nlj_coordinated_fallback
            && need_produce_result_in_final(self.join_type)
            && right_partition_count > 1;
        let can_spill = context.runtime_env().disk_manager.tmp_files_enabled()
            && !coordinated_fallback_disabled;

        let build_side_data = self.build_side_data.try_once(|| {
            let stream = self.left.execute(0, Arc::clone(&context))?;
            // Built here rather than on demand: by the time the load hits the memory limit the
            // stream is partly consumed, and it has to be spillable at that point without going
            // back to the left child.
            let left_spill_manager = can_spill.then(|| {
                SpillManager::new(
                    context.runtime_env(),
                    metrics.spill_metrics.clone(),
                    stream.schema(),
                )
                .with_compression_type(context.session_config().spill_compression())
            });

            Ok(collect_left_input(
                stream,
                metrics.join_metrics.clone(),
                load_reservation,
                need_produce_result_in_final(self.join_type),
                right_partition_count,
                left_spill_manager,
            ))
        })?;

        let probe_side_data = self.right.execute(partition, Arc::clone(&context))?;

        // Determine if OOM fallback to memory-limited mode is possible.
        // Condition: disk manager supports temp files (needed for spilling).
        //
        // For join types that emit unmatched left rows in the final output
        // (LEFT, LEFT SEMI, LEFT ANTI, LEFT MARK, FULL), the fallback path
        // shares per-chunk `JoinLeftData` (visited bitmap + probe-thread
        // counter) across all right-side partitions via
        // [`FallbackCoordinator`], so left-side tracking is coordinated
        // exactly as in the single-pass path.
        //
        // That coordination assumes all right partitions run in the same
        // process. Distributed engines run each partition as an independent
        // task with its own coordinator, so the shared probe-thread counter
        // would never reach zero and the fallback would stall. When
        // `enable_nlj_coordinated_fallback` is disabled, such engines opt
        // out of the coordinated fallback for the affected join types
        // (left-emitting joins with a multi-partition right side); those cases
        // use `SpillState::Disabled` and fail with resource exhaustion under
        // memory pressure instead of deadlocking. Single-partition and
        // non-left-emitting joins are always safe and keep the fallback.
        let coordinated_fallback_disabled = !context
            .session_config()
            .options()
            .execution
            .enable_nlj_coordinated_fallback
            && need_produce_result_in_final(self.join_type)
            && right_partition_count > 1;
        let spill_state = if context.runtime_env().disk_manager.tmp_files_enabled()
            && !coordinated_fallback_disabled
        {
            SpillState::Pending {
                task_context: Arc::clone(&context),
                fallback_coordinator: Arc::clone(&self.fallback_coordinator),
            }
        } else {
            SpillState::Disabled
        };

        Ok(Box::pin(NestedLoopJoinStream::new(
            self.schema(),
            self.filter.clone(),
            self.join_type,
            probe_side_data,
            build_side_data,
            column_indices_after_projection,
            metrics,
            batch_size,
            spill_state,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        // Left side is always broadcast, so it always needs overall stats.
        // Right side is partitioned, so it needs per-partition stats.
        vec![ChildStats::At(None), ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        // NestedLoopJoinExec is designed for joins without equijoin keys in the
        // ON clause (e.g., `t1 JOIN t2 ON (t1.v1 + t2.v1) % 2 = 0`). Any join
        // predicates are stored in `self.filter`, but `estimate_join_statistics`
        // currently doesn't support selectivity estimation for such arbitrary
        // filter expressions. We pass an empty join column list, which means
        // the cardinality estimation cannot use column statistics and returns
        // unknown row counts.
        let join_columns = Vec::new();

        let left_stats = input_stats[0].as_ref().clone();
        let right_stats = input_stats[1].as_ref().clone();

        let stats = estimate_join_statistics(
            left_stats,
            right_stats,
            &join_columns,
            NullEquality::NullEqualsNothing,
            &self.join_type,
            &self.join_schema,
        )?;

        Ok(Arc::new(stats.project(self.projection.as_ref())))
    }

    /// Tries to push `projection` down through `nested_loop_join`. If possible, performs the
    /// pushdown and returns a new [`NestedLoopJoinExec`] as the top plan which has projections
    /// as its children. Otherwise, returns `None`.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // TODO: currently if there is projection in NestedLoopJoinExec, we can't push down projection to left or right input. Maybe we can pushdown the mixed projection later.
        if self.contains_projection() {
            return Ok(None);
        }

        let schema = self.schema();
        if let Some(JoinData {
            projected_left_child,
            projected_right_child,
            join_filter,
            ..
        }) = try_pushdown_through_join_with_column_indices(
            projection,
            self.left(),
            self.right(),
            &[],
            &schema,
            self.filter(),
            self.column_indices.as_slice(),
        )? {
            Ok(Some(Arc::new(
                NestedLoopJoinExecBuilder::new(
                    Arc::new(projected_left_child),
                    Arc::new(projected_right_child),
                    *self.join_type(),
                )
                .with_filter(join_filter)
                .build()?,
            )))
        } else {
            try_embed_projection(projection, self)
        }
    }
    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;

        // Destructure exhaustively (no `..`) so that a newly added field is a
        // compile error here instead of being silently left out of the proto.
        let Self {
            left,
            right,
            filter,
            join_type,
            projection,
            // derived from the children's schemas by `try_new` on decode
            join_schema: _,
            // runtime build-side state, not part of the plan
            build_side_data: _,
            // runtime fallback coordination state, not part of the plan;
            // rebuilt from the right child's partition count on decode
            fallback_coordinator: _,
            // recomputed by `try_new` on decode
            column_indices: _,
            // runtime metrics, not part of the plan
            metrics: _,
            // recomputed by `try_new` on decode
            cache: _,
        } = self;

        let left = ctx.encode_child(left)?;
        let right = ctx.encode_child(right)?;

        let join_type = crate::joins::proto::join_type_to_proto(*join_type);

        let filter = filter
            .as_ref()
            .map(|f| crate::joins::proto::join_filter_to_proto(f, ctx))
            .transpose()?;

        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::NestedLoopJoin(Box::new(
                    protobuf::NestedLoopJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        join_type: join_type.into(),
                        filter,
                        projection: match projection.as_ref() {
                            None => Vec::new(),
                            Some(v) if v.is_empty() => vec![u32::MAX],
                            Some(v) => v.iter().map(|x| *x as u32).collect(),
                        },
                    },
                )),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl NestedLoopJoinExec {
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_proto_models::protobuf;

        let join = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::NestedLoopJoin,
            "NestedLoopJoinExec",
        );

        // Destructure exhaustively (no `..`) so that a newly added proto field
        // is a compile error here instead of being silently ignored.
        let protobuf::NestedLoopJoinExecNode {
            left,
            right,
            join_type,
            filter,
            projection,
        } = &**join;

        let left =
            ctx.decode_required_child(left.as_deref(), "NestedLoopJoinExec", "left")?;
        let right =
            ctx.decode_required_child(right.as_deref(), "NestedLoopJoinExec", "right")?;

        let join_type =
            crate::joins::proto::join_type_from_proto(*join_type, "NestedLoopJoinExec")?;

        let filter = filter
            .as_ref()
            .map(|f| {
                crate::joins::proto::join_filter_from_proto(f, ctx, "NestedLoopJoinExec")
            })
            .transpose()?;

        let projection = match projection.as_slice() {
            [] => None,
            [u32::MAX] => Some(Vec::new()),
            indices => Some(indices.iter().map(|i| *i as usize).collect()),
        };

        Ok(Arc::new(NestedLoopJoinExec::try_new(
            left, right, filter, &join_type, projection,
        )?))
    }
}

/// Field-level tests for the `try_to_proto` / `try_from_proto` hooks.
///
/// `projection` carries the same three states as on `HashJoinExec`, encoded the
/// same way; the mapping is written out again here, so it is tested again here.
#[cfg(all(test, feature = "proto"))]
mod proto_tests {
    use super::*;
    use crate::proto::{ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx};
    use crate::proto_test_util::{
        StubPlanDecoder, StubPlanEncoder, UnreachablePlanDecoder, encoded_child_node,
        stub_child,
    };
    use datafusion_proto_models::protobuf;

    /// Encode an inner nested loop join with the given projection.
    fn encode_projection(projection: Option<Vec<usize>>) -> Vec<u32> {
        let plan = NestedLoopJoinExec::try_new(
            stub_child(),
            stub_child(),
            None,
            &JoinType::Inner,
            projection,
        )
        .unwrap();
        let encoder = StubPlanEncoder::ok();
        let ctx = ExecutionPlanEncodeCtx::new(&encoder);
        let node = plan
            .try_to_proto(&ctx)
            .unwrap()
            .expect("NestedLoopJoinExec should encode to Some(node)");
        match node.physical_plan_type {
            Some(protobuf::physical_plan_node::PhysicalPlanType::NestedLoopJoin(
                join,
            )) => join.projection,
            other => panic!("expected a NestedLoopJoin node, got {other:?}"),
        }
    }

    /// A hand-built `NestedLoopJoinExecNode` wrapped in its `PhysicalPlanNode`.
    fn join_node(projection: Vec<u32>) -> protobuf::PhysicalPlanNode {
        protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::NestedLoopJoin(Box::new(
                    protobuf::NestedLoopJoinExecNode {
                        left: Some(Box::new(encoded_child_node())),
                        right: Some(Box::new(encoded_child_node())),
                        join_type: protobuf::JoinType::Inner.into(),
                        filter: None,
                        projection,
                    },
                )),
            ),
        }
    }

    /// Decode a node with the given projection field, returning the plan's
    /// reconstructed projection.
    fn decode_projection(projection: Vec<u32>) -> Option<Vec<usize>> {
        let decoder = StubPlanDecoder::ok();
        let ctx = ExecutionPlanDecodeCtx::new(&decoder);
        let plan =
            NestedLoopJoinExec::try_from_proto(&join_node(projection), &ctx).unwrap();
        plan.downcast_ref::<NestedLoopJoinExec>()
            .expect("decoded plan should be a NestedLoopJoinExec")
            .projection
            .as_ref()
            .map(|p| p.to_vec())
    }

    #[test]
    fn projection_states_survive_the_encode_side() {
        assert_eq!(encode_projection(None), Vec::<u32>::new());
        // An empty projection changes the output schema, so it must not share
        // the "absent" encoding.
        assert_eq!(encode_projection(Some(vec![])), vec![u32::MAX]);
        assert_eq!(encode_projection(Some(vec![0, 1])), vec![0, 1]);
    }

    #[test]
    fn projection_states_survive_the_decode_side() {
        assert_eq!(decode_projection(vec![]), None);
        assert_eq!(decode_projection(vec![u32::MAX]), Some(vec![]));
        assert_eq!(decode_projection(vec![0, 1]), Some(vec![0, 1]));
    }

    #[test]
    fn try_from_proto_rejects_a_different_plan_variant() {
        let decoder = UnreachablePlanDecoder::new();
        let ctx = ExecutionPlanDecodeCtx::new(&decoder);

        let err =
            NestedLoopJoinExec::try_from_proto(&encoded_child_node(), &ctx).unwrap_err();
        assert!(err.to_string().contains("not a NestedLoopJoinExec"));
    }
}

impl EmbeddedProjection for NestedLoopJoinExec {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        self.with_projection(projection)
    }
}

/// Left (build-side) data
pub(crate) struct JoinLeftData {
    /// Build-side data collected to single batch
    batch: RecordBatch,
    /// Shared bitmap builder for visited left indices
    bitmap: SharedBitmapBuilder,
    /// Counter of running probe-threads, potentially able to update `bitmap`
    probe_threads_counter: AtomicUsize,
    /// Memory reservation for tracking batch and bitmap
    /// Cleared on `JoinLeftData` drop
    /// reservation is cleared on Drop
    #[expect(dead_code)]
    reservation: MemoryReservation,
}

impl JoinLeftData {
    pub(crate) fn new(
        batch: RecordBatch,
        bitmap: SharedBitmapBuilder,
        probe_threads_counter: AtomicUsize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            batch,
            bitmap,
            probe_threads_counter,
            reservation,
        }
    }

    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(crate) fn bitmap(&self) -> &SharedBitmapBuilder {
        &self.bitmap
    }

    /// Decrements counter of running threads, and returns `true`
    /// if caller is the last running thread
    pub(crate) fn report_probe_completed(&self) -> bool {
        self.probe_threads_counter.fetch_sub(1, Ordering::Relaxed) == 1
    }
}

/// Asynchronously collect the left input in a single pass over the stream.
///
/// The whole side is buffered in memory when it fits the budget. When it does not and
/// `spill_manager` is available, the batches collected so far and the rest of the same stream are
/// written to one spill file, which is what the memory-limited mode then reads in chunks. The
/// stream is consumed exactly once either way, so a left child that cannot be executed twice (or
/// replayed) stays correct.
async fn collect_left_input(
    mut stream: SendableRecordBatchStream,
    join_metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    with_visited_left_side: bool,
    probe_threads_count: usize,
    spill_manager: Option<SpillManager>,
) -> Result<LeftLoad> {
    let schema = stream.schema();
    let metrics = join_metrics;
    let mut batches: Vec<RecordBatch> = Vec::new();

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let build_timer = metrics.build_time.timer();
        let batch_size = batch.get_array_memory_size();
        match reservation.try_grow(batch_size) {
            Ok(()) => {
                metrics.build_mem_used.add(batch_size);
                metrics.build_input_batches.add(1);
                metrics.build_input_rows.add(batch.num_rows());
                batches.push(batch);
            }
            Err(e) if is_spillable_oom(&e, spill_manager.as_ref()) => {
                // Do not keep the operator timer running while the spill path
                // drains the child stream.
                build_timer.done();
                let spill_manager = spill_manager.expect("checked by is_spillable_oom");
                let spilled = spill_left_input(
                    spill_manager,
                    Arc::clone(&schema),
                    batches,
                    Some(batch),
                    stream,
                    metrics,
                    &reservation,
                )
                .await?;
                return Ok(left_load_from_spill(
                    spilled,
                    schema,
                    probe_threads_count,
                    reservation,
                ));
            }
            Err(e) => return Err(e),
        }
    }

    // Only time the build-side materialization performed by this operator, not
    // polling the child stream above.
    let build_timer = metrics.build_time.timer();

    let merged_batch = concat_batches(&schema, &batches)?;

    // Reserve memory for visited_left_side bitmap if required by join type
    let visited_left_side = if with_visited_left_side {
        let n_rows = merged_batch.num_rows();
        let buffer_size = n_rows.div_ceil(8);
        match reservation.try_grow(buffer_size) {
            Ok(()) => {}
            Err(e) if is_spillable_oom(&e, spill_manager.as_ref()) => {
                // `spill_left_input` owns its timing and polls the input stream
                // outside that timer.
                build_timer.done();
                let spill_manager = spill_manager.expect("checked by is_spillable_oom");
                drop(batches);
                let spilled = spill_left_input(
                    spill_manager,
                    Arc::clone(&schema),
                    vec![merged_batch],
                    None,
                    stream,
                    metrics,
                    &reservation,
                )
                .await?;
                return Ok(left_load_from_spill(
                    spilled,
                    schema,
                    probe_threads_count,
                    reservation,
                ));
            }
            Err(e) => return Err(e),
        }
        metrics.build_mem_used.add(buffer_size);

        let mut buffer = BooleanBufferBuilder::new(n_rows);
        buffer.append_n(n_rows, false);
        buffer
    } else {
        BooleanBufferBuilder::new(0)
    };

    Ok(LeftLoad::InMemory(Arc::new(JoinLeftData::new(
        merged_batch,
        Mutex::new(visited_left_side),
        AtomicUsize::new(probe_threads_count),
        reservation,
    ))))
}

/// A left side with no rows needs no spill file, so it stays on the in-memory path.
fn left_load_from_spill(
    spilled: Option<LeftSpillData>,
    schema: SchemaRef,
    probe_threads_count: usize,
    reservation: MemoryReservation,
) -> LeftLoad {
    match spilled {
        Some(data) => LeftLoad::Spilled(Arc::new(data)),
        // No rows means no bitmap either, whatever the join type.
        None => LeftLoad::InMemory(Arc::new(JoinLeftData::new(
            RecordBatch::new_empty(schema),
            Mutex::new(BooleanBufferBuilder::new(0)),
            AtomicUsize::new(probe_threads_count),
            reservation,
        ))),
    }
}

/// Whether a failed reservation is an exhausted pool that the caller can spill its way out of.
fn is_spillable_oom(
    error: &datafusion_common::DataFusionError,
    spill_manager: Option<&SpillManager>,
) -> bool {
    spill_manager.is_some()
        && matches!(
            error.find_root(),
            datafusion_common::DataFusionError::ResourcesExhausted(_)
        )
}

/// Write the already-buffered left batches plus the remainder of the same stream to one spill file.
/// Returns `None` when the left side carried no rows at all, which needs no spill file.
async fn spill_left_input(
    spill_manager: SpillManager,
    schema: SchemaRef,
    buffered: Vec<RecordBatch>,
    pending: Option<RecordBatch>,
    mut stream: SendableRecordBatchStream,
    metrics: BuildProbeJoinMetrics,
    reservation: &MemoryReservation,
) -> Result<Option<LeftSpillData>> {
    let build_timer = metrics.build_time.timer();
    let mut spill_file =
        spill_manager.create_in_progress_file("NestedLoopJoin left spill")?;

    for batch in buffered {
        if batch.num_rows() > 0 {
            spill_file.append_batch(&batch)?;
        }
    }
    // The in-memory batches are spilled and dropped, so their reservation goes back to the pool
    // before the rest of the stream is drained.
    reservation.free();

    for batch in pending.into_iter() {
        if batch.num_rows() > 0 {
            metrics.build_input_batches.add(1);
            metrics.build_input_rows.add(batch.num_rows());
            spill_file.append_batch(&batch)?;
        }
    }
    build_timer.done();

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let _build_timer = metrics.build_time.timer();
        if batch.num_rows() > 0 {
            metrics.build_input_batches.add(1);
            metrics.build_input_rows.add(batch.num_rows());
            spill_file.append_batch(&batch)?;
        }
    }

    let _build_timer = metrics.build_time.timer();
    Ok(spill_file.finish()?.map(|file| LeftSpillData {
        spill_manager,
        spill_file: file,
        schema,
    }))
}

/// States for join processing. See `poll_next()` comment for more details about
/// state transitions.
#[derive(Debug, Clone, Copy)]
enum NLJState {
    BufferingLeft,
    FetchingRight,
    ProbeRight,
    EmitRightUnmatched,
    /// Entered exactly once per left chunk, when the probe (right) side is
    /// exhausted and probing for the current chunk is finished. This state
    /// owns the single [`JoinLeftData::report_probe_completed`] call that
    /// decrements the shared probe-threads counter, and records in
    /// `is_unmatched_left_emitter` whether this stream is the one responsible
    /// for emitting unmatched-left rows. Splitting this decision out of
    /// `EmitLeftUnmatched` makes "decrement exactly once" a structural
    /// property of the state graph, so the (re-enterable) emit state no longer
    /// has to guard against decrementing twice.
    ProbeEnd,
    EmitLeftUnmatched,
    /// Emit unmatched right rows using the global bitmap accumulated across
    /// all left chunks. Only used in memory-limited mode for join types that
    /// require tracking right-side matches in the final output (RIGHT, FULL,
    /// RIGHT SEMI, RIGHT ANTI, RIGHT MARK).
    EmitGlobalRightUnmatched,
    Done,
}
/// Outcome of the single pass over the left (build) input.
pub(crate) enum LeftLoad {
    /// The left side fit the memory budget and is buffered as one batch.
    InMemory(Arc<JoinLeftData>),
    /// The budget ran out, so the left side was spilled during that same pass. Every partition
    /// shares this handle, and each left chunk pass re-opens the file.
    Spilled(Arc<LeftSpillData>),
}

/// The spilled left side, shared by every output partition.
pub(crate) struct LeftSpillData {
    /// SpillManager used to read the spill file (has the left schema)
    spill_manager: SpillManager,
    /// The spill file containing all left-side batches
    spill_file: Arc<dyn SpillFile>,
    /// Left-side schema
    schema: SchemaRef,
}

/// Per-chunk shared state in the memory-limited fallback path.
///
/// Each chunk's `JoinLeftData` is loaded once by a "leader" partition and
/// shared (via `Arc`) with every right-side output partition. The
/// `probe_threads_counter` inside the `JoinLeftData` is initialized to
/// `right_partition_count`, so `report_probe_completed` returns `true`
/// only when the *last* partition has finished probing the chunk. That
/// last partition is then responsible for emitting unmatched left rows
/// for the chunk, mirroring the single-pass path's coordination via
/// `collect_left_input(..., probe_threads_count)`.
struct CurrentChunk {
    /// 0-based monotonically increasing chunk index.
    chunk_index: usize,
    /// Shared per-chunk left data. Cloned by every partition that probes
    /// this chunk; the last to call `report_probe_completed` emits
    /// unmatched left rows.
    data: Arc<JoinLeftData>,
    /// True if the left stream was exhausted while loading this chunk —
    /// no further chunks will be produced after it.
    is_last: bool,
}

/// Inner state of [`FallbackCoordinator`], guarded by a synchronous mutex.
///
/// Synchronous because cancellation and chunk release have to complete without
/// another poll or await -- cancellation runs from `Drop`, which has neither --
/// rather than depending on a future a dropped stream would take with it. No
/// critical section awaits: the one slow operation, reading a chunk, runs after
/// the guard is released.
struct FallbackCoordinatorInner {
    /// Reservation the leader borrows to bound one chunk load.
    ///
    /// On a successful load `load_one_chunk` moves the accounted bytes into the
    /// chunk's `JoinLeftData` with `take()`, so the accounting follows the data
    /// rather than staying with this slot. Lazily registered by the first
    /// leader, once a runtime context is available.
    reservation: Option<MemoryReservation>,
    /// The shared left spill stream from which chunks are read. Owned by
    /// the coordinator so only one partition reads it at a time.
    left_stream: Option<SendableRecordBatchStream>,
    /// Left schema. Set after the first leader resolves the spill future.
    left_schema: Option<SchemaRef>,
    /// One batch carried over from the previous chunk's load: when
    /// reservation `try_grow` failed for chunk N, the offending batch is
    /// recorded here and becomes the first batch of chunk N+1.
    carryover: Option<RecordBatch>,
    /// True once the left spill stream has produced `None`.
    left_exhausted: bool,
    /// Index of the next chunk to be loaded.
    next_chunk_index: usize,
    /// The currently-loaded chunk, or `None` if no chunk is currently
    /// loaded (initial state, or the last partition has just released
    /// chunk `next_chunk_index - 1` and the next leader hasn't taken
    /// over yet).
    current: Option<CurrentChunk>,
    /// True while a partition has claimed leader role for the next
    /// chunk and is loading it; prevents two partitions from racing.
    loader_in_flight: bool,
    /// A partition was dropped while still `Pending`, before the shared load
    /// decided whether the left side spills.
    ///
    /// `Pending` is entered by every eligible execution, including those whose
    /// left side ends up fitting in memory -- and those never build a shared
    /// chunk counter, so their right partitions stay independent and a dropped
    /// peer has nothing to coordinate. Cancelling on such a drop would fail a
    /// query that had no fallback at all, so it is only recorded here.
    ///
    /// Paired with `coordination_started`: whichever of the two happens second
    /// performs the cancellation, so the drop is honoured whether it precedes or
    /// follows the execution becoming coordinated. A bool suffices -- one lost
    /// partition is enough to cancel, and nothing reads a count.
    pending_drop: bool,
    /// Set once any partition has entered the coordinated path.
    ///
    /// Remembered rather than checked in the moment, because a `Pending` drop can
    /// arrive after a peer is already coordinating; that drop must cancel, and
    /// without this flag there would be nobody left to notice.
    coordination_started: bool,
    /// Set when a stream is dropped before finishing, which cancels the whole
    /// coordinated fallback.
    ///
    /// The partitions of a coordinated fallback are not independent: chunk
    /// advancement requires every one of them to report, so a partition that
    /// disappears mid-probe would otherwise leave the survivors waiting on a
    /// release nobody will ever make. Once set, chunk state is dropped, waiters
    /// are woken with an error, and a loader that is still reading must discard
    /// its result instead of publishing it.
    cancelled: bool,
}

/// Plan-level shared coordinator for the memory-limited fallback path.
///
/// All right-side output partitions share one of these. It serializes
/// access to the left spill stream (so each chunk is read exactly once),
/// publishes the loaded chunk as an `Arc<JoinLeftData>` for every
/// partition to clone, and uses a `Notify` so partitions waiting for the
/// next chunk can sleep without busy-looping.
pub(crate) struct FallbackCoordinator {
    /// Number of right-side partitions; equals the
    /// `probe_threads_counter` initial value for each chunk.
    right_partition_count: usize,
    /// Whether `JoinLeftData` should carry a left visited bitmap (for
    /// join types that emit unmatched left rows in the final output).
    with_visited_bitmap: bool,
    inner: Mutex<FallbackCoordinatorInner>,
    /// Notified when a new chunk becomes available, when the left stream
    /// is exhausted, or when a chunk is released.
    notify: tokio::sync::Notify,
    /// Broadcast signalled when the fallback is cancelled.
    ///
    /// This carries no state of its own -- `cancelled` is what persists. A
    /// delivered broadcast is itself sufficient to establish cancellation;
    /// observers read the flag before awaiting, so neither alone is relied on.
    /// Kept separate from `notify`
    /// because cancellation has to reach tasks that are not waiting on chunk
    /// progress at all: a stream parked on its right input, or a loader parked
    /// on a spill read. Waiters enable their `Notified` before reading
    /// `cancelled`, so a cancellation landing between those two steps is
    /// delivered rather than lost.
    cancel_notify: tokio::sync::Notify,
    /// Test seam that reproduces one cancellation interleaving deterministically.
    ///
    /// Set to `1` to make the leader cancel after claiming the load but before
    /// it registers its cancellation watcher. A cancellation lost in that window
    /// strands the loader itself -- other observers may already be returning
    /// errors -- which is what the paired test checks. Consumed when it fires,
    /// so one store arms it once.
    #[cfg(test)]
    cancel_at_leader_claim: AtomicUsize,
}

impl FallbackCoordinator {
    fn new(right_partition_count: usize, with_visited_bitmap: bool) -> Self {
        Self {
            right_partition_count,
            with_visited_bitmap,
            inner: Mutex::new(FallbackCoordinatorInner {
                reservation: None,
                left_stream: None,
                left_schema: None,
                carryover: None,
                left_exhausted: false,
                next_chunk_index: 0,
                current: None,
                loader_in_flight: false,
                pending_drop: false,
                coordination_started: false,
                cancelled: false,
            }),
            notify: tokio::sync::Notify::new(),
            cancel_notify: tokio::sync::Notify::new(),
            #[cfg(test)]
            cancel_at_leader_claim: AtomicUsize::new(0),
        }
    }

    /// After the last partition finishes processing chunk
    /// `released_chunk_index`, drop the slot so the next leader can
    /// load chunk `released_chunk_index + 1`.
    fn release_chunk(self: &Arc<Self>, released_chunk_index: usize) {
        {
            let mut inner = self.inner.lock();
            if let Some(cur) = &inner.current
                && cur.chunk_index == released_chunk_index
            {
                inner.current = None;
                inner.next_chunk_index = released_chunk_index + 1;
            }
        }
        // Always notify: waiters may be blocked because they couldn't
        // become leader while a previous chunk was current.
        self.notify.notify_waiters();
    }

    /// True once a partition was dropped unfinished, cancelling the fallback.
    ///
    /// Production code observes cancellation through `cancellation_watcher`, so
    /// that a waker is registered; this plain read is for assertions only.
    #[cfg(test)]
    fn is_cancelled(&self) -> bool {
        self.inner.lock().cancelled
    }

    /// A future that resolves when the fallback is cancelled.
    ///
    /// Registration happens on the future's **first poll**, not at construction:
    /// that poll enables the `Notified` and then reads `cancelled`, so whichever
    /// happens first is observed. Callers must therefore poll it, not merely hold
    /// it.
    ///
    /// Callers that park on something other than chunk progress -- a stream
    /// waiting on its right input, for instance -- need one of these polled
    /// alongside their own work, or a peer's cancellation never reaches their
    /// waker.
    fn cancellation_watcher(self: &Arc<Self>) -> BoxFuture<'static, ()> {
        let coordinator = Arc::clone(self);
        async move {
            let notified = coordinator.cancel_notify.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if coordinator.inner.lock().cancelled {
                return;
            }
            notified.await;
        }
        .boxed()
    }

    /// Records a partition dropped before the shared load decided whether the
    /// left side spills.
    ///
    /// Whether this cancels depends on what the surviving partitions are doing,
    /// which is why the drop is recorded rather than acted on unconditionally:
    ///
    /// * No peer has coordinated yet -- only `pending_drop` is set. If the load
    ///   resolves to `InMemory` nobody ever coordinates and this stays inert, so
    ///   an execution that never needed the coordinator is not failed by it.
    /// * A peer is already coordinating -- cancel now. That peer is waiting on a
    ///   probe report this partition will never make.
    ///
    /// The second case is the mirror of [`Self::begin_coordination`]: the two
    /// share `pending_drop` and `coordination_started` under one lock, so
    /// whichever runs second performs the cancellation and neither order is lost.
    fn record_pending_drop(self: &Arc<Self>) {
        let cancel_now = {
            let mut inner = self.inner.lock();
            if inner.cancelled {
                return;
            }
            inner.pending_drop = true;
            // A peer may already be coordinating, in which case this drop is not
            // hypothetical: that peer is waiting on a report this partition will
            // never make.
            inner.coordination_started
        };
        if cancel_now {
            self.cancel();
        }
    }

    /// Records that this execution is now coordinating, and honours any drop that
    /// happened while it was not.
    ///
    /// Called when a stream enters memory-limited mode. Setting the flag matters
    /// as much as the check: a `Pending` partition dropped *after* this point has
    /// to cancel too, and `record_pending_drop` reads this flag to decide that.
    fn begin_coordination(self: &Arc<Self>) {
        let cancel_now = {
            let mut inner = self.inner.lock();
            inner.coordination_started = true;
            !inner.cancelled && inner.pending_drop
        };
        if cancel_now {
            self.cancel();
        }
    }

    /// Cancels the coordinated fallback and drops everything the coordinator
    /// holds, synchronously.
    ///
    /// Called from a stream's drop guard when it goes away without finishing.
    /// Chunks other partitions still hold stay accounted until they release
    /// them; what this drops is the coordinator's own state, which nothing will
    /// come back for.
    ///
    /// This is the only place that signals `cancel_notify`. Watchers rely on
    /// that: a wake from it always means a real cancellation, which is why they
    /// need no re-arm loop. Keep it that way if you add call sites.
    fn cancel(self: &Arc<Self>) {
        {
            let mut inner = self.inner.lock();
            if inner.cancelled {
                return;
            }
            inner.cancelled = true;
            inner.current = None;
            inner.carryover = None;
            inner.left_stream = None;
            inner.reservation = None;
        }
        self.notify.notify_waiters();
        self.cancel_notify.notify_waiters();
    }

    /// Fetch `expected_chunk_index`, becoming leader to load it from the
    /// left spill stream if no other partition has done so. Returns
    /// `Ok(None)` when the left stream is exhausted and no chunk with
    /// the requested index exists.
    async fn next_chunk(
        self: Arc<Self>,
        expected_chunk_index: usize,
        spill_data: Arc<LeftSpillData>,
        task_context: Arc<TaskContext>,
        build_time: Time,
    ) -> Result<Option<(Arc<JoinLeftData>, bool)>> {
        // `spill_data` is already resolved: every partition receives the
        // same `Arc<LeftSpillData>` from the shared `OnceAsync<LeftLoad>`,
        // so the left child is executed and spilled exactly once.
        loop {
            // Decide what to do with the lock held, then act on that decision
            // after releasing it. The guard must not survive into the `.await`
            // below -- it is not `Send`, and holding it across the load would
            // serialize every partition behind the leader's disk reads.
            let decision = {
                let mut inner = self.inner.lock();

                if inner.cancelled {
                    Decision::Cancelled
                } else if let Some(cur) = &inner.current
                    && cur.chunk_index == expected_chunk_index
                {
                    // Case 1: requested chunk is already loaded.
                    Decision::Serve(Arc::clone(&cur.data), cur.is_last)
                } else if inner.left_exhausted
                    && inner.current.is_none()
                    && inner.carryover.is_none()
                {
                    // Case 2: left side finished and nothing left to deliver.
                    Decision::Finished
                } else if inner.current.is_none() && !inner.loader_in_flight {
                    // Case 3: claim the leader role and take the shared
                    // resources out so the load can run without the lock.
                    inner.loader_in_flight = true;
                    let stream = inner.left_stream.take();
                    let reservation = inner.reservation.take();
                    let carryover = inner.carryover.take();
                    let chunk_index_to_load = inner.next_chunk_index;
                    debug_assert_eq!(chunk_index_to_load, expected_chunk_index);
                    Decision::Load {
                        stream,
                        reservation,
                        carryover,
                        chunk_index: chunk_index_to_load,
                    }
                } else {
                    // Case 4: someone else is loading, or this chunk index has
                    // already been passed -- wait to be notified.
                    Decision::Wait(self.notify.notified())
                }
            };

            match decision {
                Decision::Cancelled => {
                    return exec_err!(
                        "NestedLoopJoin coordinated fallback was cancelled because a \
                         partition was dropped before finishing"
                    );
                }
                Decision::Serve(data, is_last) => return Ok(Some((data, is_last))),
                Decision::Finished => return Ok(None),
                Decision::Wait(notified) => {
                    notified.await;
                }
                Decision::Load {
                    stream,
                    reservation,
                    carryover,
                    chunk_index,
                } => {
                    // Build whatever the slot did not already have. A failure
                    // here must clear the leader flag and wake waiters, or they
                    // block on a release the failed leader never makes.
                    let (mut left_stream, left_schema) = match stream {
                        Some(stream) => {
                            let schema = {
                                let inner = self.inner.lock();
                                inner.left_schema.clone()
                            };
                            let schema = match schema {
                                Some(schema) => schema,
                                None => Arc::clone(&spill_data.schema),
                            };
                            (stream, schema)
                        }
                        None => {
                            match spill_data.spill_manager.read_spill_as_stream(
                                Arc::clone(&spill_data.spill_file),
                                None,
                            ) {
                                Ok(stream) => {
                                    let mut inner = self.inner.lock();
                                    inner.left_schema =
                                        Some(Arc::clone(&spill_data.schema));
                                    drop(inner);
                                    (stream, Arc::clone(&spill_data.schema))
                                }
                                Err(e) => {
                                    {
                                        let mut inner = self.inner.lock();
                                        inner.loader_in_flight = false;
                                    }
                                    self.notify.notify_waiters();
                                    return Err(e);
                                }
                            }
                        }
                    };
                    let mut reservation = match reservation {
                        Some(reservation) => reservation,
                        None => {
                            MemoryConsumer::new("NestedLoopJoinFallbackChunk".to_string())
                                .with_can_spill(true)
                                .register(task_context.memory_pool())
                        }
                    };

                    // Race the read against cancellation. A loader parked on
                    // its input is not waiting on `notify`, so without this a
                    // `cancel` would not be observed until the read finished on
                    // its own -- which may be never if the input is gone.
                    // Test seam: fire a cancellation in the window between
                    // claiming the load and registering the watcher below. See
                    // `cancel_at_leader_claim`.
                    #[cfg(test)]
                    if self.cancel_at_leader_claim.swap(0, Ordering::SeqCst) == 1 {
                        self.cancel();
                    }
                    let cancelled = self.cancel_notify.notified();
                    let load = Arc::clone(&self).load_one_chunk(
                        chunk_index,
                        &mut left_stream,
                        &mut reservation,
                        carryover,
                        Arc::clone(&left_schema),
                        build_time.clone(),
                    );
                    let load_result = {
                        let mut load = std::pin::pin!(load);
                        let mut cancelled = std::pin::pin!(cancelled);
                        // Queue the waiter, then read the flag. If cancellation
                        // already happened we bail without awaiting; if it lands
                        // just after, the enabled future receives the broadcast
                        // rather than losing it.
                        cancelled.as_mut().enable();
                        if self.inner.lock().cancelled {
                            None
                        } else {
                            tokio::select! {
                                biased;
                                result = &mut load => Some(result),
                                () = &mut cancelled => None,
                            }
                        }
                    };
                    let Some(load_result) = load_result else {
                        // Cancelled mid-read. Drop the local stream and
                        // reservation instead of putting them back, and
                        // clear the leader claim so nothing waits on us.
                        drop(left_stream);
                        drop(reservation);
                        {
                            let mut inner = self.inner.lock();
                            inner.loader_in_flight = false;
                        }
                        self.notify.notify_waiters();
                        return exec_err!(
                            "NestedLoopJoin coordinated fallback was cancelled \
                             while a chunk was being loaded"
                        );
                    };

                    // Publish, unless the fallback was cancelled while this
                    // load was running -- putting the stream and reservation
                    // back would undo the cleanup `cancel` just did.
                    let published = {
                        let mut inner = self.inner.lock();
                        inner.loader_in_flight = false;
                        if inner.cancelled {
                            None
                        } else {
                            inner.left_stream = Some(left_stream);
                            inner.reservation = Some(reservation);
                            match load_result {
                                Ok(LoadOutcome::Chunk {
                                    data,
                                    is_last,
                                    carryover,
                                }) => {
                                    inner.carryover = carryover;
                                    if is_last {
                                        inner.left_exhausted = true;
                                    }
                                    let arc_data = Arc::new(data);
                                    inner.current = Some(CurrentChunk {
                                        chunk_index,
                                        data: Arc::clone(&arc_data),
                                        is_last,
                                    });
                                    Some(Ok(Some((arc_data, is_last))))
                                }
                                Ok(LoadOutcome::Empty) => {
                                    inner.left_exhausted = true;
                                    Some(Ok(None))
                                }
                                Err(e) => Some(Err(e)),
                            }
                        }
                    };
                    self.notify.notify_waiters();
                    match published {
                        Some(result) => return result,
                        None => {
                            return exec_err!(
                                "NestedLoopJoin coordinated fallback was cancelled \
                                 while a chunk was being loaded"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Read one chunk worth of left batches into a `JoinLeftData`,
    /// honoring the coordinator's reservation as the memory budget.
    async fn load_one_chunk(
        self: Arc<Self>,
        _chunk_index: usize,
        left_stream: &mut SendableRecordBatchStream,
        reservation: &mut MemoryReservation,
        carryover: Option<RecordBatch>,
        left_schema: SchemaRef,
        build_time: Time,
    ) -> Result<LoadOutcome> {
        // The previous chunk's bytes were moved into its `JoinLeftData`, so
        // this reservation is already back to zero; resize defensively in case
        // a load bailed out after growing it (an error path, or `Empty`).
        reservation.resize(0);

        let mut pending_batches: Vec<RecordBatch> = Vec::new();
        let mut left_stream_exhausted = false;
        let mut next_carryover: Option<RecordBatch> = None;

        // First, account for any carryover batch from the previous
        // chunk's load attempt. Its memory is already in-flight, so we
        // grow the reservation infallibly.
        if let Some(batch) = carryover {
            let bytes = batch.get_array_memory_size();
            reservation.grow(bytes);
            pending_batches.push(batch);
        }

        loop {
            match left_stream.next().await {
                Some(Ok(batch)) => {
                    // Times only the work this operator does on the batch, not
                    // the wait for the child stream to produce it.
                    let _build_timer = build_time.timer();
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let bytes = batch.get_array_memory_size();
                    let can_grow = reservation.try_grow(bytes).is_ok();
                    if !can_grow && !pending_batches.is_empty() {
                        // Defer this batch to the next chunk.
                        next_carryover = Some(batch);
                        break;
                    } else if !can_grow {
                        // No pending batches — accept the batch even
                        // over budget so we make progress.
                        reservation.grow(bytes);
                    }
                    pending_batches.push(batch);
                }
                Some(Err(e)) => return Err(e),
                None => {
                    left_stream_exhausted = true;
                    break;
                }
            }
        }

        if pending_batches.is_empty() {
            debug_assert!(left_stream_exhausted);
            return Ok(LoadOutcome::Empty);
        }

        let _build_timer = build_time.timer();
        let merged_batch = concat_batches(&left_schema, &pending_batches)?;
        let n_rows = merged_batch.num_rows();
        let visited_left_side = if self.with_visited_bitmap {
            let buffer_size = n_rows.div_ceil(8);
            reservation.grow(buffer_size);
            let mut buffer = BooleanBufferBuilder::new(n_rows);
            buffer.append_n(n_rows, false);
            buffer
        } else {
            BooleanBufferBuilder::new(0)
        };

        // Move the bytes accounted for this chunk out of the coordinator's
        // reservation and into the chunk's `JoinLeftData`, whose reservation is
        // released on drop. The coordinator elects the unmatched-left emitter
        // from the probe-threads counter, but that is the last stream to finish
        // *probing* -- not necessarily the last to drop its `Arc` to the chunk,
        // since a stream can flush a completed output batch and return while
        // still holding one. Tying the reservation to the data means the bytes
        // stay accounted until the final reference goes away, whichever stream
        // holds it, instead of being released when the slot is freed.
        //
        // `take` keeps the same `MemoryConsumer`, so this is a transfer of
        // ownership rather than a new registration, and it leaves the
        // coordinator's reservation at zero for the next chunk.
        let chunk_reservation = reservation.take();

        let data = JoinLeftData::new(
            merged_batch,
            Mutex::new(visited_left_side),
            AtomicUsize::new(self.right_partition_count),
            chunk_reservation,
        );

        Ok(LoadOutcome::Chunk {
            data,
            is_last: left_stream_exhausted,
            carryover: next_carryover,
        })
    }
}

/// What [`FallbackCoordinator::next_chunk`] decided to do while holding the
/// lock, so the work itself can happen after the guard is released.
enum Decision<'a> {
    /// Chunk is loaded and can be handed straight back.
    Serve(Arc<JoinLeftData>, bool),
    /// Left side is finished; the caller is past the last chunk.
    Finished,
    /// This caller is the leader and owns the shared resources for one load.
    Load {
        stream: Option<SendableRecordBatchStream>,
        reservation: Option<MemoryReservation>,
        carryover: Option<RecordBatch>,
        chunk_index: usize,
    },
    /// Someone else is loading, or this index has been passed: wait.
    Wait(tokio::sync::futures::Notified<'a>),
    /// A partition was dropped before finishing, so the fallback is cancelled.
    Cancelled,
}

enum LoadOutcome {
    Chunk {
        data: JoinLeftData,
        is_last: bool,
        carryover: Option<RecordBatch>,
    },
    Empty,
}

impl std::fmt::Debug for FallbackCoordinator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallbackCoordinator")
            .field("right_partition_count", &self.right_partition_count)
            .finish()
    }
}

/// Tracks the state of the memory-limited spill fallback for NLJ.
///
/// The NLJ always tries to buffer the whole left side in memory. If that does not fit and
/// conditions allow, the load spills the left side (see [`collect_left_input`]) and the operator
/// switches to a multi-pass strategy where left chunks are read back from that spill file and the
/// right side is spilled for re-scanning.
pub(crate) enum SpillState {
    /// Memory-limited mode is not possible (e.g., join type requires global right bitmap,
    /// or disk manager is disabled). OOM errors will propagate as-is.
    Disabled,

    /// Memory-limited mode is possible but not entered: the left side is still expected to fit.
    /// Holds the context needed to set the mode up.
    Pending {
        /// TaskContext for reservations and SpillManager creation
        task_context: Arc<TaskContext>,
        /// Shared coordinator that publishes per-chunk `JoinLeftData` to
        /// every right-side partition.
        fallback_coordinator: Arc<FallbackCoordinator>,
    },

    /// Memory-limited mode is running. Left data is read back in chunks
    /// and the right side is spilled to disk for re-scanning.
    Active(Box<SpillStateActive>),
}

/// Result of a single chunk fetch from the [`FallbackCoordinator`]:
/// either the chunk itself with a flag indicating whether it is the
/// final chunk, or `None` if the left input is fully consumed.
type ChunkFetchOutput = Option<(Arc<JoinLeftData>, bool)>;
/// In-flight future for a chunk fetch.
type ChunkFetchFuture = BoxFuture<'static, Result<ChunkFetchOutput>>;

/// State for active memory-limited spill execution.
/// Boxed inside [`SpillState::Active`] to reduce enum size.
pub(crate) struct SpillStateActive {
    /// The spilled left side, shared by every partition.
    left_spill: Arc<LeftSpillData>,
    /// Left-side schema, set from the first chunk the coordinator delivers.
    /// Used by `EmitGlobalRightUnmatched` to build NULL-padded left columns.
    left_schema: Option<SchemaRef>,
    /// Plan-level coordinator that publishes per-chunk `JoinLeftData`
    /// shared across all right-side partitions.
    coordinator: Arc<FallbackCoordinator>,
    /// Index of the next chunk this partition expects from the
    /// coordinator. Increments after the partition finishes processing
    /// a chunk (regardless of whether it was the one that emitted
    /// unmatched left rows).
    next_chunk_index: usize,
    /// Captured `TaskContext` so that the first leader can register the
    /// coordinator's reservation against the runtime's memory pool.
    task_context: Arc<TaskContext>,
    /// Right input that spills on the first pass and replays from spill later.
    right_input: ReplayableStreamSource,
    /// Per-batch accumulated right bitmaps across all left chunks.
    /// Index = right batch sequence number (0-based, non-empty batches only).
    /// Only populated when `should_track_unmatched_right` is true.
    global_right_bitmaps: Vec<BooleanBuffer>,
    /// Separate reservation for `global_right_bitmaps`. These buffers live
    /// for the full operator lifetime (not per-chunk).
    global_right_bitmaps_reservation: MemoryReservation,
    /// Current right batch sequence index within the current pass.
    right_batch_index: usize,
    /// In-flight chunk fetch future. Created by `BufferingLeft` when a
    /// new chunk is needed; polled across iterations of `poll_next`
    /// until it resolves to either the next chunk or `None` (left side
    /// exhausted with no chunk to deliver).
    chunk_fetch_in_flight: Option<ChunkFetchFuture>,
}

impl SpillStateActive {
    /// Merge a per-pass right bitmap into the global accumulator at the
    /// given batch index, growing the dedicated reservation when seeing
    /// a batch index for the first time.
    ///
    /// On first encounter of `idx`, the bitmap is stored as-is and its
    /// size is reserved. On subsequent encounters (later left chunk
    /// passes over the same right batch), the existing entry is OR-merged
    /// with `values`. Because `bitor` produces a buffer of the same bit
    /// length, the reservation does not need to be adjusted on merge.
    fn merge_current_right_bitmap(&mut self, idx: usize, values: BooleanBuffer) {
        if idx >= self.global_right_bitmaps.len() {
            // First encounter of this right batch — account memory and store.
            // The bitmap has one bit per right row, so for very large right
            // inputs the accumulated size can be non-negligible (e.g.,
            // 1M rows ≈ 125 KB per batch).
            // Use infallible `grow` because we must accept the bitmap to
            // preserve correctness — the fallback path has no other recourse.
            let bytes = values.len().div_ceil(8);
            self.global_right_bitmaps_reservation.grow(bytes);
            self.global_right_bitmaps.push(values);
        } else {
            // Subsequent left chunk pass — OR merge. Same bit length, so
            // no reservation adjustment is needed.
            self.global_right_bitmaps[idx] =
                self.global_right_bitmaps[idx].bitor(&values);
        }
    }
}

pub(crate) struct NestedLoopJoinStream {
    // ========================================================================
    // PROPERTIES:
    // Operator's properties that remain constant
    //
    // Note: The implementation uses the terms left/build-side table and
    // right/probe-side table interchangeably. Treating the left side as the
    // build side is a convention in DataFusion: the planner always tries to
    // swap the smaller table to the left side.
    // ========================================================================
    /// Output schema
    pub(crate) output_schema: Arc<Schema>,
    /// join filter
    pub(crate) join_filter: Option<JoinFilter>,
    /// type of the join
    pub(crate) join_type: JoinType,
    /// the probe-side(right) table data of the nested loop join
    /// `Option` is used because memory-limited path requires resetting it.
    pub(crate) right_data: Option<SendableRecordBatchStream>,
    /// the build-side table data of the nested loop join
    pub(crate) left_data: OnceFut<LeftLoad>,
    /// Projection to construct the output schema from the left and right tables.
    /// Example:
    /// - output_schema: `['a', 'c']`
    /// - left_schema: `['a', 'b']`
    /// - right_schema: `['c']`
    ///
    /// The column indices would be [(left, 0), (right, 0)] -- taking the left
    /// 0th column and right 0th column can construct the output schema.
    ///
    /// Note there are other columns ('b' in the example) still kept after
    /// projection pushdown; this is because they might be used to evaluate
    /// the join filter (e.g., `JOIN ON (b+c)>0`).
    pub(crate) column_indices: Vec<ColumnIndex>,
    /// Join execution metrics
    pub(crate) metrics: NestedLoopJoinMetrics,

    /// `batch_size` from configuration
    batch_size: usize,

    /// See comments in [`need_produce_right_in_final`] for more detail
    should_track_unmatched_right: bool,

    // ========================================================================
    // STATE FLAGS/BUFFERS:
    // Fields that hold intermediate data/flags during execution
    // ========================================================================
    /// State Tracking
    state: NLJState,
    /// Set when the stream terminated by reporting a cancellation.
    ///
    /// `Done` normally means "finished cleanly", and `handle_done` pads an empty
    /// result with one empty batch so an all-filtered join still carries its
    /// schema. A cancelled stream has already returned an error and must not then
    /// emit anything, empty batch included, so it takes the terminal path
    /// directly.
    cancelled_terminally: bool,
    /// Registered watcher for the coordinator's cancellation broadcast.
    ///
    /// Polled on every `poll_next` iteration so a stream parked on its own input
    /// still has a waker registered with the coordinator; without it a peer's
    /// cancellation would not be observed until some unrelated event happened to
    /// wake this task.
    cancel_watch: Option<BoxFuture<'static, ()>>,

    /// Output buffer holds the join result to output. It will emit eagerly when
    /// the threshold is reached.
    output_buffer: Box<BatchCoalescer>,
    /// See comments in [`NLJState::Done`] for its purpose
    handled_empty_output: bool,

    // Buffer(left) side
    // -----------------
    /// The current buffered left data to join
    buffered_left_data: Option<Arc<JoinLeftData>>,
    /// Index into the left buffered batch. Used in `ProbeRight` state
    left_probe_idx: usize,
    /// Index into the left buffered batch. Used in `EmitLeftUnmatched` state
    left_emit_idx: usize,
    /// Should we go back to `BufferingLeft` state again after `EmitLeftUnmatched`
    /// state is over.
    left_exhausted: bool,
    /// If we can buffer all left data in one pass (false means memory-limited multi-pass)
    left_buffered_in_one_pass: bool,

    // Probe(right) side
    // -----------------
    /// The current probe batch to process
    current_right_batch: Option<RecordBatch>,
    // For right join, keep track of matched rows in `current_right_batch`
    // Constructed when fetching each new incoming right batch in `FetchingRight` state.
    current_right_batch_matched: Option<BooleanArray>,

    /// Memory-limited spill fallback state. See [`SpillState`] for details.
    spill_state: SpillState,

    /// Whether this stream is the one responsible for emitting unmatched-left
    /// rows for the current left chunk. Set in the [`NLJState::ProbeEnd`] state,
    /// which is entered exactly once per chunk and owns the single
    /// [`JoinLeftData::report_probe_completed`] call: the stream that drives the
    /// shared probe-threads counter to zero (the last to finish probing) becomes
    /// the emitter. Because the decrement happens once in `ProbeEnd` rather than
    /// in the re-enterable `EmitLeftUnmatched` state, the counter can never be
    /// decremented twice, so it cannot reach zero before all partitions finish
    /// probing (which would otherwise let a partition emit spurious NULL-padded
    /// unmatched-left rows early).
    is_unmatched_left_emitter: bool,
}

pub(crate) struct NestedLoopJoinMetrics {
    /// Join execution metrics
    pub(crate) join_metrics: BuildProbeJoinMetrics,
    /// Selectivity of the join: output_rows / (left_rows * right_rows)
    pub(crate) selectivity: RatioMetrics,
    /// Spill metrics for memory-limited execution
    pub(crate) spill_metrics: SpillMetrics,
}

impl NestedLoopJoinMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            join_metrics: BuildProbeJoinMetrics::new(partition, metrics),
            selectivity: MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .ratio_metrics("selectivity", partition),
            spill_metrics: SpillMetrics::new(metrics, partition),
        }
    }
}

impl Stream for NestedLoopJoinStream {
    type Item = Result<RecordBatch>;

    /// See the comments [`NestedLoopJoinExec`] for high-level design ideas.
    ///
    /// # Implementation
    ///
    /// This function is the entry point of NLJ operator's state machine
    /// transitions. The rough state transition graph is as follow, for more
    /// details see the comment in each state's matching arm.
    ///
    /// ============================
    /// State transition graph:
    /// ============================
    ///
    /// (start) --> BufferingLeft
    /// ----------------------------
    /// BufferingLeft → FetchingRight
    ///
    /// FetchingRight → ProbeRight (if right batch available)
    /// FetchingRight → ProbeEnd (if right exhausted)
    ///
    /// ProbeRight → ProbeRight (next left row or after yielding output)
    /// ProbeRight → EmitRightUnmatched (for special join types like right join)
    /// ProbeRight → FetchingRight (done with the current right batch)
    ///
    /// EmitRightUnmatched → FetchingRight
    ///
    /// ProbeEnd → EmitLeftUnmatched (records whether this stream is the
    /// unmatched-left emitter, then always continues to EmitLeftUnmatched)
    ///
    /// EmitLeftUnmatched → EmitLeftUnmatched (only process 1 chunk for each
    /// iteration)
    /// EmitLeftUnmatched → Done (if finished)
    /// ----------------------------
    /// Done → (end)
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            // A peer partition may have been dropped unfinished at any point,
            // including while this stream was working on the final chunk. The
            // coordinated execution cannot produce a complete result after that,
            // so fail rather than emit output from partial input.
            if !matches!(self.state, NLJState::Done) {
                // Poll a registered watcher rather than only reading the flag:
                // this stream may be about to park on its own input, and the
                // poll leaves a waker with the coordinator so a peer's
                // cancellation actually reaches it.
                if self.cancel_watch.is_none() {
                    self.cancel_watch = match &self.spill_state {
                        SpillState::Active(active) => {
                            Some(active.coordinator.cancellation_watcher())
                        }
                        SpillState::Pending {
                            fallback_coordinator,
                            ..
                        } => Some(fallback_coordinator.cancellation_watcher()),
                        SpillState::Disabled => None,
                    };
                }
                let cancelled = match self.cancel_watch.as_mut() {
                    Some(watch) => watch.poll_unpin(cx).is_ready(),
                    None => false,
                };
                if cancelled {
                    self.state = NLJState::Done;
                    self.cancelled_terminally = true;
                    self.cancel_watch = None;
                    return Poll::Ready(Some(exec_err!(
                        "NestedLoopJoin coordinated fallback was cancelled because \
                         a partition was dropped before finishing"
                    )));
                }
            }

            match self.state {
                // # NLJState transitions
                // --> FetchingRight
                // This state will prepare the left side batches, next state
                // `FetchingRight` is responsible for preparing a single probe
                // side batch, before start joining.
                NLJState::BufferingLeft => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    match self.handle_buffering_left(cx) {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(poll) => return poll,
                    }
                }

                // # NLJState transitions:
                // 1. --> ProbeRight
                //    Start processing the join for the newly fetched right
                //    batch.
                // 2. --> ProbeEnd: When the right side input is exhausted,
                //    probing for the current left chunk is finished.
                //
                // After fetching a new batch from the right side, it will
                // process all rows from the buffered left data:
                // ```text
                // for batch in right_side:
                //     for row in left_buffer:
                //         join(batch, row)
                // ```
                // Note: the implementation does this step incrementally,
                // instead of materializing all intermediate Cartesian products
                // at once in memory.
                //
                // So after the right side input is exhausted, the join phase
                // for the current buffered left data is finished. We go to the
                // `ProbeEnd` state, which records probe completion before the
                // `EmitLeftUnmatched` phase checks if there is any special
                // handling (e.g., in cases like left join).
                NLJState::FetchingRight => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    match self.handle_fetching_right(cx) {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(poll) => return poll,
                    }
                }

                // NLJState transitions:
                // 1. --> ProbeRight(1)
                //    If we have already buffered enough output to yield, it
                //    will first give back control to the parent state machine,
                //    then resume at the same place.
                // 2. --> ProbeRight(2)
                //    After probing one right batch, and evaluating the
                //    join filter on (left-row x right-batch), it will advance
                //    to the next left row, then re-enter the current state and
                //    continue joining.
                // 3. --> FetchRight
                //    After it has done with the current right batch (to join
                //    with all rows in the left buffer), it will go to
                //    FetchRight state to check what to do next.
                NLJState::ProbeRight => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_probe_right() {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // In the `current_right_batch_matched` bitmap, all trues mean
                // it has been output by the join. In this state we have to
                // output unmatched rows for current right batch (with null
                // padding for left relation)
                // Precondition: we have checked the join type so that it's
                // possible to output right unmatched (e.g. it's right join)
                NLJState::EmitRightUnmatched => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_emit_right_unmatched() {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // NLJState transitions:
                // 1. --> EmitLeftUnmatched
                //    Probing for the current left chunk is finished. Report
                //    probe completion exactly once (decrementing the shared
                //    probe-threads counter) and record whether this stream is
                //    the unmatched-left emitter, then always advance to
                //    `EmitLeftUnmatched`.
                NLJState::ProbeEnd => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_probe_end() {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // NLJState transitions:
                // 1. --> EmitLeftUnmatched(1)
                //    If we have already buffered enough output to yield, it
                //    will first give back control to the parent state machine,
                //    then resume at the same place.
                // 2. --> EmitLeftUnmatched(2)
                //    After processing some unmatched rows, it will re-enter
                //    the same state, to check if there are any more final
                //    results to output.
                // 3. --> Done
                //    It has processed all data, go to the final state and ready
                //    to exit.
                // 4. --> BufferingLeft (memory-limited mode only)
                //    When left data was loaded in chunks and more chunks remain,
                //    go back to BufferingLeft to load the next chunk.
                NLJState::EmitLeftUnmatched => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_emit_left_unmatched() {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // Replay all right batches from spill and emit unmatched
                // right rows using the global bitmap accumulated across all
                // left chunks. Only entered in memory-limited mode for join
                // types where `should_track_unmatched_right` is true
                // (RIGHT, FULL, RIGHT SEMI, RIGHT ANTI, RIGHT MARK).
                NLJState::EmitGlobalRightUnmatched => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    match self.handle_emit_global_right_unmatched(cx) {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }

                // The final state and the exit point
                NLJState::Done => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();
                    // counting it in join timer due to there might be some
                    // final resout batches to output in this state

                    let poll = self.handle_done();
                    return self.metrics.join_metrics.baseline.record_poll(poll);
                }
            }
        }
    }
}

impl RecordBatchStream for NestedLoopJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

/// Cancels the coordinated fallback if a stream goes away before finishing.
///
/// The partitions of a coordinated fallback are not independent: chunk
/// advancement needs every one of them to report, so a partition that
/// disappears mid-probe would leave the survivors waiting on a release nobody
/// will ever make, and the coordinator -- owned by the plan, not the stream --
/// would keep holding the chunk it had published. Cancelling is the honest
/// outcome: this execution can no longer produce a complete result, so the
/// remaining partitions are failed rather than left hanging or allowed to
/// report success from partial input.
///
/// A stream that reached `Done` finished its work and does not cancel anything.
impl Drop for NestedLoopJoinStream {
    fn drop(&mut self) {
        if matches!(self.state, NLJState::Done) {
            return;
        }
        // `Active` means this stream was probing shared chunks, so its peers are
        // already relying on coordination and have to be told now.
        //
        // `Pending` is different: it is entered before the shared load decides
        // whether the left side spills at all, so it does not yet imply a
        // coordinated execution. Record the drop instead and let the shared
        // `coordination_started` flag decide: if a peer already coordinates, the
        // recording call itself cancels; if none has yet, the next one to enter
        // the coordinated path does. An execution whose left side fits in memory
        // never enters it and so never cancels -- which is the point, since it
        // has no coordination to lose.
        match &self.spill_state {
            SpillState::Active(active) => Arc::clone(&active.coordinator).cancel(),
            SpillState::Pending {
                fallback_coordinator,
                ..
            } => Arc::clone(fallback_coordinator).record_pending_drop(),
            SpillState::Disabled => {}
        }
    }
}

impl NestedLoopJoinStream {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: Arc<Schema>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        right_data: SendableRecordBatchStream,
        left_data: OnceFut<LeftLoad>,
        column_indices: Vec<ColumnIndex>,
        metrics: NestedLoopJoinMetrics,
        batch_size: usize,
        spill_state: SpillState,
    ) -> Self {
        Self {
            output_schema: Arc::clone(&schema),
            join_filter: filter,
            join_type,
            right_data: Some(right_data),
            column_indices,
            left_data,
            metrics,
            buffered_left_data: None,
            output_buffer: Box::new(BatchCoalescer::new(schema, batch_size)),
            batch_size,
            current_right_batch: None,
            current_right_batch_matched: None,
            state: NLJState::BufferingLeft,
            cancelled_terminally: false,
            cancel_watch: None,
            left_probe_idx: 0,
            left_emit_idx: 0,
            left_exhausted: false,
            left_buffered_in_one_pass: true,
            handled_empty_output: false,
            should_track_unmatched_right: need_produce_right_in_final(join_type),
            spill_state,
            is_unmatched_left_emitter: false,
        }
    }

    /// Returns true if this stream is operating in memory-limited mode
    fn is_memory_limited(&self) -> bool {
        matches!(self.spill_state, SpillState::Active(_))
    }

    /// Enter memory-limited mode with the left side already spilled by the load.
    ///
    /// Every partition resolves the same shared `LeftLoad`, so they all read the
    /// one spill file the load wrote; the left child is never executed a second
    /// time.
    fn enter_memory_limited_mode(
        &mut self,
        left_spill: Arc<LeftSpillData>,
    ) -> Result<()> {
        let SpillState::Pending {
            task_context: context,
            fallback_coordinator,
        } = std::mem::replace(&mut self.spill_state, SpillState::Disabled)
        else {
            return internal_err!(
                "enter_memory_limited_mode called in non-Pending spill state"
            );
        };

        // Separate reservation for the global right bitmaps. These buffers
        // are per-partition (each partition tracks matches against its own
        // right input) and persist across all left chunks.
        let global_right_bitmaps_reservation =
            MemoryConsumer::new("NestedLoopJoinGlobalRightBitmaps".to_string())
                .register(context.memory_pool());

        // Create SpillManager for right-side spilling
        let right_schema = self
            .right_data
            .as_ref()
            .expect("right_data must be present before entering memory-limited mode")
            .schema();
        let right_data = self
            .right_data
            .take()
            .expect("right_data must be present before entering memory-limited mode");
        let right_spill_manager = SpillManager::new(
            context.runtime_env(),
            self.metrics.spill_metrics.clone(),
            right_schema,
        )
        .with_compression_type(context.session_config().spill_compression());

        self.spill_state = SpillState::Active(Box::new(SpillStateActive {
            left_spill,
            left_schema: None,
            coordinator: fallback_coordinator,
            next_chunk_index: 0,
            task_context: Arc::clone(&context),
            right_input: ReplayableStreamSource::new(
                right_data,
                right_spill_manager,
                "NestedLoopJoin right spill",
            ),
            global_right_bitmaps: Vec::new(),
            global_right_bitmaps_reservation,
            right_batch_index: 0,
            chunk_fetch_in_flight: None,
        }));

        // State stays BufferingLeft — next poll will enter
        // handle_buffering_left_memory_limited via is_memory_limited() check
        self.state = NLJState::BufferingLeft;

        Ok(())
    }

    // ==== State handler functions ====

    /// Handle BufferingLeft state - prepare left side batches.
    ///
    /// In standard mode, uses OnceFut to load all left data at once.
    /// In memory-limited mode, incrementally buffers left batches until the
    /// memory budget is reached or the left stream is exhausted.
    fn handle_buffering_left(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        if self.is_memory_limited() {
            self.handle_buffering_left_memory_limited(cx)
        } else {
            // Standard path: use OnceFut
            match self.left_data.get_shared(cx) {
                Poll::Ready(Ok(load)) => match load.as_ref() {
                    LeftLoad::InMemory(left_data) => {
                        self.buffered_left_data = Some(Arc::clone(left_data));
                        self.left_exhausted = true;
                        self.state = NLJState::FetchingRight;
                        ControlFlow::Continue(())
                    }
                    LeftLoad::Spilled(left_spill) => {
                        debug!(
                            "NestedLoopJoin: left side exceeded the budget and was spilled, \
                             entering memory-limited mode"
                        );
                        match self.enter_memory_limited_mode(Arc::clone(left_spill)) {
                            Ok(()) => {
                                // Now that this execution is known to coordinate,
                                // a partition dropped while still `Pending`
                                // becomes a cancellation.
                                if let SpillState::Active(active) = &self.spill_state {
                                    Arc::clone(&active.coordinator).begin_coordination();
                                }
                                ControlFlow::Continue(())
                            }
                            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
                        }
                    }
                },
                Poll::Ready(Err(e)) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
                Poll::Pending => ControlFlow::Break(Poll::Pending),
            }
        }
    }

    /// Memory-limited path for handle_buffering_left.
    ///
    /// Drives an in-flight `next_chunk` future on the coordinator, which
    /// loads (or re-uses) the next per-chunk shared `JoinLeftData`.
    fn handle_buffering_left_memory_limited(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        let build_metric_for_chunk = self.metrics.join_metrics.build_time.clone();
        let SpillState::Active(active) = &mut self.spill_state else {
            unreachable!(
                "handle_buffering_left_memory_limited called without Active spill state"
            );
        };

        // Lazily start a chunk-fetch future for `active.next_chunk_index`.
        if active.chunk_fetch_in_flight.is_none() {
            let coordinator = Arc::clone(&active.coordinator);
            let spill_data = Arc::clone(&active.left_spill);
            let task_context = Arc::clone(&active.task_context);
            let expected = active.next_chunk_index;
            let build_metric = build_metric_for_chunk.clone();
            active.chunk_fetch_in_flight = Some(
                coordinator
                    .next_chunk(expected, spill_data, task_context, build_metric)
                    .boxed(),
            );
        }

        let fut = active
            .chunk_fetch_in_flight
            .as_mut()
            .expect("chunk_fetch_in_flight installed above");
        let result = match fut.poll_unpin(cx) {
            Poll::Ready(r) => r,
            Poll::Pending => return ControlFlow::Break(Poll::Pending),
        };
        active.chunk_fetch_in_flight = None;

        match result {
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
            Ok(None) => {
                // No chunk to deliver: left side fully consumed.
                self.left_exhausted = true;
                if self.is_memory_limited() && self.should_track_unmatched_right {
                    self.right_data = None;
                    self.state = NLJState::EmitGlobalRightUnmatched;
                } else {
                    self.state = NLJState::Done;
                }
                ControlFlow::Continue(())
            }
            Ok(Some((data, is_last))) => {
                // The operator's own work on the delivered chunk: recording
                // metrics, caching the schema and opening the right-side pass.
                // `load_one_chunk` times the reading it does, but a chunk can
                // also be served straight from the coordinator's slot, in which
                // case this is the only build work there is.
                let _build_timer = build_metric_for_chunk.timer();
                let n_rows = data.batch().num_rows();
                self.metrics.join_metrics.build_input_batches.add(1);
                self.metrics.join_metrics.build_input_rows.add(n_rows);
                if active.left_schema.is_none() {
                    active.left_schema = Some(data.batch().schema());
                }
                self.buffered_left_data = Some(data);
                self.left_exhausted = is_last;
                self.left_buffered_in_one_pass = is_last && active.next_chunk_index == 0;

                active.right_batch_index = 0;
                match active.right_input.open_pass() {
                    Ok(stream) => {
                        self.right_data = Some(stream);
                    }
                    Err(e) => {
                        return ControlFlow::Break(Poll::Ready(Some(Err(e))));
                    }
                }

                self.state = NLJState::FetchingRight;
                ControlFlow::Continue(())
            }
        }
    }

    /// Handle FetchingRight state - fetch next right batch and prepare for processing.
    ///
    /// In memory-limited mode during the first pass, each right batch is also
    /// written to a spill file so it can be re-read on subsequent passes.
    fn handle_fetching_right(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        let result = match self
            .right_data
            .as_mut()
            .expect("right_data must be present while fetching right")
            .poll_next_unpin(cx)
        {
            Poll::Ready(result) => result,
            Poll::Pending => return ControlFlow::Break(Poll::Pending),
        };

        let join_metric = self.metrics.join_metrics.join_time.clone();
        let _join_timer = join_metric.timer();

        match result {
            Some(Ok(right_batch)) => {
                // Update metrics
                let right_batch_rows = right_batch.num_rows();
                self.metrics.join_metrics.input_rows.add(right_batch_rows);
                self.metrics.join_metrics.input_batches.add(1);

                // Skip the empty batch
                if right_batch_rows == 0 {
                    return ControlFlow::Continue(());
                }

                self.current_right_batch = Some(right_batch);

                // Prepare right bitmap
                if self.should_track_unmatched_right {
                    let zeroed_buf = BooleanBuffer::new_unset(right_batch_rows);
                    self.current_right_batch_matched =
                        Some(BooleanArray::new(zeroed_buf, None));
                }

                self.left_probe_idx = 0;
                self.state = NLJState::ProbeRight;
                ControlFlow::Continue(())
            }
            Some(Err(e)) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
            None => {
                // Right side exhausted: probing for the current left chunk
                // is finished. `ProbeEnd` reports probe completion before
                // emitting unmatched-left rows.
                self.state = NLJState::ProbeEnd;
                ControlFlow::Continue(())
            }
        }
    }

    /// Handle ProbeRight state - process current probe batch
    fn handle_probe_right(&mut self) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Return any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        // Process current probe state
        match self.process_probe_batch() {
            // State unchanged (ProbeRight)
            // Continue probing until we have done joining the
            // current right batch with all buffered left rows.
            Ok(true) => ControlFlow::Continue(()),
            // To next FetchRightState
            // We have finished joining
            // (cur_right_batch x buffered_left_batches)
            Ok(false) => {
                // Left exhausted, transition to FetchingRight
                self.left_probe_idx = 0;

                // Selectivity Metric: Update total possibilities for the batch (left_rows * right_rows)
                // If memory-limited execution is implemented, this logic must be updated accordingly.
                if let (Ok(left_data), Some(right_batch)) =
                    (self.get_left_data(), self.current_right_batch.as_ref())
                {
                    let left_rows = left_data.batch().num_rows();
                    let right_rows = right_batch.num_rows();
                    self.metrics.selectivity.add_total(left_rows * right_rows);
                }

                if self.should_track_unmatched_right {
                    debug_assert!(
                        self.current_right_batch_matched.is_some(),
                        "If it's required to track matched rows in the right input, the right bitmap must be present"
                    );
                    self.state = NLJState::EmitRightUnmatched;
                } else {
                    self.current_right_batch = None;
                    self.state = NLJState::FetchingRight;
                }
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        }
    }

    /// Handle EmitRightUnmatched state - emit unmatched right rows.
    ///
    /// In memory-limited mode, instead of emitting unmatched right rows
    /// per-batch (which would be incorrect since more left chunks may
    /// match those rows), we merge the bitmap into the global accumulator
    /// and defer emission to `EmitGlobalRightUnmatched`.
    fn handle_emit_right_unmatched(
        &mut self,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // In memory-limited mode, merge bitmap into global and move on
        if self.is_memory_limited() {
            debug_assert!(
                self.current_right_batch_matched.is_some(),
                "right bitmap must be present"
            );
            let bitmap = std::mem::take(&mut self.current_right_batch_matched)
                .expect("right bitmap should be available");
            let (values, _nulls) = bitmap.into_parts();

            if let SpillState::Active(ref mut active) = self.spill_state {
                let idx = active.right_batch_index;
                active.merge_current_right_bitmap(idx, values);
                active.right_batch_index += 1;
            }

            self.current_right_batch = None;
            self.state = NLJState::FetchingRight;
            return ControlFlow::Continue(());
        }

        // Standard (single-pass) mode: emit unmatched right rows immediately
        // Return any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        debug_assert!(
            self.current_right_batch_matched.is_some()
                && self.current_right_batch.is_some(),
            "This state is yielding output for unmatched rows in the current right batch, so both the right batch and the bitmap must be present"
        );
        match self.process_right_unmatched() {
            Ok(Some(batch)) => match self.output_buffer.push_batch(batch) {
                Ok(()) => {
                    debug_assert!(self.current_right_batch.is_none());
                    self.state = NLJState::FetchingRight;
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
            },
            Ok(None) => {
                debug_assert!(self.current_right_batch.is_none());
                self.state = NLJState::FetchingRight;
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        }
    }

    /// Handle ProbeEnd state - record probe completion for the current chunk.
    ///
    /// Entered exactly once per left chunk, when the right side is exhausted.
    /// This is the single place that decrements the shared probe-threads counter
    /// via [`JoinLeftData::report_probe_completed`]: the stream that drives the
    /// counter to zero (the last to finish probing) is the one responsible for
    /// emitting unmatched-left rows, recorded in `is_unmatched_left_emitter`.
    ///
    /// Owning the decrement here — rather than in the re-enterable
    /// `EmitLeftUnmatched` state — makes "decrement exactly once per stream" a
    /// structural property of the state graph, so the counter cannot reach zero
    /// before all partitions finish probing (which would let a partition emit
    /// spurious NULL-padded unmatched-left rows early).
    ///
    /// Always transitions to `EmitLeftUnmatched`.
    fn handle_probe_end(&mut self) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Decrement the shared counter exactly once for this stream/chunk. The
        // last stream to finish probing (the one that drives the counter to
        // zero) becomes the unmatched-left emitter.
        let is_emitter = match self.get_left_data() {
            Ok(left_data) => left_data.report_probe_completed(),
            Err(e) => return ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        };
        self.is_unmatched_left_emitter = is_emitter;
        self.state = NLJState::EmitLeftUnmatched;
        ControlFlow::Continue(())
    }

    /// Handle EmitLeftUnmatched state - emit unmatched left rows.
    ///
    /// In memory-limited mode, after processing all unmatched rows for the
    /// current left chunk, transitions back to `BufferingLeft` to load the
    /// next chunk (if the left stream is not yet exhausted).
    fn handle_emit_left_unmatched(
        &mut self,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Return any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        // Process current unmatched state
        match self.process_left_unmatched() {
            // State unchanged (EmitLeftUnmatched)
            // Continue processing until we have processed all unmatched rows
            Ok(true) => ControlFlow::Continue(()),
            // We have finished processing all unmatched rows for this chunk
            Ok(false) => match self.output_buffer.finish_buffered_batch() {
                Ok(()) => {
                    // Flush any completed batch before transitioning.
                    // This is critical for the memory-limited path: the
                    // ProbeRight results must be emitted before we discard
                    // the current chunk and load the next one.
                    if let Some(poll) = self.maybe_flush_ready_batch() {
                        return ControlFlow::Break(poll);
                    }

                    // Drop our reference to the current chunk's
                    // `JoinLeftData` before releasing the slot. The coordinator
                    // slot holds a strong `Arc` too, so the reservation inside
                    // the chunk is freed only once every probing partition *and*
                    // the slot have let go.
                    //
                    // The slot's reference is load-bearing, not redundant: a
                    // faster partition can reach here and let go before a slower
                    // one has taken the chunk at all, and the slow one is served
                    // from the slot (`Decision::Serve`). The slot therefore has
                    // to keep the chunk alive across that handoff, which is why
                    // it cannot hold it weakly.
                    self.buffered_left_data = None;

                    if self.is_memory_limited() {
                        let is_emitter = self.is_unmatched_left_emitter;
                        if let SpillState::Active(active) = &mut self.spill_state {
                            // The last partition for this chunk (the
                            // unmatched-left emitter elected in `ProbeEnd`)
                            // releases the coordinator slot so the next
                            // leader can load the following chunk.
                            if is_emitter {
                                // Synchronous now, so the slot is freed before
                                // this poll returns. It can no longer be lost by
                                // the stream being dropped mid-release.
                                let coordinator = Arc::clone(&active.coordinator);
                                coordinator.release_chunk(active.next_chunk_index);
                            }
                            active.next_chunk_index += 1;
                        }
                        // `is_unmatched_left_emitter` is recomputed when
                        // `ProbeEnd` is re-entered for the next chunk, so it
                        // does not need to be reset here.
                    }

                    if !self.left_exhausted && self.is_memory_limited() {
                        // More left data to process — go back to
                        // BufferingLeft for the next chunk.
                        self.left_probe_idx = 0;
                        self.left_emit_idx = 0;
                        self.state = NLJState::BufferingLeft;
                    } else if self.is_memory_limited()
                        && self.should_track_unmatched_right
                    {
                        // All left chunks done — emit global right unmatched.
                        // Drop the exhausted right stream so that
                        // EmitGlobalRightUnmatched opens a fresh replay pass
                        // from the spill file.
                        self.right_data = None;
                        self.state = NLJState::EmitGlobalRightUnmatched;
                    } else {
                        self.state = NLJState::Done;
                    }
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
            },
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        }
    }

    /// Handle EmitGlobalRightUnmatched state.
    ///
    /// Replays all right batches from the spill file and emits unmatched
    /// right rows using the global bitmap accumulated across all left chunks.
    fn handle_emit_global_right_unmatched(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Flush any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        // On first entry, open a new replay pass on the right input
        if self.right_data.is_none() {
            let join_metric = self.metrics.join_metrics.join_time.clone();
            let _join_timer = join_metric.timer();
            let SpillState::Active(ref mut active) = self.spill_state else {
                unreachable!("EmitGlobalRightUnmatched without Active spill state");
            };
            active.right_batch_index = 0;
            match active.right_input.open_pass() {
                Ok(stream) => {
                    self.right_data = Some(stream);
                }
                Err(e) => {
                    return ControlFlow::Break(Poll::Ready(Some(Err(e))));
                }
            }
        }

        // Poll the replay stream for the next right batch
        let result = match self
            .right_data
            .as_mut()
            .expect("right_data must be present")
            .poll_next_unpin(cx)
        {
            Poll::Ready(result) => result,
            Poll::Pending => return ControlFlow::Break(Poll::Pending),
        };

        let join_metric = self.metrics.join_metrics.join_time.clone();
        let _join_timer = join_metric.timer();
        match result {
            Some(Ok(right_batch)) => {
                if right_batch.num_rows() == 0 {
                    return ControlFlow::Continue(());
                }

                let SpillState::Active(ref mut active) = self.spill_state else {
                    unreachable!();
                };
                let idx = active.right_batch_index;
                active.right_batch_index += 1;

                // Build BooleanArray from the global bitmap
                let bitmap = if idx < active.global_right_bitmaps.len() {
                    BooleanArray::new(active.global_right_bitmaps[idx].clone(), None)
                } else {
                    // Batch never seen — treat all rows as unmatched
                    BooleanArray::new(
                        BooleanBuffer::new_unset(right_batch.num_rows()),
                        None,
                    )
                };

                let left_schema = Arc::clone(
                    active
                        .left_schema
                        .as_ref()
                        .expect("left_schema must be set"),
                );

                match build_unmatched_batch(
                    &self.output_schema,
                    &right_batch,
                    bitmap,
                    &left_schema,
                    &self.column_indices,
                    self.join_type,
                    JoinSide::Right,
                ) {
                    Ok(Some(batch)) => match self.output_buffer.push_batch(batch) {
                        Ok(()) => ControlFlow::Continue(()),
                        Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
                    },
                    Ok(None) => ControlFlow::Continue(()),
                    Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
                }
            }
            Some(Err(e)) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
            None => {
                // All right batches replayed
                match self.output_buffer.finish_buffered_batch() {
                    Ok(()) => {
                        self.state = NLJState::Done;
                        ControlFlow::Continue(())
                    }
                    Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
                }
            }
        }
    }

    /// Handle Done state - final state processing
    fn handle_done(&mut self) -> Poll<Option<Result<RecordBatch>>> {
        // A cancelled stream already reported its error. Nothing may follow it --
        // not buffered output, and not the empty-result padding below.
        if self.cancelled_terminally {
            return Poll::Ready(None);
        }

        // Return any remaining completed batches before final termination
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return poll;
        }

        // HACK for the doc test in https://github.com/apache/datafusion/blob/main/datafusion/core/src/dataframe/mod.rs#L1265
        // If this operator directly return `Poll::Ready(None)`
        // for empty result, the final result will become an empty
        // batch with empty schema, however the expected result
        // should be with the expected schema for this operator
        if !self.handled_empty_output {
            let zero_count = Count::new();
            if *self.metrics.join_metrics.baseline.output_rows() == zero_count {
                let empty_batch = RecordBatch::new_empty(Arc::clone(&self.output_schema));
                self.handled_empty_output = true;
                return Poll::Ready(Some(Ok(empty_batch)));
            }
        }

        Poll::Ready(None)
    }

    // ==== Core logic handling for each state ====

    /// Returns bool to indicate should it continue probing
    /// true -> continue in the same ProbeRight state
    /// false -> It has done with the (buffered_left x cur_right_batch), go to
    /// next state (ProbeRight)
    fn process_probe_batch(&mut self) -> Result<bool> {
        let left_data = Arc::clone(self.get_left_data()?);
        let right_batch = self
            .current_right_batch
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("Right batch should be available"))?
            .clone();

        // stop probing, the caller will go to the next state
        if self.left_probe_idx >= left_data.batch().num_rows() {
            return Ok(false);
        }

        // ========
        // Join (l_row x right_batch)
        // and push the result into output_buffer
        // ========

        // Special case:
        // When the right batch is very small, join with multiple left rows at once,
        //
        // The regular implementation is not efficient if the plan's right child is
        // very small (e.g. 1 row total), because inside the inner loop of NLJ, it's
        // handling one input right batch at once, if it's not large enough, the
        // overheads like filter evaluation can't be amortized through vectorization.
        debug_assert_ne!(
            right_batch.num_rows(),
            0,
            "When fetching the right batch, empty batches will be skipped"
        );

        let l_row_cnt_ratio = self.batch_size / right_batch.num_rows();
        if l_row_cnt_ratio > 10 {
            // Calculate max left rows to handle at once. This operator tries to handle
            // up to `datafusion.execution.batch_size` rows at once in the intermediate
            // batch.
            let l_row_count = std::cmp::min(
                l_row_cnt_ratio,
                left_data.batch().num_rows() - self.left_probe_idx,
            );

            debug_assert!(
                l_row_count != 0,
                "This function should only be entered when there are remaining left rows to process"
            );
            let joined_batch = self.process_left_range_join(
                &left_data,
                &right_batch,
                self.left_probe_idx,
                l_row_count,
            )?;

            if let Some(batch) = joined_batch {
                self.output_buffer.push_batch(batch)?;
            }

            self.left_probe_idx += l_row_count;

            return Ok(true);
        }

        let l_idx = self.left_probe_idx;
        let joined_batch =
            self.process_single_left_row_join(&left_data, &right_batch, l_idx)?;

        if let Some(batch) = joined_batch {
            self.output_buffer.push_batch(batch)?;
        }

        // ==== Prepare for the next iteration ====

        // Advance left cursor
        self.left_probe_idx += 1;

        // Return true to continue probing
        Ok(true)
    }

    /// Process [l_start_index, l_start_index + l_count) JOIN right_batch
    /// Returns a RecordBatch containing the join results (None if empty)
    ///
    /// Side Effect: If the join type requires, left or right side matched bitmap
    /// will be set for matched indices.
    fn process_left_range_join(
        &mut self,
        left_data: &JoinLeftData,
        right_batch: &RecordBatch,
        l_start_index: usize,
        l_row_count: usize,
    ) -> Result<Option<RecordBatch>> {
        // Construct the Cartesian product between the specified range of left rows
        // and the entire right_batch. First, it calculates the index vectors, then
        // materializes the intermediate batch, and finally applies the join filter
        // to it.
        // -----------------------------------------------------------
        let right_rows = right_batch.num_rows();
        let total_rows = l_row_count * right_rows;

        // Build index arrays for cartesian product: left_range X right_batch
        let left_indices: UInt32Array =
            UInt32Array::from_iter_values((0..l_row_count).flat_map(|i| {
                std::iter::repeat_n((l_start_index + i) as u32, right_rows)
            }));
        let right_indices: UInt32Array = UInt32Array::from_iter_values(
            (0..l_row_count).flat_map(|_| 0..right_rows as u32),
        );

        debug_assert!(
            left_indices.len() == right_indices.len()
                && right_indices.len() == total_rows,
            "The length or cartesian product should be (left_size * right_size)",
        );

        // Evaluate the join filter (if any) over an intermediate batch built
        // using the filter's own schema/column indices.
        let bitmap_combined = if let Some(filter) = &self.join_filter {
            // Build the intermediate batch for filter evaluation
            let intermediate_batch = if filter.schema.fields().is_empty() {
                // Constant predicate (e.g., TRUE/FALSE). Use an empty schema with row_count
                create_record_batch_with_empty_schema(
                    Arc::new((*filter.schema).clone()),
                    total_rows,
                )?
            } else {
                let mut filter_columns: Vec<Arc<dyn Array>> =
                    Vec::with_capacity(filter.column_indices().len());
                for column_index in filter.column_indices() {
                    let array = if column_index.side == JoinSide::Left {
                        let col = left_data.batch().column(column_index.index);
                        take(col.as_ref(), &left_indices, None)?
                    } else {
                        let col = right_batch.column(column_index.index);
                        take(col.as_ref(), &right_indices, None)?
                    };
                    filter_columns.push(array);
                }

                RecordBatch::try_new(Arc::new((*filter.schema).clone()), filter_columns)?
            };

            let filter_result = filter
                .expression()
                .evaluate(&intermediate_batch)?
                .into_array(intermediate_batch.num_rows())?;
            let filter_arr = as_boolean_array(&filter_result)?;

            // Combine with null bitmap to get a unified mask
            boolean_mask_from_filter(filter_arr)
        } else {
            // No filter: all pairs match
            BooleanArray::from(vec![true; total_rows])
        };

        // Update the global left or right bitmap for matched indices
        // -----------------------------------------------------------

        // None means we don't have to update left bitmap for this join type
        let mut left_bitmap = if need_produce_result_in_final(self.join_type) {
            Some(left_data.bitmap().lock())
        } else {
            None
        };

        // 'local' meaning: we want to collect 'is_matched' flag for the current
        // right batch, after it has joining all of the left buffer, here it's only
        // the partial result for joining given left range
        let mut local_right_bitmap = if self.should_track_unmatched_right {
            let mut current_right_batch_bitmap = BooleanBufferBuilder::new(right_rows);
            // Ensure builder has logical length so set_bit is in-bounds
            current_right_batch_bitmap.append_n(right_rows, false);
            Some(current_right_batch_bitmap)
        } else {
            None
        };

        // Set the matched bit for left and right side bitmap
        for (i, is_matched) in bitmap_combined.iter().enumerate() {
            let is_matched = is_matched.ok_or_else(|| {
                internal_datafusion_err!("Must be Some after the previous combining step")
            })?;

            let l_index = l_start_index + i / right_rows;
            let r_index = i % right_rows;

            if let Some(bitmap) = left_bitmap.as_mut()
                && is_matched
            {
                // Map local index back to absolute left index within the batch
                bitmap.set_bit(l_index, true);
            }

            if let Some(bitmap) = local_right_bitmap.as_mut()
                && is_matched
            {
                bitmap.set_bit(r_index, true);
            }
        }

        // Apply the local right bitmap to the global bitmap
        if self.should_track_unmatched_right {
            // Remember to put it back after update
            let global_right_bitmap =
                std::mem::take(&mut self.current_right_batch_matched).ok_or_else(
                    || internal_datafusion_err!("right batch's bitmap should be present"),
                )?;
            let (buf, nulls) = global_right_bitmap.into_parts();
            debug_assert!(nulls.is_none());

            let current_right_bitmap = local_right_bitmap
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Should be Some if the current join type requires right bitmap"
                    )
                })?
                .finish();
            let updated_global_right_bitmap = buf.bitor(&current_right_bitmap);

            self.current_right_batch_matched =
                Some(BooleanArray::new(updated_global_right_bitmap, None));
        }

        // For the following join types: only bitmaps are updated; do not emit rows now
        if matches!(
            self.join_type,
            JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::RightAnti
                | JoinType::RightMark
                | JoinType::RightSemi
        ) {
            return Ok(None);
        }

        // Build the projected output batch (using output schema/column_indices),
        // then apply the bitmap filter to it.
        if self.output_schema.fields().is_empty() {
            // Empty projection: only row count matters
            let row_count = bitmap_combined.true_count();
            return Ok(Some(create_record_batch_with_empty_schema(
                Arc::clone(&self.output_schema),
                row_count,
            )?));
        }

        let mut out_columns: Vec<Arc<dyn Array>> =
            Vec::with_capacity(self.output_schema.fields().len());
        for column_index in &self.column_indices {
            let array = if column_index.side == JoinSide::Left {
                let col = left_data.batch().column(column_index.index);
                take(col.as_ref(), &left_indices, None)?
            } else {
                let col = right_batch.column(column_index.index);
                take(col.as_ref(), &right_indices, None)?
            };
            out_columns.push(array);
        }
        let pre_filtered =
            RecordBatch::try_new(Arc::clone(&self.output_schema), out_columns)?;
        let filtered = filter_record_batch(&pre_filtered, &bitmap_combined)?;
        Ok(Some(filtered))
    }

    /// Process a single left row join with the current right batch.
    /// Returns a RecordBatch containing the join results (None if empty)
    ///
    /// Side Effect: If the join type requires, left or right side matched bitmap
    /// will be set for matched indices.
    fn process_single_left_row_join(
        &mut self,
        left_data: &JoinLeftData,
        right_batch: &RecordBatch,
        l_index: usize,
    ) -> Result<Option<RecordBatch>> {
        let right_row_count = right_batch.num_rows();
        if right_row_count == 0 {
            return Ok(None);
        }

        let cur_right_bitmap = if let Some(filter) = &self.join_filter {
            apply_filter_to_row_join_batch(
                left_data.batch(),
                l_index,
                right_batch,
                filter,
            )?
        } else {
            BooleanArray::from(vec![true; right_row_count])
        };

        self.update_matched_bitmap(l_index, &cur_right_bitmap)?;

        // For the following join types: here we only have to set the left/right
        // bitmap, and no need to output result
        if matches!(
            self.join_type,
            JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::RightAnti
                | JoinType::RightMark
                | JoinType::RightSemi
        ) {
            return Ok(None);
        }

        if !cur_right_bitmap.has_true() {
            // If none of the pairs has passed the join predicate/filter
            Ok(None)
        } else {
            // Use the optimized approach similar to build_intermediate_batch_for_single_left_row
            let join_batch = build_row_join_batch(
                &self.output_schema,
                left_data.batch(),
                l_index,
                right_batch,
                Some(cur_right_bitmap),
                &self.column_indices,
                JoinSide::Left,
            )?;
            Ok(join_batch)
        }
    }

    /// Returns bool to indicate should it continue processing unmatched rows
    /// true -> continue in the same EmitLeftUnmatched state
    /// false -> next state (Done)
    fn process_left_unmatched(&mut self) -> Result<bool> {
        let left_data = self.get_left_data()?;
        let left_batch = left_data.batch();

        // ========
        // Check early return conditions
        // ========

        // Early return if join type can't have unmatched rows
        let join_type_no_produce_left = !need_produce_result_in_final(self.join_type);
        // Stop processing unmatched rows, the caller will go to the next state
        let finished = self.left_emit_idx >= left_batch.num_rows();

        // `ProbeEnd` already recorded whether this stream emits unmatched-left
        // rows. Every probe partition passes through this state, but only the
        // one that finished probing last is the emitter, so this flag is false
        // for the others.
        if join_type_no_produce_left || !self.is_unmatched_left_emitter || finished {
            return Ok(false);
        }

        // ========
        // Process unmatched rows and push the result into output_buffer
        // Each time, the number to process is up to batch size
        // ========
        let start_idx = self.left_emit_idx;
        let end_idx = std::cmp::min(start_idx + self.batch_size, left_batch.num_rows());

        if let Some(batch) =
            self.process_left_unmatched_range(left_data, start_idx, end_idx)?
        {
            self.output_buffer.push_batch(batch)?;
        }

        // ==== Prepare for the next iteration ====
        self.left_emit_idx = end_idx;

        // Return true to continue processing unmatched rows
        Ok(true)
    }

    /// Process unmatched rows from the left data within the specified range.
    /// Returns a RecordBatch containing the unmatched rows (None if empty).
    ///
    /// # Arguments
    /// * `left_data` - The left side data containing the batch and bitmap
    /// * `start_idx` - Start index (inclusive) of the range to process
    /// * `end_idx` - End index (exclusive) of the range to process
    ///
    /// # Safety
    /// The caller is responsible for ensuring that `start_idx` and `end_idx` are
    /// within valid bounds of the left batch. This function does not perform
    /// bounds checking.
    fn process_left_unmatched_range(
        &self,
        left_data: &JoinLeftData,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<Option<RecordBatch>> {
        if start_idx == end_idx {
            return Ok(None);
        }

        // Slice both left batch, and bitmap to range [start_idx, end_idx)
        // The range is bit index (not byte)
        let left_batch = left_data.batch();
        let left_batch_sliced = left_batch.slice(start_idx, end_idx - start_idx);

        // Can this be more efficient?
        let mut bitmap_sliced = BooleanBufferBuilder::new(end_idx - start_idx);
        bitmap_sliced.append_n(end_idx - start_idx, false);
        let bitmap = left_data.bitmap().lock();
        for i in start_idx..end_idx {
            assert!(
                i - start_idx < bitmap_sliced.capacity(),
                "DBG: {start_idx}, {end_idx}"
            );
            bitmap_sliced.set_bit(i - start_idx, bitmap.get_bit(i));
        }
        let bitmap_sliced = BooleanArray::new(bitmap_sliced.finish(), None);

        let right_schema = self
            .right_data
            .as_ref()
            .expect("right_data must be present when building unmatched batch")
            .schema();
        build_unmatched_batch(
            &self.output_schema,
            &left_batch_sliced,
            bitmap_sliced,
            &right_schema,
            &self.column_indices,
            self.join_type,
            JoinSide::Left,
        )
    }

    /// Process unmatched rows from the current right batch and reset the bitmap.
    /// Returns a RecordBatch containing the unmatched right rows (None if empty).
    fn process_right_unmatched(&mut self) -> Result<Option<RecordBatch>> {
        // ==== Take current right batch and its bitmap ====
        let right_batch_bitmap: BooleanArray =
            std::mem::take(&mut self.current_right_batch_matched).ok_or_else(|| {
                internal_datafusion_err!("right bitmap should be available")
            })?;

        let right_batch = self.current_right_batch.take();
        let cur_right_batch = unwrap_or_internal_err!(right_batch);

        let left_data = self.get_left_data()?;
        let left_schema = left_data.batch().schema();

        let res = build_unmatched_batch(
            &self.output_schema,
            &cur_right_batch,
            right_batch_bitmap,
            &left_schema,
            &self.column_indices,
            self.join_type,
            JoinSide::Right,
        );

        // ==== Clean-up ====
        self.current_right_batch_matched = None;

        res
    }

    // ==== Utilities ====

    /// Get the build-side data of the left input, errors if it's None
    fn get_left_data(&self) -> Result<&Arc<JoinLeftData>> {
        self.buffered_left_data
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("LeftData should be available"))
    }

    /// Flush the `output_buffer` if there are batches ready to output
    /// None if no result batch ready.
    fn maybe_flush_ready_batch(&mut self) -> Option<Poll<Option<Result<RecordBatch>>>> {
        if self.output_buffer.has_completed_batch()
            && let Some(batch) = self.output_buffer.next_completed_batch()
        {
            // Update output rows for selectivity metric
            let output_rows = batch.num_rows();
            self.metrics.selectivity.add_part(output_rows);

            return Some(Poll::Ready(Some(Ok(batch))));
        }

        None
    }

    /// After joining (l_index@left_buffer x current_right_batch), it will result
    /// in a bitmap (the same length as current_right_batch) as the join match
    /// result. Use this bitmap to update the global bitmap, for special join
    /// types like full joins.
    ///
    /// Example:
    /// After joining l_index=1 (1-indexed row in the left buffer), and the
    /// current right batch with 3 elements, this function will be called with
    /// arguments: l_index = 1, r_matched = [false, false, true]
    /// - If the join type is FullJoin, the 1-index in the left bitmap will be
    ///   set to true, and also the right bitmap will be bitwise-ORed with the
    ///   input r_matched bitmap.
    /// - For join types that don't require output unmatched rows, this
    ///   function can be a no-op. For inner joins, this function is a no-op; for left
    ///   joins, only the left bitmap may be updated.
    fn update_matched_bitmap(
        &mut self,
        l_index: usize,
        r_matched_bitmap: &BooleanArray,
    ) -> Result<()> {
        let left_data = self.get_left_data()?;

        // 1. Maybe update the left bitmap
        if need_produce_result_in_final(self.join_type) && r_matched_bitmap.has_true() {
            let mut bitmap = left_data.bitmap().lock();
            bitmap.set_bit(l_index, true);
        }

        // 2. Maybe update the right bitmap
        if self.should_track_unmatched_right {
            debug_assert!(self.current_right_batch_matched.is_some());
            // after bit-wise or, it will be put back
            let right_bitmap = std::mem::take(&mut self.current_right_batch_matched)
                .ok_or_else(|| {
                    internal_datafusion_err!("right batch's bitmap should be present")
                })?;
            let (buf, nulls) = right_bitmap.into_parts();
            debug_assert!(nulls.is_none());
            let updated_right_bitmap = buf.bitor(r_matched_bitmap.values());

            self.current_right_batch_matched =
                Some(BooleanArray::new(updated_right_bitmap, None));
        }

        Ok(())
    }
}

// ==== Utilities ====

/// Apply the join filter between:
/// (l_index th row in left buffer) x (right batch)
/// Returns a bitmap, with successfully joined indices set to true
fn apply_filter_to_row_join_batch(
    left_batch: &RecordBatch,
    l_index: usize,
    right_batch: &RecordBatch,
    filter: &JoinFilter,
) -> Result<BooleanArray> {
    debug_assert!(left_batch.num_rows() != 0 && right_batch.num_rows() != 0);

    let intermediate_batch = if filter.schema.fields().is_empty() {
        // If filter is constant (e.g. literal `true`), empty batch can be used
        // in the later filter step.
        create_record_batch_with_empty_schema(
            Arc::new((*filter.schema).clone()),
            right_batch.num_rows(),
        )?
    } else {
        build_row_join_batch(
            &filter.schema,
            left_batch,
            l_index,
            right_batch,
            None,
            &filter.column_indices,
            JoinSide::Left,
        )?
        .ok_or_else(|| internal_datafusion_err!("This function assume input batch is not empty, so the intermediate batch can't be empty too"))?
    };

    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows())?;
    let filter_arr = as_boolean_array(&filter_result)?;

    // Convert boolean array with potential nulls into a unified mask bitmap
    let bitmap_combined = boolean_mask_from_filter(filter_arr);

    Ok(bitmap_combined)
}

/// Convert a boolean filter array into a unified mask bitmap.
///
/// Caution: The filter result is NOT a bitmap; it contains true/false/null values.
/// For example, `1 < NULL` evaluates to NULL. Therefore, we must combine (AND)
/// the boolean array with its null bitmap to construct a unified bitmap.
#[inline]
fn boolean_mask_from_filter(filter_arr: &BooleanArray) -> BooleanArray {
    let (values, nulls) = filter_arr.clone().into_parts();
    match nulls {
        Some(nulls) => BooleanArray::new(nulls.inner() & &values, None),
        None => BooleanArray::new(values, None),
    }
}

/// This function performs the following steps:
/// 1. Apply filter to probe-side batch
/// 2. Broadcast the left row (build_side_batch\[build_side_index\]) to the
///    filtered probe-side batch
/// 3. Concat them together according to `col_indices`, and return the result
///    (None if the result is empty)
///
/// Example:
/// build_side_batch:
/// a
/// ----
/// 1
/// 2
/// 3
///
/// # 0 index element in the build_side_batch (that is `1`) will be used
/// build_side_index: 0
///
/// probe_side_batch:
/// b
/// ----
/// 10
/// 20
/// 30
/// 40
///
/// # After applying it, only index 1 and 3 elements in probe_side_batch will be
/// # kept
/// probe_side_filter:
/// false
/// true
/// false
/// true
///
///
/// # Projections to the build/probe side batch, to construct the output batch
/// col_indices:
/// [(left, 0), (right, 0)]
///
/// build_side: left
///
/// ====
/// Result batch:
/// a b
/// ----
/// 1 20
/// 1 40
fn build_row_join_batch(
    output_schema: &Schema,
    build_side_batch: &RecordBatch,
    build_side_index: usize,
    probe_side_batch: &RecordBatch,
    probe_side_filter: Option<BooleanArray>,
    // See [`NLJStream`] struct's `column_indices` field for more detail
    col_indices: &[ColumnIndex],
    // If the build side is left or right, used to interpret the side information
    // in `col_indices`
    build_side: JoinSide,
) -> Result<Option<RecordBatch>> {
    debug_assert_ne!(build_side, JoinSide::None);

    // TODO(perf): since the output might be projection of right batch, this
    // filtering step is more efficient to be done inside the column_index loop
    let filtered_probe_batch = if let Some(filter) = probe_side_filter {
        &filter_record_batch(probe_side_batch, &filter)?
    } else {
        probe_side_batch
    };

    if filtered_probe_batch.num_rows() == 0 {
        return Ok(None);
    }

    // Edge case: downstream operator does not require any columns from this NLJ,
    // so allow an empty projection.
    // Example:
    //  SELECT DISTINCT 32 AS col2
    //  FROM tab0 AS cor0
    //  LEFT OUTER JOIN tab2 AS cor1
    //  ON ( NULL ) IS NULL;
    if output_schema.fields.is_empty() {
        return Ok(Some(create_record_batch_with_empty_schema(
            Arc::new(output_schema.clone()),
            filtered_probe_batch.num_rows(),
        )?));
    }

    let mut columns: Vec<Arc<dyn Array>> =
        Vec::with_capacity(output_schema.fields().len());

    for column_index in col_indices {
        let array = if column_index.side == build_side {
            // Broadcast the single build-side row to match the filtered
            // probe-side batch length
            let original_left_array = build_side_batch.column(column_index.index);

            // Use `arrow::compute::take` directly for `List(Utf8View)` rather
            // than going through `ScalarValue::to_array_of_size()`, which
            // avoids some intermediate allocations.
            //
            // In other cases, `to_array_of_size()` is faster.
            match original_left_array.data_type() {
                DataType::List(field) | DataType::LargeList(field)
                    if field.data_type() == &DataType::Utf8View =>
                {
                    let indices_iter = std::iter::repeat_n(
                        build_side_index as u64,
                        filtered_probe_batch.num_rows(),
                    );
                    let indices_array = UInt64Array::from_iter_values(indices_iter);
                    take(original_left_array.as_ref(), &indices_array, None)?
                }
                _ => {
                    let scalar_value = ScalarValue::try_from_array(
                        original_left_array.as_ref(),
                        build_side_index,
                    )?;
                    scalar_value.to_array_of_size(filtered_probe_batch.num_rows())?
                }
            }
        } else {
            // Take the filtered probe-side column using compute::take
            Arc::clone(filtered_probe_batch.column(column_index.index))
        };

        columns.push(array);
    }

    Ok(Some(RecordBatch::try_new(
        Arc::new(output_schema.clone()),
        columns,
    )?))
}

/// Special case for `PlaceHolderRowExec`
/// Minimal example:  SELECT 1 WHERE EXISTS (SELECT 1);
//
/// # Return
/// If Some, that's the result batch
/// If None, it's not for this special case. Continue execution.
fn build_unmatched_batch_empty_schema(
    output_schema: &SchemaRef,
    batch_bitmap: &BooleanArray,
    // For left/right/full joins, it needs to fill nulls for another side
    join_type: JoinType,
) -> Result<Option<RecordBatch>> {
    let result_size = match join_type {
        JoinType::Left
        | JoinType::Right
        | JoinType::Full
        | JoinType::LeftAnti
        | JoinType::RightAnti => batch_bitmap.false_count(),
        JoinType::LeftSemi | JoinType::RightSemi => batch_bitmap.true_count(),
        JoinType::LeftMark | JoinType::RightMark => batch_bitmap.len(),
        _ => unreachable!(),
    };

    if output_schema.fields().is_empty() {
        Ok(Some(create_record_batch_with_empty_schema(
            Arc::clone(output_schema),
            result_size,
        )?))
    } else {
        Ok(None)
    }
}

/// Creates an empty RecordBatch with a specific row count.
/// This is useful for cases where we need a batch with the correct schema and row count
/// but no actual data columns (e.g., for constant filters).
fn create_record_batch_with_empty_schema(
    schema: SchemaRef,
    row_count: usize,
) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new()
        .with_match_field_names(true)
        .with_row_count(Some(row_count));

    RecordBatch::try_new_with_options(schema, vec![], &options).map_err(|e| {
        internal_datafusion_err!("Failed to create empty record batch: {}", e)
    })
}

/// # Example:
/// batch:
/// a
/// ----
/// 1
/// 2
/// 3
///
/// batch_bitmap:
/// ----
/// false
/// true
/// false
///
/// another_side_schema:
/// [(b, bool), (c, int32)]
///
/// join_type: JoinType::Left
///
/// col_indices: ...(please refer to the comment in `NLJStream::column_indices``)
///
/// batch_side: right
///
/// # Walkthrough:
///
/// This executor is performing a right join, and the currently processed right
/// batch is as above. After joining it with all buffered left rows, the joined
/// entries are marked by the `batch_bitmap`.
/// This method will keep the unmatched indices on the batch side (right), and pad
/// the left side with nulls. The result would be:
///
/// b          c           a
/// ------------------------
/// Null(bool) Null(Int32) 1
/// Null(bool) Null(Int32) 3
fn build_unmatched_batch(
    output_schema: &SchemaRef,
    batch: &RecordBatch,
    batch_bitmap: BooleanArray,
    // For left/right/full joins, it needs to fill nulls for another side
    another_side_schema: &SchemaRef,
    col_indices: &[ColumnIndex],
    join_type: JoinType,
    batch_side: JoinSide,
) -> Result<Option<RecordBatch>> {
    // Should not call it for inner joins
    debug_assert_ne!(join_type, JoinType::Inner);
    debug_assert_ne!(batch_side, JoinSide::None);

    // Handle special case (see function comment)
    if let Some(batch) =
        build_unmatched_batch_empty_schema(output_schema, &batch_bitmap, join_type)?
    {
        return Ok(Some(batch));
    }

    match join_type {
        JoinType::Full | JoinType::Right | JoinType::Left => {
            if join_type == JoinType::Right {
                debug_assert_eq!(batch_side, JoinSide::Right);
            }
            if join_type == JoinType::Left {
                debug_assert_eq!(batch_side, JoinSide::Left);
            }

            // 1. Filter the batch with *flipped* bitmap
            // 2. Fill left side with nulls
            let flipped_bitmap = not(&batch_bitmap)?;

            // create a record batch, with left_schema, of only one row of all nulls
            let left_null_columns: Vec<Arc<dyn Array>> = another_side_schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), 1))
                .collect();

            // Hack: If the left schema is not nullable, the full join result
            // might contain null, this is only a temporary batch to construct
            // such full join result.
            let nullable_left_schema = Arc::new(Schema::new(
                another_side_schema
                    .fields()
                    .iter()
                    .map(|field| (**field).clone().with_nullable(true))
                    .collect::<Vec<_>>(),
            ));
            let left_null_batch = if nullable_left_schema.fields.is_empty() {
                // Left input can be an empty relation, in this case left relation
                // won't be used to construct the result batch (i.e. not in `col_indices`)
                create_record_batch_with_empty_schema(nullable_left_schema, 0)?
            } else {
                RecordBatch::try_new(nullable_left_schema, left_null_columns)?
            };

            debug_assert_ne!(batch_side, JoinSide::None);
            let opposite_side = batch_side.negate();

            build_row_join_batch(
                output_schema,
                &left_null_batch,
                0,
                batch,
                Some(flipped_bitmap),
                col_indices,
                opposite_side,
            )
        }
        JoinType::RightSemi
        | JoinType::RightAnti
        | JoinType::LeftSemi
        | JoinType::LeftAnti => {
            if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
                debug_assert_eq!(batch_side, JoinSide::Right);
            }
            if matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                debug_assert_eq!(batch_side, JoinSide::Left);
            }

            let bitmap = if matches!(join_type, JoinType::LeftSemi | JoinType::RightSemi)
            {
                batch_bitmap.clone()
            } else {
                not(&batch_bitmap)?
            };

            if !bitmap.has_true() {
                return Ok(None);
            }

            let mut columns: Vec<Arc<dyn Array>> =
                Vec::with_capacity(output_schema.fields().len());

            for column_index in col_indices {
                debug_assert_eq!(column_index.side, batch_side);

                let col = batch.column(column_index.index);
                let filtered_col = filter(col, &bitmap)?;

                columns.push(filtered_col);
            }

            Ok(Some(RecordBatch::try_new(
                Arc::clone(output_schema),
                columns,
            )?))
        }
        JoinType::RightMark | JoinType::LeftMark => {
            if join_type == JoinType::RightMark {
                debug_assert_eq!(batch_side, JoinSide::Right);
            }
            if join_type == JoinType::LeftMark {
                debug_assert_eq!(batch_side, JoinSide::Left);
            }

            let mut columns: Vec<Arc<dyn Array>> =
                Vec::with_capacity(output_schema.fields().len());

            // Hack to deal with the borrow checker
            let mut right_batch_bitmap_opt = Some(batch_bitmap);

            for column_index in col_indices {
                if column_index.side == batch_side {
                    let col = batch.column(column_index.index);

                    columns.push(Arc::clone(col));
                } else if column_index.side == JoinSide::None {
                    let right_batch_bitmap = std::mem::take(&mut right_batch_bitmap_opt);
                    match right_batch_bitmap {
                        Some(right_batch_bitmap) => {
                            columns.push(Arc::new(right_batch_bitmap))
                        }
                        None => unreachable!("Should only be one mark column"),
                    }
                } else {
                    return internal_err!(
                        "Not possible to have this join side for RightMark join"
                    );
                }
            }

            Ok(Some(RecordBatch::try_new(
                Arc::clone(output_schema),
                columns,
            )?))
        }
        _ => internal_err!(
            "If batch is at right side, this function must be handling Full/Right/RightSemi/RightAnti/RightMark joins"
        ),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::pin::Pin;
    use std::time::Duration;

    use super::*;
    use crate::statistics::{StatisticsArgs, StatisticsContext};
    use crate::test::{TestMemoryExec, assert_join_metrics};
    use crate::{
        common, expressions::Column, repartition::RepartitionExec, test::build_table_i32,
    };

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field};
    use bytes::Bytes;
    use datafusion_common::assert_contains;
    use datafusion_common::instant::Instant;
    use datafusion_common::test_util::batches_to_sort_string;
    use datafusion_common_runtime::SpawnedTask;
    use datafusion_execution::disk_manager::{
        DiskManager, DiskManagerBuilder, DiskManagerMode,
    };
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_execution::spill_file::{SpillFile, SpillWriter, TempFileFactory};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion_physical_expr::{Partitioning, PhysicalExpr};
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

    use insta::allow_duplicates;
    use insta::assert_snapshot;
    use rstest::rstest;

    fn delayed_stream(batch: RecordBatch, delay: Duration) -> SendableRecordBatchStream {
        let schema = batch.schema();
        Box::pin(crate::stream::RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                std::thread::sleep(delay);
                Ok(batch)
            }),
        ))
    }

    /// Delays the first item while the spill stream is polled, making an
    /// incorrectly scoped operator timer include the delay.
    struct DelayedReadSpillFile {
        inner: Arc<dyn SpillFile>,
        delay: Duration,
        read_count: Arc<AtomicUsize>,
    }

    impl SpillFile for DelayedReadSpillFile {
        fn path(&self) -> Option<&std::path::Path> {
            self.inner.path()
        }

        fn size(&self) -> Option<u64> {
            self.inner.size()
        }

        fn read_stream(
            &self,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
            let delay = self.delay;
            let read_count = Arc::clone(&self.read_count);
            let mut delay_first_item = true;
            let stream = self.inner.read_stream()?.map(move |item| {
                if delay_first_item {
                    delay_first_item = false;
                    read_count.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(delay);
                }
                item
            });
            Ok(Box::pin(stream))
        }

        fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
            self.inner.open_writer()
        }
    }

    /// Wraps local spill files so replay reads can be delayed deterministically.
    struct DelayedReadTempFileFactory {
        inner: Arc<DiskManager>,
        delay: Duration,
        read_count: Arc<AtomicUsize>,
    }

    impl TempFileFactory for DelayedReadTempFileFactory {
        fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>> {
            Ok(Arc::new(DelayedReadSpillFile {
                inner: self.inner.create_tmp_file(description)?,
                delay: self.delay,
                read_count: Arc::clone(&self.read_count),
            }))
        }
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
        batch_size: Option<usize>,
        sorted_column_names: Vec<&str>,
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();

        let batches = if let Some(batch_size) = batch_size {
            let num_batches = batch.num_rows().div_ceil(batch_size);
            (0..num_batches)
                .map(|i| {
                    let start = i * batch_size;
                    let remaining_rows = batch.num_rows() - start;
                    batch.slice(start, batch_size.min(remaining_rows))
                })
                .collect::<Vec<_>>()
        } else {
            vec![batch]
        };

        let mut sort_info = vec![];
        for name in sorted_column_names {
            let index = schema.index_of(name).unwrap();
            let sort_expr = PhysicalSortExpr::new(
                Arc::new(Column::new(name, index)),
                SortOptions::new(false, false),
            );
            sort_info.push(sort_expr);
        }
        let mut source = TestMemoryExec::try_new(&[batches], schema, None).unwrap();
        if let Some(ordering) = LexOrdering::new(sort_info) {
            source = source.try_with_sort_information(vec![ordering]).unwrap();
        }

        let source = Arc::new(source);
        Arc::new(TestMemoryExec::update_cache(&source))
    }

    /// An input that can be executed only once: later executions yield no batches, the way a
    /// stream backed by an external one-shot iterator behaves.
    #[derive(Debug)]
    struct OneShotExec {
        inner: Arc<dyn ExecutionPlan>,
        executions: Arc<AtomicUsize>,
    }

    impl DisplayAs for OneShotExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "OneShotExec")
        }
    }

    impl ExecutionPlan for OneShotExec {
        fn name(&self) -> &str {
            "OneShotExec"
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            if self.executions.fetch_add(1, Ordering::Relaxed) == 0 {
                self.inner.execute(partition, context)
            } else {
                Ok(Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                    self.inner.schema(),
                    futures::stream::empty(),
                )))
            }
        }
    }

    /// The left side is spilled by the load that consumed it, so nothing asks the left child for
    /// its batches a second time. Before that, the memory-limited fallback re-executed the child
    /// and silently dropped every batch the first pass had already consumed.
    #[tokio::test]
    async fn memory_limited_left_side_reads_the_child_once() -> Result<()> {
        let executions = Arc::new(AtomicUsize::new(0));
        let left = Arc::new(OneShotExec {
            inner: build_left_table(),
            executions: Arc::clone(&executions),
        });
        let right = build_right_table();
        let filter = prepare_join_filter();
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;

        let (_, batches, metrics) =
            join_collect(left, right, &JoinType::Inner, Some(filter), task_ctx).await?;

        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "expected the tight memory limit to spill the left side"
        );
        assert_eq!(
            executions.load(Ordering::Relaxed),
            1,
            "the left child must be executed exactly once"
        );
        // Same answer as the in-memory path (see test_nlj_memory_limited_inner_join).
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b2 | c2 |
        +----+----+----+----+----+----+
        | 5  | 5  | 50 | 2  | 2  | 80 |
        +----+----+----+----+----+----+
        "));
        Ok(())
    }

    fn build_left_table() -> Arc<dyn ExecutionPlan> {
        build_table(
            ("a1", &vec![5, 9, 11]),
            ("b1", &vec![5, 8, 8]),
            ("c1", &vec![50, 90, 110]),
            None,
            Vec::new(),
        )
    }

    fn build_right_table() -> Arc<dyn ExecutionPlan> {
        build_table(
            ("a2", &vec![12, 2, 10]),
            ("b2", &vec![10, 2, 10]),
            ("c2", &vec![40, 80, 100]),
            None,
            Vec::new(),
        )
    }

    async fn run_join_with_child_poll_delays(
        left_delay: Duration,
        right_delay: Duration,
        memory_limited: bool,
    ) -> Result<(Duration, Duration, Duration)> {
        run_join_with_poll_delays(
            left_delay,
            right_delay,
            memory_limited,
            JoinType::Inner,
            None,
        )
        .await
    }

    async fn run_join_with_poll_delays(
        left_delay: Duration,
        right_delay: Duration,
        memory_limited: bool,
        join_type: JoinType,
        spill_read_delay: Option<(Duration, Arc<AtomicUsize>)>,
    ) -> Result<(Duration, Duration, Duration)> {
        let left_batch =
            build_table_i32(("a1", &vec![1]), ("b1", &vec![2]), ("c1", &vec![3]));
        let right_batch =
            build_table_i32(("a2", &vec![4]), ("b2", &vec![5]), ("c2", &vec![6]));
        let left_schema = left_batch.schema();
        let right_schema = right_batch.schema();

        let left_stream = delayed_stream(left_batch.clone(), left_delay);
        let right_stream = delayed_stream(right_batch, right_delay);

        let task_ctx = if let Some((delay, read_count)) = spill_read_delay {
            let inner = Arc::new(
                DiskManagerBuilder::default()
                    .with_mode(DiskManagerMode::OsTmpDirectory)
                    .build()?,
            );
            let runtime = RuntimeEnvBuilder::new()
                .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
                    DiskManagerMode::Custom(Arc::new(DelayedReadTempFileFactory {
                        inner,
                        delay,
                        read_count,
                    })),
                ))
                .build_arc()?;
            Arc::new(TaskContext::default().with_runtime(runtime))
        } else {
            Arc::new(TaskContext::default())
        };
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics = NestedLoopJoinMetrics::new(&metrics_set, 0);
        let build_time = metrics.join_metrics.build_time.clone();
        let join_time = metrics.join_metrics.join_time.clone();
        let (left_data, right_stream, spill_state) = if memory_limited {
            let global_right_bitmaps_reservation =
                MemoryConsumer::new("NestedLoopJoinGlobalRightBitmaps[test]".to_string())
                    .register(task_ctx.memory_pool());
            let spill_manager = SpillManager::new(
                task_ctx.runtime_env(),
                metrics.spill_metrics.clone(),
                Arc::clone(&right_schema),
            );
            let left_spill_manager = SpillManager::new(
                task_ctx.runtime_env(),
                metrics.spill_metrics.clone(),
                Arc::clone(&left_schema),
            );
            let mut left_spill_file =
                left_spill_manager.create_in_progress_file("test left spill")?;
            left_spill_file.append_batch(&left_batch)?;
            let left_spill_file = left_spill_file
                .finish()?
                .expect("the test left spill contains one batch");
            // Two tests share this helper and need different things from it.
            //
            // `join_time_excludes_global_right_unmatched_replay_poll` runs a
            // RIGHT join with `spill_read_delay` and asserts the run performs
            // exactly one spill read, the final right replay. Preloading the
            // left chunk into the coordinator's slot gives it that.
            //
            // `build_time_excludes_spill_stream_poll` runs an INNER join with no
            // `spill_read_delay`; its delay lives in the stream wrapping the
            // left input, and it must be paid while the join is being polled
            // (the helper starts its wall clock at `common::collect` below).
            // Handing that stream to the coordinator keeps the delay on the
            // build side and inside the measured window. An INNER join never
            // reaches `EmitGlobalRightUnmatched`, so the extra left read does
            // not disturb the other test's count.
            let coordinator = Arc::new(FallbackCoordinator::new(
                1,
                need_produce_result_in_final(join_type),
            ));
            let preload_left_chunk = need_produce_result_in_final(join_type)
                || matches!(join_type, JoinType::Right);
            if preload_left_chunk {
                let n_rows = left_batch.num_rows();
                let visited = if need_produce_result_in_final(join_type) {
                    let mut buffer = BooleanBufferBuilder::new(n_rows);
                    buffer.append_n(n_rows, false);
                    buffer
                } else {
                    BooleanBufferBuilder::new(0)
                };
                let chunk = Arc::new(JoinLeftData::new(
                    left_batch.clone(),
                    Mutex::new(visited),
                    AtomicUsize::new(1),
                    MemoryConsumer::new("NestedLoopJoinFallbackChunk[test]".to_string())
                        .register(task_ctx.memory_pool()),
                ));
                let mut inner = coordinator.inner.lock();
                inner.left_exhausted = true;
                inner.current = Some(CurrentChunk {
                    chunk_index: 0,
                    data: chunk,
                    is_last: true,
                });
            } else {
                let mut inner = coordinator.inner.lock();
                inner.left_schema = Some(Arc::clone(&left_schema));
                inner.left_stream = Some(left_stream);
            }
            let active = SpillStateActive {
                left_spill: Arc::new(LeftSpillData {
                    spill_manager: left_spill_manager,
                    spill_file: left_spill_file,
                    schema: Arc::clone(&left_schema),
                }),
                left_schema: Some(Arc::clone(&left_schema)),
                // Preload the single left chunk into the coordinator's slot so
                // `next_chunk` serves it from `current` without reading the
                // spill file. That keeps this helper's premise intact: the left
                // side is supplied directly, so the only spill read is the
                // final right replay in `EmitGlobalRightUnmatched`, which is
                // what `join_time_excludes_global_right_unmatched_replay_poll`
                // counts.
                coordinator,
                next_chunk_index: 0,
                task_context: Arc::clone(&task_ctx),
                right_input: ReplayableStreamSource::new(
                    right_stream,
                    spill_manager,
                    "test right spill",
                ),
                global_right_bitmaps: Vec::new(),
                global_right_bitmaps_reservation,
                right_batch_index: 0,
                chunk_fetch_in_flight: None,
            };
            (
                OnceFut::new(async { internal_err!("unused left data was polled") }),
                Box::pin(crate::EmptyRecordBatchStream::new(Arc::clone(
                    &right_schema,
                ))) as SendableRecordBatchStream,
                SpillState::Active(Box::new(active)),
            )
        } else {
            let reservation = MemoryConsumer::new("NestedLoopJoinLoad[test]".to_string())
                .register(task_ctx.memory_pool());
            (
                OnceFut::new(collect_left_input(
                    left_stream,
                    metrics.join_metrics.clone(),
                    reservation,
                    false,
                    1,
                    None,
                )),
                right_stream,
                SpillState::Disabled,
            )
        };
        let (output_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, &join_type);
        let stream = NestedLoopJoinStream::new(
            Arc::new(output_schema),
            None,
            join_type,
            right_stream,
            left_data,
            column_indices,
            metrics,
            1024,
            spill_state,
        );

        let start = Instant::now();
        let batches = common::collect(Box::pin(stream)).await?;
        let wall_time = start.elapsed();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        Ok((
            Duration::from_nanos(build_time.value() as u64),
            Duration::from_nanos(join_time.value() as u64),
            wall_time,
        ))
    }

    async fn check_child_poll_time_excluded<F, Fut>(mut run: F) -> Result<()>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = Result<(Duration, Duration)>>,
    {
        // Escalating the delay filters out fixed-size scheduler preemption without
        // masking the bug: an incorrectly scoped timer grows with every delay.
        let mut delay = Duration::from_millis(50);
        for attempt in 0..3 {
            let (operator_time, wall_time) = run(delay).await?;
            assert!(
                !operator_time.is_zero(),
                "operator work should still be timed"
            );
            assert!(
                wall_time >= delay,
                "child poll delay should dominate wall time: {wall_time:?} < {delay:?}"
            );
            if operator_time < delay {
                return Ok(());
            }
            assert!(
                attempt < 2,
                "operator time ({operator_time:?}) included the child poll delay ({delay:?})"
            );
            delay *= 4;
        }
        unreachable!()
    }

    #[tokio::test]
    async fn build_time_excludes_left_child_poll() -> Result<()> {
        check_child_poll_time_excluded(|delay| async move {
            let (build_time, _, wall_time) =
                run_join_with_child_poll_delays(delay, Duration::ZERO, false).await?;
            Ok((build_time, wall_time))
        })
        .await
    }

    #[tokio::test]
    async fn join_time_excludes_right_child_poll() -> Result<()> {
        check_child_poll_time_excluded(|delay| async move {
            let (_, join_time, wall_time) =
                run_join_with_child_poll_delays(Duration::ZERO, delay, false).await?;
            Ok((join_time, wall_time))
        })
        .await
    }

    #[tokio::test]
    async fn build_time_excludes_spill_stream_poll() -> Result<()> {
        check_child_poll_time_excluded(|delay| async move {
            let (build_time, _, wall_time) =
                run_join_with_child_poll_delays(delay, Duration::ZERO, true).await?;
            Ok((build_time, wall_time))
        })
        .await
    }

    #[tokio::test]
    async fn join_time_excludes_replayable_input_poll() -> Result<()> {
        check_child_poll_time_excluded(|delay| async move {
            let (_, join_time, wall_time) =
                run_join_with_child_poll_delays(Duration::ZERO, delay, true).await?;
            Ok((join_time, wall_time))
        })
        .await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn join_time_excludes_global_right_unmatched_replay_poll() -> Result<()> {
        check_child_poll_time_excluded(|delay| async move {
            let read_count = Arc::new(AtomicUsize::new(0));
            // The only left chunk is supplied directly, so the sole spill read
            // is the final right replay in EmitGlobalRightUnmatched.
            let (_, join_time, wall_time) = run_join_with_poll_delays(
                Duration::ZERO,
                Duration::ZERO,
                true,
                JoinType::Right,
                Some((delay, Arc::clone(&read_count))),
            )
            .await?;
            assert_eq!(
                read_count.load(Ordering::Relaxed),
                1,
                "EmitGlobalRightUnmatched should replay the right spill exactly once"
            );
            Ok((join_time, wall_time))
        })
        .await
    }

    fn prepare_join_filter() -> JoinFilter {
        let column_indices = vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("x", DataType::Int32, true),
        ]);
        // left.b1!=8
        let left_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;
        // right.b2!=10
        let right_filter = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 1)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;
        // filter = left.b1!=8 and right.b2!=10
        // after filter:
        // left table:
        // ("a1", &vec![5]),
        // ("b1", &vec![5]),
        // ("c1", &vec![50]),
        // right table:
        // ("a2", &vec![12, 2]),
        // ("b2", &vec![10, 2]),
        // ("c2", &vec![40, 80]),
        let filter_expression =
            Arc::new(BinaryExpr::new(left_filter, Operator::And, right_filter))
                as Arc<dyn PhysicalExpr>;

        JoinFilter::new(
            filter_expression,
            column_indices,
            Arc::new(intermediate_schema),
        )
    }

    pub(crate) async fn multi_partitioned_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let partition_count = 4;

        // Redistributing right input
        let right = Arc::new(RepartitionExec::try_new(
            right,
            Partitioning::RoundRobinBatch(partition_count),
        )?) as Arc<dyn ExecutionPlan>;

        // Use the required distribution for nested loop join to test partition data
        let nested_loop_join =
            NestedLoopJoinExec::try_new(left, right, join_filter, join_type, None)?;
        let columns = columns(&nested_loop_join.schema());
        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = nested_loop_join.execute(i, Arc::clone(&context))?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .inspect(|b| {
                        assert!(b.num_rows() <= context.session_config().batch_size())
                    })
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }

        let metrics = nested_loop_join.metrics().unwrap();

        Ok((columns, batches, metrics))
    }

    fn new_task_ctx(batch_size: usize) -> Arc<TaskContext> {
        let base = TaskContext::default();
        // limit max size of intermediate batch used in nlj to 1
        let cfg = base.session_config().clone().with_batch_size(batch_size);
        Arc::new(base.with_session_config(cfg))
    }

    #[rstest]
    #[tokio::test]
    async fn join_inner_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Inner,
            Some(filter),
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b2 | c2 |
        +----+----+----+----+----+----+
        | 5  | 5  | 50 | 2  | 2  | 80 |
        +----+----+----+----+----+----+
        "));

        assert_join_metrics!(metrics, 1);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_left_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Left,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+----+----+----+
        | a1 | b1 | c1  | a2 | b2 | c2 |
        +----+----+-----+----+----+----+
        | 11 | 8  | 110 |    |    |    |
        | 5  | 5  | 50  | 2  | 2  | 80 |
        | 9  | 8  | 90  |    |    |    |
        +----+----+-----+----+----+----+
        "));

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_right_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Right,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+----+----+-----+
        | a1 | b1 | c1 | a2 | b2 | c2  |
        +----+----+----+----+----+-----+
        |    |    |    | 10 | 10 | 100 |
        |    |    |    | 12 | 10 | 40  |
        | 5  | 5  | 50 | 2  | 2  | 80  |
        +----+----+----+----+----+-----+
        "));

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_full_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::Full,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+----+----+-----+
        | a1 | b1 | c1  | a2 | b2 | c2  |
        +----+----+-----+----+----+-----+
        |    |    |     | 10 | 10 | 100 |
        |    |    |     | 12 | 10 | 40  |
        | 11 | 8  | 110 |    |    |     |
        | 5  | 5  | 50  | 2  | 2  | 80  |
        | 9  | 8  | 90  |    |    |     |
        +----+----+-----+----+----+-----+
        "));

        assert_join_metrics!(metrics, 5);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_left_semi_with_filter(
        #[values(1, 2, 16)] batch_size: usize,
    ) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::LeftSemi,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 5  | 5  | 50 |
        +----+----+----+
        "));

        assert_join_metrics!(metrics, 1);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_left_anti_with_filter(
        #[values(1, 2, 16)] batch_size: usize,
    ) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::LeftAnti,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+
        | a1 | b1 | c1  |
        +----+----+-----+
        | 11 | 8  | 110 |
        | 9  | 8  | 90  |
        +----+----+-----+
        "));

        assert_join_metrics!(metrics, 2);

        Ok(())
    }

    #[tokio::test]
    async fn join_has_correct_stats() -> Result<()> {
        let left = build_left_table();
        let right = build_right_table();
        let nested_loop_join = NestedLoopJoinExec::try_new(
            left,
            right,
            None,
            &JoinType::Left,
            Some(vec![1, 2]),
        )?;
        let stats = StatisticsContext::new()
            .compute(&nested_loop_join, &StatisticsArgs::new())?;
        assert_eq!(
            nested_loop_join.schema().fields().len(),
            stats.column_statistics.len(),
        );
        assert_eq!(2, stats.column_statistics.len());
        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_right_semi_with_filter(
        #[values(1, 2, 16)] batch_size: usize,
    ) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::RightSemi,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a2", "b2", "c2"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+
        | a2 | b2 | c2 |
        +----+----+----+
        | 2  | 2  | 80 |
        +----+----+----+
        "));

        assert_join_metrics!(metrics, 1);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_right_anti_with_filter(
        #[values(1, 2, 16)] batch_size: usize,
    ) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::RightAnti,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a2", "b2", "c2"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+
        | a2 | b2 | c2  |
        +----+----+-----+
        | 10 | 10 | 100 |
        | 12 | 10 | 40  |
        +----+----+-----+
        "));

        assert_join_metrics!(metrics, 2);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_left_mark_with_filter(
        #[values(1, 2, 16)] batch_size: usize,
    ) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::LeftMark,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "mark"]);
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+-------+
        | a1 | b1 | c1  | mark  |
        +----+----+-----+-------+
        | 11 | 8  | 110 | false |
        | 5  | 5  | 50  | true  |
        | 9  | 8  | 90  | false |
        +----+----+-----+-------+
        "));

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[rstest]
    #[tokio::test]
    async fn join_right_mark_with_filter(
        #[values(1, 2, 16)] batch_size: usize,
    ) -> Result<()> {
        let task_ctx = new_task_ctx(batch_size);
        let left = build_left_table();
        let right = build_right_table();

        let filter = prepare_join_filter();
        let (columns, batches, metrics) = multi_partitioned_join_collect(
            left,
            right,
            &JoinType::RightMark,
            Some(filter),
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a2", "b2", "c2", "mark"]);

        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+-------+
        | a2 | b2 | c2  | mark  |
        +----+----+-----+-------+
        | 10 | 10 | 100 | false |
        | 12 | 10 | 40  | false |
        | 2  | 2  | 80  | true  |
        +----+----+-----+-------+
        "));

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_overallocation() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            None,
            Vec::new(),
        );
        let right = build_table(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
            None,
            Vec::new(),
        );
        let filter = prepare_join_filter();

        // All join types support memory-limited fallback under
        // multi-partition right inputs (left visited state is shared
        // across partitions via `FallbackCoordinator`).
        let fallback_join_types = vec![
            JoinType::Inner,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::RightMark,
            JoinType::Full,
        ];

        for join_type in &fallback_join_types {
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(100, 1.0)
                .build_arc()?;
            let task_ctx = TaskContext::default().with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            // Should succeed via spill fallback, not OOM
            let _result = multi_partitioned_join_collect(
                Arc::clone(&left),
                Arc::clone(&right),
                join_type,
                Some(filter.clone()),
                task_ctx,
            )
            .await?;
        }

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    // ========================================================================
    // Memory-limited execution tests
    // ========================================================================

    /// Helper to run a NLJ using partition 0 and collect results + metrics.
    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let nested_loop_join =
            NestedLoopJoinExec::try_new(left, right, join_filter, join_type, None)?;
        let columns = columns(&nested_loop_join.schema());
        let stream = nested_loop_join.execute(0, context)?;
        let batches: Vec<RecordBatch> = common::collect(stream)
            .await?
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect();
        let metrics = nested_loop_join.metrics().unwrap();
        Ok((columns, batches, metrics))
    }

    /// Create a TaskContext with tight memory limit and disk spilling enabled.
    fn task_ctx_with_memory_limit(
        memory_limit: usize,
        batch_size: usize,
    ) -> Result<Arc<TaskContext>> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .build_arc()?;
        let cfg = TaskContext::default()
            .session_config()
            .clone()
            .with_batch_size(batch_size);
        let task_ctx = TaskContext::default()
            .with_runtime(runtime)
            .with_session_config(cfg);
        Ok(Arc::new(task_ctx))
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_inner_join() -> Result<()> {
        // Use a very small memory limit to force OOM → fallback to spill.
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::Inner, Some(filter), task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        // Verify spill actually occurred (memory-limited path was taken)
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Result should be identical to the non-memory-limited case
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b2 | c2 |
        +----+----+----+----+----+----+
        | 5  | 5  | 50 | 2  | 2  | 80 |
        +----+----+----+----+----+----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_left_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::Left, Some(filter), task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        // Verify spill actually occurred
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+----+----+----+
        | a1 | b1 | c1  | a2 | b2 | c2 |
        +----+----+-----+----+----+----+
        | 11 | 8  | 110 |    |    |    |
        | 5  | 5  | 50  | 2  | 2  | 80 |
        | 9  | 8  | 90  |    |    |    |
        +----+----+-----+----+----+----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_left_semi_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::LeftSemi, Some(filter), task_ctx)
                .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Left semi: only left rows that matched at least one right row.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 5  | 5  | 50 |
        +----+----+----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_left_anti_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::LeftAnti, Some(filter), task_ctx)
                .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Left anti: left rows that did NOT match any right row.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+
        | a1 | b1 | c1  |
        +----+----+-----+
        | 11 | 8  | 110 |
        | 9  | 8  | 90  |
        +----+----+-----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_left_mark_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::LeftMark, Some(filter), task_ctx)
                .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "mark"]);

        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Left mark: all left rows with a bool column indicating match.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+-------+
        | a1 | b1 | c1  | mark  |
        +----+----+-----+-------+
        | 11 | 8  | 110 | false |
        | 5  | 5  | 50  | true  |
        | 9  | 8  | 90  | false |
        +----+----+-----+-------+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_fits_in_memory_no_spill() -> Result<()> {
        // Use a large memory limit — everything fits, no spilling needed.
        let task_ctx = task_ctx_with_memory_limit(10_000_000, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::Inner, Some(filter), task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        // Verify no spilling occurred (standard OnceFut path was used)
        assert_eq!(
            metrics.spill_count().unwrap_or(0),
            0,
            "Expected no spilling with generous memory limit"
        );

        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b2 | c2 |
        +----+----+----+----+----+----+
        | 5  | 5  | 50 | 2  | 2  | 80 |
        +----+----+----+----+----+----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_empty_inputs() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;

        // Empty left table
        let empty_left = build_table(
            ("a1", &vec![]),
            ("b1", &vec![]),
            ("c1", &vec![]),
            None,
            Vec::new(),
        );
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (_columns, batches, _metrics) =
            join_collect(empty_left, right, &JoinType::Inner, Some(filter), task_ctx)
                .await?;
        assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));

        // Empty right table
        let task_ctx2 = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let empty_right = build_table(
            ("a2", &vec![]),
            ("b2", &vec![]),
            ("c2", &vec![]),
            None,
            Vec::new(),
        );
        let filter2 = prepare_join_filter();

        let (_columns, batches, _metrics) = join_collect(
            left,
            empty_right,
            &JoinType::Inner,
            Some(filter2),
            task_ctx2,
        )
        .await?;
        assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));

        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_no_disk_falls_back_to_oom() -> Result<()> {
        // When disk is disabled, fallback is not possible and OOM should occur.
        use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(100, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));

        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let err = join_collect(left, right, &JoinType::Inner, Some(filter), task_ctx)
            .await
            .unwrap_err();

        assert_contains!(err.to_string(), "Resources exhausted");
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_right_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::Right, Some(filter), task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        // Verify spill actually occurred
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Right join: all right rows appear. Unmatched right rows get NULLs on left.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+----+----+-----+
        | a1 | b1 | c1 | a2 | b2 | c2  |
        +----+----+----+----+----+-----+
        |    |    |    | 10 | 10 | 100 |
        |    |    |    | 12 | 10 | 40  |
        | 5  | 5  | 50 | 2  | 2  | 80  |
        +----+----+----+----+----+-----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_full_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::Full, Some(filter), task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        // Verify spill actually occurred
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Full join: unmatched from both sides appear with NULL padding.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+----+----+-----+
        | a1 | b1 | c1  | a2 | b2 | c2  |
        +----+----+-----+----+----+-----+
        |    |    |     | 10 | 10 | 100 |
        |    |    |     | 12 | 10 | 40  |
        | 11 | 8  | 110 |    |    |     |
        | 5  | 5  | 50  | 2  | 2  | 80  |
        | 9  | 8  | 90  |    |    |     |
        +----+----+-----+----+----+-----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_right_semi_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::RightSemi, Some(filter), task_ctx)
                .await?;

        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Right semi: only right rows that matched at least one left row.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+
        | a2 | b2 | c2 |
        +----+----+----+
        | 2  | 2  | 80 |
        +----+----+----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_right_anti_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::RightAnti, Some(filter), task_ctx)
                .await?;

        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Right anti: right rows that did NOT match any left row.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+
        | a2 | b2 | c2  |
        +----+----+-----+
        | 10 | 10 | 100 |
        | 12 | 10 | 40  |
        +----+----+-----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_right_mark_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            join_collect(left, right, &JoinType::RightMark, Some(filter), task_ctx)
                .await?;

        assert_eq!(columns, vec!["a2", "b2", "c2", "mark"]);

        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling to occur under tight memory limit"
        );

        // Right mark: all right rows with a bool column indicating match.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+-------+
        | a2 | b2 | c2  | mark  |
        +----+----+-----+-------+
        | 10 | 10 | 100 | false |
        | 12 | 10 | 40  | false |
        | 2  | 2  | 80  | true  |
        +----+----+-----+-------+
        "));
        Ok(())
    }

    // ========================================================================
    // Multi-partition memory-limited correctness tests
    //
    // These tests reproduce the cross-partition coordination bug in the
    // memory-limited fallback path: each output partition independently
    // constructs a per-chunk `JoinLeftData` with `AtomicUsize::new(1)`,
    // so left-side visited state is not shared across right partitions.
    // For join types that emit unmatched left rows in the final output
    // (LEFT, LEFT SEMI, LEFT ANTI, LEFT MARK, FULL), this leads to a
    // left row being emitted as unmatched by partitions whose right
    // input did not match it — even when another partition did match.
    // ========================================================================

    /// Build the right table as one batch per row, so RepartitionExec can
    /// distribute rows across multiple output partitions.
    fn build_right_table_one_batch_per_row() -> Arc<dyn ExecutionPlan> {
        build_table(
            ("a2", &vec![12, 2, 10]),
            ("b2", &vec![10, 2, 10]),
            ("c2", &vec![40, 80, 100]),
            Some(1),
            Vec::new(),
        )
    }

    /// Run a NLJ across 4 right partitions under a tight memory limit, so
    /// every output partition takes the memory-limited fallback path. The
    /// right side is shuffled via `RepartitionExec(RoundRobinBatch(4))`.
    async fn multi_partition_memory_limited_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let partition_count = 4;
        let right = Arc::new(RepartitionExec::try_new(
            right,
            Partitioning::RoundRobinBatch(partition_count),
        )?) as Arc<dyn ExecutionPlan>;

        let nested_loop_join =
            NestedLoopJoinExec::try_new(left, right, join_filter, join_type, None)?;
        let columns = columns(&nested_loop_join.schema());

        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = nested_loop_join.execute(i, Arc::clone(&context))?;
            let more = common::collect(stream).await?;
            batches.extend(more.into_iter().filter(|b| b.num_rows() > 0));
        }

        let metrics = nested_loop_join.metrics().unwrap();
        Ok((columns, batches, metrics))
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_multi_partition_left_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table_one_batch_per_row();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) = multi_partition_memory_limited_join_collect(
            left,
            right,
            &JoinType::Left,
            Some(filter),
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling under tight memory limit"
        );

        // Expected output is identical to the single-partition spill path
        // and the multi-partition non-spill path. Each left row appears
        // exactly once: the matched (5,5,50)+(2,2,80) row, plus the two
        // left rows filtered out by `b1 != 8` as unmatched.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+----+----+----+
        | a1 | b1 | c1  | a2 | b2 | c2 |
        +----+----+-----+----+----+----+
        | 11 | 8  | 110 |    |    |    |
        | 5  | 5  | 50  | 2  | 2  | 80 |
        | 9  | 8  | 90  |    |    |    |
        +----+----+-----+----+----+----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_multi_partition_full_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table_one_batch_per_row();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) = multi_partition_memory_limited_join_collect(
            left,
            right,
            &JoinType::Full,
            Some(filter),
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling under tight memory limit"
        );

        // Expected: 1 matched + 2 left-unmatched + 2 right-unmatched.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+----+----+-----+
        | a1 | b1 | c1  | a2 | b2 | c2  |
        +----+----+-----+----+----+-----+
        |    |    |     | 10 | 10 | 100 |
        |    |    |     | 12 | 10 | 40  |
        | 11 | 8  | 110 |    |    |     |
        | 5  | 5  | 50  | 2  | 2  | 80  |
        | 9  | 8  | 90  |    |    |     |
        +----+----+-----+----+----+-----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_multi_partition_left_semi_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        // Two right rows that both match the same left row (5,5,50). Without
        // shared left-visited state across partitions, each matching partition
        // emits the left row once, producing duplicates.
        let right = build_table(
            ("a2", &vec![2, 3, 10]),
            ("b2", &vec![2, 2, 10]),
            ("c2", &vec![80, 70, 100]),
            Some(1),
            Vec::new(),
        );
        let filter = prepare_join_filter();

        let (columns, batches, metrics) = multi_partition_memory_limited_join_collect(
            left,
            right,
            &JoinType::LeftSemi,
            Some(filter),
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1"]);
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling under tight memory limit"
        );

        // Left semi: each left row appears at most once, even if it matches
        // multiple right rows distributed across partitions.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 5  | 5  | 50 |
        +----+----+----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_multi_partition_left_anti_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table_one_batch_per_row();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) = multi_partition_memory_limited_join_collect(
            left,
            right,
            &JoinType::LeftAnti,
            Some(filter),
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1"]);
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling under tight memory limit"
        );

        // Left anti: only left rows with no matching right row.
        // (5,5,50) matches (2,2,80) under the filter, so it must NOT appear.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+
        | a1 | b1 | c1  |
        +----+----+-----+
        | 11 | 8  | 110 |
        | 9  | 8  | 90  |
        +----+----+-----+
        "));
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_multi_partition_left_mark_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table();
        let right = build_right_table_one_batch_per_row();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) = multi_partition_memory_limited_join_collect(
            left,
            right,
            &JoinType::LeftMark,
            Some(filter),
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "mark"]);
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling under tight memory limit"
        );

        // Left mark: every left row appears exactly once with a bool
        // indicating whether it matched at least one right row.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+-------+
        | a1 | b1 | c1  | mark  |
        +----+----+-----+-------+
        | 11 | 8  | 110 | false |
        | 5  | 5  | 50  | true  |
        | 9  | 8  | 90  | false |
        +----+----+-----+-------+
        "));
        Ok(())
    }

    /// A left table with one batch per row, spanning enough rows that the
    /// memory-limited fallback must split it across MULTIPLE chunks. Only
    /// `(5,5,50)` matches the right side under `prepare_join_filter`
    /// (`b1 != 8`); every other row has `b1 = 8` and is therefore an
    /// unmatched left row.
    fn build_left_table_multi_chunk() -> Arc<dyn ExecutionPlan> {
        build_table(
            ("a1", &vec![5, 9, 11, 13, 15, 17, 19, 21]),
            ("b1", &vec![5, 8, 8, 8, 8, 8, 8, 8]),
            ("c1", &vec![50, 90, 110, 130, 150, 170, 190, 210]),
            // One row per batch, so the tight per-chunk memory budget forces
            // the coordinator to load each row as a separate chunk.
            Some(1),
            Vec::new(),
        )
    }

    /// Waker that counts how often it is woken.
    ///
    /// Cancellation tests assert that a parked stream is *woken*, not merely that
    /// a flag flipped, so they need to see the waker fire.
    struct WakeCount(AtomicUsize);

    impl futures::task::ArcWake for WakeCount {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl WakeCount {
        fn new() -> Arc<Self> {
            Arc::new(Self(AtomicUsize::new(0)))
        }

        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }

        fn reset(&self) {
            self.0.store(0, Ordering::SeqCst);
        }
    }

    /// Spills a plan's single partition to a `LeftSpillData`, so a test can hand
    /// the coordinator the same input the fallback path would.
    async fn spill_left_for_test(
        left: Arc<dyn ExecutionPlan>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<Arc<LeftSpillData>> {
        let mut stream = left.execute(0, Arc::clone(&task_ctx))?;
        let schema = stream.schema();
        let spill_manager = SpillManager::new(
            task_ctx.runtime_env(),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(&schema),
        );
        let mut spill_file =
            spill_manager.create_in_progress_file("NLJ left spill (test)")?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                spill_file.append_batch(&batch)?;
            }
        }
        let file = spill_file
            .finish()?
            .expect("the fixture has rows, so a spill file must exist");
        Ok(Arc::new(LeftSpillData {
            spill_manager,
            spill_file: file,
            schema,
        }))
    }

    #[tokio::test]
    async fn nlj_cancel_wakes_stream_parked_on_build_input() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;
        let _chunk = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&ctx), Time::new())
            .await?
            .expect("chunk");
        let make_stream = || {
            let right_schema = build_right_table().schema();
            let (schema, columns) =
                build_join_schema(&spill.schema, &right_schema, &JoinType::Left);
            NestedLoopJoinStream::new(
                Arc::new(schema),
                None,
                JoinType::Left,
                Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                    right_schema,
                    futures::stream::pending::<Result<RecordBatch>>(),
                )),
                OnceFut::new(futures::future::pending::<Result<LeftLoad>>()),
                columns,
                NestedLoopJoinMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                1,
                SpillState::Pending {
                    task_context: Arc::clone(&ctx),
                    fallback_coordinator: Arc::clone(&coordinator),
                },
            )
        };
        let peer = make_stream();
        let mut survivor = make_stream();
        let wakes = WakeCount::new();
        let waker = futures::task::waker(Arc::clone(&wakes));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(survivor.poll_next_unpin(&mut cx).is_pending());
        assert!(matches!(survivor.state, NLJState::BufferingLeft));
        wakes.reset();
        drop(peer);
        // Both streams are parked on a build load that never resolves, so neither
        // ever enters the coordinated path and the drop is only recorded -- which
        // is the correct behaviour, since an execution whose left side might still
        // fit in memory has no coordination to lose.
        assert!(
            !coordinator.is_cancelled(),
            "a pending drop must not cancel before anything coordinates"
        );
        // Once some partition does begin coordinating, the recorded drop applies
        // and has to reach this parked stream.
        Arc::clone(&coordinator).begin_coordination();
        assert!(coordinator.is_cancelled());
        assert!(
            wakes.count() > 0,
            "cancel must wake the survivor waiting on build input"
        );
        Ok(())
    }

    #[tokio::test]
    async fn nlj_cancel_observed_when_watcher_polled_after_cancellation() {
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let before = coordinator.cancellation_watcher();
        coordinator.cancel();
        coordinator.cancel();
        let after = coordinator.cancellation_watcher();
        assert!(before.now_or_never().is_some());
        assert!(after.now_or_never().is_some());
    }

    #[tokio::test]
    async fn nlj_cancel_reaches_every_watcher_and_survives_waker_replacement() {
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let mut first = coordinator.cancellation_watcher();
        let mut second = coordinator.cancellation_watcher();
        let old_count = WakeCount::new();
        let new_count = WakeCount::new();
        let other_count = WakeCount::new();
        let old_waker = futures::task::waker(Arc::clone(&old_count));
        let new_waker = futures::task::waker(Arc::clone(&new_count));
        let other_waker = futures::task::waker(Arc::clone(&other_count));
        assert!(
            first
                .poll_unpin(&mut std::task::Context::from_waker(&old_waker))
                .is_pending()
        );
        assert!(
            first
                .poll_unpin(&mut std::task::Context::from_waker(&new_waker))
                .is_pending()
        );
        assert!(
            second
                .poll_unpin(&mut std::task::Context::from_waker(&other_waker))
                .is_pending()
        );
        coordinator.notify.notify_waiters();
        assert_eq!(new_count.count(), 0);
        assert_eq!(other_count.count(), 0);
        coordinator.cancel();
        coordinator.cancel();
        assert!(new_count.count() > 0);
        assert!(other_count.count() > 0);
        assert!(first.now_or_never().is_some());
        assert!(second.now_or_never().is_some());
    }

    #[tokio::test]
    async fn nlj_normal_completion_does_not_cancel_peers() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (plan, ctx) = cancellation_test_plan()?;
            let mut rows = 0;
            for partition in 0..2 {
                let batches =
                    common::collect(plan.execute(partition, Arc::clone(&ctx))?).await?;
                rows += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                assert!(!plan.fallback_coordinator.is_cancelled());
            }
            assert_eq!(rows, 9);
            assert_eq!(ctx.memory_pool().reserved(), 0);
            Ok(())
        })
        .await
        .expect("normal execution hung")
    }

    /// A stream that ends in an error is unfinished, so dropping it cancels the
    /// peers.
    ///
    /// The error path reaches `Drop` without passing through `Done`. The build
    /// side has to resolve for the poll to get as far as the failing right input,
    /// so this uses a `LeftLoad::Spilled` future rather than a pending one, and
    /// asserts the injected error actually surfaced before the drop -- otherwise
    /// the test would silently degrade into "an unstarted stream cancels", which
    /// other tests already cover.
    #[tokio::test]
    async fn nlj_errored_stream_drop_cancels_peers() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;

        let right_schema = build_right_table().schema();
        let (schema, columns) =
            build_join_schema(&spill.schema, &right_schema, &JoinType::Left);
        let left_spill = Arc::clone(&spill);
        let mut failing = NestedLoopJoinStream::new(
            Arc::new(schema),
            None,
            JoinType::Left,
            Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                right_schema,
                futures::stream::once(async {
                    exec_err!("injected right-input failure")
                }),
            )),
            OnceFut::new(async move { Ok(LeftLoad::Spilled(left_spill)) }),
            columns,
            NestedLoopJoinMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            1,
            SpillState::Pending {
                task_context: Arc::clone(&ctx),
                fallback_coordinator: Arc::clone(&coordinator),
            },
        );

        // Drive it until the injected error comes out. Bounded so a fixture that
        // stops reaching the right input fails instead of spinning.
        let err = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match failing.next().await {
                    Some(Err(e)) => return e,
                    Some(Ok(_)) => {}
                    None => panic!("stream finished without surfacing the error"),
                }
            }
        })
        .await
        .expect("fixture never reached its failing right input");
        assert_contains!(err.to_string(), "injected right-input failure");
        assert!(
            !matches!(failing.state, NLJState::Done),
            "an errored stream must not look finished"
        );
        assert!(!coordinator.is_cancelled());

        drop(failing);
        assert!(
            coordinator.is_cancelled(),
            "an unfinished stream must cancel its peers when dropped, including \
             one that ended in an error"
        );
        Ok(())
    }

    /// A survivor parked on the global-right replay must be woken by a peer's
    /// cancellation.
    ///
    /// `EmitGlobalRightUnmatched` is only reachable with `Active` spill state, so
    /// the fixture installs one rather than assigning the state on top of
    /// `Pending` -- otherwise the handler reaches its pending read through a path
    /// a real execution never takes, and the test would be checking a `Pending`
    /// watcher while claiming to check the replay configuration. The replay
    /// reader is injected directly, which skips the reopen step; that is the
    /// shortcut here, and reopening itself is covered elsewhere.
    #[tokio::test]
    async fn nlj_cancel_wakes_stream_parked_on_global_right_replay() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;
        let _chunk = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&ctx), Time::new())
            .await?
            .expect("chunk");

        let right_schema = build_right_table().schema();
        let make_stream = || {
            let (schema, columns) =
                build_join_schema(&spill.schema, &right_schema, &JoinType::Full);
            let left_spill = Arc::clone(&spill);
            NestedLoopJoinStream::new(
                Arc::new(schema),
                None,
                JoinType::Full,
                Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                    Arc::clone(&right_schema),
                    futures::stream::pending::<Result<RecordBatch>>(),
                )),
                OnceFut::new(async move { Ok(LeftLoad::Spilled(left_spill)) }),
                columns,
                NestedLoopJoinMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                1,
                SpillState::Pending {
                    task_context: Arc::clone(&ctx),
                    fallback_coordinator: Arc::clone(&coordinator),
                },
            )
        };
        let peer = make_stream();
        let mut survivor = make_stream();

        // Reach `Active` the way execution does, by letting the spilled build
        // side resolve, then park in the replay stage.
        let wakes = WakeCount::new();
        let waker = futures::task::waker(Arc::clone(&wakes));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(survivor.poll_next_unpin(&mut cx).is_pending());
        assert!(
            matches!(survivor.spill_state, SpillState::Active(_)),
            "the global-right replay only exists with Active spill state"
        );

        survivor.state = NLJState::EmitGlobalRightUnmatched;
        survivor.left_exhausted = true;
        // Inject the replay reader so the handler parks on it rather than
        // reopening the spill file, which other tests cover.
        survivor.right_data =
            Some(Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                Arc::clone(&right_schema),
                futures::stream::pending::<Result<RecordBatch>>(),
            )));
        assert!(survivor.poll_next_unpin(&mut cx).is_pending());
        assert!(matches!(survivor.state, NLJState::EmitGlobalRightUnmatched));
        wakes.reset();

        // Poll the peer into `Active` first, so its drop exercises the real
        // wiring: an `Active` partition disappearing cancels immediately, with no
        // test standing in for the coordinated path being entered.
        let mut peer = peer;
        assert!(peer.poll_next_unpin(&mut cx).is_pending());
        assert!(matches!(peer.spill_state, SpillState::Active(_)));
        wakes.reset();
        drop(peer);
        assert!(coordinator.is_cancelled());
        assert!(
            wakes.count() > 0,
            "cancel must wake a survivor parked on the global-right replay"
        );

        // And the wake must actually surface the cancellation, not just tick.
        match survivor.poll_next_unpin(&mut cx) {
            Poll::Ready(Some(Err(e))) => {
                assert_contains!(e.to_string(), "cancelled");
            }
            _ => panic!("the woken survivor must report the cancellation"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn nlj_cancel_wakes_stream_parked_on_right_input() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;
        let _chunk = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&ctx), Time::new())
            .await?
            .expect("chunk");
        let make_stream = || {
            let right_schema = build_right_table().schema();
            let (schema, columns) =
                build_join_schema(&spill.schema, &right_schema, &JoinType::Left);
            let left_spill = Arc::clone(&spill);
            NestedLoopJoinStream::new(
                Arc::new(schema),
                None,
                JoinType::Left,
                Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                    right_schema,
                    futures::stream::pending::<Result<RecordBatch>>(),
                )),
                OnceFut::new(async move { Ok(LeftLoad::Spilled(left_spill)) }),
                columns,
                NestedLoopJoinMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                1,
                SpillState::Pending {
                    task_context: Arc::clone(&ctx),
                    fallback_coordinator: Arc::clone(&coordinator),
                },
            )
        };
        let peer = make_stream();
        let mut survivor = make_stream();
        let wakes = WakeCount::new();
        let waker = futures::task::waker(Arc::clone(&wakes));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(survivor.poll_next_unpin(&mut cx).is_pending());
        assert!(matches!(survivor.state, NLJState::FetchingRight));
        wakes.reset();
        // Poll the peer into `Active` first, so its drop exercises the real
        // wiring: an `Active` partition disappearing cancels immediately, with no
        // test standing in for the coordinated path being entered.
        let mut peer = peer;
        assert!(peer.poll_next_unpin(&mut cx).is_pending());
        assert!(matches!(peer.spill_state, SpillState::Active(_)));
        wakes.reset();
        drop(peer);
        assert!(coordinator.is_cancelled());
        assert!(
            wakes.count() > 0,
            "cancel must wake the survivor waiting on right input"
        );
        Ok(())
    }

    /// Same terminal behaviour once a partition has already emitted output.
    ///
    /// A cancellation that arrives before any row is produced is the easy case.
    /// Here both partitions emit first -- which is also what carries them into
    /// the coordinated path -- and the survivor must still end in an error
    /// followed by `None`, not resume delivering rows from a left side that is
    /// now missing a prober.
    ///
    /// This does not assert that the coalescer holds a partial batch at the
    /// moment of cancellation; nothing here forces it to. The buffered case is
    /// covered by `nlj_cancellation_after_buffered_rows_ends_without_output`,
    /// which establishes that premise before checking the terminal sequence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn nlj_cancellation_after_emitting_output_ends_in_error() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(50, 1.0)
            .build_arc()?;
        // Large enough that the coalescer batches rows rather than emitting each
        // one, so this exercises a different output path than the shared
        // `batch_size = 1` fixtures.
        let cfg = TaskContext::default()
            .session_config()
            .clone()
            .with_batch_size(8192);
        let ctx = Arc::new(
            TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(cfg),
        );
        let right = Arc::new(RepartitionExec::try_new(
            build_right_table_one_batch_per_row(),
            Partitioning::RoundRobinBatch(2),
        )?) as Arc<dyn ExecutionPlan>;
        let plan = Arc::new(NestedLoopJoinExec::try_new(
            build_left_table(),
            right,
            None,
            &JoinType::Left,
            None,
        )?);

        let mut peer = plan.execute(0, Arc::clone(&ctx))?;
        let mut survivor = plan.execute(1, Arc::clone(&ctx))?;

        // Both partitions produce output first, which is what carries them into
        // the coordinated path -- a peer dropped while still `Pending` would only
        // record the drop, since the left side might not spill at all.
        peer.next().await.expect("peer output")?;
        survivor.next().await.expect("survivor output")?;

        drop(peer);

        // Terminal behaviour must be an error, then nothing -- never a flush of
        // rows produced before a partition went missing.
        let mut saw_error = false;
        for _ in 0..8 {
            match survivor.next().await {
                Some(Err(_)) => {
                    saw_error = true;
                    break;
                }
                Some(Ok(_)) => {}
                None => break,
            }
        }
        assert!(saw_error, "the survivor must report the cancellation");
        assert!(
            survivor.next().await.is_none(),
            "nothing may follow the cancellation error"
        );
        Ok(())
    }

    #[tokio::test]
    async fn nlj_stream_stops_producing_after_cancellation_error() -> Result<()> {
        let (plan, ctx) = cancellation_test_plan()?;
        let mut peer = plan.execute(0, Arc::clone(&ctx))?;
        let mut survivor = plan.execute(1, Arc::clone(&ctx))?;
        peer.next().await.expect("output")?;
        survivor.next().await.expect("output")?;
        drop(peer);
        assert!(survivor.next().await.expect("cancellation").is_err());
        let after_error = survivor.next().await;
        assert!(
            after_error.is_none(),
            "cancellation did not discard buffered output"
        );
        Ok(())
    }

    #[tokio::test]
    async fn nlj_cancel_before_watcher_registration_is_not_lost() -> Result<()> {
        let ctx = Arc::new(TaskContext::default());
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;
        {
            let mut inner = coordinator.inner.lock();
            inner.left_schema = Some(Arc::clone(&spill.schema));
            inner.left_stream =
                Some(Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                    Arc::clone(&spill.schema),
                    futures::stream::pending::<Result<RecordBatch>>(),
                )));
        }
        coordinator
            .cancel_at_leader_claim
            .store(1, Ordering::SeqCst);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            Arc::clone(&coordinator).next_chunk(0, spill, ctx, Time::new()),
        )
        .await;
        assert!(coordinator.is_cancelled());
        assert!(
            result.is_ok(),
            "cancel before watcher registration was lost"
        );
        assert!(result.unwrap().is_err());
        Ok(())
    }

    /// Chunk-progress notifications must not disturb a loader's cancellation
    /// watcher.
    ///
    /// The re-arm gap the review found came from watching the shared `notify`,
    /// where an unrelated wake forced a re-register and could lose a concurrent
    /// cancel. The watcher now waits on a dedicated `cancel_notify` that only
    /// `cancel` ever signals, so there is no re-arm to race: any wake is a real
    /// cancellation. This drives unrelated `notify_waiters()` traffic past a
    /// parked loader and then cancels for real.
    #[tokio::test]
    async fn nlj_chunk_progress_does_not_resolve_cancel_watcher() -> Result<()> {
        let ctx = Arc::new(TaskContext::default());
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;
        {
            let mut inner = coordinator.inner.lock();
            inner.left_schema = Some(Arc::clone(&spill.schema));
            inner.left_stream =
                Some(Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                    Arc::clone(&spill.schema),
                    futures::stream::pending::<Result<RecordBatch>>(),
                )));
        }
        let mut load = Arc::clone(&coordinator)
            .next_chunk(0, spill, ctx, Time::new())
            .boxed();
        assert!(futures::poll!(load.as_mut()).is_pending());

        // Unrelated progress traffic: must neither complete nor cancel the load.
        for _ in 0..5 {
            coordinator.notify.notify_waiters();
            assert!(
                futures::poll!(load.as_mut()).is_pending(),
                "chunk-progress traffic must not resolve the cancellation watcher"
            );
        }
        assert!(!coordinator.is_cancelled());

        // A real cancellation must still be observed while the read is parked.
        coordinator.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), load)
            .await
            .expect("a parked loader must observe cancellation");
        assert!(result.is_err());
        assert!(coordinator.inner.lock().current.is_none());
        Ok(())
    }

    /// Entering the coordinated path applies a drop recorded before it.
    ///
    /// This is the drop-then-coordinate direction: the deferral holds while
    /// nobody coordinates, and `begin_coordination` converts it. The opposite
    /// order -- a peer already `Active` when the drop happens -- runs on a real
    /// plan in `nlj_pending_drop_cancels_active_peer_real_plan`, since the two
    /// orders take different branches and only both together cover the flags.
    #[tokio::test]
    async fn nlj_begin_coordination_applies_prior_pending_drop() -> Result<()> {
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));

        // A partition goes away while still `Pending`: recorded, not applied.
        Arc::clone(&coordinator).record_pending_drop();
        assert!(
            !coordinator.is_cancelled(),
            "a pending drop must not cancel before anything coordinates"
        );

        // A peer then enters the coordinated path and picks the drop up.
        Arc::clone(&coordinator).begin_coordination();
        assert!(
            coordinator.is_cancelled(),
            "entering the coordinated path must apply a recorded pending drop"
        );
        Ok(())
    }

    /// A recorded pending drop is inert if the execution never coordinates.
    #[tokio::test]
    async fn nlj_pending_drop_stays_inert_without_coordination() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;

        Arc::clone(&coordinator).record_pending_drop();
        // Nobody calls `begin_coordination`, so chunks are still served.
        let served = Arc::clone(&coordinator)
            .next_chunk(0, spill, Arc::clone(&ctx), Time::new())
            .await?;
        assert!(
            served.is_some(),
            "a recorded pending drop must not block chunk service on its own"
        );
        assert!(!coordinator.is_cancelled());
        Ok(())
    }

    /// Dropping an unfinished partition must not fail its peers when the left
    /// side turns out to fit in memory.
    ///
    /// Every eligible execution passes through `SpillState::Pending` before the
    /// shared load decides between `InMemory` and `Spilled`. With an ample pool
    /// the load resolves to `InMemory`, there is no shared chunk counter, and the
    /// right partitions stay independent -- so a dropped peer has nothing to
    /// coordinate and must not cancel anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn nlj_drop_pending_partition_does_not_cancel_when_left_fits_in_memory()
    -> Result<()> {
        // Ample pool: the left side never spills, so the fallback is never used.
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let right = Arc::new(RepartitionExec::try_new(
            build_right_table_one_batch_per_row(),
            Partitioning::RoundRobinBatch(2),
        )?) as Arc<dyn ExecutionPlan>;
        let plan = Arc::new(NestedLoopJoinExec::try_new(
            build_left_table(),
            right,
            None,
            &JoinType::Left,
            None,
        )?);

        let abandoned = plan.execute(0, Arc::clone(&task_ctx))?;
        let survivor = plan.execute(1, Arc::clone(&task_ctx))?;
        drop(abandoned);

        let batches = common::collect(survivor).await?;
        assert!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>() > 0,
            "the survivor must still produce its rows when nothing spilled"
        );
        Ok(())
    }

    #[tokio::test]
    async fn nlj_pending_drop_cancels_active_peer_real_plan() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (plan, ctx) = cancellation_test_plan()?;
            let pending = plan.execute(0, Arc::clone(&ctx))?;
            let mut active = plan.execute(1, Arc::clone(&ctx))?;
            active.next().await.expect("active output")?;
            assert!(plan.fallback_coordinator.inner.lock().current.is_some());
            drop(pending);
            // The drop must have cancelled immediately: a peer is already
            // coordinating, so this is not a deferred record.
            assert!(
                plan.fallback_coordinator.is_cancelled(),
                "dropping a pending peer must cancel once a peer coordinates"
            );
            let result = common::collect(active).await;
            assert!(
                result.is_err(),
                "already-active survivor must fail when pending peer disappears"
            );
            assert_eq!(ctx.memory_pool().reserved(), 0);
            Ok(())
        })
        .await
        .expect("survivor hung")
    }

    #[tokio::test]
    async fn nlj_cancellation_after_buffered_rows_ends_without_output() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;
        let _chunk = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&ctx), Time::new())
            .await?
            .expect("chunk");
        let right_batches =
            common::collect(build_right_table().execute(0, Arc::clone(&ctx))?).await?;
        let make_stream = |emit_input: bool| {
            let right_schema = build_right_table().schema();
            let (schema, columns) =
                build_join_schema(&spill.schema, &right_schema, &JoinType::Left);
            let left_spill = Arc::clone(&spill);
            let batches = if emit_input {
                vec![Ok(right_batches[0].slice(0, 1))]
            } else {
                vec![]
            };
            NestedLoopJoinStream::new(
                Arc::new(schema),
                None,
                JoinType::Left,
                Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                    right_schema,
                    futures::stream::iter(batches)
                        .chain(futures::stream::pending::<Result<RecordBatch>>()),
                )),
                OnceFut::new(async move { Ok(LeftLoad::Spilled(left_spill)) }),
                columns,
                NestedLoopJoinMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                8192,
                SpillState::Pending {
                    task_context: Arc::clone(&ctx),
                    fallback_coordinator: Arc::clone(&coordinator),
                },
            )
        };
        let mut peer = make_stream(false);
        let mut survivor = make_stream(true);
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(peer.poll_next_unpin(&mut cx).is_pending());
        assert!(matches!(peer.spill_state, SpillState::Active(_)));
        assert!(survivor.poll_next_unpin(&mut cx).is_pending());
        let buffered = survivor.output_buffer.get_buffered_rows();
        assert!(buffered > 0 && buffered < 8192);
        assert!(!survivor.output_buffer.has_completed_batch());
        drop(peer);
        assert!(coordinator.is_cancelled());
        assert!(matches!(
            survivor.poll_next_unpin(&mut cx),
            Poll::Ready(Some(Err(_)))
        ));
        let after = survivor.poll_next_unpin(&mut cx);
        assert!(
            matches!(after, Poll::Ready(None)),
            "cancellation must terminate without an output batch"
        );
        Ok(())
    }

    fn cancellation_test_plan() -> Result<(Arc<NestedLoopJoinExec>, Arc<TaskContext>)> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(50, 1.0)
            .build_arc()?;
        let cfg = TaskContext::default()
            .session_config()
            .clone()
            .with_batch_size(1);
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(cfg),
        );
        let right = Arc::new(RepartitionExec::try_new(
            build_right_table_one_batch_per_row(),
            Partitioning::RoundRobinBatch(2),
        )?) as Arc<dyn ExecutionPlan>;
        let plan = Arc::new(NestedLoopJoinExec::try_new(
            build_left_table(),
            right,
            None,
            &JoinType::Left,
            None,
        )?);
        Ok((plan, task_ctx))
    }

    #[tokio::test]
    async fn nlj_drop_pending_partition_does_not_retain_chunk_memory() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (plan, ctx) = cancellation_test_plan()?;
            let abandoned = plan.execute(0, Arc::clone(&ctx))?;
            let survivor = plan.execute(1, Arc::clone(&ctx))?;
            drop(abandoned);
            let _ = common::collect(survivor).await;
            assert_eq!(
                ctx.memory_pool().reserved(),
                0,
                "pending partition drop must not strand the chunk"
            );
            Ok(())
        })
        .await
        .expect("stream hung")
    }

    #[tokio::test]
    async fn nlj_active_survivor_observes_cancel() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (plan, ctx) = cancellation_test_plan()?;
            let mut abandoned = plan.execute(0, Arc::clone(&ctx))?;
            let mut survivor = plan.execute(1, Arc::clone(&ctx))?;
            abandoned.next().await.expect("first output")?;
            survivor.next().await.expect("first output")?;
            assert!(
                plan.fallback_coordinator
                    .inner
                    .lock()
                    .current
                    .as_ref()
                    .expect("chunk")
                    .is_last
            );
            drop(abandoned);
            assert!(
                plan.fallback_coordinator.inner.lock().cancelled,
                "Drop must have cancelled"
            );
            let result = common::collect(survivor).await;
            assert!(
                result.is_err(),
                "survivor holding final chunk reported success after cancellation"
            );
            Ok(())
        })
        .await
        .expect("stream hung")
    }

    async fn run_paused_loader_cancellation(complete_read: bool) -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let pool = Arc::clone(&runtime.memory_pool);
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill = spill_left_for_test(build_left_table(), Arc::clone(&ctx)).await?;
        let batches =
            common::collect(build_left_table().execute(0, Arc::clone(&ctx))?).await?;
        let batch = batches[0].clone();
        let schema = batch.schema();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut inner = coordinator.inner.lock();
            inner.left_schema = Some(Arc::clone(&schema));
            inner.left_stream =
                Some(Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::once(async move {
                        rx.await.expect("release the test read");
                        Ok(batch)
                    }),
                )));
        }
        let mut load = Arc::clone(&coordinator)
            .next_chunk(0, spill, ctx, Time::new())
            .boxed();
        assert!(futures::poll!(load.as_mut()).is_pending());
        assert!(coordinator.inner.lock().loader_in_flight);
        coordinator.cancel();
        let _keep_sender = if complete_read {
            tx.send(()).expect("read is waiting");
            None
        } else {
            Some(tx)
        };
        let outcome = tokio::time::timeout(Duration::from_secs(1), load)
            .await
            .expect("cancelled loader still waits for its input");
        assert!(outcome.is_err());
        assert!(coordinator.inner.lock().current.is_none());
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn nlj_cancelled_inflight_load_discards_its_publish() -> Result<()> {
        run_paused_loader_cancellation(true).await
    }

    #[tokio::test]
    async fn nlj_cancelled_inflight_load_stops_waiting() -> Result<()> {
        run_paused_loader_cancellation(false).await
    }

    /// A partition dropped before finishing must cancel the coordinated
    /// fallback rather than leave the survivors waiting forever.
    ///
    /// Chunk advancement needs every partition to report, so before this the
    /// survivors fell through to the `notified()` wait in `next_chunk` and hung.
    #[tokio::test]
    async fn test_nlj_cancelled_partition_does_not_hang_survivors() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));

        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill =
            spill_left_for_test(build_left_table(), Arc::clone(&task_ctx)).await?;

        let (chunk_a, _) = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&task_ctx), Time::new())
            .await?
            .expect("chunk 0");

        // Partition A disappears mid-probe: it never reports completion, so no
        // emitter is elected and nothing would ever release the slot.
        drop(chunk_a);
        Arc::clone(&coordinator).cancel();

        // The survivor must be told the execution is over, not left waiting.
        let survivor = tokio::time::timeout(
            Duration::from_secs(5),
            Arc::clone(&coordinator).next_chunk(
                1,
                Arc::clone(&spill),
                Arc::clone(&task_ctx),
                Time::new(),
            ),
        )
        .await;
        assert!(
            survivor.is_ok(),
            "a cancelled partition must not leave the survivors hanging"
        );
        assert!(
            survivor.unwrap().is_err(),
            "the survivor should see the cancellation as an error"
        );
        Ok(())
    }

    /// Cancelling drops what the coordinator holds, so the chunk's reservation
    /// is not stranded for the lifetime of a retained plan.
    #[tokio::test]
    async fn test_nlj_cancel_releases_coordinator_held_memory() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let pool = Arc::clone(&runtime.memory_pool);
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));

        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let spill =
            spill_left_for_test(build_left_table(), Arc::clone(&task_ctx)).await?;

        let (chunk, _) = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&task_ctx), Time::new())
            .await?
            .expect("chunk 0");
        assert!(pool.reserved() > 0);

        // Streams go away without releasing the slot, then the drop guard
        // cancels. The coordinator is still alive, as for a retained plan.
        drop(chunk);
        Arc::clone(&coordinator).cancel();
        assert_eq!(
            pool.reserved(),
            0,
            "cancelling must release what the coordinator was holding"
        );
        Ok(())
    }

    /// An already-cancelled coordinator refuses to serve chunks at all.
    ///
    /// This is the entry check in `next_chunk`, nothing more: cancellation
    /// happens before any load starts. The publish-after-an-in-flight-read path
    /// is covered by `nlj_cancelled_inflight_load_discards_its_publish`, which pauses a real
    /// read and then completes it.
    #[tokio::test]
    async fn test_nlj_cancelled_coordinator_refuses_to_serve_chunks() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let pool = Arc::clone(&runtime.memory_pool);
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));

        let coordinator = Arc::new(FallbackCoordinator::new(1, true));
        let spill =
            spill_left_for_test(build_left_table(), Arc::clone(&task_ctx)).await?;

        // Cancel first, then ask for a chunk: the entry check must refuse. The
        // publish-after-an-in-flight-read path is covered by
        // `nlj_cancelled_inflight_load_discards_its_publish`, which pauses a real read.
        Arc::clone(&coordinator).cancel();
        let result = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&task_ctx), Time::new())
            .await;
        assert!(
            result.is_err(),
            "a cancelled fallback must not serve chunks"
        );
        assert_eq!(
            pool.reserved(),
            0,
            "a discarded load must not leave the reservation reinstated"
        );
        Ok(())
    }

    /// A coordinator nobody cancelled keeps serving chunks.
    #[tokio::test]
    async fn test_nlj_uncancelled_coordinator_serves_and_stays_live() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let coordinator = Arc::new(FallbackCoordinator::new(1, true));
        let spill =
            spill_left_for_test(build_left_table(), Arc::clone(&task_ctx)).await?;

        // A plain sanity check that nothing marks the coordinator cancelled on
        // its own. Real stream drops are covered by
        // `nlj_normal_completion_does_not_cancel_peers`, which runs both partitions
        // to completion and drops them.
        let (chunk, _) = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&task_ctx), Time::new())
            .await?
            .expect("chunk 0");
        assert!(!coordinator.inner.lock().cancelled);
        drop(chunk);
        Ok(())
    }

    /// The chunk's memory must be owned by the chunk's `JoinLeftData`, so it
    /// stays accounted while *any* holder still references it.
    ///
    /// The emitter elected in `ProbeEnd` is the last stream to finish probing,
    /// which is not necessarily the last to drop its `Arc<JoinLeftData>`: a
    /// non-emitter can flush a completed output batch from
    /// `maybe_flush_ready_batch` and return while still holding one, since that
    /// return happens before `buffered_left_data = None`. Freeing the bytes when
    /// the coordinator slot is released would therefore under-account memory
    /// that is still live.
    ///
    /// This drives the coordinator directly so the check does not depend on
    /// scheduling: after releasing the slot, the pool must still account for the
    /// chunk while a reference is held, and drop to zero only once it is gone.
    #[tokio::test]
    async fn test_nlj_chunk_memory_is_owned_by_the_chunk_data() -> Result<()> {
        let runtime = RuntimeEnvBuilder::new().build_arc()?;
        let pool = Arc::clone(&runtime.memory_pool);
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));

        // One chunk, tracked bitmap, two nominal probe partitions.
        let coordinator = Arc::new(FallbackCoordinator::new(2, true));
        let left = build_left_table();
        let spill = spill_left_for_test(Arc::clone(&left), Arc::clone(&task_ctx)).await?;

        let (chunk, is_last) = Arc::clone(&coordinator)
            .next_chunk(0, Arc::clone(&spill), Arc::clone(&task_ctx), Time::new())
            .await?
            .expect("the left side has rows, so a chunk must be produced");
        assert!(is_last, "the fixture fits in a single chunk");

        let accounted_while_held = pool.reserved();
        assert!(
            accounted_while_held > 0,
            "loading a chunk must account for its batch and bitmap"
        );

        // Release the coordinator slot while still holding the chunk. This is
        // the emitter's release: the slot is freed, but the data is alive.
        coordinator.release_chunk(0);
        assert_eq!(
            pool.reserved(),
            accounted_while_held,
            "releasing the slot must not release memory that is still referenced"
        );

        // Dropping the last reference is what returns the bytes.
        drop(chunk);
        assert_eq!(
            pool.reserved(),
            0,
            "dropping the last chunk reference must release its memory"
        );
        Ok(())
    }

    /// The final chunk must be released before the stream finishes.
    ///
    /// `release_chunk` is what drops the coordinator's `Arc<JoinLeftData>` and
    /// lets the next load `resize(0)` the coordinator reservation. For every
    /// non-final chunk that happens on the way back to `BufferingLeft`, but the
    /// final chunk used to create the release future and then transition
    /// straight to `Done` (LEFT) or `EmitGlobalRightUnmatched` (FULL), neither
    /// of which polls it. Because the coordinator hangs off the *plan*, not the
    /// stream, the final chunk's batch, bitmap and reservation stayed accounted
    /// against the pool for as long as the plan was alive.
    async fn assert_final_chunk_released(join_type: JoinType) -> Result<()> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(50, 1.0)
            .build_arc()?;
        let pool = Arc::clone(&runtime.memory_pool);
        let cfg = TaskContext::default()
            .session_config()
            .clone()
            .with_batch_size(16);
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(cfg),
        );

        let partition_count = 4;
        let right = Arc::new(RepartitionExec::try_new(
            build_right_table_one_batch_per_row(),
            Partitioning::RoundRobinBatch(partition_count),
        )?) as Arc<dyn ExecutionPlan>;
        // Held for the whole test, exactly as a cached or still-referenced
        // physical plan would be after its query finished.
        let nested_loop_join = Arc::new(NestedLoopJoinExec::try_new(
            build_left_table(),
            right,
            Some(prepare_join_filter()),
            &join_type,
            None,
        )?);

        for i in 0..partition_count {
            let stream = nested_loop_join.execute(i, Arc::clone(&task_ctx))?;
            let _ = common::collect(stream).await?;
        }
        assert!(
            nested_loop_join
                .metrics()
                .unwrap()
                .spill_count()
                .unwrap_or(0)
                > 0,
            "{join_type}: expected spilling under a tight memory limit"
        );

        assert_eq!(
            pool.reserved(),
            0,
            "{join_type}: the coordinator still holds the final chunk's memory \
             while the plan is alive"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_releases_final_chunk_left_join() -> Result<()> {
        assert_final_chunk_released(JoinType::Left).await
    }

    #[tokio::test]
    async fn test_nlj_memory_limited_releases_final_chunk_full_join() -> Result<()> {
        assert_final_chunk_released(JoinType::Full).await
    }

    /// Run a NLJ across 4 right partitions, collecting every output
    /// partition CONCURRENTLY. This is required for the multi-chunk
    /// coordinator path: a chunk is not released until all partitions
    /// finish probing it, and a partition cannot advance to the next chunk
    /// until the current one is released. Collecting partitions
    /// sequentially would therefore deadlock; concurrent collection mirrors
    /// how partitions actually run under the runtime.
    async fn multi_partition_memory_limited_join_collect_concurrent(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let partition_count = 4;
        let right = Arc::new(RepartitionExec::try_new(
            right,
            Partitioning::RoundRobinBatch(partition_count),
        )?) as Arc<dyn ExecutionPlan>;

        let nested_loop_join = Arc::new(NestedLoopJoinExec::try_new(
            left,
            right,
            join_filter,
            join_type,
            None,
        )?);
        let columns = columns(&nested_loop_join.schema());

        let mut handles = vec![];
        for i in 0..partition_count {
            let stream = nested_loop_join.execute(i, Arc::clone(&context))?;
            handles.push(SpawnedTask::spawn(
                async move { common::collect(stream).await },
            ));
        }

        let mut batches = vec![];
        for handle in handles {
            let more = handle.join().await.expect("partition task panicked")?;
            batches.extend(more.into_iter().filter(|b| b.num_rows() > 0));
        }

        let metrics = nested_loop_join.metrics().unwrap();
        Ok((columns, batches, metrics))
    }

    /// Regression test for the multi-chunk coordinator path of a LEFT join.
    ///
    /// Unlike the other multi-partition tests, the left side here is split
    /// into multiple chunks (one row per batch under a tight memory limit),
    /// so this exercises `carryover` between chunks, `release_chunk`
    /// advancing `next_chunk_index`, waiter notification, and re-entering
    /// `BufferingLeft` for each subsequent chunk. Every left row must appear
    /// exactly once: duplicates would indicate the per-chunk `JoinLeftData`
    /// (visited bitmap + probe-thread counter) was not shared across right
    /// partitions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_multi_partition_multi_chunk_left_join() -> Result<()>
    {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table_multi_chunk();
        let right = build_right_table_one_batch_per_row();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            multi_partition_memory_limited_join_collect_concurrent(
                left,
                right,
                &JoinType::Left,
                Some(filter),
                task_ctx,
            )
            .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling under tight memory limit"
        );

        // (5,5,50) matches (2,2,80); all other left rows (b1 = 8) are
        // unmatched and appear exactly once.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+----+----+----+
        | a1 | b1 | c1  | a2 | b2 | c2 |
        +----+----+-----+----+----+----+
        | 11 | 8  | 110 |    |    |    |
        | 13 | 8  | 130 |    |    |    |
        | 15 | 8  | 150 |    |    |    |
        | 17 | 8  | 170 |    |    |    |
        | 19 | 8  | 190 |    |    |    |
        | 21 | 8  | 210 |    |    |    |
        | 5  | 5  | 50  | 2  | 2  | 80 |
        | 9  | 8  | 90  |    |    |    |
        +----+----+-----+----+----+----+
        "));
        Ok(())
    }

    /// Regression test for the multi-chunk coordinator path of a FULL join.
    ///
    /// In addition to the per-chunk left sharing exercised by the LEFT case,
    /// this covers the global right-unmatched bitmap accumulated ACROSS all
    /// left chunks and emitted once in `EmitGlobalRightUnmatched`. The two
    /// right rows with `b2 = 10` are filtered out of every match and must
    /// appear exactly once as unmatched-right rows, regardless of how many
    /// left chunks were processed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_multi_partition_multi_chunk_full_join() -> Result<()>
    {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table_multi_chunk();
        let right = build_right_table_one_batch_per_row();
        let filter = prepare_join_filter();

        let (columns, batches, metrics) =
            multi_partition_memory_limited_join_collect_concurrent(
                left,
                right,
                &JoinType::Full,
                Some(filter),
                task_ctx,
            )
            .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
        assert!(
            metrics.spill_count().unwrap_or(0) > 0,
            "Expected spilling under tight memory limit"
        );

        // Matched: (5,5,50)+(2,2,80). Unmatched left: the seven b1 = 8 rows.
        // Unmatched right: (12,10,40) and (10,10,100), each emitted once from
        // the global right bitmap accumulated across all left chunks.
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+-----+----+----+-----+
        | a1 | b1 | c1  | a2 | b2 | c2  |
        +----+----+-----+----+----+-----+
        |    |    |     | 10 | 10 | 100 |
        |    |    |     | 12 | 10 | 40  |
        | 11 | 8  | 110 |    |    |     |
        | 13 | 8  | 130 |    |    |     |
        | 15 | 8  | 150 |    |    |     |
        | 17 | 8  | 170 |    |    |     |
        | 19 | 8  | 190 |    |    |     |
        | 21 | 8  | 210 |    |    |     |
        | 5  | 5  | 50  | 2  | 2  | 80  |
        | 9  | 8  | 90  |    |    |     |
        +----+----+-----+----+----+-----+
        "));
        Ok(())
    }

    /// Like `task_ctx_with_memory_limit`, but also disables the coordinated
    /// memory-limited fallback via `enable_nlj_coordinated_fallback = false`
    /// (the opt-out a distributed engine would use).
    fn task_ctx_with_memory_limit_no_coordinated_fallback(
        memory_limit: usize,
        batch_size: usize,
    ) -> Result<Arc<TaskContext>> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .build_arc()?;
        let mut cfg = TaskContext::default()
            .session_config()
            .clone()
            .with_batch_size(batch_size);
        cfg.options_mut().execution.enable_nlj_coordinated_fallback = false;
        let task_ctx = TaskContext::default()
            .with_runtime(runtime)
            .with_session_config(cfg);
        Ok(Arc::new(task_ctx))
    }

    /// Collect a multi-partition NLJ under a tight memory limit and return the
    /// first error, if any. Used to assert that disabling the coordinated
    /// fallback makes a left-emitting multi-partition join fail with resource
    /// exhaustion (rather than spill, or — in a distributed setting — hang).
    async fn multi_partition_join_collect_err(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        join_type: &JoinType,
        join_filter: Option<JoinFilter>,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let partition_count = 4;
        let right = Arc::new(RepartitionExec::try_new(
            right,
            Partitioning::RoundRobinBatch(partition_count),
        )?) as Arc<dyn ExecutionPlan>;
        let nested_loop_join = Arc::new(NestedLoopJoinExec::try_new(
            left,
            right,
            join_filter,
            join_type,
            None,
        )?);

        let mut handles = vec![];
        for i in 0..partition_count {
            let stream = nested_loop_join.execute(i, Arc::clone(&context))?;
            handles.push(SpawnedTask::spawn(
                async move { common::collect(stream).await },
            ));
        }
        for handle in handles {
            handle.join().await.expect("partition task panicked")?;
        }
        Ok(())
    }

    /// When `enable_nlj_coordinated_fallback` is disabled, a LEFT join with
    /// a multi-partition right side must NOT take the coordinated fallback: it
    /// fails with resource exhaustion under a tight memory limit instead. This
    /// is the distributed-safe opt-out (the coordinated fallback would
    /// otherwise deadlock across processes).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_fallback_disabled_left_join_oom() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit_no_coordinated_fallback(50, 16)?;
        let left = build_left_table_multi_chunk();
        let right = build_right_table_one_batch_per_row();
        let filter = prepare_join_filter();

        let err = multi_partition_join_collect_err(
            left,
            right,
            &JoinType::Left,
            Some(filter),
            task_ctx,
        )
        .await
        .unwrap_err();
        assert_contains!(err.to_string(), "Resources exhausted");
        Ok(())
    }

    /// FULL join counterpart of the above: the opt-out disables the coordinated
    /// fallback for FULL (also a left-emitting join) with a multi-partition
    /// right side, so it fails with resource exhaustion rather than
    /// coordinating.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_fallback_disabled_full_join_oom() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit_no_coordinated_fallback(50, 16)?;
        let left = build_left_table_multi_chunk();
        let right = build_right_table_one_batch_per_row();
        let filter = prepare_join_filter();

        let err = multi_partition_join_collect_err(
            left,
            right,
            &JoinType::Full,
            Some(filter),
            task_ctx,
        )
        .await
        .unwrap_err();
        assert_contains!(err.to_string(), "Resources exhausted");
        Ok(())
    }

    /// The opt-out is scoped to *left-emitting* joins. A RIGHT join over a
    /// multi-partition right side is unaffected by the missing coordination —
    /// each partition owns its right rows exclusively — so disabling the
    /// coordinated fallback must still leave it spilling rather than failing.
    /// This pins the scope of the guard so a future change cannot quietly turn
    /// off the spill fallback for join types that never needed coordination.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_fallback_disabled_right_join_still_spills()
    -> Result<()> {
        for join_type in [JoinType::Right, JoinType::Inner] {
            let task_ctx = task_ctx_with_memory_limit_no_coordinated_fallback(50, 16)?;
            // Must drain the partitions concurrently: the coordinator seeds
            // every chunk's probe counter with `right_partition_count`
            // regardless of join type, so a multi-chunk left side cannot
            // advance if the partitions are collected one after another.
            let (_columns, _batches, metrics) =
                multi_partition_memory_limited_join_collect_concurrent(
                    build_left_table_multi_chunk(),
                    build_right_table_one_batch_per_row(),
                    &join_type,
                    Some(prepare_join_filter()),
                    task_ctx,
                )
                .await?;
            assert!(
                metrics.spill_count().unwrap_or(0) > 0,
                "{join_type}: the opt-out must not disable the spill fallback for \
                 a join type that does not need left-bitmap coordination"
            );
        }
        Ok(())
    }
}
