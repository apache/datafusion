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
use std::sync::Arc;

use super::materializing_stream::{
    JoinLeftData, LeftSpillData, NestedLoopJoinMetrics, NestedLoopJoinStream, SpillState,
    collect_left_input,
};
use crate::common::can_project;
use crate::execution_plan::{EmissionType, boundedness_from_children};
use crate::joins::utils::{
    ColumnIndex, JoinFilter, OnceAsync, build_join_schema, check_join_is_valid,
    estimate_join_statistics,
};
use crate::joins::utils::{
    asymmetric_join_output_partitioning, need_produce_result_in_final,
    reorder_output_after_swap, swap_join_projection,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::projection::{
    EmbeddedProjection, JoinData, ProjectionExec, try_embed_projection,
    try_pushdown_through_join_with_column_indices,
};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties, ReplaceChildrenOptions,
    SendableRecordBatchStream, validate_child_count,
};

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    NullEquality, Result, Statistics, assert_eq_or_internal_err, project_schema,
};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_expr::JoinType;
use datafusion_physical_expr::equivalence::{
    ProjectionMapping, join_equivalence_properties,
};

use datafusion_physical_expr::projection::{ProjectionRef, combine_projections};

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
    build_side_data: OnceAsync<JoinLeftData>,
    /// Shared left-side spill data for OOM fallback.
    ///
    /// When `build_side_data` fails with OOM, the first partition to
    /// initiate fallback spills the entire left side to disk. Other
    /// partitions share the same spill file via this `OnceAsync`,
    /// avoiding redundant re-execution of the left child.
    left_spill_data: Arc<OnceAsync<LeftSpillData>>,
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
        Ok(NestedLoopJoinExec {
            left,
            right,
            filter,
            join_type,
            join_schema,
            build_side_data: Default::default(),
            left_spill_data: Arc::new(OnceAsync::default()),
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
        let new_join = NestedLoopJoinExec::try_new(
            Arc::clone(right),
            Arc::clone(left),
            self.filter().map(JoinFilter::swap),
            &self.join_type().swap(),
            swap_join_projection(
                left.schema().fields().len(),
                right.schema().fields().len(),
                self.projection.as_deref(),
                self.join_type(),
            ),
        )?;

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
                Ok(Arc::new(Self {
                    left,
                    right,
                    metrics: ExecutionPlanMetricsSet::new(),
                    build_side_data: Default::default(),
                    left_spill_data: Arc::new(OnceAsync::default()),
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

        // Always try to buffer all left data in memory via OnceFut.
        // If that fails with OOM, the stream will fallback to memory-limited
        // mode (if conditions allow).
        let load_reservation =
            MemoryConsumer::new(format!("NestedLoopJoinLoad[{partition}]"))
                .register(context.memory_pool());

        let build_side_data = self.build_side_data.try_once(|| {
            let stream = self.left.execute(0, Arc::clone(&context))?;

            Ok(collect_left_input(
                stream,
                metrics.join_metrics.clone(),
                load_reservation,
                need_produce_result_in_final(self.join_type),
                right_partition_count,
            ))
        })?;

        let probe_side_data = self.right.execute(partition, Arc::clone(&context))?;

        // Determine if OOM fallback to memory-limited mode is possible.
        // Conditions:
        // 1. Disk manager supports temp files (needed for spilling).
        // 2. FULL join with multiple right partitions is not yet supported
        //    in the fallback path. FULL join needs to track BOTH left-side
        //    matches (for unmatched left rows) AND right-side matches (for
        //    unmatched right rows). The fallback path builds a per-partition
        //    `JoinLeftData` with `probe_threads_counter == 1`, so each
        //    partition emits unmatched left rows based only on its own
        //    right-side matches, producing incorrect duplicate output for
        //    left rows that match in another partition. Other join types
        //    that need only one-sided final emission (LEFT, LEFT SEMI,
        //    LEFT ANTI, LEFT MARK) have a similar latent issue in the
        //    fallback path which predates this change; tracking is out of
        //    scope for this PR.
        let full_join_multi_partition =
            matches!(self.join_type, JoinType::Full) && right_partition_count > 1;
        let spill_state = if context.runtime_env().disk_manager.tmp_files_enabled()
            && !full_join_multi_partition
        {
            SpillState::Pending {
                left_plan: Arc::clone(&self.left),
                task_context: Arc::clone(&context),
                left_spill_data: Arc::clone(&self.left_spill_data),
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
            Ok(Some(Arc::new(NestedLoopJoinExec::try_new(
                Arc::new(projected_left_child),
                Arc::new(projected_right_child),
                join_filter,
                self.join_type(),
                // Returned early if projection is not None
                None,
            )?)))
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

        let left = ctx.encode_child(self.left())?;
        let right = ctx.encode_child(self.right())?;

        let join_type = crate::joins::proto::join_type_to_proto(*self.join_type());

        let filter = self
            .filter()
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
                        projection: match self.projection.as_ref() {
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

        let left = ctx.decode_required_child(
            join.left.as_deref(),
            "NestedLoopJoinExec",
            "left",
        )?;
        let right = ctx.decode_required_child(
            join.right.as_deref(),
            "NestedLoopJoinExec",
            "right",
        )?;

        let join_type = crate::joins::proto::join_type_from_proto(
            join.join_type,
            "NestedLoopJoinExec",
        )?;

        let filter = join
            .filter
            .as_ref()
            .map(|f| {
                crate::joins::proto::join_filter_from_proto(f, ctx, "NestedLoopJoinExec")
            })
            .transpose()?;

        let projection = match join.projection.as_slice() {
            [] => None,
            [u32::MAX] => Some(Vec::new()),
            indices => Some(indices.iter().map(|i| *i as usize).collect()),
        };

        Ok(Arc::new(NestedLoopJoinExec::try_new(
            left, right, filter, &join_type, projection,
        )?))
    }
}

impl EmbeddedProjection for NestedLoopJoinExec {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        self.with_projection(projection)
    }
}
