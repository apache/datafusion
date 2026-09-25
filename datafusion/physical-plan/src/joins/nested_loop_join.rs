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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;

use super::utils::{
    asymmetric_join_output_partitioning, need_produce_result_in_final,
    reorder_output_after_swap, swap_join_projection,
};
use crate::common::can_project;
use crate::execution_plan::{EmissionType, boundedness_from_children};
use crate::filter_pushdown::{
    ChildFilterDescription, FilterDescription, FilterPushdownPhase, PushedDownPredicate,
};
use crate::joins::SharedBitmapBuilder;
use crate::joins::logical_batch::{BatchRow, LogicalBatch};
use crate::joins::utils::{
    BuildProbeJoinMetrics, ColumnIndex, JoinFilter, OnceAsync, OnceFut,
    boolean_mask_from_filter, build_join_schema, check_join_is_valid,
    estimate_join_statistics, need_produce_right_in_final,
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
use arrow::compute::{BatchCoalescer, filter, filter_record_batch, not, take};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_iterator::BitIndexIterator;
use arrow_schema::DataType;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    DataFusionError, JoinSide, NullEquality, Result, ScalarValue, SharedResult,
    Statistics, arrow_err, assert_eq_or_internal_err, internal_datafusion_err,
    internal_err, project_schema, unwrap_or_internal_err,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_execution::{SpillFile, TaskContext};
use datafusion_expr::JoinType;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::equivalence::{
    ProjectionMapping, join_equivalence_properties,
};
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;

use datafusion_physical_expr::projection::{ProjectionRef, combine_projections};
use futures::future::{BoxFuture, Shared};
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
///   util a memory limit is reached. The batches are kept as they arrive and
///   addressed as one contiguous batch (see `LogicalBatch`), so buffering
///   does not copy them into a merged batch.
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
/// fails with `ResourcesExhausted` and disk spilling is available. The left
/// child is executed once and spilled to one file during that same load, and
/// each output partition spills its own right input.
///
/// Re-reading the right side once per left chunk is the cost of the fallback,
/// so chunks should be as large as memory allows. All output partitions
/// therefore share one chunk at a time, which is as large as it gets, and move
/// on to the next one together (see [`LeftChunkBarrier`]).
///
/// All join types are supported, and both sides defer their unmatched rows
/// until every chunk has been probed:
/// - For RIGHT/FULL/RIGHT SEMI/RIGHT ANTI/RIGHT MARK joins, each partition
///   keeps a global right-side bitmap (indexed by right batch sequence number)
///   that accumulates matches across all left chunks. After the last left
///   chunk, the right side is replayed one more time to emit unmatched right
///   rows using the accumulated bitmap.
/// - For LEFT/FULL/LEFT SEMI/LEFT ANTI/LEFT MARK joins, one global left-side
///   bitmap (one bit per spilled left row, see [`LeftSpillData`]) is shared by
///   all partitions. Each partition merges a chunk's matches into it when it
///   finishes the chunk, and the last partition to finish streams the left
///   spill file once more to emit the final left rows, the same way the
///   in-memory path elects a single partition for that step.
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
    /// Paces the output streams through the left chunks when the left side is
    /// spilled. *Shared* across all output streams, like `build_side_data`.
    left_chunk_barrier: Arc<LeftChunkBarrier>,
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
        let left_chunk_barrier = Arc::new(LeftChunkBarrier::new(
            right.output_partitioning().partition_count(),
        ));
        Ok(NestedLoopJoinExec {
            left,
            right,
            filter,
            join_type,
            join_schema,
            build_side_data: Default::default(),
            left_chunk_barrier,
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

        let emission_type =
            // LeftSemi does not emit rows during probing. It records matching build-side
            // rows in a bitmap and can only emit them after the probe side is exhausted.
            if left.boundedness().is_unbounded() || join_type == JoinType::LeftSemi {
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
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
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
                // Counts the partitions of the right child, so it is built for
                // the new one rather than shared with `self`.
                let left_chunk_barrier = Arc::new(LeftChunkBarrier::new(
                    right.output_partitioning().partition_count(),
                ));
                Ok(Arc::new(Self {
                    left,
                    right,
                    metrics: ExecutionPlanMetricsSet::new(),
                    build_side_data: Default::default(),
                    left_chunk_barrier,
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
        //    shares one global bitmap and one probe-thread counter, seeded
        //    with `right_partition_count`, through [`LeftSpillData`], exactly
        //    as the single-pass path does through [`JoinLeftData`].
        //
        //    That sharing assumes all right partitions run in the same
        //    process. A distributed engine executes each partition as an
        //    independent task with its own copy of the shared state, so the
        //    counter would never reach zero, and the partitions it never
        //    runs would be waited for at every left chunk (see
        //    [`LeftChunkBarrier`]). Such engines set
        //    `enable_nlj_coordinated_fallback = false` to opt out for the
        //    affected join types (left-emitting joins over a multi-partition
        //    right side), which then fail with resource exhaustion under
        //    memory pressure instead of stalling. Single-partition joins
        //    always keep the fallback, as do non-left-emitting ones.
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
                Arc::clone(&self.left_chunk_barrier),
            ))
        })?;

        let probe_side_data = self.right.execute(partition, Arc::clone(&context))?;

        let spill_state = if can_spill {
            SpillState::Pending {
                task_context: Arc::clone(&context),
                partition,
                left_chunk_barrier: Arc::clone(&self.left_chunk_barrier),
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

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        if phase != FilterPushdownPhase::Post
            || !config.optimizer.enable_join_dynamic_filter_pushdown
        {
            return Ok(FilterDescription::new()
                .with_child(ChildFilterDescription::all_unsupported(&parent_filters))
                .with_child(ChildFilterDescription::all_unsupported(&parent_filters)));
        }

        // Removing rows from the non-preserved side of an outer or anti join
        // can create new unmatched rows. Only route filters to output-preserving
        // inputs; unlike a hash join, an NLJ has no equijoin keys to translate
        // filters onto the other input of a semi join.
        let (left_preserved, right_preserved) = match self.join_type {
            JoinType::Inner => (true, true),
            JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark => (true, false),
            JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => (false, true),
            JoinType::Full => (false, false),
        };
        let output_indices: Vec<_> = match &self.projection {
            Some(projection) => projection.to_vec(),
            None => (0..self.column_indices.len()).collect(),
        };
        let mut description = FilterDescription::new();
        for (side, preserved, child) in [
            (JoinSide::Left, left_preserved, self.left()),
            (JoinSide::Right, right_preserved, self.right()),
        ] {
            // Map positions explicitly: names can repeat within an input as
            // well as across the two sides of the join.
            let column_mapping = output_indices
                .iter()
                .enumerate()
                .filter_map(|(output, input)| {
                    let column = &self.column_indices[*input];
                    (column.side == side).then_some((output, column.index))
                })
                .collect();
            let mut child_description = if preserved {
                ChildFilterDescription::from_child_with_column_mapping(
                    &parent_filters,
                    column_mapping,
                    child,
                )?
            } else {
                ChildFilterDescription::all_unsupported(&parent_filters)
            };
            for (filter, pushed) in parent_filters
                .iter()
                .zip(&mut child_description.parent_filters)
            {
                if !filter.is::<DynamicFilterPhysicalExpr>() {
                    *pushed = PushedDownPredicate::unsupported(Arc::clone(filter));
                }
            }
            description = description.with_child(child_description);
        }
        Ok(description)
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
            left_chunk_barrier: _,
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
    /// Build-side rows. See [`LogicalBatch`] for details on this layout
    /// and why it is used.
    batch: LogicalBatch,
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
        batch: LogicalBatch,
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

    pub(crate) fn batch(&self) -> &LogicalBatch {
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
    left_chunk_barrier: Arc<LeftChunkBarrier>,
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
                    with_visited_left_side,
                    probe_threads_count,
                    reservation,
                    &left_chunk_barrier,
                ));
            }
            Err(e) => return Err(e),
        }
    }

    // Only time the build-side materialization performed by this operator, not
    // polling the child stream above.
    let build_timer = metrics.build_time.timer();

    let buffered_build_batch = LogicalBatch::new(Arc::clone(&schema), batches)?;

    // Reserve memory for visited_left_side bitmap if required by join type
    let visited_left_side = if with_visited_left_side {
        let n_rows = buffered_build_batch.num_rows();
        let buffer_size = n_rows.div_ceil(8);
        match reservation.try_grow(buffer_size) {
            Ok(()) => {}
            Err(e) if is_spillable_oom(&e, spill_manager.as_ref()) => {
                // `spill_left_input` owns its timing and polls the input stream
                // outside that timer.
                build_timer.done();
                let spill_manager = spill_manager.expect("checked by is_spillable_oom");
                let spilled = spill_left_input(
                    spill_manager,
                    buffered_build_batch.into_batches(),
                    None,
                    stream,
                    metrics,
                    &reservation,
                )
                .await?;
                return Ok(left_load_from_spill(
                    spilled,
                    schema,
                    with_visited_left_side,
                    probe_threads_count,
                    reservation,
                    &left_chunk_barrier,
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
        buffered_build_batch,
        Mutex::new(visited_left_side),
        AtomicUsize::new(probe_threads_count),
        reservation,
    ))))
}

/// A left side with no rows needs no spill file, so it stays on the in-memory path.
fn left_load_from_spill(
    spilled: Option<SpilledLeftFile>,
    schema: SchemaRef,
    with_visited_left_side: bool,
    probe_threads_count: usize,
    reservation: MemoryReservation,
    left_chunk_barrier: &LeftChunkBarrier,
) -> LeftLoad {
    match spilled {
        Some(spilled) => {
            let left_spill = Arc::new(LeftSpillData::new(
                spilled,
                schema,
                with_visited_left_side,
                probe_threads_count,
                reservation,
            ));
            left_chunk_barrier.register_left_spill(&left_spill);
            LeftLoad::Spilled(left_spill)
        }
        // No rows means no bitmap either, whatever the join type.
        None => LeftLoad::InMemory(Arc::new(JoinLeftData::new(
            LogicalBatch::new_empty(schema),
            Mutex::new(BooleanBufferBuilder::new(0)),
            AtomicUsize::new(probe_threads_count),
            reservation,
        ))),
    }
}

/// Whether a failed reservation is an exhausted pool that the caller can spill its way out of.
fn is_spillable_oom(
    error: &DataFusionError,
    spill_manager: Option<&SpillManager>,
) -> bool {
    spill_manager.is_some()
        && matches!(error.find_root(), DataFusionError::ResourcesExhausted(_))
}

/// Write the already-buffered left batches plus the remainder of the same stream to one spill file.
/// Returns `None` when the left side carried no rows at all, which needs no spill file.
async fn spill_left_input(
    spill_manager: SpillManager,
    buffered: Vec<RecordBatch>,
    pending: Option<RecordBatch>,
    mut stream: SendableRecordBatchStream,
    metrics: BuildProbeJoinMetrics,
    reservation: &MemoryReservation,
) -> Result<Option<SpilledLeftFile>> {
    let build_timer = metrics.build_time.timer();
    let mut spill_file =
        spill_manager.create_in_progress_file("NestedLoopJoin left spill")?;
    let mut num_rows = 0;

    for batch in buffered {
        if batch.num_rows() > 0 {
            num_rows += batch.num_rows();
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
            num_rows += batch.num_rows();
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
            num_rows += batch.num_rows();
            spill_file.append_batch(&batch)?;
        }
    }

    let _build_timer = metrics.build_time.timer();
    Ok(spill_file.finish()?.map(|spill_file| SpilledLeftFile {
        spill_manager,
        spill_file,
        num_rows,
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
    /// Entered exactly once per stream, when it has finished probing the whole
    /// left side: the probe (right) side is exhausted or, in memory-limited
    /// mode, every left chunk has been probed. This state owns the single
    /// `report_probe_completed` call that decrements the shared probe-threads
    /// counter, and records in `is_unmatched_left_emitter` whether this stream
    /// is the one responsible for emitting unmatched-left rows. Splitting this
    /// decision out of `EmitLeftUnmatched` makes "decrement exactly once" a
    /// structural property of the state graph, so the (re-enterable) emit state
    /// no longer has to guard against decrementing twice.
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
    /// shares this handle, and the chunks that are read back from the file.
    Spilled(Arc<LeftSpillData>),
}

/// The spill file [`spill_left_input`] wrote, before it is wrapped in a [`LeftSpillData`].
struct SpilledLeftFile {
    spill_manager: SpillManager,
    spill_file: Arc<dyn SpillFile>,
    /// Total number of rows written to `spill_file`
    num_rows: usize,
}

/// The spilled left side, shared by every output partition.
///
/// This is the memory-limited counterpart of [`JoinLeftData`]: the rows live in
/// a spill file instead of memory, but the visited bitmap and the probe-threads
/// counter cover the whole left side and are shared by all partitions in the
/// same way.
///
/// The rows come back one chunk at a time (see [`LeftChunkBarrier`]), while the
/// bitmap spans all of them: bits are addressed by the row's position in the
/// file. Keeping match tracking apart from the chunks is what lets a chunk be
/// dropped as soon as it has been probed, with the final left rows emitted in
/// one pass at the very end.
pub(crate) struct LeftSpillData {
    /// SpillManager used to read the spill file (has the left schema)
    spill_manager: SpillManager,
    /// The spill file containing all left-side batches
    spill_file: Arc<dyn SpillFile>,
    /// Left-side schema
    schema: SchemaRef,
    /// Total number of rows in `spill_file`
    num_rows: usize,
    /// The pass over `spill_file` that chunks are read from. Each chunk load
    /// takes it and hands it back for the load of the following chunk.
    reader: Arc<Mutex<Option<LeftChunkReader>>>,
    /// Visited bitmap over every row of `spill_file`. Empty when the join type
    /// does not need it.
    visited: SharedBitmapBuilder,
    /// Counter of partitions that have neither finished probing every chunk
    /// nor gone away
    probe_threads_counter: AtomicUsize,
    /// Set when a partition went away before it finished probing. The final
    /// left rows are then not emitted, as on the in-memory path.
    incomplete: AtomicBool,
    /// Memory reservation for `visited`
    reservation: MemoryReservation,
}

impl LeftSpillData {
    fn new(
        spilled: SpilledLeftFile,
        schema: SchemaRef,
        with_visited_left_side: bool,
        probe_threads_count: usize,
        reservation: MemoryReservation,
    ) -> Self {
        let SpilledLeftFile {
            spill_manager,
            spill_file,
            num_rows,
        } = spilled;
        let visited = if with_visited_left_side {
            // Use infallible `grow`: one bit per row is all that stays in
            // memory, and the fallback path has no other recourse.
            reservation.grow(num_rows.div_ceil(8));
            let mut buffer = BooleanBufferBuilder::new(num_rows);
            buffer.append_n(num_rows, false);
            buffer
        } else {
            BooleanBufferBuilder::new(0)
        };
        Self {
            spill_manager,
            spill_file,
            schema,
            num_rows,
            reader: Arc::new(Mutex::new(None)),
            visited: Mutex::new(visited),
            probe_threads_counter: AtomicUsize::new(probe_threads_count),
            incomplete: AtomicBool::new(false),
            reservation,
        }
    }

    /// Open a new pass over the spilled left rows
    fn open_pass(&self) -> Result<SendableRecordBatchStream> {
        self.spill_manager
            .read_spill_as_stream(Arc::clone(&self.spill_file), None)
    }

    /// Load the next chunk, accounting for it in `reservation`.
    ///
    /// The load is a shared future, the way the whole left side is a shared
    /// [`OnceFut`]: every partition waiting for the chunk holds a clone, and
    /// whichever of them is polled drives it. So it does not matter which
    /// partition started the load, or whether that one is still around.
    fn load_chunk(
        &self,
        reservation: MemoryReservation,
        build_time: &Time,
    ) -> LeftChunkFut {
        load_left_chunk(
            Arc::clone(&self.reader),
            self.spill_manager.clone(),
            Arc::clone(&self.spill_file),
            Arc::clone(&self.schema),
            reservation,
            build_time.clone(),
        )
        .map(|chunk| chunk.map(Arc::new).map_err(Arc::new))
        .boxed()
        .shared()
    }

    /// Record the matches of a finished chunk, whose first row is the
    /// `row_offset`-th row of the spill file.
    fn merge_visited(&self, row_offset: usize, chunk_visited: &BooleanBufferBuilder) {
        let mut visited = self.visited.lock();
        for idx in BitIndexIterator::new(chunk_visited.as_slice(), 0, chunk_visited.len())
        {
            visited.set_bit(row_offset + idx, true);
        }
    }

    /// Decrements counter of running threads. If the caller is the last
    /// running thread and no partition went away unfinished, it is the emitter
    /// and gets the complete visited bitmap, which it must account for.
    fn report_probe_completed(&self) -> Option<BooleanBuffer> {
        if self.probe_threads_counter.fetch_sub(1, Ordering::AcqRel) != 1 {
            return None;
        }
        let visited = self.take_visited();
        if self.incomplete.load(Ordering::Acquire) {
            // Nobody will emit, so nobody needs the bitmap
            return None;
        }
        Some(visited)
    }

    /// `count` partitions went away before they finished probing.
    ///
    /// They are taken out of the probe-threads counter, like a report of probe
    /// completion, so that the bitmap is released by whichever partition is
    /// the last to finish or go away, while the plan may live on. But the final
    /// left rows are then not emitted.
    fn depart_unfinished(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.incomplete.store(true, Ordering::Release);
        if self
            .probe_threads_counter
            .fetch_sub(count, Ordering::AcqRel)
            == count
        {
            drop(self.take_visited());
        }
    }

    /// Take the visited bitmap for the final emission, or to release it when
    /// there is none. Only the last partition to finish or go away may call
    /// this, after which no partition updates the bitmap.
    ///
    /// The bitmap's memory is no longer accounted for here afterwards, so that
    /// a plan that outlives its execution does not keep it reserved. The
    /// caller accounts for the returned buffer instead.
    fn take_visited(&self) -> BooleanBuffer {
        self.reservation.free();
        self.visited.lock().finish()
    }
}

/// One chunk of the spilled left side, shared by the partitions probing it.
struct LeftChunk {
    batch: LogicalBatch,
    /// Memory reservation for `batch`, cleared on drop
    #[expect(dead_code)]
    reservation: MemoryReservation,
}

/// A load of a [`LeftChunk`] that several partitions can wait on
type LeftChunkFut = Shared<BoxFuture<'static, SharedResult<Arc<LeftChunk>>>>;

/// The pass over the left spill file that chunks are read from
struct LeftChunkReader {
    stream: SendableRecordBatchStream,
    /// The batch that did not fit the previous chunk, which starts the next
    carryover: Option<RecordBatch>,
}

/// Load the next chunk: as many batches as `reservation` accepts, and always at
/// least one so that the join makes progress.
async fn load_left_chunk(
    reader_slot: Arc<Mutex<Option<LeftChunkReader>>>,
    spill_manager: SpillManager,
    spill_file: Arc<dyn SpillFile>,
    schema: SchemaRef,
    reservation: MemoryReservation,
    build_time: Time,
) -> Result<LeftChunk> {
    // Chunks are loaded one after another, so the reader the previous load
    // handed back is where this chunk starts. The first load opens it.
    let reader = reader_slot.lock().take();
    let mut reader = match reader {
        Some(reader) => reader,
        None => LeftChunkReader {
            stream: spill_manager.read_spill_as_stream(spill_file, None)?,
            carryover: None,
        },
    };

    let mut batches = vec![];
    // The batch that did not fit the previous chunk is already in memory, so it
    // is accounted for infallibly.
    if let Some(batch) = reader.carryover.take() {
        reservation.grow(batch.get_array_memory_size());
        batches.push(batch);
    }
    while let Some(batch) = reader.stream.next().await {
        let batch = batch?;
        // Times only the work this operator does on the batch, not the wait
        // for the spill stream to produce it.
        let _build_timer = build_time.timer();
        if batch.num_rows() == 0 {
            continue;
        }
        let batch_size = batch.get_array_memory_size();
        if reservation.try_grow(batch_size).is_err() {
            if !batches.is_empty() {
                // Chunk is full, defer this batch to the next chunk.
                reader.carryover = Some(batch);
                break;
            }
            // No batches yet -- accept the batch even over budget so we make
            // progress.
            reservation.grow(batch_size);
        }
        batches.push(batch);
    }

    let _build_timer = build_time.timer();
    let batch = LogicalBatch::new(schema, batches)?;
    *reader_slot.lock() = Some(reader);
    Ok(LeftChunk { batch, reservation })
}

/// Lets the partitions of a memory-limited join share one left chunk at a time.
///
/// Re-reading the right side once per left chunk is what the fallback costs, so
/// chunks should be as large as memory allows. One chunk shared by every
/// partition is as large as it gets, but it means the next chunk can only be
/// loaded once every partition is done with the current one. That is all this
/// type arranges: it counts the partitions that have finished the current
/// chunk, and moves on when all of them have.
///
/// It lives on the plan, next to the shared left load, because partitions take
/// part before it is known whether the left side spills at all. A partition
/// that goes away unfinished is taken out of the count, so the others carry on
/// without it, as they do when the left side fits in memory.
#[derive(Debug)]
pub(crate) struct LeftChunkBarrier {
    inner: Mutex<LeftChunkBarrierInner>,
    /// Signalled when the barrier moves on to the next chunk
    notify: tokio::sync::Notify,
}

struct LeftChunkBarrierInner {
    /// Index of the chunk the partitions are on
    chunk_index: usize,
    /// The load of that chunk, once a partition has asked for it. Holding it
    /// keeps the chunk in memory for partitions that get to it later.
    chunk: Option<LeftChunkFut>,
    /// Partitions that have not gone away
    live: usize,
    /// How many of them have finished the current chunk
    finished: usize,
    /// Accounts for the chunks. Registered by the first load, because partitions
    /// take part before it is known that there will be one.
    reservation: Option<MemoryReservation>,
    /// The spilled left side, once the load has spilled it. Partitions that go
    /// away before they finished probing are taken out of its probe-threads
    /// counter through here, since a partition may not have seen it yet.
    left_spill: Option<Arc<LeftSpillData>>,
    /// Partitions that went away unfinished before the left side was spilled,
    /// to take out of its counter once it is.
    departed_before_spill: usize,
}

impl std::fmt::Debug for LeftChunkBarrierInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeftChunkBarrierInner")
            .field("chunk_index", &self.chunk_index)
            .field("live", &self.live)
            .field("finished", &self.finished)
            .finish()
    }
}

impl LeftChunkBarrierInner {
    /// Move on if every remaining partition has finished the current chunk.
    /// Returns whether it did.
    fn advance_if_all_finished(&mut self) -> bool {
        if self.live > 0 && self.finished < self.live {
            return false;
        }
        // Letting go of the load is what frees the chunk: by now no partition
        // holds it either.
        self.chunk = None;
        self.chunk_index += 1;
        self.finished = 0;
        true
    }
}

impl LeftChunkBarrier {
    fn new(right_partition_count: usize) -> Self {
        Self {
            inner: Mutex::new(LeftChunkBarrierInner {
                chunk_index: 0,
                chunk: None,
                live: right_partition_count,
                finished: 0,
                reservation: None,
                left_spill: None,
                departed_before_spill: 0,
            }),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// The chunk with index `chunk_index`, once every partition has finished
    /// the one before it.
    async fn chunk(
        self: Arc<Self>,
        chunk_index: usize,
        left_spill: Arc<LeftSpillData>,
        memory_pool: Arc<dyn MemoryPool>,
        build_time: Time,
    ) -> Result<Arc<LeftChunk>> {
        loop {
            // Created under the lock, so that it cannot miss the signal of an
            // advance that happens right after the lock is released.
            let advanced = {
                let mut inner = self.inner.lock();
                if chunk_index == inner.chunk_index {
                    let inner = &mut *inner;
                    let reservation = inner.reservation.get_or_insert_with(|| {
                        MemoryConsumer::new("NestedLoopJoinFallbackChunk")
                            .with_can_spill(true)
                            .register(&memory_pool)
                    });
                    let chunk = inner.chunk.get_or_insert_with(|| {
                        left_spill.load_chunk(reservation.new_empty(), &build_time)
                    });
                    Err(chunk.clone())
                } else {
                    Ok(self.notify.notified())
                }
            };
            match advanced {
                Ok(advanced) => advanced.await,
                Err(chunk) => return chunk.await.map_err(DataFusionError::Shared),
            }
        }
    }

    /// A partition has finished the current chunk
    fn finish_chunk(&self) {
        let mut inner = self.inner.lock();
        inner.finished += 1;
        if inner.advance_if_all_finished() {
            drop(inner);
            self.notify.notify_waiters();
        }
    }

    /// The load has spilled the left side
    fn register_left_spill(&self, left_spill: &Arc<LeftSpillData>) {
        let mut inner = self.inner.lock();
        left_spill.depart_unfinished(inner.departed_before_spill);
        inner.left_spill = Some(Arc::clone(left_spill));
    }

    /// A partition has gone away before finishing.
    ///
    /// `chunk_index` is the index the partition had advanced to: that of the
    /// chunk it was probing or waiting for, or one past the last chunk once it
    /// has probed them all. `probe_reported` is whether it had already reported
    /// probe completion, in which case the left side's counter already
    /// accounts for it.
    fn depart(&self, chunk_index: usize, probe_reported: bool) {
        let mut inner = self.inner.lock();
        inner.live = inner.live.saturating_sub(1);
        if chunk_index > inner.chunk_index {
            // It had finished the current chunk and was waiting for the next
            inner.finished = inner.finished.saturating_sub(1);
        }
        if !probe_reported {
            match &inner.left_spill {
                Some(left_spill) => left_spill.depart_unfinished(1),
                None => inner.departed_before_spill += 1,
            }
        }
        if inner.advance_if_all_finished() {
            drop(inner);
            self.notify.notify_waiters();
        }
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
        /// Output partition of this stream, used to name its reservations
        partition: usize,
        /// Paces this stream through the left chunks, together with the
        /// streams of the other partitions.
        left_chunk_barrier: Arc<LeftChunkBarrier>,
    },

    /// Memory-limited mode is running. Left data is read back in chunks
    /// and the right side is spilled to disk for re-scanning.
    Active(Box<SpillStateActive>),
}

/// State for active memory-limited spill execution.
/// Boxed inside [`SpillState::Active`] to reduce enum size.
pub(crate) struct SpillStateActive {
    /// The spilled left side, shared by every partition.
    left_spill: Arc<LeftSpillData>,
    /// Paces this stream through the left chunks, together with the streams of
    /// the other partitions.
    left_chunk_barrier: Arc<LeftChunkBarrier>,
    /// Index of the chunk being probed, or of the next one while none is
    chunk_index: usize,
    /// The wait for the next chunk, while it is in flight
    chunk_fetch: Option<BoxFuture<'static, Result<Arc<LeftChunk>>>>,
    /// The chunk being probed, held to keep its memory accounted for
    current_chunk: Option<Arc<LeftChunk>>,
    /// Memory pool the chunks are accounted in
    memory_pool: Arc<dyn MemoryPool>,
    /// Accounts for what this partition keeps beside the shared chunk: its
    /// visited bitmap for the chunk and, for the emitter, the global one.
    chunk_reservation: MemoryReservation,
    /// Position in the left spill file of the current chunk's first row. See
    /// [`LeftSpillData`] for why bits are addressed this way.
    chunk_row_offset: usize,
    /// Final pass over the left spill file that emits the unmatched-left rows.
    /// Only the elected emitter has one, from `ProbeEnd` on.
    left_unmatched_pass: Option<LeftUnmatchedPass>,
    /// Right-side schema, used to build NULL-padded right columns.
    right_schema: SchemaRef,
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
}

/// The emitter's final pass over the left spill file. See
/// [`NestedLoopJoinStream::handle_emit_left_unmatched_memory_limited`].
struct LeftUnmatchedPass {
    /// Opened on the first entry to `EmitLeftUnmatched`
    stream: Option<SendableRecordBatchStream>,
    /// The complete global visited bitmap, taken from [`LeftSpillData`] and
    /// accounted for in the stream's `chunk_reservation`, so it is released
    /// with the stream whichever way the pass ends.
    visited: BooleanBuffer,
    /// Position in the left spill file of the next batch's first row
    row_offset: usize,
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
    /// rows. Set in the [`NLJState::ProbeEnd`] state, which is entered exactly
    /// once per stream and owns the single `report_probe_completed` call (on
    /// [`JoinLeftData`], or on [`LeftSpillData`] in memory-limited mode): the
    /// stream that drives the
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
    ///
    /// Memory-limited mode (see 'Memory-limited Execution' in
    /// [`NestedLoopJoinExec`]) loops over left chunks before reaching `ProbeEnd`:
    ///
    /// BufferingLeft → FetchingRight (next left chunk loaded)
    /// FetchingRight → BufferingLeft (right pass exhausted, more chunks remain)
    /// FetchingRight → EmitGlobalRightUnmatched → ProbeEnd (after the last chunk,
    /// for join types tracking unmatched right rows)
    /// FetchingRight → ProbeEnd (after the last chunk, otherwise)
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
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
                //    probing for the buffered left data is finished.
                //    (Memory-limited mode moves on to the next left chunk
                //    first, see `finish_left_chunk`.)
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
                //    Probing for the whole left side is finished. Report
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
                //
                // In memory-limited mode the left rows are no longer buffered,
                // so the emitter streams them from the left spill file instead.
                NLJState::EmitLeftUnmatched if self.is_memory_limited() => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    match self.handle_emit_left_unmatched_memory_limited(cx) {
                        ControlFlow::Continue(()) => {}
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll);
                        }
                    }
                }
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

/// A stream that goes away before it has finished stops being waited for.
///
/// The partitions of a memory-limited join move through the left chunks
/// together (see [`LeftChunkBarrier`]), so the others would otherwise wait for
/// it forever. They carry on without it, the same as when the left side fits in
/// memory: there the streams share nothing but the visited bitmap, and a stream
/// that never reports probe completion only means that the final left rows are
/// not emitted. That holds here too. What differs is the bitmap's memory: with
/// no emitter to take it, it is released by the last partition to finish or go
/// away rather than kept for as long as the plan.
impl Drop for NestedLoopJoinStream {
    fn drop(&mut self) {
        if matches!(self.state, NLJState::Done) {
            return;
        }
        // `ProbeEnd` is the only way into `EmitLeftUnmatched`
        let probe_reported = matches!(self.state, NLJState::EmitLeftUnmatched);
        match &self.spill_state {
            SpillState::Active(active) => active
                .left_chunk_barrier
                .depart(active.chunk_index, probe_reported),
            // Not known yet whether the left side spills. If it does not, the
            // barrier is never used and this has no effect.
            SpillState::Pending {
                left_chunk_barrier, ..
            } => left_chunk_barrier.depart(0, probe_reported),
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
            left_probe_idx: 0,
            left_emit_idx: 0,
            left_exhausted: false,
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
            partition,
            left_chunk_barrier,
        } = std::mem::replace(&mut self.spill_state, SpillState::Disabled)
        else {
            return internal_err!(
                "enter_memory_limited_mode called in non-Pending spill state"
            );
        };

        let chunk_reservation =
            MemoryConsumer::new(format!("NestedLoopJoinLeftVisited[{partition}]"))
                .register(context.memory_pool());

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
            Arc::clone(&right_schema),
        )
        .with_compression_type(context.session_config().spill_compression());

        self.spill_state = SpillState::Active(Box::new(SpillStateActive {
            left_spill,
            left_chunk_barrier,
            chunk_index: 0,
            chunk_fetch: None,
            current_chunk: None,
            memory_pool: Arc::clone(context.memory_pool()),
            chunk_reservation,
            chunk_row_offset: 0,
            left_unmatched_pass: None,
            right_schema,
            right_input: ReplayableStreamSource::new(
                right_data,
                right_spill_manager,
                "NestedLoopJoin right spill",
            ),
            global_right_bitmaps: Vec::new(),
            global_right_bitmaps_reservation,
            right_batch_index: 0,
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
                            Ok(()) => ControlFlow::Continue(()),
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
    /// Gets the next left chunk, which every partition shares (see
    /// [`LeftChunkBarrier`]).
    fn handle_buffering_left_memory_limited(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        let build_time = self.metrics.join_metrics.build_time.clone();
        let SpillState::Active(active) = &mut self.spill_state else {
            unreachable!(
                "handle_buffering_left_memory_limited called without Active spill state"
            );
        };

        let row_offset = active.chunk_row_offset;
        if row_offset >= active.left_spill.num_rows {
            // Only an empty spill file ends up here, and the load does not
            // write one. Handled anyway: there is nothing left to probe.
            self.left_exhausted = true;
            self.enter_state_after_last_left_chunk();
            return ControlFlow::Continue(());
        }

        if active.chunk_fetch.is_none() {
            active.chunk_fetch = Some(
                Arc::clone(&active.left_chunk_barrier)
                    .chunk(
                        active.chunk_index,
                        Arc::clone(&active.left_spill),
                        Arc::clone(&active.memory_pool),
                        build_time.clone(),
                    )
                    .boxed(),
            );
        }
        let chunk_fetch = active
            .chunk_fetch
            .as_mut()
            .expect("chunk_fetch installed above");
        let chunk = match chunk_fetch.poll_unpin(cx) {
            Poll::Ready(Ok(chunk)) => chunk,
            Poll::Ready(Err(e)) => return ControlFlow::Break(Poll::Ready(Some(Err(e)))),
            Poll::Pending => return ControlFlow::Break(Poll::Pending),
        };
        active.chunk_fetch = None;

        let _build_timer = build_time.timer();
        let batch = chunk.batch.clone();
        let n_rows = batch.num_rows();
        self.left_exhausted = row_offset + n_rows >= active.left_spill.num_rows;

        // Matches are tracked per partition while probing, so the probe path
        // never contends with other partitions, and merged into the global
        // bitmap once in `finish_left_chunk`.
        let visited_left_side = if need_produce_result_in_final(self.join_type) {
            // Use infallible `grow` for the bitmap -- it's small
            active.chunk_reservation.grow(n_rows.div_ceil(8));
            let mut buffer = BooleanBufferBuilder::new(n_rows);
            buffer.append_n(n_rows, false);
            buffer
        } else {
            BooleanBufferBuilder::new(0)
        };

        // This `JoinLeftData` is private to the partition: it shares the
        // chunk's rows but has its own bitmap, so its probe-threads counter is
        // not used. Probe completion is reported once for the whole left side,
        // on `LeftSpillData`.
        self.buffered_left_data = Some(Arc::new(JoinLeftData::new(
            batch,
            Mutex::new(visited_left_side),
            AtomicUsize::new(1),
            active.chunk_reservation.take(),
        )));
        active.current_chunk = Some(chunk);

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

    /// Record the matches of the chunk that was just probed and move on to the
    /// next one. Memory-limited mode only.
    ///
    /// Unmatched-left rows are not emitted here, which would mean holding on to
    /// the chunk until every partition had probed it and one of them had gone
    /// through its rows again. Emission is deferred until every partition has
    /// probed every chunk, see
    /// [`Self::handle_emit_left_unmatched_memory_limited`].
    fn finish_left_chunk(&mut self) -> Result<()> {
        let Some(left_data) = self.buffered_left_data.take() else {
            return internal_err!("LeftData should be available");
        };
        let SpillState::Active(active) = &mut self.spill_state else {
            return internal_err!("finish_left_chunk called without Active spill state");
        };

        if need_produce_result_in_final(self.join_type) {
            active
                .left_spill
                .merge_visited(active.chunk_row_offset, &left_data.bitmap().lock());
        }
        active.chunk_row_offset += left_data.batch().num_rows();
        // Let go of the chunk before reporting, so that its memory is free by
        // the time the last partition has reported and the next one is loaded.
        drop(left_data);
        active.current_chunk = None;
        // These two go together: `LeftChunkBarrier::depart` tells a stream that
        // has finished the current chunk from one still probing it by
        // `chunk_index` being past the barrier's.
        active.chunk_index += 1;
        active.left_chunk_barrier.finish_chunk();

        self.left_probe_idx = 0;
        if self.left_exhausted {
            self.enter_state_after_last_left_chunk();
        } else {
            self.state = NLJState::BufferingLeft;
        }
        Ok(())
    }

    /// Every left chunk has been probed. Memory-limited mode only.
    fn enter_state_after_last_left_chunk(&mut self) {
        if self.should_track_unmatched_right {
            // Drop the exhausted right stream so that EmitGlobalRightUnmatched
            // opens a fresh replay pass from the spill file.
            self.right_data = None;
            self.state = NLJState::EmitGlobalRightUnmatched;
        } else {
            self.state = NLJState::ProbeEnd;
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
            None if self.is_memory_limited() => {
                // Right pass exhausted: probing for the current left chunk
                // is finished, but more chunks may remain.
                match self.finish_left_chunk() {
                    Ok(()) => ControlFlow::Continue(()),
                    Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
                }
            }
            None => {
                // Right side exhausted: probing is finished. `ProbeEnd`
                // reports probe completion before emitting unmatched-left
                // rows.
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

    /// Handle ProbeEnd state - record probe completion for this stream.
    ///
    /// Entered exactly once per stream, when it has probed the whole left side.
    /// This is the single place that decrements the shared probe-threads counter
    /// via `report_probe_completed`: the stream that drives the
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
        // Decrement the shared counter exactly once for this stream. The
        // last stream to finish probing (the one that drives the counter to
        // zero) becomes the unmatched-left emitter.
        let is_emitter = if let SpillState::Active(active) = &mut self.spill_state {
            match active.left_spill.report_probe_completed() {
                Some(visited) => {
                    if need_produce_result_in_final(self.join_type) {
                        // Every chunk is done, which leaves `chunk_reservation`
                        // free to account for the bitmap this stream now owns.
                        active.chunk_reservation.grow(visited.len().div_ceil(8));
                        active.left_unmatched_pass = Some(LeftUnmatchedPass {
                            stream: None,
                            visited,
                            row_offset: 0,
                        });
                    }
                    true
                }
                None => false,
            }
        } else {
            match self.get_left_data() {
                Ok(left_data) => left_data.report_probe_completed(),
                Err(e) => return ControlFlow::Break(Poll::Ready(Some(Err(e)))),
            }
        };
        self.is_unmatched_left_emitter = is_emitter;
        self.state = NLJState::EmitLeftUnmatched;
        ControlFlow::Continue(())
    }

    /// Handle EmitLeftUnmatched state - emit unmatched left rows.
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
            // We have finished processing all unmatched rows
            Ok(false) => match self.output_buffer.finish_buffered_batch() {
                Ok(()) => {
                    self.buffered_left_data = None;
                    self.state = NLJState::Done;
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
            },
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        }
    }

    /// Memory-limited path for handle_emit_left_unmatched.
    ///
    /// The global visited bitmap is complete once the last stream has reported
    /// probe completion, and that stream is the emitter. It streams the left
    /// spill file one more time and emits the final left rows batch by batch,
    /// so no left chunk has to be held in memory for it. This mirrors
    /// `EmitGlobalRightUnmatched` on the right side.
    fn handle_emit_left_unmatched_memory_limited(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Return any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        let SpillState::Active(active) = &mut self.spill_state else {
            unreachable!("memory-limited EmitLeftUnmatched without Active spill state");
        };

        // On first entry, the emitter opens its pass over the left spill file
        if let Some(pass) = active.left_unmatched_pass.as_mut()
            && pass.stream.is_none()
        {
            let join_metric = self.metrics.join_metrics.join_time.clone();
            let _join_timer = join_metric.timer();
            match active.left_spill.open_pass() {
                Ok(stream) => pass.stream = Some(stream),
                Err(e) => return ControlFlow::Break(Poll::Ready(Some(Err(e)))),
            }
        }

        // Poll the spill stream for the next left batch. Streams that are not
        // the emitter have nothing to read.
        let result = match active.left_unmatched_pass.as_mut() {
            Some(pass) => match pass
                .stream
                .as_mut()
                .expect("the pass was opened above")
                .poll_next_unpin(cx)
            {
                Poll::Ready(result) => result,
                Poll::Pending => return ControlFlow::Break(Poll::Pending),
            },
            None => None,
        };

        let join_metric = self.metrics.join_metrics.join_time.clone();
        let _join_timer = join_metric.timer();
        match result {
            Some(Ok(left_batch)) => {
                let pass = active
                    .left_unmatched_pass
                    .as_mut()
                    .expect("a left batch was read from the pass");
                let n_rows = left_batch.num_rows();
                let bitmap =
                    BooleanArray::new(pass.visited.slice(pass.row_offset, n_rows), None);
                pass.row_offset += n_rows;

                match build_unmatched_batch(
                    &self.output_schema,
                    &left_batch,
                    bitmap,
                    &active.right_schema,
                    &self.column_indices,
                    self.join_type,
                    JoinSide::Left,
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
                active.left_unmatched_pass = None;
                active.chunk_reservation.free();
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

                match build_unmatched_batch(
                    &self.output_schema,
                    &right_batch,
                    bitmap,
                    &active.left_spill.schema,
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
                // All right batches replayed. This stream has now probed the
                // whole left side, which `ProbeEnd` reports.
                self.right_data = None;
                self.state = NLJState::ProbeEnd;
                ControlFlow::Continue(())
            }
        }
    }

    /// Handle Done state - final state processing
    fn handle_done(&mut self) -> Poll<Option<Result<RecordBatch>>> {
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
        let left_batch = left_data.batch();
        let right_rows = right_batch.num_rows();
        let total_rows = l_row_count * right_rows;

        // Build index arrays for cartesian product: left_range X right_batch
        let left_indices = left_batch.row_indices(
            (0..l_row_count)
                .flat_map(|i| std::iter::repeat_n(l_start_index + i, right_rows)),
        )?;
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
                        left_batch.take_column(column_index.index, &left_indices)?
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
                left_batch.take_column(column_index.index, &left_indices)?
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

        let left_row = left_data.batch().row(l_index)?;
        let cur_right_bitmap = if let Some(filter) = &self.join_filter {
            apply_filter_to_row_join_batch(left_row, right_batch, filter)?
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
                left_row,
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
        let left_batch_sliced =
            left_data.batch().slice(start_idx, end_idx - start_idx)?;

        let bitmap_sliced = {
            let bitmap = left_data.bitmap().lock();
            BooleanBuffer::collect_bool(end_idx - start_idx, |i| {
                bitmap.get_bit(start_idx + i)
            })
        };
        let bitmap_sliced = BooleanArray::new(bitmap_sliced, None);

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
/// (left_row in left buffer) x (right batch)
/// Returns a bitmap, with successfully joined indices set to true
fn apply_filter_to_row_join_batch(
    left_row: BatchRow<'_>,
    right_batch: &RecordBatch,
    filter: &JoinFilter,
) -> Result<BooleanArray> {
    debug_assert!(right_batch.num_rows() != 0);

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
            left_row,
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

/// This function performs the following steps:
/// 1. Apply filter to probe-side batch
/// 2. Broadcast the build row (`build_row`) to the filtered probe-side batch
/// 3. Concat them together according to `col_indices`, and return the result
///    (None if the result is empty)
///
/// Example:
/// build side batch:
/// a
/// ----
/// 1
/// 2
/// 3
///
/// # 0 index row of the build side batch (that is `1`) will be used
/// build_row: row 0
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
    build_row: BatchRow<'_>,
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
            let original_left_array = build_row.column(column_index.index)?;

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
                        build_row.index() as u64,
                        filtered_probe_batch.num_rows(),
                    );
                    let indices_array = UInt64Array::from_iter_values(indices_iter);
                    take(original_left_array.as_ref(), &indices_array, None)?
                }
                _ => {
                    let scalar_value = ScalarValue::try_from_array(
                        original_left_array.as_ref(),
                        build_row.index(),
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
                // Keep the placeholder row even when no columns from this
                // side are projected, so BatchRow can address row 0.
                create_record_batch_with_empty_schema(nullable_left_schema, 1)?
            } else {
                RecordBatch::try_new(nullable_left_schema, left_null_columns)?
            };

            debug_assert_ne!(batch_side, JoinSide::None);
            let opposite_side = batch_side.negate();

            build_row_join_batch(
                output_schema,
                BatchRow::new(&left_null_batch, 0)?,
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

    #[test]
    fn test_nlj_dynamic_filter_pushdown() -> Result<()> {
        use crate::filter_pushdown::PushedDown;
        use arrow::array::record_batch;
        use datafusion_physical_expr::expressions::lit;

        // Identical names within and across inputs must not affect routing.
        // Reordered outputs also exercise the NLJ's embedded projection.
        let batch = record_batch!(("key", Int32, [1, 2]), ("key", Int32, [2, 1]))?;
        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?;
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::LeftMark,
            JoinType::RightMark,
        ] {
            let join = NestedLoopJoinExec::try_new(
                Arc::clone(&input),
                Arc::clone(&input),
                None,
                &join_type,
                None,
            )?;
            for reorder in [false, true] {
                let projection =
                    reorder.then(|| (0..join.schema().fields().len()).rev().collect());
                let join = join.with_projection(projection)?;
                for output in 0..join.schema().fields().len() {
                    let column: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(join.schema().field(output).name(), output));
                    let source = Arc::new(DynamicFilterPhysicalExpr::new(
                        vec![Arc::clone(&column)],
                        lit(true),
                    ));
                    let filters = join
                        .gather_filters_for_pushdown(
                            FilterPushdownPhase::Post,
                            vec![Arc::clone(&source) as _],
                            &ConfigOptions::default(),
                        )?
                        .parent_filters();
                    let unprojected =
                        join.projection.as_ref().map_or(output, |p| p[output]);
                    let side = join.column_indices[unprojected].side;
                    let expected = match join_type {
                        JoinType::Inner => {
                            [side == JoinSide::Left, side == JoinSide::Right]
                        }
                        JoinType::Left
                        | JoinType::LeftSemi
                        | JoinType::LeftAnti
                        | JoinType::LeftMark => [side == JoinSide::Left, false],
                        JoinType::Right
                        | JoinType::RightSemi
                        | JoinType::RightAnti
                        | JoinType::RightMark => [false, side == JoinSide::Right],
                        JoinType::Full => [false, false],
                    };
                    // Update after routing to prove the remapped consumer stays live.
                    source.update(Arc::new(BinaryExpr::new(
                        column,
                        Operator::Eq,
                        lit(2i32),
                    )))?;
                    for (child, accepted) in filters.iter().zip(expected) {
                        assert_eq!(
                            matches!(child[0].discriminant, PushedDown::Yes),
                            accepted,
                            "{join_type:?}, reorder={reorder}, output={output}"
                        );
                        if accepted {
                            assert_eq!(
                                child[0].predicate.expression_id(),
                                source.expression_id()
                            );
                            let values = child[0]
                                .predicate
                                .evaluate(&batch)?
                                .into_array(batch.num_rows())?;
                            assert_eq!(
                                as_boolean_array(&values)?.iter().collect::<Vec<_>>(),
                                if join.column_indices[unprojected].index == 0 {
                                    vec![Some(false), Some(true)]
                                } else {
                                    vec![Some(true), Some(false)]
                                }
                            );
                        }
                    }
                }
            }
        }

        let join = NestedLoopJoinExec::try_new(
            Arc::clone(&input),
            input,
            None,
            &JoinType::Inner,
            None,
        )?;
        let left: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let right: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 2));
        let whole: Arc<dyn PhysicalExpr> = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&left)],
            lit(true),
        ));
        let mixed: Arc<dyn PhysicalExpr> = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&left), right],
            lit(true),
        ));
        let static_filter: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(left, Operator::Eq, lit(2i32)));
        for (phase, enabled, filters) in [
            (FilterPushdownPhase::Pre, true, vec![Arc::clone(&whole)]),
            (FilterPushdownPhase::Post, false, vec![whole]),
            (FilterPushdownPhase::Post, true, vec![mixed, static_filter]),
        ] {
            let mut config = ConfigOptions::default();
            config.optimizer.enable_join_dynamic_filter_pushdown = enabled;
            let description =
                join.gather_filters_for_pushdown(phase, filters, &config)?;
            assert!(
                description
                    .parent_filters()
                    .iter()
                    .flatten()
                    .all(|f| matches!(f.discriminant, PushedDown::No))
            );
        }
        Ok(())
    }

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
            let left_spill = LeftSpillData::new(
                SpilledLeftFile {
                    spill_manager: left_spill_manager,
                    spill_file: left_spill_file,
                    num_rows: left_batch.num_rows(),
                },
                Arc::clone(&left_schema),
                need_produce_result_in_final(join_type),
                1,
                MemoryConsumer::new("NestedLoopJoinLoad[test]".to_string())
                    .register(task_ctx.memory_pool()),
            );
            // The left chunk is read from the delayed stream rather than from
            // the spill file. `build_time_excludes_spill_stream_poll` needs
            // that delay on the build side, and it keeps the final right replay
            // the only spill read, which is what
            // `join_time_excludes_global_right_unmatched_replay_poll` counts.
            *left_spill.reader.lock() = Some(LeftChunkReader {
                stream: left_stream,
                carryover: None,
            });
            let active = SpillStateActive {
                left_spill: Arc::new(left_spill),
                left_chunk_barrier: Arc::new(LeftChunkBarrier::new(1)),
                chunk_index: 0,
                chunk_fetch: None,
                current_chunk: None,
                memory_pool: Arc::clone(task_ctx.memory_pool()),
                chunk_reservation: MemoryConsumer::new(
                    "NestedLoopJoinLeftVisited[test]".to_string(),
                )
                .register(task_ctx.memory_pool()),
                chunk_row_offset: 0,
                left_unmatched_pass: None,
                right_schema: Arc::clone(&right_schema),
                right_input: ReplayableStreamSource::new(
                    right_stream,
                    spill_manager,
                    "test right spill",
                ),
                global_right_bitmaps: Vec::new(),
                global_right_bitmaps_reservation,
                right_batch_index: 0,
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
                    Arc::new(LeftChunkBarrier::new(1)),
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

    #[tokio::test]
    async fn test_left_semi_join_reports_final_emission() -> Result<()> {
        let left = build_left_table();
        let right = build_right_table();
        let join =
            NestedLoopJoinExec::try_new(left, right, None, &JoinType::LeftSemi, None)?;

        assert_eq!(join.properties().emission_type, EmissionType::Final);

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
            // each row to be loaded as a separate chunk.
            Some(1),
            Vec::new(),
        )
    }

    /// Run a NLJ across 4 right partitions under a tight memory limit, so
    /// every output partition takes the memory-limited fallback path. The
    /// right side is shuffled via `RepartitionExec(RoundRobinBatch(4))`.
    ///
    /// The partitions are collected concurrently, which mirrors how they run
    /// under the runtime. They have to be: they share one left chunk at a
    /// time, so the next chunk is only loaded once every partition has
    /// finished the current one (see [`LeftChunkBarrier`]).
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

        let mut handles = vec![];
        for i in 0..partition_count {
            let stream = nested_loop_join.execute(i, Arc::clone(&context))?;
            handles.push(SpawnedTask::spawn(
                async move { common::collect(stream).await },
            ));
        }
        let mut batches = vec![];
        for handle in handles {
            batches.extend(handle.join().await.expect("partition task panicked")?);
        }
        batches.retain(|b| b.num_rows() > 0);

        let metrics = nested_loop_join.metrics().unwrap();
        Ok((columns, batches, metrics))
    }

    /// The left side is split into multiple chunks, and every partition reads
    /// its own. Every left row must appear exactly once: duplicates would
    /// indicate that the visited-left bitmap was not shared across right
    /// partitions, and missing rows that a chunk's matches were merged at the
    /// wrong offset.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_multi_partition_left_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table_multi_chunk();
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

    /// In addition to the global left bitmap exercised by the LEFT case, this
    /// covers the global right-unmatched bitmap accumulated ACROSS all left
    /// chunks and emitted once in `EmitGlobalRightUnmatched`. The two right
    /// rows with `b2 = 10` are filtered out of every match and must appear
    /// exactly once as unmatched-right rows, regardless of how many left
    /// chunks were processed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_multi_partition_full_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table_multi_chunk();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_multi_partition_left_semi_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table_multi_chunk();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_multi_partition_left_anti_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table_multi_chunk();
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
        | 13 | 8  | 130 |
        | 15 | 8  | 150 |
        | 17 | 8  | 170 |
        | 19 | 8  | 190 |
        | 21 | 8  | 210 |
        | 9  | 8  | 90  |
        +----+----+-----+
        "));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_multi_partition_left_mark_join() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(50, 16)?;
        let left = build_left_table_multi_chunk();
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
        | 13 | 8  | 130 | false |
        | 15 | 8  | 150 | false |
        | 17 | 8  | 170 | false |
        | 19 | 8  | 190 | false |
        | 21 | 8  | 210 | false |
        | 5  | 5  | 50  | true  |
        | 9  | 8  | 90  | false |
        +----+----+-----+-------+
        "));
        Ok(())
    }

    /// A finished execution must not keep memory accounted, even while the plan
    /// itself is still alive, as a cached or still-referenced physical plan
    /// would be. The barrier lives on the plan and holds the load of the current
    /// chunk, so it has to let go of the last one as well.
    async fn assert_memory_released_after_completion(join_type: JoinType) -> Result<()> {
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
            build_left_table_multi_chunk(),
            right,
            Some(prepare_join_filter()),
            &join_type,
            None,
        )?);

        let mut handles = vec![];
        for i in 0..partition_count {
            let stream = nested_loop_join.execute(i, Arc::clone(&task_ctx))?;
            handles.push(SpawnedTask::spawn(
                async move { common::collect(stream).await },
            ));
        }
        for handle in handles {
            handle.join().await.expect("partition task panicked")?;
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
            "{join_type}: memory is still reserved after every partition finished"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_releases_memory_left_join() -> Result<()> {
        assert_memory_released_after_completion(JoinType::Left).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_releases_memory_full_join() -> Result<()> {
        assert_memory_released_after_completion(JoinType::Full).await
    }

    /// The join types that emit final left rows, which are the ones whose
    /// partitions share the visited bitmap
    const LEFT_EMITTING_JOIN_TYPES: [JoinType; 5] = [
        JoinType::Left,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::LeftMark,
        JoinType::Full,
    ];

    /// The final left rows among `batches`: for LEFT and FULL the rows with a
    /// left row and NULL right columns, and for the other left-emitting types
    /// every row, as they emit nothing else.
    fn count_final_left_rows(join_type: JoinType, batches: &[RecordBatch]) -> usize {
        batches
            .iter()
            .map(|batch| match join_type {
                JoinType::Left | JoinType::Full => {
                    let left = batch.column(0);
                    let right = batch.column(3);
                    (0..batch.num_rows())
                        .filter(|&i| left.is_valid(i) && right.is_null(i))
                        .count()
                }
                _ => batch.num_rows(),
            })
            .sum()
    }

    /// A two-partition join over a left side of several chunks, under a memory
    /// limit that makes it spill.
    fn dropped_partition_test_plan(
        right: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
    ) -> Result<(Arc<NestedLoopJoinExec>, Arc<TaskContext>)> {
        let task_ctx = task_ctx_with_memory_limit(50, 1)?;
        let right = Arc::new(RepartitionExec::try_new(
            right,
            Partitioning::RoundRobinBatch(2),
        )?) as Arc<dyn ExecutionPlan>;
        let plan = Arc::new(NestedLoopJoinExec::try_new(
            build_left_table_multi_chunk(),
            right,
            Some(prepare_join_filter()),
            &join_type,
            None,
        )?);
        Ok((plan, task_ctx))
    }

    /// The partitions move through the left chunks together, so one that is
    /// dropped before it finishes (as a `LIMIT` above the join may do) has to
    /// stop being waited for. The others then carry on without it, and neither
    /// stall nor fail. As on the in-memory path, the dropped partition never
    /// reports probe completion, so the final left rows are not emitted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_dropped_partition_does_not_stall_peers() -> Result<()>
    {
        let (plan, task_ctx) = dropped_partition_test_plan(
            build_right_table_one_batch_per_row(),
            JoinType::Left,
        )?;

        // Every partition is dropped as soon as it returns rows. The only match
        // comes from the first left chunk, so the partition that finds it goes
        // away with most chunks still to probe, and the other one runs to the
        // end.
        let mut handles = vec![];
        for i in 0..2 {
            let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
            handles.push(SpawnedTask::spawn(async move {
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() > 0 {
                        return Ok(Some(batch));
                    }
                }
                Ok::<_, DataFusionError>(None)
            }));
        }
        let mut batches = vec![];
        for handle in handles {
            let batch = tokio::time::timeout(Duration::from_secs(30), handle.join())
                .await
                .expect("a partition stalled after its peer was dropped")
                .expect("partition task panicked")?;
            batches.extend(batch);
        }
        assert!(
            plan.metrics().unwrap().spill_count().unwrap_or(0) > 0,
            "Expected spilling under tight memory limit"
        );

        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b2 | c2 |
        +----+----+----+----+----+----+
        | 5  | 5  | 50 | 2  | 2  | 80 |
        +----+----+----+----+----+----+
        "));
        // Nothing stays reserved, although the plan is still alive
        assert_eq!(
            task_ctx.memory_pool().reserved(),
            0,
            "a dropped partition must not strand the chunk's or the bitmap's memory"
        );
        drop(plan);
        Ok(())
    }

    /// A partition that has finished the current chunk and is waiting for the
    /// others is counted among the finished ones. When it is dropped it must
    /// come out of that count as well, or the barrier would move on before the
    /// remaining partition has probed the chunk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_partition_dropped_while_waiting_for_peers()
    -> Result<()> {
        for join_type in LEFT_EMITTING_JOIN_TYPES {
            assert_partition_dropped_while_waiting_for_peers(join_type).await?;
        }
        Ok(())
    }

    async fn assert_partition_dropped_while_waiting_for_peers(
        join_type: JoinType,
    ) -> Result<()> {
        // No right row matches, so the join returns nothing until the very end
        let right = build_table(
            ("a2", &vec![12, 10]),
            ("b2", &vec![10, 10]),
            ("c2", &vec![40, 100]),
            Some(1),
            Vec::new(),
        );
        let (plan, task_ctx) = dropped_partition_test_plan(right, join_type)?;
        let mut waiting = plan.execute(0, Arc::clone(&task_ctx))?;
        let survivor = plan.execute(1, Arc::clone(&task_ctx))?;

        tokio::time::timeout(Duration::from_secs(30), async {
            while plan.left_chunk_barrier.inner.lock().finished == 0 {
                assert!(
                    futures::poll!(waiting.next()).is_pending(),
                    "the partition cannot get past the first chunk on its own"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the partition never finished the first chunk");
        drop(waiting);
        {
            let barrier = plan.left_chunk_barrier.inner.lock();
            assert_eq!(
                (barrier.chunk_index, barrier.live, barrier.finished),
                (0, 1, 0)
            );
        }

        let batches =
            tokio::time::timeout(Duration::from_secs(30), common::collect(survivor))
                .await
                .expect("the remaining partition stalled")?;
        // No final left rows, as the dropped partition never reported. A FULL
        // join still emits the survivor's own unmatched right row.
        let expected_rows = usize::from(join_type == JoinType::Full);
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows,
            "{join_type}"
        );
        assert_eq!(plan.left_chunk_barrier.inner.lock().chunk_index, 8);

        // Nothing stays reserved, although the plan is still alive
        assert_eq!(task_ctx.memory_pool().reserved(), 0, "{join_type}");
        Ok(())
    }

    /// The load of a chunk is not tied to the partition that started it, so
    /// dropping that partition right away leaves the chunk to the others.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_partition_dropped_while_loading() -> Result<()> {
        let (plan, task_ctx) = dropped_partition_test_plan(
            build_right_table_one_batch_per_row(),
            JoinType::Left,
        )?;
        let mut loading = plan.execute(0, Arc::clone(&task_ctx))?;
        let survivor = plan.execute(1, Arc::clone(&task_ctx))?;

        // Far enough to spill the left side and ask for the first chunk
        tokio::time::timeout(Duration::from_secs(30), async {
            while plan.left_chunk_barrier.inner.lock().chunk.is_none() {
                let _ = futures::poll!(loading.next());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the partition never asked for the first chunk");
        drop(loading);

        tokio::time::timeout(Duration::from_secs(30), common::collect(survivor))
            .await
            .expect("the remaining partition stalled")?;
        assert_eq!(plan.left_chunk_barrier.inner.lock().chunk_index, 8);

        // Nothing stays reserved, although the plan is still alive
        assert_eq!(task_ctx.memory_pool().reserved(), 0);
        Ok(())
    }

    /// Fails every `read_stream` of the left spill file after the first, which
    /// is the one the chunks are read from. The final pass is the second.
    struct FailFinalPassSpillFile {
        inner: Arc<dyn SpillFile>,
        reads: AtomicUsize,
    }

    impl SpillFile for FailFinalPassSpillFile {
        fn path(&self) -> Option<&std::path::Path> {
            self.inner.path()
        }

        fn size(&self) -> Option<u64> {
            self.inner.size()
        }

        fn read_stream(
            &self,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
            if self.reads.fetch_add(1, Ordering::Relaxed) > 0 {
                return internal_err!("final pass cannot be opened");
            }
            self.inner.read_stream()
        }

        fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
            self.inner.open_writer()
        }
    }

    #[derive(Debug)]
    struct FailFinalPassTempFileFactory {
        inner: Arc<DiskManager>,
    }

    impl TempFileFactory for FailFinalPassTempFileFactory {
        fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>> {
            let file = self.inner.create_tmp_file(description)?;
            if description != "NestedLoopJoin left spill" {
                return Ok(file);
            }
            Ok(Arc::new(FailFinalPassSpillFile {
                inner: file,
                reads: AtomicUsize::new(0),
            }))
        }
    }

    /// The emitter owns the global bitmap from `ProbeEnd` on, so an error
    /// opening the final pass does not leave it reserved on a live plan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_final_pass_open_error_releases_bitmap() -> Result<()>
    {
        let inner = Arc::new(
            DiskManagerBuilder::default()
                .with_mode(DiskManagerMode::OsTmpDirectory)
                .build()?,
        );
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(50, 1.0)
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
                DiskManagerMode::Custom(Arc::new(FailFinalPassTempFileFactory { inner })),
            ))
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
            build_left_table_multi_chunk(),
            right,
            Some(prepare_join_filter()),
            &JoinType::Left,
            None,
        )?);

        let streams = (0..2)
            .map(|i| plan.execute(i, Arc::clone(&task_ctx)))
            .collect::<Result<Vec<_>>>()?;
        let results = tokio::time::timeout(
            Duration::from_secs(30),
            futures::future::join_all(streams.into_iter().map(common::collect)),
        )
        .await
        .expect("the join stalled");
        let err = results
            .into_iter()
            .find_map(Result::err)
            .expect("the emitter fails to open the final pass");
        assert_contains!(err.to_string(), "final pass cannot be opened");

        // Nothing stays reserved, although the plan is still alive
        assert_eq!(task_ctx.memory_pool().reserved(), 0);
        Ok(())
    }

    /// A partition dropped before it was ever polled has not seen the spilled
    /// left side, whether or not it exists yet. It must still stop counting as
    /// a partition that will report probe completion, or the bitmap would stay
    /// reserved for as long as the plan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_unpolled_partition_dropped() -> Result<()> {
        for join_type in LEFT_EMITTING_JOIN_TYPES {
            for after_spill in [false, true] {
                let (plan, task_ctx) = dropped_partition_test_plan(
                    build_right_table_one_batch_per_row(),
                    join_type,
                )?;
                let unpolled = plan.execute(0, Arc::clone(&task_ctx))?;
                let mut survivor = plan.execute(1, Arc::clone(&task_ctx))?;
                let mut batches = vec![];
                if after_spill {
                    // The survivor finishes the first chunk, then waits for
                    // the unpolled partition
                    tokio::time::timeout(Duration::from_secs(30), async {
                        while plan.left_chunk_barrier.inner.lock().finished == 0 {
                            if let Poll::Ready(Some(batch)) =
                                futures::poll!(survivor.next())
                            {
                                batches.push(batch?);
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        Ok::<_, DataFusionError>(())
                    })
                    .await
                    .expect("the survivor never finished the first chunk")?;
                    assert!(plan.left_chunk_barrier.inner.lock().left_spill.is_some());
                }
                drop(unpolled);

                batches.extend(
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        common::collect(survivor),
                    )
                    .await
                    .expect("the remaining partition stalled")?,
                );
                assert!(
                    plan.metrics().unwrap().spill_count().unwrap_or(0) > 0,
                    "{join_type}: expected spilling under tight memory limit"
                );
                // No final left rows, as the dropped partition never reported
                assert_eq!(
                    count_final_left_rows(join_type, &batches),
                    0,
                    "{join_type} after_spill={after_spill}"
                );
                assert_eq!(
                    task_ctx.memory_pool().reserved(),
                    0,
                    "{join_type} after_spill={after_spill}: memory still reserved \
                     while the plan is alive"
                );
            }
        }
        Ok(())
    }

    /// A FULL partition that has probed every chunk goes on to replay its right
    /// spill file for the unmatched right rows, and only reports probe
    /// completion after that. Dropping it during the replay leaves it past the
    /// barrier's last chunk but unreported.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_full_join_dropped_during_right_replay() -> Result<()>
    {
        let (plan, task_ctx) = dropped_partition_test_plan(
            build_right_table_one_batch_per_row(),
            JoinType::Full,
        )?;
        let mut streams = vec![
            plan.execute(0, Arc::clone(&task_ctx))?,
            plan.execute(1, Arc::clone(&task_ctx))?,
        ];

        // Poll both until one of them emits an unmatched right row (NULL left
        // columns), which only happens in the final right replay. With a batch
        // size of 1 it has not reported probe completion by then.
        let replaying = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                for (i, stream) in streams.iter_mut().enumerate() {
                    if let Poll::Ready(Some(batch)) = futures::poll!(stream.next()) {
                        let batch = batch?;
                        if batch.num_rows() > 0 && batch.column(0).null_count() > 0 {
                            return Ok::<_, DataFusionError>(i);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("no partition reached the right replay")?;
        let left_spill = Arc::clone(
            plan.left_chunk_barrier
                .inner
                .lock()
                .left_spill
                .as_ref()
                .expect("the left side spilled"),
        );
        assert!(
            left_spill.probe_threads_counter.load(Ordering::Acquire) >= 1,
            "the replaying partition has not reported"
        );
        drop(streams.remove(replaying));
        let survivor = streams.pop().unwrap();

        let batches =
            tokio::time::timeout(Duration::from_secs(30), common::collect(survivor))
                .await
                .expect("the remaining partition stalled")?;
        // The survivor emits its own unmatched right rows, but the final left
        // rows are not emitted
        assert_eq!(count_final_left_rows(JoinType::Full, &batches), 0);
        assert!(left_spill.incomplete.load(Ordering::Acquire));
        assert_eq!(left_spill.probe_threads_counter.load(Ordering::Acquire), 0);
        drop(left_spill);

        // Nothing stays reserved, although the plan is still alive
        assert_eq!(task_ctx.memory_pool().reserved(), 0);
        Ok(())
    }

    /// Like `task_ctx_with_memory_limit`, but also sets
    /// `enable_nlj_coordinated_fallback = false` (the opt-out a distributed
    /// engine would use).
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
    /// first error, if any. Used to assert that the opt-out makes a
    /// left-emitting multi-partition join fail with resource exhaustion (rather
    /// than spill, or -- in a distributed setting -- lose its final left rows).
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
    /// a multi-partition right side must NOT take the fallback: it fails with
    /// resource exhaustion under a tight memory limit instead. This is the
    /// distributed-safe opt-out (across processes, the partitions would not
    /// share the visited-left bitmap the fallback relies on).
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

    /// FULL join counterpart of the above: the opt-out disables the fallback
    /// for FULL (also a left-emitting join) with a multi-partition right side,
    /// so it fails with resource exhaustion rather than spilling.
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
    /// multi-partition right side needs no visited-left bitmap -- each
    /// partition owns its right rows exclusively -- so the opt-out must still
    /// leave it spilling rather than failing. This pins the scope of the guard
    /// so a future change cannot quietly turn off the spill fallback for join
    /// types that never needed the shared bitmap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_nlj_memory_limited_fallback_disabled_right_join_still_spills()
    -> Result<()> {
        for join_type in [JoinType::Right, JoinType::Inner] {
            let task_ctx = task_ctx_with_memory_limit_no_coordinated_fallback(50, 16)?;
            let (_columns, _batches, metrics) =
                multi_partition_memory_limited_join_collect(
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
