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

use std::any::Any;
use std::fmt::Formatter;
use std::ops::{BitOr, ControlFlow};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;

use super::utils::{
    asymmetric_join_output_partitioning, need_produce_result_in_final,
    reorder_output_after_swap, swap_join_projection,
};
use crate::common::can_project;
use crate::execution_plan::{boundedness_from_children, EmissionType};
use crate::joins::utils::{
    build_join_schema, check_join_is_valid, estimate_join_statistics,
    need_produce_right_in_final, BuildProbeJoinMetrics, ColumnIndex, JoinFilter,
    OnceAsync, OnceFut,
};
use crate::joins::SharedBitmapBuilder;
use crate::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricType, MetricsSet, RatioMetrics,
};
use crate::projection::{
    try_embed_projection, try_pushdown_through_join, EmbeddedProjection, JoinData,
    ProjectionExec,
};
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::{
    new_null_array, Array, BooleanArray, BooleanBufferBuilder, RecordBatchOptions,
    UInt32Array, UInt64Array,
};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{
    concat_batches, filter, filter_record_batch, not, take, BatchCoalescer,
};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{
    arrow_err, assert_eq_or_internal_err, internal_datafusion_err, internal_err,
    project_schema, unwrap_or_internal_err, JoinSide, Result, ScalarValue, Statistics,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_expr::JoinType;
use datafusion_physical_expr::equivalence::{
    join_equivalence_properties, ProjectionMapping,
};

use futures::{Stream, StreamExt, TryStreamExt};
use log::debug;
use parking_lot::Mutex;

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
/// # TODO: Memory-limited Execution
/// If the memory budget is exceeded during left-side buffering, fallback
/// strategies such as streaming left batches and re-scanning the right side
/// may be implemented in the future.
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
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Projection to apply to the output of the join
    projection: Option<Vec<usize>>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
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
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);
        let join_schema = Arc::new(join_schema);
        let cache = Self::compute_properties(
            &left,
            &right,
            &join_schema,
            *join_type,
            projection.as_ref(),
        )?;

        Ok(NestedLoopJoinExec {
            left,
            right,
            filter,
            join_type: *join_type,
            join_schema,
            build_side_data: Default::default(),
            column_indices,
            projection,
            metrics: Default::default(),
            cache,
        })
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

    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        join_type: JoinType,
        projection: Option<&Vec<usize>>,
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
            let out_schema = project_schema(schema, Some(projection))?;
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
        // check if the projection is valid
        can_project(&self.schema(), projection.as_ref())?;
        let projection = match projection {
            Some(projection) => match &self.projection {
                Some(p) => Some(projection.iter().map(|i| p[*i]).collect()),
                None => Some(projection),
            },
            None => None,
        };
        Self::try_new(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            self.filter.clone(),
            &self.join_type,
            projection,
        )
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
                self.projection.as_ref(),
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![
            Distribution::SinglePartition,
            Distribution::UnspecifiedDistribution,
        ]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NestedLoopJoinExec::try_new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.filter.clone(),
            &self.join_type,
            self.projection.clone(),
        )?))
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

        // Initialization reservation for load of inner table
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
                self.right().output_partitioning().partition_count(),
            ))
        })?;

        let batch_size = context.session_config().batch_size();

        let probe_side_data = self.right.execute(partition, context)?;

        // update column indices to reflect the projection
        let column_indices_after_projection = match &self.projection {
            Some(projection) => projection
                .iter()
                .map(|i| self.column_indices[*i].clone())
                .collect(),
            None => self.column_indices.clone(),
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
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_some() {
            return Ok(Statistics::new_unknown(&self.schema()));
        }
        let join_columns = Vec::new();
        estimate_join_statistics(
            self.left.partition_statistics(None)?,
            self.right.partition_statistics(None)?,
            &join_columns,
            &self.join_type,
            &self.schema(),
        )
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
        }) = try_pushdown_through_join(
            projection,
            self.left(),
            self.right(),
            &[],
            &schema,
            self.filter(),
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

/// Asynchronously collect input into a single batch, and creates `JoinLeftData` from it
async fn collect_left_input(
    stream: SendableRecordBatchStream,
    join_metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    with_visited_left_side: bool,
    probe_threads_count: usize,
) -> Result<JoinLeftData> {
    let schema = stream.schema();

    // Load all batches and count the rows
    let (batches, metrics, mut reservation) = stream
        .try_fold(
            (Vec::new(), join_metrics, reservation),
            |(mut batches, metrics, mut reservation), batch| async {
                let batch_size = batch.get_array_memory_size();
                // Reserve memory for incoming batch
                reservation.try_grow(batch_size)?;
                // Update metrics
                metrics.build_mem_used.add(batch_size);
                metrics.build_input_batches.add(1);
                metrics.build_input_rows.add(batch.num_rows());
                // Push batch to output
                batches.push(batch);
                Ok((batches, metrics, reservation))
            },
        )
        .await?;

    let merged_batch = concat_batches(&schema, &batches)?;

    // Reserve memory for visited_left_side bitmap if required by join type
    let visited_left_side = if with_visited_left_side {
        let n_rows = merged_batch.num_rows();
        let buffer_size = n_rows.div_ceil(8);
        reservation.try_grow(buffer_size)?;
        metrics.build_mem_used.add(buffer_size);

        let mut buffer = BooleanBufferBuilder::new(n_rows);
        buffer.append_n(n_rows, false);
        buffer
    } else {
        BooleanBufferBuilder::new(0)
    };

    Ok(JoinLeftData::new(
        merged_batch,
        Mutex::new(visited_left_side),
        AtomicUsize::new(probe_threads_count),
        reservation,
    ))
}

/// States for join processing. See `poll_next()` comment for more details about
/// state transitions.
#[derive(Debug, Clone, Copy)]
enum NLJState {
    BufferingLeft,
    FetchingRight,
    ProbeRight,
    EmitRightUnmatched,
    EmitLeftUnmatched,
    Done,
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
    pub(crate) right_data: SendableRecordBatchStream,
    /// the build-side table data of the nested loop join
    pub(crate) left_data: OnceFut<JoinLeftData>,
    /// Projection to construct the output schema from the left and right tables.
    /// Example:
    /// - output_schema: ['a', 'c']
    /// - left_schema: ['a', 'b']
    /// - right_schema: ['c']
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
    /// If we can buffer all left data in one pass
    /// TODO(now): this is for the (unimplemented) memory-limited execution
    #[expect(dead_code)]
    left_buffered_in_one_pass: bool,

    // Probe(right) side
    // -----------------
    /// The current probe batch to process
    current_right_batch: Option<RecordBatch>,
    // For right join, keep track of matched rows in `current_right_batch`
    // Constructed when fetching each new incoming right batch in `FetchingRight` state.
    current_right_batch_matched: Option<BooleanArray>,
}

pub(crate) struct NestedLoopJoinMetrics {
    /// Join execution metrics
    pub(crate) join_metrics: BuildProbeJoinMetrics,
    /// Selectivity of the join: output_rows / (left_rows * right_rows)
    pub(crate) selectivity: RatioMetrics,
}

impl NestedLoopJoinMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            join_metrics: BuildProbeJoinMetrics::new(partition, metrics),
            selectivity: MetricBuilder::new(metrics)
                .with_type(MetricType::SUMMARY)
                .ratio_metrics("selectivity", partition),
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
    /// FetchingRight → EmitLeftUnmatched (if right exhausted)
    ///
    /// ProbeRight → ProbeRight (next left row or after yielding output)
    /// ProbeRight → EmitRightUnmatched (for special join types like right join)
    /// ProbeRight → FetchingRight (done with the current right batch)
    ///
    /// EmitRightUnmatched → FetchingRight
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
            match self.state {
                // # NLJState transitions
                // --> FetchingRight
                // This state will prepare the left side batches, next state
                // `FetchingRight` is responsible for preparing a single probe
                // side batch, before start joining.
                NLJState::BufferingLeft => {
                    debug!("[NLJState] Entering: {:?}", self.state);
                    // inside `collect_left_input` (the routine to buffer build
                    // -side batches), related metrics except build time will be
                    // updated.
                    // stop on drop
                    let build_metric = self.metrics.join_metrics.build_time.clone();
                    let _build_timer = build_metric.timer();

                    match self.handle_buffering_left(cx) {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => return poll,
                    }
                }

                // # NLJState transitions:
                // 1. --> ProbeRight
                //    Start processing the join for the newly fetched right
                //    batch.
                // 2. --> EmitLeftUnmatched: When the right side input is exhausted, (maybe) emit
                //    unmatched left side rows.
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
                // for the current buffered left data is finished. We can go to
                // the next `EmitLeftUnmatched` phase to check if there is any
                // special handling (e.g., in cases like left join).
                NLJState::FetchingRight => {
                    debug!("[NLJState] Entering: {:?}", self.state);
                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_fetching_right(cx) {
                        ControlFlow::Continue(()) => continue,
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
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll)
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
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll)
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
                // TODO: For memory-limited case, go back to `BufferingLeft`
                // state again.
                NLJState::EmitLeftUnmatched => {
                    debug!("[NLJState] Entering: {:?}", self.state);

                    // stop on drop
                    let join_metric = self.metrics.join_metrics.join_time.clone();
                    let _join_timer = join_metric.timer();

                    match self.handle_emit_left_unmatched() {
                        ControlFlow::Continue(()) => continue,
                        ControlFlow::Break(poll) => {
                            return self.metrics.join_metrics.baseline.record_poll(poll)
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

impl NestedLoopJoinStream {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: Arc<Schema>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        right_data: SendableRecordBatchStream,
        left_data: OnceFut<JoinLeftData>,
        column_indices: Vec<ColumnIndex>,
        metrics: NestedLoopJoinMetrics,
        batch_size: usize,
    ) -> Self {
        Self {
            output_schema: Arc::clone(&schema),
            join_filter: filter,
            join_type,
            right_data,
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
            left_buffered_in_one_pass: true,
            handled_empty_output: false,
            should_track_unmatched_right: need_produce_right_in_final(join_type),
        }
    }

    // ==== State handler functions ====

    /// Handle BufferingLeft state - prepare left side batches
    fn handle_buffering_left(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        match self.left_data.get_shared(cx) {
            Poll::Ready(Ok(left_data)) => {
                self.buffered_left_data = Some(left_data);
                // TODO: implement memory-limited case
                self.left_exhausted = true;
                self.state = NLJState::FetchingRight;
                // Continue to next state immediately
                ControlFlow::Continue(())
            }
            Poll::Ready(Err(e)) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
            Poll::Pending => ControlFlow::Break(Poll::Pending),
        }
    }

    /// Handle FetchingRight state - fetch next right batch and prepare for processing
    fn handle_fetching_right(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        match self.right_data.poll_next_unpin(cx) {
            Poll::Ready(result) => match result {
                Some(Ok(right_batch)) => {
                    // Update metrics
                    let right_batch_size = right_batch.num_rows();
                    self.metrics.join_metrics.input_rows.add(right_batch_size);
                    self.metrics.join_metrics.input_batches.add(1);

                    // Skip the empty batch
                    if right_batch_size == 0 {
                        return ControlFlow::Continue(());
                    }

                    self.current_right_batch = Some(right_batch);

                    // Prepare right bitmap
                    if self.should_track_unmatched_right {
                        let zeroed_buf = BooleanBuffer::new_unset(right_batch_size);
                        self.current_right_batch_matched =
                            Some(BooleanArray::new(zeroed_buf, None));
                    }

                    self.left_probe_idx = 0;
                    self.state = NLJState::ProbeRight;
                    ControlFlow::Continue(())
                }
                Some(Err(e)) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
                None => {
                    // Right stream exhausted
                    self.state = NLJState::EmitLeftUnmatched;
                    ControlFlow::Continue(())
                }
            },
            Poll::Pending => ControlFlow::Break(Poll::Pending),
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

    /// Handle EmitRightUnmatched state - emit unmatched right rows
    fn handle_emit_right_unmatched(
        &mut self,
    ) -> ControlFlow<Poll<Option<Result<RecordBatch>>>> {
        // Return any completed batches first
        if let Some(poll) = self.maybe_flush_ready_batch() {
            return ControlFlow::Break(poll);
        }

        debug_assert!(
            self.current_right_batch_matched.is_some()
                && self.current_right_batch.is_some(),
            "This state is yielding output for unmatched rows in the current right batch, so both the right batch and the bitmap must be present"
        );
        // Construct the result batch for unmatched right rows using a utility function
        match self.process_right_unmatched() {
            Ok(Some(batch)) => {
                match self.output_buffer.push_batch(batch) {
                    Ok(()) => {
                        // Processed all in one pass
                        // cleared inside `process_right_unmatched`
                        debug_assert!(self.current_right_batch.is_none());
                        self.state = NLJState::FetchingRight;
                        ControlFlow::Continue(())
                    }
                    Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
                }
            }
            Ok(None) => {
                // Processed all in one pass
                // cleared inside `process_right_unmatched`
                debug_assert!(self.current_right_batch.is_none());
                self.state = NLJState::FetchingRight;
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
        }
    }

    /// Handle EmitLeftUnmatched state - emit unmatched left rows
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
            // To Done state
            // We have finished processing all unmatched rows
            Ok(false) => match self.output_buffer.finish_buffered_batch() {
                Ok(()) => {
                    self.state = NLJState::Done;
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(Poll::Ready(Some(arrow_err!(e)))),
            },
            Err(e) => ControlFlow::Break(Poll::Ready(Some(Err(e)))),
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

            debug_assert!(l_row_count != 0, "This function should only be entered when there are remaining left rows to process");
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

            if let Some(bitmap) = left_bitmap.as_mut() {
                if is_matched {
                    // Map local index back to absolute left index within the batch
                    bitmap.set_bit(l_index, true);
                }
            }

            if let Some(bitmap) = local_right_bitmap.as_mut() {
                if is_matched {
                    bitmap.set_bit(r_index, true);
                }
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

        if cur_right_bitmap.true_count() == 0 {
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
        // Early return if another thread is already processing unmatched rows
        let handled_by_other_partition =
            self.left_emit_idx == 0 && !left_data.report_probe_completed();
        // Stop processing unmatched rows, the caller will go to the next state
        let finished = self.left_emit_idx >= left_batch.num_rows();

        if join_type_no_produce_left || handled_by_other_partition || finished {
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

        let right_schema = self.right_data.schema();
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
        if self.output_buffer.has_completed_batch() {
            if let Some(batch) = self.output_buffer.next_completed_batch() {
                // Update output rows for selectivity metric
                let output_rows = batch.num_rows();
                self.metrics.selectivity.add_part(output_rows);

                return Some(Poll::Ready(Some(Ok(batch))));
            }
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

        // number of successfully joined pairs from (l_index x cur_right_batch)
        let joined_len = r_matched_bitmap.true_count();

        // 1. Maybe update the left bitmap
        if need_produce_result_in_final(self.join_type) && (joined_len > 0) {
            let mut bitmap = left_data.bitmap().lock();
            bitmap.set_bit(l_index, true);
        }

        // 2. Maybe updateh the right bitmap
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
    debug_assert!(build_side != JoinSide::None);

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
            // Avoid using `ScalarValue::to_array_of_size()` for `List(Utf8View)` to avoid
            // deep copies for buffers inside `Utf8View` array. See below for details.
            // https://github.com/apache/datafusion/issues/18159
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

            // create a recordbatch, with left_schema, of only one row of all nulls
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
                    .map(|field| {
                        (**field).clone().with_nullable(true)
                    })
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

            build_row_join_batch(output_schema, &left_null_batch, 0, batch, Some(flipped_bitmap), col_indices, opposite_side)

        },
        JoinType::RightSemi | JoinType::RightAnti | JoinType::LeftSemi | JoinType::LeftAnti => {
            if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
                debug_assert_eq!(batch_side, JoinSide::Right);
            }
            if matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                debug_assert_eq!(batch_side, JoinSide::Left);
            }

            let bitmap = if matches!(join_type, JoinType::LeftSemi | JoinType::RightSemi) {
                batch_bitmap.clone()
            } else {
                not(&batch_bitmap)?
            };

            if bitmap.true_count() == 0 {
                return Ok(None);
            }

            let mut columns: Vec<Arc<dyn Array>> =
                Vec::with_capacity(output_schema.fields().len());

            for column_index in col_indices {
                debug_assert!(column_index.side == batch_side);

                let col = batch.column(column_index.index);
                let filtered_col = filter(col, &bitmap)?;

                columns.push(filtered_col);
            }

            Ok(Some(RecordBatch::try_new(Arc::clone(output_schema), columns)?))
        },
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
                        Some(right_batch_bitmap) => {columns.push(Arc::new(right_batch_bitmap))},
                        None => unreachable!("Should only be one mark column"),
                    }
                } else {
                    return internal_err!("Not possible to have this join side for RightMark join");
                }
            }

            Ok(Some(RecordBatch::try_new(Arc::clone(output_schema), columns)?))
        }
        _ => internal_err!("If batch is at right side, this function must be handling Full/Right/RightSemi/RightAnti/RightMark joins"),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::test::{assert_join_metrics, TestMemoryExec};
    use crate::{
        common, expressions::Column, repartition::RepartitionExec, test::build_table_i32,
    };

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::test_util::batches_to_sort_string;
    use datafusion_common::{assert_contains, ScalarValue};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion_physical_expr::{Partitioning, PhysicalExpr};
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

    use insta::allow_duplicates;
    use insta::assert_snapshot;
    use rstest::rstest;

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
        dbg!(&batch_size);
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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            | 5  | 5  | 50 | 2  | 2  | 80 |
            +----+----+----+----+----+----+
            "#));

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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+----+----+----+
            | a1 | b1 | c1  | a2 | b2 | c2 |
            +----+----+-----+----+----+----+
            | 11 | 8  | 110 |    |    |    |
            | 5  | 5  | 50  | 2  | 2  | 80 |
            | 9  | 8  | 90  |    |    |    |
            +----+----+-----+----+----+----+
            "#));

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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+-----+
            | a1 | b1 | c1 | a2 | b2 | c2  |
            +----+----+----+----+----+-----+
            |    |    |    | 10 | 10 | 100 |
            |    |    |    | 12 | 10 | 40  |
            | 5  | 5  | 50 | 2  | 2  | 80  |
            +----+----+----+----+----+-----+
            "#));

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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+----+----+-----+
            | a1 | b1 | c1  | a2 | b2 | c2  |
            +----+----+-----+----+----+-----+
            |    |    |     | 10 | 10 | 100 |
            |    |    |     | 12 | 10 | 40  |
            | 11 | 8  | 110 |    |    |     |
            | 5  | 5  | 50  | 2  | 2  | 80  |
            | 9  | 8  | 90  |    |    |     |
            +----+----+-----+----+----+-----+
            "#));

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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c1 |
            +----+----+----+
            | 5  | 5  | 50 |
            +----+----+----+
            "#));

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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+
            | a1 | b1 | c1  |
            +----+----+-----+
            | 11 | 8  | 110 |
            | 9  | 8  | 90  |
            +----+----+-----+
            "#));

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
        let stats = nested_loop_join.partition_statistics(None)?;
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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+
            | a2 | b2 | c2 |
            +----+----+----+
            | 2  | 2  | 80 |
            +----+----+----+
            "#));

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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+
            | a2 | b2 | c2  |
            +----+----+-----+
            | 10 | 10 | 100 |
            | 12 | 10 | 40  |
            +----+----+-----+
            "#));

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
        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+-------+
            | a1 | b1 | c1  | mark  |
            +----+----+-----+-------+
            | 11 | 8  | 110 | false |
            | 5  | 5  | 50  | true  |
            | 9  | 8  | 90  | false |
            +----+----+-----+-------+
            "#));

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

        allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+-------+
            | a2 | b2 | c2  | mark  |
            +----+----+-----+-------+
            | 10 | 10 | 100 | false |
            | 12 | 10 | 40  | false |
            | 2  | 2  | 80  | true  |
            +----+----+-----+-------+
            "#));

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

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::LeftMark,
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::RightMark,
        ];

        for join_type in join_types {
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(100, 1.0)
                .build_arc()?;
            let task_ctx = TaskContext::default().with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let err = multi_partitioned_join_collect(
                Arc::clone(&left),
                Arc::clone(&right),
                &join_type,
                Some(filter.clone()),
                task_ctx,
            )
            .await
            .unwrap_err();

            assert_contains!(
                err.to_string(),
                "Resources exhausted: Additional allocation failed for NestedLoopJoinLoad[0] with top memory consumers (across reservations) as:\n  NestedLoopJoinLoad[0]"
            );
        }

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
