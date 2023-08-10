// Copyright (C) Synnada, Inc. - All Rights Reserved.
// This file does not contain any Apache Software Foundation copyrighted code.

//! This file implements the sliding window hash join algorithm with range-based
//! data pruning to join two (potentially infinite) streams.
//!
//! A [`SlidingHashJoinExec`] plan takes two children plan (with appropriate
//! output ordering) and produces the join output according to the given join
//! type and other options. This operator is appropriate when there is a sliding
//! window constraint among the join conditions. In such cases, the algorithm
//! preserves the output ordering of its probe side yet still achieves bounded
//! memory execution by exploiting the sliding window constraint.
//!
//! This plan uses the [`OneSideHashJoiner`] object to facilitate join calculations
//! for both its children.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::task::Poll;
use std::vec;
use std::{any::Any, usize};
use std::{fmt, mem};

use crate::error::{DataFusionError, Result};
use crate::logical_expr::JoinType;
use crate::physical_plan::common::SharedMemoryReservation;
use crate::physical_plan::expressions::{Column, PhysicalSortExpr};
use crate::physical_plan::joins::{
    hash_join_utils::{
        build_filter_expression_graph, calculate_filter_expr_intervals,
        combine_two_batches, record_visited_indices, IntervalCalculatorInnerState,
        SortedFilterExpr,
    },
    sliding_window_join_utils::{
        adjust_probe_side_indices_by_join_type,
        calculate_build_outer_indices_by_join_type,
        calculate_the_necessary_build_side_range,
        check_if_sliding_window_condition_is_met, get_probe_batch,
        is_batch_suitable_interval_calculation, JoinStreamState,
    },
    symmetric_hash_join::{build_join_indices, OneSideHashJoiner},
    utils::{
        build_batch_from_indices, build_join_schema, calculate_join_output_ordering,
        check_join_is_valid, combine_join_equivalence_properties,
        combine_join_ordering_equivalence_properties,
        partitioned_join_output_partitioning, ColumnIndex, JoinFilter, JoinOn, JoinSide,
    },
    StreamJoinPartitionMode,
};
use crate::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, EquivalenceProperties, ExecutionPlan,
    Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::intervals::ExprIntervalGraph;
use datafusion_physical_expr::{OrderingEquivalenceProperties, PhysicalSortRequirement};

use ahash::RandomState;
use futures::{ready, Stream, StreamExt};
use parking_lot::Mutex;

/// Sliding hash join implementation for sliding window joins.
/// ```text
///              Build
///            +---------+
///            | a  | b  |        Probe
///            |---------|       +-------+
///            | 1  | a  |       | x | y |
///            |    |    |       |-------|
///            | 2  | b  |       | 3 | a |
///            |    |    |       |   |   |
///            | 3  | c  |       | 4 | v |
///            |    |    |       |   |   |
///            | 5  | c  |       | 5 | a |
///            |    |    |       |   |   |
///            | 6  | a  |       | 6 | x |
///            |    |    |       |   |   |
///            | 8  | a  |       +-------+
///            |    |    |
///            | 8  | d  |
///            |    |    |
///            | 10 | c  |
///            |    |    |
///            | 12 | y  |
///            |    |    |
///            +---------+
///
///  Query: b=y AND a > x - 3 AND a < x + 8
///
///  This query implies a sliding window since
///  a and x columns ordered in direction.
///
///  We use this information for partially materializing
///  and prune the build side.
///
///              Build
///            +---------+
///            | a  | b  |        Probe
///            |---------|       +-------+
///            | 1  | a  |       | x | y |
///            |    |    |      /|-------|
///            | 2  | b  |    /- | 3 | a |
///            |    |    |   /   |   |   |
///            | 3  | c  | /-    | 4 | v |
///            |    |    |-      |   |   |
///            | 5  | c  |       | 5 | a |
///            |    |    |       |   |   |
///            | 6  | a  |       | 6 | x |
///            |    |    |       |   |   |
///            | 8  | a  |      |+-------+
///            |    |    |      /
///            | 8  | d  |     |
///            |    |    |     /
///            | 10 | c  |    |
///            |    |    |    /
///            | 12 | y  |   |
///            |    |    |   /
///            +---------+  |
///                         / Sliding Window
///                        |
///                        |
///            -------------
///
///
///  The probe side requires the build side until
///  the a column has a < 6 + 8 => a < 14. Thus,
///  we are fetching the build side until we see a value
///  in column a greater than 14. This is how we guarantee the
///  probe side is met all possible rows from build side.
/// ```
///
#[derive(Debug)]
pub struct SlidingHashJoinExec {
    /// Left side stream
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// Right side stream
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    pub(crate) on: Vec<(Column, Column)>,
    /// Filters applied when finding matching rows
    pub(crate) filter: JoinFilter,
    /// How the join is performed
    pub(crate) join_type: JoinType,
    /// Expression graph and `SortedFilterExpr`s for interval calculations
    filter_state: Arc<Mutex<IntervalCalculatorInnerState>>,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Shares the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// If null_equals_null is true, null == null else null != null
    pub(crate) null_equals_null: bool,
    /// Left side sort expression(s)
    left_sort_exprs: Vec<PhysicalSortExpr>,
    /// Right side sort expression(s)
    right_sort_exprs: Vec<PhysicalSortExpr>,
    /// The output ordering
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    /// Partition mode
    pub(crate) mode: StreamJoinPartitionMode,
}

/// This object encapsulates metrics pertaining to a single input (i.e. side)
/// of the operator `SlidingHashJoinExec`.
#[derive(Debug)]
struct SlidingHashJoinSideMetrics {
    /// Number of batches consumed by this operator
    input_batches: metrics::Count,
    /// Number of rows consumed by this operator
    input_rows: metrics::Count,
}

/// Metrics for operator `SlidingHashJoinExec`.
#[derive(Debug)]
struct SlidingHashJoinMetrics {
    /// Number of left batches/rows consumed by this operator
    left: SlidingHashJoinSideMetrics,
    /// Number of right batches/rows consumed by this operator
    right: SlidingHashJoinSideMetrics,
    /// Memory used by sides in bytes
    pub(crate) stream_memory_usage: metrics::Gauge,
    /// Number of batches produced by this operator
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
}

impl SlidingHashJoinMetrics {
    /// Creates a new `SlidingHashJoinMetrics` object according to the
    /// given number of partitions and the metrics set.    
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let left = SlidingHashJoinSideMetrics {
            input_batches,
            input_rows,
        };

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let right = SlidingHashJoinSideMetrics {
            input_batches,
            input_rows,
        };

        let stream_memory_usage =
            MetricBuilder::new(metrics).gauge("stream_memory_usage", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            left,
            right,
            output_batches,
            stream_memory_usage,
            output_rows,
        }
    }
}

impl SlidingHashJoinExec {
    /// Try to create a new [`SlidingHashJoinExec`].
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: &JoinType,
        null_equals_null: bool,
        left_sort_exprs: Vec<PhysicalSortExpr>,
        right_sort_exprs: Vec<PhysicalSortExpr>,
        mode: StreamJoinPartitionMode,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        // Error out if no "on" constraints are given:
        if on.is_empty() {
            return Err(DataFusionError::Plan(
                "On constraints in SlidingHashJoinExec should be non-empty".to_string(),
            ));
        }

        // Check if the join is valid with the given on constraints:
        check_join_is_valid(&left_schema, &right_schema, &on)?;

        // Build the join schema from the left and right schemas:
        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        // Initialize the random state for the join operation:
        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let filter_state = Arc::new(Mutex::new(IntervalCalculatorInnerState::default()));

        let output_ordering = calculate_join_output_ordering(
            &left_sort_exprs,
            &right_sort_exprs,
            *join_type,
            &on,
            left_schema.fields.len(),
            &Self::maintains_input_order(*join_type),
            Some(JoinSide::Right),
        )?;

        Ok(SlidingHashJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            filter_state,
            schema: Arc::new(schema),
            random_state,
            metrics: ExecutionPlanMetricsSet::new(),
            column_indices,
            null_equals_null,
            output_ordering,
            left_sort_exprs,
            right_sort_exprs,
            mode,
        })
    }

    /// Calculate order preservation flags for this join.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        vec![
            false,
            matches!(
                join_type,
                JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightAnti
                    | JoinType::RightSemi
            ),
        ]
    }
}

impl DisplayAs for SlidingHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = format!(", filter={}", self.filter.expression());
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({}, {})", c1, c2))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "SlidingHashJoinExec: join_type={:?}, on=[{}]{}",
                    self.join_type, on, display_filter
                )
            }
        }
    }
}

impl ExecutionPlan for SlidingHashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        partitioned_join_output_partitioning(
            self.join_type,
            self.left.output_partitioning(),
            self.right.output_partitioning(),
            self.left.schema().fields.len(),
        )
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children.iter().any(|u| *u))
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_ordering.as_deref()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.mode {
            StreamJoinPartitionMode::Partitioned => {
                let (left_expr, right_expr) = self
                    .on
                    .iter()
                    .map(|(l, r)| (Arc::new(l.clone()) as _, Arc::new(r.clone()) as _))
                    .unzip();
                vec![
                    Distribution::HashPartitioned(left_expr),
                    Distribution::HashPartitioned(right_expr),
                ]
            }
            StreamJoinPartitionMode::SinglePartition => {
                vec![Distribution::SinglePartition, Distribution::SinglePartition]
            }
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![
            Some(PhysicalSortRequirement::from_sort_exprs(
                &self.left_sort_exprs,
            )),
            Some(PhysicalSortRequirement::from_sort_exprs(
                &self.right_sort_exprs,
            )),
        ]
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        let left_columns_len = self.left.schema().fields.len();
        combine_join_equivalence_properties(
            self.join_type,
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            left_columns_len,
            &self.on,
            self.schema(),
        )
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn ordering_equivalence_properties(&self) -> OrderingEquivalenceProperties {
        combine_join_ordering_equivalence_properties(
            &self.join_type,
            &self.left,
            &self.right,
            self.schema(),
            &self.maintains_input_order(),
            Some(JoinSide::Right),
            self.equivalence_properties(),
        )
        .unwrap()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(SlidingHashJoinExec::try_new(
                left.clone(),
                right.clone(),
                self.on.clone(),
                self.filter.clone(),
                &self.join_type,
                self.null_equals_null,
                self.left_sort_exprs.clone(),
                self.right_sort_exprs.clone(),
                self.mode,
            )?)),
            _ => Err(DataFusionError::Internal(
                "SlidingHashJoinExec wrong number of children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();
        if left_partitions != right_partitions {
            return Err(DataFusionError::Internal(format!(
                "Invalid SlidingHashJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec",
            )));
        }

        let (left_sorted_filter_expr, right_sorted_filter_expr, graph) = if let (
            Some(left_sorted_filter_expr),
            Some(right_sorted_filter_expr),
            Some(graph),
        ) =
            build_filter_expression_graph(
                &self.filter_state,
                &self.left,
                &self.right,
                &self.filter,
            )? {
            (left_sorted_filter_expr, right_sorted_filter_expr, graph)
        } else {
            return Err(DataFusionError::Internal("AsymmetricHashJoin can not operate without both sides are not prunning tables.".to_owned()));
        };

        let (on_left, on_right) = self.on.iter().cloned().unzip();

        let left_stream = self.left.execute(partition, context.clone())?;

        let right_stream = self.right.execute(partition, context.clone())?;

        let reservation = Arc::new(Mutex::new(
            MemoryConsumer::new(format!("SlidingHashJoinStream[{partition}]"))
                .register(context.memory_pool()),
        ));
        reservation.lock().try_grow(graph.size())?;

        Ok(Box::pin(SlidingHashJoinStream {
            left_stream,
            right_stream,
            probe_buffer: ProbeBuffer::new(self.right.schema(), on_right),
            build_buffer: OneSideHashJoiner::new(
                JoinSide::Left,
                on_left,
                self.left.schema(),
            ),
            schema: self.schema(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            random_state: self.random_state.clone(),
            column_indices: self.column_indices.clone(),
            metrics: SlidingHashJoinMetrics::new(partition, &self.metrics),
            graph,
            left_sorted_filter_expr,
            right_sorted_filter_expr,
            null_equals_null: self.null_equals_null,
            reservation,
            state: JoinStreamState::PullProbe,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        // TODO stats: it is not possible in general to know the output size of joins
        Statistics::default()
    }
}

/// We use this buffer to keep track of the probe side pulling.
struct ProbeBuffer {
    /// The batch used for join operations.
    current_batch: RecordBatch,
    /// The batches buffered in `ProbePull` state.
    candidate_buffer: Vec<RecordBatch>,
    /// Join keys/columns.
    on: Vec<Column>,
}

impl ProbeBuffer {
    /// Creates a new `ProbeBuffer` with the given schema and join keys.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the input batches.
    /// * `on` - A vector storing join columns.
    ///
    /// # Returns
    ///
    /// A new `BuildSideBuffer`.    
    pub fn new(schema: SchemaRef, on: Vec<Column>) -> Self {
        Self {
            current_batch: RecordBatch::new_empty(schema),
            candidate_buffer: vec![],
            on,
        }
    }

    /// Returns the size of this `ProbeBuffer` in bytes.
    ///
    /// # Returns
    ///
    /// The size of this `ProbeBuffer` in bytes.    
    pub fn size(&self) -> usize {
        let mut size = 0;
        size += self.current_batch.get_array_memory_size();
        size += std::mem::size_of_val(&self.on);
        size
    }
}

/// This method determines if the result of the join should be produced in the final step or not.
///
/// # Arguments
///
/// * `join_type` - Enum indicating the type of join to be performed.
///
/// # Returns
///
/// A boolean indicating whether the result of the join should be produced in
/// the final step or not. The result will be true if the join type is one of
/// `JoinType::Left`, `JoinType::LeftAnti`, `JoinType::Full` or `JoinType::LeftSemi`.
fn need_to_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::LeftAnti | JoinType::Full | JoinType::LeftSemi
    )
}

/// This function produces unmatched record results based on the build side,
/// join type and other parameters.
///
/// The method uses first `prune_length` rows from the build side input buffer
/// to produce results.
///
/// # Arguments
///
/// * `build_side_joiner`: A reference to the build-side buffer containing join information.
/// * `output_schema`: The schema for the output record batch.
/// * `prune_length`: The length used for pruning the join result.
/// * `probe_schema`: The schema for the probe side of the join.
/// * `join_type`: The type of join being performed.
/// * `column_indices`: A slice of column indices used in the join.
///
/// # Returns
///
/// An optional [`RecordBatch`] containing the joined results, or `None` if no
/// results are to be produced.
fn build_side_determined_results(
    build_hash_joiner: &OneSideHashJoiner,
    output_schema: &SchemaRef,
    prune_length: usize,
    probe_schema: SchemaRef,
    join_type: JoinType,
    column_indices: &[ColumnIndex],
) -> Result<Option<RecordBatch>> {
    if prune_length == 0 || !need_to_produce_result_in_final(join_type) {
        return Ok(None);
    }
    let (build_indices, probe_indices) = calculate_build_outer_indices_by_join_type(
        prune_length,
        &build_hash_joiner.visited_rows,
        build_hash_joiner.deleted_offset,
        join_type,
    )?;

    // Create an empty probe record batch:
    let empty_probe_batch = RecordBatch::new_empty(probe_schema);
    // Build the final result from the indices of build and probe sides:
    build_batch_from_indices(
        output_schema.as_ref(),
        &build_hash_joiner.input_buffer,
        &empty_probe_batch,
        &build_indices,
        &probe_indices,
        column_indices,
        JoinSide::Left,
    )
    .map(|batch| (batch.num_rows() > 0).then_some(batch))
}

/// This method performs a join between the build side input buffer and the probe side batch.
///
/// # Arguments
///
/// * `build_hash_joiner` - Build side hash joiner
/// * `probe_hash_joiner` - Probe side hash joiner
/// * `schema` - A reference to the schema of the output record batch.
/// * `join_type` - The type of join to be performed.
/// * `on_probe` - An array of columns on which the join will be performed. The columns are from the probe side of the join.
/// * `filter` - An optional filter on the join condition.
/// * `probe_batch` - The second record batch to be joined.
/// * `column_indices` - An array of columns to be selected for the result of the join.
/// * `random_state` - The random state for the join.
/// * `null_equals_null` - A boolean indicating whether NULL values should be treated as equal when joining.
///
/// # Returns
///
/// A `Result` object containing an `Option<RecordBatch>`. If the resulting batch
/// contains any rows, the result will be `Some(RecordBatch)`. If the probe batch
/// is empty, the function will return `Ok(None)`.
#[allow(clippy::too_many_arguments)]
fn join_with_probe_batch(
    build_hash_joiner: &mut OneSideHashJoiner,
    probe_hash_joiner: &ProbeBuffer,
    schema: &SchemaRef,
    join_type: JoinType,
    filter: &JoinFilter,
    probe_batch: &RecordBatch,
    column_indices: &[ColumnIndex],
    random_state: &RandomState,
    null_equals_null: bool,
) -> Result<Option<RecordBatch>> {
    // Checks if probe batch is empty, exit early if so:
    if probe_batch.num_rows() == 0 {
        return Ok(None);
    }

    // Calculates the indices to use for build and probe sides of the join:
    let (build_indices, probe_indices) = build_join_indices(
        probe_batch,
        &build_hash_joiner.hashmap,
        &build_hash_joiner.input_buffer,
        &build_hash_joiner.on,
        &probe_hash_joiner.on,
        Some(filter),
        random_state,
        null_equals_null,
        &mut build_hash_joiner.hashes_buffer,
        Some(build_hash_joiner.deleted_offset),
        JoinSide::Left,
    )?;

    // Record indices of the rows that were visited (if required by the join type):
    if need_to_produce_result_in_final(join_type) {
        record_visited_indices(
            &mut build_hash_joiner.visited_rows,
            build_hash_joiner.deleted_offset,
            &build_indices,
        );
    }

    // Adjust indices according to the type of join:
    let (build_indices, probe_indices) = adjust_probe_side_indices_by_join_type(
        build_indices,
        probe_indices,
        probe_batch.num_rows(),
        join_type,
    )?;

    // Build a new batch from build and probe indices, return the batch if it contains any rows:
    build_batch_from_indices(
        schema,
        &build_hash_joiner.input_buffer,
        probe_batch,
        &build_indices,
        &probe_indices,
        column_indices,
        JoinSide::Left,
    )
    .map(|batch| (batch.num_rows() > 0).then_some(batch))
}

/// A stream that issues [`RecordBatch`]es as they arrive from the left and
/// right sides of the join.
struct SlidingHashJoinStream {
    /// Left stream
    left_stream: SendableRecordBatchStream,
    /// Right stream
    right_stream: SendableRecordBatchStream,
    /// Left globally sorted filter expression.
    /// This expression is used to range calculations from the left stream.
    left_sorted_filter_expr: SortedFilterExpr,
    /// Right globally sorted filter expression.
    /// This expression is used to range calculations from the right stream.
    right_sorted_filter_expr: SortedFilterExpr,
    /// Hash joiner for the right side. It is responsible for creating a hash map
    /// from the right side data, which can be used to quickly look up matches when
    /// joining with left side data.
    build_buffer: OneSideHashJoiner,
    /// Buffer for the left side data. It keeps track of the current batch of data
    /// from the left stream that we're working with.
    probe_buffer: ProbeBuffer,
    /// Schema of the input data. This defines the structure of the data in both
    /// the left and right streams.
    schema: Arc<Schema>,
    /// The join filter expression. This is a boolean expression that determines
    /// whether a pair of rows, one from the left side and one from the right side,
    /// should be included in the output of the join.
    filter: JoinFilter,
    /// The type of the join operation. This can be one of: inner, left, right, full,
    /// semi, or anti join.
    join_type: JoinType,
    /// Information about the index and placement of columns. This is used when
    /// constructing the output record batch, to know where to get data for each column.
    column_indices: Vec<ColumnIndex>,
    /// Expression graph for range pruning. This graph describes the dependencies
    /// between different columns in terms of range bounds, which can be used for
    /// advanced optimizations, such as range calculations and pruning.
    graph: ExprIntervalGraph,
    /// Random state used for initializing the hash function in the hash joiner.
    random_state: RandomState,
    /// If true, null values are considered equal to other null values. If false,
    /// null values are considered distinct from everything, including other null values.
    null_equals_null: bool,
    /// Metrics for monitoring the performance of the join operation.
    metrics: SlidingHashJoinMetrics,
    /// Memory reservation for this join operation.
    reservation: SharedMemoryReservation,
    /// Current state of the stream. This state machine tracks what the stream is
    /// currently doing or should do next, e.g., pulling data from the probe side,
    /// pulling data from the build side, performing the join, etc.
    state: JoinStreamState,
}

impl RecordBatchStream for SlidingHashJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SlidingHashJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl SlidingHashJoinStream {
    /// Returns the total memory size of the stream. It's the sum of memory size of each field.
    fn size(&self) -> usize {
        let mut size = 0;
        size += mem::size_of_val(&self.left_stream);
        size += mem::size_of_val(&self.right_stream);
        size += mem::size_of_val(&self.schema);
        size += mem::size_of_val(&self.filter);
        size += mem::size_of_val(&self.join_type);
        size += self.build_buffer.size();
        size += self.probe_buffer.size();
        size += mem::size_of_val(&self.column_indices);
        size += self.graph.size();
        size += mem::size_of_val(&self.left_sorted_filter_expr);
        size += mem::size_of_val(&self.right_sorted_filter_expr);
        size += mem::size_of_val(&self.random_state);
        size += mem::size_of_val(&self.null_equals_null);
        size += mem::size_of_val(&self.metrics);
        size
    }

    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                // When the state is "PullProbe", poll the right (probe) stream:
                JoinStreamState::PullProbe => {
                    loop {
                        match ready!(self.right_stream.poll_next_unpin(cx)) {
                            Some(Ok(batch)) => {
                                // Update metrics for polled batch:
                                self.metrics.right.input_batches.add(1);
                                self.metrics.right.input_rows.add(batch.num_rows());

                                // Check if batch meets interval calculation criteria:
                                let stop_polling =
                                    is_batch_suitable_interval_calculation(
                                        &self.right_sorted_filter_expr,
                                        &batch,
                                    )?;
                                // Add the batch into candidate buffer:
                                self.probe_buffer.candidate_buffer.push(batch);
                                if stop_polling {
                                    break;
                                }
                            }
                            None => break,
                            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                    if self.probe_buffer.candidate_buffer.is_empty() {
                        // If no batches were collected, change state to "ProbeExhausted":
                        self.state = JoinStreamState::ProbeExhausted;
                        continue;
                    }
                    // Get probe batch by joining all the collected batches:
                    self.probe_buffer.current_batch = get_probe_batch(mem::take(
                        &mut self.probe_buffer.candidate_buffer,
                    ))?;

                    if self.probe_buffer.current_batch.num_rows() == 0 {
                        continue;
                    }
                    // Update the probe side with the new probe batch:
                    let calculated_build_side_interval =
                        calculate_the_necessary_build_side_range(
                            &self.build_buffer.input_buffer,
                            &mut self.graph,
                            &mut self.left_sorted_filter_expr,
                            &mut self.right_sorted_filter_expr,
                            &self.probe_buffer.current_batch,
                        )?;
                    // Update state to "PullBuild" with the calculated interval:
                    self.state = JoinStreamState::PullBuild {
                        interval: calculated_build_side_interval,
                    };
                }
                JoinStreamState::PullBuild { interval } => {
                    // Get the expression used to determine the order in which
                    // rows from the left stream are added to the batches:
                    let build_order = self.left_sorted_filter_expr.origin_sorted_expr();
                    let sort_options = &build_order.options;
                    let build_interval = interval.clone();
                    // Keep pulling data from the left stream until a suitable
                    // range on batches is found:
                    loop {
                        match ready!(self.left_stream.poll_next_unpin(cx)) {
                            Some(Ok(batch)) => {
                                if batch.num_rows() == 0 {
                                    continue;
                                }
                                self.metrics.left.input_batches.add(1);
                                self.metrics.left.input_rows.add(batch.num_rows());
                                let array_ref = build_order
                                    .expr
                                    .evaluate(&batch.slice(batch.num_rows() - 1, 1))?
                                    .into_array(batch.num_rows());

                                self.build_buffer
                                    .update_internal_state(&batch, &self.random_state)?;
                                self.build_buffer.offset += batch.num_rows();

                                let latest_value =
                                    ScalarValue::try_from_array(&array_ref, 0)?;
                                if check_if_sliding_window_condition_is_met(
                                    &latest_value,
                                    &build_interval,
                                    sort_options,
                                ) {
                                    self.state = JoinStreamState::Join;
                                    break;
                                }
                            }
                            // If the poll doesn't return any data, check if there
                            // are any batches. If so, combine them into one and
                            // update the build buffer's internal state:
                            None => {
                                self.state = JoinStreamState::BuildExhausted;
                                break;
                            }
                            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                }
                JoinStreamState::Join => {
                    // Create a tuple of references to various objects for convenience:
                    let (
                        probe_side_buffer,
                        build_hash_joiner,
                        build_side_sorted_filter_expr,
                        probe_side_sorted_filter_expr,
                    ) = (
                        &self.probe_buffer,
                        &mut self.build_buffer,
                        &mut self.left_sorted_filter_expr,
                        &mut self.right_sorted_filter_expr,
                    );

                    // Perform the join operation using probe side batch data.
                    // The result is a new batch that contains rows from the
                    // probe side that have matching rows in the build side.
                    let equal_result = join_with_probe_batch(
                        build_hash_joiner,
                        probe_side_buffer,
                        &self.schema,
                        self.join_type,
                        &self.filter,
                        &probe_side_buffer.current_batch,
                        &self.column_indices,
                        &self.random_state,
                        self.null_equals_null,
                    )?;

                    // Calculate the filter expression intervals for both sides of the join:
                    calculate_filter_expr_intervals(
                        &build_hash_joiner.input_buffer,
                        build_side_sorted_filter_expr,
                        &probe_side_buffer.current_batch,
                        probe_side_sorted_filter_expr,
                    )?;

                    // Determine how much of the internal state can be pruned by
                    // comparing the filter expression intervals:
                    let prune_length = build_hash_joiner
                        .calculate_prune_length_with_probe_batch(
                            build_side_sorted_filter_expr,
                            probe_side_sorted_filter_expr,
                            &mut self.graph,
                        )?;

                    // If some of the internal state can be pruned on build side,
                    // calculate the "anti" join result. The anti join result
                    // contains rows from the probe side that do not have matching
                    // rows in the build side.
                    let anti_result = build_side_determined_results(
                        build_hash_joiner,
                        &self.schema,
                        prune_length,
                        probe_side_buffer.current_batch.schema(),
                        self.join_type,
                        &self.column_indices,
                    )?;

                    // Prune the internal state of the build hash joiner:
                    build_hash_joiner.prune_internal_state(prune_length)?;

                    // Combine the "equal" join result and the "anti" join
                    // result into a single batch:
                    let result =
                        combine_two_batches(&self.schema, equal_result, anti_result)?;

                    // Update the state to "PullProbe", so the next iteration
                    // will pull from the probe side:
                    self.state = JoinStreamState::PullProbe;

                    // Calculate the current memory usage of the stream:
                    let capacity = self.size();
                    self.metrics.stream_memory_usage.set(capacity);

                    // Attempt to resize the memory reservation to match the
                    // current memory usage:
                    self.reservation.lock().try_resize(capacity)?;

                    // If a result batch was produced, update the metrics and
                    // return the batch:
                    if let Some(batch) = result {
                        self.metrics.output_batches.add(1);
                        self.metrics.output_rows.add(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
                // When state is "BuildExhausted", switch to "Join" state:
                JoinStreamState::BuildExhausted => {
                    self.state = JoinStreamState::Join;
                }
                JoinStreamState::ProbeExhausted => {
                    // Ready the left stream and match its states.
                    match ready!(self.left_stream.poll_next_unpin(cx)) {
                        // If the poll returns some batch of data:
                        Some(Ok(batch)) => {
                            // Update the metrics:
                            self.metrics.left.input_batches.add(1);
                            self.metrics.left.input_rows.add(batch.num_rows());
                            // Update the internal state of the build buffer
                            // with the data batch and random state:
                            self.build_buffer
                                .update_internal_state(&batch, &self.random_state)?;

                            let result = build_side_determined_results(
                                &self.build_buffer,
                                &self.schema,
                                self.build_buffer.input_buffer.num_rows(),
                                self.probe_buffer.current_batch.schema(),
                                self.join_type,
                                &self.column_indices,
                            )?;

                            self.build_buffer.prune_internal_state(
                                self.build_buffer.input_buffer.num_rows(),
                            )?;

                            if let Some(batch) = result {
                                // Update output metrics:
                                self.metrics.output_batches.add(1);
                                self.metrics.output_rows.add(batch.num_rows());
                                return Poll::Ready(Some(Ok(batch)));
                            }
                        }
                        // If the poll doesn't return any data, update the state
                        // to indicate both streams are exhausted:
                        None => {
                            self.state = JoinStreamState::BothExhausted {
                                final_result: false,
                            };
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    }
                }
                // When state is "BothExhausted" with final result as true, we
                // are done. Return `None`:
                JoinStreamState::BothExhausted { final_result: true } => {
                    return Poll::Ready(None)
                }
                // When state is "BothExhausted" with final result as false:
                JoinStreamState::BothExhausted {
                    final_result: false,
                } => {
                    // Create result `RecordBatch` from the build side since
                    // there will be no new probe batches coming:
                    let result = build_side_determined_results(
                        &self.build_buffer,
                        &self.schema,
                        self.build_buffer.input_buffer.num_rows(),
                        self.probe_buffer.current_batch.schema(),
                        self.join_type,
                        &self.column_indices,
                    )?;
                    // Update state to "BothExhausted" with final result as true:
                    self.state = JoinStreamState::BothExhausted { final_result: true };
                    if let Some(batch) = result {
                        // Update output metrics if we have a result:
                        self.metrics.output_batches.add(1);
                        self.metrics.output_rows.add(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    const TABLE_SIZE: i32 = 100;

    use std::sync::Arc;

    use super::*;
    use crate::execution::context::SessionConfig;
    use crate::physical_plan::joins::hash_join_utils::tests::complicated_filter;
    use crate::physical_plan::joins::test_utils::{
        build_sides_record_batches, compare_batches, create_memory_table,
        join_expr_tests_fixture_i32, partitioned_hash_join_with_filter,
    };
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::{common, expressions::Column, joins::utils::JoinSide};
    use crate::prelude::SessionContext;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SortOptions;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, col};

    use rstest::*;

    pub async fn partitioned_swhj_join_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: &JoinType,
        null_equals_null: bool,
        context: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let partition_count = 4;
        let (left_expr, right_expr) = on
            .iter()
            .map(|(l, r)| (Arc::new(l.clone()) as _, Arc::new(r.clone()) as _))
            .unzip();

        let left_sort_expr = left
            .output_ordering()
            .map(|order| order.to_vec())
            .ok_or(DataFusionError::Internal(
                "SlidingHashJoinExec needs left and right side ordered.".to_owned(),
            ))
            .unwrap();
        let right_sort_expr = right
            .output_ordering()
            .map(|order| order.to_vec())
            .ok_or(DataFusionError::Internal(
                "SlidingHashJoinExec needs left and right side ordered.".to_owned(),
            ))
            .unwrap();

        let join = Arc::new(SlidingHashJoinExec::try_new(
            Arc::new(RepartitionExec::try_new(
                left,
                Partitioning::Hash(left_expr, partition_count),
            )?),
            Arc::new(RepartitionExec::try_new(
                right,
                Partitioning::Hash(right_expr, partition_count),
            )?),
            on,
            filter,
            join_type,
            null_equals_null,
            left_sort_expr,
            right_sort_expr,
            StreamJoinPartitionMode::Partitioned,
        )?);

        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = join.execute(i, context.clone())?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }

        Ok(batches)
    }

    async fn experiment(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: JoinFilter,
        join_type: JoinType,
        on: JoinOn,
        task_ctx: Arc<TaskContext>,
    ) -> Result<()> {
        let first_batches = partitioned_swhj_join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter.clone(),
            &join_type,
            false,
            task_ctx.clone(),
        )
        .await?;
        let second_batches = partitioned_hash_join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            Some(filter.clone()),
            &join_type,
            false,
            task_ctx.clone(),
        )
        .await?;
        compare_batches(&first_batches, &second_batches);
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn join_all_one_ascending_numeric(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(
        (4, 5),
        (11, 21),
        (31, 71),
        (99, 12),
        )]
        cardinality: (i32, i32),
        #[values(0, 1, 2, 3, 4, 5, 6, 7)] case_expr: usize,
        #[values(13, 10)] batch_size: usize,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("la1", left_schema)?,
            options: SortOptions::default(),
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1", right_schema)?,
            options: SortOptions::default(),
        }];
        let (left, right) = create_memory_table(
            left_batch,
            right_batch,
            Some(left_sorted),
            Some(right_sorted),
            batch_size,
        )?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, filter, join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn join_all_one_ascending_numeric_2() -> Result<()> {
        let join_type = JoinType::Inner;
        let cardinality = (4, 5);
        let case_expr = 1;
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("la1", left_schema)?,
            options: SortOptions::default(),
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1", right_schema)?,
            options: SortOptions::default(),
        }];
        let (left, right) = create_memory_table(
            left_batch,
            right_batch,
            Some(left_sorted),
            Some(right_sorted),
            13,
        )?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, filter, join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_null_columns_last() -> Result<()> {
        let join_type = JoinType::Full;
        let cardinality = (10, 11);
        let case_expr = 1;
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("l_asc_null_last", left_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("r_asc_null_last", right_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];
        let (left, right) = create_memory_table(
            left_batch,
            right_batch,
            Some(left_sorted),
            Some(right_sorted),
            13,
        )?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 7,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 7,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, filter, join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_null_columns_first_descending() -> Result<()> {
        let join_type = JoinType::Full;
        let cardinality = (10, 11);
        let case_expr = 1;
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("l_desc_null_first", left_schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("r_desc_null_first", right_schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let (left, right) = create_memory_table(
            left_batch,
            right_batch,
            Some(left_sorted),
            Some(right_sorted),
            13,
        )?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 8,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 8,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, filter, join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn join_all_one_descending_numeric_particular(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(
        (4, 5),
        (11, 21),
        (31, 71),
        (99, 12),
        )]
        cardinality: (i32, i32),
        #[values(0, 1, 2, 3, 4, 5, 6)] case_expr: usize,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("la1_des", left_schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1_des", right_schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let (left, right) = create_memory_table(
            left_batch,
            right_batch,
            Some(left_sorted),
            Some(right_sorted),
            13,
        )?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture_i32(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 5,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 5,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, filter, join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn complex_join_all_one_ascending_numeric(
        #[values(
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::RightSemi,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::Full
        )]
        join_type: JoinType,
        #[values(
        (4, 5),
        (11, 21),
        (31, 71),
        (99, 12),
        )]
        cardinality: (i32, i32),
    ) -> Result<()> {
        // a + b > c + 10 AND a + b < c + 100
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: binary(
                col("la1", left_schema)?,
                Operator::Plus,
                col("la2", left_schema)?,
                left_schema,
            )?,
            options: SortOptions::default(),
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1", right_schema)?,
            options: SortOptions::default(),
        }];
        let (left, right) = create_memory_table(
            left_batch,
            right_batch,
            Some(left_sorted),
            Some(right_sorted),
            13,
        )?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&intermediate_schema)?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 4,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, filter, join_type, on, task_ctx).await?;
        Ok(())
    }
}
