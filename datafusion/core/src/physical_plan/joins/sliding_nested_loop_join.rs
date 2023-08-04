// Copyright (C) Synnada, Inc. - All Rights Reserved.
// This file does not contain any Apache Software Foundation copyrighted code.

//! Defines the sliding window nested loop join plan, which supports all [`JoinType`]s.
//! The sliding window nested loop join can execute in parallel by partitions depending
//! on the [`JoinType`].

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::Arc;
use std::task::Poll;

use crate::physical_plan::common::AbortOnDropMany;
use crate::physical_plan::joins::hash_join_utils::{
    calculate_filter_expr_intervals, record_visited_indices, SortedFilterExpr,
};
use crate::physical_plan::joins::nested_loop_join::distribution_from_join_type;
use crate::physical_plan::joins::sliding_window_join_utils::{
    adjust_probe_side_indices_by_join_type, calculate_build_outer_indices_by_join_type,
    calculate_the_necessary_build_side_range, check_if_sliding_window_condition_is_met,
    combine_two_batches, get_probe_batch, is_batch_suitable_interval_calculation,
    JoinStreamState,
};
use crate::physical_plan::joins::symmetric_hash_join::determine_prune_length;
use crate::physical_plan::joins::utils;
use crate::physical_plan::joins::utils::{
    apply_join_filter_to_indices, build_batch_from_indices, build_join_schema,
    calculate_sliding_join_output_order, combine_join_equivalence_properties,
    estimate_join_statistics, partitioned_join_output_partitioning, ColumnIndex,
    JoinFilter, JoinSide,
};
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use crate::physical_plan::stream::RecordBatchBroadcastStreamsBuilder;
use crate::physical_plan::{
    metrics, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::{UInt32Array, UInt32Builder, UInt64Array, UInt64Builder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, ScalarValue, Statistics};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::TaskContext;
use datafusion_expr::JoinType;
use datafusion_physical_expr::intervals::ExprIntervalGraph;
use datafusion_physical_expr::{
    EquivalenceProperties, PhysicalSortExpr, PhysicalSortRequirement,
};

use futures::{ready, Stream, StreamExt};
use hashbrown::HashSet;
use parking_lot::Mutex;

/// The operator `SlidingNestedLoopJoinExec` executes partitions in parallel
/// and in a streaming fashion. We apply interval arithmetic to prune the
/// build side, which makes the executor pipeline-friendly.
///
/// One input will be collected into a single partition, which is called the
/// build stream. The other input is treated as the probe side, and partitioning
/// properties of the output comes from this side.
///
/// Single partitions are broadcast into the partition count of the other side.
/// This makes it easier to create a shared source in the streaming context.
///
/// Left side will become build side, and the right side will be the probe
/// side. The order preserving characteristics are similar to sliding window
/// hash join algorithm.
///
/// The following table associates join types with distribution properties:
///
/// ```text
/// ┌--------------------------------┰------------------------------------------------------┐
/// | JoinType                       | Distribution (build, probe)                          |
/// |--------------------------------|------------------------------------------------------|
/// | Inner/Left/LeftSemi/LeftAnti   | (UnspecifiedDistribution, Broadcast SinglePartition) |
/// | Right/RightSemi/RightAnti/Full | (Broadcast SinglePartition, UnspecifiedDistribution) |
/// └--------------------------------┴------------------------------------------------------┘
/// ```
#[derive(Debug)]
pub struct SlidingNestedLoopJoinExec {
    /// Left side
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// Right side
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Filters that are applied while finding matching rows
    pub(crate) filter: JoinFilter,
    /// Join type specifying how the join is performed
    pub(crate) join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Information of index and left/right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// The left `SortExpr`
    left_sort_exprs: Vec<PhysicalSortExpr>,
    /// The right `SortExpr`
    right_sort_exprs: Vec<PhysicalSortExpr>,
    /// The output ordering
    output_ordering: Option<Vec<PhysicalSortExpr>>,
    /// Inner state that is initialized when the first output stream is created.
    /// This is kept in a `Arc<Mutex<T>>` since only one partition should be
    /// responsible for getting the broadcast streams.
    state: Arc<Mutex<SlidingNestedLoopJoinExecState>>,
}

impl Debug for SlidingNestedLoopJoinExecState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlidingNestedLoopJoinExecState")
    }
}

/// Inner state of the [`SlidingNestedLoopJoinExec`].
struct SlidingNestedLoopJoinExecState {
    /// Replica streams:
    channels: Vec<SendableRecordBatchStream>,
    /// Helper that ensures background job(s) are killed once no longer needed.
    abort_helper: Arc<AbortOnDropMany<()>>,
}

/// This object encapsulates metrics pertaining to a single input (i.e. side)
/// of the operator `SlidingNestedLoopJoinExec`.
#[derive(Debug)]
struct SlidingNestedLoopJoinSideMetrics {
    /// Number of batches consumed by this operator
    input_batches: metrics::Count,
    /// Number of rows consumed by this operator
    input_rows: metrics::Count,
}

/// Metrics for operator `SlidingNestedLoopJoinExec`.
#[derive(Debug)]
struct SlidingNestedLoopJoinMetrics {
    /// Number of build batches/rows consumed by this operator
    build: SlidingNestedLoopJoinSideMetrics,
    /// Number of probe batches/rows consumed by this operator
    probe: SlidingNestedLoopJoinSideMetrics,
    /// Memory used by sides in bytes
    pub(crate) stream_memory_usage: metrics::Gauge,
    /// Number of batches produced by this operator
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
}

impl SlidingNestedLoopJoinMetrics {
    // Creates a new `SlidingNestedLoopJoinMetrics` object according to the
    // given number of partitions and the metrics set.
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let build = SlidingNestedLoopJoinSideMetrics {
            input_batches,
            input_rows,
        };

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let probe = SlidingNestedLoopJoinSideMetrics {
            input_batches,
            input_rows,
        };

        let stream_memory_usage =
            MetricBuilder::new(metrics).gauge("stream_memory_usage", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            build,
            probe,
            output_batches,
            stream_memory_usage,
            output_rows,
        }
    }
}

impl SlidingNestedLoopJoinExec {
    /// Try to create a new [`SlidingNestedLoopJoinExec`].
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: JoinFilter,
        join_type: &JoinType,
        left_sort_exprs: Vec<PhysicalSortExpr>,
        right_sort_exprs: Vec<PhysicalSortExpr>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        let output_ordering = calculate_sliding_join_output_order(
            join_type,
            Some(&left_sort_exprs),
            Some(&right_sort_exprs),
            left_schema.fields.len(),
        )?;

        Ok(SlidingNestedLoopJoinExec {
            left,
            right,
            filter,
            join_type: *join_type,
            schema: Arc::new(schema),
            column_indices,
            metrics: ExecutionPlanMetricsSet::new(),
            output_ordering,
            left_sort_exprs,
            right_sort_exprs,
            state: Arc::new(Mutex::new(SlidingNestedLoopJoinExecState {
                channels: Vec::new(),
                abort_helper: Arc::new(AbortOnDropMany::<()>(vec![])),
            })),
        })
    }

    /// left (build) side
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right (probe) side
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Filters applied before join output
    pub fn filter(&self) -> &JoinFilter {
        &self.filter
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// In this section, we are employing the strategy of broadcasting
    /// single partition sides. This approach mirrors how we distribute
    /// `OnceFut<JoinLeftData>` in `NestedLoopJoinStream`(s). Each partition
    /// gets access to the same data with a single poll, and this data is
    /// shared among them. As there's no mechanism to pause until a side
    /// completes, we resort to broadcasting the data, thereby creating a
    /// shared resource.
    fn get_streams(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<(SendableRecordBatchStream, SendableRecordBatchStream)> {
        match self.required_input_distribution()[..] {
            [Distribution::UnspecifiedDistribution, Distribution::SinglePartition] => {
                let mut state = self.state.lock();
                let build_stream = self.left.execute(partition, context.clone())?;
                if state.channels.is_empty() {
                    let num_input_partitions =
                        self.left.output_partitioning().partition_count();
                    let builder = RecordBatchBroadcastStreamsBuilder::new(
                        self.right.schema(),
                        num_input_partitions,
                    );
                    (state.channels, state.abort_helper) =
                        builder.broadcast(self.right.clone(), 0, context)?;
                }
                Ok((build_stream, state.channels.swap_remove(0)))
            }
            [Distribution::SinglePartition, Distribution::UnspecifiedDistribution] => {
                let mut state = self.state.lock();
                let probe_stream = self.right.execute(partition, context.clone())?;
                if state.channels.is_empty() {
                    let num_input_partitions =
                        self.right.output_partitioning().partition_count();
                    let builder = RecordBatchBroadcastStreamsBuilder::new(
                        self.left.schema(),
                        num_input_partitions,
                    );
                    (state.channels, state.abort_helper) =
                        builder.broadcast(self.left.clone(), 0, context)?;
                }
                Ok((state.channels.swap_remove(0), probe_stream))
            }
            [Distribution::SinglePartition, Distribution::SinglePartition] => {
                let build_stream = self.left.execute(partition, context.clone())?;
                let probe_stream = self.right.execute(partition, context)?;
                Ok((build_stream, probe_stream))
            }
            _ => unreachable!(),
        }
    }
}

impl DisplayAs for SlidingNestedLoopJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = format!(", filter={}", self.filter.expression());
                write!(
                    f,
                    "SlidingNestedLoopJoinExec: join_type={:?}{}",
                    self.join_type, display_filter
                )
            }
        }
    }
}

impl ExecutionPlan for SlidingNestedLoopJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // the partition of output is determined by the rule of `required_input_distribution`
        if self.join_type == JoinType::Full {
            self.left.output_partitioning()
        } else {
            partitioned_join_output_partitioning(
                self.join_type,
                self.left.output_partitioning(),
                self.right.output_partitioning(),
                self.left.schema().fields.len(),
            )
        }
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children.iter().any(|c| *c))
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_ordering.as_deref()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        distribution_from_join_type(&self.join_type)
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![
            false,
            matches!(
                self.join_type,
                JoinType::Inner
                    | JoinType::RightAnti
                    | JoinType::RightSemi
                    | JoinType::Right
            ),
        ]
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        let left_columns_len = self.left.schema().fields.len();
        combine_join_equivalence_properties(
            self.join_type,
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            left_columns_len,
            &[], // empty join keys
            self.schema(),
        )
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(SlidingNestedLoopJoinExec::try_new(
                left.clone(),
                right.clone(),
                self.filter.clone(),
                &self.join_type,
                self.left_sort_exprs.clone(),
                self.right_sort_exprs.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "SlidingNestedLoopJoinExec should have two children".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = SlidingNestedLoopJoinMetrics::new(partition, &self.metrics);

        // Initialization of stream-level reservation
        let reservation =
            MemoryConsumer::new(format!("SlidingNestedLoopJoinStream[{partition}]"))
                .register(context.memory_pool());

        let (left_sorted_filter_expr, right_sorted_filter_expr, graph) =
            utils::prepare_sorted_exprs(
                &self.filter,
                &self.left,
                &self.right,
                &self.left_sort_exprs,
                &self.right_sort_exprs,
            )?;

        let (build_stream, probe_stream) = self.get_streams(partition, context)?;

        let build_buffer = BuildSideBuffer::new(self.left.schema());
        let probe_buffer = ProbeBuffer::new(self.right.schema());
        Ok(Box::pin(SlidingNestedLoopJoinStream {
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            probe_stream,
            probe_buffer,
            build_stream,
            build_buffer,
            column_indices: self.column_indices.clone(),
            metrics,
            reservation,
            state: JoinStreamState::PullProbe,
            left_sorted_filter_expr,
            right_sorted_filter_expr,
            graph,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        estimate_join_statistics(
            self.left.clone(),
            self.right.clone(),
            vec![],
            &self.join_type,
        )
    }
}

/// A buffer that stores build side data for a sliding nested loop join.
///
/// The buffer stores the input batches as well as a set of visited rows.
/// ```text
///             | Deleted Offset                              | Offset
///             |                                             |
///             |                                             |
/// +---------+ | +-----------------------------------------+ | +-----------+
/// |         | | |                                         | | |           |
/// | Pruned  | | |           Build Side Buffer             | | | New Batch |
/// |         | | |                                         | | |           |
/// +---------+ | +-----------------------------------------+ | +-----------+
///             |                                             |
///             |                                             |
///             |                                             |     <---
/// ```
struct BuildSideBuffer {
    /// Input record batch buffer
    input_buffer: RecordBatch,
    /// The set of visited rows
    visited_rows: HashSet<usize>,
    /// The offset of the new coming batch indexing
    offset: usize,
    /// Deleted offset of the build side, used for indexing the inner buffer
    deleted_offset: usize,
}

impl BuildSideBuffer {
    /// Creates a new `BuildSideBuffer` with the given schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the input batches.
    ///
    /// # Returns
    ///
    /// A new `BuildSideBuffer`.
    fn new(schema: SchemaRef) -> Self {
        Self {
            input_buffer: RecordBatch::new_empty(schema),
            visited_rows: HashSet::new(),
            offset: 0,
            deleted_offset: 0,
        }
    }

    /// Returns the size of this `BuildSideBuffer` in bytes.
    ///
    /// # Returns
    ///
    /// The size of this `BuildSideBuffer` in bytes.
    pub fn size(&self) -> usize {
        let mut size = 0;
        size += mem::size_of_val(self);
        size += self.input_buffer.get_array_memory_size();
        size += self.visited_rows.capacity() * mem::size_of::<usize>();
        size += mem::size_of_val(&self.offset);
        size += mem::size_of_val(&self.deleted_offset);
        size
    }

    /// Updates the internal state of this [`BuildSideBuffer`] with the incoming batch.
    ///
    /// # Arguments
    ///
    /// * `batch` - The incoming [`RecordBatch`] to merge with the internal input buffer.
    ///
    /// # Returns
    ///
    /// Returns a [`Result`] encapsulating any intermediate errors.
    fn update_internal_state(&mut self, batch: &RecordBatch) -> Result<()> {
        // Merge the incoming batch with the existing input buffer:
        self.input_buffer =
            arrow::compute::concat_batches(&batch.schema(), [&self.input_buffer, batch])?;
        self.offset += batch.num_rows();
        Ok(())
    }

    /// Prunes this [`BuildSideBuffer`] according to the given interval.
    ///
    /// # Arguments
    ///
    /// * `prune_length` - The number of rows to prune from the buffer.
    ///
    /// # Returns
    ///
    /// Returns a [`Result`] encapsulating any intermediate errors.
    pub(crate) fn prune_internal_state(&mut self, prune_length: usize) -> Result<()> {
        if prune_length > 0 {
            // Remove pruned rows from the visited rows set:
            for row in self.deleted_offset..(self.deleted_offset + prune_length) {
                self.visited_rows.remove(&row);
            }
            // Update the input buffer after pruning:
            self.input_buffer = self
                .input_buffer
                .slice(prune_length, self.input_buffer.num_rows() - prune_length);
            // Increment the deleted offset:
            self.deleted_offset += prune_length;
        }
        Ok(())
    }

    /// Calculates the pruning length for the [`BuildSideBuffer`] based on the given probe batch.
    ///
    /// # Arguments
    ///
    /// * `build_side_sorted_filter_expr` - The build side sorted filter expression.
    /// * `probe_side_sorted_filter_expr` - The probe side sorted filter expression.
    /// * `graph` - The physical expression graph.
    ///
    /// # Returns
    ///
    /// The pruning length for the `BuildSideBuffer`.
    fn calculate_prune_length_with_probe_batch(
        &mut self,
        build_side_sorted_filter_expr: &mut SortedFilterExpr,
        probe_side_sorted_filter_expr: &mut SortedFilterExpr,
        graph: &mut ExprIntervalGraph,
    ) -> Result<usize> {
        // Return early if the input buffer is empty:
        if self.input_buffer.num_rows() == 0 {
            return Ok(0);
        }
        // Process the build and probe side sorted filter expressions if both are present.
        // Collect the sorted filter expressions into a vector of (node_index, interval) tuples:
        let mut filter_intervals = vec![];
        for expr in [
            &build_side_sorted_filter_expr,
            &probe_side_sorted_filter_expr,
        ] {
            filter_intervals.push((expr.node_index(), expr.interval().clone()))
        }
        // Update the physical expression graph using the join filter intervals:
        graph.update_ranges(&mut filter_intervals)?;
        // Extract the new join filter interval for the build side:
        let (_, calculated_build_side_interval) = filter_intervals.remove(0);
        // If the intervals have not changed, return early without pruning:
        if calculated_build_side_interval.eq(build_side_sorted_filter_expr.interval()) {
            return Ok(0);
        }
        // Update the build side interval and determine the pruning length:
        build_side_sorted_filter_expr.set_interval(calculated_build_side_interval);

        determine_prune_length(&self.input_buffer, build_side_sorted_filter_expr)
    }
}

/// We use this buffer to keep track of probe side pulling.
struct ProbeBuffer {
    /// The batch used for join operations
    current_batch: RecordBatch,
    /// The batches buffered in ProbePull state.
    candidate_buffer: Vec<RecordBatch>,
}

impl ProbeBuffer {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            current_batch: RecordBatch::new_empty(schema),
            candidate_buffer: vec![],
        }
    }
    pub fn size(&self) -> usize {
        let mut size = 0;
        size += self.current_batch.get_array_memory_size();
        size
    }
}

/// A stream that issues [`RecordBatch`]es as they arrive from the right of the join.
struct SlidingNestedLoopJoinStream {
    schema: Arc<Schema>,
    filter: JoinFilter,
    join_type: JoinType,
    probe_stream: SendableRecordBatchStream,
    probe_buffer: ProbeBuffer,
    build_buffer: BuildSideBuffer,
    build_stream: SendableRecordBatchStream,
    /// Left globally sorted filter expression.
    /// This expression is used to range calculations from the left stream.
    left_sorted_filter_expr: SortedFilterExpr,
    /// Right globally sorted filter expression.
    /// This expression is used to range calculations from the right stream.
    right_sorted_filter_expr: SortedFilterExpr,
    graph: ExprIntervalGraph,
    column_indices: Vec<ColumnIndex>,
    metrics: SlidingNestedLoopJoinMetrics,
    reservation: MemoryReservation,
    state: JoinStreamState,
}

/// Builds the join indices for a nested loop join operation between a probe
/// side batch and a build side batch.
///
/// This function creates two arrays, representing the indices of the probe
/// (left) and the build (right) sides of the join. The probe indices array
/// is filled with repeated values corresponding to the probe row index, while
/// the build indices array is filled with a sequence of integers; i.e.
///
/// probe indices: `[left_index, left_index, ...., left_index]`
/// build indices: `[0, 1, 2, 3, 4,....,right_row_count]`
///
/// If a filter is provided, it is applied to the indices using the
/// `apply_join_filter_to_indices` function.
///
/// # Arguments
/// * `probe_batch`: The record batch for the probe side (left) of the join.
/// * `build_batch`: The record batch for the build side (right) of the join.
/// * `filter`: An optional filter to apply to the join indices.
///
/// # Returns
/// A tuple containing two arrays: the build indices ([`UInt64Array`]) and the
/// probe indices ([`UInt32Array`]).
fn build_join_indices(
    probe_batch: &RecordBatch,
    build_batch: &RecordBatch,
    filter: Option<&JoinFilter>,
) -> Result<(UInt64Array, UInt32Array)> {
    // Calculate the row counts for the probe and build sides.
    let probe_row_count = probe_batch.num_rows();
    let build_row_count = build_batch.num_rows();

    // Determine the capacity for building index arrays based on the row counts.
    let capacity = probe_row_count * build_row_count;

    // Initialize the build indices builder with the capacity.
    let mut build_indices_builder = UInt64Builder::with_capacity(capacity);

    // Extend the build indices builder with a sequence for each probe row.
    for _ in 0..probe_row_count {
        build_indices_builder.extend((0..(build_row_count as u64)).map(Some))
    }

    // Initialize the probe indices builder with the capacity.
    let mut probe_indices_builder = UInt32Builder::with_capacity(capacity);

    // Extend the probe indices builder with repeated values for each probe row.
    for probe_index in 0..probe_row_count {
        probe_indices_builder.extend(vec![Some(probe_index as u32); build_row_count])
    }

    // Finalize the index arrays.
    let build_indices = build_indices_builder.finish();
    let probe_indices = probe_indices_builder.finish();

    // Apply the join filter to the indices if provided, otherwise return the
    // indices directly.
    if let Some(filter) = filter {
        apply_join_filter_to_indices(
            build_batch,
            probe_batch,
            build_indices,
            probe_indices,
            filter,
            JoinSide::Left,
        )
    } else {
        Ok((build_indices, probe_indices))
    }
}

fn need_to_produce_outer_results_on_build_side(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::LeftAnti | JoinType::Full | JoinType::LeftSemi
    )
}

/// Constructs a record batch from the join results on the build side of a hash
/// join operation.
///
/// This function determines whether the build side needs to produce a result
/// in the final output based on the join type and prune length. If the join
/// type requires the build side to produce outer results, the decision regarding
/// unmatched rows can only be made through pruning. Pruning involves discarding
/// these rows, indicating that they are no longer needed; or in other words, they
/// will never be joined with a row from the probe side.
///
///
/// # Arguments
/// * `build_side_joiner`: A reference to the build-side buffer containing join information.
/// * `output_schema`: The schema for the output record batch.
/// * `prune_length`: The length used for pruning the join result.
/// * `probe_schema`: The schema for the probe side of the join.
/// * `join_type`: The type of join being performed.
/// * `column_indices`: A slice of column indices used in the join.
///
/// # Returns
/// An optional [`RecordBatch`] containing the joined results, or `None` if no
/// results are to be produced.
fn build_side_determined_results(
    build_side_joiner: &BuildSideBuffer,
    output_schema: &SchemaRef,
    prune_length: usize,
    probe_schema: SchemaRef,
    join_type: JoinType,
    column_indices: &[ColumnIndex],
) -> Result<Option<RecordBatch>> {
    // Check if we need to produce a result in the final output based on the
    // prune length and join type.
    if prune_length == 0 || !need_to_produce_outer_results_on_build_side(join_type) {
        Ok(None)
    } else {
        // Calculate the indices for the build and probe sides based on join type
        // and build side data.
        let (build_indices, probe_indices) = calculate_build_outer_indices_by_join_type(
            prune_length,
            &build_side_joiner.visited_rows,
            build_side_joiner.deleted_offset,
            join_type,
        )?;

        // Create an empty probe record batch, since the probe indices may include
        // only `None`s.
        let empty_probe_batch = RecordBatch::new_empty(probe_schema);

        // Build the final result from the indices of the build and probe sides,
        // constructing a record batch.
        build_batch_from_indices(
            output_schema.as_ref(),
            &build_side_joiner.input_buffer,
            &empty_probe_batch,
            &build_indices,
            &probe_indices,
            column_indices,
            JoinSide::Left,
        )
        .map(|batch| (batch.num_rows() > 0).then_some(batch))
    }
}

/// Joins the build side with a probe side batch based on the specified join
/// type and the given filter. This function returns the resulting record batch
/// if it contains any rows, or `None` otherwise.
///
/// # Arguments
///
/// * `build_side_joiner` - A mutable reference to the build side buffer used in the join operation.
/// * `schema` - A reference to the schema used for the record batches.
/// * `join_type` - The type of join to perform (e.g., Inner, Left, Right, or Outer).
/// * `filter` - A reference to the join filter used to filter rows during the join.
/// * `probe_batch` - A reference to the probe batch that will be joined with the build side.
/// * `column_indices` - An array containing the indices of columns to join.
///
/// # Returns
///
/// A `Result` object containing an `Option<RecordBatch>`. If the resulting batch
/// contains any rows, the result will be `Some(RecordBatch)`. If the probe batch
/// is empty, the function will return `Ok(None)`.
#[allow(clippy::too_many_arguments)]
fn join_with_probe_batch(
    build_side_joiner: &mut BuildSideBuffer,
    schema: &SchemaRef,
    join_type: JoinType,
    filter: &JoinFilter,
    probe_batch: &RecordBatch,
    column_indices: &[ColumnIndex],
) -> Result<Option<RecordBatch>> {
    // Checks if probe batch is empty, exit early if so:
    if probe_batch.num_rows() == 0 {
        return Ok(None);
    }

    let (build_indices, probe_indices) =
        build_join_indices(probe_batch, &build_side_joiner.input_buffer, Some(filter))?;

    // Record indices of the rows that were visited (if required by the join type):
    if need_to_produce_outer_results_on_build_side(join_type) {
        record_visited_indices(
            &mut build_side_joiner.visited_rows,
            build_side_joiner.deleted_offset,
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
        &build_side_joiner.input_buffer,
        probe_batch,
        &build_indices,
        &probe_indices,
        column_indices,
        JoinSide::Left,
    )
    .map(|batch| (batch.num_rows() > 0).then_some(batch))
}

impl SlidingNestedLoopJoinStream {
    fn size(&self) -> usize {
        let mut size = 0;
        size += mem::size_of_val(&self.build_stream);
        size += mem::size_of_val(&self.probe_stream);
        size += mem::size_of_val(&self.schema);
        size += mem::size_of_val(&self.filter);
        size += mem::size_of_val(&self.join_type);
        size += self.build_buffer.size();
        size += self.probe_buffer.size();
        size += mem::size_of_val(&self.column_indices);
        size += self.graph.size();
        size += mem::size_of_val(&self.left_sorted_filter_expr);
        size += mem::size_of_val(&self.right_sorted_filter_expr);
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
                        match ready!(self.probe_stream.poll_next_unpin(cx)) {
                            Some(Ok(batch)) => {
                                // Update metrics for polled batch:
                                self.metrics.probe.input_batches.add(1);
                                self.metrics.probe.input_rows.add(batch.num_rows());
                                // Check if the batch suits interval calculation criteria:
                                let continue_pooling =
                                    !is_batch_suitable_interval_calculation(
                                        &self.right_sorted_filter_expr,
                                        &batch,
                                    )?;
                                self.probe_buffer.candidate_buffer.push(batch);
                                if !continue_pooling {
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
                    self.probe_buffer.current_batch = get_probe_batch(std::mem::take(
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
                    // Update state to "PullBuild" with calculated interval:
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
                        match ready!(self.build_stream.poll_next_unpin(cx)) {
                            Some(Ok(batch)) => {
                                if batch.num_rows() == 0 {
                                    continue;
                                }
                                self.metrics.build.input_batches.add(1);
                                self.metrics.build.input_rows.add(batch.num_rows());
                                let array_ref = build_order
                                    .expr
                                    .evaluate(&batch)?
                                    .into_array(batch.num_rows());

                                self.build_buffer.update_internal_state(&batch)?;

                                let latest_value = ScalarValue::try_from_array(
                                    &array_ref,
                                    batch.num_rows() - 1,
                                )?;
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
                        build_side_joiner,
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
                        build_side_joiner,
                        &self.schema,
                        self.join_type,
                        &self.filter,
                        &probe_side_buffer.current_batch,
                        &self.column_indices,
                    )?;

                    // Calculate the filter expression intervals for both sides of the join:
                    calculate_filter_expr_intervals(
                        &build_side_joiner.input_buffer,
                        build_side_sorted_filter_expr,
                        &probe_side_buffer.current_batch,
                        probe_side_sorted_filter_expr,
                    )?;

                    // Determine how much of the internal state can be pruned by
                    // comparing the filter expression intervals:
                    let prune_length = build_side_joiner
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
                        build_side_joiner,
                        &self.schema,
                        prune_length,
                        probe_side_buffer.current_batch.schema(),
                        self.join_type,
                        &self.column_indices,
                    )?;

                    // Prune the internal state of the build side joiner:
                    build_side_joiner.prune_internal_state(prune_length)?;

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
                    // current memory usage.:
                    self.reservation.try_resize(capacity)?;

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
                    match ready!(self.build_stream.poll_next_unpin(cx)) {
                        // If the poll returns some batch of data:
                        Some(Ok(batch)) => {
                            // Update the metrics:
                            self.metrics.build.input_batches.add(1);
                            self.metrics.build.input_rows.add(batch.num_rows());
                            // Update the internal state of the build buffer
                            // with the data batch and random state:
                            self.build_buffer.update_internal_state(&batch)?;

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
                // are done. Return "None":
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

impl Stream for SlidingNestedLoopJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for SlidingNestedLoopJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::physical_plan::joins::hash_join_utils::tests::complicated_filter;
    use crate::physical_plan::joins::test_utils::{
        build_sides_record_batches, compare_batches, create_memory_table,
        join_expr_tests_fixture_i32, partitioned_nested_join_with_filter,
        partitioned_sliding_nested_join_with_filter,
    };
    use crate::physical_plan::joins::utils::JoinSide;
    use crate::prelude::SessionContext;

    use arrow::datatypes::{DataType, Field};
    use arrow_schema::SortOptions;
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, col};

    use rstest::*;

    async fn experiment(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: JoinFilter,
        join_type: JoinType,
        task_ctx: Arc<TaskContext>,
    ) -> Result<()> {
        let first_batches = partitioned_sliding_nested_join_with_filter(
            left.clone(),
            right.clone(),
            &join_type,
            filter.clone(),
            task_ctx.clone(),
        )
        .await?;
        let (_, second_batches) = partitioned_nested_join_with_filter(
            left.clone(),
            right.clone(),
            &join_type,
            Some(filter),
            task_ctx.clone(),
        )
        .await?;
        compare_batches(&first_batches, &second_batches);
        Ok(())
    }

    const TABLE_SIZE: i32 = 100;

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
        (31, 71),
        )]
        cardinality: (i32, i32),
        #[values(0, 1, 2, 3, 4, 5, 6, 7)] case_expr: usize,
        #[values(13, 15, 33, 100, 101, 123)] batch_size: usize,
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

        experiment(left, right, filter, join_type, task_ctx).await?;
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

        experiment(left, right, filter, join_type, task_ctx).await?;
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

        experiment(left, right, filter, join_type, task_ctx).await?;
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
        (31, 71),
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

        experiment(left, right, filter, join_type, task_ctx).await?;
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
        (31, 71),
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

        experiment(left, right, filter, join_type, task_ctx).await?;
        Ok(())
    }
}
