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

//! This file implements the symmetric hash join algorithm with range-based
//! data pruning to join two (potentially infinite) streams.
//!
//! A [SymmetricHashJoinExec] plan takes two children plan (with appropriate
//! output ordering) and produces the join output according to the given join
//! type and other options.
//!
//! This plan uses the [OneSideHashJoiner] object to facilitate join calculations
//! for both its children.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::task::Poll;
use std::vec;
use std::{any::Any, usize};

use ahash::RandomState;
use arrow::array::{
    ArrowPrimitiveType, BooleanBufferBuilder, NativeAdapter, PrimitiveArray,
    PrimitiveBuilder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{ArrowNativeType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use hashbrown::{raw::RawTable, HashSet};

use datafusion_common::{utils::bisect, ScalarValue};
use datafusion_physical_expr::intervals::{ExprIntervalGraph, Interval};

use crate::error::{DataFusionError, Result};
use crate::execution::context::TaskContext;
use crate::logical_expr::JoinType;
use crate::physical_plan::{
    expressions::Column,
    expressions::PhysicalSortExpr,
    joins::{
        hash_join::{build_join_indices, update_hash, JoinHashMap},
        hash_join_utils::{build_filter_input_order, SortedFilterExpr},
        utils::{
            build_batch_from_indices, build_join_schema, check_join_is_valid,
            combine_join_equivalence_properties, partitioned_join_output_partitioning,
            ColumnIndex, JoinFilter, JoinOn, JoinSide,
        },
    },
    metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
    DisplayFormatType, Distribution, EquivalenceProperties, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};

/// A symmetric hash join with range conditions is when both streams are hashed on the
/// join key and the resulting hash tables are used to join the streams.
/// The join is considered symmetric because the hash table is built on the join keys from both
/// streams, and the matching of rows is based on the values of the join keys in both streams.
/// This type of join is efficient in streaming context as it allows for fast lookups in the hash
/// table, rather than having to scan through one or both of the streams to find matching rows, also it
/// only considers the elements from the stream that fall within a certain sliding window (w/ range conditions),
/// making it more efficient and less likely to store stale data. This enables operating on unbounded streaming
/// data without any memory issues.
///
/// For each input stream, create a hash table.
///   - For each new [RecordBatch] in build side, hash and insert into inputs hash table. Update offsets.
///   - Test if input is equal to a predefined set of other inputs.
///   - If so record the visited rows. If the matched row results must be produced (INNER, LEFT), output the [RecordBatch].
///   - Try to prune other side (probe) with new [RecordBatch].
///   - If the join type indicates that the unmatched rows results must be produced (LEFT, FULL etc.),
/// output the [RecordBatch] when a pruning happens or at the end of the data.
///
///
/// ``` text
///                        +-------------------------+
///                        |                         |
///   left stream ---------|  Left OneSideHashJoiner |---+
///                        |                         |   |
///                        +-------------------------+   |
///                                                      |
///                                                      |--------- Joined output
///                                                      |
///                        +-------------------------+   |
///                        |                         |   |
///  right stream ---------| Right OneSideHashJoiner |---+
///                        |                         |
///                        +-------------------------+
///
/// Prune build side when the new RecordBatch comes to the probe side. We utilize interval arithmetic
/// on JoinFilter's sorted PhysicalExprs to calculate the joinable range.
///
///
///               PROBE SIDE          BUILD SIDE
///                 BUFFER              BUFFER
///             +-------------+     +------------+
///             |             |     |            |    Unjoinable
///             |             |     |            |    Range
///             |             |     |            |
///             |             |  |---------------------------------
///             |             |  |  |            |
///             |             |  |  |            |
///             |             | /   |            |
///             |             | |   |            |
///             |             | |   |            |
///             |             | |   |            |
///             |             | |   |            |
///             |             | |   |            |    Joinable
///             |             |/    |            |    Range
///             |             ||    |            |
///             |+-----------+||    |            |
///             || Record    ||     |            |
///             || Batch     ||     |            |
///             |+-----------+||    |            |
///             +-------------+\    +------------+
///                             |
///                             \
///                              |---------------------------------
///
///  This happens when range conditions are provided on sorted columns. E.g.
///
///        SELECT * FROM left_table, right_table
///        ON
///          left_key = right_key AND
///          left_time > right_time - INTERVAL 12 MINUTES AND left_time < right_time + INTERVAL 2 HOUR
///
/// or
///       SELECT * FROM left_table, right_table
///        ON
///          left_key = right_key AND
///          left_sorted > right_sorted - 3 AND left_sorted < right_sorted + 10
///
/// For general purpose, in the second scenario, when the new data comes to probe side, the conditions can be used to
/// determine a specific threshold for discarding rows from the inner buffer. For example, if the sort order the
/// two columns ("left_sorted" and "right_sorted") are ascending (it can be different in another scenarios)
/// and the join condition is "left_sorted > right_sorted - 3" and the latest value on the right input is 1234, meaning
/// that the left side buffer must only keep rows where "leftTime > rightTime - 3 > 1234 - 3 > 1231" ,
/// making the smallest value in 'left_sorted' 1231 and any rows below (since ascending)
/// than that can be dropped from the inner buffer.
/// ```
pub struct SymmetricHashJoinExec {
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
    /// Order information of filter expressions
    sorted_filter_exprs: Vec<SortedFilterExpr>,
    /// Left required sort
    left_required_sort_exprs: Vec<PhysicalSortExpr>,
    /// Right required sort
    right_required_sort_exprs: Vec<PhysicalSortExpr>,
    /// Expression graph for interval calculations
    physical_expr_graph: ExprIntervalGraph,
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
}

#[derive(Debug)]
struct SymmetricHashJoinSideMetrics {
    /// Number of batches consumed by this operator
    input_batches: metrics::Count,
    /// Number of rows consumed by this operator
    input_rows: metrics::Count,
}

/// Metrics for HashJoinExec
#[derive(Debug)]
struct SymmetricHashJoinMetrics {
    /// Number of left batches/rows consumed by this operator
    left: SymmetricHashJoinSideMetrics,
    /// Number of right batches/rows consumed by this operator
    right: SymmetricHashJoinSideMetrics,
    /// Number of batches produced by this operator
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
}

impl SymmetricHashJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let left = SymmetricHashJoinSideMetrics {
            input_batches,
            input_rows,
        };

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let right = SymmetricHashJoinSideMetrics {
            input_batches,
            input_rows,
        };

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            left,
            right,
            output_batches,
            output_rows,
        }
    }
}

impl SymmetricHashJoinExec {
    /// Tries to create a new [SymmetricHashJoinExec].
    /// # Error
    /// This function errors when:
    /// - It is not possible to join the left and right sides on keys `on`, or
    /// - It fails to construct [SortedFilterExpr]s, or
    /// - It fails to create the [ExprIntervalGraph].
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: &JoinType,
        null_equals_null: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        // Error out if no "on" contraints are given:
        if on.is_empty() {
            return Err(DataFusionError::Plan(
                "On constraints in SymmetricHashJoinExec should be non-empty".to_string(),
            ));
        }

        // Check if the join is valid with the given on constraints:
        check_join_is_valid(&left_schema, &right_schema, &on)?;

        // Build the join schema from the left and right schemas:
        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        // Set a random state for the join:
        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        // Create an expression DAG for the join filter:
        let mut physical_expr_graph =
            ExprIntervalGraph::try_new(filter.expression().clone())?;

        // Interval calculations require each column to exhibit monotonicity
        // independently. However, a `PhysicalSortExpr` object defines a
        // lexicographical ordering, so we can only use their first elements.
        // when deducing column monotonicities.
        // TODO: Extend the `PhysicalSortExpr` mechanism to express independent
        //       (i.e. simultaneous) ordering properties of columns.
        let (left_ordering, right_ordering) = match (
            left.output_ordering(),
            right.output_ordering(),
        ) {
            (Some([left_ordering, ..]), Some([right_ordering, ..])) => {
                (left_ordering, right_ordering)
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "Symmetric hash join requires its children to have an output ordering".to_string(),
                ));
            }
        };

        // Build the sorted filter expression for the left child:
        let left_filter_expression = build_filter_input_order(
            JoinSide::Left,
            &filter,
            &left.schema(),
            left_ordering,
        )?;

        // Build the sorted filter expression for the right child:
        let right_filter_expression = build_filter_input_order(
            JoinSide::Right,
            &filter,
            &right.schema(),
            right_ordering,
        )?;

        // Store the left and right sorted filter expressions in a vector
        let mut sorted_filter_exprs =
            vec![left_filter_expression, right_filter_expression];

        // Gather node indices of converted filter expressions in `SortedFilterExpr`
        // using the filter columns vector:
        let child_node_indexes = physical_expr_graph.gather_node_indices(
            &sorted_filter_exprs
                .iter()
                .map(|sorted_expr| sorted_expr.filter_expr().clone())
                .collect::<Vec<_>>(),
        );

        // Inject calculated node indices into SortedFilterExpr:
        for (sorted_expr, (_, index)) in sorted_filter_exprs
            .iter_mut()
            .zip(child_node_indexes.iter())
        {
            sorted_expr.set_node_index(*index);
        }

        let left_required_sort_exprs = vec![left_ordering.clone()];
        let right_required_sort_exprs = vec![right_ordering.clone()];

        Ok(SymmetricHashJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            sorted_filter_exprs,
            left_required_sort_exprs,
            right_required_sort_exprs,
            physical_expr_graph,
            schema: Arc::new(schema),
            random_state,
            metrics: ExecutionPlanMetricsSet::new(),
            column_indices,
            null_equals_null,
        })
    }

    /// left stream
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right stream
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Set of common columns used to join on
    pub fn on(&self) -> &[(Column, Column)] {
        &self.on
    }

    /// Filters applied before join output
    pub fn filter(&self) -> &JoinFilter {
        &self.filter
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// Get null_equals_null
    pub fn null_equals_null(&self) -> bool {
        self.null_equals_null
    }
}

impl Debug for SymmetricHashJoinExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for SymmetricHashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn required_input_ordering(&self) -> Vec<Option<&[PhysicalSortExpr]>> {
        vec![
            Some(&self.left_required_sort_exprs),
            Some(&self.right_required_sort_exprs),
        ]
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children.iter().any(|u| *u))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let (left_expr, right_expr) = self
            .on
            .iter()
            .map(|(l, r)| (Arc::new(l.clone()) as _, Arc::new(r.clone()) as _))
            .unzip();
        // TODO: This will change when we extend collected executions.
        vec![
            if self.left.output_partitioning().partition_count() == 1 {
                Distribution::SinglePartition
            } else {
                Distribution::HashPartitioned(left_expr)
            },
            if self.right.output_partitioning().partition_count() == 1 {
                Distribution::SinglePartition
            } else {
                Distribution::HashPartitioned(right_expr)
            },
        ]
    }

    fn output_partitioning(&self) -> Partitioning {
        let left_columns_len = self.left.schema().fields.len();
        partitioned_join_output_partitioning(
            self.join_type,
            self.left.output_partitioning(),
            self.right.output_partitioning(),
            left_columns_len,
        )
    }

    // TODO: Output ordering might be kept for some cases.
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        let left_columns_len = self.left.schema().fields.len();
        combine_join_equivalence_properties(
            self.join_type,
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            left_columns_len,
            self.on(),
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
        Ok(Arc::new(SymmetricHashJoinExec::try_new(
            children[0].clone(),
            children[1].clone(),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.null_equals_null,
        )?))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let display_filter = format!(", filter={:?}", self.filter.expression());
                write!(
                    f,
                    "SymmetricHashJoinExec: join_type={:?}, on={:?}{}",
                    self.join_type, self.on, display_filter
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        // TODO stats: it is not possible in general to know the output size of joins
        Statistics::default()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_left = self.on.iter().map(|on| on.0.clone()).collect::<Vec<_>>();
        let on_right = self.on.iter().map(|on| on.1.clone()).collect::<Vec<_>>();
        let left_side_joiner = OneSideHashJoiner::new(
            JoinSide::Left,
            self.sorted_filter_exprs[0].clone(),
            on_left,
            self.left.schema(),
        );
        let right_side_joiner = OneSideHashJoiner::new(
            JoinSide::Right,
            self.sorted_filter_exprs[1].clone(),
            on_right,
            self.right.schema(),
        );
        let left_stream = self.left.execute(partition, context.clone())?;
        let right_stream = self.right.execute(partition, context)?;

        Ok(Box::pin(SymmetricHashJoinStream {
            left_stream,
            right_stream,
            schema: self.schema(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            random_state: self.random_state.clone(),
            left: left_side_joiner,
            right: right_side_joiner,
            column_indices: self.column_indices.clone(),
            metrics: SymmetricHashJoinMetrics::new(partition, &self.metrics),
            physical_expr_graph: self.physical_expr_graph.clone(),
            null_equals_null: self.null_equals_null,
            final_result: false,
            probe_side: JoinSide::Left,
        }))
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct SymmetricHashJoinStream {
    /// Left stream
    left_stream: SendableRecordBatchStream,
    /// right stream
    right_stream: SendableRecordBatchStream,
    /// Input schema
    schema: Arc<Schema>,
    /// join filter
    filter: JoinFilter,
    /// type of the join
    join_type: JoinType,
    // left hash joiner
    left: OneSideHashJoiner,
    /// right hash joiner
    right: OneSideHashJoiner,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    // Range pruner.
    physical_expr_graph: ExprIntervalGraph,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
    /// Metrics
    metrics: SymmetricHashJoinMetrics,
    /// Flag indicating whether there is nothing to process anymore
    final_result: bool,
    /// The current probe side. We choose build and probe side according to this attribute.
    probe_side: JoinSide,
}

impl RecordBatchStream for SymmetricHashJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SymmetricHashJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

fn prune_hash_values(
    prune_length: usize,
    hashmap: &mut JoinHashMap,
    row_hash_values: &mut VecDeque<u64>,
    offset: u64,
) -> Result<()> {
    // Create a (hash)-(row number set) map
    let mut hash_value_map: HashMap<u64, HashSet<u64>> = HashMap::new();
    for index in 0..prune_length {
        let hash_value = row_hash_values.pop_front().unwrap();
        if let Some(set) = hash_value_map.get_mut(&hash_value) {
            set.insert(offset + index as u64);
        } else {
            let mut set = HashSet::new();
            set.insert(offset + index as u64);
            hash_value_map.insert(hash_value, set);
        }
    }
    for (hash_value, index_set) in hash_value_map.iter() {
        if let Some((_, separation_chain)) = hashmap
            .0
            .get_mut(*hash_value, |(hash, _)| hash_value == hash)
        {
            separation_chain.retain(|n| !index_set.contains(n));
            if separation_chain.is_empty() {
                hashmap
                    .0
                    .remove_entry(*hash_value, |(hash, _)| hash_value == hash);
            }
        }
    }
    Ok(())
}

/// Calculate the filter expression intervals.
///
/// This function updates the `interval` field of each `SortedFilterExpr` based
/// on the first or the last value of the expression in `build_input_buffer`
/// and `probe_batch`.
///
/// # Arguments
///
/// * `build_input_buffer` - The [RecordBatch] on the build side of the join.
/// * `build_sorted_filter_expr` - Build side [SortedFilterExpr] to update.
/// * `probe_batch` - The `RecordBatch` on the probe side of the join.
/// * `probe_sorted_filter_expr` - Probe side `SortedFilterExpr` to update.
///
/// ### Note
/// ```text
///
/// Interval arithmetic is used to calculate viable join ranges for build-side
/// pruning. This is done by first creating an interval for join filter values in
/// the build side of the join, which spans [-∞, FV] or [FV, ∞] depending on the
/// ordering (descending/ascending) of the filter expression. Here, FV denotes the
/// first value on the build side. This range is then compared with the probe side
/// interval, which either spans [-∞, LV] or [LV, ∞] depending on the ordering
/// (ascending/descending) of the probe side. Here, LV denotes the last value on
/// the probe side.
///
/// As a concrete example, consider the following query:
///
///   SELECT * FROM left_table, right_table
///   WHERE
///     left_key = right_key AND
///     a > b - 3 AND
///     a < b + 10
///
/// where columns "a" and "b" come from tables "left_table" and "right_table",
/// respectively. When a new `RecordBatch` arrives at the right side, the
/// condition a > b - 3 will possibly indicate a prunable range for the left
/// side. Conversely, when a new `RecordBatch` arrives at the left side, the
/// condition a < b + 10 will possibly indicate prunability for the right side.
/// Let’s inspect what happens when a new RecordBatch` arrives at the right
/// side (i.e. when the left side is the build side):
///
///         Build      Probe
///       +-------+  +-------+
///       | a | z |  | b | y |
///       |+--|--+|  |+--|--+|
///       | 1 | 2 |  | 4 | 3 |
///       |+--|--+|  |+--|--+|
///       | 3 | 1 |  | 4 | 3 |
///       |+--|--+|  |+--|--+|
///       | 5 | 7 |  | 6 | 1 |
///       |+--|--+|  |+--|--+|
///       | 7 | 1 |  | 6 | 3 |
///       +-------+  +-------+
///
/// In this case, the interval representing viable (i.e. joinable) values for
/// column "a" is [1, ∞], and the interval representing possible future values
/// for column "b" is [6, ∞]. With these intervals at hand, we next calculate
/// intervals for the whole filter expression and propagate join constraint by
/// traversing the expression graph.
/// ```
fn calculate_filter_expr_intervals(
    build_input_buffer: &RecordBatch,
    build_sorted_filter_expr: &mut SortedFilterExpr,
    probe_batch: &RecordBatch,
    probe_sorted_filter_expr: &mut SortedFilterExpr,
) -> Result<()> {
    // If either build or probe side has no data, return early:
    if build_input_buffer.num_rows() == 0 || probe_batch.num_rows() == 0 {
        return Ok(());
    }
    // Evaluate build side filter expression and convert the result to an array
    let build_array = build_sorted_filter_expr
        .origin_sorted_expr()
        .expr
        .evaluate(&build_input_buffer.slice(0, 1))?
        .into_array(1);
    // Evaluate probe side filter expression and convert the result to an array
    let probe_array = probe_sorted_filter_expr
        .origin_sorted_expr()
        .expr
        .evaluate(&probe_batch.slice(probe_batch.num_rows() - 1, 1))?
        .into_array(1);

    // Update intervals for both build and probe side filter expressions
    for (array, sorted_expr) in vec![
        (build_array, build_sorted_filter_expr),
        (probe_array, probe_sorted_filter_expr),
    ] {
        // Convert the array to a ScalarValue:
        let value = ScalarValue::try_from_array(&array, 0)?;
        // Create a ScalarValue representing positive or negative infinity for the same data type:
        let infinite = ScalarValue::try_from(value.get_datatype())?;
        // Update the interval with lower and upper bounds based on the sort option
        sorted_expr.set_interval(
            if sorted_expr.origin_sorted_expr().options.descending {
                Interval {
                    lower: infinite,
                    upper: value,
                }
            } else {
                Interval {
                    lower: value,
                    upper: infinite,
                }
            },
        );
    }
    Ok(())
}

/// Determine the pruning length for `buffer`.
///
/// This function evaluates the build side filter expression, converts the
/// result into an array and determines the pruning length by performing a
/// binary search on the array.
///
/// # Arguments
///
/// * `buffer`: The record batch to be pruned.
/// * `build_side_filter_expr`: The filter expression on the build side used
/// to determine the pruning length.
///
/// # Returns
///
/// A [Result] object that contains the pruning length. The function will return
/// an error if there is an issue evaluating the build side filter expression.
fn determine_prune_length(
    buffer: &RecordBatch,
    build_side_filter_expr: &SortedFilterExpr,
) -> Result<usize> {
    let origin_sorted_expr = build_side_filter_expr.origin_sorted_expr();
    let interval = build_side_filter_expr.interval();
    // Evaluate the build side filter expression and convert it into an array
    let batch_arr = origin_sorted_expr
        .expr
        .evaluate(buffer)?
        .into_array(buffer.num_rows());

    // Get the lower or upper interval based on the sort direction
    let target = if origin_sorted_expr.options.descending {
        interval.upper.clone()
    } else {
        interval.lower.clone()
    };

    // Perform binary search on the array to determine the length of the record batch to be pruned
    bisect::<true>(&[batch_arr], &[target], &[origin_sorted_expr.options])
}

/// This method determines if the result of the join should be produced in the final step or not.
///
/// # Arguments
///
/// * `build_side` - Enum indicating the side of the join used as the build side.
/// * `join_type` - Enum indicating the type of join to be performed.
///
/// # Returns
///
/// A boolean indicating whether the result of the join should be produced in the final step or not.
/// The result will be true if the build side is JoinSide::Left and the join type is one of
/// JoinType::Left, JoinType::LeftAnti, JoinType::Full or JoinType::LeftSemi.
/// If the build side is JoinSide::Right, the result will be true if the join type
/// is one of JoinType::Right, JoinType::RightAnti, JoinType::Full, or JoinType::RightSemi.
fn need_to_produce_result_in_final(build_side: JoinSide, join_type: JoinType) -> bool {
    if build_side == JoinSide::Left {
        matches!(
            join_type,
            JoinType::Left | JoinType::LeftAnti | JoinType::Full | JoinType::LeftSemi
        )
    } else {
        matches!(
            join_type,
            JoinType::Right | JoinType::RightAnti | JoinType::Full | JoinType::RightSemi
        )
    }
}

/// Get the anti join indices from the visited hash set.
///
/// This method returns the indices from the original input that were not present in the visited hash set.
///
/// # Arguments
///
/// * `prune_length` - The length of the pruned record batch.
/// * `deleted_offset` - The offset to the indices.
/// * `visited_rows` - The hash set of visited indices.
///
/// # Returns
///
/// A `PrimitiveArray` of the anti join indices.
fn get_anti_indices<T: ArrowPrimitiveType>(
    prune_length: usize,
    deleted_offset: usize,
    visited_rows: &HashSet<usize>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(prune_length);
    bitmap.append_n(prune_length, false);
    // mark the indices as true if they are present in the visited hash set
    for v in 0..prune_length {
        let row = v + deleted_offset;
        bitmap.set_bit(v, visited_rows.contains(&row));
    }
    // get the anti index
    (0..prune_length)
        .filter_map(|idx| (!bitmap.get_bit(idx)).then_some(T::Native::from_usize(idx)))
        .collect()
}

/// This method creates a boolean buffer from the visited rows hash set
/// and the indices of the pruned record batch slice.
///
/// It gets the indices from the original input that were present in the visited hash set.
///
/// # Arguments
///
/// * `prune_length` - The length of the pruned record batch.
/// * `deleted_offset` - The offset to the indices.
/// * `visited_rows` - The hash set of visited indices.
///
/// # Returns
///
/// A [PrimitiveArray] of the specified type T, containing the semi indices.
fn get_semi_indices<T: ArrowPrimitiveType>(
    prune_length: usize,
    deleted_offset: usize,
    visited_rows: &HashSet<usize>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(prune_length);
    bitmap.append_n(prune_length, false);
    // mark the indices as true if they are present in the visited hash set
    (0..prune_length).for_each(|v| {
        let row = &(v + deleted_offset);
        bitmap.set_bit(v, visited_rows.contains(row));
    });
    // get the semi index
    (0..prune_length)
        .filter_map(|idx| (bitmap.get_bit(idx)).then_some(T::Native::from_usize(idx)))
        .collect::<PrimitiveArray<T>>()
}
/// Records the visited indices from the input `PrimitiveArray` of type `T` into the given hash set `visited`.
/// This function will insert the indices (offset by `offset`) into the `visited` hash set.
///
/// # Arguments
///
/// * `visited` - A hash set to store the visited indices.
/// * `offset` - An offset to the indices in the `PrimitiveArray`.
/// * `indices` - The input `PrimitiveArray` of type `T` which stores the indices to be recorded.
///
fn record_visited_indices<T: ArrowPrimitiveType>(
    visited: &mut HashSet<usize>,
    offset: usize,
    indices: &PrimitiveArray<T>,
) {
    for i in indices.values() {
        visited.insert(i.as_usize() + offset);
    }
}

/// Calculate indices by join type.
///
/// This method returns a tuple of two arrays: build and probe indices.
/// The length of both arrays will be the same.
///
/// # Arguments
///
/// * `build_side`: Join side which defines the build side.
/// * `prune_length`: Length of the prune data.
/// * `visited_rows`: Hash set of visited rows of the build side.
/// * `deleted_offset`: Deleted offset of the build side.
/// * `join_type`: The type of join to be performed.
///
/// # Returns
///
/// A tuple of two arrays of primitive types representing the build and probe indices.
///
fn calculate_indices_by_join_type<L: ArrowPrimitiveType, R: ArrowPrimitiveType>(
    build_side: JoinSide,
    prune_length: usize,
    visited_rows: &HashSet<usize>,
    deleted_offset: usize,
    join_type: JoinType,
) -> Result<(PrimitiveArray<L>, PrimitiveArray<R>)>
where
    NativeAdapter<L>: From<<L as ArrowPrimitiveType>::Native>,
{
    // Store the result in a tuple
    let result = match (build_side, join_type) {
        // In the case of `Left` or `Right` join, or `Full` join, get the anti indices
        (JoinSide::Left, JoinType::Left | JoinType::LeftAnti)
        | (JoinSide::Right, JoinType::Right | JoinType::RightAnti)
        | (_, JoinType::Full) => {
            let build_unmatched_indices =
                get_anti_indices(prune_length, deleted_offset, visited_rows);
            let mut builder =
                PrimitiveBuilder::<R>::with_capacity(build_unmatched_indices.len());
            builder.append_nulls(build_unmatched_indices.len());
            let probe_indices = builder.finish();
            (build_unmatched_indices, probe_indices)
        }
        // In the case of `LeftSemi` or `RightSemi` join, get the semi indices
        (JoinSide::Left, JoinType::LeftSemi) | (JoinSide::Right, JoinType::RightSemi) => {
            let build_unmatched_indices =
                get_semi_indices(prune_length, deleted_offset, visited_rows);
            let mut builder =
                PrimitiveBuilder::<R>::with_capacity(build_unmatched_indices.len());
            builder.append_nulls(build_unmatched_indices.len());
            let probe_indices = builder.finish();
            (build_unmatched_indices, probe_indices)
        }
        // The case of other join types is not considered
        _ => unreachable!(),
    };
    Ok(result)
}

struct OneSideHashJoiner {
    /// Build side
    build_side: JoinSide,
    /// Build side filter sort information
    sorted_filter_expr: SortedFilterExpr,
    /// Input record batch buffer
    input_buffer: RecordBatch,
    /// Columns from the side
    on: Vec<Column>,
    /// Hashmap
    hashmap: JoinHashMap,
    /// To optimize hash deleting in case of pruning, we hold them in memory
    row_hash_values: VecDeque<u64>,
    /// Reuse the hashes buffer
    hashes_buffer: Vec<u64>,
    /// Matched rows
    visited_rows: HashSet<usize>,
    /// Offset
    offset: usize,
    /// Deleted offset
    deleted_offset: usize,
    /// Side is exhausted
    exhausted: bool,
}

impl OneSideHashJoiner {
    pub fn new(
        build_side: JoinSide,
        sorted_filter_expr: SortedFilterExpr,
        on: Vec<Column>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            build_side,
            input_buffer: RecordBatch::new_empty(schema),
            on,
            hashmap: JoinHashMap(RawTable::with_capacity(10_000)),
            row_hash_values: VecDeque::new(),
            hashes_buffer: vec![],
            sorted_filter_expr,
            visited_rows: HashSet::new(),
            offset: 0,
            deleted_offset: 0,
            exhausted: false,
        }
    }

    /// Updates the internal state of the [OneSideHashJoiner] with the incoming batch.
    ///
    /// # Arguments
    ///
    /// * `batch` - The incoming [RecordBatch] to be merged with the internal input buffer
    /// * `random_state` - The random state used to hash values
    ///
    /// # Returns
    ///
    /// Returns a [Result] encapsulating any intermediate errors.
    fn update_internal_state(
        &mut self,
        batch: &RecordBatch,
        random_state: &RandomState,
    ) -> Result<()> {
        // Merge the incoming batch with the existing input buffer:
        self.input_buffer = concat_batches(&batch.schema(), [&self.input_buffer, batch])?;
        // Resize the hashes buffer to the number of rows in the incoming batch:
        self.hashes_buffer.resize(batch.num_rows(), 0);
        // Update the hashmap with the join key values and hashes of the incoming batch:
        update_hash(
            &self.on,
            batch,
            &mut self.hashmap,
            self.offset,
            random_state,
            &mut self.hashes_buffer,
        )?;
        // Add the hashes buffer to the hash value deque:
        self.row_hash_values.extend(self.hashes_buffer.iter());
        Ok(())
    }

    /// This method performs a join between the build side input buffer and the probe side batch.
    ///
    /// # Arguments
    ///
    /// * `schema` - A reference to the schema of the output record batch.
    /// * `join_type` - The type of join to be performed.
    /// * `on_probe` - An array of columns on which the join will be performed. The columns are from the probe side of the join.
    /// * `filter` - An optional filter on the join condition.
    /// * `probe_batch` - The second record batch to be joined.
    /// * `probe_visited` - A hash set to store the visited indices from the probe batch.
    /// * `probe_offset` - The offset of the probe side for visited indices calculations.
    /// * `column_indices` - An array of columns to be selected for the result of the join.
    /// * `random_state` - The random state for the join.
    /// * `null_equals_null` - A boolean indicating whether NULL values should be treated as equal when joining.
    ///
    /// # Returns
    ///
    /// A [Result] containing an optional record batch if the join type is not one of `LeftAnti`, `RightAnti`, `LeftSemi` or `RightSemi`.
    /// If the join type is one of the above four, the function will return [None].
    #[allow(clippy::too_many_arguments)]
    fn join_with_probe_batch(
        &mut self,
        schema: &SchemaRef,
        join_type: JoinType,
        on_probe: &[Column],
        filter: &JoinFilter,
        probe_batch: &RecordBatch,
        probe_visited: &mut HashSet<usize>,
        probe_offset: usize,
        column_indices: &[ColumnIndex],
        random_state: &RandomState,
        null_equals_null: bool,
    ) -> Result<Option<RecordBatch>> {
        if self.input_buffer.num_rows() == 0 || probe_batch.num_rows() == 0 {
            return Ok(Some(RecordBatch::new_empty(schema.clone())));
        }
        let (build_indices, probe_indices) = build_join_indices(
            probe_batch,
            &self.hashmap,
            &self.input_buffer,
            &self.on,
            on_probe,
            Some(filter),
            random_state,
            null_equals_null,
            &mut self.hashes_buffer,
            Some(self.deleted_offset),
            self.build_side,
        )?;
        if need_to_produce_result_in_final(self.build_side, join_type) {
            record_visited_indices(
                &mut self.visited_rows,
                self.deleted_offset,
                &build_indices,
            );
        }
        if need_to_produce_result_in_final(self.build_side.negate(), join_type) {
            record_visited_indices(probe_visited, probe_offset, &probe_indices);
        }
        if matches!(
            join_type,
            JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::LeftSemi
                | JoinType::RightSemi
        ) {
            Ok(None)
        } else {
            build_batch_from_indices(
                schema,
                &self.input_buffer,
                probe_batch,
                build_indices,
                probe_indices,
                column_indices,
                self.build_side,
            )
            .map(Some)
        }
    }

    /// This function produces unmatched record results based on the build side,
    /// join type and other parameters.
    ///
    /// The method uses first `prune_length` rows from the build side input buffer
    /// to produce results.
    ///
    /// # Arguments
    ///
    /// * `output_schema` - The schema of the final output record batch.
    /// * `prune_length` - The length of the determined prune length.
    /// * `probe_schema` - The schema of the probe [RecordBatch].
    /// * `join_type` - The type of join to be performed.
    /// * `column_indices` - Indices of columns that are being joined.
    ///
    /// # Returns
    ///
    /// * `Option<RecordBatch>` - The final output record batch if required, otherwise [None].
    fn build_side_determined_results(
        &self,
        output_schema: &SchemaRef,
        prune_length: usize,
        probe_schema: SchemaRef,
        join_type: JoinType,
        column_indices: &[ColumnIndex],
    ) -> Result<Option<RecordBatch>> {
        // Check if we need to produce a result in the final output:
        if need_to_produce_result_in_final(self.build_side, join_type) {
            // Calculate the indices for build and probe sides based on join type and build side:
            let (build_indices, probe_indices) = calculate_indices_by_join_type(
                self.build_side,
                prune_length,
                &self.visited_rows,
                self.deleted_offset,
                join_type,
            )?;

            // Create an empty probe record batch:
            let empty_probe_batch = RecordBatch::new_empty(probe_schema);
            // Build the final result from the indices of build and probe sides:
            build_batch_from_indices(
                output_schema.as_ref(),
                &self.input_buffer,
                &empty_probe_batch,
                build_indices,
                probe_indices,
                column_indices,
                self.build_side,
            )
            .map(Some)
        } else {
            // If we don't need to produce a result, return None
            Ok(None)
        }
    }

    /// Prunes the internal buffer.
    ///
    /// Argument `probe_batch` is used to update the intervals of the sorted
    /// filter expressions. The updated build interval determines the new length
    /// of the build side. If there are rows to prune, they are removed from the
    /// internal buffer.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the final output record batch
    /// * `probe_batch` - Incoming RecordBatch of the probe side.
    /// * `probe_side_sorted_filter_expr` - Probe side mutable sorted filter expression.
    /// * `join_type` - The type of join (e.g. inner, left, right, etc.).
    /// * `column_indices` - A vector of column indices that specifies which columns from the
    ///     build side should be included in the output.
    /// * `physical_expr_graph` - A mutable reference to the physical expression graph.
    ///
    /// # Returns
    ///
    /// If there are rows to prune, returns the pruned build side record batch wrapped in an `Ok` variant.
    /// Otherwise, returns `Ok(None)`.
    fn prune_with_probe_batch(
        &mut self,
        schema: &SchemaRef,
        probe_batch: &RecordBatch,
        probe_side_sorted_filter_expr: &mut SortedFilterExpr,
        join_type: JoinType,
        column_indices: &[ColumnIndex],
        physical_expr_graph: &mut ExprIntervalGraph,
    ) -> Result<Option<RecordBatch>> {
        // Check if the input buffer is empty:
        if self.input_buffer.num_rows() == 0 {
            return Ok(None);
        }
        // Convert the sorted filter expressions into a vector of (node_index, interval)
        // tuples for use when updating the interval graph.
        let mut filter_intervals = vec![
            (
                self.sorted_filter_expr.node_index(),
                self.sorted_filter_expr.interval().clone(),
            ),
            (
                probe_side_sorted_filter_expr.node_index(),
                probe_side_sorted_filter_expr.interval().clone(),
            ),
        ];
        // Use the join filter intervals to update the physical expression graph:
        physical_expr_graph.update_ranges(&mut filter_intervals)?;
        // Get the new join filter interval for build side:
        let calculated_build_side_interval = filter_intervals.remove(0).1;
        // Check if the intervals changed, exit early if not:
        if calculated_build_side_interval.eq(self.sorted_filter_expr.interval()) {
            return Ok(None);
        }
        // Determine the pruning length if there was a change in the intervals:
        self.sorted_filter_expr
            .set_interval(calculated_build_side_interval);
        let prune_length =
            determine_prune_length(&self.input_buffer, &self.sorted_filter_expr)?;
        // If we can not prune, exit early:
        if prune_length == 0 {
            return Ok(None);
        }
        // Compute the result, and perform pruning if there are rows to prune:
        let result = self.build_side_determined_results(
            schema,
            prune_length,
            probe_batch.schema(),
            join_type,
            column_indices,
        );
        prune_hash_values(
            prune_length,
            &mut self.hashmap,
            &mut self.row_hash_values,
            self.deleted_offset as u64,
        )?;
        for row in self.deleted_offset..(self.deleted_offset + prune_length) {
            self.visited_rows.remove(&row);
        }
        self.input_buffer = self
            .input_buffer
            .slice(prune_length, self.input_buffer.num_rows() - prune_length);
        self.deleted_offset += prune_length;
        result
    }
}

fn combine_two_batches(
    output_schema: &SchemaRef,
    left_batch: Option<RecordBatch>,
    right_batch: Option<RecordBatch>,
) -> Result<Option<RecordBatch>> {
    match (left_batch, right_batch) {
        (Some(batch), None) | (None, Some(batch)) => {
            // If only one of the batches are present, return it:
            Ok(Some(batch))
        }
        (Some(left_batch), Some(right_batch)) => {
            // If both batches are present, concatenate them:
            concat_batches(output_schema, &[left_batch, right_batch])
                .map_err(DataFusionError::ArrowError)
                .map(Some)
        }
        (None, None) => {
            // If neither is present, return an empty batch:
            Ok(None)
        }
    }
}

impl SymmetricHashJoinStream {
    /// Polls the next result of the join operation.
    ///
    /// If the result of the join is ready, it returns the next record batch.
    /// If the join has completed and there are no more results, it returns
    /// `Poll::Ready(None)`. If the join operation is not complete, but the
    /// current stream is not ready yet, it returns `Poll::Pending`.
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            // If the final result has already been obtained, return `Poll::Ready(None)`:
            if self.final_result {
                return Poll::Ready(None);
            }
            // If both streams have been exhausted, return the final result:
            if self.right.exhausted && self.left.exhausted {
                // Get left side results:
                let left_result = self.left.build_side_determined_results(
                    &self.schema,
                    self.left.input_buffer.num_rows(),
                    self.right.input_buffer.schema(),
                    self.join_type,
                    &self.column_indices,
                )?;
                // Get right side results:
                let right_result = self.right.build_side_determined_results(
                    &self.schema,
                    self.right.input_buffer.num_rows(),
                    self.left.input_buffer.schema(),
                    self.join_type,
                    &self.column_indices,
                )?;
                self.final_result = true;
                // Combine results:
                let result =
                    combine_two_batches(&self.schema, left_result, right_result)?;
                // Update the metrics if we have a batch; otherwise, continue the loop.
                if let Some(batch) = &result {
                    self.metrics.output_batches.add(1);
                    self.metrics.output_rows.add(batch.num_rows());
                    return Poll::Ready(Ok(result).transpose());
                } else {
                    continue;
                }
            }

            // Determine which stream should be polled next. The side the
            // RecordBatch comes from becomes the probe side.
            let (
                input_stream,
                probe_hash_joiner,
                build_hash_joiner,
                build_join_side,
                probe_side_metrics,
            ) = if self.probe_side.eq(&JoinSide::Left) {
                (
                    &mut self.left_stream,
                    &mut self.left,
                    &mut self.right,
                    JoinSide::Right,
                    &mut self.metrics.left,
                )
            } else {
                (
                    &mut self.right_stream,
                    &mut self.right,
                    &mut self.left,
                    JoinSide::Left,
                    &mut self.metrics.right,
                )
            };
            // Poll the next batch from `input_stream`:
            match input_stream.poll_next_unpin(cx) {
                // Batch is available
                Poll::Ready(Some(Ok(probe_batch))) => {
                    // Update the metrics for the stream that was polled:
                    probe_side_metrics.input_batches.add(1);
                    probe_side_metrics.input_rows.add(probe_batch.num_rows());
                    // Update the internal state of the hash joiner for the build side:
                    probe_hash_joiner
                        .update_internal_state(&probe_batch, &self.random_state)?;
                    // Calculate filter intervals:
                    calculate_filter_expr_intervals(
                        &build_hash_joiner.input_buffer,
                        &mut build_hash_joiner.sorted_filter_expr,
                        &probe_batch,
                        &mut probe_hash_joiner.sorted_filter_expr,
                    )?;
                    // Join the two sides:
                    let equal_result = build_hash_joiner.join_with_probe_batch(
                        &self.schema,
                        self.join_type,
                        &probe_hash_joiner.on,
                        &self.filter,
                        &probe_batch,
                        &mut probe_hash_joiner.visited_rows,
                        probe_hash_joiner.offset,
                        &self.column_indices,
                        &self.random_state,
                        self.null_equals_null,
                    )?;
                    // Increment the offset for the probe hash joiner:
                    probe_hash_joiner.offset += probe_batch.num_rows();
                    // Prune the build side input buffer using the expression
                    // DAG and filter intervals:
                    let anti_result = build_hash_joiner.prune_with_probe_batch(
                        &self.schema,
                        &probe_batch,
                        &mut probe_hash_joiner.sorted_filter_expr,
                        self.join_type,
                        &self.column_indices,
                        &mut self.physical_expr_graph,
                    )?;
                    // Combine results:
                    let result =
                        combine_two_batches(&self.schema, equal_result, anti_result)?;
                    // Choose next poll side. If the other side is not exhausted,
                    // switch the probe side before returning the result.
                    if !build_hash_joiner.exhausted {
                        self.probe_side = build_join_side;
                    }
                    // Update the metrics if we have a batch; otherwise, continue the loop.
                    if let Some(batch) = &result {
                        self.metrics.output_batches.add(1);
                        self.metrics.output_rows.add(batch.num_rows());
                        return Poll::Ready(Ok(result).transpose());
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // Mark the probe side exhausted:
                    probe_hash_joiner.exhausted = true;
                    // Change the probe side:
                    self.probe_side = build_join_side;
                }
                Poll::Pending => {
                    if !build_hash_joiner.exhausted {
                        self.probe_side = build_join_side;
                    } else {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow::array::ArrayRef;
    use arrow::array::{Int32Array, TimestampNanosecondArray};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::util::pretty::pretty_format_batches;
    use rstest::*;
    use tempfile::TempDir;

    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, col, Column};
    use datafusion_physical_expr::intervals::test_utils::gen_conjunctive_numeric_expr;
    use datafusion_physical_expr::PhysicalExpr;

    use crate::physical_plan::joins::{
        hash_join_utils::tests::complicated_filter, HashJoinExec, PartitionMode,
    };
    use crate::physical_plan::{
        collect, common, memory::MemoryExec, repartition::RepartitionExec,
    };
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test_util;

    use super::*;

    const TABLE_SIZE: i32 = 1_000;

    fn compare_batches(collected_1: &[RecordBatch], collected_2: &[RecordBatch]) {
        // compare
        let first_formatted = pretty_format_batches(collected_1).unwrap().to_string();
        let second_formatted = pretty_format_batches(collected_2).unwrap().to_string();

        let mut first_formatted_sorted: Vec<&str> =
            first_formatted.trim().lines().collect();
        first_formatted_sorted.sort_unstable();

        let mut second_formatted_sorted: Vec<&str> =
            second_formatted.trim().lines().collect();
        second_formatted_sorted.sort_unstable();

        for (i, (first_line, second_line)) in first_formatted_sorted
            .iter()
            .zip(&second_formatted_sorted)
            .enumerate()
        {
            assert_eq!((i, first_line), (i, second_line));
        }
    }
    #[allow(clippy::too_many_arguments)]
    async fn partitioned_sym_join_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: &JoinType,
        null_equals_null: bool,
        context: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let partition_count = 4;

        let left_expr = on
            .iter()
            .map(|(l, _)| Arc::new(l.clone()) as _)
            .collect::<Vec<_>>();

        let right_expr = on
            .iter()
            .map(|(_, r)| Arc::new(r.clone()) as _)
            .collect::<Vec<_>>();

        let join = SymmetricHashJoinExec::try_new(
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
        )?;

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
    #[allow(clippy::too_many_arguments)]
    async fn partitioned_hash_join_with_filter(
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

        let join = HashJoinExec::try_new(
            Arc::new(RepartitionExec::try_new(
                left,
                Partitioning::Hash(left_expr, partition_count),
            )?),
            Arc::new(RepartitionExec::try_new(
                right,
                Partitioning::Hash(right_expr, partition_count),
            )?),
            on,
            Some(filter),
            join_type,
            PartitionMode::Partitioned,
            null_equals_null,
        )?;

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

    pub fn split_record_batches(
        batch: &RecordBatch,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        let row_num = batch.num_rows();
        let number_of_batch = row_num / batch_size;
        let mut sizes = vec![batch_size; number_of_batch];
        sizes.push(row_num - (batch_size * number_of_batch));
        let mut result = vec![];
        for (i, size) in sizes.iter().enumerate() {
            result.push(batch.slice(i * batch_size, *size));
        }
        Ok(result)
    }

    fn join_expr_tests_fixture(
        expr_id: usize,
        left_col: Arc<dyn PhysicalExpr>,
        right_col: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        match expr_id {
            // left_col + 1 > right_col + 5 AND left_col + 3 < right_col + 10
            0 => gen_conjunctive_numeric_expr(
                left_col,
                right_col,
                Operator::Plus,
                Operator::Plus,
                Operator::Plus,
                Operator::Plus,
                1,
                5,
                3,
                10,
            ),
            // left_col - 1 > right_col + 5 AND left_col + 3 < right_col + 10
            1 => gen_conjunctive_numeric_expr(
                left_col,
                right_col,
                Operator::Minus,
                Operator::Plus,
                Operator::Plus,
                Operator::Plus,
                1,
                5,
                3,
                10,
            ),
            // left_col - 1 > right_col + 5 AND left_col - 3 < right_col + 10
            2 => gen_conjunctive_numeric_expr(
                left_col,
                right_col,
                Operator::Minus,
                Operator::Plus,
                Operator::Minus,
                Operator::Plus,
                1,
                5,
                3,
                10,
            ),
            // left_col - 10 > right_col - 5 AND left_col - 3 < right_col + 10
            3 => gen_conjunctive_numeric_expr(
                left_col,
                right_col,
                Operator::Minus,
                Operator::Minus,
                Operator::Minus,
                Operator::Plus,
                10,
                5,
                3,
                10,
            ),
            // left_col - 10 > right_col - 5 AND left_col - 30 < right_col - 3
            4 => gen_conjunctive_numeric_expr(
                left_col,
                right_col,
                Operator::Minus,
                Operator::Minus,
                Operator::Minus,
                Operator::Minus,
                10,
                5,
                30,
                3,
            ),
            _ => unreachable!(),
        }
    }
    fn build_sides_record_batches(
        table_size: i32,
        key_cardinality: (i32, i32),
    ) -> Result<(RecordBatch, RecordBatch)> {
        let null_ratio: f64 = 0.4;
        let initial_range = 0..table_size;
        let index = (table_size as f64 * null_ratio).round() as i32;
        let rest_of = index..table_size;
        let ordered: ArrayRef = Arc::new(Int32Array::from_iter(
            initial_range.clone().collect::<Vec<i32>>(),
        ));
        let ordered_des = Arc::new(Int32Array::from_iter(
            initial_range.clone().rev().collect::<Vec<i32>>(),
        ));
        let cardinality = Arc::new(Int32Array::from_iter(
            initial_range.clone().map(|x| x % 4).collect::<Vec<i32>>(),
        ));
        let cardinality_key = Arc::new(Int32Array::from_iter(
            initial_range
                .clone()
                .map(|x| x % key_cardinality.0)
                .collect::<Vec<i32>>(),
        ));
        let ordered_asc_null_first = Arc::new(Int32Array::from_iter({
            std::iter::repeat(None)
                .take(index as usize)
                .chain(rest_of.clone().map(Some))
                .collect::<Vec<Option<i32>>>()
        }));
        let ordered_asc_null_last = Arc::new(Int32Array::from_iter({
            rest_of
                .clone()
                .map(Some)
                .chain(std::iter::repeat(None).take(index as usize))
                .collect::<Vec<Option<i32>>>()
        }));

        let ordered_desc_null_first = Arc::new(Int32Array::from_iter({
            std::iter::repeat(None)
                .take(index as usize)
                .chain(rest_of.rev().map(Some))
                .collect::<Vec<Option<i32>>>()
        }));

        let time = Arc::new(TimestampNanosecondArray::from(
            initial_range
                .map(|x| 1664264591000000000 + (5000000000 * (x as i64)))
                .collect::<Vec<i64>>(),
        ));

        let left = RecordBatch::try_from_iter(vec![
            ("la1", ordered.clone()),
            ("lb1", cardinality.clone()),
            ("lc1", cardinality_key.clone()),
            ("lt1", time.clone()),
            ("la2", ordered.clone()),
            ("la1_des", ordered_des.clone()),
            ("l_asc_null_first", ordered_asc_null_first.clone()),
            ("l_asc_null_last", ordered_asc_null_last.clone()),
            ("l_desc_null_first", ordered_desc_null_first.clone()),
        ])?;
        let right = RecordBatch::try_from_iter(vec![
            ("ra1", ordered.clone()),
            ("rb1", cardinality),
            ("rc1", cardinality_key),
            ("rt1", time),
            ("ra2", ordered),
            ("ra1_des", ordered_des),
            ("r_asc_null_first", ordered_asc_null_first),
            ("r_asc_null_last", ordered_asc_null_last),
            ("r_desc_null_first", ordered_desc_null_first),
        ])?;
        Ok((left, right))
    }

    fn create_memory_table(
        left_batch: RecordBatch,
        right_batch: RecordBatch,
        left_sorted: Vec<PhysicalSortExpr>,
        right_sorted: Vec<PhysicalSortExpr>,
        batch_size: usize,
    ) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
        Ok((
            Arc::new(
                MemoryExec::try_new(
                    &[split_record_batches(&left_batch, batch_size).unwrap()],
                    left_batch.schema(),
                    None,
                )?
                .with_sort_information(left_sorted),
            ),
            Arc::new(
                MemoryExec::try_new(
                    &[split_record_batches(&right_batch, batch_size).unwrap()],
                    right_batch.schema(),
                    None,
                )?
                .with_sort_information(right_sorted),
            ),
        ))
    }

    async fn experiment(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: JoinFilter,
        join_type: JoinType,
        on: JoinOn,
        task_ctx: Arc<TaskContext>,
    ) -> Result<()> {
        let first_batches = partitioned_sym_join_with_filter(
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
            left, right, on, filter, &join_type, false, task_ctx,
        )
        .await?;
        compare_batches(&first_batches, &second_batches);
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
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
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
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 13)?;

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
        #[values(0, 1, 2, 3, 4)] case_expr: usize,
    ) -> Result<()> {
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
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
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 13)?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture(
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
    async fn single_test() -> Result<()> {
        let case_expr = 1;
        let cardinality = (11, 21);
        let join_type = JoinType::Full;
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
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
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 13)?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture(
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
        #[values(0, 1, 2, 3, 4)] case_expr: usize,
    ) -> Result<()> {
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
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
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 13)?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture(
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

    #[tokio::test(flavor = "multi_thread")]
    async fn join_change_in_planner() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::with_config(config);
        let tmp_dir = TempDir::new().unwrap();
        let left_file_path = tmp_dir.path().join("left.csv");
        File::create(left_file_path.clone()).unwrap();
        test_util::test_create_unbounded_sorted_file(
            &ctx,
            left_file_path.clone(),
            "left",
        )
        .await?;
        let right_file_path = tmp_dir.path().join("right.csv");
        File::create(right_file_path.clone()).unwrap();
        test_util::test_create_unbounded_sorted_file(
            &ctx,
            right_file_path.clone(),
            "right",
        )
        .await?;
        let df = ctx.sql("EXPLAIN SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10").await?;
        let physical_plan = df.create_physical_plan().await?;
        let task_ctx = ctx.task_ctx();
        let results = collect(physical_plan.clone(), task_ctx).await.unwrap();
        let formatted = pretty_format_batches(&results).unwrap().to_string();
        let found = formatted
            .lines()
            .any(|line| line.contains("SymmetricHashJoinExec"));
        assert!(found);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_null_columns_first() -> Result<()> {
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
            expr: col("l_asc_null_first", left_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("r_asc_null_first", right_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 13)?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 6,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 6,
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
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 13)?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture(
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
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 13)?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];

        let intermediate_schema = Schema::new(vec![
            Field::new("left", DataType::Int32, true),
            Field::new("right", DataType::Int32, true),
        ]);
        let filter_expr = join_expr_tests_fixture(
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

    #[tokio::test(flavor = "multi_thread")]
    async fn complex_join_all_one_ascending_numeric_missing_stat() -> Result<()> {
        let cardinality = (3, 4);
        let join_type = JoinType::Full;

        // a + b > c + 10 AND a + b < c + 100
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
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
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 13)?;

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

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_one_side_hash_joiner_visited_rows(
        #[values(
        (JoinType::Inner, true),
        (JoinType::Left,false),
        (JoinType::Right, true),
        (JoinType::RightSemi, true),
        (JoinType::LeftSemi, false),
        (JoinType::LeftAnti, false),
        (JoinType::RightAnti, true),
        (JoinType::Full, false),
        )]
        case: (JoinType, bool),
    ) -> Result<()> {
        // Set a random state for the join
        let join_type = case.0;
        let should_be_empty = case.1;
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
        let task_ctx = session_ctx.task_ctx();
        // Ensure there will be matching rows
        let (left_batch, right_batch) = build_sides_record_batches(20, (1, 1))?;
        let left_schema = left_batch.schema();
        let right_schema = right_batch.schema();

        // Build the join schema from the left and right schemas
        let (schema, join_column_indices) =
            build_join_schema(&left_schema, &right_schema, &join_type);
        let join_schema = Arc::new(schema);

        // Sort information for MemoryExec
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("la1", &left_schema)?,
            options: SortOptions::default(),
        }];
        // Sort information for MemoryExec
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ra1", &right_schema)?,
            options: SortOptions::default(),
        }];
        // Construct MemoryExec
        let (left, right) =
            create_memory_table(left_batch, right_batch, left_sorted, right_sorted, 10)?;

        // Filter columns, ensure first batches will have matching rows.
        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
        ]);
        let filter_expr = gen_conjunctive_numeric_expr(
            col("0", &intermediate_schema)?,
            col("1", &intermediate_schema)?,
            Operator::Plus,
            Operator::Minus,
            Operator::Plus,
            Operator::Plus,
            0,
            3,
            0,
            3,
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

        let left_sorted_filter_expr = SortedFilterExpr::new(
            PhysicalSortExpr {
                expr: col("la1", &left_schema)?,
                options: SortOptions::default(),
            },
            Arc::new(Column::new("0", 0)),
        );
        let mut left_side_joiner = OneSideHashJoiner::new(
            JoinSide::Left,
            left_sorted_filter_expr,
            vec![Column::new_with_schema("lc1", &left_schema)?],
            left_schema,
        );

        let right_sorted_filter_expr = SortedFilterExpr::new(
            PhysicalSortExpr {
                expr: col("ra1", &right_schema)?,
                options: SortOptions::default(),
            },
            Arc::new(Column::new("1", 0)),
        );
        let mut right_side_joiner = OneSideHashJoiner::new(
            JoinSide::Right,
            right_sorted_filter_expr,
            vec![Column::new_with_schema("rc1", &right_schema)?],
            right_schema,
        );

        let mut left_stream = left.execute(0, task_ctx.clone())?;
        let mut right_stream = right.execute(0, task_ctx)?;

        let initial_left_batch = left_stream.next().await.unwrap()?;
        left_side_joiner.update_internal_state(&initial_left_batch, &random_state)?;
        assert_eq!(
            left_side_joiner.input_buffer.num_rows(),
            initial_left_batch.num_rows()
        );

        let initial_right_batch = right_stream.next().await.unwrap()?;
        right_side_joiner.update_internal_state(&initial_right_batch, &random_state)?;
        assert_eq!(
            right_side_joiner.input_buffer.num_rows(),
            initial_right_batch.num_rows()
        );

        left_side_joiner.join_with_probe_batch(
            &join_schema,
            join_type,
            &right_side_joiner.on,
            &filter,
            &initial_right_batch,
            &mut right_side_joiner.visited_rows,
            right_side_joiner.offset,
            &join_column_indices,
            &random_state,
            false,
        )?;
        assert_eq!(left_side_joiner.visited_rows.is_empty(), should_be_empty);
        Ok(())
    }
}
