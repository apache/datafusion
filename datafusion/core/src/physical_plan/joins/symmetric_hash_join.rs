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
use futures::stream::{select, BoxStream};
use futures::{Stream, StreamExt};
use hashbrown::{raw::RawTable, HashSet};
use parking_lot::Mutex;

use datafusion_common::{utils::bisect, ScalarValue};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_physical_expr::intervals::{ExprIntervalGraph, Interval, IntervalBound};

use crate::error::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use crate::logical_expr::JoinType;
use crate::physical_plan::common::SharedMemoryReservation;
use crate::physical_plan::joins::hash_join_utils::convert_sort_expr_with_filter_schema;
use crate::physical_plan::joins::hash_join_utils::JoinHashMap;
use crate::physical_plan::{
    expressions::Column,
    expressions::PhysicalSortExpr,
    joins::{
        hash_join::{build_join_indices, update_hash},
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

const HASHMAP_SHRINK_SCALE_FACTOR: usize = 4;

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
#[derive(Debug)]
pub struct SymmetricHashJoinExec {
    /// Left side stream
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// Right side stream
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    pub(crate) on: Vec<(Column, Column)>,
    /// Filters applied when finding matching rows
    pub(crate) filter: Option<JoinFilter>,
    /// How the join is performed
    pub(crate) join_type: JoinType,
    /// Expression graph and `SortedFilterExpr`s for interval calculations
    filter_state: Option<Arc<Mutex<IntervalCalculatorInnerState>>>,
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

struct IntervalCalculatorInnerState {
    /// Expression graph for interval calculations
    graph: Option<ExprIntervalGraph>,
    sorted_exprs: Vec<Option<SortedFilterExpr>>,
    calculated: bool,
}

impl Debug for IntervalCalculatorInnerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Exprs({:?})", self.sorted_exprs)
    }
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
    /// Memory used by sides in bytes
    pub(crate) stream_memory_usage: metrics::Gauge,
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

impl SymmetricHashJoinExec {
    /// Tries to create a new [SymmetricHashJoinExec].
    /// # Error
    /// This function errors when:
    /// - It is not possible to join the left and right sides on keys `on`, or
    /// - It fails to construct `SortedFilterExpr`s, or
    /// - It fails to create the [ExprIntervalGraph].
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
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

        // Initialize the random state for the join operation:
        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let filter_state = if filter.is_some() {
            let inner_state = IntervalCalculatorInnerState {
                graph: None,
                sorted_exprs: vec![],
                calculated: false,
            };
            Some(Arc::new(Mutex::new(inner_state)))
        } else {
            None
        };

        Ok(SymmetricHashJoinExec {
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
    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// Get null_equals_null
    pub fn null_equals_null(&self) -> bool {
        self.null_equals_null
    }

    /// Check if order information covers every column in the filter expression.
    pub fn check_if_order_information_available(&self) -> Result<bool> {
        if let Some(filter) = self.filter() {
            let left = self.left();
            if let Some(left_ordering) = left.output_ordering() {
                let right = self.right();
                if let Some(right_ordering) = right.output_ordering() {
                    let left_convertible = convert_sort_expr_with_filter_schema(
                        &JoinSide::Left,
                        filter,
                        &left.schema(),
                        &left_ordering[0],
                    )?
                    .is_some();
                    let right_convertible = convert_sort_expr_with_filter_schema(
                        &JoinSide::Right,
                        filter,
                        &right.schema(),
                        &right_ordering[0],
                    )?
                    .is_some();
                    return Ok(left_convertible && right_convertible);
                }
            }
        }
        Ok(false)
    }
}

impl ExecutionPlan for SymmetricHashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children.iter().any(|u| *u))
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let (left_expr, right_expr) = self
            .on
            .iter()
            .map(|(l, r)| (Arc::new(l.clone()) as _, Arc::new(r.clone()) as _))
            .unzip();
        // TODO:  This will change when we extend collected executions.
        vec![
            Distribution::HashPartitioned(left_expr),
            Distribution::HashPartitioned(right_expr),
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
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
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
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();
        if left_partitions != right_partitions {
            return Err(DataFusionError::Internal(format!(
                "Invalid SymmetricHashJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec",
            )));
        }
        // If `filter_state` and `filter` are both present, then calculate sorted filter expressions
        // for both sides, and build an expression graph if one is not already built.
        let (left_sorted_filter_expr, right_sorted_filter_expr, graph) =
            match (&self.filter_state, &self.filter) {
                (Some(interval_state), Some(filter)) => {
                    // Lock the mutex of the interval state:
                    let mut filter_state = interval_state.lock();
                    // If this is the first partition to be invoked, then we need to initialize our state
                    // (the expression graph for pruning, sorted filter expressions etc.)
                    if !filter_state.calculated {
                        // Interval calculations require each column to exhibit monotonicity
                        // independently. However, a `PhysicalSortExpr` object defines a
                        // lexicographical ordering, so we can only use their first elements.
                        // when deducing column monotonicities.
                        // TODO: Extend the `PhysicalSortExpr` mechanism to express independent
                        //       (i.e. simultaneous) ordering properties of columns.

                        // Build sorted filter expressions for the left and right join side:
                        let join_sides = [JoinSide::Left, JoinSide::Right];
                        let children = [&self.left, &self.right];
                        for (join_side, child) in join_sides.iter().zip(children.iter()) {
                            let sorted_expr = child
                                .output_ordering()
                                .and_then(|orders| {
                                    build_filter_input_order(
                                        *join_side,
                                        filter,
                                        &child.schema(),
                                        &orders[0],
                                    )
                                    .transpose()
                                })
                                .transpose()?;

                            filter_state.sorted_exprs.push(sorted_expr);
                        }

                        // Collect available sorted filter expressions:
                        let sorted_exprs_size = filter_state.sorted_exprs.len();
                        let mut sorted_exprs = filter_state
                            .sorted_exprs
                            .iter_mut()
                            .flatten()
                            .collect::<Vec<_>>();

                        // Create the expression graph if we can create sorted filter expressions for both children:
                        filter_state.graph = if sorted_exprs.len() == sorted_exprs_size {
                            let mut graph =
                                ExprIntervalGraph::try_new(filter.expression().clone())?;

                            // Gather filter expressions:
                            let filter_exprs = sorted_exprs
                                .iter()
                                .map(|sorted_expr| sorted_expr.filter_expr().clone())
                                .collect::<Vec<_>>();

                            // Gather node indices of converted filter expressions in `SortedFilterExpr`s
                            // using the filter columns vector:
                            let child_node_indices =
                                graph.gather_node_indices(&filter_exprs);

                            // Update SortedFilterExpr instances with the corresponding node indices:
                            for (sorted_expr, (_, index)) in
                                sorted_exprs.iter_mut().zip(child_node_indices.iter())
                            {
                                sorted_expr.set_node_index(*index);
                            }

                            Some(graph)
                        } else {
                            None
                        };
                        filter_state.calculated = true;
                    }
                    // Return the sorted filter expressions for both sides along with the expression graph:
                    (
                        filter_state.sorted_exprs[0].clone(),
                        filter_state.sorted_exprs[1].clone(),
                        filter_state.graph.as_ref().cloned(),
                    )
                }
                // If `filter_state` or `filter` is not present, then return None for all three values:
                (_, _) => (None, None, None),
            };

        let on_left = self.on.iter().map(|on| on.0.clone()).collect::<Vec<_>>();
        let on_right = self.on.iter().map(|on| on.1.clone()).collect::<Vec<_>>();

        let left_side_joiner =
            OneSideHashJoiner::new(JoinSide::Left, on_left, self.left.schema());
        let right_side_joiner =
            OneSideHashJoiner::new(JoinSide::Right, on_right, self.right.schema());

        let left_stream = self
            .left
            .execute(partition, context.clone())?
            .map(|val| (JoinSide::Left, val));

        let right_stream = self
            .right
            .execute(partition, context.clone())?
            .map(|val| (JoinSide::Right, val));
        // This function will attempt to pull items from both streams.
        // Each stream will be polled in a round-robin fashion, and whenever a stream is
        // ready to yield an item that item is yielded.
        // After one of the two input streams completes, the remaining one will be polled exclusively.
        // The returned stream completes when both input streams have completed.
        let input_stream = select(left_stream, right_stream).boxed();

        let reservation = Arc::new(Mutex::new(
            MemoryConsumer::new(format!("SymmetricHashJoinStream[{partition}]"))
                .register(context.memory_pool()),
        ));
        if let Some(g) = graph.as_ref() {
            reservation.lock().try_grow(g.size())?;
        }

        Ok(Box::pin(SymmetricHashJoinStream {
            input_stream,
            schema: self.schema(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            random_state: self.random_state.clone(),
            left: left_side_joiner,
            right: right_side_joiner,
            column_indices: self.column_indices.clone(),
            metrics: SymmetricHashJoinMetrics::new(partition, &self.metrics),
            graph,
            left_sorted_filter_expr,
            right_sorted_filter_expr,
            null_equals_null: self.null_equals_null,
            final_result: false,
            reservation,
        }))
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct SymmetricHashJoinStream {
    /// Input stream
    input_stream: BoxStream<'static, (JoinSide, Result<RecordBatch>)>,
    /// Input schema
    schema: Arc<Schema>,
    /// join filter
    filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    // left hash joiner
    left: OneSideHashJoiner,
    /// right hash joiner
    right: OneSideHashJoiner,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    // Expression graph for range pruning.
    graph: Option<ExprIntervalGraph>,
    // Left globally sorted filter expr
    left_sorted_filter_expr: Option<SortedFilterExpr>,
    // Right globally sorted filter expr
    right_sorted_filter_expr: Option<SortedFilterExpr>,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
    /// Metrics
    metrics: SymmetricHashJoinMetrics,
    /// Memory reservation
    reservation: SharedMemoryReservation,
    /// Flag indicating whether there is nothing to process anymore
    final_result: bool,
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
    ) -> Poll<Option<Self::Item>> {
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
    hashmap.shrink_if_necessary(HASHMAP_SHRINK_SCALE_FACTOR);
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
    // Calculate the interval for the build side filter expression (if present):
    update_filter_expr_interval(
        &build_input_buffer.slice(0, 1),
        build_sorted_filter_expr,
    )?;
    // Calculate the interval for the probe side filter expression (if present):
    update_filter_expr_interval(
        &probe_batch.slice(probe_batch.num_rows() - 1, 1),
        probe_sorted_filter_expr,
    )
}

/// This is a subroutine of the function [`calculate_filter_expr_intervals`].
/// It constructs the current interval using the given `batch` and updates
/// the filter expression (i.e. `sorted_expr`) with this interval.
fn update_filter_expr_interval(
    batch: &RecordBatch,
    sorted_expr: &mut SortedFilterExpr,
) -> Result<()> {
    // Evaluate the filter expression and convert the result to an array:
    let array = sorted_expr
        .origin_sorted_expr()
        .expr
        .evaluate(batch)?
        .into_array(1);
    // Convert the array to a ScalarValue:
    let value = ScalarValue::try_from_array(&array, 0)?;
    // Create a ScalarValue representing positive or negative infinity for the same data type:
    let unbounded = IntervalBound::make_unbounded(value.get_datatype())?;
    // Update the interval with lower and upper bounds based on the sort option:
    let interval = if sorted_expr.origin_sorted_expr().options.descending {
        Interval::new(unbounded, IntervalBound::new(value, false))
    } else {
        Interval::new(IntervalBound::new(value, false), unbounded)
    };
    // Set the calculated interval for the sorted filter expression:
    sorted_expr.set_interval(interval);
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
        interval.upper.value.clone()
    } else {
        interval.lower.value.clone()
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
}

impl OneSideHashJoiner {
    pub fn size(&self) -> usize {
        let mut size = 0;
        size += std::mem::size_of_val(self);
        size += std::mem::size_of_val(&self.build_side);
        size += self.input_buffer.get_array_memory_size();
        size += std::mem::size_of_val(&self.on);
        size += self.hashmap.size();
        size += self.row_hash_values.capacity() * std::mem::size_of::<u64>();
        size += self.hashes_buffer.capacity() * std::mem::size_of::<u64>();
        size += self.visited_rows.capacity() * std::mem::size_of::<usize>();
        size += std::mem::size_of_val(&self.offset);
        size += std::mem::size_of_val(&self.deleted_offset);
        size
    }
    pub fn new(build_side: JoinSide, on: Vec<Column>, schema: SchemaRef) -> Self {
        Self {
            build_side,
            input_buffer: RecordBatch::new_empty(schema),
            on,
            hashmap: JoinHashMap(RawTable::with_capacity(0)),
            row_hash_values: VecDeque::new(),
            hashes_buffer: vec![],
            visited_rows: HashSet::new(),
            offset: 0,
            deleted_offset: 0,
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
        // Get allocation_info before adding the item
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
        filter: Option<&JoinFilter>,
        probe_batch: &RecordBatch,
        probe_visited: &mut HashSet<usize>,
        probe_offset: usize,
        column_indices: &[ColumnIndex],
        random_state: &RandomState,
        null_equals_null: bool,
    ) -> Result<Option<RecordBatch>> {
        if self.input_buffer.num_rows() == 0 || probe_batch.num_rows() == 0 {
            return Ok(None);
        }
        let (build_indices, probe_indices) = build_join_indices(
            probe_batch,
            &self.hashmap,
            &self.input_buffer,
            &self.on,
            on_probe,
            filter,
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
            .map(|batch| (batch.num_rows() > 0).then_some(batch))
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
            .map(|batch| (batch.num_rows() > 0).then_some(batch))
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
    /// * `graph` - A mutable reference to the physical expression graph.
    ///
    /// # Returns
    ///
    /// If there are rows to prune, returns the pruned build side record batch wrapped in an `Ok` variant.
    /// Otherwise, returns `Ok(None)`.
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
        // Process the build and probe side sorted filter expressions if both are present:
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
        let calculated_build_side_interval = filter_intervals.remove(0).1;
        // If the intervals have not changed, return early without pruning:
        if calculated_build_side_interval.eq(build_side_sorted_filter_expr.interval()) {
            return Ok(0);
        }
        // Update the build side interval and determine the pruning length:
        build_side_sorted_filter_expr.set_interval(calculated_build_side_interval);

        determine_prune_length(&self.input_buffer, build_side_sorted_filter_expr)
    }

    fn prune_internal_state_and_build_anti_result(
        &mut self,
        prune_length: usize,
        schema: &SchemaRef,
        probe_batch: &RecordBatch,
        join_type: JoinType,
        column_indices: &[ColumnIndex],
    ) -> Result<Option<RecordBatch>> {
        // Compute the result and perform pruning if there are rows to prune:
        let result = self.build_side_determined_results(
            schema,
            prune_length,
            probe_batch.schema(),
            join_type,
            column_indices,
        );
        // Prune the hash values:
        prune_hash_values(
            prune_length,
            &mut self.hashmap,
            &mut self.row_hash_values,
            self.deleted_offset as u64,
        )?;
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
    fn size(&self) -> usize {
        let mut size = 0;
        size += std::mem::size_of_val(&self.input_stream);
        size += std::mem::size_of_val(&self.schema);
        size += std::mem::size_of_val(&self.filter);
        size += std::mem::size_of_val(&self.join_type);
        size += self.left.size();
        size += self.right.size();
        size += std::mem::size_of_val(&self.column_indices);
        size += self.graph.as_ref().map(|g| g.size()).unwrap_or(0);
        size += std::mem::size_of_val(&self.left_sorted_filter_expr);
        size += std::mem::size_of_val(&self.right_sorted_filter_expr);
        size += std::mem::size_of_val(&self.random_state);
        size += std::mem::size_of_val(&self.null_equals_null);
        size += std::mem::size_of_val(&self.metrics);
        size += std::mem::size_of_val(&self.final_result);
        size
    }
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
            // Poll the next batch from `input_stream`:
            match self.input_stream.poll_next_unpin(cx) {
                // Batch is available
                Poll::Ready(Some((side, Ok(probe_batch)))) => {
                    // Determine which stream should be polled next. The side the
                    // RecordBatch comes from becomes the probe side.
                    let (
                        probe_hash_joiner,
                        build_hash_joiner,
                        probe_side_sorted_filter_expr,
                        build_side_sorted_filter_expr,
                        probe_side_metrics,
                    ) = if side.eq(&JoinSide::Left) {
                        (
                            &mut self.left,
                            &mut self.right,
                            &mut self.left_sorted_filter_expr,
                            &mut self.right_sorted_filter_expr,
                            &mut self.metrics.left,
                        )
                    } else {
                        (
                            &mut self.right,
                            &mut self.left,
                            &mut self.right_sorted_filter_expr,
                            &mut self.left_sorted_filter_expr,
                            &mut self.metrics.right,
                        )
                    };
                    // Update the metrics for the stream that was polled:
                    probe_side_metrics.input_batches.add(1);
                    probe_side_metrics.input_rows.add(probe_batch.num_rows());
                    // Update the internal state of the hash joiner for the build side:
                    probe_hash_joiner
                        .update_internal_state(&probe_batch, &self.random_state)?;
                    // Join the two sides:
                    let equal_result = build_hash_joiner.join_with_probe_batch(
                        &self.schema,
                        self.join_type,
                        &probe_hash_joiner.on,
                        self.filter.as_ref(),
                        &probe_batch,
                        &mut probe_hash_joiner.visited_rows,
                        probe_hash_joiner.offset,
                        &self.column_indices,
                        &self.random_state,
                        self.null_equals_null,
                    )?;
                    // Increment the offset for the probe hash joiner:
                    probe_hash_joiner.offset += probe_batch.num_rows();

                    let anti_result = if let (
                        Some(build_side_sorted_filter_expr),
                        Some(probe_side_sorted_filter_expr),
                        Some(graph),
                    ) = (
                        build_side_sorted_filter_expr.as_mut(),
                        probe_side_sorted_filter_expr.as_mut(),
                        self.graph.as_mut(),
                    ) {
                        // Calculate filter intervals:
                        calculate_filter_expr_intervals(
                            &build_hash_joiner.input_buffer,
                            build_side_sorted_filter_expr,
                            &probe_batch,
                            probe_side_sorted_filter_expr,
                        )?;
                        let prune_length = build_hash_joiner
                            .calculate_prune_length_with_probe_batch(
                                build_side_sorted_filter_expr,
                                probe_side_sorted_filter_expr,
                                graph,
                            )?;

                        if prune_length > 0 {
                            build_hash_joiner.prune_internal_state_and_build_anti_result(
                                prune_length,
                                &self.schema,
                                &probe_batch,
                                self.join_type,
                                &self.column_indices,
                            )?
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Combine results:
                    let result =
                        combine_two_batches(&self.schema, equal_result, anti_result)?;
                    let capacity = self.size();
                    self.metrics.stream_memory_usage.set(capacity);
                    self.reservation.lock().try_resize(capacity)?;
                    // Update the metrics if we have a batch; otherwise, continue the loop.
                    if let Some(batch) = &result {
                        self.metrics.output_batches.add(1);
                        self.metrics.output_rows.add(batch.num_rows());
                        return Poll::Ready(Ok(result).transpose());
                    }
                }
                Poll::Ready(Some((_, Err(e)))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // If the final result has already been obtained, return `Poll::Ready(None)`:
                    if self.final_result {
                        return Poll::Ready(None);
                    }
                    self.final_result = true;
                    // Get the left side results:
                    let left_result = self.left.build_side_determined_results(
                        &self.schema,
                        self.left.input_buffer.num_rows(),
                        self.right.input_buffer.schema(),
                        self.join_type,
                        &self.column_indices,
                    )?;
                    // Get the right side results:
                    let right_result = self.right.build_side_determined_results(
                        &self.schema,
                        self.right.input_buffer.num_rows(),
                        self.left.input_buffer.schema(),
                        self.join_type,
                        &self.column_indices,
                    )?;

                    // Combine the left and right results:
                    let result =
                        combine_two_batches(&self.schema, left_result, right_result)?;

                    // Update the metrics and return the result:
                    if let Some(batch) = &result {
                        // Update the metrics:
                        self.metrics.output_batches.add(1);
                        self.metrics.output_rows.add(batch.num_rows());
                        return Poll::Ready(Ok(result).transpose());
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow::array::{ArrayRef, Float64Array, IntervalDayTimeArray};
    use arrow::array::{Int32Array, TimestampMillisecondArray};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
    use arrow::util::pretty::pretty_format_batches;
    use rstest::*;
    use tempfile::TempDir;

    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, col, Column};
    use datafusion_physical_expr::intervals::test_utils::{
        gen_conjunctive_numerical_expr, gen_conjunctive_temporal_expr,
    };
    use datafusion_physical_expr::PhysicalExpr;

    use crate::physical_plan::joins::{
        hash_join_utils::tests::complicated_filter, HashJoinExec, PartitionMode,
    };
    use crate::physical_plan::{
        common, displayable, memory::MemoryExec, repartition::RepartitionExec,
    };
    use crate::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use crate::test_util::register_unbounded_file_with_ordering;

    use super::*;

    const TABLE_SIZE: i32 = 100;

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

    async fn partitioned_sym_join_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
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

    async fn partitioned_hash_join_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
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
            filter,
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

    // It creates join filters for different type of fields for testing.
    macro_rules! join_expr_tests {
        ($func_name:ident, $type:ty, $SCALAR:ident) => {
            fn $func_name(
                expr_id: usize,
                left_col: Arc<dyn PhysicalExpr>,
                right_col: Arc<dyn PhysicalExpr>,
            ) -> Arc<dyn PhysicalExpr> {
                match expr_id {
                    // left_col + 1 > right_col + 5 AND left_col + 3 < right_col + 10
                    0 => gen_conjunctive_numerical_expr(
                        left_col,
                        right_col,
                        (
                            Operator::Plus,
                            Operator::Plus,
                            Operator::Plus,
                            Operator::Plus,
                        ),
                        ScalarValue::$SCALAR(Some(1 as $type)),
                        ScalarValue::$SCALAR(Some(5 as $type)),
                        ScalarValue::$SCALAR(Some(3 as $type)),
                        ScalarValue::$SCALAR(Some(10 as $type)),
                        (Operator::Gt, Operator::Lt),
                    ),
                    // left_col - 1 > right_col + 5 AND left_col + 3 < right_col + 10
                    1 => gen_conjunctive_numerical_expr(
                        left_col,
                        right_col,
                        (
                            Operator::Minus,
                            Operator::Plus,
                            Operator::Plus,
                            Operator::Plus,
                        ),
                        ScalarValue::$SCALAR(Some(1 as $type)),
                        ScalarValue::$SCALAR(Some(5 as $type)),
                        ScalarValue::$SCALAR(Some(3 as $type)),
                        ScalarValue::$SCALAR(Some(10 as $type)),
                        (Operator::Gt, Operator::Lt),
                    ),
                    // left_col - 1 > right_col + 5 AND left_col - 3 < right_col + 10
                    2 => gen_conjunctive_numerical_expr(
                        left_col,
                        right_col,
                        (
                            Operator::Minus,
                            Operator::Plus,
                            Operator::Minus,
                            Operator::Plus,
                        ),
                        ScalarValue::$SCALAR(Some(1 as $type)),
                        ScalarValue::$SCALAR(Some(5 as $type)),
                        ScalarValue::$SCALAR(Some(3 as $type)),
                        ScalarValue::$SCALAR(Some(10 as $type)),
                        (Operator::Gt, Operator::Lt),
                    ),
                    // left_col - 10 > right_col - 5 AND left_col - 3 < right_col + 10
                    3 => gen_conjunctive_numerical_expr(
                        left_col,
                        right_col,
                        (
                            Operator::Minus,
                            Operator::Minus,
                            Operator::Minus,
                            Operator::Plus,
                        ),
                        ScalarValue::$SCALAR(Some(10 as $type)),
                        ScalarValue::$SCALAR(Some(5 as $type)),
                        ScalarValue::$SCALAR(Some(3 as $type)),
                        ScalarValue::$SCALAR(Some(10 as $type)),
                        (Operator::Gt, Operator::Lt),
                    ),
                    // left_col - 10 > right_col - 5 AND left_col - 30 < right_col - 3
                    4 => gen_conjunctive_numerical_expr(
                        left_col,
                        right_col,
                        (
                            Operator::Minus,
                            Operator::Minus,
                            Operator::Minus,
                            Operator::Minus,
                        ),
                        ScalarValue::$SCALAR(Some(10 as $type)),
                        ScalarValue::$SCALAR(Some(5 as $type)),
                        ScalarValue::$SCALAR(Some(30 as $type)),
                        ScalarValue::$SCALAR(Some(3 as $type)),
                        (Operator::Gt, Operator::Lt),
                    ),
                    // left_col - 2 >= right_col - 5 AND left_col - 7 <= right_col - 3
                    5 => gen_conjunctive_numerical_expr(
                        left_col,
                        right_col,
                        (
                            Operator::Minus,
                            Operator::Plus,
                            Operator::Plus,
                            Operator::Minus,
                        ),
                        ScalarValue::$SCALAR(Some(2 as $type)),
                        ScalarValue::$SCALAR(Some(5 as $type)),
                        ScalarValue::$SCALAR(Some(7 as $type)),
                        ScalarValue::$SCALAR(Some(3 as $type)),
                        (Operator::GtEq, Operator::LtEq),
                    ),
                    // left_col - 28 >= right_col - 11 AND left_col - 21 <= right_col - 39
                    6 => gen_conjunctive_numerical_expr(
                        left_col,
                        right_col,
                        (
                            Operator::Plus,
                            Operator::Minus,
                            Operator::Plus,
                            Operator::Plus,
                        ),
                        ScalarValue::$SCALAR(Some(28 as $type)),
                        ScalarValue::$SCALAR(Some(11 as $type)),
                        ScalarValue::$SCALAR(Some(21 as $type)),
                        ScalarValue::$SCALAR(Some(39 as $type)),
                        (Operator::Gt, Operator::LtEq),
                    ),
                    // left_col - 28 >= right_col - 11 AND left_col - 21 <= right_col - 39
                    7 => gen_conjunctive_numerical_expr(
                        left_col,
                        right_col,
                        (
                            Operator::Plus,
                            Operator::Minus,
                            Operator::Minus,
                            Operator::Plus,
                        ),
                        ScalarValue::$SCALAR(Some(28 as $type)),
                        ScalarValue::$SCALAR(Some(11 as $type)),
                        ScalarValue::$SCALAR(Some(21 as $type)),
                        ScalarValue::$SCALAR(Some(39 as $type)),
                        (Operator::GtEq, Operator::Lt),
                    ),
                    _ => panic!("No case"),
                }
            }
        };
    }

    join_expr_tests!(join_expr_tests_fixture_i32, i32, Int32);
    join_expr_tests!(join_expr_tests_fixture_f64, f64, Float64);

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::iter::Iterator;

    struct AscendingRandomFloatIterator {
        prev: f64,
        max: f64,
        rng: StdRng,
    }

    impl AscendingRandomFloatIterator {
        fn new(min: f64, max: f64) -> Self {
            let mut rng = StdRng::seed_from_u64(42);
            let initial = rng.gen_range(min..max);
            AscendingRandomFloatIterator {
                prev: initial,
                max,
                rng,
            }
        }
    }

    impl Iterator for AscendingRandomFloatIterator {
        type Item = f64;

        fn next(&mut self) -> Option<Self::Item> {
            let value = self.rng.gen_range(self.prev..self.max);
            self.prev = value;
            Some(value)
        }
    }

    fn join_expr_tests_fixture_temporal(
        expr_id: usize,
        left_col: Arc<dyn PhysicalExpr>,
        right_col: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match expr_id {
            // constructs ((left_col - INTERVAL '100ms')  > (right_col - INTERVAL '200ms')) AND ((left_col - INTERVAL '450ms') < (right_col - INTERVAL '300ms'))
            0 => gen_conjunctive_temporal_expr(
                left_col,
                right_col,
                Operator::Minus,
                Operator::Minus,
                Operator::Minus,
                Operator::Minus,
                ScalarValue::new_interval_dt(0, 100), // 100 ms
                ScalarValue::new_interval_dt(0, 200), // 200 ms
                ScalarValue::new_interval_dt(0, 450), // 450 ms
                ScalarValue::new_interval_dt(0, 300), // 300 ms
                schema,
            ),
            // constructs ((left_col - TIMESTAMP '2023-01-01:12.00.03')  > (right_col - TIMESTAMP '2023-01-01:12.00.01')) AND ((left_col - TIMESTAMP '2023-01-01:12.00.00') < (right_col - TIMESTAMP '2023-01-01:12.00.02'))
            1 => gen_conjunctive_temporal_expr(
                left_col,
                right_col,
                Operator::Minus,
                Operator::Minus,
                Operator::Minus,
                Operator::Minus,
                ScalarValue::TimestampMillisecond(Some(1672574403000), None), // 2023-01-01:12.00.03
                ScalarValue::TimestampMillisecond(Some(1672574401000), None), // 2023-01-01:12.00.01
                ScalarValue::TimestampMillisecond(Some(1672574400000), None), // 2023-01-01:12.00.00
                ScalarValue::TimestampMillisecond(Some(1672574402000), None), // 2023-01-01:12.00.02
                schema,
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
        let cardinality_key_left = Arc::new(Int32Array::from_iter(
            initial_range
                .clone()
                .map(|x| x % key_cardinality.0)
                .collect::<Vec<i32>>(),
        ));
        let cardinality_key_right = Arc::new(Int32Array::from_iter(
            initial_range
                .clone()
                .map(|x| x % key_cardinality.1)
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

        let time = Arc::new(TimestampMillisecondArray::from(
            initial_range
                .clone()
                .map(|x| x as i64 + 1672531200000) // x + 2023-01-01:00.00.00
                .collect::<Vec<i64>>(),
        ));
        let interval_time: ArrayRef = Arc::new(IntervalDayTimeArray::from(
            initial_range
                .map(|x| x as i64 * 100) // x * 100ms
                .collect::<Vec<i64>>(),
        ));

        let float_asc = Arc::new(Float64Array::from_iter_values(
            AscendingRandomFloatIterator::new(0., table_size as f64)
                .take(table_size as usize),
        ));

        let left = RecordBatch::try_from_iter(vec![
            ("la1", ordered.clone()),
            ("lb1", cardinality.clone()),
            ("lc1", cardinality_key_left),
            ("lt1", time.clone()),
            ("la2", ordered.clone()),
            ("la1_des", ordered_des.clone()),
            ("l_asc_null_first", ordered_asc_null_first.clone()),
            ("l_asc_null_last", ordered_asc_null_last.clone()),
            ("l_desc_null_first", ordered_desc_null_first.clone()),
            ("li1", interval_time.clone()),
            ("l_float", float_asc.clone()),
        ])?;
        let right = RecordBatch::try_from_iter(vec![
            ("ra1", ordered.clone()),
            ("rb1", cardinality),
            ("rc1", cardinality_key_right),
            ("rt1", time),
            ("ra2", ordered),
            ("ra1_des", ordered_des),
            ("r_asc_null_first", ordered_asc_null_first),
            ("r_asc_null_last", ordered_asc_null_last),
            ("r_desc_null_first", ordered_desc_null_first),
            ("ri1", interval_time),
            ("r_float", float_asc),
        ])?;
        Ok((left, right))
    }

    fn create_memory_table(
        left_batch: RecordBatch,
        right_batch: RecordBatch,
        left_sorted: Option<Vec<PhysicalSortExpr>>,
        right_sorted: Option<Vec<PhysicalSortExpr>>,
        batch_size: usize,
    ) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
        let mut left = MemoryExec::try_new(
            &[split_record_batches(&left_batch, batch_size)?],
            left_batch.schema(),
            None,
        )?;
        if let Some(sorted) = left_sorted {
            left = left.with_sort_information(sorted);
        }
        let mut right = MemoryExec::try_new(
            &[split_record_batches(&right_batch, batch_size)?],
            right_batch.schema(),
            None,
        )?;
        if let Some(sorted) = right_sorted {
            right = right.with_sort_information(sorted);
        }
        Ok((Arc::new(left), Arc::new(right)))
    }

    async fn experiment(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filter: Option<JoinFilter>,
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

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
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

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn join_without_sort_information(
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
        let (left, right) = create_memory_table(left_batch, right_batch, None, None, 13)?;

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

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn join_without_filter(
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
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) = build_sides_record_batches(TABLE_SIZE, (11, 21))?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let (left, right) = create_memory_table(left_batch, right_batch, None, None, 13)?;

        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];
        experiment(left, right, None, join_type, on, task_ctx).await?;
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

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }

    #[tokio::test]
    async fn join_change_in_planner() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::with_config(config);
        let tmp_dir = TempDir::new().unwrap();
        let left_file_path = tmp_dir.path().join("left.csv");
        File::create(left_file_path.clone()).unwrap();
        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::UInt32, false),
            Field::new("a2", DataType::UInt32, false),
        ]));
        // Specify the ordering:
        let file_sort_order = vec![[datafusion_expr::col("a1")]
            .into_iter()
            .map(|e| {
                let ascending = true;
                let nulls_first = false;
                e.sort(ascending, nulls_first)
            })
            .collect::<Vec<_>>()];
        register_unbounded_file_with_ordering(
            &ctx,
            schema.clone(),
            &left_file_path,
            "left",
            file_sort_order.clone(),
            true,
        )
        .await?;
        let right_file_path = tmp_dir.path().join("right.csv");
        File::create(right_file_path.clone()).unwrap();
        register_unbounded_file_with_ordering(
            &ctx,
            schema,
            &right_file_path,
            "right",
            file_sort_order,
            true,
        )
        .await?;
        let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            [
                "SymmetricHashJoinExec: join_type=Full, on=[(Column { name: \"a2\", index: 1 }, Column { name: \"a2\", index: 1 })], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10",
                "  CoalesceBatchesExec: target_batch_size=8192",
                "    RepartitionExec: partitioning=Hash([Column { name: \"a2\", index: 1 }], 8), input_partitions=1",
                // "   CsvExec: file_groups={1 group: [[tempdir/left.csv]]}, projection=[a1, a2], has_header=false",
                "  CoalesceBatchesExec: target_batch_size=8192",
                "    RepartitionExec: partitioning=Hash([Column { name: \"a2\", index: 1 }], 8), input_partitions=1",
                // "   CsvExec: file_groups={1 group: [[tempdir/right.csv]]}, projection=[a1, a2], has_header=false"
            ]
        };
        let mut actual: Vec<&str> = formatted.trim().lines().collect();
        // Remove CSV lines
        actual.remove(3);
        actual.remove(5);

        assert_eq!(
            expected,
            actual[..],
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn join_change_in_planner_without_sort() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::with_config(config);
        let tmp_dir = TempDir::new()?;
        let left_file_path = tmp_dir.path().join("left.csv");
        File::create(left_file_path.clone())?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::UInt32, false),
            Field::new("a2", DataType::UInt32, false),
        ]));
        ctx.register_csv(
            "left",
            left_file_path.as_os_str().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema).mark_infinite(true),
        )
        .await?;
        let right_file_path = tmp_dir.path().join("right.csv");
        File::create(right_file_path.clone())?;
        ctx.register_csv(
            "right",
            right_file_path.as_os_str().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema).mark_infinite(true),
        )
        .await?;
        let sql = "SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10";
        let dataframe = ctx.sql(sql).await?;
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            [
                "SymmetricHashJoinExec: join_type=Full, on=[(Column { name: \"a2\", index: 1 }, Column { name: \"a2\", index: 1 })], filter=CAST(a1@0 AS Int64) > CAST(a1@1 AS Int64) + 3 AND CAST(a1@0 AS Int64) < CAST(a1@1 AS Int64) + 10",
                "  CoalesceBatchesExec: target_batch_size=8192",
                "    RepartitionExec: partitioning=Hash([Column { name: \"a2\", index: 1 }], 8), input_partitions=1",
                // "   CsvExec: file_groups={1 group: [[tempdir/left.csv]]}, projection=[a1, a2], has_header=false",
                "  CoalesceBatchesExec: target_batch_size=8192",
                "    RepartitionExec: partitioning=Hash([Column { name: \"a2\", index: 1 }], 8), input_partitions=1",
                // "   CsvExec: file_groups={1 group: [[tempdir/right.csv]]}, projection=[a1, a2], has_header=false"
            ]
        };
        let mut actual: Vec<&str> = formatted.trim().lines().collect();
        // Remove CSV lines
        actual.remove(3);
        actual.remove(5);

        assert_eq!(
            expected,
            actual[..],
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn join_change_in_planner_without_sort_not_allowed() -> Result<()> {
        let config = SessionConfig::new()
            .with_target_partitions(8)
            .with_allow_symmetric_joins_without_pruning(false);
        let ctx = SessionContext::with_config(config);
        let tmp_dir = TempDir::new()?;
        let left_file_path = tmp_dir.path().join("left.csv");
        File::create(left_file_path.clone())?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::UInt32, false),
            Field::new("a2", DataType::UInt32, false),
        ]));
        ctx.register_csv(
            "left",
            left_file_path.as_os_str().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema).mark_infinite(true),
        )
        .await?;
        let right_file_path = tmp_dir.path().join("right.csv");
        File::create(right_file_path.clone())?;
        ctx.register_csv(
            "right",
            right_file_path.as_os_str().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema).mark_infinite(true),
        )
        .await?;
        let df = ctx.sql("SELECT t1.a1, t1.a2, t2.a1, t2.a2 FROM left as t1 FULL JOIN right as t2 ON t1.a2 = t2.a2 AND t1.a1 > t2.a1 + 3 AND t1.a1 < t2.a1 + 10").await?;
        match df.create_physical_plan().await {
            Ok(_) => panic!("Expecting error."),
            Err(e) => {
                assert_eq!(e.to_string(), "PipelineChecker\ncaused by\nError during planning: Join operation cannot operate on a non-prunable stream without enabling the 'allow_symmetric_joins_without_pruning' configuration flag")
            }
        }
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
                index: 6,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 6,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);
        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
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

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
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

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
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

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
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
        let (left, right) = create_memory_table(
            left_batch,
            right_batch,
            Some(left_sorted),
            Some(right_sorted),
            10,
        )?;

        // Filter columns, ensure first batches will have matching rows.
        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
        ]);
        let filter_expr = gen_conjunctive_numerical_expr(
            col("0", &intermediate_schema)?,
            col("1", &intermediate_schema)?,
            (
                Operator::Plus,
                Operator::Minus,
                Operator::Plus,
                Operator::Plus,
            ),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int32(Some(3)),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int32(Some(3)),
            (Operator::Gt, Operator::Lt),
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

        let mut left_side_joiner = OneSideHashJoiner::new(
            JoinSide::Left,
            vec![Column::new_with_schema("lc1", &left_schema)?],
            left_schema,
        );

        let mut right_side_joiner = OneSideHashJoiner::new(
            JoinSide::Right,
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
            Some(&filter),
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
    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn testing_with_temporal_columns(
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
               (99, 12),
               )]
        cardinality: (i32, i32),
        #[values(0, 1)] case_expr: usize,
    ) -> Result<()> {
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("lt1", left_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("rt1", right_schema)?,
            options: SortOptions {
                descending: false,
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
            Field::new(
                "left",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "right",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]);
        let filter_expr = join_expr_tests_fixture_temporal(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
            &intermediate_schema,
        )?;
        let column_indices = vec![
            ColumnIndex {
                index: 3,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 3,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);
        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }
    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_with_interval_columns(
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
        (99, 12),
        )]
        cardinality: (i32, i32),
    ) -> Result<()> {
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let on = vec![(
            Column::new_with_schema("lc1", left_schema)?,
            Column::new_with_schema("rc1", right_schema)?,
        )];
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("li1", left_schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("ri1", right_schema)?,
            options: SortOptions {
                descending: false,
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
            Field::new("left", DataType::Interval(IntervalUnit::DayTime), false),
            Field::new("right", DataType::Interval(IntervalUnit::DayTime), false),
        ]);
        let filter_expr = join_expr_tests_fixture_temporal(
            0,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
            &intermediate_schema,
        )?;
        let column_indices = vec![
            ColumnIndex {
                index: 9,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 9,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);
        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;

        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn testing_ascending_float_pruning(
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
        (99, 12),
        )]
        cardinality: (i32, i32),
        #[values(0, 1, 2, 3, 4, 5, 6, 7)] case_expr: usize,
    ) -> Result<()> {
        let config = SessionConfig::new().with_repartition_joins(false);
        let session_ctx = SessionContext::with_config(config);
        let task_ctx = session_ctx.task_ctx();
        let (left_batch, right_batch) =
            build_sides_record_batches(TABLE_SIZE, cardinality)?;
        let left_schema = &left_batch.schema();
        let right_schema = &right_batch.schema();
        let left_sorted = vec![PhysicalSortExpr {
            expr: col("l_float", left_schema)?,
            options: SortOptions::default(),
        }];
        let right_sorted = vec![PhysicalSortExpr {
            expr: col("r_float", right_schema)?,
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
            Field::new("left", DataType::Float64, true),
            Field::new("right", DataType::Float64, true),
        ]);
        let filter_expr = join_expr_tests_fixture_f64(
            case_expr,
            col("left", &intermediate_schema)?,
            col("right", &intermediate_schema)?,
        );
        let column_indices = vec![
            ColumnIndex {
                index: 10, // l_float
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 10, // r_float
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        experiment(left, right, Some(filter), join_type, on, task_ctx).await?;
        Ok(())
    }
}
