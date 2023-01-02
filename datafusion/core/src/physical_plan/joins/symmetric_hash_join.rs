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

//! Defines the join plan for executing partitions in parallel and then joining the results
//! into a set of partitions.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::task::Poll;
use std::vec;
use std::{any::Any, usize};

use ahash::RandomState;
use arrow::array::PrimitiveArray;
use arrow::array::{ArrowPrimitiveType, NativeAdapter, PrimitiveBuilder};
use arrow::compute::{concat_batches, SortOptions};
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use hashbrown::raw::RawTable;
use hashbrown::HashSet;
use itertools::Itertools;

use datafusion_common::bisect::bisect;
use datafusion_common::ScalarValue;
use datafusion_physical_expr::intervals::interval_aritmetics::{Interval, Range};
use datafusion_physical_expr::intervals::ExprIntervalGraph;

use crate::arrow::array::BooleanBufferBuilder;
use crate::error::{DataFusionError, Result};
use crate::execution::context::TaskContext;
use crate::logical_expr::JoinType;
use crate::physical_plan::common::merge_batches;
use crate::physical_plan::joins::hash_join_utils::update_hash;
use crate::physical_plan::joins::hash_join_utils::{build_join_indices, JoinHashMap};
use crate::physical_plan::joins::utils::build_batch_from_indices;
use crate::physical_plan::{
    expressions::Column,
    expressions::PhysicalSortExpr,
    joins::utils::{
        build_join_schema, check_join_is_valid, combine_join_equivalence_properties,
        partitioned_join_output_partitioning, ColumnIndex, JoinFilter, JoinOn, JoinSide,
    },
    metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
    DisplayFormatType, Distribution, EquivalenceProperties, ExecutionPlan, Partitioning,
    PhysicalExpr, RecordBatchStream, SendableRecordBatchStream, Statistics,
};

#[derive(Debug, Clone)]
/// The SortedFilterExpr struct is used to represent a sorted filter expression in the
/// [SymmetricHashJoinExec] struct. It contains information about the join side, the origin
/// expression, the filter expression, and the sort option.
/// The struct has several methods to access and modify its fields.
pub struct SortedFilterExpr {
    // Column side
    join_side: JoinSide,
    // Sorted expr from a particular join side (child)
    origin_expr: Arc<dyn PhysicalExpr>,
    // For interval calculations, one to one mapping of the columns according to filter expression,
    // and column indices.
    filter_expr: Arc<dyn PhysicalExpr>,
    // Sort option
    sort_option: SortOptions,
    // Interval
    interval: Option<Interval>,
}

impl SortedFilterExpr {
    /// Constructor
    pub fn new(
        join_side: JoinSide,
        origin_expr: Arc<dyn PhysicalExpr>,
        filter_expr: Arc<dyn PhysicalExpr>,
        sort_option: SortOptions,
    ) -> Self {
        Self {
            join_side,
            origin_expr,
            filter_expr,
            sort_option,
            interval: None,
        }
    }
    /// Get origin expr information
    pub fn origin_expr(&self) -> Arc<dyn PhysicalExpr> {
        self.origin_expr.clone()
    }
    /// Get filter expr information
    pub fn filter_expr(&self) -> Arc<dyn PhysicalExpr> {
        self.filter_expr.clone()
    }
    /// Get sort information
    pub fn sort_option(&self) -> SortOptions {
        self.sort_option
    }
    /// Get interval information
    pub fn interval(&self) -> &Option<Interval> {
        &self.interval
    }
    /// Sets interval
    pub fn set_interval(&mut self, interval: Option<Interval>) {
        self.interval = interval;
    }
}

/// The symmetric hash join is a special type of hash join designed for data streams and large
/// datasets.
/// For each input, create a hash table.
///   - For each new record, hash and insert into inputs hash table.
///   - Test if input is equal to a predefined set of other inputs.
///   - If so, output the records, record the visited rows.
///   - If the join type indicates that unmatched rows results must be produced (LEFT, FULL etc.),
/// produce result if a pruning happens or at the end of the data.
pub struct SymmetricHashJoinExec {
    /// left side stream
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// right side stream
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    pub(crate) on: Vec<(Column, Column)>,
    /// Filters which are applied while finding matching rows
    pub(crate) filter: JoinFilter,
    /// How the join is performed
    pub(crate) join_type: JoinType,
    /// Order information of filter columns
    filter_columns: Vec<SortedFilterExpr>,
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

/// Metrics for HashJoinExec
#[derive(Debug)]
struct SymmetricHashJoinMetrics {
    /// Number of left batches consumed by this operator
    left_input_batches: metrics::Count,
    /// Number of right batches consumed by this operator
    right_input_batches: metrics::Count,
    /// Number of left rows consumed by this operator
    left_input_rows: metrics::Count,
    /// Number of right rows consumed by this operator
    right_input_rows: metrics::Count,
    /// Number of batches produced by this operator
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
}

impl SymmetricHashJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let left_input_batches =
            MetricBuilder::new(metrics).counter("left_input_batches", partition);
        let right_input_batches =
            MetricBuilder::new(metrics).counter("right_input_batches", partition);

        let left_input_rows =
            MetricBuilder::new(metrics).counter("left_input_rows", partition);

        let right_input_rows =
            MetricBuilder::new(metrics).counter("right_input_rows", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            left_input_batches,
            right_input_batches,
            left_input_rows,
            right_input_rows,
            output_batches,
            output_rows,
        }
    }
}

impl SymmetricHashJoinExec {
    /// Tries to create a new [SymmetricHashJoinExec].
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    /// TODO: Support for CollectLeft (or CorrectRight options.)
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        filter_columns: Vec<SortedFilterExpr>,
        join_type: &JoinType,
        null_equals_null: &bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if on.is_empty() {
            return Err(DataFusionError::Plan(
                "On constraints in HashJoinExec should be non-empty".to_string(),
            ));
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        Ok(SymmetricHashJoinExec {
            left,
            right,
            on,
            filter,
            filter_columns,
            join_type: *join_type,
            schema: Arc::new(schema),
            random_state,
            metrics: ExecutionPlanMetricsSet::new(),
            column_indices,
            null_equals_null: *null_equals_null,
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
    pub fn null_equals_null(&self) -> &bool {
        &self.null_equals_null
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
        vec![]
    }

    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        let (left, right) = (children[0], children[1]);
        Ok(left || right)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let (left_expr, right_expr) = self
            .on
            .iter()
            .map(|(l, r)| {
                (
                    Arc::new(l.clone()) as Arc<dyn PhysicalExpr>,
                    Arc::new(r.clone()) as Arc<dyn PhysicalExpr>,
                )
            })
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
    // TODO Output ordering might be kept for some cases.
    // For example if it is inner join then the stream side order can be kept
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
            self.filter_columns.clone(),
            &self.join_type,
            &self.null_equals_null,
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
        // TODO: Currently, working on partitioned left and right. We can also coalesce.
        let left_side_joiner = OneSideHashJoiner::new(JoinSide::Left, self.left.clone());
        let right_side_joiner =
            OneSideHashJoiner::new(JoinSide::Right, self.right.clone());
        // TODO: Discuss unbounded mpsc and bounded one.
        let left_stream = self.left.execute(partition, context.clone())?;
        let right_stream = self.right.execute(partition, context)?;
        let physical_expr_graph = ExprIntervalGraph::try_new(
            self.filter.expression().clone(),
            &self
                .filter_columns
                .iter()
                .map(|sorted_expr| sorted_expr.filter_expr())
                .collect_vec(),
        )?;

        Ok(Box::pin(SymmetricHashJoinStream {
            left_stream,
            right_stream,
            schema: self.schema(),
            on_left,
            on_right,
            filter: self.filter.clone(),
            join_type: self.join_type,
            random_state: self.random_state.clone(),
            left: left_side_joiner,
            right: right_side_joiner,
            column_indices: self.column_indices.clone(),
            join_metrics: SymmetricHashJoinMetrics::new(partition, &self.metrics),
            physical_expr_graph,
            null_equals_null: self.null_equals_null,
            filter_columns: self.filter_columns.clone(),
            final_result: false,
            data_side: JoinSide::Left,
        }))
    }
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct SymmetricHashJoinStream {
    left_stream: SendableRecordBatchStream,
    right_stream: SendableRecordBatchStream,
    /// Input schema
    schema: Arc<Schema>,
    /// columns from the left
    on_left: Vec<Column>,
    /// columns from the right used to compute the hash
    on_right: Vec<Column>,
    /// join filter
    filter: JoinFilter,
    /// type of the join
    join_type: JoinType,
    // left
    left: OneSideHashJoiner,
    /// right
    right: OneSideHashJoiner,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    // Range Prunner.
    physical_expr_graph: ExprIntervalGraph,
    /// Information of filter columns
    filter_columns: Vec<SortedFilterExpr>,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
    /// Metrics
    join_metrics: SymmetricHashJoinMetrics,
    /// There is nothing to process anymore
    final_result: bool,
    data_side: JoinSide,
}

impl RecordBatchStream for SymmetricHashJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SymmetricHashJoinStream {
    type Item = ArrowResult<RecordBatch>;

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
            .get_mut(*hash_value, |(hash, _)| *hash_value == *hash)
        {
            separation_chain.retain(|n| !index_set.contains(n));
            if separation_chain.is_empty() {
                hashmap
                    .0
                    .remove_entry(*hash_value, |(hash, _)| *hash_value == *hash);
            }
        }
    }
    Ok(())
}

fn prune_visited_rows(
    prune_length: usize,
    visited_rows: &mut HashSet<usize>,
    deleted_offset: usize,
) -> Result<()> {
    (deleted_offset..(deleted_offset + prune_length)).for_each(|row| {
        visited_rows.remove(&row);
    });
    Ok(())
}

/// We can prune build side when a probe batch comes.
fn column_stats_two_side(
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    filter_columns: &mut [SortedFilterExpr],
    build_side: JoinSide,
) -> Result<()> {
    for sorted_expr in filter_columns.iter_mut() {
        let SortedFilterExpr {
            join_side,
            origin_expr,
            sort_option,
            interval,
            ..
        } = sorted_expr;
        let array = if build_side.eq(join_side) {
            // Get first value for expr
            origin_expr
                .evaluate(&build_input_buffer.slice(0, 1))?
                .into_array(1)
        } else {
            // Get last value for expr
            origin_expr
                .evaluate(&probe_batch.slice(probe_batch.num_rows() - 1, 1))?
                .into_array(1)
        };
        let value = ScalarValue::try_from_array(&array, 0)?;
        let infinite = ScalarValue::try_from(value.get_datatype())?;
        *interval = Some(if sort_option.descending {
            Interval::Range(Range {
                lower: infinite,
                upper: value,
            })
        } else {
            Interval::Range(Range {
                lower: value,
                upper: infinite,
            })
        });
    }
    Ok(())
}

fn determine_prune_length(
    buffer: &RecordBatch,
    filter_columns: &[SortedFilterExpr],
    build_side: JoinSide,
) -> Result<usize> {
    Ok(filter_columns
        .iter()
        .flat_map(|sorted_expr| {
            let SortedFilterExpr {
                join_side,
                origin_expr,
                sort_option,
                interval,
                ..
            } = sorted_expr;
            if build_side.eq(join_side) {
                let batch_arr = origin_expr
                    .evaluate(buffer)
                    .unwrap()
                    .into_array(buffer.num_rows());
                let target = if sort_option.descending {
                    interval.as_ref()?.upper_value()
                } else {
                    interval.as_ref()?.lower_value()
                };
                Some(bisect::<true>(&[batch_arr], &[target], &[*sort_option]))
            } else {
                None
            }
        })
        .collect::<Vec<Result<usize>>>()
        .into_iter()
        .collect::<Result<Vec<usize>>>()?
        .into_iter()
        .min()
        .unwrap())
}

fn need_produce_result_in_final(build_side: JoinSide, join_type: JoinType) -> bool {
    if build_side.eq(&JoinSide::Left) {
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
    (0..prune_length).for_each(|v| {
        let row = &(v + deleted_offset);
        bitmap.set_bit(v, visited_rows.contains(row));
    });
    // get the anti index
    (0..prune_length)
        .filter_map(|idx| (!bitmap.get_bit(idx)).then_some(T::Native::from_usize(idx)))
        .collect::<PrimitiveArray<T>>()
}

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
    (0..prune_length).for_each(|v| {
        let row = &(v + deleted_offset);
        bitmap.set_bit(v, visited_rows.contains(row));
    });
    // get the semi index
    (0..prune_length)
        .filter_map(|idx| (bitmap.get_bit(idx)).then_some(T::Native::from_usize(idx)))
        .collect::<PrimitiveArray<T>>()
}

fn record_visited_indices<T: ArrowPrimitiveType>(
    visited: &mut HashSet<usize>,
    offset: usize,
    indices: &PrimitiveArray<T>,
) {
    let batch_indices: &[T::Native] = indices.values();
    for i in batch_indices {
        visited.insert(i.as_usize() + offset);
    }
}

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
    let result = match (build_side, join_type) {
        (JoinSide::Left, JoinType::Left | JoinType::LeftAnti)
        | (JoinSide::Right, JoinType::Right | JoinType::RightAnti)
        | (_, JoinType::Full) => {
            let build_unmatched_indices =
                get_anti_indices(prune_length, deleted_offset, visited_rows);
            // right_indices
            // all the element in the right side is None
            let mut builder =
                PrimitiveBuilder::<R>::with_capacity(build_unmatched_indices.len());
            builder.append_nulls(build_unmatched_indices.len());
            let probe_indices = builder.finish();
            (build_unmatched_indices, probe_indices)
        }
        (JoinSide::Left, JoinType::LeftSemi) | (JoinSide::Right, JoinType::RightSemi) => {
            let build_unmatched_indices =
                get_semi_indices(prune_length, deleted_offset, visited_rows);
            let mut builder =
                PrimitiveBuilder::<R>::with_capacity(build_unmatched_indices.len());
            builder.append_nulls(build_unmatched_indices.len());
            let probe_indices = builder.finish();
            (build_unmatched_indices, probe_indices)
        }
        _ => unreachable!(),
    };
    Ok(result)
}

struct OneSideHashJoiner {
    // Build side
    build_side: JoinSide,
    // Inout record batch buffer
    input_buffer: RecordBatch,
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
    pub fn new(build_side: JoinSide, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            build_side,
            input_buffer: RecordBatch::new_empty(plan.schema()),
            hashmap: JoinHashMap(RawTable::with_capacity(10_000)),
            row_hash_values: VecDeque::new(),
            hashes_buffer: vec![],
            visited_rows: HashSet::new(),
            offset: 0,
            deleted_offset: 0,
            exhausted: false,
        }
    }

    fn update_internal_state(
        &mut self,
        on_build: &[Column],
        batch: &RecordBatch,
        random_state: &RandomState,
    ) -> Result<()> {
        self.input_buffer =
            merge_batches(&self.input_buffer, batch, batch.schema()).unwrap();
        self.hashes_buffer.resize(batch.num_rows(), 0);
        update_hash(
            on_build,
            batch,
            &mut self.hashmap,
            self.offset,
            random_state,
            &mut self.hashes_buffer,
        )?;
        self.hashes_buffer
            .drain(0..)
            .into_iter()
            .for_each(|hash| self.row_hash_values.push_back(hash));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_batch_from_other_side(
        &mut self,
        schema: SchemaRef,
        join_type: JoinType,
        on_build: &[Column],
        on_probe: &[Column],
        filter: Option<&JoinFilter>,
        probe_batch: &RecordBatch,
        probe_visited: &mut HashSet<usize>,
        probe_offset: usize,
        column_indices: &[ColumnIndex],
        random_state: &RandomState,
        null_equals_null: &bool,
    ) -> ArrowResult<Option<RecordBatch>> {
        if self.input_buffer.num_rows() == 0 {
            return Ok(Some(RecordBatch::new_empty(schema)));
        }
        let (build_side, probe_side) = build_join_indices(
            probe_batch,
            &self.hashmap,
            &self.input_buffer,
            on_build,
            on_probe,
            filter,
            random_state,
            null_equals_null,
            &mut self.hashes_buffer,
            Some(self.deleted_offset),
            self.build_side,
        )?;
        if need_produce_result_in_final(self.build_side, join_type) {
            record_visited_indices(
                &mut self.visited_rows,
                self.deleted_offset,
                &build_side,
            );
        }
        if need_produce_result_in_final(self.build_side.negate(), join_type) {
            record_visited_indices(probe_visited, probe_offset, &probe_side);
        }
        match (self.build_side, join_type) {
            (
                _,
                JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::LeftSemi
                | JoinType::RightSemi,
            ) => Ok(None),
            (_, _) => {
                let res = build_batch_from_indices(
                    schema.as_ref(),
                    &self.input_buffer,
                    probe_batch,
                    build_side.clone(),
                    probe_side.clone(),
                    column_indices,
                    self.build_side,
                )?;
                Ok(Some(res))
            }
        }
    }

    fn build_side_determined_results(
        &self,
        output_schema: SchemaRef,
        prune_length: usize,
        probe_schema: SchemaRef,
        join_type: JoinType,
        column_indices: &[ColumnIndex],
    ) -> ArrowResult<Option<RecordBatch>> {
        let result = if need_produce_result_in_final(self.build_side, join_type) {
            let (build_indices, probe_indices) = calculate_indices_by_join_type(
                self.build_side,
                prune_length,
                &self.visited_rows,
                self.deleted_offset,
                join_type,
            )?;
            let empty_probe_batch = RecordBatch::new_empty(probe_schema);
            Some(build_batch_from_indices(
                output_schema.as_ref(),
                &self.input_buffer,
                &empty_probe_batch,
                build_indices,
                probe_indices,
                column_indices,
                self.build_side,
            )?)
        } else {
            None
        };
        Ok(result)
    }

    fn prune_build_side(
        &mut self,
        schema: SchemaRef,
        probe_batch: &RecordBatch,
        filter_columns: &mut [SortedFilterExpr],
        join_type: JoinType,
        column_indices: &[ColumnIndex],
        physical_expr_graph: &mut ExprIntervalGraph,
    ) -> ArrowResult<Option<RecordBatch>> {
        if self.input_buffer.num_rows() == 0 {
            return Ok(None);
        }
        column_stats_two_side(
            &self.input_buffer,
            probe_batch,
            filter_columns,
            self.build_side,
        )?;
        let mut filter_intervals: Vec<(Arc<dyn PhysicalExpr>, Interval)> = filter_columns
            .iter()
            .map(|sorted_expr| {
                (
                    sorted_expr.filter_expr(),
                    sorted_expr.interval().as_ref().unwrap().clone(),
                )
            })
            .collect_vec();
        physical_expr_graph.calculate_new_intervals(&mut filter_intervals)?;
        for (sorted_expr, (_, interval)) in
            filter_columns.iter_mut().zip(filter_intervals.into_iter())
        {
            sorted_expr.set_interval(Some(interval.clone()))
        }

        let prune_length =
            determine_prune_length(&self.input_buffer, filter_columns, self.build_side)?;
        if prune_length > 0 {
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
            prune_visited_rows(
                prune_length,
                &mut self.visited_rows,
                self.deleted_offset,
            )?;
            self.input_buffer = self
                .input_buffer
                .slice(prune_length, self.input_buffer.num_rows() - prune_length);
            self.deleted_offset += prune_length;
            result
        } else {
            Ok(None)
        }
    }
}

fn produce_batch_result(
    output_schema: SchemaRef,
    equal_batch: ArrowResult<Option<RecordBatch>>,
    anti_batch: ArrowResult<Option<RecordBatch>>,
) -> ArrowResult<RecordBatch> {
    match (equal_batch, anti_batch) {
        (Ok(Some(batch)), Ok(None)) | (Ok(None), Ok(Some(batch))) => Ok(batch),
        (Err(e), _) | (_, Err(e)) => Err(e),
        (Ok(Some(equal_batch)), Ok(Some(anti_batch))) => {
            concat_batches(&output_schema, &[equal_batch, anti_batch])
        }
        (Ok(None), Ok(None)) => Ok(RecordBatch::new_empty(output_schema)),
    }
}

impl SymmetricHashJoinStream {
    /// Separate implementation function that unpins the [`SymmetricHashJoinStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        if self.final_result {
            return Poll::Ready(None);
        }
        if self.right.exhausted && self.left.exhausted {
            let left_result = self.left.build_side_determined_results(
                self.schema.clone(),
                self.left.input_buffer.num_rows(),
                self.right.input_buffer.schema(),
                self.join_type,
                &self.column_indices,
            );
            let right_result = self.right.build_side_determined_results(
                self.schema.clone(),
                self.right.input_buffer.num_rows(),
                self.left.input_buffer.schema(),
                self.join_type,
                &self.column_indices,
            );
            self.final_result = true;
            let result =
                produce_batch_result(self.schema.clone(), left_result, right_result);
            if let Ok(batch) = &result {
                self.join_metrics.output_batches.add(1);
                self.join_metrics.output_rows.add(batch.num_rows());
            }
            return Poll::Ready(Some(result));
        }
        if self.data_side.eq(&JoinSide::Left) {
            match self.left_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(probe_batch))) => {
                    self.join_metrics.left_input_batches.add(1);
                    self.join_metrics
                        .left_input_rows
                        .add(probe_batch.num_rows());
                    match self.left.update_internal_state(
                        &self.on_left,
                        &probe_batch,
                        &self.random_state,
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            return Poll::Ready(Some(Err(ArrowError::ComputeError(
                                e.to_string(),
                            ))))
                        }
                    }
                    // Using right as build side.
                    let equal_result = self.right.record_batch_from_other_side(
                        self.schema.clone(),
                        self.join_type,
                        &self.on_right,
                        &self.on_left,
                        Some(&self.filter),
                        &probe_batch,
                        &mut self.left.visited_rows,
                        self.left.offset,
                        &self.column_indices,
                        &self.random_state,
                        &self.null_equals_null,
                    );
                    self.left.offset += probe_batch.num_rows();
                    // Right side will be pruned since the batch coming from left.
                    let anti_result = self.right.prune_build_side(
                        self.schema.clone(),
                        &probe_batch,
                        &mut self.filter_columns,
                        self.join_type,
                        &self.column_indices,
                        &mut self.physical_expr_graph,
                    );
                    let result = produce_batch_result(
                        self.schema.clone(),
                        equal_result,
                        anti_result,
                    );
                    if let Ok(batch) = &result {
                        self.join_metrics.output_batches.add(1);
                        self.join_metrics.output_rows.add(batch.num_rows());
                    }
                    if !self.right.exhausted {
                        self.data_side = JoinSide::Right;
                    }
                    Poll::Ready(Some(result))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.left.exhausted = true;
                    self.data_side = JoinSide::Right;
                    Poll::Ready(Some(Ok(RecordBatch::new_empty(self.schema()))))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            match self.right_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(probe_batch))) => {
                    self.join_metrics.right_input_batches.add(1);
                    self.join_metrics
                        .right_input_rows
                        .add(probe_batch.num_rows());
                    // Right is build side
                    match self.right.update_internal_state(
                        &self.on_right,
                        &probe_batch,
                        &self.random_state,
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            return Poll::Ready(Some(Err(ArrowError::ComputeError(
                                e.to_string(),
                            ))))
                        }
                    }
                    let equal_result = self.left.record_batch_from_other_side(
                        self.schema.clone(),
                        self.join_type,
                        &self.on_left,
                        &self.on_right,
                        Some(&self.filter),
                        &probe_batch,
                        &mut self.right.visited_rows,
                        self.right.offset,
                        &self.column_indices,
                        &self.random_state,
                        &self.null_equals_null,
                    );
                    self.right.offset += probe_batch.num_rows();
                    let anti_result = self.left.prune_build_side(
                        self.schema.clone(),
                        &probe_batch,
                        &mut self.filter_columns,
                        self.join_type,
                        &self.column_indices,
                        &mut self.physical_expr_graph,
                    );
                    let result = produce_batch_result(
                        self.schema.clone(),
                        equal_result,
                        anti_result,
                    );
                    if let Ok(batch) = &result {
                        self.join_metrics.output_batches.add(1);
                        self.join_metrics.output_rows.add(batch.num_rows());
                    }
                    if !self.left.exhausted {
                        self.data_side = JoinSide::Left;
                    }
                    Poll::Ready(Some(result))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.right.exhausted = true;
                    self.data_side = JoinSide::Left;
                    Poll::Ready(Some(Ok(RecordBatch::new_empty(self.schema()))))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow::array::ArrayRef;
    use arrow::array::{Int32Array, TimestampNanosecondArray};
    use arrow::compute::CastOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::util::pretty::pretty_format_batches;
    use rstest::*;
    use tempfile::TempDir;

    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
    use datafusion_physical_expr::utils;

    use crate::physical_plan::collect;
    use crate::physical_plan::joins::{HashJoinExec, PartitionMode};
    use crate::physical_plan::{
        common, memory::MemoryExec, repartition::RepartitionExec,
    };
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test_util;

    use super::*;

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
        sorted_filter_columns: Vec<SortedFilterExpr>,
        join_type: &JoinType,
        null_equals_null: bool,
        context: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let partition_count = 1;

        let left_expr: Vec<Arc<dyn PhysicalExpr>> = on
            .iter()
            .map(|(l, _)| Arc::new(l.clone()) as Arc<dyn PhysicalExpr>)
            .collect_vec();

        let right_expr: Vec<Arc<dyn PhysicalExpr>> = on
            .iter()
            .map(|(_, r)| Arc::new(r.clone()) as Arc<dyn PhysicalExpr>)
            .collect_vec();

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
            sorted_filter_columns,
            join_type,
            &null_equals_null,
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
        let partition_count = 1;

        let (left_expr, right_expr) = on
            .iter()
            .map(|(l, r)| {
                (
                    Arc::new(l.clone()) as Arc<dyn PhysicalExpr>,
                    Arc::new(r.clone()) as Arc<dyn PhysicalExpr>,
                )
            })
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
            &null_equals_null,
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
        num_split: usize,
    ) -> Result<Vec<RecordBatch>> {
        let row_num = batch.num_rows();
        let number_of_batch = row_num / num_split;
        let mut sizes = vec![num_split; number_of_batch];
        sizes.push(row_num - (num_split * number_of_batch));
        let mut result = vec![];
        for (i, size) in sizes.iter().enumerate() {
            result.push(batch.slice(i * num_split, *size));
        }
        Ok(result)
    }

    fn build_table(columns: Vec<(&str, ArrayRef)>) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
                .collect(),
        );
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap();

        let schema = batch.schema();
        Arc::new(
            MemoryExec::try_new(
                &[split_record_batches(&batch, 13).unwrap()],
                schema,
                None,
            )
            .unwrap(),
        )
    }

    fn join_expr_tests_fixture(
        expr_id: usize,
        left_watermark: Arc<dyn PhysicalExpr>,
        right_watermark: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        match expr_id {
            // left_watermark + 1 > right_watermark + 5 AND left_watermark + 3 < right_watermark + 10
            0 => utils::filter_numeric_expr_generation(
                left_watermark.clone(),
                right_watermark.clone(),
                Operator::Plus,
                Operator::Plus,
                Operator::Plus,
                Operator::Plus,
                1,
                5,
                3,
                10,
            ),
            // left_watermark - 1 > right_watermark + 5 AND left_watermark + 3 < right_watermark + 10
            1 => utils::filter_numeric_expr_generation(
                left_watermark.clone(),
                right_watermark.clone(),
                Operator::Minus,
                Operator::Plus,
                Operator::Plus,
                Operator::Plus,
                1,
                5,
                3,
                10,
            ),
            // left_watermark - 1 > right_watermark + 5 AND left_watermark - 3 < right_watermark + 10
            2 => utils::filter_numeric_expr_generation(
                left_watermark.clone(),
                right_watermark.clone(),
                Operator::Minus,
                Operator::Plus,
                Operator::Minus,
                Operator::Plus,
                1,
                5,
                3,
                10,
            ),
            // left_watermark - 10 > right_watermark - 5 AND left_watermark - 3 < right_watermark + 10
            3 => utils::filter_numeric_expr_generation(
                left_watermark.clone(),
                right_watermark.clone(),
                Operator::Minus,
                Operator::Minus,
                Operator::Minus,
                Operator::Plus,
                10,
                5,
                3,
                10,
            ),
            // left_watermark - 10 > right_watermark - 5 AND left_watermark - 30 < right_watermark - 3
            4 => utils::filter_numeric_expr_generation(
                left_watermark.clone(),
                right_watermark.clone(),
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

    fn create_memory_table(
        table_size: i32,
        key_cardinality: (i32, i32),
    ) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
        let initial_range = 0..table_size;
        let left = build_table(vec![
            (
                "la1",
                Arc::new(Int32Array::from_iter(
                    initial_range.clone().collect::<Vec<i32>>(),
                )),
            ),
            (
                "lb1",
                Arc::new(Int32Array::from_iter(
                    initial_range.clone().map(|x| x % 4).collect::<Vec<i32>>(),
                )),
            ),
            (
                "lc1",
                Arc::new(Int32Array::from_iter(
                    initial_range
                        .clone()
                        .map(|x| x % key_cardinality.0)
                        .collect::<Vec<i32>>(),
                )),
            ),
            (
                "lt1",
                Arc::new(TimestampNanosecondArray::from(
                    initial_range
                        .clone()
                        .map(|x| 1664264591000000000 + (5000000000 * (x as i64)))
                        .collect::<Vec<i64>>(),
                )),
            ),
            (
                "la2",
                Arc::new(Int32Array::from_iter(
                    initial_range.clone().collect::<Vec<i32>>(),
                )),
            ),
            (
                "la1_des",
                Arc::new(Int32Array::from_iter(
                    initial_range.clone().rev().collect::<Vec<i32>>(),
                )),
            ),
        ]);
        let right = build_table(vec![
            (
                "ra1",
                Arc::new(Int32Array::from_iter(
                    initial_range.clone().collect::<Vec<i32>>(),
                )),
            ),
            (
                "rb1",
                Arc::new(Int32Array::from_iter(
                    initial_range.clone().map(|x| x % 7).collect::<Vec<i32>>(),
                )),
            ),
            (
                "rc1",
                Arc::new(Int32Array::from_iter(
                    initial_range
                        .clone()
                        .map(|x| x % key_cardinality.1)
                        .collect::<Vec<i32>>(),
                )),
            ),
            (
                "rt1",
                Arc::new(TimestampNanosecondArray::from(
                    initial_range
                        .clone()
                        .map(|x| 1664264591000000000 + (5000000000 * (x as i64)))
                        .collect::<Vec<i64>>(),
                )),
            ),
            (
                "ra2",
                Arc::new(Int32Array::from_iter(
                    initial_range.clone().collect::<Vec<i32>>(),
                )),
            ),
            (
                "ra1_des",
                Arc::new(Int32Array::from_iter(
                    initial_range.rev().collect::<Vec<i32>>(),
                )),
            ),
        ]);
        (left, right)
    }

    fn complicated_fiter() -> Arc<dyn PhysicalExpr> {
        let left_expr = BinaryExpr {
            left: Arc::new(CastExpr {
                expr: Arc::new(BinaryExpr {
                    left: Arc::new(Column {
                        name: "0".to_string(),
                        index: 0,
                    }),
                    op: Operator::Plus,
                    right: Arc::new(Column {
                        name: "1".to_string(),
                        index: 1,
                    }),
                }),
                cast_type: DataType::Int64,
                cast_options: CastOptions { safe: false },
            }),
            op: Operator::Gt,
            right: Arc::new(BinaryExpr {
                left: Arc::new(CastExpr {
                    expr: Arc::new(Column {
                        name: "2".to_string(),
                        index: 2,
                    }),
                    cast_type: DataType::Int64,
                    cast_options: CastOptions { safe: false },
                }),
                op: Operator::Plus,
                right: Arc::new(Literal::new(ScalarValue::Int64(Some(10)))),
            }),
        };
        let right_expr = BinaryExpr {
            left: Arc::new(CastExpr {
                expr: Arc::new(BinaryExpr {
                    left: Arc::new(Column {
                        name: "0".to_string(),
                        index: 0,
                    }),
                    op: Operator::Plus,
                    right: Arc::new(Column {
                        name: "1".to_string(),
                        index: 1,
                    }),
                }),
                cast_type: DataType::Int64,
                cast_options: CastOptions { safe: false },
            }),
            op: Operator::Lt,
            right: Arc::new(BinaryExpr {
                left: Arc::new(CastExpr {
                    expr: Arc::new(Column {
                        name: "2".to_string(),
                        index: 2,
                    }),
                    cast_type: DataType::Int64,
                    cast_options: CastOptions { safe: false },
                }),
                op: Operator::Plus,
                right: Arc::new(Literal::new(ScalarValue::Int64(Some(100)))),
            }),
        };
        Arc::new(BinaryExpr::new(
            Arc::new(left_expr),
            Operator::And,
            Arc::new(right_expr),
        ))
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
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
        let (left, right) = create_memory_table(1000, cardinality);

        let on = vec![(
            Column::new_with_schema("lc1", &left.schema())?,
            Column::new_with_schema("rc1", &right.schema())?,
        )];

        let filter_col_0 = Arc::new(Column::new("0", 0));
        let filter_col_1 = Arc::new(Column::new("1", 1));
        let filter_col_2 = Arc::new(Column::new("2", 2));

        let main_sorted_binary = Arc::new(BinaryExpr {
            left: Arc::new(Column::new_with_schema("la1", &left.schema())?),
            op: Operator::Plus,
            right: Arc::new(Column::new_with_schema("la2", &left.schema())?),
        });

        let filter_sorted_binary = Arc::new(BinaryExpr {
            left: filter_col_0.clone(),
            op: Operator::Plus,
            right: filter_col_1.clone(),
        });

        let right_main_sorted_col =
            Arc::new(Column::new_with_schema("ra1", &right.schema())?);

        let sorted_filter_columns = vec![
            SortedFilterExpr::new(
                JoinSide::Left,
                main_sorted_binary,
                filter_sorted_binary,
                SortOptions::default(),
            ),
            SortedFilterExpr::new(
                JoinSide::Right,
                right_main_sorted_col.clone(),
                filter_col_2.clone(),
                SortOptions::default(),
            ),
        ];

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
        let intermediate_schema = Schema::new(vec![
            Field::new(filter_col_0.name(), DataType::Int32, true),
            Field::new(filter_col_1.name(), DataType::Int32, true),
            Field::new(filter_col_2.name(), DataType::Int32, true),
        ]);

        let filter_expr = complicated_fiter();

        let filter = JoinFilter::new(
            filter_expr,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let first_batches = partitioned_sym_join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter.clone(),
            sorted_filter_columns,
            &join_type,
            false,
            task_ctx.clone(),
        )
        .await?;
        let second_batches = partitioned_hash_join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter.clone(),
            &join_type,
            false,
            task_ctx.clone(),
        )
        .await?;
        compare_batches(&first_batches, &second_batches);
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
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
        let (left, right) = create_memory_table(1000, cardinality);

        let on = vec![(
            Column::new_with_schema("lc1", &left.schema())?,
            Column::new_with_schema("rc1", &right.schema())?,
        )];

        let left_col = Arc::new(Column::new("left", 0));
        let right_col = Arc::new(Column::new("right", 1));

        let sorted_filter_columns = vec![
            SortedFilterExpr::new(
                JoinSide::Left,
                Arc::new(Column::new_with_schema("la1", &left.schema())?),
                left_col.clone(),
                SortOptions::default(),
            ),
            SortedFilterExpr::new(
                JoinSide::Right,
                Arc::new(Column::new_with_schema("ra1", &right.schema())?),
                right_col.clone(),
                SortOptions::default(),
            ),
        ];

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
        let intermediate_schema = Schema::new(vec![
            Field::new(left_col.name(), DataType::Int32, true),
            Field::new(right_col.name(), DataType::Int32, true),
        ]);

        let filter_expr =
            join_expr_tests_fixture(case_expr, left_col.clone(), right_col.clone());

        let filter = JoinFilter::new(
            filter_expr,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let first_batches = partitioned_sym_join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter.clone(),
            sorted_filter_columns,
            &join_type,
            false,
            task_ctx.clone(),
        )
        .await?;
        let second_batches = partitioned_hash_join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter.clone(),
            &join_type,
            false,
            task_ctx.clone(),
        )
        .await?;
        compare_batches(&first_batches, &second_batches);
        Ok(())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
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
        let (left, right) = create_memory_table(20, cardinality);

        let on = vec![(
            Column::new_with_schema("lc1", &left.schema())?,
            Column::new_with_schema("rc1", &right.schema())?,
        )];

        let left_col = Arc::new(Column::new("left", 0));
        let right_col = Arc::new(Column::new("right", 1));

        let sorted_filter_columns = vec![
            SortedFilterExpr::new(
                JoinSide::Left,
                Arc::new(Column::new_with_schema("la1_des", &left.schema())?),
                left_col.clone(),
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            ),
            SortedFilterExpr::new(
                JoinSide::Right,
                Arc::new(Column::new_with_schema("ra1_des", &right.schema())?),
                right_col.clone(),
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            ),
        ];

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
        let intermediate_schema = Schema::new(vec![
            Field::new(left_col.name(), DataType::Int32, true),
            Field::new(right_col.name(), DataType::Int32, true),
        ]);

        let filter_expr =
            join_expr_tests_fixture(case_expr, left_col.clone(), right_col.clone());

        let filter = JoinFilter::new(
            filter_expr,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let first_batches = partitioned_sym_join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter.clone(),
            sorted_filter_columns,
            &join_type,
            false,
            task_ctx.clone(),
        )
        .await?;
        let second_batches = partitioned_hash_join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter.clone(),
            &join_type,
            false,
            task_ctx.clone(),
        )
        .await?;
        compare_batches(&first_batches, &second_batches);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
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
        println!("Query Output:\n\n{formatted}");
        let found = formatted
            .lines()
            .any(|line| line.contains("SymmetricHashJoinExec"));
        assert!(found);
        Ok(())
    }
}
