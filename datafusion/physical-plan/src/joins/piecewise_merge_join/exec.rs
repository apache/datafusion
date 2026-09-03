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

use arrow::array::Array;
use arrow::{
    array::{ArrayRef, RecordBatch},
    compute::{concat, concat_batches},
};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::not_impl_err;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{JoinSide, Result, internal_datafusion_err, internal_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::{
    SendableRecordBatchStream,
    memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
};
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::{
    Distribution, LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalExprRef,
    PhysicalSortExpr,
};
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use futures::{StreamExt, TryStreamExt};
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::execution_plan::{EmissionType, boundedness_from_children};

use crate::joins::piecewise_merge_join::classic_join::{
    ClassicPWMJStream, PiecewiseMergeJoinStreamState,
};
use crate::joins::piecewise_merge_join::existence_join::{
    ExistencePWMJStream, extreme_key,
};
use crate::joins::piecewise_merge_join::right_existence_join::RightExistencePWMJStream;
use crate::joins::piecewise_merge_join::utils::{
    is_existence_join, is_supported_existence_join, is_supported_right_existence_join,
};
use crate::joins::utils::asymmetric_join_output_partitioning;
use crate::metrics::MetricsSet;
use crate::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlanProperties,
    ReplaceChildrenOptions, validate_child_count,
};
use crate::{
    ExecutionPlan, PlanProperties,
    joins::utils::{BuildProbeJoinMetrics, OnceAsync, OnceFut, build_join_schema},
    metrics::ExecutionPlanMetricsSet,
    spill::get_record_batch_memory_size,
};

/// `PiecewiseMergeJoinExec` is a join execution plan that only evaluates single range filter and show much
/// better performance for these workloads than `NestedLoopJoin`
///
/// The physical planner will choose to evaluate this join when there is only one comparison filter. This
/// is a binary expression which contains [`Operator::Lt`], [`Operator::LtEq`], [`Operator::Gt`], and
/// [`Operator::GtEq`].:
/// Examples:
///  - `col0` < `colb`, `col0` <= `colb`, `col0` > `colb`, `col0` >= `colb`
///
/// # Execution Plan Inputs
/// For `PiecewiseMergeJoin` we label all right inputs as the `streamed' side and the left outputs as the
/// 'buffered' side.
///
/// `PiecewiseMergeJoin` takes a sorted input for the side to be buffered and is able to sort streamed record
/// batches during processing. Sorted input must specifically be ascending/descending based on the operator.
///
/// # Algorithms
/// Classic joins are processed differently compared to existence joins.
///
/// ## Classic Joins (Inner, Full, Left, Right)
/// For classic joins we buffer the build side and stream the probe side (the "probe" side).
/// Both sides are sorted so that we can iterate from index 0 to the end on each side.  This ordering ensures
/// that when we find the first matching pair of rows, we can emit the current stream row joined with all remaining
/// probe rows from the match position onward, without rescanning earlier probe rows.
///
/// For `<` and `<=` operators, both inputs are sorted in **descending** order, while for `>` and `>=` operators
/// they are sorted in **ascending** order. This choice ensures that the pointer on the buffered side can advance
/// monotonically as we stream new batches from the stream side.
///
/// The streamed side may arrive unsorted, so this operator sorts each incoming batch in memory before
/// processing. The buffered side is required to be globally sorted; the plan declares this requirement
/// in `requires_input_order`, which allows the optimizer to automatically insert a `SortExec` on that side if needed.
/// By the time this operator runs, the buffered side is guaranteed to be in the proper order.
///
/// The pseudocode for the algorithm looks like this:
///
/// ```text
/// for stream_row in stream_batch:
///     for buffer_row in buffer_batch:
///         if compare(stream_row, probe_row):
///             output stream_row X buffer_batch[buffer_row:]
///         else:
///             continue
/// ```
///
/// The algorithm uses the streamed side (larger) to drive the loop. This is due to every row on the stream side iterating
/// the buffered side to find every first match. By doing this, each match can output more result so that output
/// handling can be better vectorized for performance.
///
/// Here is an example:
///
/// We perform a `JoinType::Left` with these two batches and the operator being `Operator::Lt`(<). For each
/// row on the streamed side we move a pointer on the buffered until it matches the condition. Once we reach
/// the row which matches (in this case with row 1 on streamed will have its first match on row 2 on
/// buffered; 100 < 200 is true), we can emit all rows after that match. We can emit the rows like this because
/// if the batch is sorted in ascending order, every subsequent row will also satisfy the condition as they will
/// all be larger values.
///
/// ```text
/// SQL statement:
/// SELECT *
/// FROM (VALUES (100), (200), (500)) AS streamed(a)
/// LEFT JOIN (VALUES (100), (200), (200), (300), (400)) AS buffered(b)
///   ON streamed.a < buffered.b;
///
/// Processing Row 1:
///
///       Sorted Buffered Side                                         Sorted Streamed Side
///       ┌──────────────────┐                                         ┌──────────────────┐
///     1 │       100        │                                       1 │       100        │
///       ├──────────────────┤                                         ├──────────────────┤
///     2 │       200        │ ─┐                                    2 │       200        │
///       ├──────────────────┤  │  For row 1 on streamed side with     ├──────────────────┤
///     3 │       200        │  │  value 100, we emit rows 2 - 5.    3 │       500        │
///       ├──────────────────┤  │  as matches when the operator is     └──────────────────┘
///     4 │       300        │  │  `Operator::Lt` (<) Emitting all
///       ├──────────────────┤  │  rows after the first match (row
///     5 │       400        │ ─┘  2 buffered side; 100 < 200)
///       └──────────────────┘
///
/// Processing Row 2:
///   By sorting the streamed side we know
///
///       Sorted Buffered Side                                         Sorted Streamed Side
///       ┌──────────────────┐                                         ┌──────────────────┐
///     1 │       100        │                                       1 │       100        │
///       ├──────────────────┤                                         ├──────────────────┤
///     2 │       200        │ <- Start here when probing for the    2 │       200        │
///       ├──────────────────┤    streamed side row 2.                 ├──────────────────┤
///     3 │       200        │                                       3 │       500        │
///       ├──────────────────┤                                         └──────────────────┘
///     4 │       300        │
///       ├──────────────────┤
///     5 │       400        │
///       └──────────────────┘
/// ```
///
/// ## Existence Joins (Semi, Anti, Mark)
/// Every Semi/Anti join is supported; the Mark joins are rejected in [`Self::try_new`], as they
/// need an extra boolean column rather than a subset of one side's rows. The two sides are
/// served by different streams, because a single range predicate makes them different problems.
///
/// `LeftSemi`/`LeftAnti` mark the buffered (left) side, which is `ExistencePWMJStream` (see
/// `existence_join.rs`). Instead of materializing row pairs it records the matched set as a
/// single index -- the start of the matched suffix of the buffered side -- and slices the
/// buffered batch at that index once every streamed partition has been consumed.
///
/// ```text
/// // Using the example of a less than `<` operation
/// let max = max_batch(streamed_batch)
///
/// for buffer_row in buffer_batch:
///     if buffer_row < max:
///         output buffer_batch[buffer_row:]
/// ```
///
/// Only need to find the min/max value and iterate through the buffered side once.
///
/// Here is an example:
/// We perform a `JoinType::LeftSemi` with these two batches and the operator being `Operator::Lt`(<). Because
/// the operator is `Operator::Lt` we can find the minimum value in the streamed side; in this case it is 200.
/// We can then advance a pointer from the start of the buffer side until we find the first value that satisfies
/// the predicate. All rows after that first matched value satisfy the condition 200 < x so we can mark all of
/// those rows as matched.
///
/// ```text
/// SQL statement:
/// SELECT *
/// FROM (VALUES (500), (200), (300)) AS streamed(a)
/// LEFT SEMI JOIN (VALUES (100), (200), (200), (300), (400)) AS buffered(b)
///   ON streamed.a < buffered.b;
///
///          Sorted Buffered Side             Unsorted Streamed Side
///            ┌──────────────────┐          ┌──────────────────┐
///          1 │       100        │        1 │       500        │
///            ├──────────────────┤          ├──────────────────┤
///          2 │       200        │        2 │       200        │
///            ├──────────────────┤          ├──────────────────┤
///          3 │       200        │        3 │       300        │
///            ├──────────────────┤          └──────────────────┘
///          4 │       300        │ ─┐
///            ├──────────────────┤  | We emit matches for row 4 - 5
///          5 │       400        │ ─┘ on the buffered side.
///            └──────────────────┘
///             min value: 200
/// ```
///
/// `RightSemi`/`RightAnti` mark the streamed (right) side, which is
/// `RightExistencePWMJStream` (see `right_existence_join.rs`). Asking whether any buffered
/// row matches a given streamed row is, for a single range predicate, decided by one buffered
/// key -- the minimum for `<`/`<=`, the maximum for `>`/`>=`. So the buffered side is folded
/// down to that key as it arrives and never materialized, and each streamed batch is compared
/// against it, filtered, and emitted straight away rather than at the end. These are the only
/// join types here that require no ordered input and hold `O(1)` state.
///
/// ```text
/// // Using the example of a less than `<` operation
/// let min = min_batch(buffered)        // folded per batch, nothing retained
///
/// for stream_row in stream_batch:
///     if min < stream_row:             // some buffered row matches
///         output stream_row
/// ```
///
/// Except for `RightSemi`/`RightAnti`, the buffered side must be sorted ascending for
/// `Operator::Lt` (<) or `Operator::LtEq` (<=) and descending for `Operator::Gt` (>) or
/// `Operator::GtEq` (>=).
///
/// # Partitioning Logic
/// Piecewise Merge Join requires one buffered side partition + round robin partitioned stream side. A counter
/// is used in the buffered side to coordinate when all streamed partitions are finished execution. This allows
/// for processing the rest of the unmatched rows for Left and Full joins. The last partition that finishes
/// execution will be responsible for outputting the unmatched rows.
///
/// `RightSemi`/`RightAnti` need no such coordination: each streamed row is decided on its own,
/// so every partition emits its own output and none has a final pass to run. They also place no
/// single-partition requirement on the buffered side -- a min/max combines across partitions, so
/// each is folded on its own task rather than funnelled through a `CoalescePartitionsExec`.
///
/// # Performance Explanation (cost)
/// Piecewise Merge Join is used over Nested Loop Join due to its superior performance. Here is the breakdown:
///
/// R: Buffered Side
/// S: Streamed Side
///
/// ## Piecewise Merge Join (PWMJ)
///
/// # Classic Join:
/// Requires sorting the probe side and, for each probe row, scanning the buffered side until the first match
/// is found.
///     Complexity: `O(sort(S) + num_of_batches(|S|) * scan(R))`.
///
/// # Mark Join:
/// Sorts the probe side, then computes the min/max range of the probe keys and scans the buffered side only
/// within that range.
///   Complexity: `O(|S| + scan(R[range]))`.
///
/// ## Nested Loop Join
/// Compares every row from `S` with every row from `R`.
///   Complexity: `O(|S| * |R|)`.
///
/// ## Nested Loop Join
///   Always going to be probe (O(S) * O(R)).
///
/// # Further Reference Material
/// DuckDB blog on Range Joins: [Range Joins in DuckDB](https://duckdb.org/2022/05/27/iejoin.html)
#[derive(Debug)]
pub struct PiecewiseMergeJoinExec {
    /// Left buffered execution plan
    pub buffered: Arc<dyn ExecutionPlan>,
    /// Right streamed execution plan
    pub streamed: Arc<dyn ExecutionPlan>,
    /// The two expressions being compared
    pub on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
    /// Comparison operator in the range predicate
    pub operator: Operator,
    /// How the join is performed
    pub join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Buffered data, collected once and shared by every streamed partition. Unused by right
    /// existence joins, which take `buffered_extreme_fut` instead.
    buffered_fut: OnceAsync<BufferedSideData>,
    /// Right existence joins only: the buffered side folded down to one key, so that side is
    /// never materialized. Unused by every other join type.
    buffered_extreme_fut: OnceAsync<BufferedExtreme>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Sort expressions - See above for more details [`PiecewiseMergeJoinExec`]
    ///
    /// The left sort order, descending for `<`, `<=` operations + ascending for `>`, `>=` operations
    left_child_plan_required_order: LexOrdering,
    /// The right sort order, descending for `<`, `<=` operations + ascending for `>`, `>=` operations
    /// Unsorted for mark joins
    right_batch_required_orders: LexOrdering,

    /// This determines the sort order of all join columns used in sorting the stream and buffered execution plans.
    sort_options: SortOptions,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
    /// Number of partitions to process
    num_partitions: usize,
}

impl PiecewiseMergeJoinExec {
    pub fn try_new(
        buffered: Arc<dyn ExecutionPlan>,
        streamed: Arc<dyn ExecutionPlan>,
        on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        operator: Operator,
        join_type: JoinType,
        num_partitions: usize,
    ) -> Result<Self> {
        // Semi/Anti joins are handled by the existence streams; Mark joins are not
        // supported yet.
        if is_existence_join(join_type) && !is_supported_existence_join(join_type) {
            return not_impl_err!(
                "Existence join {join_type} is currently not supported for PiecewiseMergeJoin"
            );
        }

        // Take the operator and enforce a sort order on the streamed + buffered side based on
        // the operator type.
        let sort_options = match operator {
            Operator::Lt | Operator::LtEq => SortOptions::new(true, true),
            Operator::Gt | Operator::GtEq => SortOptions::new(false, true),
            _ => {
                return internal_err!(
                    "Cannot contain non-range operator in PiecewiseMergeJoinExec"
                );
            }
        };

        // Give the same `sort_option for comparison later`
        let left_child_plan_required_order =
            vec![PhysicalSortExpr::new(Arc::clone(&on.0), sort_options)];
        let right_batch_required_orders =
            vec![PhysicalSortExpr::new(Arc::clone(&on.1), sort_options)];

        let Some(left_child_plan_required_order) =
            LexOrdering::new(left_child_plan_required_order)
        else {
            return internal_err!(
                "PiecewiseMergeJoinExec requires valid sort expressions for its left side"
            );
        };
        let Some(right_batch_required_orders) =
            LexOrdering::new(right_batch_required_orders)
        else {
            return internal_err!(
                "PiecewiseMergeJoinExec requires valid sort expressions for its right side"
            );
        };

        let buffered_schema = buffered.schema();
        let streamed_schema = streamed.schema();

        // Create output schema for the join
        let schema =
            Arc::new(build_join_schema(&buffered_schema, &streamed_schema, &join_type).0);
        let cache = Self::compute_properties(
            &buffered,
            &streamed,
            Arc::clone(&schema),
            join_type,
        )?;

        Ok(Self {
            streamed,
            buffered,
            on,
            operator,
            join_type,
            schema,
            buffered_fut: Default::default(),
            buffered_extreme_fut: Default::default(),
            metrics: ExecutionPlanMetricsSet::new(),
            left_child_plan_required_order,
            right_batch_required_orders,
            sort_options,
            cache: Arc::new(cache),
            num_partitions,
        })
    }

    /// Reference to buffered side execution plan
    pub fn buffered(&self) -> &Arc<dyn ExecutionPlan> {
        &self.buffered
    }

    /// Reference to streamed side execution plan
    pub fn streamed(&self) -> &Arc<dyn ExecutionPlan> {
        &self.streamed
    }

    /// Join type
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Reference to sort options
    pub fn sort_options(&self) -> &SortOptions {
        &self.sort_options
    }

    /// Get probe side (streamed side) for the PiecewiseMergeJoin
    /// In current implementation, probe side is determined according to join type.
    pub fn probe_side(join_type: &JoinType) -> JoinSide {
        match join_type {
            JoinType::Right
            | JoinType::Inner
            | JoinType::Full
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => JoinSide::Right,
            JoinType::Left
            | JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::LeftMark => JoinSide::Left,
        }
    }

    pub fn compute_properties(
        buffered: &Arc<dyn ExecutionPlan>,
        streamed: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
    ) -> Result<PlanProperties> {
        let eq_properties = join_equivalence_properties(
            buffered.equivalence_properties().clone(),
            streamed.equivalence_properties().clone(),
            &join_type,
            schema,
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side(&join_type)),
            // `PiecewiseMergeJoin`'s `on` is a range predicate (e.g. `l < r`),
            // not an equijoin key. Passing it here would register a false
            // `left == right` output equivalence, letting the optimizer drop a
            // required sort and return wrongly ordered results. Range joins add
            // no column equivalences, so pass none.
            &[],
        )?;

        let output_partitioning =
            asymmetric_join_output_partitioning(buffered, streamed, &join_type)?;

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            boundedness_from_children([buffered, streamed]),
        ))
    }

    // TODO: Add input order. Now they're all `false` indicating it will not maintain the input order.
    // However, for certain join types the order is maintained. This can be updated in the future after
    // more testing.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        match join_type {
            // One output batch per streamed batch, in arrival order, with rows only ever
            // *removed* by a filter, so each output partition keeps its streamed partition's
            // order. The buffered side contributes no output column, hence `false` there.
            // `RightMark` is excluded: it adds a `mark` column, so its orderings would not
            // map across unchanged.
            JoinType::RightSemi | JoinType::RightAnti => vec![false, true],
            // Unlike the right existence joins above, output here is gated on a watermark
            // shared across every streamed partition (see `ExistencePWMJStream`) and emitted
            // only from whichever partition finishes last, so no streamed partition's output
            // order tracks its input order.
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                vec![false, false]
            }
            JoinType::RightMark => vec![false, false],
            // Left, Right, Full, Inner Join is not guaranteed to maintain
            // input order as the streamed side will be sorted during
            // execution for `PiecewiseMergeJoin`
            _ => vec![false, false],
        }
    }

    // TODO
    pub fn swap_inputs(&self) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    /// Sets up the buffered-side collection used by the classic and left existence streams:
    /// buffered partition 0 is executed here, and folded into one sorted batch when the
    /// returned future is first polled.
    ///
    /// Right existence joins use `buffered_extreme_fut` instead, consuming every buffered
    /// partition themselves.
    fn buffered_side(
        &self,
        context: &Arc<datafusion_execution::TaskContext>,
        on_buffered: &PhysicalExprRef,
        metrics: &BuildProbeJoinMetrics,
        streamed_partitions: usize,
    ) -> Result<BufferedSide> {
        let buffered_fut = self.buffered_fut.try_once(|| {
            let reservation = MemoryConsumer::new("PiecewiseMergeJoinInput")
                .register(context.memory_pool());

            let buffered_stream = self.buffered.execute(0, Arc::clone(context))?;
            Ok(build_buffered_data(
                buffered_stream,
                Arc::clone(on_buffered),
                metrics.clone(),
                reservation,
                streamed_partitions,
            ))
        })?;

        Ok(BufferedSide::Initial(BufferedSideInitialState {
            buffered_fut,
        }))
    }
}

impl ExecutionPlan for PiecewiseMergeJoinExec {
    fn name(&self) -> &str {
        "PiecewiseMergeJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.buffered, &self.streamed]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // Apply to the two expressions being compared in the range predicate
        crate::apply_expression_roots([&self.on.0, &self.on.1], f)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> crate::InputDistributionRequirements {
        // Right existence joins reduce the buffered side to a min/max, which combines across
        // partitions, so that side keeps whatever parallelism the plan gave it -- no
        // `CoalescePartitionsExec` funnelling every buffered row through one thread. Every
        // other join type walks the buffered side as a single sorted run and does need it.
        let buffered = if is_supported_right_existence_join(self.join_type) {
            Distribution::UnspecifiedDistribution
        } else {
            Distribution::SinglePartition
        };
        crate::InputDistributionRequirements::new(vec![
            buffered,
            Distribution::UnspecifiedDistribution,
        ])
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Derived exactly as the default does, from this operator's own distribution
        // requirements, so the two cannot drift apart as those change.
        let mut benefits: Vec<bool> = self
            .input_distribution_requirements()
            .per_child_distributions()
            .map(|dist| !matches!(dist, Distribution::SinglePartition))
            .collect();

        // One deviation: right existence joins ask for `UnspecifiedDistribution` on the buffered
        // side, which that rule reads as "worth fanning out". Folding a batch is one linear scan,
        // which does not pay for a channel hop, so decline the round-robin `RepartitionExec`.
        if is_supported_right_existence_join(self.join_type)
            && let Some(buffered) = benefits.first_mut()
        {
            *buffered = false;
        }

        benefits
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // Right existence joins read nothing but a single min/max off the buffered side, which
        // is `O(B)` from any order, so they require none. Every other join type walks the
        // buffered side in order and does.
        //
        // The streamed side never carries a requirement: the classic and left existence
        // streams sort each batch in memory, and the right existence stream needs no order.
        if is_supported_right_existence_join(self.join_type) {
            vec![None, None]
        } else {
            vec![
                Some(OrderingRequirements::from(
                    self.left_child_plan_required_order.clone(),
                )),
                None,
            ]
        }
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => {
                let buffered = children.swap_remove(0);
                let streamed = children.swap_remove(0);
                Ok(Arc::new(Self {
                    buffered,
                    streamed,
                    on: self.on.clone(),
                    operator: self.operator,
                    join_type: self.join_type,
                    schema: Arc::clone(&self.schema),
                    left_child_plan_required_order: self
                        .left_child_plan_required_order
                        .clone(),
                    right_batch_required_orders: self.right_batch_required_orders.clone(),
                    sort_options: self.sort_options,
                    cache: Arc::clone(&self.cache),
                    num_partitions: self.num_partitions,

                    // Re-set state.
                    metrics: ExecutionPlanMetricsSet::new(),
                    buffered_fut: Default::default(),
                    buffered_extreme_fut: Default::default(),
                }))
            }
            ChildrenPropertiesMode::Recompute => match &children[..] {
                [left, right] => Ok(Arc::new(PiecewiseMergeJoinExec::try_new(
                    Arc::clone(left),
                    Arc::clone(right),
                    self.on.clone(),
                    self.operator,
                    self.join_type,
                    self.num_partitions,
                )?)),
                _ => internal_err!(
                    "PiecewiseMergeJoin should have 2 children, found {}",
                    children.len()
                ),
            },
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

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let buffered = Arc::clone(&self.buffered);
        let streamed = Arc::clone(&self.streamed);
        self.replace_children(
            vec![buffered, streamed],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_buffered = Arc::clone(&self.on.0);
        let on_streamed = Arc::clone(&self.on.1);

        let metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);
        // The final pass over unmatched/existence rows must run exactly once, on the
        // last streamed partition to finish. That is coordinated by an atomic counter
        // seeded with the number of streamed partitions that will actually call
        // `execute`, which is the streamed side's output partition count — not the
        // planner's `target_partitions` (they can differ, e.g. when the streamed input
        // has a single partition), otherwise the counter never reaches 1 and the final
        // pass is skipped.
        let streamed_partitions = self.streamed.output_partitioning().partition_count();

        let batch_size = context.session_config().batch_size();
        match self.join_type {
            // Right existence joins never read a buffered *row*, only a single min/max over
            // the whole side, so they fold the buffered input away as it arrives instead of
            // collecting it.
            JoinType::RightSemi | JoinType::RightAnti => {
                // `∃b. b < s` is decided by the smallest buffered key, `∃b. b > s` by the
                // largest, so the operator alone picks the extreme.
                let descending = matches!(self.operator, Operator::Gt | Operator::GtEq);
                let extreme_fut = self.buffered_extreme_fut.try_once(|| {
                    let reservation =
                        MemoryConsumer::new("PiecewiseMergeJoinBufferedExtreme")
                            .register(context.memory_pool());

                    // Every buffered partition, not just partition 0: this join type does not
                    // require the buffered side coalesced, so it must consume all of it.
                    let buffered_partitions =
                        self.buffered.output_partitioning().partition_count();
                    let buffered_streams = (0..buffered_partitions)
                        .map(|p| self.buffered.execute(p, Arc::clone(&context)))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(build_buffered_extreme(
                        buffered_streams,
                        self.buffered.schema(),
                        Arc::clone(&on_buffered),
                        metrics.clone(),
                        reservation,
                        Arc::clone(context.memory_pool()),
                        descending,
                    ))
                })?;

                let streamed = self.streamed.execute(partition, Arc::clone(&context))?;

                Ok(Box::pin(RightExistencePWMJStream::try_new(
                    Arc::clone(&self.schema),
                    on_streamed,
                    self.join_type,
                    self.operator,
                    streamed,
                    extreme_fut,
                    metrics,
                )))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                let buffered_side = self.buffered_side(
                    &context,
                    &on_buffered,
                    &metrics,
                    streamed_partitions,
                )?;
                let streamed = self.streamed.execute(partition, Arc::clone(&context))?;

                Ok(Box::pin(ExistencePWMJStream::try_new(
                    Arc::clone(&self.schema),
                    on_streamed,
                    self.join_type,
                    self.operator,
                    streamed,
                    buffered_side,
                    self.sort_options,
                    metrics,
                    batch_size,
                )))
            }
            JoinType::LeftMark | JoinType::RightMark => internal_err!(
                "PiecewiseMergeJoin does not support existence join {} (should have been rejected in try_new)",
                self.join_type
            ),
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                let buffered_side = self.buffered_side(
                    &context,
                    &on_buffered,
                    &metrics,
                    streamed_partitions,
                )?;
                let streamed = self.streamed.execute(partition, Arc::clone(&context))?;

                Ok(Box::pin(ClassicPWMJStream::try_new(
                    Arc::clone(&self.schema),
                    on_streamed,
                    self.join_type,
                    self.operator,
                    streamed,
                    buffered_side,
                    PiecewiseMergeJoinStreamState::WaitBufferedSide,
                    self.sort_options,
                    metrics,
                    batch_size,
                )))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
            buffered,
            streamed,
            on,
            operator,
            join_type,
            num_partitions,
            // derived from the children's schemas by `try_new` on decode
            schema: _,
            // buffered side collected at execution time, not part of the plan
            buffered_fut: _,
            // buffered-side extreme collected at execution time for right existence
            // joins, not part of the plan
            buffered_extreme_fut: _,
            // runtime metrics, not part of the plan
            metrics: _,
            // recomputed from `on` and `sort_options` by `try_new` on decode
            left_child_plan_required_order: _,
            // recomputed from `on` and `sort_options` by `try_new` on decode
            right_batch_required_orders: _,
            // recomputed from `operator` and `join_type` by `try_new` on decode
            sort_options: _,
            // recomputed by `try_new` on decode
            cache: _,
        } = self;

        let (on_buffered, on_streamed) = on;
        let buffered = ctx.encode_child(buffered)?;
        let streamed = ctx.encode_child(streamed)?;
        let on_buffered = ctx.encode_expr(on_buffered)?;
        let on_streamed = ctx.encode_expr(on_streamed)?;
        let join_type = crate::joins::proto::join_type_to_proto(*join_type);

        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::PiecewiseMergeJoin(
                    Box::new(protobuf::PiecewiseMergeJoinExecNode {
                        buffered: Some(Box::new(buffered)),
                        streamed: Some(Box::new(streamed)),
                        on_buffered: Some(on_buffered),
                        on_streamed: Some(on_streamed),
                        // Matches the `Operator` encoding used for `BinaryExpr`:
                        // the `Debug` name of the variant.
                        operator: format!("{operator:?}"),
                        join_type: join_type.into(),
                        num_partitions: *num_partitions as u64,
                    }),
                ),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl PiecewiseMergeJoinExec {
    /// Reconstruct a [`PiecewiseMergeJoinExec`] from its protobuf representation.
    ///
    /// The exact inverse of [`ExecutionPlan::try_to_proto`]. Every other field of
    /// the operator (schema, sort options, required orderings, plan properties) is
    /// derived by [`PiecewiseMergeJoinExec::try_new`], so it is not on the wire.
    ///
    /// [`ExecutionPlan::try_to_proto`]: crate::ExecutionPlan::try_to_proto
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_common::{internal_datafusion_err, plan_datafusion_err};
        use datafusion_proto_models::protobuf;

        let join = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::PiecewiseMergeJoin,
            "PiecewiseMergeJoinExec",
        );
        // Destructure exhaustively (no `..`) so that a newly added proto field
        // is a compile error here instead of being silently ignored.
        let protobuf::PiecewiseMergeJoinExecNode {
            buffered,
            streamed,
            on_buffered,
            on_streamed,
            operator,
            join_type,
            num_partitions,
        } = &**join;

        let buffered = ctx.decode_required_child(
            buffered.as_deref(),
            "PiecewiseMergeJoinExec",
            "buffered",
        )?;
        let streamed = ctx.decode_required_child(
            streamed.as_deref(),
            "PiecewiseMergeJoinExec",
            "streamed",
        )?;
        let on_buffered = ctx.decode_required_expr(
            on_buffered.as_ref(),
            buffered.schema().as_ref(),
            "PiecewiseMergeJoinExec",
            "on_buffered",
        )?;
        let on_streamed = ctx.decode_required_expr(
            on_streamed.as_ref(),
            streamed.schema().as_ref(),
            "PiecewiseMergeJoinExec",
            "on_streamed",
        )?;

        let operator = Operator::from_proto_name(operator).ok_or_else(|| {
            internal_datafusion_err!(
                "PiecewiseMergeJoinExec: unknown Operator '{operator}'"
            )
        })?;
        let join_type = crate::joins::proto::join_type_from_proto(
            *join_type,
            "PiecewiseMergeJoinExec",
        )?;

        // Checked rather than `as usize`: a truncated partition count would not
        // fail loudly, it would silently change how the buffered side is split.
        let num_partitions =
            usize::try_from(*num_partitions).map_err(|_| {
                plan_datafusion_err!(
                     "PiecewiseMergeJoinExec: num_partitions {num_partitions} cannot be represented as usize on this target"
                )
            })?;

        Ok(Arc::new(Self::try_new(
            buffered,
            streamed,
            (on_buffered, on_streamed),
            operator,
            join_type,
            num_partitions,
        )?))
    }
}

impl DisplayAs for PiecewiseMergeJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let on_str = format!(
            "({} {} {})",
            fmt_sql(self.on.0.as_ref()),
            self.operator,
            fmt_sql(self.on.1.as_ref())
        );

        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PiecewiseMergeJoin: operator={:?}, join_type={:?}, on={}",
                    self.operator, self.join_type, on_str
                )
            }

            DisplayFormatType::TreeRender => {
                writeln!(f, "operator={:?}", self.operator)?;
                if self.join_type != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)?;
                }
                writeln!(f, "on={on_str}")
            }
        }
    }
}

async fn build_buffered_data(
    buffered: SendableRecordBatchStream,
    on_buffered: PhysicalExprRef,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    remaining_partitions: usize,
) -> Result<BufferedSideData> {
    let schema = buffered.schema();

    // Combine batches and record number of rows
    let initial = (Vec::new(), metrics, reservation);
    let (batches, metrics, reservation) = buffered
        .try_fold(initial, |mut acc, batch| async {
            let batch_size = get_record_batch_memory_size(&batch);
            acc.2.try_grow(batch_size)?;
            acc.1.build_mem_used.add(batch_size);
            acc.1.build_input_batches.add(1);
            acc.1.build_input_rows.add(batch.num_rows());
            // Push batch to output
            acc.0.push(batch);
            Ok(acc)
        })
        .await?;

    let single_batch = concat_batches(&schema, batches.iter())?;

    // Evaluate physical expression on the buffered side.
    let buffered_values = on_buffered
        .evaluate(&single_batch)?
        .into_array(single_batch.num_rows())?;

    // We add the single batch size + the memory of the join keys
    // size of the size estimation
    let size_estimation = get_record_batch_memory_size(&single_batch)
        + buffered_values.get_array_memory_size();
    reservation.try_grow(size_estimation)?;
    metrics.build_mem_used.add(size_estimation);

    let buffered_data = BufferedSideData::new(
        single_batch,
        buffered_values,
        remaining_partitions,
        reservation,
    );

    Ok(buffered_data)
}

pub(super) struct BufferedSideData {
    pub(super) batch: RecordBatch,
    values: ArrayRef,
    pub(super) remaining_partitions: AtomicUsize,
    /// The start of the matched suffix of the buffered side, or `usize::MAX` before the
    /// first match. `[min_marked, len)` *is* the matched set and `[0, min_marked)` the
    /// unmatched one -- no bitmap is allocated.
    ///
    /// Both stream kinds only ever mark a suffix, which is what makes one index enough:
    ///  - `ExistencePWMJStream` marks `[k, len)` for the first buffered row `k` matching
    ///    a streamed batch's extreme key.
    ///  - `ClassicPWMJStream` emits `buffered[k..] x streamed_row` on each match, so the
    ///    rows it marks are exactly that same suffix.
    ///
    /// Shared so each partition benefits from what the others have marked; it only ever
    /// decreases, so a stale read is safe.
    pub(super) min_marked: AtomicUsize,
    _reservation: MemoryReservation,
}

impl BufferedSideData {
    pub(super) fn new(
        batch: RecordBatch,
        values: ArrayRef,
        remaining_partitions: usize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            batch,
            values,
            remaining_partitions: AtomicUsize::new(remaining_partitions),
            min_marked: AtomicUsize::new(usize::MAX),
            _reservation: reservation,
        }
    }

    pub(super) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(super) fn values(&self) -> &ArrayRef {
        &self.values
    }
}

/// The entire buffered side of a right existence join, reduced to the one key that decides
/// every streamed row -- the minimum for `<`/`<=`, the maximum for `>`/`>=`.
///
/// Right existence joins never look at a buffered *row*, so unlike [`BufferedSideData`] this
/// holds no batch: the buffered input is folded away as it streams in and the state is `O(1)`
/// however large that side is. See `right_existence_join.rs`.
pub(super) struct BufferedExtreme {
    /// One-row array. The row is null exactly when no buffered key is non-null (an empty or
    /// all-NULL buffered side), in which case nothing can ever match.
    extreme: ArrayRef,
    _reservation: MemoryReservation,
}

impl BufferedExtreme {
    pub(super) fn extreme(&self) -> &ArrayRef {
        &self.extreme
    }
}

/// Reduces one buffered partition to a single extreme key, dropping each batch as it goes.
/// `None` when the partition produced no batch at all.
async fn partition_extreme(
    mut buffered: SendableRecordBatchStream,
    on_buffered: PhysicalExprRef,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    descending: bool,
) -> Result<Option<ArrayRef>> {
    let mut extreme: Option<ArrayRef> = None;

    while let Some(batch) = buffered.next().await.transpose()? {
        metrics.build_input_batches.add(1);
        metrics.build_input_rows.add(batch.num_rows());

        let keys = on_buffered.evaluate(&batch)?.into_array(batch.num_rows())?;

        // Reduced and dropped within this iteration, but as wide as the batch, and every
        // partition folds one of these at once. Resized rather than grown, so what the pool
        // sees is the widest key array in flight here and not their sum.
        reservation.try_resize(keys.get_array_memory_size())?;

        let batch_extreme = extreme_key(&keys, descending)?;

        // Re-reduced as a pair rather than compared, so the running value is ordered exactly
        // as each single reduction was. `extreme_key` ignores nulls, so a null from an
        // all-NULL batch never displaces a real key.
        extreme = Some(match extreme {
            Some(running) => {
                let pair = concat(&[running.as_ref(), batch_extreme.as_ref()])?;
                extreme_key(&pair, descending)?
            }
            None => batch_extreme,
        });
    }

    Ok(extreme)
}

/// Folds every buffered partition down to one extreme key. `O(B)` time, and the only state it
/// retains is that one key -- no buffered batch is concatenated or held. The transient cost is
/// one key array per folding partition, which each task accounts against the pool for as long
/// as it holds it.
///
/// A min/max combines across partitions, so this side needs no single-partition funnel --
/// `input_distribution_requirements` asks for `UnspecifiedDistribution` here and the partitions
/// are folded independently, each in its own spawned task.
async fn build_buffered_extreme(
    buffered_streams: Vec<SendableRecordBatchStream>,
    buffered_schema: SchemaRef,
    on_buffered: PhysicalExprRef,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    memory_pool: Arc<dyn MemoryPool>,
    descending: bool,
) -> Result<BufferedExtreme> {
    let tasks: Vec<_> = buffered_streams
        .into_iter()
        .enumerate()
        .map(|(partition, stream)| {
            let on_buffered = Arc::clone(&on_buffered);
            let metrics = metrics.clone();
            // One reservation per task rather than a shared one: `MemoryReservation` is not
            // shareable, and each task's transient is freed as soon as it finishes.
            let reservation = MemoryConsumer::new(format!(
                "PiecewiseMergeJoinBufferedFold[{partition}]"
            ))
            .register(&memory_pool);
            SpawnedTask::spawn(partition_extreme(
                stream,
                on_buffered,
                metrics,
                reservation,
                descending,
            ))
        })
        .collect();

    // The tasks run concurrently; this only collects them. Awaiting in order is fine, and a
    // failure propagates after the rest have been joined, since `SpawnedTask` aborts on drop.
    let mut extreme: Option<ArrayRef> = None;
    for task in tasks {
        let partition_extreme = task.join_unwind().await.map_err(|e| {
            internal_datafusion_err!("buffered extreme task failed: {e}")
        })??;
        if let Some(partition_extreme) = partition_extreme {
            // Re-reduced as a pair, exactly as the batches within a partition were, so a value
            // accumulated across partitions is ordered by the same rule.
            extreme = Some(match extreme {
                Some(running) => {
                    let pair = concat(&[running.as_ref(), partition_extreme.as_ref()])?;
                    extreme_key(&pair, descending)?
                }
                None => partition_extreme,
            });
        }
    }

    // No partition produced a batch, so there was nothing to reduce. Evaluating the key
    // expression over an empty batch gives a correctly typed empty array, whose reduction is
    // the null that represents "no buffered key".
    let extreme = match extreme {
        Some(extreme) => extreme,
        None => {
            let empty = RecordBatch::new_empty(buffered_schema);
            let keys = on_buffered.evaluate(&empty)?.into_array(0)?;
            extreme_key(&keys, descending)?
        }
    };

    // The one-row extreme is all this join type ever holds -- a few bytes, and unrelated to the
    // size of the buffered input.
    let size = extreme.get_array_memory_size();
    reservation.try_grow(size)?;
    metrics.build_mem_used.add(size);

    Ok(BufferedExtreme {
        extreme,
        _reservation: reservation,
    })
}

pub(super) enum BufferedSide {
    /// Indicates that build-side not collected yet
    Initial(BufferedSideInitialState),
    /// Indicates that build-side data has been collected
    Ready(BufferedSideReadyState),
}

impl BufferedSide {
    // Takes a mutable state of the buffered row batches
    pub(super) fn try_as_initial_mut(&mut self) -> Result<&mut BufferedSideInitialState> {
        match self {
            BufferedSide::Initial(state) => Ok(state),
            _ => internal_err!("Expected build side in initial state"),
        }
    }

    pub(super) fn try_as_ready(&self) -> Result<&BufferedSideReadyState> {
        match self {
            BufferedSide::Ready(state) => Ok(state),
            _ => {
                internal_err!("Expected build side in ready state")
            }
        }
    }

    /// Tries to extract BuildSideReadyState from BuildSide enum.
    /// Returns an error if state is not Ready.
    pub(super) fn try_as_ready_mut(&mut self) -> Result<&mut BufferedSideReadyState> {
        match self {
            BufferedSide::Ready(state) => Ok(state),
            _ => internal_err!("Expected build side in ready state"),
        }
    }
}

pub(super) struct BufferedSideInitialState {
    pub(crate) buffered_fut: OnceFut<BufferedSideData>,
}

pub(super) struct BufferedSideReadyState {
    /// Collected build-side data
    pub(super) buffered_data: Arc<BufferedSideData>,
}
