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
    array::{ArrayRef, BooleanBufferBuilder, RecordBatch},
    compute::concat_batches,
    util::bit_util,
};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::not_impl_err;
use datafusion_common::{internal_err, JoinSide, Result};
use datafusion_execution::{
    memory_pool::{MemoryConsumer, MemoryReservation},
    SendableRecordBatchStream,
};
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::{
    Distribution, LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalExprRef,
    PhysicalSortExpr,
};
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use futures::TryStreamExt;
use parking_lot::Mutex;
use std::fmt::Formatter;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::execution_plan::{boundedness_from_children, EmissionType};

use crate::joins::piecewise_merge_join::classic_join::{
    ClassicPWMJStream, PiecewiseMergeJoinStreamState,
};
use crate::joins::piecewise_merge_join::utils::{
    build_visited_indices_map, is_existence_join, is_right_existence_join,
};
use crate::joins::utils::asymmetric_join_output_partitioning;
use crate::{
    joins::{
        utils::{build_join_schema, BuildProbeJoinMetrics, OnceAsync, OnceFut},
        SharedBitmapBuilder,
    },
    metrics::ExecutionPlanMetricsSet,
    spill::get_record_batch_memory_size,
    ExecutionPlan, PlanProperties,
};
use crate::{DisplayAs, DisplayFormatType, ExecutionPlanProperties};

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
/// Existence joins are made magnitudes of times faster with a `PiecewiseMergeJoin` as we only need to find
/// the min/max value of the streamed side to be able to emit all matches on the buffered side. By putting
/// the side we need to mark onto the sorted buffer side, we can emit all these matches at once.
///
/// For less than operations (`<`) both inputs are to be sorted in descending order and vice versa for greater
/// than (`>`) operations. `SortExec` is used to enforce sorting on the buffered side and streamed side does not
/// need to be sorted due to only needing to find the min/max.
///
/// For Left Semi, Anti, and Mark joins we swap the inputs so that the marked side is on the buffered side.
///
/// The pseudocode for the algorithm looks like this:
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
/// For both types of joins, the buffered side must be sorted ascending for `Operator::Lt` (<) or
/// `Operator::LtEq` (<=) and descending for `Operator::Gt` (>) or `Operator::GtEq` (>=).
///
/// # Partitioning Logic
/// Piecewise Merge Join requires one buffered side partition + round robin partitioned stream side. A counter
/// is used in the buffered side to coordinate when all streamed partitions are finished execution. This allows
/// for processing the rest of the unmatched rows for Left and Full joins. The last partition that finishes
/// execution will be responsible for outputting the unmatched rows.
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
    /// Buffered data
    buffered_fut: OnceAsync<BufferedSideData>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Sort expressions - See above for more details [`PiecewiseMergeJoinExec`]
    ///
    /// The left sort order, descending for `<`, `<=` operations + ascending for `>`, `>=` operations
    left_child_plan_required_order: LexOrdering,
    /// The right sort order, descending for `<`, `<=` operations + ascending for `>`, `>=` operations
    /// Unsorted for mark joins
    #[expect(dead_code)]
    right_batch_required_orders: LexOrdering,

    /// This determines the sort order of all join columns used in sorting the stream and buffered execution plans.
    sort_options: SortOptions,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
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
        // TODO: Implement existence joins for PiecewiseMergeJoin
        if is_existence_join(join_type) {
            return not_impl_err!(
                "Existence Joins are currently not supported for PiecewiseMergeJoin"
            );
        }

        // Take the operator and enforce a sort order on the streamed + buffered side based on
        // the operator type.
        let sort_options = match operator {
            Operator::Lt | Operator::LtEq => {
                // For left existence joins the inputs will be swapped so the sort
                // options are switched
                if is_right_existence_join(join_type) {
                    SortOptions::new(false, true)
                } else {
                    SortOptions::new(true, true)
                }
            }
            Operator::Gt | Operator::GtEq => {
                if is_right_existence_join(join_type) {
                    SortOptions::new(true, true)
                } else {
                    SortOptions::new(false, true)
                }
            }
            _ => {
                return internal_err!(
                    "Cannot contain non-range operator in PiecewiseMergeJoinExec"
                )
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
            &on,
        )?;

        Ok(Self {
            streamed,
            buffered,
            on,
            operator,
            join_type,
            schema,
            buffered_fut: Default::default(),
            metrics: ExecutionPlanMetricsSet::new(),
            left_child_plan_required_order,
            right_batch_required_orders,
            sort_options,
            cache,
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
        join_on: &(PhysicalExprRef, PhysicalExprRef),
    ) -> Result<PlanProperties> {
        let eq_properties = join_equivalence_properties(
            buffered.equivalence_properties().clone(),
            streamed.equivalence_properties().clone(),
            &join_type,
            schema,
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side(&join_type)),
            std::slice::from_ref(join_on),
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
            // The existence side is expected to come in sorted
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                vec![false, false]
            }
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                vec![false, false]
            }
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
}

impl ExecutionPlan for PiecewiseMergeJoinExec {
    fn name(&self) -> &str {
        "PiecewiseMergeJoinExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.buffered, &self.streamed]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![
            Distribution::SinglePartition,
            Distribution::UnspecifiedDistribution,
        ]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // Existence joins don't need to be sorted on one side.
        if is_right_existence_join(self.join_type) {
            unimplemented!()
        } else {
            // Sort the right side in memory, so we do not need to enforce any sorting
            vec![
                Some(OrderingRequirements::from(
                    self.left_child_plan_required_order.clone(),
                )),
                None,
            ]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
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
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_buffered = Arc::clone(&self.on.0);
        let on_streamed = Arc::clone(&self.on.1);

        let metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);
        let buffered_fut = self.buffered_fut.try_once(|| {
            let reservation = MemoryConsumer::new("PiecewiseMergeJoinInput")
                .register(context.memory_pool());

            let buffered_stream = self.buffered.execute(0, Arc::clone(&context))?;
            Ok(build_buffered_data(
                buffered_stream,
                Arc::clone(&on_buffered),
                metrics.clone(),
                reservation,
                build_visited_indices_map(self.join_type),
                self.num_partitions,
            ))
        })?;

        let streamed = self.streamed.execute(partition, Arc::clone(&context))?;

        let batch_size = context.session_config().batch_size();

        // TODO: Add existence joins + this is guarded at physical planner
        if is_existence_join(self.join_type()) {
            unreachable!()
        } else {
            Ok(Box::pin(ClassicPWMJStream::try_new(
                Arc::clone(&self.schema),
                on_streamed,
                self.join_type,
                self.operator,
                streamed,
                BufferedSide::Initial(BufferedSideInitialState { buffered_fut }),
                PiecewiseMergeJoinStreamState::WaitBufferedSide,
                self.sort_options,
                metrics,
                batch_size,
            )))
        }
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
    build_map: bool,
    remaining_partitions: usize,
) -> Result<BufferedSideData> {
    let schema = buffered.schema();

    // Combine batches and record number of rows
    let initial = (Vec::new(), 0, metrics, reservation);
    let (batches, num_rows, metrics, mut reservation) = buffered
        .try_fold(initial, |mut acc, batch| async {
            let batch_size = get_record_batch_memory_size(&batch);
            acc.3.try_grow(batch_size)?;
            acc.2.build_mem_used.add(batch_size);
            acc.2.build_input_batches.add(1);
            acc.2.build_input_rows.add(batch.num_rows());
            // Update row count
            acc.1 += batch.num_rows();
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

    // Created visited indices bitmap only if the join type requires it
    let visited_indices_bitmap = if build_map {
        let bitmap_size = bit_util::ceil(single_batch.num_rows(), 8);
        reservation.try_grow(bitmap_size)?;
        metrics.build_mem_used.add(bitmap_size);

        let mut bitmap_buffer = BooleanBufferBuilder::new(single_batch.num_rows());
        bitmap_buffer.append_n(num_rows, false);
        bitmap_buffer
    } else {
        BooleanBufferBuilder::new(0)
    };

    let buffered_data = BufferedSideData::new(
        single_batch,
        buffered_values,
        Mutex::new(visited_indices_bitmap),
        remaining_partitions,
        reservation,
    );

    Ok(buffered_data)
}

pub(super) struct BufferedSideData {
    pub(super) batch: RecordBatch,
    values: ArrayRef,
    pub(super) visited_indices_bitmap: SharedBitmapBuilder,
    pub(super) remaining_partitions: AtomicUsize,
    _reservation: MemoryReservation,
}

impl BufferedSideData {
    pub(super) fn new(
        batch: RecordBatch,
        values: ArrayRef,
        visited_indices_bitmap: SharedBitmapBuilder,
        remaining_partitions: usize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            batch,
            values,
            visited_indices_bitmap,
            remaining_partitions: AtomicUsize::new(remaining_partitions),
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
