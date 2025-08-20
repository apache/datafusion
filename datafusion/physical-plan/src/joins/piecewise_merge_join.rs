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

use arrow::array::{
    new_null_array, Array, PrimitiveArray, PrimitiveBuilder, RecordBatchOptions,
};
use arrow::compute::take;
use arrow::datatypes::{UInt32Type, UInt64Type};
use arrow::{
    array::{
        ArrayRef, BooleanBufferBuilder, RecordBatch, UInt32Array, UInt32Builder,
        UInt64Array, UInt64Builder,
    },
    compute::{concat_batches, sort_to_indices, take_record_batch},
    util::bit_util,
};
use arrow_schema::{ArrowError, Schema, SchemaRef, SortOptions};
use datafusion_common::{
    exec_err, internal_err, plan_err, utils::compare_rows, JoinSide, Result, ScalarValue,
};
use datafusion_common::{not_impl_err, NullEquality};
use datafusion_execution::{
    memory_pool::{MemoryConsumer, MemoryReservation},
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_expr::{JoinType, Operator};
use datafusion_functions_aggregate_common::min_max::{max_batch, min_batch};
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::{
    LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalExprRef, PhysicalSortExpr,
};
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use std::fmt::Formatter;
use std::{cmp::Ordering, task::ready};
use std::{sync::Arc, task::Poll};

use crate::execution_plan::{boundedness_from_children, EmissionType};

use crate::joins::sort_merge_join::compare_join_arrays;
use crate::joins::utils::{
    get_final_indices_from_shared_bitmap, symmetric_join_output_partitioning,
};
use crate::{handle_state, DisplayAs, DisplayFormatType, ExecutionPlanProperties};
use crate::{
    joins::{
        utils::{
            build_join_schema, BuildProbeJoinMetrics, OnceAsync, OnceFut,
            StatefulStreamResult,
        },
        SharedBitmapBuilder,
    },
    metrics::ExecutionPlanMetricsSet,
    spill::get_record_batch_memory_size,
    ExecutionPlan, PlanProperties,
};

/// Batch emits this number of rows when processing
pub const DEFAULT_INCREMENTAL_BATCH_VALUE: usize = 1;

/// `PiecewiseMergeJoinExec` is a join execution plan that only evaluates single range filter.
///
/// The physical planner will choose to evalute this join when there is only one range predicate. This
/// is a binary expression which contains [`Operator::Lt`], [`Operator::LtEq`], [`Operator::Gt`], and
/// [`Operator::GtEq`].:
/// Examples:
///  - `col0` < `colb`, `col0` <= `colb`, `col0` > `colb`, `col0` >= `colb`
///
/// Since the join only support range predicates, equijoins are not supported in `PiecewiseMergeJoinExec`,
/// however you can first evaluate another join and run `PiecewiseMergeJoinExec` if left with one range
/// predicate.
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
/// For classic joins we buffer the right side (buffered), and incrementally process the left side (streamed).
/// Every streamed batch is sorted so we can perform a sort merge algorithm. For the buffered side we want to
/// have it already sorted either ascending or descending based on the operator as this allows us to emit all
/// the rows from a given point to the end as matches. Sorting the streamed side allows us to start the pointer
/// from the previous row's match on the buffered side.
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
///
/// ```
///
/// ## Existence Joins (Semi, Anti, Mark)
/// Existence joins are made magnitudes of times faster with a `PiecewiseMergeJoin` as we only need to find
/// the min/max value of the streamed side to be able to emit all matches on the buffered side. By putting
/// the side we need to mark onto the sorted buffer side, we can emit all these matches at once.
///
/// For Left Semi, Anti, and Mark joins we swap the inputs so that the marked side is on the buffered side.
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
/// FROM (VALUES (100), (200), (500)) AS streamed(a)
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
/// For both types of joins, the buffered side must be sorted ascending for `Operator::Lt`(<) or
/// `Operator::LtEq`(<=) and descending for `Operator::Gt`(>) or `Operator::GtEq`(>=).
///
/// ## Assumptions / Notation
/// - [R], [S]: number of pages (blocks) of R and S
/// - |R|, |S|: number of tuples in R and S
/// - B: number of buffer pages
///
/// # Performance (cost)
/// Piecewise Merge Join is used over Nested Loop Join due to its superior performance. Here is a breakdown
/// of the calculations:
///
/// ## Piecewise Merge Join (PWMJ)
/// Intuition: Keep the buffered side (R) sorted and in memory (or scan it in sorted order),
/// sort the streamed side (S), then merge in order while advancing a pivot on R.
///
/// Average I/O cost:
///   cost(PWMJ) = cost_to_sort(R) + cost_to_sort(S) + ([R] + [S])
///              = sort(R) + sort(S) + [R] + [S]
///
///   - If R (buffered) already sorted on the join key:     cost(PWMJ) = sort(S) + [R] + [S]
///   - If S already sorted and R not:                      cost(PWMJ) = sort(R) + [R] + [S]
///   - If both already sorted:                             cost(PWMJ) = [R] + [S]
///
/// ## Nested Loop Join
///   cost(NLJ) ≈ [R] + |R|·[S]
///
/// Takeaway:
///   - When at least one side needs sorting, PWMJ ≈ sort(R) + sort(S) + [R] + [S] on average,
///     typically beating NLJ’s |R|·[S] (or its buffered variant) for nontrivial |R|, [S].
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
    /// The left SortExpr
    left_sort_exprs: LexOrdering,
    /// The right SortExpr
    right_sort_exprs: LexOrdering,
    /// Sort options of join columns used in sorting the stream and buffered execution plans
    sort_options: SortOptions,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl PiecewiseMergeJoinExec {
    pub fn try_new(
        buffered: Arc<dyn ExecutionPlan>,
        streamed: Arc<dyn ExecutionPlan>,
        on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<Self> {
        // TODO: Implement mark joins for PiecewiseMergeJoin
        if matches!(join_type, JoinType::LeftMark | JoinType::RightMark) {
            return not_impl_err!(
                "Mark Joins are currently not supported for PiecewiseMergeJoin"
            );
        }

        // Take the operator and enforce a sort order on the streamed + buffered side based on
        // the operator type.
        let sort_options = match operator {
            Operator::Lt | Operator::LtEq => {
                // For left existence joins the inputs will be swapped so the sort
                // options are switched
                if is_right_existence_join(join_type) {
                    SortOptions::new(false, false)
                } else {
                    SortOptions::new(true, false)
                }
            }
            Operator::Gt | Operator::GtEq => {
                if is_right_existence_join(join_type) {
                    SortOptions::new(true, false)
                } else {
                    SortOptions::new(false, false)
                }
            }
            _ => {
                return plan_err!(
                    "Cannot contain non-range operator in PiecewiseMergeJoinExec"
                )
            }
        };

        // Give the same `sort_option for comparison later`
        let left_sort_exprs =
            vec![PhysicalSortExpr::new(Arc::clone(&on.0), sort_options)];
        let right_sort_exprs =
            vec![PhysicalSortExpr::new(Arc::clone(&on.1), sort_options)];

        let Some(left_sort_exprs) = LexOrdering::new(left_sort_exprs) else {
            return plan_err!(
                "PiecewiseMergeJoinExec requires valid sort expressions for its left side"
            );
        };
        let Some(right_sort_exprs) = LexOrdering::new(right_sort_exprs) else {
            return plan_err!(
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
            left_sort_exprs,
            right_sort_exprs,
            sort_options,
            cache,
        })
    }

    /// Refeerence to buffered side execution plan
    pub fn buffered(&self) -> &Arc<dyn ExecutionPlan> {
        &self.buffered
    }

    /// Refeference to streamed side execution plan
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
            &[join_on.clone()],
        )?;

        let output_partitioning =
            symmetric_join_output_partitioning(buffered, streamed, &join_type)?;

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            boundedness_from_children([buffered, streamed]),
        ))
    }

    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        match join_type {
            // The existence side is expected to come in sorted
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                vec![false, true]
            }
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                vec![true, false]
            }
            // Left, Right, Full, Inner Join is not guaranteed to maintain
            // input order as the streamed side will be sorted during
            // execution for `PiecewiseMergeJoin`
            _ => vec![false, false],
        }
    }

    // TODO: We implement this with the physical planner.
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

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // Existence joins don't need to be sorted on one side.
        if is_right_existence_join(self.join_type) {
            // Right side needs to be sorted because this will be swapped to the
            // buffered side
            vec![
                None,
                Some(OrderingRequirements::from(self.left_sort_exprs.clone())),
            ]
        } else {
            // Sort the right side in memory, so we do not need to enforce any sorting
            vec![
                Some(OrderingRequirements::from(self.right_sort_exprs.clone())),
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

        // If the join type is either RightSemi, RightAnti, or RightMark we will swap the inputs
        // and sort ordering because we want the mark side to be the buffered side.
        let (buffered, streamed, on_buffered, on_streamed, operator) =
            if is_right_existence_join(self.join_type) {
                (
                    Arc::clone(&self.streamed),
                    Arc::clone(&self.buffered),
                    on_streamed,
                    on_buffered,
                    self.operator.swap().unwrap(),
                )
            } else {
                (
                    Arc::clone(&self.buffered),
                    Arc::clone(&self.streamed),
                    on_buffered,
                    on_streamed,
                    self.operator,
                )
            };

        let metrics = BuildProbeJoinMetrics::new(0, &self.metrics);
        let buffered_fut = self.buffered_fut.try_once(|| {
            let reservation = MemoryConsumer::new("PiecewiseMergeJoinInput")
                .register(context.memory_pool());
            let buffered_stream = buffered.execute(partition, Arc::clone(&context))?;
            Ok(build_buffered_data(
                buffered_stream,
                Arc::clone(&on_buffered),
                metrics.clone(),
                reservation,
                build_visited_indices_map(self.join_type),
            ))
        })?;

        let streamed = streamed.execute(partition, Arc::clone(&context))?;
        let existence_join = is_existence_join(self.join_type());

        Ok(Box::pin(PiecewiseMergeJoinStream::try_new(
            Arc::clone(&self.schema),
            on_streamed,
            self.join_type,
            operator,
            streamed,
            BufferedSide::Initial(BufferedSideInitialState { buffered_fut }),
            if existence_join {
                PiecewiseMergeJoinStreamState::FetchStreamBatch
            } else {
                PiecewiseMergeJoinStreamState::WaitBufferedSide
            },
            existence_join,
            self.sort_options,
            metrics,
        )))
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

// Returns boolean for whether the join is an existence join
fn is_existence_join(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftMark
            | JoinType::RightMark
    )
}

// Returns boolean for whether the join is a right existence join
fn is_right_existence_join(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::RightAnti | JoinType::RightSemi | JoinType::RightMark
    )
}

// Returns boolean to check if the join type needs to record
// buffered side matches for classic joins
fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Full | JoinType::Left)
}

// Returns boolean for whether or not we need to build the buffered side
// bitmap for marking matched rows on the buffered side.
fn build_visited_indices_map(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Full
            | JoinType::Left
            | JoinType::LeftAnti
            | JoinType::RightAnti
            | JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftMark
            | JoinType::RightMark
    )
}

async fn build_buffered_data(
    buffered: SendableRecordBatchStream,
    on_buffered: PhysicalExprRef,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    build_map: bool,
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

    let batches_iter = batches.iter().rev();
    let single_batch = concat_batches(&schema, batches_iter)?;

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
        reservation,
    );

    Ok(buffered_data)
}

struct BufferedSideData {
    batch: RecordBatch,
    values: ArrayRef,
    visited_indices_bitmap: SharedBitmapBuilder,
    _reservation: MemoryReservation,
}

impl BufferedSideData {
    fn new(
        batch: RecordBatch,
        values: ArrayRef,
        visited_indices_bitmap: SharedBitmapBuilder,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            batch,
            values,
            visited_indices_bitmap,
            _reservation: reservation,
        }
    }

    fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    fn values(&self) -> &ArrayRef {
        &self.values
    }
}

enum BufferedSide {
    /// Indicates that build-side not collected yet
    Initial(BufferedSideInitialState),
    /// Indicates that build-side data has been collected
    Ready(BufferedSideReadyState),
}

impl BufferedSide {
    // Takes a mutable state of the buffered row batches
    fn try_as_initial_mut(&mut self) -> Result<&mut BufferedSideInitialState> {
        match self {
            BufferedSide::Initial(state) => Ok(state),
            _ => internal_err!("Expected build side in initial state"),
        }
    }

    fn try_as_ready(&self) -> Result<&BufferedSideReadyState> {
        match self {
            BufferedSide::Ready(state) => Ok(state),
            _ => {
                internal_err!("Expected build side in ready state")
            }
        }
    }

    /// Tries to extract BuildSideReadyState from BuildSide enum.
    /// Returns an error if state is not Ready.
    fn try_as_ready_mut(&mut self) -> Result<&mut BufferedSideReadyState> {
        match self {
            BufferedSide::Ready(state) => Ok(state),
            _ => internal_err!("Expected build side in ready state"),
        }
    }
}

struct BufferedSideInitialState {
    buffered_fut: OnceFut<BufferedSideData>,
}

struct BufferedSideReadyState {
    /// Collected build-side data
    buffered_data: Arc<BufferedSideData>,
}

enum PiecewiseMergeJoinStreamState {
    WaitBufferedSide,
    FetchStreamBatch,
    ProcessStreamBatch(StreamedBatch),
    ExhaustedStreamSide,
    Completed,
}

impl PiecewiseMergeJoinStreamState {
    // Grab mutable reference to the current stream batch
    fn try_as_process_stream_batch_mut(&mut self) -> Result<&mut StreamedBatch> {
        match self {
            PiecewiseMergeJoinStreamState::ProcessStreamBatch(state) => Ok(state),
            _ => internal_err!("Expected streamed batch in StreamBatch"),
        }
    }
}

struct StreamedBatch {
    pub batch: RecordBatch,
    values: Vec<ArrayRef>,
}

impl StreamedBatch {
    fn new(batch: RecordBatch, values: Vec<ArrayRef>) -> Self {
        Self { batch, values }
    }

    fn values(&self) -> &Vec<ArrayRef> {
        &self.values
    }
}

struct PiecewiseMergeJoinStream {
    // Output schema of the `PiecewiseMergeJoin`
    pub schema: Arc<Schema>,

    // Physical expression that is evaluated on the streamed side
    // We do not need on_buffered as this is already evaluated when
    // creating the buffered side which happens before initializing
    // `PiecewiseMergeJoinStream`
    pub on_streamed: PhysicalExprRef,
    // Type of join
    pub join_type: JoinType,
    // Comparison operator
    pub operator: Operator,
    // Streamed batch
    pub streamed: SendableRecordBatchStream,
    // Streamed schema
    streamed_schema: SchemaRef,
    // Buffered side data
    buffered_side: BufferedSide,
    // Stores the min max value for the streamed side, only needed
    // for existence joins.
    streamed_global_min_max: Mutex<Option<ScalarValue>>,
    // Tracks the state of the `PiecewiseMergeJoin`
    state: PiecewiseMergeJoinStreamState,
    // Flag for whehter or not the join_type is an existence join.
    existence_join: bool,
    // Sort option for buffered and streamed side (specifies whether
    // the sort is ascending or descending)
    sort_option: SortOptions,
    // Metrics for build + probe joins
    join_metrics: BuildProbeJoinMetrics,
    // Tracking incremental state for emitting record batches
    batch_process_state: BatchProcessState,
}

impl RecordBatchStream for PiecewiseMergeJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// `PiecewiseMergeJoinStreamState` is separated into `WaitBufferedSide`, `FetchStreamBatch`,
// `ProcessStreamBatch`, `ExhaustedStreamSide` and `Completed`. Classic joins and existence
// joins have a different processing order and behaviour for these states.
//
// Classic Joins
//  1. `WaitBufferedSide` - Load in the buffered side data into memory.
//  2. `FetchStreamBatch` -  Fetch + sort incoming stream batches. We switch the state to
//      `ExhaustedStreamBatch` once stream batches are exhausted.
//  3. `ProcessStreamBatch` - Compare stream batch row values against the buffered side data.
//  4. `ExhaustedStreamBatch` - If the join type is Left or Inner we will return state as
//      `Completed` however for Full and Right we will need to process the matched/unmatched rows.
//
// Existence Joins
//  1. `FetchStreamBatch` - Fetch incoming stream batch until exhausted. We find the min/max variable
//      within this state. We switch the state to `WaitBufferedSide` once we have exhausted all stream
//      batches.
//  3. `WaitBufferedSide` - Load buffered side data into memory.
//  4. `ExhaustedStreamBatch` - Use the global minimum or maximum value to find the matches on
//      the buffered side.
impl PiecewiseMergeJoinStream {
    // Creates a new `PiecewiseMergeJoinStream` instance
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        schema: Arc<Schema>,
        on_streamed: PhysicalExprRef,
        join_type: JoinType,
        operator: Operator,
        streamed: SendableRecordBatchStream,
        buffered_side: BufferedSide,
        state: PiecewiseMergeJoinStreamState,
        existence_join: bool,
        sort_option: SortOptions,
        join_metrics: BuildProbeJoinMetrics,
    ) -> Self {
        let streamed_schema = streamed.schema();
        Self {
            schema,
            on_streamed,
            join_type,
            operator,
            streamed_schema,
            streamed,
            buffered_side,
            streamed_global_min_max: Mutex::new(None),
            state,
            existence_join,
            sort_option,
            join_metrics,
            batch_process_state: BatchProcessState::new(),
        }
    }

    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                PiecewiseMergeJoinStreamState::WaitBufferedSide => {
                    handle_state!(ready!(self.collect_buffered_side(cx)))
                }
                PiecewiseMergeJoinStreamState::FetchStreamBatch => {
                    handle_state!(ready!(self.fetch_stream_batch(cx)))
                }
                PiecewiseMergeJoinStreamState::ProcessStreamBatch(_) => {
                    handle_state!(self.process_stream_batch())
                }
                PiecewiseMergeJoinStreamState::ExhaustedStreamSide => {
                    handle_state!(self.process_unmatched_buffered_batch())
                }
                PiecewiseMergeJoinStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    // Collects buffered side data
    fn collect_buffered_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        let buffered_data = ready!(self
            .buffered_side
            .try_as_initial_mut()?
            .buffered_fut
            .get_shared(cx))?;
        build_timer.done();

        self.state = if self.existence_join {
            // For existence joins we will start to compare the buffered
            // side to the global max min to get matches.
            PiecewiseMergeJoinStreamState::ExhaustedStreamSide
        } else {
            // We will start fetching stream batches for classic joins
            PiecewiseMergeJoinStreamState::FetchStreamBatch
        };

        self.buffered_side =
            BufferedSide::Ready(BufferedSideReadyState { buffered_data });

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    // Fetches incoming stream batches
    fn fetch_stream_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.streamed.poll_next_unpin(cx)) {
            None => {
                if self.existence_join {
                    self.state = PiecewiseMergeJoinStreamState::WaitBufferedSide;
                } else {
                    self.state = PiecewiseMergeJoinStreamState::ExhaustedStreamSide;
                }
            }
            Some(Ok(batch)) => {
                // Evaluate the streamed physical expression on the stream batch
                let stream_values: ArrayRef = self
                    .on_streamed
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                // For existence joins we do not need to sort the output, and we only need to
                // find the min or max value (depending on the operator) of all the stream batches
                if self.existence_join {
                    // Run timer during this phase as finding the min/max on streamed side is considered join time.
                    let timer = self.join_metrics.join_time.timer();
                    let mut global_min_max = self.streamed_global_min_max.lock();
                    let streamed_batch = StreamedBatch::new(batch, vec![stream_values]);

                    // Finds the min/max value of the streamed batch and compares it against the global
                    // min/max
                    resolve_existence_join(
                        &streamed_batch,
                        &mut global_min_max,
                        self.operator,
                    )
                    .unwrap();
                    timer.done();

                    self.state = PiecewiseMergeJoinStreamState::FetchStreamBatch;
                    return Poll::Ready(Ok(StatefulStreamResult::Continue));
                }

                // Sort stream values and change the streamed record batch accordingly
                let indices = sort_to_indices(
                    stream_values.as_ref(),
                    Some(self.sort_option),
                    None,
                )?;
                let stream_batch = take_record_batch(&batch, &indices)?;
                let stream_values = take(stream_values.as_ref(), &indices, None)?;

                self.state =
                    PiecewiseMergeJoinStreamState::ProcessStreamBatch(StreamedBatch {
                        batch: stream_batch,
                        values: vec![stream_values],
                    });
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    // Only classic join will call. This function will process stream batches and evaluate against
    // the buffered side data.
    fn process_stream_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let buffered_side = self.buffered_side.try_as_ready_mut()?;
        let stream_batch = self.state.try_as_process_stream_batch_mut()?;

        let batch = resolve_classic_join(
            buffered_side,
            stream_batch,
            Arc::clone(&self.schema),
            self.operator,
            self.sort_option,
            self.join_type,
            &mut self.batch_process_state,
        )?;

        if self.batch_process_state.continue_process {
            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        self.state = PiecewiseMergeJoinStreamState::FetchStreamBatch;
        Ok(StatefulStreamResult::Ready(Some(batch)))
    }

    // Process remaining unmatched rows
    fn process_unmatched_buffered_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        // Return early for `JoinType::Right` and `JoinType::Inner`
        if matches!(self.join_type, JoinType::Right | JoinType::Inner) {
            self.state = PiecewiseMergeJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Ready(None));
        }

        let timer = self.join_metrics.join_time.timer();

        let buffered_data =
            Arc::clone(&self.buffered_side.try_as_ready().unwrap().buffered_data);

        // Check if the same batch needs to be checked for values again
        if let Some(start_idx) = self.batch_process_state.process_rest {
            if let Some(buffered_indices) = &self.batch_process_state.buffered_indices {
                let remaining = buffered_indices.len() - start_idx;

                // Branch into this and return value if there are more rows to deal with
                if remaining > DEFAULT_INCREMENTAL_BATCH_VALUE {
                    let buffered_batch = buffered_data.batch();
                    let empty_stream_batch =
                        RecordBatch::new_empty(Arc::clone(&self.streamed_schema));

                    let buffered_chunk_ref = buffered_indices
                        .slice(start_idx, DEFAULT_INCREMENTAL_BATCH_VALUE);
                    let new_buffered_indices = buffered_chunk_ref
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .expect("downcast to UInt64Array after slice");

                    let streamed_indices: UInt32Array =
                        (0..new_buffered_indices.len() as u32).collect();

                    let batch = build_matched_indices(
                        self.join_type,
                        Arc::clone(&self.schema),
                        &empty_stream_batch,
                        buffered_batch,
                        streamed_indices,
                        new_buffered_indices.clone(),
                    )?;

                    self.batch_process_state.set_process_rest(Some(
                        start_idx + DEFAULT_INCREMENTAL_BATCH_VALUE,
                    ));
                    self.batch_process_state.continue_process = true;

                    return Ok(StatefulStreamResult::Ready(Some(batch)));
                }

                let buffered_batch = buffered_data.batch();
                let empty_stream_batch =
                    RecordBatch::new_empty(Arc::clone(&self.streamed_schema));

                let buffered_chunk_ref = buffered_indices.slice(start_idx, remaining);
                let new_buffered_indices = buffered_chunk_ref
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("downcast to UInt64Array after slice");

                let streamed_indices: UInt32Array =
                    (0..new_buffered_indices.len() as u32).collect();

                let batch = build_matched_indices(
                    self.join_type,
                    Arc::clone(&self.schema),
                    &empty_stream_batch,
                    buffered_batch,
                    streamed_indices,
                    new_buffered_indices.clone(),
                )?;

                self.batch_process_state.reset();

                timer.done();
                self.join_metrics.output_batches.add(1);
                self.state = PiecewiseMergeJoinStreamState::Completed;

                return Ok(StatefulStreamResult::Ready(Some(batch)));
            }

            return exec_err!("Batch process state should hold buffered indices");
        }

        // For Semi/Anti/Mark joins that mark indices on the buffered side, and retrieve final indices from
        // `get_final_indices_bitmap`
        if matches!(
            self.join_type,
            JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::RightMark
        ) {
            let global_min_max = self.streamed_global_min_max.lock();
            let threshold = match &*global_min_max {
                Some(v) => v.clone(),
                // This shouldn't be possible
                None => return exec_err!("Stream batch was empty."),
            };

            let buffered_values = buffered_data.values();
            let mut threshold_idx: Option<usize> = None;

            // Iterate the buffered size values while comparing the threshold value (min/max)
            // and record our first match
            for buffered_idx in 0..buffered_data.values.len() {
                let buffered_value =
                    ScalarValue::try_from_array(&buffered_values, buffered_idx)?;
                let ord = compare_rows(
                    &[threshold.clone()],
                    &[buffered_value.clone()],
                    &[self.sort_option],
                )?;

                // Decide “past the threshold” by operator
                let keep = match self.operator {
                    Operator::Gt | Operator::Lt => ord == Ordering::Less,
                    Operator::GtEq | Operator::LtEq => {
                        ord == Ordering::Less || ord == Ordering::Equal
                    }
                    _ => false,
                };

                // Record match
                if keep {
                    threshold_idx = Some(buffered_idx);
                    break;
                }
            }

            let mut buffered_indices = UInt64Builder::default();

            // If a match is found then append all indices from the threshold index
            // to the end of the buffered size rows
            if let Some(threshold_idx) = threshold_idx {
                let buffered_range: Vec<u64> =
                    (threshold_idx as u64..buffered_data.values.len() as u64).collect();
                buffered_indices.append_slice(&buffered_range);
            }

            let buffered_indices_array = buffered_indices.finish();

            // Mark bitmap here because the visited bitmap hasn't been marked yet for existence joins
            let mut bitmap = buffered_data.visited_indices_bitmap.lock();
            buffered_indices_array.iter().flatten().for_each(|x| {
                bitmap.set_bit(x as usize, true);
            });
        }

        // Pass in piecewise flag to allow Right Semi/Anti/Mark joins to also be processed
        let (buffered_indices, streamed_indices) = get_final_indices_from_shared_bitmap(
            &buffered_data.visited_indices_bitmap,
            self.join_type,
            true,
        );

        // If the output indices is larger than the limit for the incremental batching then
        // proceed to outputting all matches up to that index, return batch, and the matching
        // will start next on the updated index (`process_rest`)
        if buffered_indices.len() > DEFAULT_INCREMENTAL_BATCH_VALUE {
            let buffered_batch = buffered_data.batch();
            let empty_stream_batch =
                RecordBatch::new_empty(Arc::clone(&self.streamed_schema));

            let indices_chunk_ref = buffered_indices.slice(
                self.batch_process_state.start_idx,
                DEFAULT_INCREMENTAL_BATCH_VALUE,
            );

            let indices_chunk = indices_chunk_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("downcast to UInt64Array after slice");

            let batch = build_matched_indices(
                self.join_type,
                Arc::clone(&self.schema),
                &empty_stream_batch,
                buffered_batch,
                streamed_indices,
                indices_chunk.clone(),
            )?;

            self.batch_process_state.buffered_indices = Some(buffered_indices);
            self.batch_process_state
                .set_process_rest(Some(DEFAULT_INCREMENTAL_BATCH_VALUE));
            self.batch_process_state.continue_process = true;

            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        let buffered_batch = buffered_data.batch();
        let empty_stream_batch =
            RecordBatch::new_empty(Arc::clone(&self.streamed_schema));

        let batch = build_matched_indices(
            self.join_type,
            Arc::clone(&self.schema),
            &empty_stream_batch,
            buffered_batch,
            streamed_indices,
            buffered_indices,
        )?;

        timer.done();
        self.join_metrics.output_batches.add(1);
        self.state = PiecewiseMergeJoinStreamState::Completed;

        Ok(StatefulStreamResult::Ready(Some(batch)))
    }
}

// Holds all information for processing incremental output
struct BatchProcessState {
    // Used to pick up from the last index on the stream side
    start_idx: usize,
    // Used to pick up from the last index on the buffered side
    pivot: usize,
    // Tracks the number of rows processed; default starts at 0
    num_rows: usize,
    // Processes the rest of the batch
    process_rest: Option<usize>,
    // Used to skip fully processing the row
    not_found: bool,
    // Signals whether to call `ProcessStreamBatch` again
    continue_process: bool,
    // Holding the buffered indices when processing the remaining marked rows.
    buffered_indices: Option<PrimitiveArray<UInt64Type>>,
}

impl BatchProcessState {
    pub fn new() -> Self {
        Self {
            start_idx: 0,
            num_rows: 0,
            pivot: 0,
            process_rest: None,
            not_found: false,
            continue_process: false,
            buffered_indices: None,
        }
    }

    pub fn reset(&mut self) {
        self.start_idx = 0;
        self.num_rows = 0;
        self.pivot = 0;
        self.process_rest = None;
        self.not_found = false;
        self.continue_process = false;
        self.buffered_indices = None;
    }

    pub fn pivot(&self) -> usize {
        self.pivot
    }

    pub fn set_pivot(&mut self, pivot: usize) {
        self.pivot += pivot;
    }

    pub fn set_start_idx(&mut self, start_idx: usize) {
        self.start_idx = start_idx;
    }

    pub fn set_rows(&mut self, num_rows: usize) {
        self.num_rows = num_rows;
    }

    pub fn set_process_rest(&mut self, process_rest: Option<usize>) {
        self.process_rest = process_rest;
    }
}

impl Stream for PiecewiseMergeJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

// For Semi, Anti, and Mark joins, we are only required to have to return the marked side, so
// we just need to find the min/max value of either side.
fn resolve_existence_join(
    stream_batch: &StreamedBatch,
    global_min_max: &mut Option<ScalarValue>,
    operator: Operator,
) -> Result<()> {
    let min_max_value = global_min_max;

    // Based on the operator we will find the minimum or maximum value.
    match operator {
        Operator::Lt | Operator::LtEq => {
            let max_value = max_batch(&stream_batch.values[0])?;
            let new_max = if let Some(prev) = (*min_max_value).clone() {
                if max_value.partial_cmp(&prev).unwrap() == Ordering::Greater {
                    max_value
                } else {
                    prev
                }
            } else {
                max_value
            };
            *min_max_value = Some(new_max);
        }

        Operator::Gt | Operator::GtEq => {
            let min_value = min_batch(&stream_batch.values[0])?;
            let new_min = if let Some(prev) = (*min_max_value).clone() {
                if min_value.partial_cmp(&prev).unwrap() == Ordering::Less {
                    min_value
                } else {
                    prev
                }
            } else {
                min_value
            };
            *min_max_value = Some(new_min);
        }
        _ => {
            return exec_err!(
                "PiecewiseMergeJoin should not contain operator, {}",
                operator
            )
        }
    };

    Ok(())
}

// For Left, Right, Full, and Inner joins, incoming stream batches will already be sorted.
fn resolve_classic_join(
    buffered_side: &mut BufferedSideReadyState,
    stream_batch: &StreamedBatch,
    join_schema: Arc<Schema>,
    operator: Operator,
    sort_options: SortOptions,
    join_type: JoinType,
    batch_process_state: &mut BatchProcessState,
) -> Result<RecordBatch> {
    let buffered_values = buffered_side.buffered_data.values();
    let buffered_len = buffered_values.len();
    let stream_values = stream_batch.values();

    let mut buffered_indices = UInt64Builder::default();
    let mut stream_indices = UInt32Builder::default();

    // Our pivot variable allows us to start probing on the buffered side where we last matched
    // in the previous stream row.
    let mut pivot = batch_process_state.pivot();
    for row_idx in batch_process_state.start_idx..stream_values[0].len() {
        let mut found = false;

        // Check once to see if it is a redo of a null value if not we do not try to process the batch
        if !batch_process_state.not_found {
            while pivot < buffered_values.len()
                || batch_process_state.process_rest.is_some()
            {
                // If there is still data left in the batch to process, use the index and output
                if let Some(start_idx) = batch_process_state.process_rest {
                    let count = buffered_values.len() - start_idx;
                    if count >= DEFAULT_INCREMENTAL_BATCH_VALUE {
                        let stream_repeated =
                            vec![row_idx as u32; DEFAULT_INCREMENTAL_BATCH_VALUE];
                        batch_process_state.set_process_rest(Some(
                            start_idx + DEFAULT_INCREMENTAL_BATCH_VALUE,
                        ));
                        batch_process_state.set_rows(
                            batch_process_state.num_rows
                                + DEFAULT_INCREMENTAL_BATCH_VALUE,
                        );
                        let buffered_range: Vec<u64> = (start_idx as u64
                            ..((start_idx as u64)
                                + (DEFAULT_INCREMENTAL_BATCH_VALUE as u64)))
                            .collect();
                        stream_indices.append_slice(&stream_repeated);
                        buffered_indices.append_slice(&buffered_range);

                        let batch = process_batch(
                            &mut buffered_indices,
                            &mut stream_indices,
                            stream_batch,
                            buffered_side,
                            join_type,
                            join_schema,
                        )?;
                        batch_process_state.continue_process = true;
                        batch_process_state.set_rows(0);

                        return Ok(batch);
                    }

                    batch_process_state.set_rows(batch_process_state.num_rows + count);
                    let stream_repeated = vec![row_idx as u32; count];
                    let buffered_range: Vec<u64> =
                        (start_idx as u64..buffered_len as u64).collect();
                    stream_indices.append_slice(&stream_repeated);
                    buffered_indices.append_slice(&buffered_range);
                    batch_process_state.process_rest = None;

                    found = true;

                    break;
                }

                let compare = compare_join_arrays(
                    &[Arc::clone(&stream_values[0])],
                    row_idx,
                    &[Arc::clone(buffered_values)],
                    pivot,
                    &[sort_options],
                    NullEquality::NullEqualsNothing,
                )?;

                // If we find a match we append all indices and move to the next stream row index
                match operator {
                    Operator::Gt | Operator::Lt => {
                        if matches!(compare, Ordering::Less) {
                            let count = buffered_values.len() - pivot;

                            // If the current output + new output is over our process value then we want to be
                            // able to change that
                            if batch_process_state.num_rows + count
                                >= DEFAULT_INCREMENTAL_BATCH_VALUE
                            {
                                let process_batch_size = DEFAULT_INCREMENTAL_BATCH_VALUE
                                    - batch_process_state.num_rows;
                                let stream_repeated =
                                    vec![row_idx as u32; process_batch_size];
                                batch_process_state.set_rows(
                                    batch_process_state.num_rows + process_batch_size,
                                );
                                let buffered_range: Vec<u64> = (pivot as u64
                                    ..(pivot + process_batch_size) as u64)
                                    .collect();
                                stream_indices.append_slice(&stream_repeated);
                                buffered_indices.append_slice(&buffered_range);

                                let batch = process_batch(
                                    &mut buffered_indices,
                                    &mut stream_indices,
                                    stream_batch,
                                    buffered_side,
                                    join_type,
                                    join_schema,
                                )?;

                                batch_process_state
                                    .set_process_rest(Some(pivot + process_batch_size));
                                batch_process_state.continue_process = true;
                                // Update the start index so it repeats the process
                                batch_process_state.set_start_idx(row_idx);
                                batch_process_state.set_pivot(pivot);
                                batch_process_state.set_rows(0);

                                return Ok(batch);
                            }

                            // Update the number of rows processed
                            batch_process_state
                                .set_rows(batch_process_state.num_rows + count);
                            let stream_repeated = vec![row_idx as u32; count];
                            let buffered_range: Vec<u64> =
                                (pivot as u64..buffered_len as u64).collect();

                            stream_indices.append_slice(&stream_repeated);
                            buffered_indices.append_slice(&buffered_range);
                            found = true;

                            break;
                        }
                    }
                    Operator::GtEq | Operator::LtEq => {
                        if matches!(compare, Ordering::Equal | Ordering::Less) {
                            let count = buffered_values.len() - pivot;

                            // If the current output + new output is over our process value then we want to be
                            // able to change that
                            if batch_process_state.num_rows + count
                                >= DEFAULT_INCREMENTAL_BATCH_VALUE
                            {
                                // Update the start index so it repeats the process
                                batch_process_state.set_start_idx(row_idx);
                                batch_process_state.set_pivot(pivot);

                                let process_batch_size = DEFAULT_INCREMENTAL_BATCH_VALUE
                                    - batch_process_state.num_rows;
                                let stream_repeated =
                                    vec![row_idx as u32; process_batch_size];
                                batch_process_state
                                    .set_process_rest(Some(pivot + process_batch_size));
                                batch_process_state.set_rows(
                                    batch_process_state.num_rows + process_batch_size,
                                );
                                let buffered_range: Vec<u64> = (pivot as u64
                                    ..(pivot + process_batch_size) as u64)
                                    .collect();
                                stream_indices.append_slice(&stream_repeated);
                                buffered_indices.append_slice(&buffered_range);

                                let batch = process_batch(
                                    &mut buffered_indices,
                                    &mut stream_indices,
                                    stream_batch,
                                    buffered_side,
                                    join_type,
                                    join_schema,
                                )?;

                                batch_process_state.continue_process = true;
                                batch_process_state.set_rows(0);

                                return Ok(batch);
                            }

                            // Update the number of rows processed
                            batch_process_state
                                .set_rows(batch_process_state.num_rows + count);
                            let stream_repeated = vec![row_idx as u32; count];
                            let buffered_range: Vec<u64> =
                                (pivot as u64..buffered_len as u64).collect();

                            stream_indices.append_slice(&stream_repeated);
                            buffered_indices.append_slice(&buffered_range);
                            found = true;

                            break;
                        }
                    }
                    _ => {
                        return exec_err!(
                            "PiecewiseMergeJoin should not contain operator, {}",
                            operator
                        )
                    }
                };

                // Increment pivot after every row
                pivot += 1;
            }
        }

        // If not found we append a null value for `JoinType::Right` and `JoinType::Full`
        if (!found || batch_process_state.not_found)
            && matches!(join_type, JoinType::Right | JoinType::Full)
        {
            let remaining = DEFAULT_INCREMENTAL_BATCH_VALUE
                .saturating_sub(batch_process_state.num_rows);
            if remaining == 0 {
                let batch = process_batch(
                    &mut buffered_indices,
                    &mut stream_indices,
                    stream_batch,
                    buffered_side,
                    join_type,
                    join_schema,
                )?;

                // Update the start index so it repeats the process
                batch_process_state.set_start_idx(row_idx);
                batch_process_state.set_pivot(pivot);
                batch_process_state.not_found = true;
                batch_process_state.continue_process = true;
                batch_process_state.set_rows(0);

                return Ok(batch);
            }

            // Append right side value + null value for left
            stream_indices.append_value(row_idx as u32);
            buffered_indices.append_null();
            batch_process_state.set_rows(batch_process_state.num_rows + 1);
            batch_process_state.not_found = false;
        }
    }

    let batch = process_batch(
        &mut buffered_indices,
        &mut stream_indices,
        stream_batch,
        buffered_side,
        join_type,
        join_schema,
    )?;

    // Resets batch process state for processing `Left` + `Full` join
    batch_process_state.reset();

    Ok(batch)
}

fn process_batch(
    buffered_indices: &mut PrimitiveBuilder<UInt64Type>,
    stream_indices: &mut PrimitiveBuilder<UInt32Type>,
    stream_batch: &StreamedBatch,
    buffered_side: &mut BufferedSideReadyState,
    join_type: JoinType,
    join_schema: Arc<Schema>,
) -> Result<RecordBatch> {
    let stream_indices_array = stream_indices.finish();
    let buffered_indices_array = buffered_indices.finish();

    // We need to mark the buffered side matched indices for `JoinType::Full` and `JoinType::Left`
    if need_produce_result_in_final(join_type) {
        let mut bitmap = buffered_side.buffered_data.visited_indices_bitmap.lock();

        buffered_indices_array.iter().flatten().for_each(|i| {
            bitmap.set_bit(i as usize, true);
        });
    }

    let batch = build_matched_indices(
        join_type,
        join_schema,
        &stream_batch.batch,
        &buffered_side.buffered_data.batch,
        stream_indices_array,
        buffered_indices_array,
    )?;

    Ok(batch)
}

fn build_matched_indices(
    join_type: JoinType,
    schema: Arc<Schema>,
    streamed_batch: &RecordBatch,
    buffered_batch: &RecordBatch,
    streamed_indices: UInt32Array,
    buffered_indices: UInt64Array,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        // Build an “empty” RecordBatch with just row‐count metadata
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(streamed_indices.len()));
        return Ok(RecordBatch::try_new_with_options(
            Arc::new((*schema).clone()),
            vec![],
            &options,
        )?);
    }

    // Gather stream columns after applying filter specified with stream indices
    let streamed_columns = if !is_existence_join(join_type) {
        streamed_batch
            .columns()
            .iter()
            .map(|column_array| {
                if column_array.is_empty()
                    || streamed_indices.null_count() == streamed_indices.len()
                {
                    assert_eq!(streamed_indices.null_count(), streamed_indices.len());
                    Ok(new_null_array(
                        column_array.data_type(),
                        streamed_indices.len(),
                    ))
                } else {
                    take(column_array, &streamed_indices, None)
                }
            })
            .collect::<Result<Vec<_>, ArrowError>>()?
    } else {
        vec![]
    };

    let mut buffered_columns = buffered_batch
        .columns()
        .iter()
        .map(|column_array| take(column_array, &buffered_indices, None))
        .collect::<Result<Vec<_>, ArrowError>>()?;

    buffered_columns.extend(streamed_columns);

    Ok(RecordBatch::try_new(
        Arc::new((*schema).clone()),
        buffered_columns,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common,
        test::{build_table_i32, TestMemoryExec},
        ExecutionPlan,
    };
    use arrow::array::{Date32Array, Date64Array};
    use arrow_schema::{DataType, Field};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_execution::TaskContext;
    use datafusion_expr::JoinType;
    use datafusion_physical_expr::expressions::Column;
    use insta::assert_snapshot;
    use std::sync::Arc;

    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn build_date_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date32, false),
            Field::new(b.0, DataType::Date32, false),
            Field::new(c.0, DataType::Date32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(a.1.clone())),
                Arc::new(Date32Array::from(b.1.clone())),
                Arc::new(Date32Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn build_date64_table(
        a: (&str, &Vec<i64>),
        b: (&str, &Vec<i64>),
        c: (&str, &Vec<i64>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date64, false),
            Field::new(b.0, DataType::Date64, false),
            Field::new(c.0, DataType::Date64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date64Array::from(a.1.clone())),
                Arc::new(Date64Array::from(b.1.clone())),
                Arc::new(Date64Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<PiecewiseMergeJoinExec> {
        PiecewiseMergeJoinExec::try_new(left, right, on, operator, join_type)
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        join_collect_with_options(left, right, on, operator, join_type).await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let task_ctx = Arc::new(TaskContext::default());
        let join = join(left, right, on, operator, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 3  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 2, 1]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 3  | 7  | 30 | 4  | 90 |
        | 2  | 2  | 8  | 30 | 4  | 90 |
        | 3  | 1  | 9  | 30 | 4  | 90 |
        | 2  | 2  | 8  | 20 | 3  | 80 |
        | 3  | 1  | 9  | 20 | 3  | 80 |
        | 3  | 1  | 9  | 10 | 2  | 70 |
        +----+----+----+----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_less_than_unsorted() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 3  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 2, 1]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // | 20 | 2  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![3, 2, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 3  | 7  | 30 | 4  | 90 |
            | 2  | 2  | 8  | 30 | 4  | 90 |
            | 3  | 1  | 9  | 30 | 4  | 90 |
            | 2  | 2  | 8  | 10 | 3  | 70 |
            | 3  | 1  | 9  | 10 | 3  | 70 |
            | 3  | 1  | 9  | 20 | 2  | 80 |
            +----+----+----+----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_greater_than_equal_to() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 2  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 4  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![2, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // | 20 | 2  | 80 |
        // | 30 | 1  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![3, 2, 1]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::GtEq, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 2  | 7  | 30 | 1  | 90 |
        | 2  | 3  | 8  | 30 | 1  | 90 |
        | 3  | 4  | 9  | 30 | 1  | 90 |
        | 1  | 2  | 7  | 20 | 2  | 80 |
        | 2  | 3  | 8  | 20 | 2  | 80 |
        | 3  | 4  | 9  | 20 | 2  | 80 |
        | 2  | 3  | 8  | 10 | 3  | 70 |
        | 3  | 4  | 9  | 10 | 3  | 70 |
        +----+----+----+----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_empty_left() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // (empty)
        // +----+----+----+
        let left = build_table(
            ("a1", &Vec::<i32>::new()),
            ("b1", &Vec::<i32>::new()),
            ("c1", &Vec::<i32>::new()),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 1  | 1  | 1  |
        // | 2  | 2  | 2  |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![1, 2]),
            ("b1", &vec![1, 2]),
            ("c2", &vec![1, 2]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let (_, batches) =
            join_collect(left, right, on, Operator::LtEq, JoinType::Inner).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        +----+----+----+----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_greater_than_equal_to() -> Result<()> {
        // +----+----+-----+
        // | a1 | b1 | c1  |
        // +----+----+-----+
        // | 1  | 1  | 100 |
        // | 2  | 2  | 200 |
        // +----+----+-----+
        let left = build_table(
            ("a1", &vec![1, 2]),
            ("b1", &vec![1, 2]),
            ("c1", &vec![100, 200]),
        );

        // +----+----+-----+
        // | a2 | b1 | c2  |
        // +----+----+-----+
        // | 10 | 3  | 300 |
        // | 20 | 2  | 400 |
        // +----+----+-----+
        let right = build_table(
            ("a2", &vec![10, 20]),
            ("b1", &vec![3, 2]),
            ("c2", &vec![300, 400]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::GtEq, JoinType::Full).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+-----+----+----+-----+
        | a1 | b1 | c1  | a2 | b1 | c2  |
        +----+----+-----+----+----+-----+
        | 2  | 2  | 200 | 20 | 2  | 400 |
        |    |    |     | 10 | 3  | 300 |
        | 1  | 1  | 100 |    |    |     |
        +----+----+-----+----+----+-----+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_greater_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 4  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // | 20 | 2  | 80 |
        // | 30 | 1  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![3, 2, 1]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::Left).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 2  | 3  | 8  | 30 | 1  | 90 |
        | 3  | 4  | 9  | 30 | 1  | 90 |
        | 2  | 3  | 8  | 20 | 2  | 80 |
        | 3  | 4  | 9  | 20 | 2  | 80 |
        | 3  | 4  | 9  | 10 | 3  | 70 |
        | 1  | 1  | 7  |    |    |    |
        +----+----+----+----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_greater_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 4  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 5  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 2  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![5, 3, 2]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 2  | 3  | 8  | 30 | 2  | 90 |
        | 3  | 4  | 9  | 30 | 2  | 90 |
        | 3  | 4  | 9  | 20 | 3  | 80 |
        |    |    |    | 10 | 5  | 70 |
        +----+----+----+----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 4  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 3, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 4  | 7  | 30 | 5  | 90 |
        | 2  | 3  | 8  | 30 | 5  | 90 |
        | 3  | 1  | 9  | 30 | 5  | 90 |
        | 3  | 1  | 9  | 20 | 3  | 80 |
        | 3  | 1  | 9  | 10 | 2  | 70 |
        +----+----+----+----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 4  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 3, 1]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::RightSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a2 | b1 | c2 |
        +----+----+----+
        | 10 | 2  | 70 |
        | 20 | 3  | 80 |
        | 30 | 5  | 90 |
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 5  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 2  | 4  | 8  |
        | 3  | 1  | 9  |
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_greater_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 5  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 4, 5]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 2  | 4  | 8  |
        | 3  | 5  | 9  |
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_greater_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 5  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 4, 5]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::LeftAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 1  | 7  |
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_greater_than_equal_to() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 4  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 1  | 70 |
        // | 20 | 2  | 80 |
        // | 30 | 3  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1, 2, 3]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::GtEq, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 1  | 7  |
        | 2  | 3  | 8  |
        | 3  | 4  | 9  |
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 5  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::LeftAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 5  | 7  |
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_less_than_equal_to() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 5  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::LtEq, JoinType::LeftAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_unsorted() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 3  | 7  |
        // | 2  | 1  | 8  |
        // | 3  | 2  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 1, 2]), // unsorted
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::RightSemi).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a2 | b1 | c2 |
        +----+----+----+
        | 10 | 2  | 70 |
        | 20 | 3  | 80 |
        | 30 | 4  | 90 |
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_less_than_equal_to() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 5  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::LtEq, JoinType::RightAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a2 | b1 | c2 |
        +----+----+----+
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 5  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 1  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::RightAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+
        | a2 | b1 | c2 |
        +----+----+----+
        | 10 | 1  | 70 |
        +----+----+----+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_date32_inner_less_than() -> Result<()> {
        // +----+-------+----+
        // | a1 |  b1   | c1 |
        // +----+-------+----+
        // | 1  | 19107 | 7  |
        // | 2  | 19107 | 8  |
        // | 3  | 19105 | 9  |
        // +----+-------+----+
        let left = build_date_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![19107, 19107, 19105]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+-------+----+
        // | a2 |  b1   | c2 |
        // +----+-------+----+
        // | 10 | 19105 | 70 |
        // | 20 | 19103 | 80 |
        // | 30 | 19107 | 90 |
        // +----+-------+----+
        let right = build_date_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![19105, 19103, 19107]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
    +------------+------------+------------+------------+------------+------------+
    | a1         | b1         | c1         | a2         | b1         | c2         |
    +------------+------------+------------+------------+------------+------------+
    | 1970-01-04 | 2022-04-23 | 1970-01-10 | 1970-01-31 | 2022-04-25 | 1970-04-01 |
    +------------+------------+------------+------------+------------+------------+
    "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64_inner_less_than() -> Result<()> {
        // +----+---------------+----+
        // | a1 |     b1        | c1 |
        // +----+---------------+----+
        // | 1  | 1650903441000 |  7 |
        // | 2  | 1650903441000 |  8 |
        // | 3  | 1650703441000 |  9 |
        // +----+---------------+----+
        let left = build_date64_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1650903441000, 1650903441000, 1650703441000]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+---------------+----+
        // | a2 |     b1        | c2 |
        // +----+---------------+----+
        // | 10 | 1650703441000 | 70 |
        // | 20 | 1650503441000 | 80 |
        // | 30 | 1650903441000 | 90 |
        // +----+---------------+----+
        let right = build_date64_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1650703441000, 1650503441000, 1650903441000]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        | 1970-01-01T00:00:00.003 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64_right_less_than() -> Result<()> {
        // +----+---------------+----+
        // | a1 |     b1        | c1 |
        // +----+---------------+----+
        // | 1  | 1650903441000 |  7 |
        // | 2  | 1650703441000 |  8 |
        // +----+---------------+----+
        let left = build_date64_table(
            ("a1", &vec![1, 2]),
            ("b1", &vec![1650903441000, 1650703441000]),
            ("c1", &vec![7, 8]),
        );

        // +----+---------------+----+
        // | a2 |     b1        | c2 |
        // +----+---------------+----+
        // | 10 | 1650703441000 | 80 |
        // | 20 | 1650903441000 | 90 |
        // +----+---------------+----+
        let right = build_date64_table(
            ("a2", &vec![10, 20]),
            ("b1", &vec![1650703441000, 1650903441000]),
            ("c2", &vec![80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
    | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
    | 1970-01-01T00:00:00.002 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.020 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
    |                         |                     |                         | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.080 |
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
"#);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64_left_semi_less_than() -> Result<()> {
        // +----+---------------+----+
        // | a1 |     b1        | c1 |
        // +----+---------------+----+
        // | 1  | 1650903441000 |  7 |
        // | 2  | 1650903441000 |  8 |
        // | 3  | 1650703441000 |  9 |
        // +----+---------------+----+
        let left = build_date64_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1650903441000, 1650903441000, 1650703441000]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+---------------+----+
        // | a2 |     b1        | c2 |
        // +----+---------------+----+
        // | 10 | 1650903441000 | 90 |
        // +----+---------------+----+
        let right = build_date64_table(
            ("a2", &vec![10]),
            ("b1", &vec![1650903441000]),
            ("c2", &vec![90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
    +-------------------------+---------------------+-------------------------+
    | a1                      | b1                  | c1                      |
    +-------------------------+---------------------+-------------------------+
    | 1970-01-01T00:00:00.003 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.009 |
    +-------------------------+---------------------+-------------------------+
"#);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64_right_semi_less_than() -> Result<()> {
        // +----+--------------+----+
        // | a1 |     b1       | c1 |
        // +----+--------------+----+
        // | 1  | 1650903441000 | 7 |
        // | 2  | 1650703441000 | 8 |
        // +----+---------------+----+
        let left = build_date64_table(
            ("a1", &vec![1, 2]),
            ("b1", &vec![1650903441000, 1650703441000]),
            ("c1", &vec![7, 8]),
        );

        // +----+---------------+----+
        // | a2 |     b1        | c2 |
        // +----+---------------+----+
        // | 10 | 1650703441000 | 80 |
        // | 20 | 1650903441000 | 90 |
        // +----+---------------+----+
        let right = build_date64_table(
            ("a2", &vec![10, 20]),
            ("b1", &vec![1650703441000, 1650903441000]),
            ("c2", &vec![80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::RightSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
    +-------------------------+---------------------+-------------------------+
    | a2                      | b1                  | c2                      |
    +-------------------------+---------------------+-------------------------+
    | 1970-01-01T00:00:00.020 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
    +-------------------------+---------------------+-------------------------+
"#);
        Ok(())
    }
}
