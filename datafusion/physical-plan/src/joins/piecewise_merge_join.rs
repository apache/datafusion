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
    new_null_array, Array, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, RecordBatchOptions, UInt16Array, UInt8Array,
};
use arrow::compute::take;
use arrow::{
    array::{
        ArrayRef, BooleanBufferBuilder, RecordBatch, UInt32Array, UInt32Builder,
        UInt64Array, UInt64Builder,
    },
    compute::{concat_batches, sort_to_indices, take_record_batch},
    util::bit_util,
};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef, SortOptions};
use datafusion_common::{
    exec_err, internal_err, plan_err, utils::compare_rows, JoinSide, Result, ScalarValue,
};
use datafusion_common::{not_impl_err, NullEquality};
use datafusion_execution::{
    memory_pool::{MemoryConsumer, MemoryReservation},
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_expr::{JoinType, Operator};
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
/// For `PiecewiseMergeJoin` we label all left inputs as the `streamed' side and the right outputs as the
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
/// Processing Row 1:
///
///            Sorted Streamed Side          Sorted Buffered Side  
///            ┌──────────────────┐          ┌──────────────────┐
///          1 │       100        │        1 │       100        │
///            ├──────────────────┤          ├──────────────────┤
///          2 │       200        │        2 │       200        │ ─┐  
///            ├──────────────────┤          ├──────────────────┤  │  For row 1 on streamed side with
///          3 │       500        │        3 │       200        │  │  value 100, we emit rows 2 - 5
///            └──────────────────┘          ├──────────────────┤  │  as matches when the operator is
///                                        4 │       300        │  │  `Operator::Lt` (<) Emitting all
///                                          ├──────────────────┤  │  rows after the first match (row 2
///                                        5 │       400        │ ─┘  buffered side; 100 < 200)
///                                          └──────────────────┘     
///
/// Processing Row 2:
///   By sorting the streamed side we know
///
///            Sorted Streamed Side          Sorted Buffered Side  
///            ┌──────────────────┐          ┌──────────────────┐
///          1 │       100        │        1 │       100        │
///            ├──────────────────┤          ├──────────────────┤
///          2 │       200        │        2 │       200        │ <- Start here when probing for the streamed
///            ├──────────────────┤          ├──────────────────┤    side row 2.
///          3 │       500        │        3 │       200        │
///            └──────────────────┘          ├──────────────────┤
///                                        4 │       300        │
///                                          ├──────────────────┤
///                                        5 │       400        |
///                                          └──────────────────┘
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
/// We perform a `JoinType::Left` with these two batches and the operator being `Operator::Lt`(<). Because
/// the operator is `Operator::Lt` we can find the minimum value in the streamed side; in this case it is 200.
/// We can then advance a pointer from the start of the buffer side until we find the first value that satisfies
/// the predicate. All rows after that first matched value satisfy the condition 200 < x so we can mark all of
/// those rows as matched.
///
/// ```text
///           Unsorted Streamed Side         Sorted Buffered Side  
///            ┌──────────────────┐          ┌──────────────────┐
///          1 │       500        │        1 │       100        │
///            ├──────────────────┤          ├──────────────────┤
///          2 │       200        │        2 │       200        │
///            ├──────────────────┤          ├──────────────────┤    
///          3 │       300        │        3 │       200        │
///            └──────────────────┘          ├──────────────────┤
///               min value: 200           4 │       300        │ ─┐
///                                          ├──────────────────┤  | We emit matches for row 4 - 5 on the
///                                        5 │       400        | ─┘ buffered side.
///                                          └──────────────────┘
/// ```
///
/// For both types of joins, the buffered side must be sorted ascending for `Operator::Lt`(<) or
/// `Operator::LtEq`(<=) and descending for `Operator::Gt`(>) or `Operator::GtEq`(>=).
///
/// # Further Reference Material
/// DuckDB blog on Range Joins: [Range Joins in DuckDB](https://duckdb.org/2022/05/27/iejoin.html)
#[derive(Debug)]
pub struct PiecewiseMergeJoinExec {
    /// Left sorted joining execution plan
    pub streamed: Arc<dyn ExecutionPlan>,
    /// Right sorting joining execution plan
    pub buffered: Arc<dyn ExecutionPlan>,
    /// The two expressions being compared
    pub on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
    /// Comparison operator in the range predicate
    pub operator: Operator,
    /// How the join is performed
    pub join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
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
        streamed: Arc<dyn ExecutionPlan>,
        buffered: Arc<dyn ExecutionPlan>,
        on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<Self> {
        // TODO: Implement mark joins for PiecewiseMergeJoin
        if matches!(join_type, JoinType::LeftMark | JoinType::RightMark) {
            return plan_err!(
                "Mark Joins are currently not supported for PiecewiseMergeJoin"
            );
        }

        // We take the operator and enforce a sort order on the streamed + buffered side based on
        // the operator type.
        let sort_options = match operator {
            Operator::Lt | Operator::LtEq => SortOptions::new(false, false),
            Operator::Gt | Operator::GtEq => SortOptions::new(true, false),
            _ => {
                return plan_err!(
                    "Cannot contain non-range operator in PiecewiseMergeJoinExec"
                )
            }
        };

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

        let streamed_schema = streamed.schema();
        let buffered_schema = buffered.schema();

        // Create output schema for the join
        let schema =
            Arc::new(build_join_schema(&streamed_schema, &buffered_schema, &join_type).0);
        let cache = Self::compute_properties(
            &streamed,
            &buffered,
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

    /// Refeference to streamed side execution plan
    pub fn streamed(&self) -> &Arc<dyn ExecutionPlan> {
        &self.streamed
    }

    /// Refeerence to buffered side execution plan
    pub fn buffered(&self) -> &Arc<dyn ExecutionPlan> {
        &self.buffered
    }

    /// Join type
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Reference to sort options
    pub fn sort_options(&self) -> &SortOptions {
        &self.sort_options
    }

    /// Get probe side (buffered side) for the PiecewiseMergeJoin
    /// In current implementation, probe side is determined according to join type.
    pub fn probe_side(join_type: &JoinType) -> JoinSide {
        match join_type {
            JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => JoinSide::Right,
            JoinType::Inner
            | JoinType::Left
            | JoinType::Full
            | JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::LeftMark => JoinSide::Left,
        }
    }

    // TODO: fix compute properties to work specifically for PiecewiseMergeJoin
    // This is currently just a filler implementation so that it actually returns
    // a PlanProperties
    pub fn compute_properties(
        streamed: &Arc<dyn ExecutionPlan>,
        buffered: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        join_on: &(PhysicalExprRef, PhysicalExprRef),
    ) -> Result<PlanProperties> {
        let eq_properties = join_equivalence_properties(
            streamed.equivalence_properties().clone(),
            buffered.equivalence_properties().clone(),
            &join_type,
            schema,
            &[false, false],
            Some(Self::probe_side(&join_type)),
            &[join_on.clone()],
        )?;

        let output_partitioning =
            symmetric_join_output_partitioning(streamed, buffered, &join_type)?;

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            boundedness_from_children([streamed, buffered]),
        ))
    }

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
        vec![&self.streamed, &self.buffered]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            Some(OrderingRequirements::from(self.left_sort_exprs.clone())),
            Some(OrderingRequirements::from(self.right_sort_exprs.clone())),
        ]
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
        let on_streamed = Arc::clone(&self.on.0);
        let on_buffered = Arc::clone(&self.on.1);

        // If the join type is either LeftSemi, LeftAnti, or LeftMark we will swap the inputs
        // and sort ordering because we want the mark side to be the buffered side.
        let (streamed, buffered, on_streamed, on_buffered, operator, sort_options) = if matches!(
            self.join_type,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark
        ) {
            (
                Arc::clone(&self.buffered),
                Arc::clone(&self.streamed),
                on_buffered,
                on_streamed,
                self.operator.swap().unwrap(),
                SortOptions::new(!self.sort_options.descending, false),
            )
        } else {
            (
                Arc::clone(&self.streamed),
                Arc::clone(&self.buffered),
                on_streamed,
                on_buffered,
                self.operator,
                self.sort_options,
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
                metrics,
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
            sort_options,
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

// Returns boolean to check if the join type needs to record
// buffered side matches for classic joins
fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Full | JoinType::Right)
}

// Returns boolean for whether or not we need to build the buffered side
// bitmap for marking matched rows on the buffered side.
fn build_visited_indices_map(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Full
            | JoinType::Right
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
        let buffered_data = ready!(self
            .buffered_side
            .try_as_initial_mut()?
            .buffered_fut
            .get_shared(cx))?;

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

                // For existence joins we do not need to sort the output, and we only need to
                // find the min or max value (depending on the operator) of all the stream batches
                if self.existence_join {
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

    // Only classic join will call this, we process stream batches and evaluate against
    // the buffered side data.
    fn process_stream_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let stream_batch = self.state.try_as_process_stream_batch_mut()?;
        let buffered_side = self.buffered_side.try_as_ready_mut()?;

        let result = resolve_classic_join(
            stream_batch,
            buffered_side,
            Arc::clone(&self.schema),
            self.operator,
            self.sort_option,
            self.join_type,
        )?;

        self.state = PiecewiseMergeJoinStreamState::FetchStreamBatch;
        Ok(StatefulStreamResult::Ready(Some(result)))
    }

    // Process remaining unmatched rows
    fn process_unmatched_buffered_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        // Return early for `JoinType::Left` and `JoinType::Inner`
        if matches!(self.join_type, JoinType::Left | JoinType::Inner) {
            self.state = PiecewiseMergeJoinStreamState::Completed;

            return Ok(StatefulStreamResult::Ready(None));
        }

        let buffered_data =
            Arc::clone(&self.buffered_side.try_as_ready().unwrap().buffered_data);

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
                None => return exec_err!("Stream batch was empty."),
            };

            let buffered_values = buffered_data.values();
            let mut threshold_idx: Option<usize> = None;

            // We iterate the buffered size values while comparing the threshold value (min/max)
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

            // If we found a match then we will append all indices from the threshold index
            // to the end of the buffered size rows
            if let Some(threshold_idx) = threshold_idx {
                let buffered_range: Vec<u64> =
                    (threshold_idx as u64..buffered_data.values.len() as u64).collect();
                buffered_indices.append_slice(&buffered_range);
            }
            let buffered_indices_array = buffered_indices.finish();

            // The visited bitmap hasn't been marked yet for existence joins
            let mut bitmap = buffered_data.visited_indices_bitmap.lock();
            buffered_indices_array.iter().flatten().for_each(|x| {
                bitmap.set_bit(x as usize, true);
            });
        }

        let (buffered_indices, streamed_indices) = get_final_indices_from_shared_bitmap(
            &buffered_data.visited_indices_bitmap,
            self.join_type,
            true,
        );

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

        self.state = PiecewiseMergeJoinStreamState::Completed;

        Ok(StatefulStreamResult::Ready(Some(batch)))
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
        Operator::Gt | Operator::GtEq => {
            let max_value = min_max(&stream_batch.values[0], true)?;
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

        Operator::Lt | Operator::LtEq => {
            let min_value = min_max(&stream_batch.values[0], false)?;
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
    stream_batch: &StreamedBatch,
    buffered_side: &BufferedSideReadyState,
    join_schema: Arc<Schema>,
    operator: Operator,
    sort_options: SortOptions,
    join_type: JoinType,
) -> Result<RecordBatch> {
    let stream_values = stream_batch.values();
    let buffered_values = buffered_side.buffered_data.values();
    let buffered_len = buffered_values.len();

    let mut stream_indices = UInt32Builder::default();
    let mut buffered_indices = UInt64Builder::default();

    // Our pivot variable allows us to start probing on the buffered side where we last matched
    // in the previous stream row.
    let mut pivot = 0;
    for row_idx in 0..stream_values[0].len() {
        let mut found = false;
        while pivot < buffered_values.len() {
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
                        let stream_repeated = vec![row_idx as u32; count];
                        let buffered_range: Vec<u64> =
                            (pivot as u64..buffered_len as u64).collect();
                        stream_indices.append_slice(&stream_repeated);
                        buffered_indices.append_slice(&buffered_range);
                        found = true;

                        break;
                    }
                    pivot += 1;
                }
                Operator::GtEq | Operator::LtEq => {
                    if matches!(compare, Ordering::Equal | Ordering::Less) {
                        let count = buffered_values.len() - pivot;
                        let stream_repeated = vec![row_idx as u32; count];
                        let buffered_range: Vec<u64> =
                            (pivot as u64..buffered_len as u64).collect();
                        stream_indices.append_slice(&stream_repeated);
                        buffered_indices.append_slice(&buffered_range);
                        found = true;

                        break;
                    }
                    pivot += 1;
                }
                _ => {
                    return exec_err!(
                        "PiecewiseMergeJoin should not contain operator, {}",
                        operator
                    )
                }
            };
        }

        // If not found we append a null value for `JoinType::Left` and `JoinType::Full`
        if !found && matches!(join_type, JoinType::Left | JoinType::Full) {
            stream_indices.append_value(row_idx as u32);
            buffered_indices.append_null();
        }
    }

    let stream_indices_array = stream_indices.finish();
    let buffered_indices_array = buffered_indices.finish();

    // We need to mark the buffered side matched indices for `JoinType::Full` and `JoinType::Right`
    if need_produce_result_in_final(join_type) {
        let mut bitmap = buffered_side.buffered_data.visited_indices_bitmap.lock();

        buffered_indices_array.iter().flatten().for_each(|x| {
            bitmap.set_bit(x as usize, true);
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
    let mut streamed_columns = if !is_existence_join(join_type) {
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

    let buffered_columns = buffered_batch
        .columns()
        .iter()
        .map(|column_array| take(column_array, &buffered_indices, None))
        .collect::<Result<Vec<_>, ArrowError>>()?;

    streamed_columns.extend(buffered_columns);

    Ok(RecordBatch::try_new(
        Arc::new((*schema).clone()),
        streamed_columns,
    )?)
}

pub fn min_max(array: &ArrayRef, find_max: bool) -> Result<ScalarValue> {
    macro_rules! find_min_max {
        ($ARR:ty, $SCALAR:ident) => {{
            let arr = array.as_any().downcast_ref::<$ARR>().unwrap();
            let mut extreme: Option<_> = None;
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    let v = arr.value(i);
                    extreme = Some(match extreme {
                        Some(cur) => {
                            if find_max {
                                if v > cur {
                                    v
                                } else {
                                    cur
                                }
                            } else {
                                if v < cur {
                                    v
                                } else {
                                    cur
                                }
                            }
                        }
                        None => v,
                    });
                }
            }
            ScalarValue::$SCALAR(extreme)
        }};
    }

    let result = match array.data_type() {
        DataType::Int8 => find_min_max!(Int8Array, Int8),
        DataType::Int16 => find_min_max!(Int16Array, Int16),
        DataType::Int32 => find_min_max!(Int32Array, Int32),
        DataType::Int64 => find_min_max!(Int64Array, Int64),
        DataType::UInt8 => find_min_max!(UInt8Array, UInt8),
        DataType::UInt16 => find_min_max!(UInt16Array, UInt16),
        DataType::UInt32 => find_min_max!(UInt32Array, UInt32),
        DataType::UInt64 => find_min_max!(UInt64Array, UInt64),
        DataType::Float32 => find_min_max!(Float32Array, Float32),
        DataType::Float64 => find_min_max!(Float64Array, Float64),

        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let mut extreme: Option<bool> = None;
            for i in 0..arr.len() {
                if arr.is_valid(i) {
                    let v = arr.value(i);
                    extreme = Some(match extreme {
                        Some(cur) => {
                            if find_max {
                                cur || v // max: true if either is true
                            } else {
                                cur && v // min: false if either is false
                            }
                        }
                        None => v,
                    });
                }
            }
            ScalarValue::Boolean(extreme)
        }

        dt => {
            return not_impl_err!(
                "Unsupported data type in PiecewiseMergeJoin min/max function: {}",
                dt
            );
        }
    };

    Ok(result)
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
    use arrow_schema::Field;
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
        Ok(PiecewiseMergeJoinExec::try_new(
            left, right, on, operator, join_type,
        )?)
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 3]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
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
            | 1  | 1  | 7  | 10 | 2  | 70 |
            | 1  | 1  | 7  | 20 | 3  | 80 |
            | 1  | 1  | 7  | 30 | 4  | 90 |
            | 2  | 2  | 8  | 20 | 3  | 80 |
            | 2  | 2  | 8  | 30 | 4  | 90 |
            | 3  | 3  | 9  | 30 | 4  | 90 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_less_than_unsorted() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 1, 2]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
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
                | 2  | 1  | 8  | 10 | 2  | 70 |
                | 2  | 1  | 8  | 20 | 3  | 80 |
                | 2  | 1  | 8  | 30 | 4  | 90 |
                | 3  | 2  | 9  | 20 | 3  | 80 |
                | 3  | 2  | 9  | 30 | 4  | 90 |
                | 1  | 3  | 7  | 30 | 4  | 90 |
                +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_greater_than_equal_to() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![2, 3, 4]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
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
            | 3  | 4  | 9  | 10 | 3  | 70 |
            | 3  | 4  | 9  | 20 | 2  | 80 |
            | 3  | 4  | 9  | 30 | 1  | 90 |
            | 2  | 3  | 8  | 10 | 3  | 70 |
            | 2  | 3  | 8  | 20 | 2  | 80 |
            | 2  | 3  | 8  | 30 | 1  | 90 |
            | 1  | 2  | 7  | 20 | 2  | 80 |
            | 1  | 2  | 7  | 30 | 1  | 90 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_empty_left() -> Result<()> {
        let left = build_table(
            ("a1", &Vec::<i32>::new()),
            ("b1", &Vec::<i32>::new()),
            ("c1", &Vec::<i32>::new()),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2]),
            ("b1", &vec![1, 2]),
            ("c1", &vec![100, 200]),
        );
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
            | 1  | 1  | 100 |    |    |     |
            |    |    |     | 10 | 3  | 300 |
            +----+----+-----+----+----+-----+
            "#);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_greater_than() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );
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
            | 3  | 4  | 9  | 10 | 3  | 70 |
            | 3  | 4  | 9  | 20 | 2  | 80 |
            | 3  | 4  | 9  | 30 | 1  | 90 |
            | 2  | 3  | 8  | 20 | 2  | 80 |
            | 2  | 3  | 8  | 30 | 1  | 90 |
            | 1  | 1  | 7  |    |    |    |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_greater_than() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );
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
            | 3  | 4  | 9  | 20 | 3  | 80 |
            | 3  | 4  | 9  | 30 | 2  | 90 |
            | 2  | 3  | 8  | 30 | 2  | 90 |
            |    |    |    | 10 | 5  | 70 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_less_than() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );
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
            | 1  | 1  | 7  | 10 | 2  | 70 |
            | 1  | 1  | 7  | 20 | 3  | 80 |
            | 1  | 1  | 7  | 30 | 5  | 90 |
            | 2  | 3  | 8  | 30 | 5  | 90 |
            | 3  | 4  | 9  | 30 | 5  | 90 |
            +----+----+----+----+----+----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_less_than() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 4, 5]),
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 4, 5]),
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 1, 2]), // unsorted
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );
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
        let left = build_date_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![19105, 19107, 19107]),
            ("c1", &vec![7, 8, 9]),
        );
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
        | 1970-01-02 | 2022-04-23 | 1970-01-08 | 1970-01-31 | 2022-04-25 | 1970-04-01 |
        +------------+------------+------------+------------+------------+------------+
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64_inner_less_than() -> Result<()> {
        let left = build_date64_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1650703441000, 1650903441000, 1650903441000]),
            ("c1", &vec![7, 8, 9]),
        );
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
            | 1970-01-01T00:00:00.001 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.007 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
            +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
            "#);
        Ok(())
    }
}
