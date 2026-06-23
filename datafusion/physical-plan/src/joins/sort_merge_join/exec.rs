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

//! Defines the Sort-Merge join execution plan.
//! A Sort-Merge join plan consumes two sorted children plans and produces
//! joined output by given join type and other options.

use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::bitwise_stream::BitwiseSortMergeJoinStream;
use super::materializing_stream::MaterializingSortMergeJoinStream;
use super::metrics::SortMergeJoinMetrics;
use crate::execution_plan::{EmissionType, boundedness_from_children};
use crate::expressions::PhysicalSortExpr;
use crate::joins::PartitionMode;
use crate::joins::utils::{
    JoinFilter, JoinOn, JoinOnRef, OnceAsync, OnceFut,
    asymmetric_join_output_partitioning, build_join_schema, check_join_is_valid,
    estimate_join_statistics, reorder_output_after_swap,
    symmetric_join_output_partitioning,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use crate::projection::{
    ProjectionExec, join_allows_pushdown, join_table_borders, new_join_children,
    physical_to_column_exprs, update_join_on,
};
use crate::spill::spill_manager::SpillManager;
use crate::statistics::StatisticsArgs;
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
    check_if_same_properties,
};

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{
    JoinSide, JoinType, NullEquality, Result, assert_eq_or_internal_err, internal_err,
    not_impl_err, plan_err,
};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr_common::physical_expr::{PhysicalExprRef, fmt_sql};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};

use futures::{Stream, StreamExt};

/// Join execution plan that executes equi-join predicates on multiple partitions using Sort-Merge
/// join algorithm and applies an optional filter post join. Can be used to join arbitrarily large
/// inputs where one or both of the inputs don't fit in the available memory.
///
/// # Join Expressions
///
/// Equi-join predicate (e.g. `<col1> = <col2>`) expressions are represented by [`Self::on`].
///
/// Non-equality predicates, which can not be pushed down to join inputs (e.g.
/// `<col1> != <col2>`) are known as "filter expressions" and are evaluated
/// after the equijoin predicates. They are represented by [`Self::filter`]. These are optional
/// expressions.
///
/// # Sorting
///
/// Assumes that both the left and right input to the join are pre-sorted. It is not the
/// responsibility of this execution plan to sort the inputs.
///
/// # "Streamed" vs "Buffered"
///
/// The number of record batches of streamed input currently present in the memory will depend
/// on the output batch size of the execution plan. There is no spilling support for streamed input.
/// The comparisons are performed from values of join keys in streamed input with the values of
/// join keys in buffered input. One row in streamed record batch could be matched with multiple rows in
/// buffered input batches. The streamed input is managed through the states in `StreamedState`
/// and streamed input batches are represented by `StreamedBatch`.
///
/// Buffered input is buffered for all record batches having the same value of join key.
/// If the memory limit increases beyond the specified value and spilling is enabled,
/// buffered batches could be spilled to disk. If spilling is disabled, the execution
/// will fail under the same conditions. Multiple record batches of buffered could currently reside
/// in memory/disk during the execution. The number of buffered batches residing in
/// memory/disk depends on the number of rows of buffered input having the same value
/// of join key as that of streamed input rows currently present in memory. Due to pre-sorted inputs,
/// the algorithm understands when it is not needed anymore, and releases the buffered batches
/// from memory/disk. The buffered input is managed through the states in `BufferedState`
/// and buffered input batches are represented by `BufferedBatch`.
///
/// Depending on the type of join, left or right input may be selected as streamed or buffered
/// respectively. For example, in a left-outer join, the left execution plan will be selected as
/// streamed input while in a right-outer join, the right execution plan will be selected as the
/// streamed input.
///
/// Reference for the algorithm:
/// <https://en.wikipedia.org/wiki/Sort-merge_join>.
///
/// Helpful short video demonstration:
/// <https://www.youtube.com/watch?v=jiWCPJtDE2c>.
#[derive(Debug, Clone)]
pub struct SortMergeJoinExec {
    /// Left sorted joining execution plan
    pub left: Arc<dyn ExecutionPlan>,
    /// Right sorting joining execution plan
    pub right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    pub on: JoinOn,
    /// Filters which are applied while finding matching rows
    pub filter: Option<JoinFilter>,
    /// How the join is performed
    pub join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// The left SortExpr
    left_sort_exprs: LexOrdering,
    /// The right SortExpr
    right_sort_exprs: LexOrdering,
    /// Sort options of join columns used in sorting left and right execution plans
    pub sort_options: Vec<SortOptions>,
    /// Defines the null equality for the join.
    pub null_equality: NullEquality,
    /// How the inputs are partitioned across this join. In
    /// [`PartitionMode::CollectLeft`] the left side is collected into a single
    /// sorted run shared by all right partitions, and the right side is not
    /// repartitioned. Otherwise both sides are hash-partitioned on the join keys.
    pub mode: PartitionMode,
    /// Shared, lazily-computed collection of the (single, sorted) left input,
    /// used only in [`PartitionMode::CollectLeft`]. Computed once and replayed
    /// by every right partition.
    left_fut: Arc<OnceAsync<CollectedLeft>>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
}

impl SortMergeJoinExec {
    /// Tries to create a new [SortMergeJoinExec].
    /// The inputs are sorted using `sort_options` are applied to the columns in the `on`
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        check_join_is_valid(&left_schema, &right_schema, &on)?;
        if sort_options.len() != on.len() {
            return plan_err!(
                "Expected number of sort options: {}, actual: {}",
                on.len(),
                sort_options.len()
            );
        }

        let (left_sort_exprs, right_sort_exprs): (Vec<_>, Vec<_>) = on
            .iter()
            .zip(sort_options.iter())
            .map(|((l, r), sort_op)| {
                let left = PhysicalSortExpr {
                    expr: Arc::clone(l),
                    options: *sort_op,
                };
                let right = PhysicalSortExpr {
                    expr: Arc::clone(r),
                    options: *sort_op,
                };
                (left, right)
            })
            .unzip();
        let Some(left_sort_exprs) = LexOrdering::new(left_sort_exprs) else {
            return plan_err!(
                "SortMergeJoinExec requires valid sort expressions for its left side"
            );
        };
        let Some(right_sort_exprs) = LexOrdering::new(right_sort_exprs) else {
            return plan_err!(
                "SortMergeJoinExec requires valid sort expressions for its right side"
            );
        };

        let schema =
            Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);
        let mode = PartitionMode::Partitioned;
        let cache = Self::compute_properties(
            &left,
            &right,
            Arc::clone(&schema),
            join_type,
            &on,
            mode,
        )?;
        Ok(Self {
            left,
            right,
            on,
            filter,
            join_type,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            left_sort_exprs,
            right_sort_exprs,
            sort_options,
            null_equality,
            mode,
            left_fut: Arc::new(OnceAsync::default()),
            cache: Arc::new(cache),
        })
    }

    /// Configure the [`PartitionMode`] for this join.
    ///
    /// [`PartitionMode::CollectLeft`] collects the left input into a single
    /// sorted run that is shared across all right partitions and leaves the
    /// right input un-repartitioned (one output partition per right partition).
    /// It is only valid for join types whose output is determined per right
    /// partition: [`JoinType::Inner`], [`JoinType::Right`],
    /// [`JoinType::RightSemi`], [`JoinType::RightAnti`] and
    /// [`JoinType::RightMark`]. Left-side joins (Left/LeftSemi/LeftAnti/
    /// LeftMark/Full) would require tracking left-row matches across all right
    /// partitions and are not supported in this mode.
    pub fn with_mode(mut self, mode: PartitionMode) -> Result<Self> {
        if mode == PartitionMode::CollectLeft
            && !matches!(
                self.join_type,
                JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::RightMark
            )
        {
            return not_impl_err!(
                "SortMergeJoinExec in CollectLeft mode does not support {:?} joins",
                self.join_type
            );
        }
        self.cache = Arc::new(Self::compute_properties(
            &self.left,
            &self.right,
            Arc::clone(&self.schema),
            self.join_type,
            &self.on,
            mode,
        )?);
        self.mode = mode;
        // Start from a fresh collection so a re-moded join never replays a
        // previously collected (and possibly stale) left side.
        self.left_fut = Arc::new(OnceAsync::default());
        Ok(self)
    }

    /// Get probe side (e.g streaming side) information for this sort merge join.
    /// In current implementation, probe side is determined according to join type.
    pub fn probe_side(join_type: &JoinType) -> JoinSide {
        // When output schema contains only the right side, probe side is right.
        // Otherwise probe side is the left side.
        match join_type {
            // TODO: sort merge support for right mark (tracked here: https://github.com/apache/datafusion/issues/16226)
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

    /// Calculate order preservation flags for this sort merge join.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        match join_type {
            JoinType::Inner => vec![true, false],
            JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark => vec![true, false],
            JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => {
                vec![false, true]
            }
            _ => vec![false, false],
        }
    }

    /// Set of common columns used to join on
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    /// Ref to right execution plan
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Join type
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Ref to left execution plan
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// Ref to join filter
    pub fn filter(&self) -> &Option<JoinFilter> {
        &self.filter
    }

    /// Ref to sort options
    pub fn sort_options(&self) -> &[SortOptions] {
        &self.sort_options
    }

    /// Null equality
    pub fn null_equality(&self) -> NullEquality {
        self.null_equality
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        join_on: JoinOnRef,
        mode: PartitionMode,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema,
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side(&join_type)),
            join_on,
        )?;

        // In CollectLeft mode the left is collected into a single partition and
        // the right side is not repartitioned, so the output partitioning
        // follows the right (probe) side. Otherwise both sides are
        // hash-partitioned and the output is symmetric.
        let output_partitioning = match mode {
            PartitionMode::CollectLeft => {
                asymmetric_join_output_partitioning(left, right, &join_type)?
            }
            PartitionMode::Partitioned | PartitionMode::Auto => {
                symmetric_join_output_partitioning(left, right, &join_type)?
            }
        };

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            boundedness_from_children([left, right]),
        ))
    }

    /// # Notes:
    ///
    /// This function should be called BEFORE inserting any repartitioning
    /// operators on the join's children. Check [`super::super::HashJoinExec::swap_inputs`]
    /// for more details.
    pub fn swap_inputs(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let left = self.left();
        let right = self.right();
        let new_join = SortMergeJoinExec::try_new(
            Arc::clone(right),
            Arc::clone(left),
            self.on()
                .iter()
                .map(|(l, r)| (Arc::clone(r), Arc::clone(l)))
                .collect::<Vec<_>>(),
            self.filter().as_ref().map(JoinFilter::swap),
            self.join_type().swap(),
            self.sort_options.clone(),
            self.null_equality,
        )?;

        // TODO: OR this condition with having a built-in projection (like
        //       ordinary hash join) when we support it.
        if matches!(
            self.join_type(),
            JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::LeftMark
                | JoinType::RightMark
        ) {
            Ok(Arc::new(new_join))
        } else {
            reorder_output_after_swap(Arc::new(new_join), &left.schema(), &right.schema())
        }
    }

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let left = children.swap_remove(0);
        let right = children.swap_remove(0);
        Self {
            left,
            right,
            metrics: ExecutionPlanMetricsSet::new(),
            // Children changed: the collected left must be recomputed.
            left_fut: Arc::new(OnceAsync::default()),
            ..Self::clone(self)
        }
    }
}

/// The fully-collected left side of a [`PartitionMode::CollectLeft`]
/// sort-merge join: every batch of the (single, sorted) left input held in
/// memory and replayed by each right partition's join.
struct CollectedLeft {
    batches: Vec<RecordBatch>,
    /// Keeps the memory for `batches` reserved for the lifetime of the join.
    _reservation: MemoryReservation,
}

/// Drain a (single-partition, already-sorted) left input into memory, reserving
/// memory as batches accumulate. Runs once and is shared across right
/// partitions via [`OnceAsync`].
async fn collect_left(
    mut stream: SendableRecordBatchStream,
    reservation: MemoryReservation,
) -> Result<CollectedLeft> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        reservation.try_grow(get_record_batch_memory_size(&batch))?;
        batches.push(batch);
    }
    Ok(CollectedLeft {
        batches,
        _reservation: reservation,
    })
}

/// Replays the shared [`CollectedLeft`] as a [`RecordBatchStream`] for one
/// right partition. On first poll it awaits the shared collection, then yields
/// the collected (sorted) batches in order.
struct CollectedLeftReplay {
    left_fut: OnceFut<CollectedLeft>,
    schema: SchemaRef,
    data: Option<Arc<CollectedLeft>>,
    index: usize,
}

impl CollectedLeftReplay {
    fn new(left_fut: OnceFut<CollectedLeft>, schema: SchemaRef) -> Self {
        Self {
            left_fut,
            schema,
            data: None,
            index: 0,
        }
    }
}

impl Stream for CollectedLeftReplay {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.data.is_none() {
            match this.left_fut.get_shared(cx) {
                Poll::Ready(Ok(data)) => this.data = Some(data),
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
        // `data` is guaranteed to be `Some` here.
        let data = this.data.as_ref().expect("collected left is ready");
        if this.index < data.batches.len() {
            let batch = data.batches[this.index].clone();
            this.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for CollectedLeftReplay {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl DisplayAs for SortMergeJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({c1}, {c2})"))
                    .collect::<Vec<String>>()
                    .join(", ");
                let display_null_equality =
                    if self.null_equality() == NullEquality::NullEqualsNull {
                        ", NullsEqual: true"
                    } else {
                        ""
                    };
                // Only display the partition mode when it differs from the
                // default, to avoid churning existing plan output.
                let display_mode = if self.mode == PartitionMode::CollectLeft {
                    ", mode=CollectLeft"
                } else {
                    ""
                };
                write!(
                    f,
                    "{}: join_type={:?}, on=[{}]{}{}{}",
                    Self::static_name(),
                    self.join_type,
                    on,
                    self.filter.as_ref().map_or_else(
                        || "".to_string(),
                        |f| format!(", filter={}", f.expression())
                    ),
                    display_null_equality,
                    display_mode,
                )
            }
            DisplayFormatType::TreeRender => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| {
                        format!("({} = {})", fmt_sql(c1.as_ref()), fmt_sql(c2.as_ref()))
                    })
                    .collect::<Vec<String>>()
                    .join(", ");

                if self.join_type() != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)?;
                }
                writeln!(f, "on={on}")?;

                if self.null_equality() == NullEquality::NullEqualsNull {
                    writeln!(f, "NullsEqual: true")?;
                }

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for SortMergeJoinExec {
    fn name(&self) -> &'static str {
        "SortMergeJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.mode {
            // Collect the left into a single sorted partition; leave the right
            // un-repartitioned (each right partition joins the full left).
            PartitionMode::CollectLeft => vec![
                Distribution::SinglePartition,
                Distribution::UnspecifiedDistribution,
            ],
            PartitionMode::Partitioned | PartitionMode::Auto => {
                let (left_expr, right_expr) = self
                    .on
                    .iter()
                    .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
                    .unzip();
                vec![
                    Distribution::HashPartitioned(left_expr),
                    Distribution::HashPartitioned(right_expr),
                ]
            }
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            Some(OrderingRequirements::from(self.left_sort_exprs.clone())),
            Some(OrderingRequirements::from(self.right_sort_exprs.clone())),
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
        check_if_same_properties!(self, children);
        match &children[..] {
            [left, right] => Ok(Arc::new(
                SortMergeJoinExec::try_new(
                    Arc::clone(left),
                    Arc::clone(right),
                    self.on.clone(),
                    self.filter.clone(),
                    self.join_type,
                    self.sort_options.clone(),
                    self.null_equality,
                )?
                .with_mode(self.mode)?,
            )),
            _ => internal_err!("SortMergeJoin wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (on_left, on_right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let probe_left = SortMergeJoinExec::probe_side(&self.join_type) == JoinSide::Left;

        // Build the streamed/buffered record-batch streams (and their join
        // keys) for this output partition, depending on the partition mode.
        let (streamed, buffered, on_streamed, on_buffered): (
            SendableRecordBatchStream,
            SendableRecordBatchStream,
            Vec<PhysicalExprRef>,
            Vec<PhysicalExprRef>,
        ) = match self.mode {
            PartitionMode::CollectLeft => {
                // The left input is collected once into a single sorted run and
                // replayed for every right partition; the right input is not
                // repartitioned, so each right partition joins the full left.
                let left_partitions = self.left.output_partitioning().partition_count();
                assert_eq_or_internal_err!(
                    left_partitions,
                    1,
                    "Invalid SortMergeJoinExec in CollectLeft mode: the left child \
                         must have a single output partition, got {left_partitions}"
                );
                let left_fut = self.left_fut.try_once(|| {
                    let left_stream = self.left.execute(0, Arc::clone(&context))?;
                    let reservation = MemoryConsumer::new("SMJCollectLeft".to_string())
                        .register(context.memory_pool());
                    Ok(collect_left(left_stream, reservation))
                })?;
                let left: SendableRecordBatchStream =
                    Box::pin(CollectedLeftReplay::new(left_fut, self.left.schema()));
                let right = self.right.execute(partition, Arc::clone(&context))?;
                if probe_left {
                    (left, right, on_left, on_right)
                } else {
                    (right, left, on_right, on_left)
                }
            }
            PartitionMode::Partitioned | PartitionMode::Auto => {
                let left_partitions = self.left.output_partitioning().partition_count();
                let right_partitions = self.right.output_partitioning().partition_count();
                assert_eq_or_internal_err!(
                    left_partitions,
                    right_partitions,
                    "Invalid SortMergeJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                         consider using RepartitionExec"
                );
                let (streamed, buffered, on_streamed, on_buffered) = if probe_left {
                    (
                        Arc::clone(&self.left),
                        Arc::clone(&self.right),
                        on_left,
                        on_right,
                    )
                } else {
                    (
                        Arc::clone(&self.right),
                        Arc::clone(&self.left),
                        on_right,
                        on_left,
                    )
                };
                // execute children plans
                (
                    streamed.execute(partition, Arc::clone(&context))?,
                    buffered.execute(partition, Arc::clone(&context))?,
                    on_streamed,
                    on_buffered,
                )
            }
        };

        let batch_size = context.session_config().batch_size();
        let reservation = MemoryConsumer::new(format!("SMJStream[{partition}]"))
            .register(context.memory_pool());
        let spill_manager = SpillManager::new(
            context.runtime_env(),
            SpillMetrics::new(&self.metrics, partition),
            buffered.schema(),
        )
        .with_compression_type(context.session_config().spill_compression());

        if matches!(
            self.join_type,
            JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::LeftMark
                | JoinType::RightMark
        ) {
            Ok(Box::pin(BitwiseSortMergeJoinStream::try_new(
                Arc::clone(&self.schema),
                self.sort_options.clone(),
                self.null_equality,
                streamed,
                buffered,
                on_streamed,
                on_buffered,
                self.filter.clone(),
                self.join_type,
                batch_size,
                partition,
                &self.metrics,
                reservation,
                spill_manager,
                context.runtime_env(),
            )?))
        } else {
            Ok(Box::pin(MaterializingSortMergeJoinStream::try_new(
                Arc::clone(&self.schema),
                self.sort_options.clone(),
                self.null_equality,
                streamed,
                buffered,
                on_streamed,
                on_buffered,
                self.filter.clone(),
                self.join_type,
                batch_size,
                SortMergeJoinMetrics::new(partition, &self.metrics),
                reservation,
                spill_manager,
                context.runtime_env(),
            )?))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_with_args(&self, args: &StatisticsArgs) -> Result<Arc<Statistics>> {
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        let (left_stats, right_stats) = match (args.partition(), self.mode) {
            // In CollectLeft mode the left is collected into a single partition and
            // shared across all right partitions, so output partition `i` joins the
            // full left with partition `i` of the right.
            (Some(_), PartitionMode::CollectLeft) => (
                args.compute_child_statistics(&self.left, None)?,
                args.compute_child_statistics(&self.right, args.partition())?,
            ),
            // In Partitioned mode both inputs are hash-partitioned on the join keys,
            // so output partition `i` joins partition `i` of the left with partition
            // `i` of the right.
            (Some(_), PartitionMode::Partitioned | PartitionMode::Auto) => (
                args.compute_child_statistics(&self.left, args.partition())?,
                args.compute_child_statistics(&self.right, args.partition())?,
            ),
            // Whole-plan statistics combine both children's full statistics.
            (None, _) => (
                args.compute_child_statistics(&self.left, None)?,
                args.compute_child_statistics(&self.right, None)?,
            ),
        };
        Ok(Arc::new(estimate_join_statistics(
            Arc::unwrap_or_clone(left_stats),
            Arc::unwrap_or_clone(right_stats),
            &self.on,
            self.null_equality,
            &self.join_type,
            &self.schema,
        )?))
    }

    /// Tries to swap the projection with its input [`SortMergeJoinExec`]. If it can be done,
    /// it returns the new swapped version having the [`SortMergeJoinExec`] as the top plan.
    /// Otherwise, it returns None.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Convert projected PhysicalExpr's to columns. If not possible, we cannot proceed.
        let Some(projection_as_columns) = physical_to_column_exprs(projection.expr())
        else {
            return Ok(None);
        };

        let (far_right_left_col_ind, far_left_right_col_ind) = join_table_borders(
            self.left().schema().fields().len(),
            &projection_as_columns,
        );

        if !join_allows_pushdown(
            &projection_as_columns,
            &self.schema(),
            far_right_left_col_ind,
            far_left_right_col_ind,
        ) {
            return Ok(None);
        }

        let Some(new_on) = update_join_on(
            &projection_as_columns[0..=far_right_left_col_ind as _],
            &projection_as_columns[far_left_right_col_ind as _..],
            self.on(),
            self.left().schema().fields().len(),
        ) else {
            return Ok(None);
        };

        let (new_left, new_right) = new_join_children(
            &projection_as_columns,
            far_right_left_col_ind,
            far_left_right_col_ind,
            self.children()[0],
            self.children()[1],
        )?;

        Ok(Some(Arc::new(
            SortMergeJoinExec::try_new(
                Arc::new(new_left),
                Arc::new(new_right),
                new_on,
                self.filter.clone(),
                self.join_type,
                self.sort_options.clone(),
                self.null_equality,
            )?
            .with_mode(self.mode)?,
        )))
    }
}
