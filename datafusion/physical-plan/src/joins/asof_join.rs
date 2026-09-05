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

//! Broadcast, left-preserving ASOF join execution.
//!
//! An ASOF join emits exactly one output row for every left row. Within an
//! optional equality-key group, it selects the closest right row that satisfies
//! one ordered comparison. This follows Snowflake's [ASOF JOIN] semantics:
//!
//! ```text
//! left.ts >= right.ts  => greatest eligible right.ts
//! left.ts <= right.ts  => smallest eligible right.ts
//! ```
//!
//! The right input is collected and shared by all output partitions. The left
//! input remains partitioned, and each partition performs an independent
//! monotonic scan over the ordered right input.
//!
//! [`AsOfJoinExec::input_distribution_requirements`] requires a single right
//! partition but leaves the left distribution unrestricted.
//! [`AsOfJoinExec::required_input_ordering`] requires both inputs to be ordered.
//! The physical optimizer satisfies these contracts by inserting operators such
//! as `RepartitionExec`, `SortExec`, `CoalescePartitionsExec`, or
//! `SortPreservingMergeExec`, depending on the input properties. The inserted
//! plan shape is therefore not fixed by this operator.
//!
//! Both inputs must be ordered by their equality keys followed by the match
//! key. For `<` and `<=`, the match ordering is reversed so all directions use
//! the same forward-only state machine. For example:
//!
//! ```text
//! ON left.symbol = right.symbol MATCH_CONDITION(left.ts >= right.ts)
//!   left:  [left.symbol ASC NULLS FIRST, left.ts ASC NULLS FIRST]
//!   right: [right.symbol ASC NULLS FIRST, right.ts ASC NULLS FIRST]
//!
//! ON left.symbol = right.symbol MATCH_CONDITION(left.ts <= right.ts)
//!   left:  [left.symbol ASC NULLS FIRST, left.ts DESC NULLS FIRST]
//!   right: [right.symbol ASC NULLS FIRST, right.ts DESC NULLS FIRST]
//! ```
//!
//! Each left partition owns its cursors, equality-group state, and current
//! candidate, while the collected right batches are immutable and shared.
//! The key state-machine entry point is [`AsOfJoinStream::poll_next_impl`].
//!
//! This mode preserves probe-side parallelism when there are no equality keys
//! or when equality keys have low cardinality or skew. It retains the complete
//! right input in the memory pool and may scan it once per left partition.
//! Alternative strategies, including broadcasting the other side or
//! repartitioning both inputs, remain future work for other input-size and
//! key-distribution profiles.
//!
//! [ASOF JOIN]: https://docs.snowflake.com/en/sql-reference/constructs/asof-join

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    Array, ArrayRef, DynComparator, RecordBatch, RecordBatchOptions, make_comparator,
    new_null_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{SortOptions, interleave};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::utils::memory::RecordBatchMemoryCounter;
use datafusion_common::utils::normalize_float_zero;
use datafusion_common::{
    ColumnStatistics, JoinSide, JoinType, NullEquality, Result, Statistics,
    assert_eq_or_internal_err, internal_err, plan_err, project_schema,
};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::projection::{ProjectionMapping, ProjectionRef};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr_common::physical_expr::{
    PhysicalExprRef, fmt_sql, is_volatile,
};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};
use futures::{Stream, StreamExt, TryStreamExt, future::poll_fn, ready, stream};

use crate::execution_plan::{Boundedness, EmissionType};
use crate::joins::utils::{
    ColumnIndex, JoinKeyComparator, JoinOn, OnceAsync, build_join_schema,
    matchable_join_keys,
};
use crate::memory::MemoryStream;
use crate::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
    RecordOutput, Time,
};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, InputDistributionRequirements, PlanProperties,
    RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
    validate_child_count,
};

/// Physical ordered comparison for an ASOF join.
#[derive(Debug, Clone)]
pub struct AsOfMatchExpr {
    /// Expression evaluated against the left input.
    pub left: PhysicalExprRef,
    /// Ordered comparison operator.
    pub op: Operator,
    /// Expression evaluated against the right input.
    pub right: PhysicalExprRef,
}

impl AsOfMatchExpr {
    /// Creates a physical ASOF match expression.
    pub fn new(left: PhysicalExprRef, op: Operator, right: PhysicalExprRef) -> Self {
        Self { left, op, right }
    }
}

/// A broadcast sort-merge ASOF join that emits one row for every left row.
#[derive(Debug)]
pub struct AsOfJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    match_condition: AsOfMatchExpr,
    /// Unprojected left-join schema used to interpret `projection`.
    join_schema: SchemaRef,
    /// Information of index and left/right placement of columns.
    column_indices: Vec<ColumnIndex>,
    /// Optional indices into the full left-then-right join schema.
    projection: Option<ProjectionRef>,
    metrics: ExecutionPlanMetricsSet,
    /// Required ordering for each left partition.
    left_ordering: LexOrdering,
    /// Required global ordering for the single right partition.
    right_ordering: LexOrdering,
    /// Shared collection future that materializes the right input only once.
    right_fut: OnceAsync<BroadcastRightInput>,
    cache: Arc<PlanProperties>,
}

impl AsOfJoinExec {
    /// Creates a bounded ASOF join over sorted inputs.
    ///
    /// The match operator must be `<`, `<=`, `>`, or `>=`. Equality and match
    /// expressions must be deterministic, reference only their corresponding
    /// input, and have matching input types. Equality types must support hashing;
    /// floating-point equality keys are not supported because Arrow sorting
    /// distinguishes signed zero while SQL equality does not. Projection indices
    /// refer to the full left-then-right join schema.
    ///
    /// The logical ASOF constructor validates the corresponding pre-coercion
    /// contract. Keep the shared operator, side-ownership, and determinism checks
    /// aligned across both public entry points.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        match_condition: AsOfMatchExpr,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        validate_asof_join(left.as_ref(), right.as_ref(), &on, &match_condition)?;
        let left_schema = left.schema();
        let right_schema = right.schema();
        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, &JoinType::Left);
        let join_schema = Arc::new(join_schema);
        let projection: Option<ProjectionRef> = projection.map(Into::into);
        let descending = matches!(match_condition.op, Operator::Lt | Operator::LtEq);
        let equality_options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let match_options = SortOptions {
            descending,
            nulls_first: true,
        };
        let mut left_sort_exprs = on
            .iter()
            .map(|(left, _)| PhysicalSortExpr {
                expr: Arc::clone(left),
                options: equality_options,
            })
            .collect::<Vec<_>>();
        left_sort_exprs.push(PhysicalSortExpr {
            expr: Arc::clone(&match_condition.left),
            options: match_options,
        });
        let mut right_sort_exprs = on
            .iter()
            .map(|(_, right)| PhysicalSortExpr {
                expr: Arc::clone(right),
                options: equality_options,
            })
            .collect::<Vec<_>>();
        right_sort_exprs.push(PhysicalSortExpr {
            expr: Arc::clone(&match_condition.right),
            options: match_options,
        });
        let left_ordering = LexOrdering::new(left_sort_exprs).ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "ASOF left ordering must not be empty"
            )
        })?;
        let right_ordering = LexOrdering::new(right_sort_exprs).ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "ASOF right ordering must not be empty"
            )
        })?;
        let cache = Arc::new(Self::compute_properties(
            &left,
            &join_schema,
            projection.as_deref(),
        )?);

        Ok(Self {
            left,
            right,
            on,
            match_condition,
            join_schema,
            column_indices,
            projection,
            metrics: ExecutionPlanMetricsSet::new(),
            left_ordering,
            right_ordering,
            right_fut: Default::default(),
            cache,
        })
    }

    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        join_schema: &SchemaRef,
        projection: Option<&[usize]>,
    ) -> Result<PlanProperties> {
        let left_schema = left.schema();
        let mapping = ProjectionMapping::try_new(
            left_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    (
                        Arc::new(PhysicalColumn::new(field.name(), index))
                            as PhysicalExprRef,
                        field.name().to_string(),
                    )
                }),
            &left_schema,
        )?;
        let input_eq_properties = left.equivalence_properties();
        let mut eq_properties =
            input_eq_properties.project(&mapping, Arc::clone(join_schema));
        let mut output_partitioning = left
            .output_partitioning()
            .project(&mapping, input_eq_properties);
        if let Some(projection) = projection {
            let projection_mapping =
                ProjectionMapping::from_indices(projection, join_schema)?;
            let output_schema = project_schema(join_schema, Some(&projection))?;
            output_partitioning =
                output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, output_schema);
        }
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl DisplayAs for AsOfJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let on = self
            .on
            .iter()
            .map(|(left, right)| {
                format!("({} = {})", fmt_sql(left.as_ref()), fmt_sql(right.as_ref()))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let match_condition = format!(
            "{} {} {}",
            fmt_sql(self.match_condition.left.as_ref()),
            self.match_condition.op,
            fmt_sql(self.match_condition.right.as_ref())
        );
        let projection = self
            .projection
            .as_ref()
            .map(|projection| {
                format!(
                    ", projection=[{}]",
                    projection
                        .iter()
                        .map(|index| format!(
                            "{}@{}",
                            self.join_schema.field(*index).name(),
                            index
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .unwrap_or_default();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "{}: on=[{}], match=[{}]{}",
                Self::static_name(),
                on,
                match_condition,
                projection
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "on={on}")?;
                writeln!(f, "match={match_condition}")
            }
        }
    }
}

impl ExecutionPlan for AsOfJoinExec {
    fn name(&self) -> &'static str {
        "AsOfJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        // Every left partition scans the complete broadcast right input, so
        // equality keys do not require the inputs to be co-partitioned.
        // `UnspecifiedDistribution` imposes no layout requirement; because this
        // operator uses the default `benefits_from_input_partitioning`, the
        // optimizer may still add round-robin repartitioning when it is useful.
        InputDistributionRequirements::new(vec![
            Distribution::UnspecifiedDistribution,
            Distribution::SinglePartition,
        ])
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            Some(OrderingRequirements::from(self.left_ordering.clone())),
            Some(OrderingRequirements::from(self.right_ordering.clone())),
        ]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // ASOF emits exactly one row for each left row and never reorders the
        // left input. The right input is scanned independently.
        vec![true, false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn crate::PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        let join_keys = self.on.iter().flat_map(|(left, right)| [left, right]);
        crate::apply_expression_roots(
            join_keys.chain([&self.match_condition.left, &self.match_condition.right]),
            f,
        )
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        let left = children.swap_remove(0);
        let right = children.swap_remove(0);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                left,
                right,
                on: self.on.clone(),
                match_condition: self.match_condition.clone(),
                join_schema: Arc::clone(&self.join_schema),
                column_indices: self.column_indices.clone(),
                projection: self.projection.clone(),
                metrics: ExecutionPlanMetricsSet::new(),
                left_ordering: self.left_ordering.clone(),
                right_ordering: self.right_ordering.clone(),
                right_fut: Default::default(),
                cache: Arc::clone(&self.cache),
            })),
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(Self::try_new(
                left,
                right,
                self.on.clone(),
                self.match_condition.clone(),
                self.projection.as_deref().map(<[usize]>::to_vec),
            )?)),
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let right_partitions = self.right.output_partitioning().partition_count();
        assert_eq_or_internal_err!(
            right_partitions,
            1,
            "AsOfJoinExec requires one right partition, found {right_partitions}"
        );
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let metrics = AsOfJoinMetrics::new(partition, &self.metrics);
        let build_metrics = metrics.clone();
        let right_fut = self.right_fut.try_once(|| {
            let right_stream = self.right.execute(0, Arc::clone(&context))?;
            let reservation =
                MemoryConsumer::new("AsOfJoinInput").register(context.memory_pool());
            Ok(collect_right_input(
                right_stream,
                reservation,
                build_metrics,
            ))
        })?;
        let (left_keys, right_keys) = self.on.iter().cloned().unzip();
        let output_schema = self.schema();
        let stream_schema = Arc::clone(&output_schema);
        let left_match = Arc::clone(&self.match_condition.left);
        let right_match = Arc::clone(&self.match_condition.right);
        let match_op = self.match_condition.op;
        let column_indices = match self.projection.as_ref() {
            Some(projection) => projection
                .iter()
                .map(|index| self.column_indices[*index].clone())
                .collect(),
            None => self.column_indices.clone(),
        };
        let batch_size = context.session_config().batch_size();
        let stream = stream::once(async move {
            let mut right_fut = right_fut;
            let right_input = poll_fn(|cx| right_fut.get_shared(cx)).await?;
            let right_stream = right_input.stream()?;
            let stream = AsOfJoinStream::new(
                Arc::clone(&stream_schema),
                InputCursor::new(left_stream, left_keys, left_match),
                InputCursor::new(right_stream, right_keys, right_match),
                match_op,
                column_indices,
                batch_size,
                metrics,
                right_input,
            );
            Ok::<SendableRecordBatchStream, datafusion_common::DataFusionError>(Box::pin(
                stream,
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition), ChildStats::Skip]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        // The default is fully unknown, but ASOF emits exactly one output row
        // per left row and preserves statistics for unmodified left columns.
        let left = &input_stats[0];
        let column_indices_after_projection = match self.projection.as_ref() {
            Some(projection) => projection
                .iter()
                .map(|index| self.column_indices[*index].clone())
                .collect(),
            None => self.column_indices.clone(),
        };
        let column_statistics = column_indices_after_projection
            .iter()
            .map(|column| match column.side {
                JoinSide::Left => left
                    .column_statistics
                    .get(column.index)
                    .cloned()
                    .unwrap_or_else(ColumnStatistics::new_unknown),
                JoinSide::Right | JoinSide::None => ColumnStatistics::new_unknown(),
            })
            .collect();
        Ok(Arc::new(Statistics {
            num_rows: left.num_rows,
            total_byte_size: Precision::Absent,
            column_statistics,
        }))
    }
}

/// Materialized right input shared by every left output partition.
struct BroadcastRightInput {
    /// Schema retained even when the input has no batches.
    schema: SchemaRef,
    /// Ordered right batches; their buffers are shared without copying.
    batches: Vec<RecordBatch>,
    /// Holds the memory-pool reservation for as long as the batches are shared.
    _reservation: MemoryReservation,
}

impl BroadcastRightInput {
    fn stream(&self) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.batches.clone(),
            Arc::clone(&self.schema),
            None,
        )?))
    }
}

async fn collect_right_input(
    input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    metrics: AsOfJoinMetrics,
) -> Result<BroadcastRightInput> {
    let schema = input.schema();
    let mut memory_counter = RecordBatchMemoryCounter::new();
    let batches = input
        .try_fold(Vec::new(), |mut batches, batch| {
            let batch_size = memory_counter.count_batch(&batch);
            futures::future::ready(reservation.try_grow(batch_size).map(|_| {
                metrics.build_mem_used.add(batch_size);
                batches.push(batch);
                batches
            }))
        })
        .await?;
    Ok(BroadcastRightInput {
        schema,
        batches,
        _reservation: reservation,
    })
}

/// Last eligible right row for the current left equality group.
///
/// The row and its evaluated keys survive right batch changes and output
/// flushes. It belongs to the join state rather than `InputCursor` because its
/// validity also depends on the current left equality group.
#[derive(Clone)]
struct Candidate {
    /// Right batch containing the nearest eligible row.
    batch: Arc<RecordBatch>,
    /// Row index within `batch`.
    row: usize,
    /// Evaluated equality keys retained when the right cursor changes batches.
    key_arrays: Arc<[ArrayRef]>,
    /// Identity used to invalidate the cached candidate/left comparator.
    key_batch_id: usize,
}

/// Cursor over one ordered input stream.
///
/// Expressions are evaluated once per non-empty batch. `key_batch_id` changes
/// whenever a new batch is loaded so comparators cannot retain stale arrays.
struct InputCursor {
    /// Remaining input batches.
    stream: SendableRecordBatchStream,
    /// Equality expressions evaluated for each batch.
    key_exprs: Vec<PhysicalExprRef>,
    /// Ordered match expression evaluated for each batch.
    match_expr: PhysicalExprRef,
    /// Current non-empty batch.
    batch: Option<Arc<RecordBatch>>,
    /// Evaluated equality-key arrays for `batch`.
    key_arrays: Arc<[ArrayRef]>,
    /// Rows whose equality keys are all non-NULL.
    key_validity: Option<NullBuffer>,
    /// Evaluated and normalized match values for `batch`.
    match_array: Option<ArrayRef>,
    /// Logical NULLs cached separately so row checks avoid constructing
    /// `ScalarValue`s and still handle nested representations such as dictionaries.
    match_validity: Option<NullBuffer>,
    /// Monotonic identity of the current arrays.
    key_batch_id: usize,
    /// Current row within `batch`.
    row: usize,
    /// Whether the input stream has returned EOF.
    eof: bool,
}

impl InputCursor {
    fn new(
        stream: SendableRecordBatchStream,
        key_exprs: Vec<PhysicalExprRef>,
        match_expr: PhysicalExprRef,
    ) -> Self {
        Self {
            stream,
            key_exprs,
            match_expr,
            batch: None,
            key_arrays: Arc::from([]),
            key_validity: None,
            match_array: None,
            match_validity: None,
            key_batch_id: 0,
            row: 0,
            eof: false,
        }
    }

    fn poll_ensure_row(
        &mut self,
        cx: &mut Context<'_>,
        elapsed_compute: &Time,
    ) -> Poll<Result<bool>> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                return Poll::Ready(Ok(true));
            }
            self.batch = None;
            self.key_arrays = Arc::from([]);
            self.key_validity = None;
            self.match_array = None;
            self.match_validity = None;
            self.row = 0;
            if self.eof {
                return Poll::Ready(Ok(false));
            }
            let Some(batch) = ready!(self.stream.poll_next_unpin(cx)).transpose()? else {
                self.eof = true;
                return Poll::Ready(Ok(false));
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = Arc::new(batch);
            let _timer = elapsed_compute.timer();
            let key_arrays = self
                .key_exprs
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;
            self.key_validity =
                matchable_join_keys(&key_arrays, NullEquality::NullEqualsNothing);
            self.key_arrays = key_arrays.into();
            let match_array = self
                .match_expr
                .evaluate(&batch)?
                .into_array(batch.num_rows())?;
            self.match_validity = match_array.logical_nulls();
            // Arrow's comparator distinguishes -0.0 from +0.0, while SQL does
            // not. Normalizing once here is cheaper than fixing every row-level
            // comparison and lets the cached comparator use the arrays directly.
            self.match_array = Some(normalize_float_zero(&match_array));
            self.key_batch_id += 1;
            self.batch = Some(batch);
        }
    }

    fn group_has_null(&self) -> bool {
        self.key_validity
            .as_ref()
            .is_some_and(|validity| validity.is_null(self.row))
    }

    fn match_is_null(&self) -> bool {
        self.match_validity
            .as_ref()
            .is_some_and(|validity| validity.is_null(self.row))
    }

    fn batch_row(&self) -> Result<(Arc<RecordBatch>, usize)> {
        let batch = self.batch.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("ASOF input batch is missing")
        })?;
        Ok((Arc::clone(batch), self.row))
    }

    fn advance(&mut self) {
        self.row += 1;
    }
}

#[derive(Clone)]
struct AsOfJoinMetrics {
    /// Standard output-row and elapsed-compute metrics.
    baseline: BaselineMetrics,
    /// Peak bytes retained for the shared right input.
    ///
    /// `peak_memory_usage` records this as `MetricValue::PeakMemoryUsage`; `Gauge`
    /// is the handle used to update that metric.
    build_mem_used: Gauge,
}

impl AsOfJoinMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            build_mem_used: MetricBuilder::new(metrics)
                .peak_memory_usage("build_mem_used", partition),
        }
    }
}

/// Row references accumulated for the next output batch.
///
/// For right output, `None` represents NULL padding for an unmatched left row.
/// For example, indices `[Some((0, 2)), None, Some((1, 0))]` select row 2 from
/// the first source batch, a NULL, and row 0 from the second source batch.
#[derive(Default)]
struct PendingRows {
    /// Distinct source batches referenced by `indices`. `Arc` keeps per-row
    /// clones O(1) and provides stable identity for deduplication.
    sources: Vec<Arc<RecordBatch>>,
    /// Maps an `Arc<RecordBatch>` pointer to its index in `sources`.
    source_by_ptr: HashMap<usize, usize>,
    /// Per-output-row `(source, row)` references or NULL padding.
    indices: Vec<Option<(usize, usize)>>,
}

impl PendingRows {
    fn len(&self) -> usize {
        self.indices.len()
    }

    fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    fn push(&mut self, batch: Arc<RecordBatch>, row: usize) {
        let ptr = Arc::as_ptr(&batch) as usize;
        let source = *self.source_by_ptr.entry(ptr).or_insert_with(|| {
            let source = self.sources.len();
            self.sources.push(batch);
            source
        });
        self.indices.push(Some((source, row)));
    }

    fn push_null(&mut self) {
        self.indices.push(None);
    }

    fn materialize_column(
        &self,
        source_column: usize,
        data_type: &arrow::datatypes::DataType,
    ) -> Result<ArrayRef> {
        if self.indices.is_empty() {
            return internal_err!("ASOF output materialization has no pending rows");
        }

        if self.sources.len() == 1
            && self.indices.iter().all(Option::is_some)
            && let Some((0, first_row)) = self.indices[0]
            && self
                .indices
                .iter()
                .enumerate()
                .all(|(offset, index)| *index == Some((0, first_row + offset)))
        {
            return Ok(self.sources[0]
                .column(source_column)
                .slice(first_row, self.indices.len()));
        }

        let has_null = self.indices.iter().any(Option::is_none);
        let null_array = has_null.then(|| new_null_array(data_type, 1));
        let mut source_arrays: Vec<&dyn Array> =
            Vec::with_capacity(self.sources.len() + usize::from(has_null));
        if let Some(null_array) = &null_array {
            source_arrays.push(null_array.as_ref());
        }
        source_arrays.extend(
            self.sources
                .iter()
                .map(|batch| batch.column(source_column).as_ref()),
        );
        let source_offset = usize::from(has_null);
        let interleave_indices = self
            .indices
            .iter()
            .map(|index| match index {
                Some((source, row)) => (source + source_offset, *row),
                None => (0, 0),
            })
            .collect::<Vec<_>>();
        interleave(&source_arrays, &interleave_indices).map_err(Into::into)
    }

    fn clear(&mut self) {
        self.sources.clear();
        self.source_by_ptr.clear();
        self.indices.clear();
    }
}

/// Per-left-partition state for the monotonic ASOF scan.
///
/// For left rows `(A, 4), (A, 7)` and right rows `(A, 2), (A, 6)`, the
/// candidate advances from `(A, 2)` to `(A, 6)` without rewinding the right
/// cursor. Cursors and the candidate survive input batch changes and output
/// flushes; a change of equality group clears the candidate before reuse.
///
/// The hot path avoids materializing scalar values: expressions are evaluated
/// once per batch, comparators are cached by batch identity, and both cursors
/// only move forward. Input batches and evaluated arrays remain shared through
/// `Arc`; output can therefore use a zero-copy slice when its rows are contiguous.
struct AsOfJoinStream {
    /// Output schema used when pending row references are materialized.
    schema: SchemaRef,
    /// Cursor over the current left partition.
    left: InputCursor,
    /// Independent cursor over the shared, ordered right input.
    right: InputCursor,
    /// Retains the shared right batches and their memory reservation.
    _right_input: Arc<BroadcastRightInput>,
    /// Validated ordered match operator.
    op: Operator,
    /// Projected output columns and their input sides.
    column_indices: Vec<ColumnIndex>,
    /// Whether any projected column needs a right row reference.
    projects_right: bool,
    /// Nearest eligible right row for the current equality group.
    candidate: Option<Candidate>,
    /// Equality-key ordering shared by the comparator caches.
    group_sort_options: Vec<SortOptions>,
    /// Cached comparator for the current right and left input batches.
    input_group_comparator: Option<(usize, usize, JoinKeyComparator)>,
    /// Cached comparator for the candidate and current left batches.
    candidate_group_comparator: Option<(usize, usize, JoinKeyComparator)>,
    /// Cached match-key comparator for the current right and left batches.
    /// Building it performs type dispatch, so doing so once per batch pair avoids
    /// repeating that work for every candidate comparison.
    input_match_comparator: Option<(usize, usize, DynComparator)>,
    /// Left row references accumulated for the next output batch.
    pending_left: PendingRows,
    /// Matched right row references, aligned with `pending_left`.
    pending_right: PendingRows,
    /// Maximum number of pending rows before an output flush.
    batch_size: usize,
    metrics: AsOfJoinMetrics,
}

impl AsOfJoinStream {
    #[expect(clippy::too_many_arguments)]
    fn new(
        schema: SchemaRef,
        left: InputCursor,
        right: InputCursor,
        op: Operator,
        column_indices: Vec<ColumnIndex>,
        batch_size: usize,
        metrics: AsOfJoinMetrics,
        right_input: Arc<BroadcastRightInput>,
    ) -> Self {
        let group_sort_options = vec![
            SortOptions {
                descending: false,
                nulls_first: true,
            };
            left.key_exprs.len()
        ];
        Self {
            pending_left: PendingRows::default(),
            pending_right: PendingRows::default(),
            schema,
            left,
            right,
            _right_input: right_input,
            op,
            projects_right: column_indices
                .iter()
                .any(|column| column.side == JoinSide::Right),
            column_indices,
            candidate: None,
            group_sort_options,
            input_group_comparator: None,
            candidate_group_comparator: None,
            input_match_comparator: None,
            batch_size: batch_size.max(1),
            metrics,
        }
    }

    fn compare_input_groups(&mut self) -> Result<Ordering> {
        if self.group_sort_options.is_empty() {
            return Ok(Ordering::Equal);
        }
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let right_batch_id = self.right.key_batch_id;
        let left_batch_id = self.left.key_batch_id;
        if self
            .input_group_comparator
            .as_ref()
            .is_none_or(|(right, left, _)| {
                *right != right_batch_id || *left != left_batch_id
            })
        {
            let comparator = JoinKeyComparator::new(
                self.right.key_arrays.as_ref(),
                self.left.key_arrays.as_ref(),
                &self.group_sort_options,
                NullEquality::NullEqualsNothing,
            )?;
            self.input_group_comparator =
                Some((right_batch_id, left_batch_id, comparator));
        }
        let (_, _, comparator) = self
            .input_group_comparator
            .as_ref()
            .expect("ASOF input group comparator must be initialized");
        Ok(comparator.compare(self.right.row, self.left.row))
    }

    fn candidate_is_other_group(&mut self) -> Result<bool> {
        let Some(candidate) = &self.candidate else {
            return Ok(false);
        };
        if self.group_sort_options.is_empty() {
            return Ok(false);
        }
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let candidate_batch_id = candidate.key_batch_id;
        let left_batch_id = self.left.key_batch_id;
        if self
            .candidate_group_comparator
            .as_ref()
            .is_none_or(|(candidate, left, _)| {
                *candidate != candidate_batch_id || *left != left_batch_id
            })
        {
            let comparator = JoinKeyComparator::new(
                candidate.key_arrays.as_ref(),
                self.left.key_arrays.as_ref(),
                &self.group_sort_options,
                NullEquality::NullEqualsNothing,
            )?;
            self.candidate_group_comparator =
                Some((candidate_batch_id, left_batch_id, comparator));
        }
        let (_, _, comparator) = self
            .candidate_group_comparator
            .as_ref()
            .expect("ASOF candidate group comparator must be initialized");
        Ok(comparator.compare(candidate.row, self.left.row) != Ordering::Equal)
    }

    /// Compares the current right match value with the current left match value.
    ///
    /// This always returns natural ascending order (`right.cmp(left)`), regardless
    /// of scan direction. [`is_eligible`] interprets that order for the four ASOF
    /// operators, keeping direction-specific logic out of the comparator cache.
    fn compare_input_matches(&mut self) -> Result<Ordering> {
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let right_batch_id = self.right.key_batch_id;
        let left_batch_id = self.left.key_batch_id;
        if self
            .input_match_comparator
            .as_ref()
            .is_none_or(|(right, left, _)| {
                *right != right_batch_id || *left != left_batch_id
            })
        {
            let right = self.right.match_array.as_ref().ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "ASOF right match array is missing"
                )
            })?;
            let left = self.left.match_array.as_ref().ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "ASOF left match array is missing"
                )
            })?;
            let comparator = make_comparator(
                right.as_ref(),
                left.as_ref(),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )?;
            self.input_match_comparator =
                Some((right_batch_id, left_batch_id, comparator));
        }
        let (_, _, comparator) = self
            .input_match_comparator
            .as_ref()
            .expect("ASOF input match comparator must be initialized");
        Ok(comparator(self.right.row, self.left.row))
    }

    /// Produces the next output batch without resetting the merge state.
    ///
    /// Each left row first validates its equality group, then advances the right
    /// cursor while right groups sort before it or right match values remain
    /// eligible. The last eligible right row becomes the candidate. Empty input
    /// batches are skipped. Right EOF preserves that candidate for later left
    /// rows in the same group; left EOF flushes the final pending rows. NULL keys
    /// and group changes clear the candidate, while output flushes only clear
    /// pending row references.
    ///
    /// ```text
    /// while the output batch is not full:
    ///   load the current left row, or flush/finish at left EOF
    ///   if its match or equality key is NULL, emit it unmatched and advance left
    ///   clear the candidate if the left equality group changed
    ///   while the current right row is before the left group or is eligible:
    ///     remember the nearest eligible row and advance right
    ///   emit the left row with the candidate (or NULLs), then advance left
    /// flush pending rows without resetting either cursor or the candidate
    /// ```
    fn poll_next_impl(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            if self.pending_left.len() >= self.batch_size {
                return Poll::Ready(Some(self.flush()));
            }
            if !ready!(
                self.left
                    .poll_ensure_row(cx, self.metrics.baseline.elapsed_compute())
            )? {
                if !self.pending_left.is_empty() {
                    return Poll::Ready(Some(self.flush()));
                }
                self.metrics.baseline.done();
                return Poll::Ready(None);
            }

            if self.left.match_is_null() || self.left.group_has_null() {
                self.candidate = None;
                self.candidate_group_comparator = None;
                self.push_current_left(None)?;
                self.left.advance();
                continue;
            }
            if self.candidate_is_other_group()? {
                self.candidate = None;
                self.candidate_group_comparator = None;
            }

            loop {
                if !ready!(
                    self.right
                        .poll_ensure_row(cx, self.metrics.baseline.elapsed_compute())
                )? {
                    break;
                }
                if self.right.group_has_null() {
                    self.right.advance();
                    continue;
                }
                match self.compare_input_groups()? {
                    Ordering::Less => {
                        self.right.advance();
                        continue;
                    }
                    Ordering::Greater => break,
                    Ordering::Equal => {}
                }
                if self.right.match_is_null() {
                    self.right.advance();
                    continue;
                }
                if !is_eligible(self.op, self.compare_input_matches()?) {
                    break;
                }
                let (batch, row) = self.right.batch_row()?;
                // Replacing the candidate selects the nearest eligible row.
                // Equal match values have no secondary ordering, so which tied
                // row wins is intentionally nondeterministic.
                self.candidate = Some(Candidate {
                    batch,
                    row,
                    key_arrays: Arc::clone(&self.right.key_arrays),
                    key_batch_id: self.right.key_batch_id,
                });
                self.right.advance();
            }

            self.push_current_left(self.candidate.clone())?;
            self.left.advance();
        }
    }

    fn push_current_left(&mut self, candidate: Option<Candidate>) -> Result<()> {
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let (left_batch, left_row) = self.left.batch_row()?;
        self.pending_left.push(left_batch, left_row);
        match candidate {
            Some(candidate) => {
                if self.projects_right {
                    self.pending_right.push(candidate.batch, candidate.row);
                }
            }
            None => {
                if self.projects_right {
                    self.pending_right.push_null();
                }
            }
        }
        Ok(())
    }

    /// Materializes pending row references while preserving both cursors and the
    /// current equality-group candidate for the next output batch.
    fn flush(&mut self) -> Result<RecordBatch> {
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let row_count = self.pending_left.len();
        let mut arrays = Vec::with_capacity(self.schema.fields().len());
        for (field, column) in self.schema.fields().iter().zip(&self.column_indices) {
            let pending = match column.side {
                JoinSide::Left => &self.pending_left,
                JoinSide::Right => &self.pending_right,
                JoinSide::None => {
                    return internal_err!("ASOF projection cannot contain a mark column");
                }
            };
            arrays.push(pending.materialize_column(column.index, field.data_type())?);
        }
        self.pending_left.clear();
        self.pending_right.clear();
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            arrays,
            &options,
        )?;
        (&batch).record_output(&self.metrics.baseline);
        Ok(batch)
    }
}

impl RecordBatchStream for AsOfJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for AsOfJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

/// Validates all invariants required by the forward-only ASOF state machine.
fn validate_asof_join(
    left: &dyn ExecutionPlan,
    right: &dyn ExecutionPlan,
    on: &JoinOn,
    match_condition: &AsOfMatchExpr,
) -> Result<()> {
    if !matches!(
        match_condition.op,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return plan_err!(
            "AsOfJoinExec requires <, <=, >, or >=, found {}",
            match_condition.op
        );
    }
    if left.boundedness().is_unbounded() || right.boundedness().is_unbounded() {
        return plan_err!("AsOfJoinExec requires bounded inputs");
    }
    if is_volatile(&match_condition.left) || is_volatile(&match_condition.right) {
        return plan_err!("AsOfJoinExec match expression must be deterministic");
    }
    if on
        .iter()
        .any(|(left, right)| is_volatile(left) || is_volatile(right))
    {
        return plan_err!("AsOfJoinExec equality expressions must be deterministic");
    }

    let left_schema = left.schema();
    let right_schema = right.schema();
    validate_expr_side(&match_condition.left, &left_schema, "left match")?;
    validate_expr_side(&match_condition.right, &right_schema, "right match")?;
    for (left_expr, right_expr) in on {
        validate_expr_side(left_expr, &left_schema, "left equality")?;
        validate_expr_side(right_expr, &right_schema, "right equality")?;
        let left_type = left_expr.data_type(&left_schema)?;
        let right_type = right_expr.data_type(&right_schema)?;
        if left_type != right_type {
            return plan_err!(
                "AsOfJoinExec equality expression types differ: {left_type} and {right_type}"
            );
        }
        if !datafusion_expr::utils::can_hash(&left_type) {
            return plan_err!(
                "AsOfJoinExec equality expressions have unsupported hash type {left_type}"
            );
        }
        if left_type.is_floating() {
            return plan_err!(
                "AsOfJoinExec equality expressions do not support floating-point type {left_type}"
            );
        }
    }
    let left_match_type = match_condition.left.data_type(&left_schema)?;
    let right_match_type = match_condition.right.data_type(&right_schema)?;
    if left_match_type != right_match_type {
        return plan_err!(
            "AsOfJoinExec match expression types differ: {left_match_type} and {right_match_type}"
        );
    }
    Ok(())
}

fn validate_expr_side(expr: &PhysicalExprRef, schema: &Schema, name: &str) -> Result<()> {
    let columns = collect_columns(expr);
    if columns.is_empty() {
        return plan_err!("AsOfJoinExec {name} expression must reference its input");
    }
    if let Some(column) = columns.iter().find(|column| {
        schema
            .fields()
            .get(column.index())
            .is_none_or(|field| field.name() != column.name())
    }) {
        return plan_err!(
            "AsOfJoinExec {name} expression references column {column} outside its input"
        );
    }
    Ok(())
}

fn is_eligible(op: Operator, right_vs_left: Ordering) -> bool {
    match op {
        Operator::Gt => right_vs_left == Ordering::Less,
        Operator::GtEq => right_vs_left != Ordering::Greater,
        Operator::Lt => right_vs_left == Ordering::Greater,
        Operator::LtEq => right_vs_left != Ordering::Less,
        _ => unreachable!("ASOF match operator is validated by try_new"),
    }
}

#[cfg(test)]
mod tests {
    // Keep physical tests focused on basic executor results, batch-boundary
    // state, shared build memory, and constructor/statistics contracts.

    use super::*;
    use crate::collect;
    use crate::test::TestMemoryExec;
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_common::test_util::batches_to_sort_string;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr::expressions::{BinaryExpr, CastExpr};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use insta::assert_snapshot;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct VolatileExpr;

    impl std::fmt::Display for VolatileExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "volatile")
        }
    }

    impl PhysicalExpr for VolatileExpr {
        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
            Ok(false)
        }

        fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))))
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }

        fn is_volatile_node(&self) -> bool {
            true
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "volatile()")
        }
    }

    fn make_batch(
        schema: &SchemaRef,
        keys: Vec<Option<&str>>,
        times: Vec<Option<i64>>,
        values: Vec<i32>,
    ) -> Result<RecordBatch> {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(times)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .map_err(Into::into)
    }

    fn test_exec() -> Result<Arc<AsOfJoinExec>> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let left_batches = vec![
            RecordBatch::new_empty(Arc::clone(&left_schema)),
            make_batch(&left_schema, vec![None], vec![Some(3)], vec![0])?,
            make_batch(
                &left_schema,
                vec![Some("A"), Some("A")],
                vec![None, Some(1)],
                vec![1, 2],
            )?,
            make_batch(
                &left_schema,
                vec![Some("A"), Some("A")],
                vec![Some(4), Some(7)],
                vec![3, 4],
            )?,
            make_batch(
                &left_schema,
                vec![Some("B"), Some("C")],
                vec![Some(2), Some(3)],
                vec![5, 6],
            )?,
        ];
        let left = TestMemoryExec::try_new_exec(
            &[left_batches],
            Arc::clone(&left_schema),
            None,
        )?;

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
            Field::new("price", DataType::Int32, false),
        ]));
        let right_batches = vec![
            RecordBatch::new_empty(Arc::clone(&right_schema)),
            make_batch(
                &right_schema,
                vec![None, Some("A")],
                vec![Some(2), None],
                vec![999, 777],
            )?,
            make_batch(&right_schema, vec![Some("A")], vec![Some(2)], vec![20])?,
            make_batch(&right_schema, vec![Some("A")], vec![Some(4)], vec![40])?,
            RecordBatch::new_empty(Arc::clone(&right_schema)),
            make_batch(
                &right_schema,
                vec![Some("A"), Some("B")],
                vec![Some(6), Some(1)],
                vec![60, 101],
            )?,
        ];
        let right = TestMemoryExec::try_new_exec(
            &[right_batches],
            Arc::clone(&right_schema),
            None,
        )?;

        let on: JoinOn = vec![(
            Arc::new(PhysicalColumn::new("key", 0)),
            Arc::new(PhysicalColumn::new("key", 0)),
        )];
        Ok(Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            on,
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                Operator::GtEq,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            Some(vec![0, 1, 2, 5]),
        )?))
    }

    #[tokio::test]
    async fn simple_query() -> Result<()> {
        let exec = test_exec()?;
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(2)),
        );
        let batches = collect(Arc::clone(&exec) as _, context).await?;
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 2, 1]
        );
        assert_snapshot!(batches_to_sort_string(&batches), @r"
        +-----+----+----+-------+
        | key | ts | id | price |
        +-----+----+----+-------+
        |     | 3  | 0  |       |
        | A   |    | 1  |       |
        | A   | 1  | 2  |       |
        | A   | 4  | 3  | 40    |
        | A   | 7  | 4  | 60    |
        | B   | 2  | 5  | 101   |
        | C   | 3  | 6  |       |
        +-----+----+----+-------+
        ");

        let metrics = exec.metrics().expect("ASOF metrics must be present");
        assert_eq!(metrics.output_rows(), Some(7));
        assert!(metrics.elapsed_compute().is_some());
        Ok(())
    }

    fn exec_without_equality_keys(
        left_times: Vec<i64>,
        right_times: Vec<i64>,
        op: Operator,
    ) -> Result<Arc<AsOfJoinExec>> {
        let left_batch = RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(Int64Array::from(left_times.clone())) as ArrayRef,
            ),
            (
                "id",
                Arc::new(Int32Array::from(
                    left_times
                        .into_iter()
                        .map(|value| value as i32)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
        ])?;
        let left_schema = left_batch.schema();
        let left = TestMemoryExec::try_new_exec(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?;

        let right_batch = RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(Int64Array::from(right_times.clone())) as ArrayRef,
            ),
            (
                "price",
                Arc::new(Int32Array::from(
                    right_times
                        .into_iter()
                        .map(|value| value as i32 * 10)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
        ])?;
        let right_schema = right_batch.schema();
        let right = TestMemoryExec::try_new_exec(
            &[vec![right_batch]],
            Arc::clone(&right_schema),
            None,
        )?;

        Ok(Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            vec![],
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 0)),
                op,
                Arc::new(PhysicalColumn::new("ts", 0)),
            ),
            Some(vec![1, 3]),
        )?))
    }

    #[tokio::test]
    async fn comparison_directions_without_equality_keys() -> Result<()> {
        let predecessor =
            exec_without_equality_keys(vec![1, 4, 7], vec![2, 4, 6], Operator::GtEq)?;
        let predecessor = collect(predecessor, Arc::new(TaskContext::default())).await?;
        assert_snapshot!(batches_to_sort_string(&predecessor), @r"
        +----+-------+
        | id | price |
        +----+-------+
        | 1  |       |
        | 4  | 40    |
        | 7  | 60    |
        +----+-------+
        ");

        let successor =
            exec_without_equality_keys(vec![7, 4, 1], vec![6, 4, 2], Operator::Lt)?;
        let successor = collect(successor, Arc::new(TaskContext::default())).await?;
        assert_snapshot!(batches_to_sort_string(&successor), @r"
        +----+-------+
        | id | price |
        +----+-------+
        | 1  | 20    |
        | 4  | 60    |
        | 7  |       |
        +----+-------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn floating_match_treats_signed_zero_as_equal() -> Result<()> {
        let left_batch = RecordBatch::try_from_iter(vec![
            ("ts", Arc::new(Float64Array::from(vec![0.0])) as ArrayRef),
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])?;
        let left_schema = left_batch.schema();
        let left = TestMemoryExec::try_new_exec(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?;

        let right_batch = RecordBatch::try_from_iter(vec![
            ("ts", Arc::new(Float64Array::from(vec![-0.0])) as ArrayRef),
            ("price", Arc::new(Int32Array::from(vec![42])) as ArrayRef),
        ])?;
        let right_schema = right_batch.schema();
        let right = TestMemoryExec::try_new_exec(
            &[vec![right_batch]],
            Arc::clone(&right_schema),
            None,
        )?;

        let exec = Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            vec![],
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 0)),
                Operator::Gt,
                Arc::new(PhysicalColumn::new("ts", 0)),
            ),
            Some(vec![1, 3]),
        )?);
        let batches = collect(exec, Arc::new(TaskContext::default())).await?;
        let price = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("price must be Int32");
        assert!(price.is_null(0));
        Ok(())
    }

    #[tokio::test]
    async fn complex_equality_and_match_expressions() -> Result<()> {
        let left_batch = RecordBatch::try_from_iter(vec![
            ("g1", Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef),
            ("g2", Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef),
            ("ts", Arc::new(Int64Array::from(vec![4, 4])) as ArrayRef),
            ("offset", Arc::new(Int64Array::from(vec![1, 0])) as ArrayRef),
            ("id", Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef),
        ])?;
        let left_schema = left_batch.schema();
        let left = TestMemoryExec::try_new_exec(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?;

        let right_batch = RecordBatch::try_from_iter(vec![
            ("g1", Arc::new(Int64Array::from(vec![0, 0, 1])) as ArrayRef),
            ("g2", Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef),
            ("ts", Arc::new(Int64Array::from(vec![2, 5, 3])) as ArrayRef),
            (
                "price",
                Arc::new(Int64Array::from(vec![12, 15, 23])) as ArrayRef,
            ),
        ])?;
        let right_schema = right_batch.schema();
        let right = TestMemoryExec::try_new_exec(
            &[vec![right_batch]],
            Arc::clone(&right_schema),
            None,
        )?;

        let left_group = Arc::new(BinaryExpr::new(
            Arc::new(PhysicalColumn::new("g1", 0)),
            Operator::Plus,
            Arc::new(PhysicalColumn::new("g2", 1)),
        ));
        let right_group = Arc::new(BinaryExpr::new(
            Arc::new(PhysicalColumn::new("g1", 0)),
            Operator::Plus,
            Arc::new(PhysicalColumn::new("g2", 1)),
        ));
        let left_match = Arc::new(BinaryExpr::new(
            Arc::new(PhysicalColumn::new("ts", 2)),
            Operator::Plus,
            Arc::new(PhysicalColumn::new("offset", 3)),
        ));
        let exec = Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            vec![(left_group, right_group)],
            AsOfMatchExpr::new(
                left_match,
                Operator::GtEq,
                Arc::new(PhysicalColumn::new("ts", 2)),
            ),
            Some(vec![4, 8]),
        )?);

        let batches = collect(exec, Arc::new(TaskContext::default())).await?;
        assert_snapshot!(batches_to_sort_string(&batches), @r"
        +----+-------+
        | id | price |
        +----+-------+
        | 10 | 15    |
        | 20 | 23    |
        +----+-------+
        ");
        Ok(())
    }

    // Ensure the build-side memory usage equals the sum of all build-side input
    // batches, verifying that the build-side buffer is shared.
    #[tokio::test]
    async fn shared_right_buffers_are_reserved_once() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let left = TestMemoryExec::try_new_exec(
            &[vec![make_batch(
                &left_schema,
                vec![Some("A")],
                vec![Some(4095)],
                vec![0],
            )?]],
            Arc::clone(&left_schema),
            None,
        )?;

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Int32, false),
        ]));
        let row_count = 4096;
        let parent = make_batch(
            &right_schema,
            vec![Some("A"); row_count],
            (0..row_count).map(|value| Some(value as i64)).collect(),
            (0..row_count as i32).collect(),
        )?;
        let mut memory_counter = RecordBatchMemoryCounter::new();
        let retained_size = memory_counter.count_batch(&parent);
        let right_batches = (0..16)
            .map(|index| parent.slice(index * 256, 256))
            .collect();
        let right = TestMemoryExec::try_new_exec(
            &[right_batches],
            Arc::clone(&right_schema),
            None,
        )?;

        let exec = Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            vec![(
                Arc::new(PhysicalColumn::new("key", 0)),
                Arc::new(PhysicalColumn::new("key", 0)),
            )],
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                Operator::GtEq,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            Some(vec![0, 1, 2, 5]),
        )?);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(retained_size, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));

        let batches = collect(Arc::clone(&exec) as _, context).await?;
        let prices = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(prices, vec![Some(4095)]);

        let metrics = exec.metrics().expect("ASOF metrics must be present");
        assert_eq!(
            metrics
                .sum_by_name("build_mem_used")
                .map(|value| value.as_usize()),
            Some(retained_size)
        );
        Ok(())
    }

    #[test]
    fn rejects_volatile_physical_expressions() -> Result<()> {
        let exec = test_exec()?;
        let volatile = Arc::new(VolatileExpr) as PhysicalExprRef;
        let match_error = AsOfJoinExec::try_new(
            Arc::clone(&exec.left),
            Arc::clone(&exec.right),
            exec.on.clone(),
            AsOfMatchExpr::new(
                Arc::clone(&volatile),
                Operator::GtEq,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            Some(vec![0, 1, 2, 5]),
        )
        .expect_err("volatile match expression must be rejected");
        assert!(match_error.to_string().contains("must be deterministic"));

        let equality_error = AsOfJoinExec::try_new(
            Arc::clone(&exec.left),
            Arc::clone(&exec.right),
            vec![(volatile, Arc::new(PhysicalColumn::new("key", 0)))],
            exec.match_condition.clone(),
            Some(vec![0, 1, 2, 5]),
        )
        .expect_err("volatile equality expression must be rejected");
        assert!(equality_error.to_string().contains("must be deterministic"));
        Ok(())
    }

    #[test]
    fn rejects_floating_equality_expressions() -> Result<()> {
        let exec = test_exec()?;
        for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
            let left = Arc::new(CastExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                data_type.clone(),
                None,
            ));
            let right = Arc::new(CastExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                data_type.clone(),
                None,
            ));
            let error = AsOfJoinExec::try_new(
                Arc::clone(&exec.left),
                Arc::clone(&exec.right),
                vec![(left, right)],
                exec.match_condition.clone(),
                Some(vec![0, 1, 2, 5]),
            )
            .expect_err("floating equality expressions must be rejected");
            assert!(
                error.to_string().contains(&format!(
                    "equality expressions do not support floating-point type {data_type}"
                )),
                "unexpected error: {error}"
            );
        }
        Ok(())
    }

    #[test]
    fn statistics_follow_left_preserving_contract() -> Result<()> {
        let exec = test_exec()?;
        let mut key_stats = ColumnStatistics::new_unknown();
        key_stats.null_count = Precision::Exact(1);
        key_stats.distinct_count = Precision::Exact(4);
        let mut ts_stats = ColumnStatistics::new_unknown();
        ts_stats.min_value = Precision::Exact(ScalarValue::Int64(Some(1)));
        ts_stats.max_value = Precision::Exact(ScalarValue::Int64(Some(7)));
        let mut id_stats = ColumnStatistics::new_unknown();
        id_stats.null_count = Precision::Exact(0);
        id_stats.distinct_count = Precision::Exact(7);
        let left_column_statistics = vec![key_stats, ts_stats, id_stats];
        let left_stats = Arc::new(Statistics {
            num_rows: Precision::Exact(7),
            total_byte_size: Precision::Exact(128),
            column_statistics: left_column_statistics.clone(),
        });
        let right_stats = Arc::new(Statistics::new_unknown(&exec.right.schema()));
        let stats = exec
            .statistics_from_inputs(&[left_stats, right_stats], &StatisticsArgs::new())?;
        assert_eq!(stats.num_rows, Precision::Exact(7));
        assert_eq!(stats.total_byte_size, Precision::Absent);
        assert_eq!(stats.column_statistics.len(), 4);
        assert_eq!(
            &stats.column_statistics[..3],
            left_column_statistics.as_slice()
        );
        assert_eq!(stats.column_statistics[3], ColumnStatistics::new_unknown());
        assert_eq!(
            exec.child_stats_requests(None),
            vec![ChildStats::At(None), ChildStats::Skip]
        );
        Ok(())
    }
}
