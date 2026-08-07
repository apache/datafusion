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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow::buffer::NullBuffer;
use arrow::compute::{SortOptions, interleave};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::utils::memory::RecordBatchMemoryCounter;
use datafusion_common::utils::normalize_float_zero_scalar;
use datafusion_common::{
    ColumnStatistics, JoinType, NullEquality, Result, ScalarValue, Statistics,
    assert_eq_or_internal_err, internal_err, plan_err,
};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::projection::ProjectionMapping;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr_common::physical_expr::{
    PhysicalExprRef, fmt_sql, is_volatile,
};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};
use futures::{StreamExt, TryStreamExt, future::poll_fn, stream};

use crate::execution_plan::{Boundedness, EmissionType};
use crate::joins::utils::{
    JoinKeyComparator, JoinOn, OnceAsync, build_join_schema, matchable_join_keys,
};
use crate::memory::MemoryStream;
use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder,
    MetricCategory, MetricsSet, RecordOutput, Time,
};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    InputDistributionRequirements, PlanProperties, SendableRecordBatchStream,
    check_if_same_properties,
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
    /// Sorted, unique indices of right columns appended after all left columns.
    right_output_indices: Vec<usize>,
    schema: SchemaRef,
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
    /// input, and have matching input types. Equality types must support hashing.
    /// Right output indices must be in bounds, sorted, and unique.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        match_condition: AsOfMatchExpr,
        right_output_indices: Vec<usize>,
    ) -> Result<Self> {
        validate_asof_join(
            left.as_ref(),
            right.as_ref(),
            &on,
            &match_condition,
            &right_output_indices,
        )?;
        let left_schema = left.schema();
        let right_schema = right.schema();
        let schema =
            build_output_schema(&left_schema, &right_schema, &right_output_indices);
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
        let cache = Arc::new(Self::compute_properties(&left, &schema)?);

        Ok(Self {
            left,
            right,
            on,
            match_condition,
            right_output_indices,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            left_ordering,
            right_ordering,
            right_fut: Default::default(),
            cache,
        })
    }

    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
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
        let eq_properties = input_eq_properties.project(&mapping, Arc::clone(schema));
        let output_partitioning = left
            .output_partitioning()
            .project(&mapping, input_eq_properties);
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

fn build_output_schema(
    left: &SchemaRef,
    right: &SchemaRef,
    right_output_indices: &[usize],
) -> SchemaRef {
    let full_schema = build_join_schema(left, right, &JoinType::Left).0;
    let left_len = left.fields().len();
    let fields = full_schema
        .fields()
        .iter()
        .take(left_len)
        .cloned()
        .chain(
            right_output_indices
                .iter()
                .map(|index| Arc::clone(&full_schema.fields()[left_len + *index])),
        )
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        full_schema.metadata().clone(),
    ))
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
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "{}: on=[{}], match=[{}]",
                Self::static_name(),
                on,
                match_condition
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
        vec![false, false]
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
            [left, right] => Ok(Arc::new(Self::try_new(
                Arc::clone(left),
                Arc::clone(right),
                self.on.clone(),
                self.match_condition.clone(),
                self.right_output_indices.clone(),
            )?)),
            _ => internal_err!("AsOfJoinExec requires two children"),
        }
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq_or_internal_err!(
            children.len(),
            2,
            "AsOfJoinExec requires two children"
        );
        let left = children.remove(0);
        let right = children.remove(0);
        Ok(Arc::new(Self {
            left,
            right,
            on: self.on.clone(),
            match_condition: self.match_condition.clone(),
            right_output_indices: self.right_output_indices.clone(),
            schema: Arc::clone(&self.schema),
            metrics: ExecutionPlanMetricsSet::new(),
            left_ordering: self.left_ordering.clone(),
            right_ordering: self.right_ordering.clone(),
            right_fut: Default::default(),
            cache: Arc::clone(&self.cache),
        }))
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
        let output_schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&output_schema);
        let left_match = Arc::clone(&self.match_condition.left);
        let right_match = Arc::clone(&self.match_condition.right);
        let match_op = self.match_condition.op;
        let right_output_indices = self.right_output_indices.clone();
        let batch_size = context.session_config().batch_size();
        let stream = stream::once(async move {
            let mut right_fut = right_fut;
            let right_input = poll_fn(|cx| right_fut.get_shared(cx)).await?;
            let right_stream = right_input.stream()?;
            let state = AsOfJoinStreamState::new(
                Arc::clone(&stream_schema),
                InputCursor::new(left_stream, left_keys, left_match),
                InputCursor::new(right_stream, right_keys, right_match),
                match_op,
                right_output_indices,
                batch_size,
                metrics,
            );
            let stream = stream::try_unfold(
                (state, right_input),
                |(mut state, right_input)| async {
                    match state.next_batch().await? {
                        Some(batch) => Ok(Some((batch, (state, right_input)))),
                        None => Ok(None),
                    }
                },
            );
            Ok::<SendableRecordBatchStream, datafusion_common::DataFusionError>(Box::pin(
                RecordBatchStreamAdapter::new(stream_schema, stream),
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
        let mut column_statistics = left.column_statistics.clone();
        column_statistics.truncate(self.left.schema().fields().len());
        column_statistics.resize_with(
            self.left.schema().fields().len(),
            ColumnStatistics::new_unknown,
        );
        column_statistics.extend(
            self.right_output_indices
                .iter()
                .map(|_| ColumnStatistics::new_unknown()),
        );
        Ok(Arc::new(Statistics {
            num_rows: left.num_rows,
            total_byte_size: Precision::Absent,
            column_statistics,
        }))
    }
    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;

        let left = ctx.encode_child(&self.left)?;
        let right = ctx.encode_child(&self.right)?;
        let on = self
            .on
            .iter()
            .map(|(left, right)| {
                Ok(protobuf::JoinOn {
                    left: Some(ctx.encode_expr(left)?),
                    right: Some(ctx.encode_expr(right)?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let match_operator = match self.match_condition.op {
            Operator::Lt => protobuf::AsOfMatchOperator::Lt,
            Operator::LtEq => protobuf::AsOfMatchOperator::LtEq,
            Operator::Gt => protobuf::AsOfMatchOperator::Gt,
            Operator::GtEq => protobuf::AsOfMatchOperator::GtEq,
            op => {
                return internal_err!(
                    "AsOfJoinExec cannot serialize unsupported match operator {op}"
                );
            }
        };

        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::AsOfJoin(Box::new(
                    protobuf::AsOfJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        on,
                        left_match_expr: Some(
                            ctx.encode_expr(&self.match_condition.left)?,
                        ),
                        right_match_expr: Some(
                            ctx.encode_expr(&self.match_condition.right)?,
                        ),
                        match_operator: match_operator.into(),
                        right_output_indices: self
                            .right_output_indices
                            .iter()
                            .map(|index| *index as u32)
                            .collect(),
                    },
                )),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl AsOfJoinExec {
    /// Reconstruct an [`AsOfJoinExec`] from its protobuf representation.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_proto_models::protobuf;

        let asof_join = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::AsOfJoin,
            "AsOfJoinExec",
        );
        let left =
            ctx.decode_required_child(asof_join.left.as_deref(), "AsOfJoinExec", "left")?;
        let right = ctx.decode_required_child(
            asof_join.right.as_deref(),
            "AsOfJoinExec",
            "right",
        )?;
        let left_schema = left.schema();
        let right_schema = right.schema();
        let on = asof_join
            .on
            .iter()
            .map(|pair| {
                let left = ctx.decode_required_expr(
                    pair.left.as_ref(),
                    left_schema.as_ref(),
                    "AsOfJoinExec",
                    "on.left",
                )?;
                let right = ctx.decode_required_expr(
                    pair.right.as_ref(),
                    right_schema.as_ref(),
                    "AsOfJoinExec",
                    "on.right",
                )?;
                Ok((left, right))
            })
            .collect::<Result<_>>()?;
        let left_match = ctx.decode_required_expr(
            asof_join.left_match_expr.as_ref(),
            left_schema.as_ref(),
            "AsOfJoinExec",
            "left_match_expr",
        )?;
        let right_match = ctx.decode_required_expr(
            asof_join.right_match_expr.as_ref(),
            right_schema.as_ref(),
            "AsOfJoinExec",
            "right_match_expr",
        )?;
        let match_operator = protobuf::AsOfMatchOperator::try_from(
            asof_join.match_operator,
        )
        .map_err(|_| {
            datafusion_common::internal_datafusion_err!(
                "AsOfJoinExec: unknown AsOfMatchOperator {}",
                asof_join.match_operator
            )
        })?;
        let op = match match_operator {
            protobuf::AsOfMatchOperator::Lt => Operator::Lt,
            protobuf::AsOfMatchOperator::LtEq => Operator::LtEq,
            protobuf::AsOfMatchOperator::Gt => Operator::Gt,
            protobuf::AsOfMatchOperator::GtEq => Operator::GtEq,
            protobuf::AsOfMatchOperator::Unspecified => {
                return internal_err!("AsOfJoinExec match operator must be specified");
            }
        };

        Ok(Arc::new(Self::try_new(
            left,
            right,
            on,
            AsOfMatchExpr::new(left_match, op, right_match),
            asof_join
                .right_output_indices
                .iter()
                .map(|index| *index as usize)
                .collect(),
        )?))
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
                metrics.build_input_batches.add(1);
                metrics.build_input_rows.add(batch.num_rows());
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
    /// Evaluated match values for `batch`.
    match_array: Option<ArrayRef>,
    /// Monotonic identity of the current key arrays.
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
            key_batch_id: 0,
            row: 0,
            eof: false,
        }
    }

    async fn ensure_row(&mut self, elapsed_compute: &Time) -> Result<bool> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                return Ok(true);
            }
            self.batch = None;
            self.key_arrays = Arc::from([]);
            self.key_validity = None;
            self.match_array = None;
            self.row = 0;
            if self.eof {
                return Ok(false);
            }
            let Some(batch) = self.stream.next().await.transpose()? else {
                self.eof = true;
                return Ok(false);
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
            self.match_array = Some(
                self.match_expr
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?,
            );
            self.key_batch_id += 1;
            self.batch = Some(batch);
        }
    }

    fn group_has_null(&self) -> bool {
        self.key_validity
            .as_ref()
            .is_some_and(|validity| validity.is_null(self.row))
    }

    fn match_value(&self) -> Result<ScalarValue> {
        let array = self.match_array.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("ASOF match array is missing")
        })?;
        ScalarValue::try_from_array(array, self.row).map(normalize_float_zero_scalar)
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
    baseline: BaselineMetrics,
    matched_rows: Count,
    unmatched_left_rows: Count,
    build_input_batches: Count,
    build_input_rows: Count,
    build_mem_used: Gauge,
}

impl AsOfJoinMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            matched_rows: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter("matched_rows", partition),
            unmatched_left_rows: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter("unmatched_left_rows", partition),
            build_input_batches: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter("build_input_batches", partition),
            build_input_rows: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter("build_input_rows", partition),
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
    /// Distinct source batches referenced by `indices`.
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
struct AsOfJoinStreamState {
    /// Output schema used when pending row references are materialized.
    schema: SchemaRef,
    /// Cursor over the current left partition.
    left: InputCursor,
    /// Independent cursor over the shared, ordered right input.
    right: InputCursor,
    /// Validated ordered match operator.
    op: Operator,
    /// Right columns appended to each output row.
    right_output_indices: Vec<usize>,
    /// Nearest eligible right row for the current equality group.
    candidate: Option<Candidate>,
    /// Equality-key ordering shared by the comparator caches.
    group_sort_options: Vec<SortOptions>,
    /// Cached comparator for the current right and left input batches.
    input_group_comparator: Option<(usize, usize, JoinKeyComparator)>,
    /// Cached comparator for the candidate and current left batches.
    candidate_group_comparator: Option<(usize, usize, JoinKeyComparator)>,
    /// Left row references accumulated for the next output batch.
    pending_left: PendingRows,
    /// Matched right row references, aligned with `pending_left`.
    pending_right: PendingRows,
    /// Maximum number of pending rows before an output flush.
    batch_size: usize,
    metrics: AsOfJoinMetrics,
}

impl AsOfJoinStreamState {
    fn new(
        schema: SchemaRef,
        left: InputCursor,
        right: InputCursor,
        op: Operator,
        right_output_indices: Vec<usize>,
        batch_size: usize,
        metrics: AsOfJoinMetrics,
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
            op,
            right_output_indices,
            candidate: None,
            group_sort_options,
            input_group_comparator: None,
            candidate_group_comparator: None,
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

    /// Produces the next output batch without resetting the merge state.
    ///
    /// Each left row first validates its equality group, then advances the right
    /// cursor while right groups sort before it or right match values remain
    /// eligible. The last eligible right row becomes the candidate. Empty input
    /// batches are skipped. Right EOF preserves that candidate for later left
    /// rows in the same group; left EOF flushes the final pending rows. NULL keys
    /// and group changes clear the candidate, while output flushes only clear
    /// pending row references.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.pending_left.len() >= self.batch_size {
                return self.flush().map(Some);
            }
            if !self
                .left
                .ensure_row(self.metrics.baseline.elapsed_compute())
                .await?
            {
                if !self.pending_left.is_empty() {
                    return self.flush().map(Some);
                }
                self.metrics.baseline.done();
                return Ok(None);
            }

            let left_match = {
                let _timer = self.metrics.baseline.elapsed_compute().timer();
                self.left.match_value()?
            };
            if left_match.is_null() || self.left.group_has_null() {
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
                if !self
                    .right
                    .ensure_row(self.metrics.baseline.elapsed_compute())
                    .await?
                {
                    break;
                }
                let action = if self.right.group_has_null() {
                    RightAction::Advance
                } else {
                    match self.compare_input_groups()? {
                        Ordering::Less => RightAction::Advance,
                        Ordering::Greater => RightAction::Stop,
                        Ordering::Equal => {
                            let _timer = self.metrics.baseline.elapsed_compute().timer();
                            let right_match = self.right.match_value()?;
                            if right_match.is_null() {
                                RightAction::Advance
                            } else if is_eligible(self.op, &left_match, &right_match)? {
                                let (batch, row) = self.right.batch_row()?;
                                RightAction::Candidate(Candidate {
                                    batch,
                                    row,
                                    key_arrays: Arc::clone(&self.right.key_arrays),
                                    key_batch_id: self.right.key_batch_id,
                                })
                            } else {
                                RightAction::Stop
                            }
                        }
                    }
                };
                match action {
                    RightAction::Advance => self.right.advance(),
                    RightAction::Candidate(candidate) => {
                        // Replacing the candidate selects the nearest eligible row.
                        // Equal match values have no secondary ordering, so which
                        // tied row wins is intentionally nondeterministic.
                        self.candidate = Some(candidate);
                        self.right.advance();
                    }
                    RightAction::Stop => break,
                }
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
                if !self.right_output_indices.is_empty() {
                    self.pending_right.push(candidate.batch, candidate.row);
                }
                self.metrics.matched_rows.add(1);
            }
            None => {
                if !self.right_output_indices.is_empty() {
                    self.pending_right.push_null();
                }
                self.metrics.unmatched_left_rows.add(1);
            }
        }
        Ok(())
    }

    /// Materializes pending row references while preserving both cursors and the
    /// current equality-group candidate for the next output batch.
    fn flush(&mut self) -> Result<RecordBatch> {
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let left_len = self.schema.fields().len() - self.right_output_indices.len();
        let mut arrays = Vec::with_capacity(self.schema.fields().len());
        for index in 0..left_len {
            arrays.push(
                self.pending_left
                    .materialize_column(index, self.schema.field(index).data_type())?,
            );
        }
        for (offset, source_index) in self.right_output_indices.iter().enumerate() {
            arrays.push(self.pending_right.materialize_column(
                *source_index,
                self.schema.field(left_len + offset).data_type(),
            )?);
        }
        self.pending_left.clear();
        self.pending_right.clear();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
        (&batch).record_output(&self.metrics.baseline);
        Ok(batch)
    }
}

/// Validates all invariants required by the forward-only ASOF state machine.
fn validate_asof_join(
    left: &dyn ExecutionPlan,
    right: &dyn ExecutionPlan,
    on: &JoinOn,
    match_condition: &AsOfMatchExpr,
    right_output_indices: &[usize],
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
    }
    let left_match_type = match_condition.left.data_type(&left_schema)?;
    let right_match_type = match_condition.right.data_type(&right_schema)?;
    if left_match_type != right_match_type {
        return plan_err!(
            "AsOfJoinExec match expression types differ: {left_match_type} and {right_match_type}"
        );
    }
    if let Some(index) = right_output_indices
        .iter()
        .find(|index| **index >= right_schema.fields().len())
    {
        return plan_err!(
            "AsOfJoinExec right output index {index} is outside schema with {} fields",
            right_schema.fields().len()
        );
    }
    if !right_output_indices
        .windows(2)
        .all(|pair| pair[0] < pair[1])
    {
        return plan_err!(
            "AsOfJoinExec right output indices must be strictly increasing"
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

enum RightAction {
    Advance,
    Candidate(Candidate),
    Stop,
}

fn is_eligible(op: Operator, left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
    let ordering = right.try_cmp(left)?;
    Ok(match op {
        Operator::Gt => ordering == Ordering::Less,
        Operator::GtEq => ordering != Ordering::Greater,
        Operator::Lt => ordering == Ordering::Greater,
        Operator::LtEq => ordering != Ordering::Less,
        _ => unreachable!("ASOF match operator is validated by try_new"),
    })
}

#[cfg(test)]
mod tests {
    // These tests cover physical-only contracts that SQL logic tests cannot
    // observe, including batch-boundary state, shared build memory, Arrow type
    // preservation, and execution properties. End-to-end SQL semantics live in
    // the dependent SQL layer.

    use super::*;
    use crate::test::TestMemoryExec;
    use crate::{collect, collect_partitioned};
    use arrow::array::{
        DictionaryArray, Int32Array, Int64Array, StringArray, StringDictionaryBuilder,
    };
    use arrow::datatypes::{DataType, Field, Int8Type};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr_common::metrics::MetricValue;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

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
            vec![2],
        )?))
    }

    #[test]
    fn eligibility_matches_public_semantics() -> Result<()> {
        let left = ScalarValue::Int64(Some(10));
        let lower = ScalarValue::Int64(Some(9));
        let equal = ScalarValue::Int64(Some(10));
        let higher = ScalarValue::Int64(Some(11));
        assert!(is_eligible(Operator::Gt, &left, &lower)?);
        assert!(!is_eligible(Operator::Gt, &left, &equal)?);
        assert!(is_eligible(Operator::GtEq, &left, &equal)?);
        assert!(is_eligible(Operator::Lt, &left, &higher)?);
        assert!(!is_eligible(Operator::Lt, &left, &equal)?);
        assert!(is_eligible(Operator::LtEq, &left, &equal)?);
        Ok(())
    }

    #[tokio::test]
    async fn state_survives_empty_input_batches_and_output_flushes() -> Result<()> {
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
        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
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
        assert_eq!(
            ids,
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
            ]
        );
        assert_eq!(
            prices,
            vec![None, None, None, Some(40), Some(60), Some(101), None]
        );

        let metrics = exec.metrics().expect("ASOF metrics must be present");
        assert_eq!(metrics.output_rows(), Some(7));
        assert_eq!(
            metrics
                .sum_by_name("matched_rows")
                .map(|value| value.as_usize()),
            Some(3)
        );
        assert_eq!(
            metrics
                .sum_by_name("unmatched_left_rows")
                .map(|value| value.as_usize()),
            Some(4)
        );
        assert!(metrics.elapsed_compute().is_some());
        assert!(
            metrics.iter().any(|metric| {
                matches!(metric.value(), MetricValue::ElapsedCompute(_))
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn broadcasts_right_input_to_all_left_partitions() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let left = TestMemoryExec::try_new_exec(
            &[
                vec![make_batch(
                    &left_schema,
                    vec![Some("A"), Some("A")],
                    vec![Some(1), Some(4)],
                    vec![0, 1],
                )?],
                vec![make_batch(
                    &left_schema,
                    vec![Some("A"), Some("A")],
                    vec![Some(2), Some(5)],
                    vec![2, 3],
                )?],
            ],
            Arc::clone(&left_schema),
            None,
        )?;
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Int32, false),
        ]));
        let right = TestMemoryExec::try_new_exec(
            &[vec![
                make_batch(&right_schema, vec![Some("A")], vec![Some(1)], vec![10])?,
                make_batch(&right_schema, vec![Some("A")], vec![Some(3)], vec![30])?,
            ]],
            Arc::clone(&right_schema),
            None,
        )?;
        let exec = Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            vec![],
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                Operator::GtEq,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            vec![2],
        )?);
        assert_eq!(exec.properties().output_partitioning().partition_count(), 2);
        assert!(matches!(
            &exec.input_distribution_requirements().into_per_child()[..],
            [
                Distribution::UnspecifiedDistribution,
                Distribution::SinglePartition
            ]
        ));

        let partitions = collect_partitioned(
            Arc::clone(&exec) as Arc<dyn ExecutionPlan>,
            Arc::new(TaskContext::default()),
        )
        .await?;
        assert_eq!(partitions.len(), 2);
        for batches in partitions {
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
            assert_eq!(prices, vec![Some(10), Some(30)]);
        }

        let metrics = exec.metrics().expect("ASOF metrics must be present");
        assert_eq!(
            metrics
                .sum_by_name("build_input_batches")
                .map(|value| value.as_usize()),
            Some(2)
        );
        assert_eq!(
            metrics
                .sum_by_name("build_input_rows")
                .map(|value| value.as_usize()),
            Some(2)
        );
        assert_eq!(metrics.output_rows(), Some(4));
        Ok(())
    }

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
            vec![2],
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

    #[tokio::test]
    async fn preserves_dictionary_outputs_across_large_flush() -> Result<()> {
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("payload", dictionary_type.clone(), false),
        ]));
        let mut left_payload = StringDictionaryBuilder::<Int8Type>::new();
        for _ in 0..129 {
            left_payload.append_value("left");
        }
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(StringArray::from(vec!["A"; 129])),
                Arc::new(Int64Array::from_iter_values(-1..128)),
                Arc::new(left_payload.finish()),
            ],
        )?;
        let left = TestMemoryExec::try_new_exec(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?;

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("payload", dictionary_type.clone(), false),
        ]));
        let mut right_payload = StringDictionaryBuilder::<Int8Type>::new();
        right_payload.append_value("right");
        let right_batch = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(right_payload.finish()),
            ],
        )?;
        let right = TestMemoryExec::try_new_exec(
            &[vec![right_batch]],
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
            vec![2],
        )?);
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(256)),
        );
        let batches = collect(exec, context).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 129);
        assert_eq!(batches[0].column(2).data_type(), &dictionary_type);
        assert_eq!(batches[0].column(3).data_type(), &dictionary_type);

        let right_output = batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("right output must remain Dictionary(Int8, Utf8)");
        assert!(right_output.is_null(0));
        assert_eq!(right_output.null_count(), 1);
        let values = right_output
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dictionary values must be Utf8");
        for row in 1..129 {
            assert_eq!(
                values.value(right_output.keys().value(row) as usize),
                "right"
            );
        }
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
            vec![2],
        )
        .expect_err("volatile match expression must be rejected");
        assert!(match_error.to_string().contains("must be deterministic"));

        let equality_error = AsOfJoinExec::try_new(
            Arc::clone(&exec.left),
            Arc::clone(&exec.right),
            vec![(volatile, Arc::new(PhysicalColumn::new("key", 0)))],
            exec.match_condition.clone(),
            vec![2],
        )
        .expect_err("volatile equality expression must be rejected");
        assert!(equality_error.to_string().contains("must be deterministic"));
        Ok(())
    }

    #[test]
    fn properties_and_statistics_follow_left_preserving_contract() -> Result<()> {
        let exec = test_exec()?;
        let exec_plan: Arc<dyn ExecutionPlan> = Arc::clone(&exec) as _;
        assert_eq!(exec.maintains_input_order(), vec![false, false]);
        assert_eq!(exec_plan.pipeline_behavior(), EmissionType::Incremental);
        assert_eq!(exec_plan.boundedness(), Boundedness::Bounded);
        assert!(matches!(
            &exec.input_distribution_requirements().into_per_child()[..],
            [
                Distribution::UnspecifiedDistribution,
                Distribution::SinglePartition
            ]
        ));
        for ordering in exec.required_input_ordering() {
            let requirement = ordering.expect("ASOF ordering is required").into_single();
            assert_eq!(requirement.len(), 2);
            assert_eq!(
                requirement[0].options,
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                })
            );
            assert_eq!(
                requirement[1].options,
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                })
            );
        }

        let no_keys: Arc<dyn ExecutionPlan> = Arc::new(AsOfJoinExec::try_new(
            Arc::clone(&exec.left),
            Arc::clone(&exec.right),
            vec![],
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                Operator::Lt,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            vec![2],
        )?);
        assert_eq!(
            no_keys.output_partitioning().partition_count(),
            exec.left.output_partitioning().partition_count()
        );
        assert!(matches!(
            &no_keys.input_distribution_requirements().into_per_child()[..],
            [
                Distribution::UnspecifiedDistribution,
                Distribution::SinglePartition
            ]
        ));
        for ordering in no_keys.required_input_ordering() {
            let requirement = ordering.expect("ASOF ordering is required").into_single();
            assert_eq!(requirement.len(), 1);
            assert_eq!(
                requirement[0].options,
                Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                })
            );
        }

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
