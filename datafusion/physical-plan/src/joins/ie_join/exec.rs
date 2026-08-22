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

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::utils::memory::RecordBatchMemoryCounter;
use datafusion_common::{
    JoinType, NullEquality, Result, Statistics, internal_err, plan_err,
};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::Operator;
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::{Distribution, PhysicalExprRef};
use datafusion_physical_expr_common::physical_expr::{fmt_sql, is_volatile};
use futures::{FutureExt, StreamExt};

use crate::execution_plan::{EmissionType, boundedness_from_children};
use crate::joins::ie_join::algorithm::IEJoinData;
use crate::joins::ie_join::stream::{IEJoinMetrics, IEJoinStream};
use crate::joins::utils::{
    ColumnIndex, JoinFilter, build_join_schema, check_join_is_valid,
    estimate_join_statistics, symmetric_join_output_partitioning,
};
use crate::joins::{JoinOn, JoinOnRef};
use crate::spill::get_record_batch_memory_size;
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    InputDistributionRequirements, Partitioning, PlanProperties,
    SendableRecordBatchStream, check_if_same_properties,
};

/// One of the two range predicates that drive [`IEJoinExec`].
#[derive(Debug, Clone)]
pub struct IEJoinCondition {
    left: PhysicalExprRef,
    right: PhysicalExprRef,
    operator: Operator,
}

impl IEJoinCondition {
    /// Creates a normalized `left OP right` range condition.
    pub fn new(
        left: PhysicalExprRef,
        right: PhysicalExprRef,
        operator: Operator,
    ) -> Self {
        Self {
            left,
            right,
            operator,
        }
    }

    /// Expression evaluated against the left input.
    pub fn left(&self) -> &PhysicalExprRef {
        &self.left
    }

    /// Expression evaluated against the right input.
    pub fn right(&self) -> &PhysicalExprRef {
        &self.right
    }

    /// Range comparison operator.
    pub fn operator(&self) -> Operator {
        self.operator
    }
}

/// An inequality join using two sorted orders, a permutation, and a bitmap.
///
/// With equality keys, both inputs are co-partitioned by those keys. Inside
/// each input partition, rows are grouped by equality-key hash before the two
/// range predicates are evaluated. Hash collisions are checked with the full
/// equality expressions, so they cannot affect correctness.
///
/// Both input partitions are materialized. Candidate pairs are emitted in
/// bounded batches and the bitmap scan retains its cursor across polls, so the
/// output cardinality never determines intermediate memory usage.
#[derive(Debug)]
pub struct IEJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    conditions: [IEJoinCondition; 2],
    filter: Option<JoinFilter>,
    join_type: JoinType,
    null_equality: NullEquality,
    schema: SchemaRef,
    column_indices: Vec<ColumnIndex>,
    metrics: crate::metrics::ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl IEJoinExec {
    /// Creates an inner IEJoin from optional equality keys and exactly two
    /// normalized range conditions. Each condition's left and right physical
    /// expressions must have identical data types. SQL planning inserts any
    /// required casts; programmatic callers must insert them explicitly.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        conditions: [IEJoinCondition; 2],
        filter: Option<JoinFilter>,
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> Result<Self> {
        if join_type != JoinType::Inner {
            return plan_err!(
                "IEJoinExec currently supports Inner joins, got {join_type}"
            );
        }
        for condition in &conditions {
            if is_volatile(condition.left()) || is_volatile(condition.right()) {
                return plan_err!(
                    "IEJoinExec range conditions must not contain volatile expressions"
                );
            }
            if !matches!(
                condition.operator,
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) {
                return plan_err!(
                    "IEJoinExec requires <, <=, >, or >=, got {}",
                    condition.operator
                );
            }
            let left_type = condition.left.data_type(&left.schema())?;
            let right_type = condition.right.data_type(&right.schema())?;
            if left_type != right_type {
                return plan_err!(
                    "IEJoinExec condition types differ: {left_type} and {right_type}; physical expressions must be explicitly coerced to identical types"
                );
            }
        }

        check_join_is_valid(&left.schema(), &right.schema(), &on)?;
        let (schema, column_indices) =
            build_join_schema(&left.schema(), &right.schema(), &join_type);
        let schema = Arc::new(schema);
        let cache = Arc::new(Self::compute_properties(
            &left, &right, &schema, join_type, &on,
        )?);

        Ok(Self {
            left,
            right,
            on,
            conditions,
            filter,
            join_type,
            null_equality,
            schema,
            column_indices,
            metrics: Default::default(),
            cache,
        })
    }

    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    pub fn on(&self) -> JoinOnRef<'_> {
        &self.on
    }

    pub fn conditions(&self) -> &[IEJoinCondition; 2] {
        &self.conditions
    }

    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn null_equality(&self) -> NullEquality {
        self.null_equality
    }

    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        join_type: JoinType,
        on: JoinOnRef<'_>,
    ) -> Result<PlanProperties> {
        let eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            Arc::clone(schema),
            &[false, false],
            None,
            on,
        )?;
        let output_partitioning = if on.is_empty() {
            Partitioning::UnknownPartitioning(1)
        } else {
            symmetric_join_output_partitioning(left, right, &join_type)?
        };
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Final,
            boundedness_from_children([left, right]),
        ))
    }
}

impl ExecutionPlan for IEJoinExec {
    fn name(&self) -> &'static str {
        "IEJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        if self.on.is_empty() {
            InputDistributionRequirements::new(vec![
                Distribution::SinglePartition,
                Distribution::SinglePartition,
            ])
        } else {
            let (left, right) = self
                .on
                .iter()
                .map(|(left, right)| (Arc::clone(left), Arc::clone(right)))
                .unzip();
            InputDistributionRequirements::co_partitioned(vec![
                Distribution::KeyPartitioned(left),
                Distribution::KeyPartitioned(right),
            ])
            .allow_range_satisfaction_for_key_partitioning()
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        let [left, right]: [Arc<dyn ExecutionPlan>; 2] =
            children.try_into().map_err(|children: Vec<_>| {
                datafusion_common::internal_datafusion_err!(
                    "IEJoinExec expected 2 children, got {}",
                    children.len()
                )
            })?;
        Ok(Arc::new(Self::try_new(
            left,
            right,
            self.on.clone(),
            self.conditions.clone(),
            self.filter.clone(),
            self.join_type,
            self.null_equality,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if self.on.is_empty() && partition != 0 {
            return internal_err!(
                "Unkeyed IEJoinExec has one output partition, got {partition}"
            );
        }

        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_stream = self.right.execute(partition, Arc::clone(&context))?;
        let conditions = self.conditions.clone();
        let on = self.on.clone();
        let null_equality = self.null_equality;
        let metrics = IEJoinMetrics::new(partition, &self.metrics);
        let reservation = MemoryConsumer::new(format!("IEJoinExec[{partition}]"))
            .register(context.memory_pool());
        let load_metrics = metrics.clone();
        let load = async move {
            let timer = load_metrics.load_time.timer();
            let (left_batch, reservation) = collect_input(
                left_stream,
                reservation,
                &load_metrics.left_input_batches,
                &load_metrics.left_input_rows,
                &load_metrics.peak_mem_used,
            )
            .await?;
            let (right_batch, reservation) = collect_input(
                right_stream,
                reservation,
                &load_metrics.right_input_batches,
                &load_metrics.right_input_rows,
                &load_metrics.peak_mem_used,
            )
            .await?;
            let data = IEJoinData::try_new(
                left_batch,
                right_batch,
                &conditions,
                &on,
                null_equality,
                reservation,
                &load_metrics.peak_mem_used,
            )?;
            timer.done();
            Ok(data)
        }
        .boxed();

        Ok(Box::pin(IEJoinStream::new(
            Arc::clone(&self.schema),
            self.column_indices.clone(),
            self.conditions.clone(),
            self.on.clone(),
            self.filter.clone(),
            self.join_type,
            self.null_equality,
            context.session_config().batch_size(),
            load,
            metrics,
        )))
    }

    fn metrics(&self) -> Option<crate::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        if self.on.is_empty() {
            vec![ChildStats::At(None), ChildStats::At(None)]
        } else {
            vec![ChildStats::At(partition), ChildStats::At(partition)]
        }
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(estimate_join_statistics(
            input_stats[0].as_ref().clone(),
            input_stats[1].as_ref().clone(),
            &self.on,
            self.null_equality,
            &self.join_type,
            &self.schema,
        )?))
    }
}

impl DisplayAs for IEJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let conditions = self
            .conditions
            .iter()
            .map(|condition| {
                format!(
                    "({} {} {})",
                    fmt_sql(condition.left.as_ref()),
                    condition.operator,
                    fmt_sql(condition.right.as_ref())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let on = self
            .on
            .iter()
            .map(|(left, right)| {
                format!("({} = {})", fmt_sql(left.as_ref()), fmt_sql(right.as_ref()))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let filter = self
            .filter
            .as_ref()
            .map(|filter| format!(", filter={filter}"))
            .unwrap_or_default();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "IEJoinExec: join_type={:?}, on=[{}], conditions=[{}]{}",
                self.join_type, on, conditions, filter
            ),
            DisplayFormatType::TreeRender => {
                if self.join_type != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)?;
                }
                if !on.is_empty() {
                    writeln!(f, "on={on}")?;
                }
                writeln!(f, "conditions={conditions}")?;
                if let Some(filter) = &self.filter {
                    writeln!(f, "filter={filter}")?;
                }
                Ok(())
            }
        }
    }
}

async fn collect_input(
    mut stream: SendableRecordBatchStream,
    reservation: MemoryReservation,
    input_batches: &crate::metrics::Count,
    input_rows: &crate::metrics::Count,
    peak_mem_used: &crate::metrics::Gauge,
) -> Result<(RecordBatch, MemoryReservation)> {
    let schema = stream.schema();
    let initial_reservation = reservation.size();
    let mut batches = Vec::new();
    let mut input_memory = RecordBatchMemoryCounter::new();
    while let Some(batch) = stream.next().await.transpose()? {
        // Count shared buffers only once across all buffered batches.
        let size = input_memory.count_batch(&batch);
        reservation.try_grow(size)?;
        peak_mem_used.set_max(reservation.size());
        input_batches.add(1);
        input_rows.add(batch.num_rows());
        batches.push(batch);
    }

    if batches.len() == 1 {
        return Ok((batches.pop().expect("length checked above"), reservation));
    }

    if batches.is_empty() {
        return Ok((RecordBatch::new_empty(schema), reservation));
    }

    // Account for the temporary coexistence of input and concatenated buffers.
    // `get_record_batch_memory_size` intentionally deduplicates shared buffers,
    // but concatenating dictionary columns may materialize one copy per column.
    // Summing Arrow's per-batch array sizes is therefore the conservative bound.
    let concat_upper_bound = concat_memory_upper_bound(&batches)?;
    reservation.try_grow(concat_upper_bound)?;
    peak_mem_used.set_max(reservation.size());
    let batch = concat_batches(&schema, batches.iter())?;
    drop(batches);
    let batch_size = get_record_batch_memory_size(&batch);
    let retained_size = initial_reservation.checked_add(batch_size).ok_or_else(|| {
        datafusion_common::internal_datafusion_err!(
            "IEJoin retained memory size overflow"
        )
    })?;
    reservation.try_resize(retained_size)?;
    peak_mem_used.set_max(reservation.size());
    Ok((batch, reservation))
}

/// Returns a conservative upper bound for buffers allocated by
/// `concat_batches`. Unlike `get_record_batch_memory_size`, this deliberately
/// counts shared buffers once per array because concatenating dictionary
/// columns can materialize an independent dictionary for each output column.
pub(super) fn concat_memory_upper_bound(batches: &[RecordBatch]) -> Result<usize> {
    batches
        .iter()
        .flat_map(RecordBatch::columns)
        .try_fold(0_usize, |size, array| {
            size.checked_add(array.get_array_memory_size())
        })
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "IEJoin concatenated input memory size overflow"
            )
        })
}
