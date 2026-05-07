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

//! Eager right-side variant of group join.
//!
//! This execution plan aggregates the right side by join key first, evaluates
//! each aggregate once into arrays, and then probes left-side batches using
//! read-only indexed lookups.

use std::fmt::{self, Debug};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, BooleanArray, Int64Array, UInt32Array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{JoinType, Result, Statistics, internal_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::{
    EquivalenceProperties, GroupsAccumulatorAdapter, PhysicalExpr, PhysicalExprRef,
};
use futures::{Stream, StreamExt, ready};
use log::debug;

use crate::aggregates::group_values::{GroupValues, new_group_values};
use crate::aggregates::order::GroupOrdering;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties};

/// A fused join + group-by operator selected when eager right-side aggregation
/// is expected to reduce probe-side work before producing group join output.
#[derive(Debug)]
pub struct EagerRightGroupJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_type: JoinType,
    group_by_exprs: Vec<(PhysicalExprRef, String)>,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    aggr_input_schema: SchemaRef,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl EagerRightGroupJoinExec {
    /// Create a new `EagerRightGroupJoinExec`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        group_by_exprs: Vec<(PhysicalExprRef, String)>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Result<Self> {
        let aggr_input_schema = right.schema();
        Self::try_new_with_aggr_input_schema(
            left,
            right,
            on,
            join_type,
            group_by_exprs,
            aggr_expr,
            aggr_input_schema,
        )
    }

    /// Create a new `EagerRightGroupJoinExec` with the schema used to build
    /// aggregate expressions.
    pub fn try_new_with_aggr_input_schema(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        group_by_exprs: Vec<(PhysicalExprRef, String)>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        aggr_input_schema: SchemaRef,
    ) -> Result<Self> {
        if !matches!(join_type, JoinType::Inner | JoinType::Left) {
            return internal_err!(
                "EagerRightGroupJoinExec only supports Inner/Left joins, got {:?}",
                join_type
            );
        }

        let left_schema = left.schema();
        let mut fields: Vec<Arc<Field>> = Vec::new();

        for (expr, alias) in &group_by_exprs {
            fields.push(Arc::new(Field::new(
                alias,
                expr.data_type(&left_schema)?,
                expr.nullable(&left_schema)?,
            )));
        }

        for agg in &aggr_expr {
            fields.push(Arc::clone(&agg.field()));
        }

        let schema = Arc::new(Schema::new(fields));
        let props = left.properties();
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            props.partitioning.clone(),
            props.emission_type,
            props.boundedness,
        ));

        Ok(Self {
            left,
            right,
            on,
            join_type,
            group_by_exprs,
            aggr_expr,
            aggr_input_schema,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// Build side input.
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// Probe side input.
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Equi-join key expressions.
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    /// Join type.
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// GROUP BY expressions with output aliases.
    pub fn group_by_exprs(&self) -> &[(PhysicalExprRef, String)] {
        &self.group_by_exprs
    }

    /// Aggregate expressions.
    pub fn aggr_expr(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggr_expr
    }

    /// Input schema used to build aggregate expressions.
    pub fn aggr_input_schema(&self) -> &SchemaRef {
        &self.aggr_input_schema
    }
}

impl DisplayAs for EagerRightGroupJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on: Vec<String> =
                    self.on.iter().map(|(l, r)| format!("({l}, {r})")).collect();
                let aggrs: Vec<String> = self
                    .aggr_expr
                    .iter()
                    .map(|a| a.name().to_string())
                    .collect();
                write!(
                    f,
                    "EagerRightGroupJoinExec: join_type={:?}, on=[{}], aggr=[{}]",
                    self.join_type,
                    on.join(", "),
                    aggrs.join(", "),
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "EagerRightGroupJoinExec")
            }
        }
    }
}

impl ExecutionPlan for EagerRightGroupJoinExec {
    fn name(&self) -> &'static str {
        "EagerRightGroupJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            EagerRightGroupJoinExec::try_new_with_aggr_input_schema(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                self.on.clone(),
                self.join_type,
                self.group_by_exprs.clone(),
                self.aggr_expr.clone(),
                Arc::clone(&self.aggr_input_schema),
            )?,
        ))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let left_exprs: Vec<PhysicalExprRef> =
            self.on.iter().map(|(l, _)| Arc::clone(l)).collect();
        let right_exprs: Vec<PhysicalExprRef> =
            self.on.iter().map(|(_, r)| Arc::clone(r)).collect();
        vec![
            Distribution::HashPartitioned(left_exprs),
            Distribution::HashPartitioned(right_exprs),
        ]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for (left_expr, right_expr) in &self.on {
            let r = f(left_expr.as_ref())?;
            if r == TreeNodeRecursion::Stop {
                return Ok(r);
            }
            let r = f(right_expr.as_ref())?;
            if r == TreeNodeRecursion::Stop {
                return Ok(r);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_stream = self.right.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let right_schema = self.right.schema();
        let right_on: Vec<PhysicalExprRef> =
            self.on.iter().map(|(_, r)| Arc::clone(r)).collect();
        let key_fields: Vec<Arc<Field>> = right_on
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                Ok(Arc::new(Field::new(
                    format!("key_{idx}"),
                    expr.data_type(&right_schema)?,
                    expr.nullable(&right_schema)?,
                )))
            })
            .collect::<Result<_>>()?;
        let key_schema = Arc::new(Schema::new(key_fields));

        let accumulators: Vec<Box<dyn GroupsAccumulator>> = self
            .aggr_expr
            .iter()
            .map(|agg_expr| -> Result<Box<dyn GroupsAccumulator>> {
                if agg_expr.groups_accumulator_supported() {
                    agg_expr.create_groups_accumulator()
                } else {
                    debug!(
                        "EagerRightGroupJoinExec: using GroupsAccumulatorAdapter for {}",
                        agg_expr.name()
                    );
                    let captured = Arc::clone(agg_expr);
                    let factory = move || captured.create_accumulator();
                    Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
                }
            })
            .collect::<Result<_>>()?;

        let group_values = new_group_values(key_schema, &GroupOrdering::None)?;
        let left_on: Vec<PhysicalExprRef> =
            self.on.iter().map(|(l, _)| Arc::clone(l)).collect();
        let group_by_exprs: Vec<PhysicalExprRef> = self
            .group_by_exprs
            .iter()
            .map(|(expr, _)| Arc::clone(expr))
            .collect();

        Ok(Box::pin(EagerRightStream {
            state: EagerRightState::AggregateRight,
            right_stream,
            left_stream: Some(left_stream),
            right_on,
            left_on,
            group_by_exprs,
            aggr_expr: self.aggr_expr.clone(),
            accumulators,
            agg_arrays: Vec::new(),
            group_values,
            group_indices: Vec::new(),
            num_eager_groups: 0,
            schema: Arc::clone(&self.schema),
            aggr_input_schema: Arc::clone(&self.aggr_input_schema),
            join_type: self.join_type,
            baseline_metrics,
        }))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.left.partition_statistics(partition)
    }
}

enum EagerRightState {
    AggregateRight,
    ProbeLeft,
    Done,
}

struct EagerRightStream {
    state: EagerRightState,
    right_stream: SendableRecordBatchStream,
    left_stream: Option<SendableRecordBatchStream>,
    right_on: Vec<PhysicalExprRef>,
    left_on: Vec<PhysicalExprRef>,
    group_by_exprs: Vec<PhysicalExprRef>,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    agg_arrays: Vec<ArrayRef>,
    group_values: Box<dyn GroupValues>,
    group_indices: Vec<usize>,
    num_eager_groups: usize,
    schema: SchemaRef,
    aggr_input_schema: SchemaRef,
    join_type: JoinType,
    baseline_metrics: BaselineMetrics,
}

impl Debug for EagerRightStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EagerRightStream").finish_non_exhaustive()
    }
}

impl EagerRightStream {
    fn aggregate_right(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match ready!(self.right_stream.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    let key_arrays: Vec<ArrayRef> = self
                        .right_on
                        .iter()
                        .map(|expr| {
                            expr.evaluate(&batch)
                                .and_then(|v| v.into_array(batch.num_rows()))
                        })
                        .collect::<Result<_>>()?;

                    self.group_indices.clear();
                    self.group_values
                        .intern(&key_arrays, &mut self.group_indices)?;
                    let total_groups = self.group_values.len();
                    let aggr_batch = aggr_input_batch(&batch, &self.aggr_input_schema)?;

                    for (acc, agg) in self.accumulators.iter_mut().zip(&self.aggr_expr) {
                        let values: Vec<ArrayRef> = agg
                            .expressions()
                            .iter()
                            .map(|expr| {
                                expr.evaluate(&aggr_batch)
                                    .and_then(|v| v.into_array(aggr_batch.num_rows()))
                            })
                            .collect::<Result<_>>()?;
                        acc.update_batch(
                            &values,
                            &self.group_indices,
                            None,
                            total_groups,
                        )?;
                    }
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                None => return Poll::Ready(Ok(())),
            }
        }
    }

    fn probe_left_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        let num_rows = batch.num_rows();
        let key_arrays: Vec<ArrayRef> = self
            .left_on
            .iter()
            .map(|expr| expr.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;

        self.group_indices.clear();
        self.group_values
            .intern(&key_arrays, &mut self.group_indices)?;

        let mut output_columns: Vec<ArrayRef> = self
            .group_by_exprs
            .iter()
            .map(|expr| expr.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;

        let indices = UInt32Array::from(
            self.group_indices
                .iter()
                .map(|&idx| {
                    if idx < self.num_eager_groups {
                        Some(idx as u32)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
        );

        for agg_arr in &self.agg_arrays {
            output_columns.push(arrow::compute::take(agg_arr.as_ref(), &indices, None)?);
        }

        if self.join_type == JoinType::Left {
            fill_null_counts_with_zero(
                &mut output_columns,
                self.group_by_exprs.len(),
                &self.aggr_expr,
            )?;
        }

        if self.join_type == JoinType::Inner {
            let filter = BooleanArray::from(
                self.group_indices
                    .iter()
                    .map(|&idx| idx < self.num_eager_groups)
                    .collect::<Vec<_>>(),
            );
            let filtered_columns = output_columns
                .iter()
                .map(|column| arrow::compute::filter(column.as_ref(), &filter))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(RecordBatch::try_new(
                Arc::clone(&self.schema),
                filtered_columns,
            )?)
        } else {
            Ok(RecordBatch::try_new(
                Arc::clone(&self.schema),
                output_columns,
            )?)
        }
    }
}

fn aggr_input_batch(
    batch: &RecordBatch,
    aggr_input_schema: &SchemaRef,
) -> Result<RecordBatch> {
    if batch.schema().as_ref() == aggr_input_schema.as_ref() {
        return Ok(batch.clone());
    }

    let batch_schema = batch.schema();
    let columns = aggr_input_schema
        .fields()
        .iter()
        .map(|field| {
            batch_schema.index_of(field.name()).map_or_else(
                |_| new_null_array(field.data_type(), batch.num_rows()),
                |idx| Arc::clone(batch.column(idx)),
            )
        })
        .collect();

    let nullable_fields = aggr_input_schema
        .fields()
        .iter()
        .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
        .collect::<Vec<_>>();
    let nullable_schema = Arc::new(Schema::new_with_metadata(
        nullable_fields,
        aggr_input_schema.metadata().clone(),
    ));

    Ok(RecordBatch::try_new(nullable_schema, columns)?)
}

fn fill_null_counts_with_zero(
    output_columns: &mut [ArrayRef],
    group_expr_count: usize,
    aggr_expr: &[Arc<AggregateFunctionExpr>],
) -> Result<()> {
    for (idx, agg) in aggr_expr.iter().enumerate() {
        if agg.fun().name() != "count" {
            continue;
        }

        let col_idx = group_expr_count + idx;
        match output_columns[col_idx].data_type() {
            DataType::Int64 => {
                let arr = output_columns[col_idx]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        datafusion_common::internal_datafusion_err!(
                            "COUNT output was not an Int64Array"
                        )
                    })?;
                let filled: Int64Array =
                    arr.iter().map(|value| Some(value.unwrap_or(0))).collect();
                output_columns[col_idx] = Arc::new(filled);
            }
            data_type => {
                return internal_err!(
                    "COUNT output type in EagerRightGroupJoinExec must be Int64, got {:?}",
                    data_type
                );
            }
        }
    }

    Ok(())
}

impl Stream for EagerRightStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                EagerRightState::AggregateRight => {
                    match ready!(self.aggregate_right(cx)) {
                        Ok(()) => {
                            self.num_eager_groups = self.group_values.len();
                            self.agg_arrays = match self
                                .accumulators
                                .iter_mut()
                                .map(|acc| acc.evaluate(EmitTo::All))
                                .collect::<Result<_>>()
                            {
                                Ok(agg_arrays) => agg_arrays,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };
                            self.state = EagerRightState::ProbeLeft;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                EagerRightState::ProbeLeft => {
                    let stream = self.left_stream.as_mut().unwrap();
                    match ready!(stream.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            let result = self.probe_left_batch(&batch);
                            if let Ok(ref batch) = result {
                                self.baseline_metrics.record_output(batch.num_rows());
                            }
                            return Poll::Ready(Some(result));
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            self.left_stream = None;
                            self.state = EagerRightState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                EagerRightState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for EagerRightStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, Int64Array};
    use datafusion_common::assert_batches_eq;
    use datafusion_execution::TaskContext;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::{Column, col};

    use crate::collect;
    use crate::test::TestMemoryExec;

    fn input_exec(
        key_name: &str,
        value_name: &str,
        keys: Vec<i32>,
        values: Vec<i64>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(key_name, DataType::Int32, false),
            Field::new(value_name, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();

        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn eager_join(join_type: JoinType) -> Result<Arc<dyn ExecutionPlan>> {
        let left = input_exec("l_key", "l_value", vec![1, 2, 3], vec![10, 20, 30]);
        let right = input_exec("r_key", "r_value", vec![1, 1, 2], vec![5, 7, 11]);
        let left_schema = left.schema();
        let right_schema = right.schema();
        let input_schema = Arc::new(Schema::new(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields().iter())
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let on = vec![(
            Arc::new(Column::new("l_key", left_schema.index_of("l_key")?)) as _,
            Arc::new(Column::new("r_key", right_schema.index_of("r_key")?)) as _,
        )];
        let group_by_exprs = vec![(
            Arc::new(Column::new("l_key", left_schema.index_of("l_key")?)) as _,
            "l_key".to_string(),
        )];
        let aggr_expr = vec![
            Arc::new(
                AggregateExprBuilder::new(
                    count_udaf(),
                    vec![col("r_value", &input_schema)?],
                )
                .schema(Arc::clone(&input_schema))
                .alias("count_values")
                .build()?,
            ),
            Arc::new(
                AggregateExprBuilder::new(
                    sum_udaf(),
                    vec![col("r_value", &input_schema)?],
                )
                .schema(Arc::clone(&input_schema))
                .alias("sum_values")
                .build()?,
            ),
        ];

        Ok(Arc::new(
            EagerRightGroupJoinExec::try_new_with_aggr_input_schema(
                left,
                right,
                on,
                join_type,
                group_by_exprs,
                aggr_expr,
                input_schema,
            )?,
        ))
    }

    #[tokio::test]
    async fn eager_right_left_join_emits_unmatched_count_zero() -> Result<()> {
        let batches = collect(
            eager_join(JoinType::Left)?,
            Arc::new(TaskContext::default()),
        )
        .await?;
        let expected = [
            "+-------+--------------+------------+",
            "| l_key | count_values | sum_values |",
            "+-------+--------------+------------+",
            "| 1     | 2            | 12         |",
            "| 2     | 1            | 11         |",
            "| 3     | 0            |            |",
            "+-------+--------------+------------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn eager_right_inner_join_filters_unmatched_left_rows() -> Result<()> {
        let batches = collect(
            eager_join(JoinType::Inner)?,
            Arc::new(TaskContext::default()),
        )
        .await?;
        let expected = [
            "+-------+--------------+------------+",
            "| l_key | count_values | sum_values |",
            "+-------+--------------+------------+",
            "| 1     | 2            | 12         |",
            "| 2     | 1            | 11         |",
            "+-------+--------------+------------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }
}
