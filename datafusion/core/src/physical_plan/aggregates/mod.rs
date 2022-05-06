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

//! Aggregates functionalities

use crate::execution::context::TaskContext;
use crate::physical_plan::aggregates::hash::GroupedHashAggregateStream;
use crate::physical_plan::aggregates::no_grouping::AggregateStream;
use crate::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use crate::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_expr::Accumulator;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{
    expressions, AggregateExpr, PhysicalExpr, PhysicalSortExpr,
};
use std::any::Any;
use std::sync::Arc;

mod hash;
mod no_grouping;
mod row_hash;

use crate::physical_plan::aggregates::row_hash::GroupedHashAggregateStreamV2;
pub use datafusion_expr::AggregateFunction;
use datafusion_physical_expr::aggregate::row_accumulator::RowAccumulator;
pub use datafusion_physical_expr::expressions::create_aggregate_expr;
use datafusion_row::{row_supported, RowType};

/// Hash aggregate modes
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AggregateMode {
    /// Partial aggregate that can be applied in parallel across input partitions
    Partial,
    /// Final aggregate that produces a single partition of output
    Final,
    /// Final aggregate that works on pre-partitioned data.
    ///
    /// This requires the invariant that all rows with a particular
    /// grouping key are in the same partitions, such as is the case
    /// with Hash repartitioning on the group keys. If a group key is
    /// duplicated, duplicate groups would be produced
    FinalPartitioned,
}

/// Hash aggregate execution plan
#[derive(Debug)]
pub struct AggregateExec {
    /// Aggregation mode (full, partial)
    mode: AggregateMode,
    /// Grouping expressions
    group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Aggregate expressions
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    /// Input plan, could be a partial aggregate or the input to the aggregate
    input: Arc<dyn ExecutionPlan>,
    /// Schema after the aggregate is applied
    schema: SchemaRef,
    /// Input schema before any aggregation is applied. For partial aggregate this will be the
    /// same as input.schema() but for the final aggregate it will be the same as the input
    /// to the partial aggregate
    input_schema: SchemaRef,
    /// Execution Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl AggregateExec {
    /// Create a new hash aggregate execution plan
    pub fn try_new(
        mode: AggregateMode,
        group_expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &group_expr, &aggr_expr, mode)?;

        let schema = Arc::new(schema);

        Ok(AggregateExec {
            mode,
            group_expr,
            aggr_expr,
            input,
            schema,
            input_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Aggregation mode (full, partial)
    pub fn mode(&self) -> &AggregateMode {
        &self.mode
    }

    /// Grouping expressions
    pub fn group_expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.group_expr
    }

    /// Grouping expressions as they occur in the output schema
    pub fn output_group_expr(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        // Update column indices. Since the group by columns come first in the output schema, their
        // indices are simply 0..self.group_expr(len).
        self.group_expr
            .iter()
            .enumerate()
            .map(|(index, (_col, name))| {
                Arc::new(expressions::Column::new(name, index)) as Arc<dyn PhysicalExpr>
            })
            .collect()
    }

    /// Aggregate expressions
    pub fn aggr_expr(&self) -> &[Arc<dyn AggregateExpr>] {
        &self.aggr_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn row_aggregate_supported(&self) -> bool {
        let group_schema = group_schema(&self.schema, self.group_expr.len());
        row_supported(&group_schema, RowType::Compact)
            && accumulator_v2_supported(&self.aggr_expr)
    }
}

impl ExecutionPlan for AggregateExec {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_child_distribution(&self) -> Distribution {
        match &self.mode {
            AggregateMode::Partial => Distribution::UnspecifiedDistribution,
            AggregateMode::FinalPartitioned => Distribution::HashPartitioned(
                self.group_expr.iter().map(|x| x.0.clone()).collect(),
            ),
            AggregateMode::Final => Distribution::SinglePartition,
        }
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AggregateExec::try_new(
            self.mode,
            self.group_expr.clone(),
            self.aggr_expr.clone(),
            children[0].clone(),
            self.input_schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let group_expr = self.group_expr.iter().map(|x| x.0.clone()).collect();

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        if self.group_expr.is_empty() {
            Ok(Box::pin(AggregateStream::new(
                self.mode,
                self.schema.clone(),
                self.aggr_expr.clone(),
                input,
                baseline_metrics,
            )?))
        } else if self.row_aggregate_supported() {
            Ok(Box::pin(GroupedHashAggregateStreamV2::new(
                self.mode,
                self.schema.clone(),
                group_expr,
                self.aggr_expr.clone(),
                input,
                baseline_metrics,
            )?))
        } else {
            Ok(Box::pin(GroupedHashAggregateStream::new(
                self.mode,
                self.schema.clone(),
                group_expr,
                self.aggr_expr.clone(),
                input,
                baseline_metrics,
            )?))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "AggregateExec: mode={:?}", self.mode)?;
                let g: Vec<String> = self
                    .group_expr
                    .iter()
                    .map(|(e, alias)| {
                        let e = e.to_string();
                        if &e != alias {
                            format!("{} as {}", e, alias)
                        } else {
                            e
                        }
                    })
                    .collect();
                write!(f, ", gby=[{}]", g.join(", "))?;

                let a: Vec<String> = self
                    .aggr_expr
                    .iter()
                    .map(|agg| agg.name().to_string())
                    .collect();
                write!(f, ", aggr=[{}]", a.join(", "))?;
            }
        }
        Ok(())
    }

    fn statistics(&self) -> Statistics {
        // TODO stats: group expressions:
        // - once expressions will be able to compute their own stats, use it here
        // - case where we group by on a column for which with have the `distinct` stat
        // TODO stats: aggr expression:
        // - aggregations somtimes also preserve invariants such as min, max...
        match self.mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned
                if self.group_expr.is_empty() =>
            {
                Statistics {
                    num_rows: Some(1),
                    is_exact: true,
                    ..Default::default()
                }
            }
            _ => Statistics::default(),
        }
    }
}

fn create_schema(
    input_schema: &Schema,
    group_expr: &[(Arc<dyn PhysicalExpr>, String)],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    mode: AggregateMode,
) -> datafusion_common::Result<Schema> {
    let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
    for (expr, name) in group_expr {
        fields.push(Field::new(
            name,
            expr.data_type(input_schema)?,
            expr.nullable(input_schema)?,
        ))
    }

    match mode {
        AggregateMode::Partial => {
            // in partial mode, the fields of the accumulator's state
            for expr in aggr_expr {
                fields.extend(expr.state_fields()?.iter().cloned())
            }
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            // in final mode, the field with the final result of the accumulator
            for expr in aggr_expr {
                fields.push(expr.field()?)
            }
        }
    }

    Ok(Schema::new(fields))
}

fn group_schema(schema: &Schema, group_count: usize) -> SchemaRef {
    let group_fields = schema.fields()[0..group_count].to_vec();
    Arc::new(Schema::new(group_fields))
}

/// returns physical expressions to evaluate against a batch
/// The expressions are different depending on `mode`:
/// * Partial: AggregateExpr::expressions
/// * Final: columns of `AggregateExpr::state_fields()`
fn aggregate_expressions(
    aggr_expr: &[Arc<dyn AggregateExpr>],
    mode: &AggregateMode,
    col_idx_base: usize,
) -> datafusion_common::Result<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    match mode {
        AggregateMode::Partial => {
            Ok(aggr_expr.iter().map(|agg| agg.expressions()).collect())
        }
        // in this mode, we build the merge expressions of the aggregation
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            let mut col_idx_base = col_idx_base;
            Ok(aggr_expr
                .iter()
                .map(|agg| {
                    let exprs = merge_expressions(col_idx_base, agg)?;
                    col_idx_base += exprs.len();
                    Ok(exprs)
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?)
        }
    }
}

/// uses `state_fields` to build a vec of physical column expressions required to merge the
/// AggregateExpr' accumulator's state.
///
/// `index_base` is the starting physical column index for the next expanded state field.
fn merge_expressions(
    index_base: usize,
    expr: &Arc<dyn AggregateExpr>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    Ok(expr
        .state_fields()?
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            Arc::new(Column::new(f.name(), index_base + idx)) as Arc<dyn PhysicalExpr>
        })
        .collect::<Vec<_>>())
}

pub(crate) type AccumulatorItem = Box<dyn Accumulator>;
pub(crate) type AccumulatorItemV2 = Box<dyn RowAccumulator>;

fn create_accumulators(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> datafusion_common::Result<Vec<AccumulatorItem>> {
    aggr_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<datafusion_common::Result<Vec<_>>>()
}

fn accumulator_v2_supported(aggr_expr: &[Arc<dyn AggregateExpr>]) -> bool {
    aggr_expr
        .iter()
        .all(|expr| expr.row_accumulator_supported())
}

fn create_accumulators_v2(
    aggr_expr: &[Arc<dyn AggregateExpr>],
) -> datafusion_common::Result<Vec<AccumulatorItemV2>> {
    let mut state_index = 0;
    aggr_expr
        .iter()
        .map(|expr| {
            let result = expr.create_row_accumulator(state_index);
            state_index += expr.state_fields().unwrap().len();
            result
        })
        .collect::<datafusion_common::Result<Vec<_>>>()
}

/// returns a vector of ArrayRefs, where each entry corresponds to either the
/// final value (mode = Final) or states (mode = Partial)
fn finalize_aggregation(
    accumulators: &[AccumulatorItem],
    mode: &AggregateMode,
) -> datafusion_common::Result<Vec<ArrayRef>> {
    match mode {
        AggregateMode::Partial => {
            // build the vector of states
            let a = accumulators
                .iter()
                .map(|accumulator| accumulator.state())
                .map(|value| {
                    value.map(|e| {
                        e.iter().map(|v| v.to_array()).collect::<Vec<ArrayRef>>()
                    })
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            Ok(a.iter().flatten().cloned().collect::<Vec<_>>())
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            // merge the state to the final value
            accumulators
                .iter()
                .map(|accumulator| accumulator.evaluate().map(|v| v.to_array()))
                .collect::<datafusion_common::Result<Vec<ArrayRef>>>()
        }
    }
}

/// Evaluates expressions against a record batch.
fn evaluate(
    expr: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    expr.iter()
        .map(|expr| expr.evaluate(batch))
        .map(|r| r.map(|v| v.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()
}

/// Evaluates expressions against a record batch.
fn evaluate_many(
    expr: &[Vec<Arc<dyn PhysicalExpr>>],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ArrayRef>>> {
    expr.iter()
        .map(|expr| evaluate(expr, batch))
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod tests {
    use crate::execution::context::TaskContext;
    use crate::from_slice::FromSlice;
    use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
    use crate::physical_plan::expressions::{col, Avg};
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::{assert_batches_sorted_eq, physical_plan::common};
    use arrow::array::{Float64Array, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::error::Result as ArrowResult;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_physical_expr::{AggregateExpr, PhysicalExpr, PhysicalSortExpr};
    use futures::{FutureExt, Stream};
    use std::any::Any;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::{
        ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
        Statistics,
    };
    use crate::prelude::SessionContext;

    /// some mock data to aggregates
    fn some_data() -> (Arc<Schema>, Vec<RecordBatch>) {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // define data.
        (
            schema.clone(),
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt32Array::from_slice(&[2, 3, 4, 4])),
                        Arc::new(Float64Array::from_slice(&[1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(UInt32Array::from_slice(&[2, 3, 3, 4])),
                        Arc::new(Float64Array::from_slice(&[1.0, 2.0, 3.0, 4.0])),
                    ],
                )
                .unwrap(),
            ],
        )
    }

    /// build the aggregates on the data from some_data() and check the results
    async fn check_aggregates(input: Arc<dyn ExecutionPlan>) -> Result<()> {
        let input_schema = input.schema();

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &input_schema)?, "a".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &input_schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            input,
            input_schema.clone(),
        )?);

        let result =
            common::collect(partial_aggregate.execute(0, task_ctx.clone())?).await?;

        let expected = vec![
            "+---+---------------+-------------+",
            "| a | AVG(b)[count] | AVG(b)[sum] |",
            "+---+---------------+-------------+",
            "| 2 | 2             | 2           |",
            "| 3 | 3             | 7           |",
            "| 4 | 3             | 11          |",
            "+---+---------------+-------------+",
        ];
        assert_batches_sorted_eq!(expected, &result);

        let merge = Arc::new(CoalescePartitionsExec::new(partial_aggregate));

        let final_group: Vec<Arc<dyn PhysicalExpr>> = (0..groups.len())
            .map(|i| col(&groups[i].1, &input_schema))
            .collect::<Result<_>>()?;

        let merged_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            final_group
                .iter()
                .enumerate()
                .map(|(i, expr)| (expr.clone(), groups[i].1.clone()))
                .collect(),
            aggregates,
            merge,
            input_schema,
        )?);

        let result =
            common::collect(merged_aggregate.execute(0, task_ctx.clone())?).await?;
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let expected = vec![
            "+---+--------------------+",
            "| a | AVG(b)             |",
            "+---+--------------------+",
            "| 2 | 1                  |",
            "| 3 | 2.3333333333333335 |", // 3, (2 + 3 + 2) / 3
            "| 4 | 3.6666666666666665 |", // 4, (3 + 4 + 4) / 3
            "+---+--------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &result);

        let metrics = merged_aggregate.metrics().unwrap();
        let output_rows = metrics.output_rows().unwrap();
        assert_eq!(3, output_rows);

        Ok(())
    }

    /// Define a test source that can yield back to runtime before returning its first item ///

    #[derive(Debug)]
    struct TestYieldingExec {
        /// True if this exec should yield back to runtime the first time it is polled
        pub yield_first: bool,
    }

    impl ExecutionPlan for TestYieldingExec {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            some_data().0
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let stream = if self.yield_first {
                TestYieldingStream::New
            } else {
                TestYieldingStream::Yielded
            };

            Ok(Box::pin(stream))
        }

        fn statistics(&self) -> Statistics {
            let (_, batches) = some_data();
            common::compute_record_batch_statistics(&[batches], &self.schema(), None)
        }
    }

    /// A stream using the demo data. If inited as new, it will first yield to runtime before returning records
    enum TestYieldingStream {
        New,
        Yielded,
        ReturnedBatch1,
        ReturnedBatch2,
    }

    impl Stream for TestYieldingStream {
        type Item = ArrowResult<RecordBatch>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            match &*self {
                TestYieldingStream::New => {
                    *(self.as_mut()) = TestYieldingStream::Yielded;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                TestYieldingStream::Yielded => {
                    *(self.as_mut()) = TestYieldingStream::ReturnedBatch1;
                    Poll::Ready(Some(Ok(some_data().1[0].clone())))
                }
                TestYieldingStream::ReturnedBatch1 => {
                    *(self.as_mut()) = TestYieldingStream::ReturnedBatch2;
                    Poll::Ready(Some(Ok(some_data().1[1].clone())))
                }
                TestYieldingStream::ReturnedBatch2 => Poll::Ready(None),
            }
        }
    }

    impl RecordBatchStream for TestYieldingStream {
        fn schema(&self) -> SchemaRef {
            some_data().0
        }
    }

    //// Tests ////

    #[tokio::test]
    async fn aggregate_source_not_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: false });

        check_aggregates(input).await
    }

    #[tokio::test]
    async fn aggregate_source_with_yielding() -> Result<()> {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestYieldingExec { yield_first: true });

        check_aggregates(input).await
    }

    #[tokio::test]
    async fn test_drop_cancel_without_groups() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let groups = vec![];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("a", &schema)?,
            "AVG(a)".to_string(),
            DataType::Float64,
        ))];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            blocking_exec,
            schema,
        )?);

        let fut = crate::physical_plan::collect(aggregate_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel_with_groups() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]));

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &schema)?, "a".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            aggregates.clone(),
            blocking_exec,
            schema,
        )?);

        let fut = crate::physical_plan::collect(aggregate_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
