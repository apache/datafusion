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

//! FilterExec evaluates a boolean predicate against all input batches to determine which rows to
//! include in its output batches.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::expressions::PhysicalSortExpr;
use super::{ColumnStatistics, RecordBatchStream, SendableRecordBatchStream, Statistics};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    Column, DisplayFormatType, EquivalenceProperties, ExecutionPlan, Partitioning,
    PhysicalExpr,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::{split_conjunction, AnalysisContext};

use log::debug;

use crate::execution::context::TaskContext;
use futures::stream::{Stream, StreamExt};

/// FilterExec evaluates a boolean predicate against all input batches to determine which rows to
/// include in its output batches.
#[derive(Debug)]
pub struct FilterExec {
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl FilterExec {
    /// Create a FilterExec on an input
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => Ok(Self {
                predicate,
                input: input.clone(),
                metrics: ExecutionPlanMetricsSet::new(),
            }),
            other => Err(DataFusionError::Plan(format!(
                "Filter predicate must return boolean values, not {other:?}"
            ))),
        }
    }

    /// The expression to filter on. This expression must evaluate to a boolean value.
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl ExecutionPlan for FilterExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        // The filter operator does not make any changes to the schema of its input
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but it its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> bool {
        // tell optimizer this operator doesn't reorder its input
        true
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        // Combine the equal predicates with the input equivalence properties
        let mut input_properties = self.input.equivalence_properties();
        let (equal_pairs, _ne_pairs) = collect_columns_from_predicate(&self.predicate);
        for new_condition in equal_pairs {
            input_properties.add_equal_conditions(new_condition)
        }
        input_properties
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FilterExec::try_new(
            self.predicate.clone(),
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!("Start FilterExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(FilterExecStream {
            schema: self.input.schema(),
            predicate: self.predicate.clone(),
            input: self.input.execute(partition, context)?,
            baseline_metrics,
        }))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "FilterExec: {}", self.predicate)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The output statistics of a filtering operation can be estimated if the
    /// predicate's selectivity value can be determined for the incoming data.
    fn statistics(&self) -> Statistics {
        let input_stats = self.input.statistics();
        let starter_ctx =
            AnalysisContext::from_statistics(self.input.schema().as_ref(), &input_stats);

        let analysis_ctx = self.predicate.analyze(starter_ctx);

        match analysis_ctx.boundaries {
            Some(boundaries) => {
                // Build back the column level statistics from the boundaries inside the
                // analysis context. It is possible that these are going to be different
                // than the input statistics, especially when a comparison is made inside
                // the predicate expression (e.g. `col1 > 100`).
                let column_statistics = analysis_ctx
                    .column_boundaries
                    .iter()
                    .map(|boundary| match boundary {
                        Some(boundary) => ColumnStatistics {
                            min_value: Some(boundary.min_value.clone()),
                            max_value: Some(boundary.max_value.clone()),
                            ..Default::default()
                        },
                        None => ColumnStatistics::default(),
                    })
                    .collect();

                Statistics {
                    num_rows: input_stats.num_rows.zip(boundaries.selectivity).map(
                        |(num_rows, selectivity)| {
                            (num_rows as f64 * selectivity).ceil() as usize
                        },
                    ),
                    total_byte_size: input_stats
                        .total_byte_size
                        .zip(boundaries.selectivity)
                        .map(|(num_rows, selectivity)| {
                            (num_rows as f64 * selectivity).ceil() as usize
                        }),
                    column_statistics: Some(column_statistics),
                    ..Default::default()
                }
            }
            None => Statistics::default(),
        }
    }
}

/// The FilterExec streams wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct FilterExecStream {
    /// Output schema, which is the same as the input schema for this operator
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// runtime metrics recording
    baseline_metrics: BaselineMetrics,
}

fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> ArrowResult<RecordBatch> {
    predicate
        .evaluate(batch)
        .map(|v| v.into_array(batch.num_rows()))
        .map_err(DataFusionError::into)
        .and_then(|array| {
            Ok(as_boolean_array(&array)?)
                // apply filter array to record batch
                .and_then(|filter_array| filter_record_batch(batch, filter_array))
        })
}

impl Stream for FilterExecStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(value) => match value {
                    Some(Ok(batch)) => {
                        let timer = self.baseline_metrics.elapsed_compute().timer();
                        let filtered_batch = batch_filter(&batch, &self.predicate)?;
                        // skip entirely filtered batches
                        if filtered_batch.num_rows() == 0 {
                            continue;
                        }
                        timer.done();
                        poll = Poll::Ready(Some(Ok(filtered_batch)));
                        break;
                    }
                    _ => {
                        poll = Poll::Ready(value);
                        break;
                    }
                },
                Poll::Pending => {
                    poll = Poll::Pending;
                    break;
                }
            }
        }
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for FilterExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Return the equals Column-Pairs and Non-equals Column-Pairs
fn collect_columns_from_predicate(predicate: &Arc<dyn PhysicalExpr>) -> EqualAndNonEqual {
    let mut eq_predicate_columns: Vec<(&Column, &Column)> = Vec::new();
    let mut ne_predicate_columns: Vec<(&Column, &Column)> = Vec::new();

    let predicates = split_conjunction(predicate);
    predicates.into_iter().for_each(|p| {
        if let Some(binary) = p.as_any().downcast_ref::<BinaryExpr>() {
            let left = binary.left();
            let right = binary.right();
            if left.as_any().is::<Column>() && right.as_any().is::<Column>() {
                let left_column = left.as_any().downcast_ref::<Column>().unwrap();
                let right_column = right.as_any().downcast_ref::<Column>().unwrap();
                match binary.op() {
                    Operator::Eq => {
                        eq_predicate_columns.push((left_column, right_column))
                    }
                    Operator::NotEq => {
                        ne_predicate_columns.push((left_column, right_column))
                    }
                    _ => {}
                }
            }
        }
    });

    (eq_predicate_columns, ne_predicate_columns)
}
/// The equals Column-Pairs and Non-equals Column-Pairs in the Predicates
pub type EqualAndNonEqual<'a> =
    (Vec<(&'a Column, &'a Column)>, Vec<(&'a Column, &'a Column)>);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::physical_plan::expressions::*;
    use crate::physical_plan::ExecutionPlan;
    use crate::physical_plan::{collect, with_new_children_if_necessary};
    use crate::prelude::SessionContext;
    use crate::test;
    use crate::test::exec::StatisticsExec;
    use crate::test_util;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ColumnStatistics;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use std::iter::Iterator;
    use std::sync::Arc;

    #[tokio::test]
    async fn simple_predicate() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema = test_util::aggr_test_schema();

        let partitions = 4;
        let csv = test::scan_partitioned_csv(partitions)?;

        let predicate: Arc<dyn PhysicalExpr> = binary(
            binary(col("c2", &schema)?, Operator::Gt, lit(1u32), &schema)?,
            Operator::And,
            binary(col("c2", &schema)?, Operator::Lt, lit(4u32), &schema)?,
            &schema,
        )?;

        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, csv)?);

        let results = collect(filter, task_ctx).await?;

        results
            .iter()
            .for_each(|batch| assert_eq!(13, batch.num_columns()));
        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(41, row_count);

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::vtable_address_comparisons)]
    async fn with_new_children() -> Result<()> {
        let schema = test_util::aggr_test_schema();
        let partitions = 4;
        let input = test::scan_partitioned_csv(partitions)?;

        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("c2", &schema)?, Operator::Gt, lit(1u32), &schema)?;

        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input.clone())?);

        let new_filter = filter.clone().with_new_children(vec![input.clone()])?;
        assert!(!Arc::ptr_eq(&filter, &new_filter));

        let new_filter2 = with_new_children_if_necessary(filter.clone(), vec![input])?;
        assert!(Arc::ptr_eq(&filter, &new_filter2));

        Ok(())
    }

    #[tokio::test]
    async fn collect_columns_predicates() -> Result<()> {
        let schema = test_util::aggr_test_schema();
        let predicate: Arc<dyn PhysicalExpr> = binary(
            binary(
                binary(col("c2", &schema)?, Operator::GtEq, lit(1u32), &schema)?,
                Operator::And,
                binary(col("c2", &schema)?, Operator::Eq, lit(4u32), &schema)?,
                &schema,
            )?,
            Operator::And,
            binary(
                binary(
                    col("c2", &schema)?,
                    Operator::Eq,
                    col("c9", &schema)?,
                    &schema,
                )?,
                Operator::And,
                binary(
                    col("c1", &schema)?,
                    Operator::NotEq,
                    col("c13", &schema)?,
                    &schema,
                )?,
                &schema,
            )?,
            &schema,
        )?;

        let (equal_pairs, ne_pairs) = collect_columns_from_predicate(&predicate);

        assert_eq!(1, equal_pairs.len());
        assert_eq!(equal_pairs[0].0.name(), "c2");
        assert_eq!(equal_pairs[0].1.name(), "c9");

        assert_eq!(1, ne_pairs.len());
        assert_eq!(ne_pairs[0].0.name(), "c1");
        assert_eq!(ne_pairs[0].1.name(), "c13");

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_basic_expr() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        let bytes_per_row = 4;
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(100),
                total_byte_size: Some(100 * bytes_per_row),
                column_statistics: Some(vec![ColumnStatistics {
                    min_value: Some(ScalarValue::Int32(Some(1))),
                    max_value: Some(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            schema.clone(),
        ));

        // a <= 25
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?;

        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.statistics();
        assert_eq!(statistics.num_rows, Some(25));
        assert_eq!(statistics.total_byte_size, Some(25 * bytes_per_row));

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_column_level_basic_expr() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(100),
                column_statistics: Some(vec![ColumnStatistics {
                    min_value: Some(ScalarValue::Int32(Some(1))),
                    max_value: Some(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            schema.clone(),
        ));

        // a <= 25
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?;

        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.statistics();

        // a must be in [1, 25] range now!
        assert_eq!(statistics.num_rows, Some(25));
        assert_eq!(
            statistics.column_statistics,
            Some(vec![ColumnStatistics {
                min_value: Some(ScalarValue::Int32(Some(1))),
                max_value: Some(ScalarValue::Int32(Some(25))),
                ..Default::default()
            }])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_column_level_nested() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(100),
                column_statistics: Some(vec![ColumnStatistics {
                    min_value: Some(ScalarValue::Int32(Some(1))),
                    max_value: Some(ScalarValue::Int32(Some(100))),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            schema.clone(),
        ));

        // WHERE a <= 25
        let sub_filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?,
            input,
        )?);

        // Nested filters (two separate physical plans, instead of AND chain in the expr)
        // WHERE a >= 10
        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::GtEq, lit(10i32), &schema)?,
            sub_filter,
        )?);

        let statistics = filter.statistics();
        assert_eq!(statistics.num_rows, Some(16));
        assert_eq!(
            statistics.column_statistics,
            Some(vec![ColumnStatistics {
                min_value: Some(ScalarValue::Int32(Some(10))),
                max_value: Some(ScalarValue::Int32(Some(25))),
                ..Default::default()
            }])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_column_level_nested_multiple() -> Result<()> {
        // Table:
        //      a: min=1, max=100
        //      b: min=1, max=50
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(100),
                column_statistics: Some(vec![
                    ColumnStatistics {
                        min_value: Some(ScalarValue::Int32(Some(1))),
                        max_value: Some(ScalarValue::Int32(Some(100))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        min_value: Some(ScalarValue::Int32(Some(1))),
                        max_value: Some(ScalarValue::Int32(Some(50))),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            },
            schema.clone(),
        ));

        // WHERE a <= 25
        let a_lte_25: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?,
            input,
        )?);

        // WHERE b > 45
        let b_gt_5: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("b", &schema)?, Operator::Gt, lit(45i32), &schema)?,
            a_lte_25,
        )?);

        // WHERE a >= 10
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            binary(col("a", &schema)?, Operator::GtEq, lit(10i32), &schema)?,
            b_gt_5,
        )?);

        let statistics = filter.statistics();
        // On a uniform distribution, only fifteen rows will satisfy the
        // filter that 'a' proposed (a >= 10 AND a <= 25) (15/100) and only
        // 5 rows will satisfy the filter that 'b' proposed (b > 45) (5/50).
        //
        // Which would result with a selectivity of  '15/100 * 5/50' or 0.015
        // and that means about %1.5 of the all rows (rounded up to 2 rows).
        assert_eq!(statistics.num_rows, Some(2));
        assert_eq!(
            statistics.column_statistics,
            Some(vec![
                ColumnStatistics {
                    min_value: Some(ScalarValue::Int32(Some(10))),
                    max_value: Some(ScalarValue::Int32(Some(25))),
                    ..Default::default()
                },
                ColumnStatistics {
                    min_value: Some(ScalarValue::Int32(Some(45))),
                    max_value: Some(ScalarValue::Int32(Some(50))),
                    ..Default::default()
                }
            ])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_statistics_when_input_stats_missing() -> Result<()> {
        // Table:
        //      a: min=???, max=??? (missing)
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let input = Arc::new(StatisticsExec::new(
            Statistics {
                column_statistics: Some(vec![ColumnStatistics {
                    ..Default::default()
                }]),
                ..Default::default()
            },
            schema.clone(),
        ));

        // a <= 25
        let predicate: Arc<dyn PhysicalExpr> =
            binary(col("a", &schema)?, Operator::LtEq, lit(25i32), &schema)?;

        // WHERE a <= 25
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, input)?);

        let statistics = filter.statistics();
        assert_eq!(statistics.num_rows, None);

        Ok(())
    }
}
