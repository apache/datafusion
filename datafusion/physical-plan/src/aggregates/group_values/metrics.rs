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

//! Metrics for the various group-by implementations.

use crate::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};

pub(crate) struct GroupByMetrics {
    /// Time spent calculating the group IDs from the evaluated grouping columns.
    pub(crate) time_calculating_group_ids: Time,
    /// Time spent evaluating the inputs to the aggregate functions.
    pub(crate) aggregate_arguments_time: Time,
    /// Time spent evaluating the aggregate expressions themselves
    /// (e.g. summing all elements and counting number of elements for `avg` aggregate).
    pub(crate) aggregation_time: Time,
    /// Time spent emitting the final results and constructing the record batch
    /// which includes finalizing the grouping expressions
    /// (e.g. emit from the hash table in case of hash aggregation) and the accumulators
    pub(crate) emitting_time: Time,
}

impl GroupByMetrics {
    pub(crate) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            time_calculating_group_ids: MetricBuilder::new(metrics)
                .subset_time("time_calculating_group_ids", partition),
            aggregate_arguments_time: MetricBuilder::new(metrics)
                .subset_time("aggregate_arguments_time", partition),
            aggregation_time: MetricBuilder::new(metrics)
                .subset_time("aggregation_time", partition),
            emitting_time: MetricBuilder::new(metrics)
                .subset_time("emitting_time", partition),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ExecutionPlan;
    use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use crate::execution_plan::collect_and_get_metrics_of;
    use crate::metrics::MetricsSet;
    use crate::test::TestMemoryExec;
    use arrow::array::{Float64Array, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_execution::TaskContext;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use std::sync::Arc;

    /// Helper function to verify all three GroupBy metrics exist and have non-zero values
    fn assert_groupby_metrics(metrics: &MetricsSet) {
        let agg_arguments_time = metrics.sum_by_name("aggregate_arguments_time");
        assert!(agg_arguments_time.is_some());
        assert!(agg_arguments_time.unwrap().as_usize() > 0);

        let aggregation_time = metrics.sum_by_name("aggregation_time");
        assert!(aggregation_time.is_some());
        assert!(aggregation_time.unwrap().as_usize() > 0);

        let emitting_time = metrics.sum_by_name("emitting_time");
        assert!(emitting_time.is_some());
        assert!(emitting_time.unwrap().as_usize() > 0);
    }

    #[tokio::test]
    async fn test_groupby_metrics_partial_mode() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // Create multiple batches to ensure metrics accumulate
        let batches = (0..5)
            .map(|i| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
                        Arc::new(Float64Array::from(vec![
                            i as f64,
                            (i + 1) as f64,
                            (i + 2) as f64,
                            (i + 3) as f64,
                        ])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let input = TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);

        let aggregates = vec![
            Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("b", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("SUM(b)")
                    .build()?,
            ),
            Arc::new(
                AggregateExprBuilder::new(count_udaf(), vec![col("b", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("COUNT(b)")
                    .build()?,
            ),
        ];

        let aggregate_exec: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggregates,
            vec![None, None],
            input,
            schema,
        )?);

        let task_ctx = Arc::new(TaskContext::default());
        let (_result, metrics) = collect_and_get_metrics_of(
            Arc::clone(&aggregate_exec),
            &aggregate_exec,
            Arc::clone(&task_ctx),
        )
        .await?;

        let metrics = metrics.unwrap();
        assert_groupby_metrics(&metrics);

        Ok(())
    }

    #[tokio::test]
    async fn test_groupby_metrics_final_mode() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        let batches = (0..3)
            .map(|i| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(UInt32Array::from(vec![1, 2, 3])),
                        Arc::new(Float64Array::from(vec![
                            i as f64,
                            (i + 1) as f64,
                            (i + 2) as f64,
                        ])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let partial_input =
            TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;

        let group_by =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);

        let aggregates = vec![Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![col("b", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("SUM(b)")
                .build()?,
        )];

        // Create partial aggregate
        let partial_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggregates.clone(),
            vec![None],
            partial_input,
            Arc::clone(&schema),
        )?);

        // Create final aggregate
        let final_aggregate: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            group_by.as_final(),
            aggregates,
            vec![None],
            partial_aggregate,
            schema,
        )?);

        let task_ctx = Arc::new(TaskContext::default());
        let (_result, metrics) = collect_and_get_metrics_of(
            Arc::clone(&final_aggregate),
            &final_aggregate,
            Arc::clone(&task_ctx),
        )
        .await?;

        let metrics = metrics.unwrap();
        assert_groupby_metrics(&metrics);

        Ok(())
    }
}
