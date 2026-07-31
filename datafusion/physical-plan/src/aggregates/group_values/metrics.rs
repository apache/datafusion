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

use crate::metrics::{ExecutionPlanMetricsSet, MetricBuilder, ScopedTimerGuard, Time};

#[derive(Clone)]
pub(crate) struct AggregateArgumentMetrics {
    argument_times: Vec<Time>,
}

impl AggregateArgumentMetrics {
    pub(crate) fn new<T>(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        aggregate_labels: impl IntoIterator<Item = T>,
    ) -> Self
    where
        T: Into<String>,
    {
        let argument_times = aggregate_labels
            .into_iter()
            .enumerate()
            .map(|(idx, label)| {
                MetricBuilder::new(metrics)
                    .with_new_label("aggregate", label.into())
                    .subset_time(format!("agg_expr_{idx}_arguments_time"), partition)
            })
            .collect();

        Self { argument_times }
    }

    pub(crate) fn scoped_argument_timer(
        &self,
        index: usize,
    ) -> Option<ScopedTimerGuard<'_>> {
        self.argument_times.get(index).map(Time::timer)
    }
}

#[derive(Clone)]
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
    use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use crate::metrics::{MetricValue, MetricsSet};
    use crate::test::TestMemoryExec;
    use crate::{ExecutionPlan, collect};
    use arrow::array::{Float64Array, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_execution::TaskContext;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
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

    fn aggregate_argument_metric_displays(metrics: &MetricsSet) -> Vec<String> {
        metrics
            .iter()
            .filter(|metric| {
                matches!(
                    metric.value(),
                    MetricValue::Time { name, .. }
                        if name.ends_with("_arguments_time")
                            && name.as_ref() != "aggregate_arguments_time"
                )
            })
            .map(|metric| metric.to_string())
            .collect()
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

        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggregates,
            vec![None, None],
            input,
            schema,
        )?);

        // This test is for `GroupByMetrics`, which are maintained by
        // `GroupedHashAggregateStream`. Use a finite memory pool so the partial
        // aggregate does not take the initial-partial stream path.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(10 * 1024 * 1024, 1.0)
            .build_arc()?;
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let _result =
            collect(Arc::clone(&aggregate_exec) as _, Arc::clone(&task_ctx)).await?;

        let metrics = aggregate_exec.metrics().unwrap();
        assert_groupby_metrics(&metrics);

        Ok(())
    }

    #[tokio::test]
    async fn test_groupby_aggregate_argument_metrics_distinguish_inputs() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::UInt32, false),
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]));

        let batches = (0..5)
            .map(|i| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(UInt32Array::from(vec![1, 2, 1, 2])),
                        Arc::new(Float64Array::from(vec![
                            i as f64,
                            (i + 1) as f64,
                            (i + 2) as f64,
                            (i + 3) as f64,
                        ])),
                        Arc::new(Float64Array::from(vec![
                            (i + 4) as f64,
                            (i + 5) as f64,
                            (i + 6) as f64,
                            (i + 7) as f64,
                        ])),
                    ],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let input = TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("k", &schema)?, "k".to_string())]);
        let aggregates = vec![
            Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("a", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("SUM(a)")
                    .build()?,
            ),
            Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("b", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("SUM(b)")
                    .build()?,
            ),
        ];

        let aggregate_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggregates,
            vec![None, None],
            input,
            schema,
        )?);

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(10 * 1024 * 1024, 1.0)
            .build_arc()?;
        let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let _result =
            collect(Arc::clone(&aggregate_exec) as _, Arc::clone(&task_ctx)).await?;

        let metrics = aggregate_exec.metrics().unwrap();
        let metric_displays = aggregate_argument_metric_displays(&metrics);
        assert_eq!(metric_displays.len(), 2, "{metric_displays:#?}");
        assert!(
            metric_displays
                .iter()
                .any(|display| display.contains("SUM(a)")),
            "{metric_displays:#?}"
        );
        assert!(
            metric_displays
                .iter()
                .any(|display| display.contains("SUM(b)")),
            "{metric_displays:#?}"
        );

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
        let final_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            group_by.as_final(),
            aggregates,
            vec![None],
            partial_aggregate,
            schema,
        )?);

        let task_ctx = Arc::new(TaskContext::default());
        let _result =
            collect(Arc::clone(&final_aggregate) as _, Arc::clone(&task_ctx)).await?;

        let metrics = final_aggregate.metrics().unwrap();
        assert_groupby_metrics(&metrics);

        Ok(())
    }
}
