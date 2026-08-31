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

    pub(crate) fn time<R>(&self, index: usize, f: impl FnOnce() -> R) -> R {
        debug_assert!(
            index < self.argument_times.len(),
            "aggregate argument metric index {index} out of range"
        );
        let _timer = self.argument_times.get(index).map(Time::timer);
        f()
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum AccumulatorPhase {
    Update,
    Merge,
    State,
    ConvertToState,
    Evaluate,
}

#[derive(Clone)]
pub(crate) struct AggregateAccumulatorMetrics {
    update_times: Option<Vec<Time>>,
    merge_times: Option<Vec<Time>>,
    state_times: Option<Vec<Time>>,
    convert_to_state_times: Option<Vec<Time>>,
    evaluate_times: Option<Vec<Time>>,
}

impl AggregateAccumulatorMetrics {
    pub(crate) fn new<T>(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        aggregate_labels: impl IntoIterator<Item = T>,
        phases: &[AccumulatorPhase],
    ) -> Self
    where
        T: Into<String>,
    {
        let aggregate_labels = aggregate_labels
            .into_iter()
            .map(Into::into)
            .collect::<Vec<String>>();
        let new_phase_metrics = |phase| {
            aggregate_labels
                .iter()
                .enumerate()
                .map(|(idx, label)| {
                    MetricBuilder::new(metrics)
                        .with_new_label("aggregate", label.clone())
                        .subset_time(format!("agg_expr_{idx}_{phase}_time"), partition)
                })
                .collect()
        };

        Self {
            update_times: phases
                .contains(&AccumulatorPhase::Update)
                .then(|| new_phase_metrics("update")),
            merge_times: phases
                .contains(&AccumulatorPhase::Merge)
                .then(|| new_phase_metrics("merge")),
            state_times: phases
                .contains(&AccumulatorPhase::State)
                .then(|| new_phase_metrics("state")),
            convert_to_state_times: phases
                .contains(&AccumulatorPhase::ConvertToState)
                .then(|| new_phase_metrics("convert_to_state")),
            evaluate_times: phases
                .contains(&AccumulatorPhase::Evaluate)
                .then(|| new_phase_metrics("evaluate")),
        }
    }

    pub(crate) fn time<R>(
        &self,
        index: usize,
        phase: AccumulatorPhase,
        f: impl FnOnce() -> R,
    ) -> R {
        let times = match phase {
            AccumulatorPhase::Update => self.update_times.as_ref(),
            AccumulatorPhase::Merge => self.merge_times.as_ref(),
            AccumulatorPhase::State => self.state_times.as_ref(),
            AccumulatorPhase::ConvertToState => self.convert_to_state_times.as_ref(),
            AccumulatorPhase::Evaluate => self.evaluate_times.as_ref(),
        };
        debug_assert!(
            times.is_some_and(|times| index < times.len()),
            "aggregate accumulator metric index {index} for uninitialized phase"
        );
        let _timer = times.and_then(|times| times.get(index)).map(Time::timer);
        f()
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
    use crate::aggregates_blocked::{BlockedAggregateExec, AggregateMode, PhysicalGroupBy};
    use crate::metrics::{MetricValue, MetricsSet};
    use crate::test::TestMemoryExec;
    use crate::{ExecutionPlan, collect};
    use arrow::array::{Float64Array, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::aggregate::{
        AggregateExprBuilder, AggregateFunctionExpr,
    };
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

    fn aggregate_metric_names_and_labels(
        metrics: &MetricsSet,
        suffix: &str,
    ) -> Vec<(String, String)> {
        metrics
            .iter()
            .filter_map(|metric| match metric.value() {
                MetricValue::Time { name, .. }
                    if name
                        .strip_prefix("agg_expr_")
                        .and_then(|name| name.split_once('_'))
                        .is_some_and(|(_, metric)| metric == suffix) =>
                {
                    let aggregate_label = metric
                        .labels()
                        .iter()
                        .find(|label| label.name() == "aggregate")?
                        .value()
                        .to_string();
                    Some((name.to_string(), aggregate_label))
                }
                _ => None,
            })
            .collect()
    }

    fn assert_aggregate_metric_labels(metrics: &MetricsSet, suffix: &str) {
        let mut metric_names_and_labels =
            aggregate_metric_names_and_labels(metrics, suffix);
        metric_names_and_labels.sort();
        assert_eq!(
            metric_names_and_labels,
            vec![
                (format!("agg_expr_0_{suffix}"), "SUM(a)".to_string()),
                (format!("agg_expr_1_{suffix}"), "SUM(b)".to_string()),
            ]
        );
    }

    fn assert_aggregate_metric_times_positive(metrics: &MetricsSet, suffix: &str) {
        let mut found = false;
        for metric in metrics.iter().filter(|metric| {
            matches!(
                metric.value(),
                MetricValue::Time { name, .. }
                    if name
                        .strip_prefix("agg_expr_")
                        .and_then(|name| name.split_once('_'))
                        .is_some_and(|(_, phase)| phase == suffix)
            )
        }) {
            found = true;
            assert!(metric.value().as_usize() > 0);
        }
        assert!(found, "expected aggregate metrics ending in {suffix}");
    }

    fn sum_aggregate(
        schema: &Arc<Schema>,
        column: &str,
        alias: &str,
    ) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![col(column, schema)?])
                .schema(Arc::clone(schema))
                .alias(alias)
                .build()?,
        ))
    }

    fn count_aggregate(
        schema: &Arc<Schema>,
        column: &str,
        alias: &str,
    ) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col(column, schema)?])
                .schema(Arc::clone(schema))
                .alias(alias)
                .build()?,
        ))
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
            sum_aggregate(&schema, "b", "SUM(b)")?,
            count_aggregate(&schema, "b", "COUNT(b)")?,
        ];

        let aggregate_exec = Arc::new(BlockedAggregateExec::try_new(
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
            sum_aggregate(&schema, "a", "SUM(a)")?,
            sum_aggregate(&schema, "b", "SUM(b)")?,
        ];

        let aggregate_exec = Arc::new(BlockedAggregateExec::try_new(
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
        let task_ctx =
            Arc::new(
                TaskContext::default()
                    .with_runtime(runtime)
                    .with_session_config(SessionConfig::new().set_bool(
                        "datafusion.execution.enable_migration_aggregate",
                        true,
                    )),
            );
        let _result =
            collect(Arc::clone(&aggregate_exec) as _, Arc::clone(&task_ctx)).await?;

        let metrics = aggregate_exec.metrics().unwrap();
        assert_aggregate_metric_labels(&metrics, "arguments_time");
        assert_aggregate_metric_labels(&metrics, "update_time");
        assert_aggregate_metric_labels(&metrics, "state_time");
        assert_aggregate_metric_times_positive(&metrics, "update_time");
        assert_aggregate_metric_times_positive(&metrics, "state_time");

        Ok(())
    }

    #[tokio::test]
    async fn test_legacy_groupby_aggregate_accumulator_metrics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::UInt32, false),
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt32Array::from(vec![1, 2, 1, 2])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                Arc::new(Float64Array::from(vec![5.0, 6.0, 7.0, 8.0])),
            ],
        )?;
        let input =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("k", &schema)?, "k".to_string())]);
        let aggregates = vec![
            sum_aggregate(&schema, "a", "SUM(a)")?,
            sum_aggregate(&schema, "b", "SUM(b)")?,
        ];
        let aggregate_exec = Arc::new(BlockedAggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggregates,
            vec![None, None],
            input,
            schema,
        )?);
        let task_ctx = Arc::new(
            TaskContext::default().with_session_config(
                SessionConfig::new()
                    .set_bool("datafusion.execution.enable_migration_aggregate", false),
            ),
        );
        let _result =
            collect(Arc::clone(&aggregate_exec) as _, Arc::clone(&task_ctx)).await?;

        let metrics = aggregate_exec.metrics().unwrap();
        assert_aggregate_metric_labels(&metrics, "arguments_time");
        assert_aggregate_metric_labels(&metrics, "update_time");
        assert_aggregate_metric_labels(&metrics, "state_time");
        assert_aggregate_metric_times_positive(&metrics, "update_time");
        assert_aggregate_metric_times_positive(&metrics, "state_time");

        Ok(())
    }

    async fn assert_groupby_metrics_final_mode(
        enable_migration_aggregate: bool,
    ) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Float64, false),
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
                        Arc::new(Float64Array::from(vec![
                            (i + 3) as f64,
                            (i + 4) as f64,
                            (i + 5) as f64,
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

        let aggregates = vec![
            sum_aggregate(&schema, "b", "SUM(b)")?,
            sum_aggregate(&schema, "c", "SUM(c)")?,
        ];

        // Create partial aggregate
        let partial_aggregate = Arc::new(BlockedAggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggregates.clone(),
            vec![None, None],
            partial_input,
            Arc::clone(&schema),
        )?);

        // Create final aggregate
        let final_aggregate = Arc::new(BlockedAggregateExec::try_new(
            AggregateMode::Final,
            group_by.as_final(),
            aggregates,
            vec![None, None],
            partial_aggregate,
            schema,
        )?);

        let task_ctx = Arc::new(TaskContext::default().with_session_config(
            SessionConfig::new().set_bool(
                "datafusion.execution.enable_migration_aggregate",
                enable_migration_aggregate,
            ),
        ));
        let _result =
            collect(Arc::clone(&final_aggregate) as _, Arc::clone(&task_ctx)).await?;

        let metrics = final_aggregate.metrics().unwrap();
        assert_groupby_metrics(&metrics);
        assert_eq!(
            aggregate_metric_names_and_labels(&metrics, "merge_time"),
            vec![
                ("agg_expr_0_merge_time".to_string(), "SUM(b)".to_string()),
                ("agg_expr_1_merge_time".to_string(), "SUM(c)".to_string()),
            ]
        );
        assert_eq!(
            aggregate_metric_names_and_labels(&metrics, "evaluate_time"),
            vec![
                ("agg_expr_0_evaluate_time".to_string(), "SUM(b)".to_string()),
                ("agg_expr_1_evaluate_time".to_string(), "SUM(c)".to_string()),
            ]
        );
        assert_aggregate_metric_times_positive(&metrics, "merge_time");
        assert_aggregate_metric_times_positive(&metrics, "evaluate_time");

        Ok(())
    }

    #[tokio::test]
    async fn test_groupby_metrics_final_mode() -> Result<()> {
        for enable_migration_aggregate in [true, false] {
            assert_groupby_metrics_final_mode(enable_migration_aggregate).await?;
        }

        Ok(())
    }
}
