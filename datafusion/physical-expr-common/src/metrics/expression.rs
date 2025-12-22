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

//! Metrics helpers for expression evaluation.

use super::{ExecutionPlanMetricsSet, MetricBuilder, MetricType, ScopedTimerGuard, Time};

/// Tracks evaluation time for a sequence of expressions.
///
/// # Example
/// Given SQL query:
///     EXPLAIN ANALYZE
///     SELECT a+1, pow(a,2)
///     FROM generate_series(1, 1000000) as t1(a)
///
/// This struct holds two time metrics for the projection expressions
/// `a+1` and `pow(a,2)`, respectively.
///
/// The output reads:
/// `ProjectionExec: expr=[a@0 + 1 as t1.a + Int64(1), power(CAST(a@0 AS Float64), 2) as pow(t1.a,Int64(2))], metrics=[... expr_0_eval_time=9.23ms, expr_1_eval_time=32.35ms...]`
#[derive(Debug, Clone)]
pub struct ExpressionEvaluatorMetrics {
    expression_times: Vec<Time>,
}

impl ExpressionEvaluatorMetrics {
    /// Create metrics for a collection of expressions.
    ///
    /// # Args
    /// - expression_labels: unique identifier for each metric, so that the metric
    ///   can get aggregated across multiple partitions. It is not the name showed
    ///   in the `EXPLAIN ANALYZE`, the metric name will be `expr_{idx}_eval_time`
    ///   according to the expression order.
    pub fn new<T>(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        expression_labels: impl IntoIterator<Item = T>,
    ) -> Self
    where
        T: Into<String>,
    {
        let expression_times = expression_labels
            .into_iter()
            .enumerate()
            .map(|(idx, label)| {
                MetricBuilder::new(metrics)
                    .with_new_label("expr", label.into())
                    .with_type(MetricType::DEV)
                    // Existing PhysicalExpr formatter is a bit verbose, so use simple name
                    .subset_time(format!("expr_{idx}_eval_time"), partition)
            })
            .collect();

        Self { expression_times }
    }

    /// Returns a timer guard for the expression at `index`, if present.
    pub fn scoped_timer(&self, index: usize) -> Option<ScopedTimerGuard<'_>> {
        self.expression_times.get(index).map(Time::timer)
    }

    /// The number of tracked expressions.
    pub fn len(&self) -> usize {
        self.expression_times.len()
    }

    /// True when no expressions are tracked.
    pub fn is_empty(&self) -> bool {
        self.expression_times.is_empty()
    }
}
