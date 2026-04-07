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

//! Metrics common for almost all operators

use std::{borrow::Cow, collections::BTreeMap, sync::Arc, task::Poll};

use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, utils::memory::get_record_batch_memory_size};

use super::{
    Count, ExecutionPlanMetricsSet, Metric, MetricBuilder, MetricsSet, Time, Timestamp,
};

const OUTPUT_ROWS_SKEW_METRIC_NAME: &str = "output_rows_skew";

/// Helper for creating and tracking common "baseline" metrics for
/// each operator
///
/// Example:
/// ```
/// use datafusion_physical_expr_common::metrics::{
///     BaselineMetrics, ExecutionPlanMetricsSet,
/// };
/// let metrics = ExecutionPlanMetricsSet::new();
///
/// let partition = 2;
/// let baseline_metrics = BaselineMetrics::new(&metrics, partition);
///
/// // during execution, in CPU intensive operation:
/// let timer = baseline_metrics.elapsed_compute().timer();
/// // .. do CPU intensive work
/// timer.done();
///
/// // when operator is finished:
/// baseline_metrics.done();
/// ```
#[derive(Debug, Clone)]
pub struct BaselineMetrics {
    /// end_time is set when `BaselineMetrics::done()` is called
    end_time: Timestamp,

    /// amount of time the operator was actively trying to use the CPU
    elapsed_compute: Time,

    /// output rows: the total output rows
    output_rows: Count,

    /// Memory usage of all output batches.
    ///
    /// Note: This value may be overestimated. If multiple output `RecordBatch`
    /// instances share underlying memory buffers, their sizes will be counted
    /// multiple times.
    /// Issue: <https://github.com/apache/datafusion/issues/16841>
    output_bytes: Count,

    /// output batches: the total output batch count
    output_batches: Count,
    // Remember to update `docs/source/user-guide/metrics.md` when updating comments
    // or adding new metrics
}

impl BaselineMetrics {
    /// Create a new BaselineMetric structure, and set `start_time` to now
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let start_time = MetricBuilder::new(metrics).start_timestamp(partition);
        start_time.record();

        Self {
            end_time: MetricBuilder::new(metrics)
                .with_type(super::MetricType::Summary)
                .end_timestamp(partition),
            elapsed_compute: MetricBuilder::new(metrics)
                .with_type(super::MetricType::Summary)
                .elapsed_compute(partition),
            output_rows: MetricBuilder::new(metrics)
                .with_type(super::MetricType::Summary)
                .output_rows(partition),
            output_bytes: MetricBuilder::new(metrics)
                .with_type(super::MetricType::Summary)
                .output_bytes(partition),
            output_batches: MetricBuilder::new(metrics)
                .with_type(super::MetricType::Dev)
                .output_batches(partition),
        }
    }

    /// Returns a [`BaselineMetrics`] that updates the same `elapsed_compute` ignoring
    /// all other metrics
    ///
    /// This is useful when an operator offloads some of its intermediate work to separate tasks
    /// that as a result won't be recorded by [`Self::record_poll`]
    pub fn intermediate(&self) -> BaselineMetrics {
        Self {
            end_time: Default::default(),
            elapsed_compute: self.elapsed_compute.clone(),
            output_rows: Default::default(),
            output_bytes: Default::default(),
            output_batches: Default::default(),
        }
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_compute(&self) -> &Time {
        &self.elapsed_compute
    }

    /// return the metric for the total number of output rows produced
    pub fn output_rows(&self) -> &Count {
        &self.output_rows
    }

    /// return the metric for the total number of output batches produced
    pub fn output_batches(&self) -> &Count {
        &self.output_batches
    }

    /// Returns a derived metric that summarizes how unevenly `output_rows`
    /// are distributed across partitions.
    ///
    /// The score is normalized to the range `[0%, 100%]`, where `0%`
    /// indicates a perfectly balanced distribution and `100%` indicates the
    /// most skewed distribution.
    ///
    /// The calculation is:
    /// `effective_parallelism = square(sum(r_i)) / sum(square(r_i))`
    /// `output_rows_skew = (1 - ((effective_parallelism - 1) / (partition_count - 1))) * 100%`
    ///
    /// Example: for 4 partitions with output rows `[10, 10, 10, 10]`,
    /// `effective_parallelism = 40^2 / (10^2 + 10^2 + 10^2 + 10^2) = 4`,
    /// so `output_rows_skew = 0%`. For `[40, 0, 0, 0]`, the score is `100%`.
    pub fn output_rows_skew_metric(metrics: &MetricsSet) -> Option<Arc<Metric>> {
        let output_rows = metrics
            .iter()
            .filter_map(|metric| match (metric.partition(), metric.value()) {
                (Some(partition), super::MetricValue::OutputRows(count)) => {
                    Some((partition, count.value() as u128))
                }
                _ => None,
            })
            .fold(
                BTreeMap::<usize, u128>::new(),
                |mut output_rows, (partition, rows)| {
                    *output_rows.entry(partition).or_default() += rows;
                    output_rows
                },
            )
            .into_values()
            .collect::<Vec<_>>();

        if output_rows.is_empty() {
            return None;
        }

        let ratio_metrics = super::RatioMetrics::new().with_display_raw_values(false);
        if let Some(score) = output_rows_skew_score(&output_rows) {
            ratio_metrics.set_part((score * 10_000.0).round() as usize);
            ratio_metrics.set_total(10_000);
        }

        Some(Arc::new(
            Metric::new(
                super::MetricValue::Ratio {
                    name: Cow::Borrowed(OUTPUT_ROWS_SKEW_METRIC_NAME),
                    ratio_metrics,
                },
                None,
            )
            .with_type(super::MetricType::Dev),
        ))
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    ///
    /// Note care should be taken to call `done()` manually if
    /// `BaselineMetrics` is not `drop`ped immediately upon operator
    /// completion, as async streams may not be dropped immediately
    /// depending on the consumer.
    pub fn done(&self) {
        self.end_time.record()
    }

    /// Record that some number of rows have been produced as output
    ///
    /// See the [`RecordOutput`] for conveniently recording record
    /// batch output for other thing
    pub fn record_output(&self, num_rows: usize) {
        self.output_rows.add(num_rows);
    }

    /// If not previously recorded `done()`, record
    pub fn try_done(&self) {
        if self.end_time.value().is_none() {
            self.end_time.record()
        }
    }

    /// Process a poll result of a stream producing output for an operator.
    ///
    /// Note: this method only updates `output_rows` and `end_time` metrics.
    /// Remember to update `elapsed_compute` and other metrics manually.
    pub fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch>>>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if let Poll::Ready(maybe_batch) = &poll {
            match maybe_batch {
                Some(Ok(batch)) => {
                    batch.record_output(self);
                }
                Some(Err(_)) => self.done(),
                None => self.done(),
            }
        }
        poll
    }
}

impl Drop for BaselineMetrics {
    fn drop(&mut self) {
        self.try_done()
    }
}

/// See [`BaselineMetrics::output_rows_skew_metric`] for the algorithm.
fn output_rows_skew_score(output_rows: &[u128]) -> Option<f64> {
    if output_rows.is_empty() {
        return None;
    }

    let partition_count = output_rows.len();
    if partition_count == 1 {
        return Some(0.0);
    }

    let (total_rows, sum_of_squares) =
        output_rows
            .iter()
            .fold((0.0, 0.0), |(total_rows, sum_of_squares), rows| {
                let rows = *rows as f64;
                (total_rows + rows, sum_of_squares + rows.powi(2))
            });
    if total_rows == 0.0 {
        return None;
    }

    if sum_of_squares == 0.0 {
        return None;
    }

    let effective_parallelism = total_rows.powi(2) / sum_of_squares;
    let balanced_score = (effective_parallelism - 1.0) / (partition_count as f64 - 1.0);

    Some((1.0 - balanced_score).clamp(0.0, 1.0))
}

/// Helper for creating and tracking spill-related metrics for
/// each operator
#[derive(Debug, Clone)]
pub struct SpillMetrics {
    /// count of spills during the execution of the operator
    pub spill_file_count: Count,

    /// total bytes actually written to disk during the execution of the operator
    pub spilled_bytes: Count,

    /// total spilled rows during the execution of the operator
    pub spilled_rows: Count,
}

impl SpillMetrics {
    /// Create a new SpillMetrics structure
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            spill_file_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            spilled_rows: MetricBuilder::new(metrics).spilled_rows(partition),
        }
    }
}

/// Metrics for tracking batch splitting activity
#[derive(Debug, Clone)]
pub struct SplitMetrics {
    /// Number of times an input [`RecordBatch`] was split
    pub batches_split: Count,
}

impl SplitMetrics {
    /// Create a new [`SplitMetrics`]
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            batches_split: MetricBuilder::new(metrics)
                .with_category(super::MetricCategory::Rows)
                .counter("batches_split", partition),
        }
    }
}

/// Trait for things that produce output rows as a result of execution.
pub trait RecordOutput {
    /// Record that some number of output rows have been produced
    ///
    /// Meant to be composable so that instead of returning `batch`
    /// the operator can return `batch.record_output(baseline_metrics)`
    fn record_output(self, bm: &BaselineMetrics) -> Self;
}

impl RecordOutput for usize {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        bm.record_output(self);
        self
    }
}

impl RecordOutput for RecordBatch {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        bm.record_output(self.num_rows());
        let n_bytes = get_record_batch_memory_size(&self);
        bm.output_bytes.add(n_bytes);
        bm.output_batches.add(1);
        self
    }
}

impl RecordOutput for &RecordBatch {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        bm.record_output(self.num_rows());
        let n_bytes = get_record_batch_memory_size(self);
        bm.output_bytes.add(n_bytes);
        bm.output_batches.add(1);
        self
    }
}

impl RecordOutput for Option<&RecordBatch> {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        if let Some(record_batch) = &self {
            record_batch.record_output(bm);
        }
        self
    }
}

impl RecordOutput for Option<RecordBatch> {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        if let Some(record_batch) = &self {
            record_batch.record_output(bm);
        }
        self
    }
}

impl RecordOutput for Result<RecordBatch> {
    fn record_output(self, bm: &BaselineMetrics) -> Self {
        if let Ok(record_batch) = &self {
            record_batch.record_output(bm);
        }
        self
    }
}
