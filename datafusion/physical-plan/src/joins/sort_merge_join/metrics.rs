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

//! Module for tracking Sort Merge Join metrics

use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, SpillMetrics,
    Time,
};

/// Metrics for SortMergeJoinExec
pub(super) struct SortMergeJoinMetrics {
    /// Total time for joining probe-side batches to the build-side batches
    join_time: Time,
    /// Number of batches consumed by this operator
    input_batches: Count,
    /// Number of rows consumed by this operator
    input_rows: Count,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
    /// Peak memory used for buffered data.
    /// Calculated as sum of peak memory values across partitions
    peak_mem_used: Gauge,
    /// Metrics related to spilling
    spill_metrics: SpillMetrics,
}

impl SortMergeJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let peak_mem_used = MetricBuilder::new(metrics).gauge("peak_mem_used", partition);
        let spill_metrics = SpillMetrics::new(metrics, partition);

        let baseline_metrics = BaselineMetrics::new(metrics, partition);

        Self {
            join_time,
            input_batches,
            input_rows,
            baseline_metrics,
            peak_mem_used,
            spill_metrics,
        }
    }

    pub fn join_time(&self) -> Time {
        self.join_time.clone()
    }

    pub fn baseline_metrics(&self) -> BaselineMetrics {
        self.baseline_metrics.clone()
    }

    pub fn input_batches(&self) -> Count {
        self.input_batches.clone()
    }

    pub fn input_rows(&self) -> Count {
        self.input_rows.clone()
    }

    pub fn peak_mem_used(&self) -> Gauge {
        self.peak_mem_used.clone()
    }

    pub fn spill_metrics(&self) -> SpillMetrics {
        self.spill_metrics.clone()
    }
}
