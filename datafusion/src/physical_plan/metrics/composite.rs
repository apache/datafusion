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

//! Metrics common for complex operators with multiple steps.

use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::metrics::tracker::MemTrackingMetrics;
use crate::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricValue, MetricsSet, Time,
    Timestamp,
};
use crate::physical_plan::Metric;
use chrono::{TimeZone, Utc};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
/// Collects all metrics during a complex operation, which is composed of multiple steps and
/// each stage reports its statistics separately.
/// Give sort as an example, when the dataset is more significant than available memory, it will report
/// multiple in-mem sort metrics and final merge-sort  metrics from `SortPreservingMergeStream`.
/// Therefore, We need a separation of metrics for which are final metrics (for output_rows accumulation),
/// and which are intermediate metrics that we only account for elapsed_compute time.
pub struct CompositeMetricsSet {
    mid: ExecutionPlanMetricsSet,
    final_: ExecutionPlanMetricsSet,
}

impl Default for CompositeMetricsSet {
    fn default() -> Self {
        Self::new()
    }
}

impl CompositeMetricsSet {
    /// Create a new aggregated set
    pub fn new() -> Self {
        Self {
            mid: ExecutionPlanMetricsSet::new(),
            final_: ExecutionPlanMetricsSet::new(),
        }
    }

    /// create a new intermediate baseline
    pub fn new_intermediate_baseline(&self, partition: usize) -> BaselineMetrics {
        BaselineMetrics::new(&self.mid, partition)
    }

    /// create a new final baseline
    pub fn new_final_baseline(&self, partition: usize) -> BaselineMetrics {
        BaselineMetrics::new(&self.final_, partition)
    }

    /// create a new intermediate memory tracking metrics
    pub fn new_intermediate_tracking(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> MemTrackingMetrics {
        MemTrackingMetrics::new_with_rt(&self.mid, partition, runtime)
    }

    /// create a new final memory tracking metrics
    pub fn new_final_tracking(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> MemTrackingMetrics {
        MemTrackingMetrics::new_with_rt(&self.final_, partition, runtime)
    }

    fn merge_compute_time(&self, dest: &Time) {
        let time1 = self
            .mid
            .clone_inner()
            .elapsed_compute()
            .map_or(0u64, |v| v as u64);
        let time2 = self
            .final_
            .clone_inner()
            .elapsed_compute()
            .map_or(0u64, |v| v as u64);
        dest.add_duration(Duration::from_nanos(time1));
        dest.add_duration(Duration::from_nanos(time2));
    }

    fn merge_spill_count(&self, dest: &Count) {
        let count1 = self.mid.clone_inner().spill_count().map_or(0, |v| v);
        let count2 = self.final_.clone_inner().spill_count().map_or(0, |v| v);
        dest.add(count1);
        dest.add(count2);
    }

    fn merge_spilled_bytes(&self, dest: &Count) {
        let count1 = self.mid.clone_inner().spilled_bytes().map_or(0, |v| v);
        let count2 = self.final_.clone_inner().spill_count().map_or(0, |v| v);
        dest.add(count1);
        dest.add(count2);
    }

    fn merge_output_count(&self, dest: &Count) {
        let count = self.final_.clone_inner().output_rows().map_or(0, |v| v);
        dest.add(count);
    }

    fn merge_start_time(&self, dest: &Timestamp) {
        let start1 = self
            .mid
            .clone_inner()
            .sum(|metric| matches!(metric.value(), MetricValue::StartTimestamp(_)))
            .map(|v| v.as_usize());
        let start2 = self
            .final_
            .clone_inner()
            .sum(|metric| matches!(metric.value(), MetricValue::StartTimestamp(_)))
            .map(|v| v.as_usize());
        match (start1, start2) {
            (Some(start1), Some(start2)) => {
                dest.set(Utc.timestamp_nanos(start1.min(start2) as i64))
            }
            (Some(start1), None) => dest.set(Utc.timestamp_nanos(start1 as i64)),
            (None, Some(start2)) => dest.set(Utc.timestamp_nanos(start2 as i64)),
            (None, None) => {}
        }
    }

    fn merge_end_time(&self, dest: &Timestamp) {
        let start1 = self
            .mid
            .clone_inner()
            .sum(|metric| matches!(metric.value(), MetricValue::EndTimestamp(_)))
            .map(|v| v.as_usize());
        let start2 = self
            .final_
            .clone_inner()
            .sum(|metric| matches!(metric.value(), MetricValue::EndTimestamp(_)))
            .map(|v| v.as_usize());
        match (start1, start2) {
            (Some(start1), Some(start2)) => {
                dest.set(Utc.timestamp_nanos(start1.max(start2) as i64))
            }
            (Some(start1), None) => dest.set(Utc.timestamp_nanos(start1 as i64)),
            (None, Some(start2)) => dest.set(Utc.timestamp_nanos(start2 as i64)),
            (None, None) => {}
        }
    }

    /// Aggregate all metrics into a one
    pub fn aggregate_all(&self) -> MetricsSet {
        let mut metrics = MetricsSet::new();
        let elapsed_time = Time::new();
        let spill_count = Count::new();
        let spilled_bytes = Count::new();
        let output_count = Count::new();
        let start_time = Timestamp::new();
        let end_time = Timestamp::new();

        metrics.push(Arc::new(Metric::new(
            MetricValue::ElapsedCompute(elapsed_time.clone()),
            None,
        )));
        metrics.push(Arc::new(Metric::new(
            MetricValue::SpillCount(spill_count.clone()),
            None,
        )));
        metrics.push(Arc::new(Metric::new(
            MetricValue::SpilledBytes(spilled_bytes.clone()),
            None,
        )));
        metrics.push(Arc::new(Metric::new(
            MetricValue::OutputRows(output_count.clone()),
            None,
        )));
        metrics.push(Arc::new(Metric::new(
            MetricValue::StartTimestamp(start_time.clone()),
            None,
        )));
        metrics.push(Arc::new(Metric::new(
            MetricValue::EndTimestamp(end_time.clone()),
            None,
        )));

        self.merge_compute_time(&elapsed_time);
        self.merge_spill_count(&spill_count);
        self.merge_spilled_bytes(&spilled_bytes);
        self.merge_output_count(&output_count);
        self.merge_start_time(&start_time);
        self.merge_end_time(&end_time);
        metrics
    }
}
