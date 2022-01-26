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

use crate::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricsSet, Time,
};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
/// Aggregates all metrics during a complex operation, which is composed of multiple steps and
/// each stage reports its statistics separately.
/// Give sort as an example, when the dataset is more significant than available memory, it will report
/// multiple in-mem sort metrics and final merge-sort  metrics from `SortPreservingMergeStream`.
/// Therefore, We need a separation of metrics for which are final metrics (for output_rows accumulation),
/// and which are intermediate metrics that we only account for elapsed_compute time.
pub struct AggregatedMetricsSet {
    intermediate: Arc<std::sync::Mutex<Vec<ExecutionPlanMetricsSet>>>,
    final_: Arc<std::sync::Mutex<Vec<ExecutionPlanMetricsSet>>>,
}

impl AggregatedMetricsSet {
    /// Create a new aggregated set
    pub(crate) fn new() -> Self {
        Self {
            intermediate: Arc::new(std::sync::Mutex::new(vec![])),
            final_: Arc::new(std::sync::Mutex::new(vec![])),
        }
    }

    /// create a new intermediate baseline
    pub(crate) fn new_intermediate_baseline(&self, partition: usize) -> BaselineMetrics {
        let ms = ExecutionPlanMetricsSet::new();
        let result = BaselineMetrics::new(&ms, partition);
        self.intermediate.lock().unwrap().push(ms);
        result
    }

    /// create a new final baseline
    pub(crate) fn new_final_baseline(&self, partition: usize) -> BaselineMetrics {
        let ms = ExecutionPlanMetricsSet::new();
        let result = BaselineMetrics::new(&ms, partition);
        self.final_.lock().unwrap().push(ms);
        result
    }

    fn merge_compute_time(&self, dest: &Time) {
        let time1 = self
            .intermediate
            .lock()
            .unwrap()
            .iter()
            .map(|es| {
                es.clone_inner()
                    .elapsed_compute()
                    .map_or(0u64, |v| v as u64)
            })
            .sum();
        let time2 = self
            .final_
            .lock()
            .unwrap()
            .iter()
            .map(|es| {
                es.clone_inner()
                    .elapsed_compute()
                    .map_or(0u64, |v| v as u64)
            })
            .sum();
        dest.add_duration(Duration::from_nanos(time1));
        dest.add_duration(Duration::from_nanos(time2));
    }

    fn merge_spill_count(&self, dest: &Count) {
        let count1 = self
            .intermediate
            .lock()
            .unwrap()
            .iter()
            .map(|es| es.clone_inner().spill_count().map_or(0, |v| v))
            .sum();
        let count2 = self
            .final_
            .lock()
            .unwrap()
            .iter()
            .map(|es| es.clone_inner().spill_count().map_or(0, |v| v))
            .sum();
        dest.add(count1);
        dest.add(count2);
    }

    fn merge_spilled_bytes(&self, dest: &Count) {
        let count1 = self
            .intermediate
            .lock()
            .unwrap()
            .iter()
            .map(|es| es.clone_inner().spilled_bytes().map_or(0, |v| v))
            .sum();
        let count2 = self
            .final_
            .lock()
            .unwrap()
            .iter()
            .map(|es| es.clone_inner().spilled_bytes().map_or(0, |v| v))
            .sum();
        dest.add(count1);
        dest.add(count2);
    }

    fn merge_output_count(&self, dest: &Count) {
        let count = self
            .final_
            .lock()
            .unwrap()
            .iter()
            .map(|es| es.clone_inner().output_rows().map_or(0, |v| v))
            .sum();
        dest.add(count);
    }

    /// Aggregate all metrics into a one
    pub(crate) fn aggregate_all(&self) -> MetricsSet {
        let metrics = ExecutionPlanMetricsSet::new();
        let baseline = BaselineMetrics::new(&metrics, 0);
        self.merge_compute_time(baseline.elapsed_compute());
        self.merge_spill_count(baseline.spill_count());
        self.merge_spilled_bytes(baseline.spilled_bytes());
        self.merge_output_count(baseline.output_rows());
        metrics.clone_inner()
    }
}
