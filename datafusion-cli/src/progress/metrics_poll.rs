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

//! Live metrics polling from physical plans

use datafusion::physical_plan::{visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor};
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use std::sync::Arc;

/// Polls live metrics from a physical plan
pub struct MetricsPoller {
    plan: Arc<dyn ExecutionPlan>,
}

impl MetricsPoller {
    pub fn new(plan: &Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan: Arc::clone(plan),
        }
    }

    /// Poll current metrics from the plan
    pub fn poll(&mut self) -> LiveMetrics {
        let mut visitor = MetricsVisitor::new();
        let _ = visit_execution_plan(self.plan.as_ref(), &mut visitor);
        visitor.into_metrics()
    }
}

/// Live metrics collected from plan execution
#[derive(Debug, Clone, Default)]
pub struct LiveMetrics {
    pub bytes_scanned: usize,
    pub rows_processed: usize,
    pub batches_processed: usize,
}

/// Visitor to collect live metrics from plan nodes
struct MetricsVisitor {
    metrics: LiveMetrics,
}

impl MetricsVisitor {
    fn new() -> Self {
        Self {
            metrics: LiveMetrics::default(),
        }
    }

    fn into_metrics(self) -> LiveMetrics {
        self.metrics
    }
}

impl ExecutionPlanVisitor for MetricsVisitor {
    type Error = datafusion::error::DataFusionError;

    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> Result<bool, Self::Error> {
        let metrics_set = plan.metrics();
        self.accumulate_metrics(&metrics_set);
        
        // Continue visiting children
        Ok(true)
    }
}

impl MetricsVisitor {
    /// Accumulate metrics from a metrics set
    fn accumulate_metrics(&mut self, metrics_set: &Option<MetricsSet>) {
        if let Some(metrics) = metrics_set {
            for metric in metrics.iter() {
                // Get metric name from the metric itself
                let name = "";  // Simplified for now
                self.process_metric(name, &metric.value());
            }
        }
    }

    /// Process an individual metric
    fn process_metric(&mut self, name: &str, value: &MetricValue) {
        match name {
            "bytes_scanned" => {
                if let Some(count) = self.extract_count_value(value) {
                    self.metrics.bytes_scanned += count;
                }
            }
            "output_rows" => {
                if let Some(count) = self.extract_count_value(value) {
                    self.metrics.rows_processed += count;
                }
            }
            "output_batches" => {
                if let Some(count) = self.extract_count_value(value) {
                    self.metrics.batches_processed += count;
                }
            }
            _ => {
                // Check for common patterns in metric names
                if name.contains("bytes") && name.contains("scan") {
                    if let Some(count) = self.extract_count_value(value) {
                        self.metrics.bytes_scanned += count;
                    }
                } else if name.contains("rows") && (name.contains("output") || name.contains("produce")) {
                    if let Some(count) = self.extract_count_value(value) {
                        self.metrics.rows_processed += count;
                    }
                }
            }
        }
    }

    /// Extract a count value from a metric value
    fn extract_count_value(&self, value: &MetricValue) -> Option<usize> {
        // This is a simplified extraction - in practice we'd need to handle
        // different metric value types more robustly
        match value {
            MetricValue::Count { name: _, count } => Some(count.value()),
            MetricValue::Gauge { name: _, gauge } => Some(gauge.value()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::empty::EmptyExec;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test] 
    fn test_metrics_poller() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
        ]));
        
        let empty_exec = EmptyExec::new(schema);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(empty_exec);
        
        let mut poller = MetricsPoller::new(&plan);
        let metrics = poller.poll();
        
        // EmptyExec should have zero metrics initially
        assert_eq!(metrics.bytes_scanned, 0);
        assert_eq!(metrics.rows_processed, 0);
        assert_eq!(metrics.batches_processed, 0);
    }
}