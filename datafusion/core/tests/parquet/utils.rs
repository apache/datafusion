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

//! Utilities for parquet tests

use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::{accept, ExecutionPlan, ExecutionPlanVisitor};

/// Find the metrics from the first DataSourceExec encountered in the plan
#[derive(Debug)]
pub struct MetricsFinder {
    metrics: Option<MetricsSet>,
}
impl MetricsFinder {
    pub fn new() -> Self {
        Self { metrics: None }
    }

    /// Return the metrics if found
    pub fn into_metrics(self) -> Option<MetricsSet> {
        self.metrics
    }

    pub fn find_metrics(plan: &dyn ExecutionPlan) -> Option<MetricsSet> {
        let mut finder = Self::new();
        accept(plan, &mut finder).unwrap();
        finder.into_metrics()
    }
}

impl ExecutionPlanVisitor for MetricsFinder {
    type Error = std::convert::Infallible;
    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
            if data_source_exec
                .downcast_to_file_source::<ParquetSource>()
                .is_some()
            {
                self.metrics = data_source_exec.metrics();
            }
        }
        // stop searching once we have found the metrics
        Ok(self.metrics.is_none())
    }
}
