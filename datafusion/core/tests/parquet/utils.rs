// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Utilities for parquet tests

use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanVisitor, accept};

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
        if let Some(data_source_exec) = plan.downcast_ref::<DataSourceExec>()
            && data_source_exec
                .downcast_to_file_source::<ParquetSource>()
                .is_some()
        {
            self.metrics = data_source_exec.metrics();
        }
        // stop searching once we have found the metrics
        Ok(self.metrics.is_none())
    }
}
