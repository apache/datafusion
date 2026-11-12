// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Module for tracking Sort Merge Join metrics

use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder,
    MetricCategory, Time,
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
}

impl SortMergeJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);
        let input_batches = MetricBuilder::new(metrics)
            .with_category(MetricCategory::Rows)
            .counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics)
            .with_category(MetricCategory::Rows)
            .counter("input_rows", partition);
        let peak_mem_used =
            MetricBuilder::new(metrics).peak_memory_usage("peak_mem_used", partition);

        let baseline_metrics = BaselineMetrics::new(metrics, partition);

        Self {
            join_time,
            input_batches,
            input_rows,
            baseline_metrics,
            peak_mem_used,
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
}
