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

//! Progress reporting for DataFusion CLI queries
//!
//! This module provides comprehensive progress tracking for DataFusion queries, including:
//!
//! # Features
//! - Real-time progress bars with percentage completion
//! - ETA estimation using multiple algorithms (Linear, Alpha filter, Kalman filter)
//! - Automatic detection of pipeline-breaking operators (sorts, joins, aggregates)
//! - Phase-aware progress tracking to avoid "stuck at 100%" issues
//! - TTY auto-detection for seamless terminal integration
//!
//! # Usage
//! ```bash
//! # Basic progress bar (auto-enabled in TTY)
//! datafusion-cli --progress auto
//!
//! # Force progress bar on with specific estimator
//! datafusion-cli --progress on --progress-estimator alpha
//!
//! # Spinner mode for unknown progress
//! datafusion-cli --progress on --progress-style spinner
//! ```
//!
//! # Implementation Notes
//! The progress system addresses review feedback from the DataFusion community:
//! - Uses robust ExecutionPlan analysis instead of brittle string matching
//! - Alpha filter is the default (simpler than Kalman, more accurate than linear)
//! - Smart handling of blocking operators prevents progress from appearing stuck
//! - Phase tracking provides user feedback during complex operations
//!
//! # Limitations
//! - Progress accuracy depends on DataFusion's metrics availability
//! - Complex queries with multiple blocking phases may show approximate progress
//! - Very fast queries may not show progress bars due to update intervals

mod config;
mod display;
mod estimator;
mod metrics_poll;
mod plan_introspect;

pub use config::{ProgressConfig, ProgressEstimator, ProgressMode, ProgressStyle};

use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common_runtime::SpawnedTask;
use std::sync::Arc;
use std::time::Duration;

/// Main progress reporter that coordinates metrics collection, ETA estimation, and display
pub struct ProgressReporter {
    _handle: SpawnedTask<()>,
}

impl ProgressReporter {
    /// Start a new progress reporter for the given physical plan
    pub async fn start(
        physical_plan: &Arc<dyn ExecutionPlan>,
        config: ProgressConfig,
    ) -> Result<Self> {
        // Clone the plan for the background task
        let plan = Arc::clone(physical_plan);

        let _handle = SpawnedTask::spawn(async move {
            let reporter = ProgressReporterInner::new(plan, config);
            reporter.run().await;
        });

        Ok(Self { _handle })
    }

    /// Stop the progress reporter
    /// Note: The task is automatically aborted when this struct is dropped
    pub async fn stop(&self) {
        // Task will be aborted automatically when this struct is dropped
    }
}

/// Internal implementation of the progress reporter
struct ProgressReporterInner {
    plan: Arc<dyn ExecutionPlan>,
    config: ProgressConfig,
}

impl ProgressReporterInner {
    fn new(plan: Arc<dyn ExecutionPlan>, config: ProgressConfig) -> Self {
        Self { plan, config }
    }

    async fn run(self) {
        // Early exit if progress is disabled
        if !self.config.should_show_progress() {
            return;
        }

        let introspector = plan_introspect::PlanIntrospector::new(&self.plan);
        let totals = introspector.get_totals();

        let mut poller = metrics_poll::MetricsPoller::new(&self.plan);
        let mut estimator = estimator::ProgressEstimator::new(self.config.estimator);
        let mut display = display::ProgressDisplay::new(self.config.style);

        let interval = Duration::from_millis(self.config.interval_ms);
        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;
            let metrics = poller.poll();
            let progress = self.calculate_progress(&totals, &metrics);

            let eta = estimator.update(progress.clone());
            display.update(&progress, eta);

            // Check for completion - when we have exact totals and current >= total
            if let (Some(total), current) = (progress.total, progress.current) {
                if current >= total && (totals.has_exact_bytes || totals.has_exact_rows) {
                    // Query has completed, exit the progress loop
                    display.finish();
                    break;
                }
            }

            // For queries without known totals, rely on task termination when query completes
        }
    }

    fn calculate_progress(
        &self,
        totals: &plan_introspect::PlanTotals,
        metrics: &metrics_poll::LiveMetrics,
    ) -> ProgressInfo {
        let (current, total, unit) =
            if totals.total_bytes > 0 && metrics.bytes_scanned > 0 {
                (
                    metrics.bytes_scanned,
                    totals.total_bytes,
                    ProgressUnit::Bytes,
                )
            } else if totals.total_rows > 0 && metrics.rows_processed > 0 {
                (
                    metrics.rows_processed,
                    totals.total_rows,
                    ProgressUnit::Rows,
                )
            } else {
                return ProgressInfo {
                    current: metrics.rows_processed,
                    total: None,
                    unit: ProgressUnit::Rows,
                    percent: None,
                    has_blocking_operators: totals.has_blocking_operators,
                    phase: ExecutionPhase::Reading,
                };
            };

        let raw_percent = if total > 0 {
            ((current as f64 / total as f64) * 100.0).min(100.0)
        } else {
            0.0
        };

        // Determine execution phase and adjust progress accordingly
        let (percent, phase) = self.determine_execution_phase(
            raw_percent,
            totals.has_blocking_operators,
            metrics,
        );

        ProgressInfo {
            current,
            total: Some(total),
            unit,
            percent: Some(percent),
            has_blocking_operators: totals.has_blocking_operators,
            phase,
        }
    }

    /// Determine which execution phase we're in and adjust progress display
    fn determine_execution_phase(
        &self,
        raw_percent: f64,
        has_blocking_operators: bool,
        _metrics: &metrics_poll::LiveMetrics,
    ) -> (f64, ExecutionPhase) {
        if !has_blocking_operators {
            // No blocking operators, simple linear progress
            return (raw_percent, ExecutionPhase::Reading);
        }

        // With blocking operators, we need to be smarter about phases
        if raw_percent < 90.0 {
            // Still reading data
            (raw_percent, ExecutionPhase::Reading)
        } else if raw_percent >= 99.0 {
            // Likely in blocking operation phase
            // Show progress as processing instead of stuck at 100%
            (75.0, ExecutionPhase::Processing)
        } else {
            // Transitioning to blocking operation
            (raw_percent * 0.9, ExecutionPhase::Reading)
        }
    }
}

/// Information about current progress
#[derive(Debug, Clone)]
pub struct ProgressInfo {
    pub current: usize,
    pub total: Option<usize>,
    pub unit: ProgressUnit,
    pub percent: Option<f64>,
    pub has_blocking_operators: bool,
    pub phase: ExecutionPhase,
}

/// Tracks which phase of execution we're in
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionPhase {
    /// Reading and processing data from sources
    Reading,
    /// Pipeline-breaking operation (sort, join, aggregate)
    Processing,
    /// Writing final output
    Finalizing,
}

/// Unit of measurement for progress
#[derive(Debug, Clone, Copy)]
pub enum ProgressUnit {
    Bytes,
    Rows,
}

impl ProgressUnit {
    pub fn format_value(&self, value: usize) -> String {
        match self {
            ProgressUnit::Bytes => format_bytes(value),
            ProgressUnit::Rows => format_number(value),
        }
    }
}

/// Format a byte count in human-readable form
fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

/// Format a number with appropriate separators
fn format_number(num: usize) -> String {
    let s = num.to_string();
    let mut result = String::new();

    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1073741824), "1.0 GB");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(100), "100");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1000000), "1,000,000");
        assert_eq!(format_number(1234567), "1,234,567");
    }
}
