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

//! Configuration for progress reporting

use clap::ValueEnum;
use is_terminal::IsTerminal;
use std::io;

/// Configuration for progress reporting
#[derive(Debug, Clone)]
pub struct ProgressConfig {
    pub mode: ProgressMode,
    pub style: ProgressStyle,
    pub interval_ms: u64,
    pub estimator: ProgressEstimator,
}

impl ProgressConfig {
    /// Create a new progress configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Check whether progress should be shown based on the configuration and environment
    pub fn should_show_progress(&self) -> bool {
        match self.mode {
            ProgressMode::On => true,
            ProgressMode::Off => false,
            ProgressMode::Auto => io::stdout().is_terminal(),
        }
    }
}

impl Default for ProgressConfig {
    fn default() -> Self {
        Self {
            mode: ProgressMode::Auto,
            style: ProgressStyle::Bar,
            interval_ms: 200,
            estimator: ProgressEstimator::Alpha,
        }
    }
}

/// Progress bar display mode
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ProgressMode {
    /// Show progress bar on TTY, off otherwise
    Auto,
    /// Always show progress bar
    On,
    /// Never show progress bar
    Off,
}

impl Default for ProgressMode {
    fn default() -> Self {
        Self::Auto
    }
}

/// Progress bar visual style
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ProgressStyle {
    /// Show a progress bar when percent is known
    Bar,
    /// Show a spinner with counters
    Spinner,
}

impl Default for ProgressStyle {
    fn default() -> Self {
        Self::Bar
    }
}

/// ETA estimation algorithm
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ProgressEstimator {
    /// Simple linear estimation based on current progress rate
    Linear,
    /// Alpha filter (exponential moving average) - recommended for most use cases
    /// Provides smooth ETA estimates with good responsiveness to progress changes
    Alpha,
    /// Kalman filter-based estimation (DuckDB-inspired) - advanced mathematical modeling
    /// Uses state estimation to predict completion time, best for variable workloads
    Kalman,
}

impl Default for ProgressEstimator {
    fn default() -> Self {
        Self::Alpha
    }
}
