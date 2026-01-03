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

//! ETA estimation using alpha filter (exponential moving average)

use crate::progress::ProgressInfo;
use datafusion_common::instant::Instant;
use std::time::Duration;

/// Estimates time to completion based on progress using an alpha filter
/// (exponential moving average).
///
/// The alpha filter provides a good balance between responsiveness and
/// smoothness for ETA estimation in a TUI environment.
///
/// The smoothed rate is calculated as:
/// `smoothed_rate = alpha * new_rate + (1 - alpha) * previous_smoothed_rate`
///
/// Where alpha is the smoothing factor:
/// - Higher alpha (closer to 1.0): More responsive to changes, less smooth
/// - Lower alpha (closer to 0.0): Less responsive, more smooth
/// - Default value of 0.3 provides good balance
pub struct ProgressEstimator {
    start_time: Instant,
    // Smoothing factor (0.0 to 1.0)
    alpha: f64,
    // Smoothed progress rate (percent per second)
    smoothed_rate: Option<f64>,
    // Previous time for calculating rate
    last_time: Option<Duration>,
    // Previous progress percentage
    last_progress: Option<f64>,
    // Number of observations
    observations: usize,
}

impl ProgressEstimator {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            alpha: 0.3, // Good balance between responsiveness and smoothness
            smoothed_rate: None,
            last_time: None,
            last_progress: None,
            observations: 0,
        }
    }

    /// Update the estimator with new progress and return ETA
    pub fn update(&mut self, progress: ProgressInfo) -> Option<Duration> {
        let elapsed = self.start_time.elapsed();

        // Need at least some progress and time to estimate
        let percent = progress.percent?;
        if percent <= 0.0 || elapsed.as_secs_f64() < 1.0 {
            return None;
        }

        self.observations += 1;

        // Need at least two observations to calculate rate
        if let (Some(last_time), Some(last_progress)) =
            (self.last_time, self.last_progress)
        {
            let dt = (elapsed - last_time).as_secs_f64();
            if dt > 0.0 {
                let measured_rate = (percent - last_progress) / dt;

                // Apply exponential moving average
                match self.smoothed_rate {
                    Some(prev_rate) => {
                        self.smoothed_rate = Some(
                            self.alpha * measured_rate + (1.0 - self.alpha) * prev_rate,
                        );
                    }
                    None => {
                        // First rate measurement, use it directly
                        self.smoothed_rate = Some(measured_rate);
                    }
                }
            }
        }

        self.last_time = Some(elapsed);
        self.last_progress = Some(percent);

        // Don't provide estimate until we have enough observations and progress
        if self.observations < 2 || percent <= 5.0 {
            return None;
        }

        self.calculate_eta(percent)
    }

    /// Calculate ETA based on smoothed rate
    fn calculate_eta(&self, current_percent: f64) -> Option<Duration> {
        let remaining_percent = 100.0 - current_percent;

        // Use smoothed rate if available
        let current_rate = self.smoothed_rate?;

        if current_rate <= 0.0 {
            return None; // No progress or going backwards
        }

        // Simple linear projection: time = remaining_percent / rate
        let eta_seconds = remaining_percent / current_rate;

        // Cap the estimate at something reasonable (24 hours)
        if eta_seconds > 0.0 && eta_seconds < 86400.0 {
            Some(Duration::from_secs_f64(eta_seconds))
        } else {
            None
        }
    }
}

impl Default for ProgressEstimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_estimator_needs_observations() {
        let mut estimator = ProgressEstimator::new();

        // Wait a bit to ensure elapsed time > 1 second
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // First observation should return None (need at least 2 observations)
        let progress1 = ProgressInfo {
            current: 500,
            total: Some(10000),
            unit: crate::progress::ProgressUnit::Rows,
            percent: Some(5.0),
            has_blocking_operators: false,
            phase: crate::progress::ExecutionPhase::Reading,
        };
        assert!(estimator.update(progress1).is_none());

        // Second observation should provide estimate
        std::thread::sleep(std::time::Duration::from_millis(200));
        let progress2 = ProgressInfo {
            current: 1000,
            total: Some(10000),
            unit: crate::progress::ProgressUnit::Rows,
            percent: Some(10.0),
            has_blocking_operators: false,
            phase: crate::progress::ExecutionPhase::Reading,
        };
        let eta = estimator.update(progress2);
        assert!(eta.is_some());
    }

    #[test]
    fn test_progress_estimator_smoothing() {
        let mut estimator = ProgressEstimator::new();

        // Wait for elapsed time > 1 second
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // First measurement
        let progress1 = ProgressInfo {
            current: 1000,
            total: Some(10000),
            unit: crate::progress::ProgressUnit::Rows,
            percent: Some(10.0),
            has_blocking_operators: false,
            phase: crate::progress::ExecutionPhase::Reading,
        };
        estimator.update(progress1);

        // Second measurement with progress
        std::thread::sleep(std::time::Duration::from_millis(200));
        let progress2 = ProgressInfo {
            current: 3000,
            total: Some(10000),
            unit: crate::progress::ProgressUnit::Rows,
            percent: Some(30.0),
            has_blocking_operators: false,
            phase: crate::progress::ExecutionPhase::Reading,
        };
        let eta = estimator.update(progress2);
        assert!(eta.is_some());

        // ETA should be reasonable (not too long since we're making progress)
        let eta_secs = eta.unwrap().as_secs();
        assert!(eta_secs < 60); // Should be less than a minute at this rate
    }
}
