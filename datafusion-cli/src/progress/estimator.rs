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

//! ETA estimation algorithms

use crate::progress::{ProgressEstimator as EstimatorType, ProgressInfo};
use datafusion_common::instant::Instant;
use std::time::Duration;

/// Estimates time to completion based on progress
pub struct ProgressEstimator {
    estimator_type: EstimatorType,
    start_time: Instant,
    linear_estimator: LinearEstimator,
    alpha_estimator: AlphaFilterEstimator,
}

impl ProgressEstimator {
    pub fn new(estimator_type: EstimatorType) -> Self {
        Self {
            estimator_type,
            start_time: Instant::now(),
            linear_estimator: LinearEstimator::new(),
            alpha_estimator: AlphaFilterEstimator::new(),
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

        match self.estimator_type {
            EstimatorType::Linear => self.linear_estimator.update(percent, elapsed),
            EstimatorType::Alpha => self.alpha_estimator.update(percent, elapsed),
        }
    }
}

/// Simple linear ETA estimation
struct LinearEstimator {
    last_update: Option<(f64, Duration)>,
}

impl LinearEstimator {
    fn new() -> Self {
        Self { last_update: None }
    }

    fn update(&mut self, percent: f64, elapsed: Duration) -> Option<Duration> {
        // Store this update for next calculation
        self.last_update = Some((percent, elapsed));

        if percent <= 5.0 {
            // Too early to provide reliable estimate
            return None;
        }

        // Simple linear extrapolation: if we're X% done in Y time,
        // total time = Y * 100 / X
        let total_time_secs = elapsed.as_secs_f64() * 100.0 / percent;
        let remaining_secs = total_time_secs - elapsed.as_secs_f64();

        if remaining_secs > 0.0 {
            Some(Duration::from_secs_f64(remaining_secs))
        } else {
            Some(Duration::from_secs(0))
        }
    }
}

/// Alpha filter (exponential moving average) for smoother ETA predictions
///
/// This implementation uses a simple exponential moving average to smooth
/// progress rate measurements. The alpha filter provides a good balance between
/// responsiveness and smoothness for ETA estimation in a TUI environment.
///
/// The smoothed rate is calculated as:
/// smoothed_rate = alpha * new_rate + (1 - alpha) * previous_smoothed_rate
///
/// Where alpha is the smoothing factor:
/// - Higher alpha (closer to 1.0): More responsive to changes, less smooth
/// - Lower alpha (closer to 0.0): Less responsive, more smooth
/// - Typical range: 0.1-0.5, with 0.3 providing good balance
struct AlphaFilterEstimator {
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

impl AlphaFilterEstimator {
    fn new() -> Self {
        Self {
            alpha: 0.3, // Good balance between responsiveness and smoothness
            smoothed_rate: None,
            last_time: None,
            last_progress: None,
            observations: 0,
        }
    }

    fn update(&mut self, percent: f64, elapsed: Duration) -> Option<Duration> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_estimator() {
        let mut estimator = LinearEstimator::new();

        // Too early to estimate
        let eta = estimator.update(2.0, Duration::from_secs(10));
        assert!(eta.is_none());

        // Should provide estimate after 5%
        let eta = estimator.update(10.0, Duration::from_secs(20));
        assert!(eta.is_some());

        // At 10% in 20 seconds, should estimate ~180 seconds remaining
        let eta_secs = eta.unwrap().as_secs();
        assert!((170..=190).contains(&eta_secs));
    }

    #[test]
    fn test_alpha_filter_estimator() {
        let mut estimator = AlphaFilterEstimator::new();

        // First update should return None (need at least 2 observations)
        assert!(estimator.update(5.0, Duration::from_secs(10)).is_none());

        // After second observation, should provide estimate
        let eta = estimator.update(10.0, Duration::from_secs(20));
        assert!(eta.is_some());
    }

    #[test]
    fn test_progress_estimator() {
        let mut estimator = ProgressEstimator::new(EstimatorType::Linear);

        // Wait a bit to ensure elapsed time > 1 second
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let progress = ProgressInfo {
            current: 1000,
            total: Some(10000),
            unit: crate::progress::ProgressUnit::Rows,
            percent: Some(10.0),
        };

        let eta = estimator.update(progress);
        assert!(eta.is_some());
    }
}
