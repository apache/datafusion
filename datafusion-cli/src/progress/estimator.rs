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
    kalman_estimator: KalmanFilterEstimator,
}

impl ProgressEstimator {
    pub fn new(estimator_type: EstimatorType) -> Self {
        Self {
            estimator_type,
            start_time: Instant::now(),
            linear_estimator: LinearEstimator::new(),
            alpha_estimator: AlphaFilterEstimator::new(),
            kalman_estimator: KalmanFilterEstimator::new(),
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
            EstimatorType::Kalman => self.kalman_estimator.update(percent, elapsed),
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

/// Kalman filter for progress estimation inspired by DuckDB's implementation
///
/// This implements a simple 1D Kalman filter to estimate query completion time.
/// The filter tracks two state variables: progress position and progress velocity.
/// It provides more robust estimates than linear extrapolation by adapting to
/// changes in query execution rate.
struct KalmanFilterEstimator {
    // State: [position, velocity] - position is progress %, velocity is %/sec
    state: [f64; 2],
    // State covariance matrix (2x2, stored as [P11, P12, P21, P22])
    covariance: [f64; 4],
    // Process noise (how much we expect the velocity to change)
    process_noise: f64,
    // Measurement noise (uncertainty in progress measurements)
    measurement_noise: f64,
    // Previous time for calculating dt
    last_time: Option<Duration>,
    // Number of observations
    observations: usize,
}

impl KalmanFilterEstimator {
    fn new() -> Self {
        Self {
            state: [0.0, 0.0],                // [position, velocity]
            covariance: [1.0, 0.0, 0.0, 1.0], // Identity matrix
            process_noise: 0.1,               // Conservative estimate of velocity changes
            measurement_noise: 1.0,           // Measurement uncertainty
            last_time: None,
            observations: 0,
        }
    }

    fn update(&mut self, percent: f64, elapsed: Duration) -> Option<Duration> {
        self.observations += 1;

        // Need at least 2 observations to calculate velocity
        if self.observations < 2 {
            self.last_time = Some(elapsed);
            self.state[0] = percent;
            return None;
        }

        let dt = (elapsed - self.last_time.unwrap()).as_secs_f64();
        self.last_time = Some(elapsed);

        if dt <= 0.0 {
            return None;
        }

        // Predict step
        self.predict(dt);

        // Update step with measurement
        self.measure(percent);

        // Calculate ETA from current state
        self.calculate_eta()
    }

    /// Predict the next state using the motion model
    fn predict(&mut self, dt: f64) {
        // State transition: position = position + velocity * dt, velocity unchanged
        let new_position = self.state[0] + self.state[1] * dt;
        self.state[0] = new_position;
        // velocity remains the same: self.state[1] = self.state[1]

        // Update covariance matrix
        // F = [1, dt]  (state transition matrix)
        //     [0,  1]
        // P = F * P * F^T + Q
        let p11 = self.covariance[0];
        let p12 = self.covariance[1];
        let p21 = self.covariance[2];
        let p22 = self.covariance[3];

        let new_p11 = p11 + 2.0 * p12 * dt + p22 * dt * dt + self.process_noise;
        let new_p12 = p12 + p22 * dt;
        let new_p21 = p21 + p22 * dt;
        let new_p22 = p22 + self.process_noise;

        self.covariance = [new_p11, new_p12, new_p21, new_p22];
    }

    /// Update state with measurement
    fn measure(&mut self, measurement: f64) {
        // Measurement model: H = [1, 0] (we only measure position)
        let h = [1.0, 0.0];

        // Innovation: y = measurement - H * state
        let innovation = measurement - (h[0] * self.state[0] + h[1] * self.state[1]);

        // Innovation covariance: S = H * P * H^T + R
        let s = h[0] * (self.covariance[0] * h[0] + self.covariance[1] * h[1])
            + h[1] * (self.covariance[2] * h[0] + self.covariance[3] * h[1])
            + self.measurement_noise;

        if s.abs() < 1e-10 {
            return; // Avoid division by zero
        }

        // Kalman gain: K = P * H^T * S^(-1)
        let k1 = (self.covariance[0] * h[0] + self.covariance[1] * h[1]) / s;
        let k2 = (self.covariance[2] * h[0] + self.covariance[3] * h[1]) / s;

        // State update: state = state + K * innovation
        self.state[0] += k1 * innovation;
        self.state[1] += k2 * innovation;

        // Covariance update: P = (I - K * H) * P
        let new_p11 = self.covariance[0]
            - k1 * h[0] * self.covariance[0]
            - k1 * h[1] * self.covariance[2];
        let new_p12 = self.covariance[1]
            - k1 * h[0] * self.covariance[1]
            - k1 * h[1] * self.covariance[3];
        let new_p21 = self.covariance[2]
            - k2 * h[0] * self.covariance[0]
            - k2 * h[1] * self.covariance[2];
        let new_p22 = self.covariance[3]
            - k2 * h[0] * self.covariance[1]
            - k2 * h[1] * self.covariance[3];

        self.covariance = [new_p11, new_p12, new_p21, new_p22];
    }

    /// Calculate ETA from current state
    fn calculate_eta(&self) -> Option<Duration> {
        let current_progress = self.state[0];
        let velocity = self.state[1];

        // Constrain progress between 0 and 100
        let progress = current_progress.clamp(0.0, 100.0);

        // Need positive velocity to estimate completion
        if velocity <= 0.0 || progress >= 99.9 {
            return None;
        }

        let remaining_progress = 100.0 - progress;
        let eta_seconds = remaining_progress / velocity;

        // Cap estimates at 24 hours like DuckDB
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
    fn test_kalman_filter_estimator() {
        let mut estimator = KalmanFilterEstimator::new();

        // First update should return None (need at least 2 observations)
        assert!(estimator.update(5.0, Duration::from_secs(10)).is_none());

        // After second observation, should provide estimate
        let eta = estimator.update(10.0, Duration::from_secs(20));
        assert!(eta.is_some());

        // Should adapt to changing velocity
        let eta2 = estimator.update(30.0, Duration::from_secs(30));
        assert!(eta2.is_some());

        // Should handle completion gracefully
        let eta_final = estimator.update(99.5, Duration::from_secs(35));
        // At very high progress, may return None
        assert!(eta_final.is_none() || eta_final.unwrap().as_secs() < 10);
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
            has_blocking_operators: false,
            phase: crate::progress::ExecutionPhase::Reading,
        };

        let eta = estimator.update(progress);
        assert!(eta.is_some());
    }
}
