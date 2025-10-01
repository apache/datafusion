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
use std::time::{Duration, Instant};

/// Estimates time to completion based on progress
pub struct ProgressEstimator {
    estimator_type: EstimatorType,
    start_time: Instant,
    linear_estimator: LinearEstimator,
    kalman_estimator: KalmanEstimator,
}

impl ProgressEstimator {
    pub fn new(estimator_type: EstimatorType) -> Self {
        Self {
            estimator_type,
            start_time: Instant::now(),
            linear_estimator: LinearEstimator::new(),
            kalman_estimator: KalmanEstimator::new(),
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

/// Kalman filter ETA estimation for smoother predictions
struct KalmanEstimator {
    // State: [progress_rate, acceleration]
    state: [f64; 2],
    // Covariance matrix (2x2, stored as [P00, P01, P10, P11])
    covariance: [f64; 4],
    // Process noise
    process_noise: f64,
    // Measurement noise  
    measurement_noise: f64,
    // Previous time for calculating dt
    last_time: Option<Duration>,
    // Previous progress
    last_progress: Option<f64>,
    // Number of observations
    observations: usize,
}

impl KalmanEstimator {
    fn new() -> Self {
        Self {
            state: [0.0, 0.0], // Initial rate = 0, acceleration = 0
            covariance: [1.0, 0.0, 0.0, 1.0], // Identity matrix
            process_noise: 0.1,
            measurement_noise: 1.0,
            last_time: None,
            last_progress: None,
            observations: 0,
        }
    }

    fn update(&mut self, percent: f64, elapsed: Duration) -> Option<Duration> {
        self.observations += 1;

        // Need at least two observations to calculate rate
        if let (Some(last_time), Some(last_progress)) = (self.last_time, self.last_progress) {
            let dt = (elapsed - last_time).as_secs_f64();
            if dt > 0.0 {
                let measured_rate = (percent - last_progress) / dt;
                self.kalman_update(measured_rate, dt);
            }
        }

        self.last_time = Some(elapsed);
        self.last_progress = Some(percent);

        // Don't provide estimate until we have enough observations and progress
        if self.observations < 3 || percent <= 5.0 {
            return None;
        }

        self.calculate_eta(percent)
    }

    /// Kalman filter prediction step
    fn predict(&mut self, dt: f64) {
        // State transition: progress_rate(k+1) = progress_rate(k) + acceleration(k) * dt
        //                  acceleration(k+1) = acceleration(k)
        
        // Update state
        self.state[0] += self.state[1] * dt; // rate += acceleration * dt
        // acceleration stays the same
        
        // Update covariance with process noise
        // F = [[1, dt], [0, 1]] (state transition matrix)
        // P = F * P * F^T + Q
        
        let p00 = self.covariance[0];
        let p01 = self.covariance[1]; 
        let p10 = self.covariance[2];
        let p11 = self.covariance[3];
        
        self.covariance[0] = p00 + 2.0 * dt * p01 + dt * dt * p11 + self.process_noise;
        self.covariance[1] = p01 + dt * p11;
        self.covariance[2] = p10 + dt * p11;
        self.covariance[3] = p11 + self.process_noise;
    }

    /// Kalman filter update step  
    fn kalman_update(&mut self, measured_rate: f64, dt: f64) {
        // Prediction step
        self.predict(dt);
        
        // Measurement update
        // H = [1, 0] (we measure the rate directly)
        // y = measured_rate - predicted_rate (innovation)
        let innovation = measured_rate - self.state[0];
        
        // S = H * P * H^T + R (innovation covariance)
        let innovation_covariance = self.covariance[0] + self.measurement_noise;
        
        if innovation_covariance > 1e-9 {
            // K = P * H^T * S^-1 (Kalman gain)
            let kalman_gain = [
                self.covariance[0] / innovation_covariance,
                self.covariance[2] / innovation_covariance,
            ];
            
            // Update state: x = x + K * y
            self.state[0] += kalman_gain[0] * innovation;
            self.state[1] += kalman_gain[1] * innovation;
            
            // Update covariance: P = (I - K * H) * P
            let p00 = self.covariance[0];
            let p01 = self.covariance[1];
            let p10 = self.covariance[2]; 
            let p11 = self.covariance[3];
            
            self.covariance[0] = (1.0 - kalman_gain[0]) * p00;
            self.covariance[1] = (1.0 - kalman_gain[0]) * p01;
            self.covariance[2] = p10 - kalman_gain[1] * p00;
            self.covariance[3] = p11 - kalman_gain[1] * p01;
        }
    }

    /// Calculate ETA based on current state
    fn calculate_eta(&self, current_percent: f64) -> Option<Duration> {
        let remaining_percent = 100.0 - current_percent;
        let current_rate = self.state[0];
        
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
        assert!(eta_secs >= 170 && eta_secs <= 190);
    }

    #[test]
    fn test_kalman_estimator() {
        let mut estimator = KalmanEstimator::new();
        
        // First few updates should return None
        assert!(estimator.update(5.0, Duration::from_secs(10)).is_none());
        assert!(estimator.update(10.0, Duration::from_secs(20)).is_none());
        
        // After enough observations, should provide estimate
        let eta = estimator.update(15.0, Duration::from_secs(30));
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