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

//! Defines physical expressions that can evaluated at runtime during query execution

use crate::aggregate::stats::StatsType;
use arrow::array::Float64Array;
use arrow::{
    array::{ArrayRef, UInt64Array},
    compute::cast,
    datatypes::DataType,
};
use datafusion_common::downcast_value;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;

// TODO only holds the definition of `VarianceAccumulator` for use by `StddevAccumulator` in `physical-expr`,
// which in turn only has it there for legacy `CorrelationAccumulator`, but this whole file should go away
// once the latter is moved to `functions-aggregate`.

/// An accumulator to compute variance
/// The algrithm used is an online implementation and numerically stable. It is based on this paper:
/// Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
/// Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.
///
/// The algorithm has been analyzed here:
/// Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
/// Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.

#[derive(Debug)]
pub struct VarianceAccumulator {
    m2: f64,
    mean: f64,
    count: u64,
    stats_type: StatsType,
}

impl VarianceAccumulator {
    /// Creates a new `VarianceAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            m2: 0_f64,
            mean: 0_f64,
            count: 0_u64,
            stats_type: s_type,
        })
    }

    pub fn get_count(&self) -> u64 {
        self.count
    }

    pub fn get_mean(&self) -> f64 {
        self.mean
    }

    pub fn get_m2(&self) -> f64 {
        self.m2
    }
}

impl Accumulator for VarianceAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.mean),
            ScalarValue::from(self.m2),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &cast(&values[0], &DataType::Float64)?;
        let arr = downcast_value!(values, Float64Array).iter().flatten();

        for value in arr {
            let new_count = self.count + 1;
            let delta1 = value - self.mean;
            let new_mean = delta1 / new_count as f64 + self.mean;
            let delta2 = value - new_mean;
            let new_m2 = self.m2 + delta1 * delta2;

            self.count += 1;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &cast(&values[0], &DataType::Float64)?;
        let arr = downcast_value!(values, Float64Array).iter().flatten();

        for value in arr {
            let new_count = self.count - 1;
            let delta1 = self.mean - value;
            let new_mean = delta1 / new_count as f64 + self.mean;
            let delta2 = new_mean - value;
            let new_m2 = self.m2 - delta1 * delta2;

            self.count -= 1;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        let means = downcast_value!(states[1], Float64Array);
        let m2s = downcast_value!(states[2], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0_u64 {
                continue;
            }
            let new_count = self.count + c;
            let new_mean = self.mean * self.count as f64 / new_count as f64
                + means.value(i) * c as f64 / new_count as f64;
            let delta = self.mean - means.value(i);
            let new_m2 = self.m2
                + m2s.value(i)
                + delta * delta * self.count as f64 * c as f64 / new_count as f64;

            self.count = new_count;
            self.mean = new_mean;
            self.m2 = new_m2;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let count = match self.stats_type {
            StatsType::Population => self.count,
            StatsType::Sample => {
                if self.count > 0 {
                    self.count - 1
                } else {
                    self.count
                }
            }
        };

        Ok(ScalarValue::Float64(match self.count {
            0 => None,
            1 => {
                if let StatsType::Population = self.stats_type {
                    Some(0.0)
                } else {
                    None
                }
            }
            _ => Some(self.m2 / count as f64),
        }))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
