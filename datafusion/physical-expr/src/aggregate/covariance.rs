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

use arrow::array::Float64Array;
use arrow::{
    array::{ArrayRef, UInt64Array},
    compute::cast,
    datatypes::DataType,
};
use datafusion_common::{downcast_value, unwrap_or_internal_err, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;

use crate::aggregate::stats::StatsType;

/// An accumulator to compute covariance
/// The algrithm used is an online implementation and numerically stable. It is derived from the following paper
/// for calculating variance:
/// Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
/// Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.
///
/// The algorithm has been analyzed here:
/// Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
/// Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.
///
/// Though it is not covered in the original paper but is based on the same idea, as a result the algorithm is online,
/// parallelizable and numerically stable.

#[derive(Debug)]
pub struct CovarianceAccumulator {
    algo_const: f64,
    mean1: f64,
    mean2: f64,
    count: u64,
    stats_type: StatsType,
}

impl CovarianceAccumulator {
    /// Creates a new `CovarianceAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            algo_const: 0_f64,
            mean1: 0_f64,
            mean2: 0_f64,
            count: 0_u64,
            stats_type: s_type,
        })
    }

    pub fn get_count(&self) -> u64 {
        self.count
    }

    pub fn get_mean1(&self) -> f64 {
        self.mean1
    }

    pub fn get_mean2(&self) -> f64 {
        self.mean2
    }

    pub fn get_algo_const(&self) -> f64 {
        self.algo_const
    }
}

impl Accumulator for CovarianceAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.mean1),
            ScalarValue::from(self.mean2),
            ScalarValue::from(self.algo_const),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values1 = &cast(&values[0], &DataType::Float64)?;
        let values2 = &cast(&values[1], &DataType::Float64)?;

        let mut arr1 = downcast_value!(values1, Float64Array).iter().flatten();
        let mut arr2 = downcast_value!(values2, Float64Array).iter().flatten();

        for i in 0..values1.len() {
            let value1 = if values1.is_valid(i) {
                arr1.next()
            } else {
                None
            };
            let value2 = if values2.is_valid(i) {
                arr2.next()
            } else {
                None
            };

            if value1.is_none() || value2.is_none() {
                continue;
            }

            let value1 = unwrap_or_internal_err!(value1);
            let value2 = unwrap_or_internal_err!(value2);
            let new_count = self.count + 1;
            let delta1 = value1 - self.mean1;
            let new_mean1 = delta1 / new_count as f64 + self.mean1;
            let delta2 = value2 - self.mean2;
            let new_mean2 = delta2 / new_count as f64 + self.mean2;
            let new_c = delta1 * (value2 - new_mean2) + self.algo_const;

            self.count += 1;
            self.mean1 = new_mean1;
            self.mean2 = new_mean2;
            self.algo_const = new_c;
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values1 = &cast(&values[0], &DataType::Float64)?;
        let values2 = &cast(&values[1], &DataType::Float64)?;
        let mut arr1 = downcast_value!(values1, Float64Array).iter().flatten();
        let mut arr2 = downcast_value!(values2, Float64Array).iter().flatten();

        for i in 0..values1.len() {
            let value1 = if values1.is_valid(i) {
                arr1.next()
            } else {
                None
            };
            let value2 = if values2.is_valid(i) {
                arr2.next()
            } else {
                None
            };

            if value1.is_none() || value2.is_none() {
                continue;
            }

            let value1 = unwrap_or_internal_err!(value1);
            let value2 = unwrap_or_internal_err!(value2);

            let new_count = self.count - 1;
            let delta1 = self.mean1 - value1;
            let new_mean1 = delta1 / new_count as f64 + self.mean1;
            let delta2 = self.mean2 - value2;
            let new_mean2 = delta2 / new_count as f64 + self.mean2;
            let new_c = self.algo_const - delta1 * (new_mean2 - value2);

            self.count -= 1;
            self.mean1 = new_mean1;
            self.mean2 = new_mean2;
            self.algo_const = new_c;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        let means1 = downcast_value!(states[1], Float64Array);
        let means2 = downcast_value!(states[2], Float64Array);
        let cs = downcast_value!(states[3], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0_u64 {
                continue;
            }
            let new_count = self.count + c;
            let new_mean1 = self.mean1 * self.count as f64 / new_count as f64
                + means1.value(i) * c as f64 / new_count as f64;
            let new_mean2 = self.mean2 * self.count as f64 / new_count as f64
                + means2.value(i) * c as f64 / new_count as f64;
            let delta1 = self.mean1 - means1.value(i);
            let delta2 = self.mean2 - means2.value(i);
            let new_c = self.algo_const
                + cs.value(i)
                + delta1 * delta2 * self.count as f64 * c as f64 / new_count as f64;

            self.count = new_count;
            self.mean1 = new_mean1;
            self.mean2 = new_mean2;
            self.algo_const = new_c;
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

        if count == 0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.algo_const / count as f64)))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
