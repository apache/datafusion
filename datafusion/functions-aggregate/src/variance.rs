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

//! [`VarianceSample`]: variance sample aggregations.
//! [`VariancePopulation`]: variance population aggregations.

use std::fmt::Debug;

use arrow::{
    array::{ArrayRef, Float64Array, UInt64Array},
    compute::kernels::cast,
    datatypes::{DataType, Field},
};

use datafusion_common::{
    downcast_value, not_impl_err, plan_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    utils::format_state_name,
    Accumulator, AggregateUDFImpl, Signature, Volatility,
};
use datafusion_functions_aggregate_common::stats::StatsType;

make_udaf_expr_and_func!(
    VarianceSample,
    var_sample,
    expression,
    "Computes the sample variance.",
    var_samp_udaf
);

make_udaf_expr_and_func!(
    VariancePopulation,
    var_pop,
    expression,
    "Computes the population variance.",
    var_pop_udaf
);

pub struct VarianceSample {
    signature: Signature,
    aliases: Vec<String>,
}

impl Debug for VarianceSample {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("VarianceSample")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for VarianceSample {
    fn default() -> Self {
        Self::new()
    }
}

impl VarianceSample {
    pub fn new() -> Self {
        Self {
            aliases: vec![String::from("var_sample"), String::from("var_samp")],
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for VarianceSample {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "var"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("Variance requires numeric input types");
        }

        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let name = args.name;
        Ok(vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2"), DataType::Float64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("VAR(DISTINCT) aggregations are not available");
        }

        Ok(Box::new(VarianceAccumulator::try_new(StatsType::Sample)?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub struct VariancePopulation {
    signature: Signature,
    aliases: Vec<String>,
}

impl Debug for VariancePopulation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("VariancePopulation")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for VariancePopulation {
    fn default() -> Self {
        Self::new()
    }
}

impl VariancePopulation {
    pub fn new() -> Self {
        Self {
            aliases: vec![String::from("var_population")],
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for VariancePopulation {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "var_pop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("Variance requires numeric input types");
        }

        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let name = args.name;
        Ok(vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2"), DataType::Float64, true),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("VAR_POP(DISTINCT) aggregations are not available");
        }

        Ok(Box::new(VarianceAccumulator::try_new(
            StatsType::Population,
        )?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// An accumulator to compute variance
/// The algorithm used is an online implementation and numerically stable. It is based on this paper:
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

    fn supports_retract_batch(&self) -> bool {
        true
    }
}
