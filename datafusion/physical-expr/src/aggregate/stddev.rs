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

use arrow::array::ArrayRef;

use datafusion_common::ScalarValue;
use datafusion_common::{internal_err, Result};
use datafusion_expr::Accumulator;

use crate::aggregate::stats::StatsType;
use crate::aggregate::variance::VarianceAccumulator;

/// An accumulator to compute the average
#[derive(Debug)]
pub struct StddevAccumulator {
    variance: VarianceAccumulator,
}

impl StddevAccumulator {
    /// Creates a new `StddevAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            variance: VarianceAccumulator::try_new(s_type)?,
        })
    }

    pub fn get_m2(&self) -> f64 {
        self.variance.get_m2()
    }
}

impl Accumulator for StddevAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.variance.get_count()),
            ScalarValue::from(self.variance.get_mean()),
            ScalarValue::from(self.variance.get_m2()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.update_batch(values)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.variance.retract_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.variance.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let variance = self.variance.evaluate()?;
        match variance {
            ScalarValue::Float64(e) => {
                if e.is_none() {
                    Ok(ScalarValue::Float64(None))
                } else {
                    Ok(ScalarValue::Float64(e.map(|f| f.sqrt())))
                }
            }
            _ => internal_err!("Variance should be f64"),
        }
    }

    fn size(&self) -> usize {
        std::mem::align_of_val(self) - std::mem::align_of_val(&self.variance)
            + self.variance.size()
    }
}
