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

use arrow::array::{Array, ArrayRef, Float64Array, UInt64Array};
use arrow_schema::{DataType, Field};
use datafusion_common::cast::as_float64_array;
use datafusion_common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_functions_aggregate_common::accumulator::{
    AccumulatorArgs, StateFieldsArgs,
};
use std::any::Any;
use std::fmt::Debug;

make_udaf_expr_and_func!(
    KurtosisPopFunction,
    kurtosis_pop,
    x,
    "Calculates the excess kurtosis (Fisher’s definition) without bias correction.",
    kurtosis_pop_udaf
);

pub struct KurtosisPopFunction {
    signature: Signature,
}

impl Debug for KurtosisPopFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KurtosisPopFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for KurtosisPopFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl KurtosisPopFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for KurtosisPopFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kurtosis_pop"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new("count", DataType::UInt64, true),
            Field::new("sum", DataType::Float64, true),
            Field::new("sum_sqr", DataType::Float64, true),
            Field::new("sum_cub", DataType::Float64, true),
            Field::new("sum_four", DataType::Float64, true),
        ])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(KurtosisPopAccumulator::new()))
    }
}

/// Accumulator for calculating the excess kurtosis (Fisher’s definition) without bias correction.
/// This implementation follows the [DuckDB implementation]:
/// <https://github.com/duckdb/duckdb/blob/main/src/core_functions/aggregate/distributive/kurtosis.cpp>
#[derive(Debug, Default)]
pub struct KurtosisPopAccumulator {
    count: u64,
    sum: f64,
    sum_sqr: f64,
    sum_cub: f64,
    sum_four: f64,
}

impl KurtosisPopAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sqr: 0.0,
            sum_cub: 0.0,
            sum_four: 0.0,
        }
    }
}

impl Accumulator for KurtosisPopAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = as_float64_array(&values[0])?;
        for value in array.iter().flatten() {
            self.count += 1;
            self.sum += value;
            self.sum_sqr += value.powi(2);
            self.sum_cub += value.powi(3);
            self.sum_four += value.powi(4);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        let sums = downcast_value!(states[1], Float64Array);
        let sum_sqrs = downcast_value!(states[2], Float64Array);
        let sum_cubs = downcast_value!(states[3], Float64Array);
        let sum_fours = downcast_value!(states[4], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0 {
                continue;
            }
            self.count += c;
            self.sum += sums.value(i);
            self.sum_sqr += sum_sqrs.value(i);
            self.sum_cub += sum_cubs.value(i);
            self.sum_four += sum_fours.value(i);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count < 1 {
            return Ok(ScalarValue::Float64(None));
        }

        let count_64 = 1_f64 / self.count as f64;
        let m4 = count_64
            * (self.sum_four - 4.0 * self.sum_cub * self.sum * count_64
                + 6.0 * self.sum_sqr * self.sum.powi(2) * count_64.powi(2)
                - 3.0 * self.sum.powi(4) * count_64.powi(3));

        let m2 = (self.sum_sqr - self.sum.powi(2) * count_64) * count_64;
        if m2 <= 0.0 {
            return Ok(ScalarValue::Float64(None));
        }

        let target = m4 / (m2.powi(2)) - 3.0;
        Ok(ScalarValue::Float64(Some(target)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.sum),
            ScalarValue::from(self.sum_sqr),
            ScalarValue::from(self.sum_cub),
            ScalarValue::from(self.sum_four),
        ])
    }
}
