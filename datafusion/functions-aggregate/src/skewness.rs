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

use arrow::array::{ArrayRef, AsArray, Float64Array, UInt64Array};
use arrow::datatypes::Float64Type;
use arrow_schema::{DataType, Field};
use datafusion_common::{downcast_value, not_impl_err, DataFusionError, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_functions_aggregate_common::accumulator::{
    AccumulatorArgs, StateFieldsArgs,
};
use std::any::Any;
use std::ops::{Mul, Sub};

make_udaf_expr_and_func!(
    SkewnessFunc,
    skewness,
    x,
    "Calculates the excess skewness.",
    skewness_udaf
);

#[derive(Debug)]
struct SkewnessFunc {
    name: String,
    signature: Signature,
}

impl Default for SkewnessFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SkewnessFunc {
    pub fn new() -> Self {
        Self {
            name: "skewness".to_string(),
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SkewnessFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion_common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!("DISTINCT is not implemented for skewness");
        }
        Ok(Box::new(SkewnessAccumulator::new()))
    }

    fn state_fields(
        &self,
        _args: StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        Ok(vec![
            Field::new("count", DataType::UInt64, true),
            Field::new("sum", DataType::Float64, true),
            Field::new("sum_sqr", DataType::Float64, true),
            Field::new("sum_cub", DataType::Float64, true),
        ])
    }

    fn coerce_types(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion_common::Result<Vec<DataType>> {
        Ok(vec![DataType::Float64])
    }
}

/// Accumulator for calculating the excess skewness
/// This implementation follows the [DuckDB implementation] :
/// <https://github.com/duckdb/duckdb/blob/main/src/core_functions/aggregate/distributive/skew.cpp>
#[derive(Debug)]
struct SkewnessAccumulator {
    count: u64,
    sum: f64,
    sum_sqr: f64,
    sum_cub: f64,
}

impl SkewnessAccumulator {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0f64,
            sum_sqr: 0f64,
            sum_cub: 0f64,
        }
    }
}

impl Accumulator for SkewnessAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let array = values[0].as_primitive::<Float64Type>();
        for val in array.iter().flatten() {
            self.count += 1;
            self.sum += val;
            self.sum_sqr = val.powi(2);
            self.sum_cub = val.powi(3);
        }
        Ok(())
    }
    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        if self.count <= 2 {
            return Ok(ScalarValue::Float64(None));
        }
        let count = self.count as f64;
        let t1 = 1f64 / self.count as f64;
        let mut p = (t1 * (self.sum_sqr - self.sum * self.sum * t1)).powi(3);
        if p < 0f64 {
            p = 0f64;
        }
        let div = p.sqrt();
        if div == 0f64 {
            return Ok(ScalarValue::Float64(None));
        }
        let t2 = (count.mul(count.sub(1f64))) / (count.sub(2f64));
        let res = t2
            * t1
            * (self.sum_cub - 3f64 * self.sum_sqr * self.sum * t1
                + 2f64 * self.sum.powi(3) * t1 * t1)
            / div;
        Ok(ScalarValue::Float64(Some(res)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::from(self.sum),
            ScalarValue::from(self.sum_sqr),
            ScalarValue::from(self.sum_cub),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        let sums = downcast_value!(states[1], Float64Array);
        let sum_sqrs = downcast_value!(states[2], Float64Array);
        let sum_cubs = downcast_value!(states[3], Float64Array);

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0 {
                continue;
            }
            self.count += c;
            self.sum += sums.value(i);
            self.sum_sqr += sum_sqrs.value(i);
            self.sum_cub += sum_cubs.value(i);
        }
        Ok(())
    }
}
