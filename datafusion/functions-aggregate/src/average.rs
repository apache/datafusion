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

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::sum;
use arrow::datatypes::{Float64Type, UInt64Type};
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::Volatility::Immutable;
use datafusion_expr::{Accumulator, AggregateUDFImpl, GroupsAccumulator, Signature};
use std::any::Any;

make_udaf_expr_and_func!(
    Average,
    avg,
    expression,
    "Returns the avg of a group of values.",
    avg_udaf
);

#[derive(Debug)]
pub struct Average {
    signature: Signature,
    aliases: Vec<String>,
}

impl Average {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, NUMERICS.to_vec(), Immutable),
            aliases: vec![String::from("mean")],
        }
    }
}

impl Default for Average {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for Average {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        todo!()
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        todo!()
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn GroupsAccumulator>> {
        todo!()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl Accumulator for AvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        // ignore all the null values from count
        self.count += (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Float64(
            self.sum.map(|f| f / self.count as f64),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::Float64(self.sum),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        // sum up count
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();
        // sum up sum
        if let Some(x) = sum(states[0].as_primitive::<Float64Type>()) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count -= (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            self.sum = Some(self.sum.unwrap() - x);
        }
        Ok(())
    }
}
