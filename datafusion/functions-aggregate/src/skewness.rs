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

use std::any::Any;
use arrow::array::ArrayRef;
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion_functions_aggregate_common::accumulator::AccumulatorArgs;

#[derive(Debug)]
struct SkewnessFunc {
    name: String,
    signature: Signature
}

impl SkewnessFunc {
    pub fn new() -> Self {
        Self {
            name: "skewness".to_string(),
            signature: Signature::any(1, Volatility::Immutable)
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

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> datafusion_common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(SkewnessAccumulator::new()))
    }
}

#[derive(Debug)]
struct SkewnessAccumulator {
    size: usize,
    sum: f64,
    sum_sqr: f64,
    sum_cub: f64
}

impl SkewnessAccumulator {
    fn new() -> Self {
        Self {
            size: 0,
            sum: 0f64,
            sum_sqr: 0f64,
            sum_cub: 0f64,
        }
    }
}

impl Accumulator for SkewnessAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        unimplemented!()
    }
    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        unimplemented!()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        unimplemented!()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        unimplemented!()
    }
}