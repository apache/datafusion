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

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Float64Array};
use arrow::compute::kernels::arity::binary;
use arrow::datatypes::{DataType, Float64Type};
use datafusion_common::Result;
use datafusion_common::utils::take_function_args;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Spark-compatible `hypot` function.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#hypot>
///
/// Returns `sqrt(expr1^2 + expr2^2)` computed without intermediate overflow or
/// underflow, matching Spark's use of `java.lang.Math.hypot`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkHypot {
    signature: Signature,
}

impl Default for SparkHypot {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkHypot {
    pub fn new() -> Self {
        Self {
            // Spark only defines hypot over doubles; `exact` makes coercion
            // guarantee both inputs are Float64 before `invoke` runs.
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkHypot {
    fn name(&self) -> &str {
        "hypot"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let [x, y] = take_function_args(self.name(), args.args)?;

        // Broadcast scalars to arrays so one path covers every combination.
        let x = x.to_array(num_rows)?;
        let y = y.to_array(num_rows)?;

        // Safe: the `exact` signature guarantees Float64 inputs.
        let x = x.as_primitive::<Float64Type>();
        let y = y.as_primitive::<Float64Type>();

        // `binary` applies the op element-wise and returns NULL when either
        // input is NULL — matching Spark's null semantics.
        let result: Float64Array = binary(x, y, |a, b| a.hypot(b))?;

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}
