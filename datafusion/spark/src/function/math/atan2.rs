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
use datafusion_functions::utils::make_scalar_function;

/// Spark-compatible `atan2` function.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#atan2>
///
/// `atan2(exprY, exprX)` returns the angle in radians between the positive
/// x-axis and the point given by the coordinates (exprX, exprY).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAtan2 {
    signature: Signature,
}

impl Default for SparkAtan2 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAtan2 {
    pub fn new() -> Self {
        Self {
            // Spark only defines atan2 over doubles
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkAtan2 {
    fn name(&self) -> &str {
        "atan2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_atan2, vec![])(&args.args)
    }
}

fn spark_atan2(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Spark arg order is atan2(exprY, exprX); Rust computes y.atan2(x).
    let [y, x] = take_function_args("atan2", args)?;
    let y = y.as_primitive::<Float64Type>();
    let x = x.as_primitive::<Float64Type>();
    let result: Float64Array = binary(y, x, |y, x| y.atan2(x))?;
    Ok(Arc::new(result))
}
