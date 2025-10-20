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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Decimal128, Float32, Float64, Int64};
use datafusion_common::{exec_err, Result};
use datafusion_expr::Signature;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use datafusion_functions::utils::make_scalar_function;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#ceil>
/// Difference between spark: There is no second optional argument to control the rounding behaviour.
/// Takes an Int64/Float32/Float64 input and returns the smallest number after rounding up that is
/// not smaller than the input.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCeil {
    signature: Signature,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![Float32, Float64, Int64, Decimal128(19, 10)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_ceil, vec![])(&args.args)
    }
}
fn spark_ceil(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("ceil expects exactly 1 argument, got {}", args.len());
    }

    let array: &dyn Array = args[0].as_ref();
    match args[0].data_type() {
        Float32 => {
            let array = array
                .as_primitive::<arrow::datatypes::Float32Type>()
                .unary::<_, arrow::datatypes::Int64Type>(|value: f32| {
                value.ceil() as i64
            });
            Ok(Arc::new(array))
        }
        Float64 => {
            let array = array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .unary::<_, arrow::datatypes::Int64Type>(|value: f64| {
                value.ceil() as i64
            });
            Ok(Arc::new(array))
        }
        Int64 => Ok(Arc::clone(&args[0])),

        Decimal128(_, scale) if *scale > 0 => {
            let decimal_array = array.as_primitive::<arrow::datatypes::Decimal128Type>();
            let div = 10_i128.pow_wrapping(*scale as u32);
            let result_array =
                decimal_array.unary::<_, arrow::datatypes::Int64Type>(|value: i128| {
                    div_ceil(value, div) as i64
                });
            Ok(Arc::new(result_array))
        }
        _ => {
            exec_err!(
                "ceil expects a numeric argument, got {}",
                args[0].data_type()
            )
        }
    }
}

// Helper function to calculate the ceil for Decimals
#[inline]
fn div_ceil(a: i128, b: i128) -> i128 {
    if b == 0 {
        panic!("division by zero");
    }
    let div = a / b;
    let rem = a % b;
    if (rem != 0) && ((b > 0 && a > 0) || (b < 0 && a < 0)) {
        div + 1
    } else {
        div
    }
}
