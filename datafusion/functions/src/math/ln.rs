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

//! Math function: `ln()`.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::error::ArrowError;
use datafusion_common::{Result, exec_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use super::{bounds, get_ln_doc, ln_order};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LnFunc {
    signature: Signature,
}

impl Default for LnFunc {
    fn default() -> Self {
        LnFunc::new()
    }
}

impl LnFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Float64, Float32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LnFunc {
    fn name(&self) -> &str {
        "ln"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        ln_order(input)
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        bounds::unbounded_bounds(inputs)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => Arc::new(
                args[0]
                    .as_primitive::<Float64Type>()
                    .try_unary::<_, Float64Type, _>(ln_checked_f64)?,
            ) as ArrayRef,
            DataType::Float32 => Arc::new(
                args[0]
                    .as_primitive::<Float32Type>()
                    .try_unary::<_, Float32Type, _>(ln_checked_f32)?,
            ) as ArrayRef,
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                );
            }
        };

        Ok(ColumnarValue::Array(arr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_ln_doc())
    }
}

fn ln_checked_f64(value: f64) -> std::result::Result<f64, ArrowError> {
    if value < 0.0 {
        Err(ArrowError::ComputeError(
            "cannot take logarithm of a negative number".to_string(),
        ))
    } else {
        Ok(value.ln())
    }
}

fn ln_checked_f32(value: f32) -> std::result::Result<f32, ArrowError> {
    if value < 0.0 {
        Err(ArrowError::ComputeError(
            "cannot take logarithm of a negative number".to_string(),
        ))
    } else {
        Ok(value.ln())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ln_checked_f64_negative_errors() {
        let err = ln_checked_f64(-1.0).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Compute error: cannot take logarithm of a negative number"
        );
    }

    #[test]
    fn test_ln_checked_f64_zero_and_positive() {
        let zero = ln_checked_f64(0.0).unwrap();
        assert!(zero.is_infinite() && zero.is_sign_negative());

        let one = ln_checked_f64(1.0).unwrap();
        assert!(one.abs() < f64::EPSILON);
    }
}
