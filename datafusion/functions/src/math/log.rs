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

//! Math function: `log()`.

use arrow::datatypes::DataType;
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{ColumnarValue, Expr, FuncMonotonicity, ScalarFunctionDefinition};

use arrow::array::{ArrayRef, Float32Array, Float64Array};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

use super::power::PowerFunc;

#[derive(Debug)]
pub struct LogFunc {
    signature: Signature,
}

impl Default for LogFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LogFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Float32]),
                    Exact(vec![Float64]),
                    Exact(vec![Float32, Float32]),
                    Exact(vec![Float64, Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LogFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "log"
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

    fn monotonicity(&self) -> Result<Option<FuncMonotonicity>> {
        Ok(Some(vec![Some(true), Some(false)]))
    }

    // Support overloaded log(base, x) and log(x) which defaults to log(10, x)
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        let mut base = ColumnarValue::Scalar(ScalarValue::Float32(Some(10.0)));

        let mut x = &args[0];
        if args.len() == 2 {
            x = &args[1];
            base = ColumnarValue::Array(args[0].clone());
        }
        // note in f64::log params order is different than in sql. e.g in sql log(base, x) == f64::log(x, base)
        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => match base {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => {
                    Arc::new(make_function_scalar_inputs!(x, "x", Float64Array, {
                        |value: f64| f64::log(value, base as f64)
                    }))
                }
                ColumnarValue::Array(base) => Arc::new(make_function_inputs2!(
                    x,
                    base,
                    "x",
                    "base",
                    Float64Array,
                    { f64::log }
                )),
                _ => {
                    return exec_err!("log function requires a scalar or array for base")
                }
            },

            DataType::Float32 => match base {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(base))) => {
                    Arc::new(make_function_scalar_inputs!(x, "x", Float32Array, {
                        |value: f32| f32::log(value, base)
                    }))
                }
                ColumnarValue::Array(base) => Arc::new(make_function_inputs2!(
                    x,
                    base,
                    "x",
                    "base",
                    Float32Array,
                    { f32::log }
                )),
                _ => {
                    return exec_err!("log function requires a scalar or array for base")
                }
            },
            other => {
                return exec_err!("Unsupported data type {other:?} for function log")
            }
        };

        Ok(ColumnarValue::Array(arr))
    }

    /// Simplify the `log` function by the relevant rules:
    /// 1. Log(a, 1) ===> 0
    /// 2. Log(a, Power(a, b)) ===> b
    /// 3. Log(a, a) ===> 1
    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let mut number = &args[0];
        let mut base =
            &Expr::Literal(ScalarValue::new_ten(&info.get_data_type(number)?)?);
        if args.len() == 2 {
            base = &args[0];
            number = &args[1];
        }

        match number {
            Expr::Literal(value)
                if value == &ScalarValue::new_one(&info.get_data_type(number)?)? =>
            {
                Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::new_zero(&info.get_data_type(base)?)?,
                )))
            }
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(fun),
                args,
            }) if base == &args[0]
                && fun
                    .as_ref()
                    .inner()
                    .as_any()
                    .downcast_ref::<PowerFunc>()
                    .is_some() =>
            {
                Ok(ExprSimplifyResult::Simplified(args[1].clone()))
            }
            _ => {
                if number == base {
                    Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                        ScalarValue::new_one(&info.get_data_type(number)?)?,
                    )))
                } else {
                    Ok(ExprSimplifyResult::Original(args))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::cast::{as_float32_array, as_float64_array};

    use super::*;

    #[test]
    fn test_log_f64() {
        let args = [
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![2.0, 2.0, 3.0, 5.0]))), // base
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                8.0, 4.0, 81.0, 625.0,
            ]))), // num
        ];

        let result = LogFunc::new()
            .invoke(&args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 4);
                assert_eq!(floats.value(0), 3.0);
                assert_eq!(floats.value(1), 2.0);
                assert_eq!(floats.value(2), 4.0);
                assert_eq!(floats.value(3), 4.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f32() {
        let args = [
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![2.0, 2.0, 3.0, 5.0]))), // base
            ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                8.0, 4.0, 81.0, 625.0,
            ]))), // num
        ];

        let result = LogFunc::new()
            .invoke(&args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 4);
                assert_eq!(floats.value(0), 3.0);
                assert_eq!(floats.value(1), 2.0);
                assert_eq!(floats.value(2), 4.0);
                assert_eq!(floats.value(3), 4.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }
}
