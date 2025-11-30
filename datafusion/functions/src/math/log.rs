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

use std::any::Any;

use super::power::PowerFunc;

use crate::utils::{calculate_binary_math, decimal128_to_i128, decimal32_to_f64};
use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::cast;
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Decimal32Type, Float16Type, Float32Type,
    Float64Type,
};
use arrow::error::ArrowError;
use arrow_buffer::i256;
use datafusion_common::types::NativeType;
use datafusion_common::{
    exec_err, internal_err, plan_datafusion_err, plan_err, Result, ScalarValue,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    lit, Coercion, ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF,
    TypeSignature, TypeSignatureClass,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use num_traits::Float;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the base-x logarithm of a number. Can either provide a specified base, or if omitted then takes the base-10 of a number.",
    syntax_example = r#"log(base, numeric_expression)
log(numeric_expression)"#,
    sql_example = r#"```sql
> SELECT log(10);
+---------+
| log(10) |
+---------+
| 1.0     |
+---------+
```"#,
    standard_argument(name = "base", prefix = "Base numeric"),
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
        // Converts decimals & integers to float64, accepting other floats as is
        let as_float = Coercion::new_implicit(
            TypeSignatureClass::Float,
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::one_of(
                // Ensure decimals have precedence over floats since we have
                // a native decimal implementation for log
                vec![
                    // log(value)
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Decimal,
                    )]),
                    TypeSignature::Coercible(vec![as_float.clone()]),
                    // log(base, value)
                    TypeSignature::Coercible(vec![
                        as_float.clone(),
                        Coercion::new_exact(TypeSignatureClass::Decimal),
                    ]),
                    TypeSignature::Coercible(vec![as_float.clone(), as_float.clone()]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

/// Binary function to calculate logarithm of Decimal32 `value` using `base` base
/// Returns error if base is invalid
fn log_decimal32(value: i32, scale: i8, base: f64) -> Result<f64, ArrowError> {
    if !base.is_finite() || base.trunc() != base {
        return Err(ArrowError::ComputeError(format!(
            "Log cannot use non-integer base: {base}"
        )));
    }
    if (base as u32) < 2 {
        return Err(ArrowError::ComputeError(format!(
            "Log base must be greater than 1: {base}"
        )));
    }

    let unscaled_value = decimal32_to_f64(value, scale)?;
    if unscaled_value > 0.0 {
        Ok(unscaled_value.log(base))
    } else {
        // Reflect f64::log behaviour
        Ok(f64::NAN)
    }
}

/// Binary function to calculate an integer logarithm of Decimal128 `value` using `base` base
/// Returns error if base is invalid
fn log_decimal128(value: i128, scale: i8, base: f64) -> Result<f64, ArrowError> {
    if !base.is_finite() || base.trunc() != base {
        return Err(ArrowError::ComputeError(format!(
            "Log cannot use non-integer base: {base}"
        )));
    }
    if (base as u32) < 2 {
        return Err(ArrowError::ComputeError(format!(
            "Log base must be greater than 1: {base}"
        )));
    }

    let unscaled_value = decimal128_to_i128(value, scale)?;
    if unscaled_value > 0 {
        let log_value: u32 = unscaled_value.ilog(base as i128);
        Ok(log_value as f64)
    } else {
        // Reflect f64::log behaviour
        Ok(f64::NAN)
    }
}

/// Binary function to calculate an integer logarithm of Decimal128 `value` using `base` base
/// Returns error if base is invalid or if value is out of bounds of Decimal128
fn log_decimal256(value: i256, scale: i8, base: f64) -> Result<f64, ArrowError> {
    match value.to_i128() {
        Some(value) => log_decimal128(value, scale, base),
        None => Err(ArrowError::NotYetImplemented(format!(
            "Log of Decimal256 larger than Decimal128 is not yet supported: {value}"
        ))),
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
        // Check last argument (value)
        match &arg_types.last().ok_or(plan_datafusion_err!("No args"))? {
            DataType::Float16 => Ok(DataType::Float16),
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let (base_sort_properties, num_sort_properties) = if input.len() == 1 {
            // log(x) defaults to log(10, x)
            (SortProperties::Singleton, input[0].sort_properties)
        } else {
            (input[0].sort_properties, input[1].sort_properties)
        };
        match (num_sort_properties, base_sort_properties) {
            (first @ SortProperties::Ordered(num), SortProperties::Ordered(base))
                if num.descending != base.descending
                    && num.nulls_first == base.nulls_first =>
            {
                Ok(first)
            }
            (
                first @ (SortProperties::Ordered(_) | SortProperties::Singleton),
                SortProperties::Singleton,
            ) => Ok(first),
            (SortProperties::Singleton, second @ SortProperties::Ordered(_)) => {
                Ok(-second)
            }
            _ => Ok(SortProperties::Unordered),
        }
    }

    // Support overloaded log(base, x) and log(x) which defaults to log(10, x)
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.arg_fields.iter().any(|a| a.data_type().is_null()) {
            return ColumnarValue::Scalar(ScalarValue::Null)
                .cast_to(args.return_type(), None);
        }

        let (base, value) = if args.args.len() == 2 {
            (args.args[0].clone(), &args.args[1])
        } else {
            // no base specified, default to 10
            (
                ColumnarValue::Scalar(ScalarValue::new_ten(args.return_type())?),
                &args.args[0],
            )
        };
        let value = value.to_array(args.number_rows)?;

        let output: ArrayRef = match value.data_type() {
            DataType::Float16 => {
                calculate_binary_math::<Float16Type, Float16Type, Float16Type, _>(
                    &value,
                    &base,
                    |value, base| Ok(value.log(base)),
                )?
            }
            DataType::Float32 => {
                calculate_binary_math::<Float32Type, Float32Type, Float32Type, _>(
                    &value,
                    &base,
                    |value, base| Ok(value.log(base)),
                )?
            }
            DataType::Float64 => {
                calculate_binary_math::<Float64Type, Float64Type, Float64Type, _>(
                    &value,
                    &base,
                    |value, base| Ok(value.log(base)),
                )?
            }
            // TODO: native log support for decimal 32 & 64; right now upcast
            //       to decimal128 to calculate
            //       https://github.com/apache/datafusion/issues/17555
            DataType::Decimal32(_, scale) => {
                calculate_binary_math::<Decimal32Type, Float64Type, Float64Type, _>(
                    &value,
                    &base,
                    |value, base| log_decimal32(value, *scale, base),
                )?
            }
            DataType::Decimal64(precision, scale) => {
                calculate_binary_math::<Decimal128Type, Float64Type, Float64Type, _>(
                    &cast(&value, &DataType::Decimal128(*precision, *scale))?,
                    &base,
                    |value, base| log_decimal128(value, *scale, base),
                )?
            }
            DataType::Decimal128(_, scale) => {
                calculate_binary_math::<Decimal128Type, Float64Type, Float64Type, _>(
                    &value,
                    &base,
                    |value, base| log_decimal128(value, *scale, base),
                )?
            }
            DataType::Decimal256(_, scale) => {
                calculate_binary_math::<Decimal256Type, Float64Type, Float64Type, _>(
                    &value,
                    &base,
                    |value, base| log_decimal256(value, *scale, base),
                )?
            }
            other => {
                return exec_err!("Unsupported data type {other:?} for function log")
            }
        };

        Ok(ColumnarValue::Array(output))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    /// Simplify the `log` function by the relevant rules:
    /// 1. Log(a, 1) ===> 0
    /// 2. Log(a, Power(a, b)) ===> b
    /// 3. Log(a, a) ===> 1
    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let mut arg_types = args
            .iter()
            .map(|arg| info.get_data_type(arg))
            .collect::<Result<Vec<_>>>()?;
        let return_type = self.return_type(&arg_types)?;

        // Null propagation
        if arg_types.iter().any(|dt| dt.is_null()) {
            return Ok(ExprSimplifyResult::Simplified(lit(
                ScalarValue::Null.cast_to(&return_type)?
            )));
        }

        // Args are either
        // log(number)
        // log(base, number)
        let num_args = args.len();
        if num_args != 1 && num_args != 2 {
            return plan_err!("Expected log to have 1 or 2 arguments, got {num_args}");
        }
        let number = args.pop().unwrap();
        let number_datatype = arg_types.pop().unwrap();
        // default to base 10
        let base = if let Some(base) = args.pop() {
            base
        } else {
            lit(ScalarValue::new_ten(&number_datatype)?)
        };

        match number {
            Expr::Literal(value, _)
                if value == ScalarValue::new_one(&number_datatype)? =>
            {
                Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::new_zero(
                    &info.get_data_type(&base)?,
                )?)))
            }
            Expr::ScalarFunction(ScalarFunction { func, mut args })
                if is_pow(&func) && args.len() == 2 && base == args[0] =>
            {
                let b = args.pop().unwrap(); // length checked above
                Ok(ExprSimplifyResult::Simplified(b))
            }
            number => {
                if number == base {
                    Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::new_one(
                        &number_datatype,
                    )?)))
                } else {
                    let args = match num_args {
                        1 => vec![number],
                        2 => vec![base, number],
                        _ => {
                            return internal_err!(
                                "Unexpected number of arguments in log::simplify"
                            )
                        }
                    };
                    Ok(ExprSimplifyResult::Original(args))
                }
            }
        }
    }
}

/// Returns true if the function is `PowerFunc`
fn is_pow(func: &ScalarUDF) -> bool {
    func.inner().as_any().downcast_ref::<PowerFunc>().is_some()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;

    use arrow::array::{
        Date32Array, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
    };
    use arrow::compute::SortOptions;
    use arrow::datatypes::{Field, DECIMAL256_MAX_PRECISION};
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::DFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::simplify::SimplifyContext;

    #[test]
    fn test_log_decimal_native() {
        let value = 10_i128.pow(35);
        assert_eq!((value as f64).log2(), 116.26748332105768);
        assert_eq!(
            log_decimal128(value, 0, 2.0).unwrap(),
            // TODO: see we're losing our decimal points compared to above
            //       https://github.com/apache/datafusion/issues/18524
            116.0
        );
    }

    #[test]
    fn test_log_decimal32_native() {
        let value = 1234567;
        assert_eq!(log_decimal32(value, 0, 2.0).unwrap(), 20.235573703046512);
        assert_eq!(log_decimal32(value, 2, 2.0).unwrap(), 13.591717513271785);
    }

    #[test]
    fn test_log_invalid_base_type() {
        let arg_fields = vec![
            Field::new("b", DataType::Date32, false).into(),
            Field::new("n", DataType::Float64, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Date32Array::from(vec![5, 10, 15, 20]))), // base
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    10.0, 100.0, 1000.0, 10000.0,
                ]))), // num
            ],
            arg_fields,
            number_rows: 4,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new().invoke_with_args(args);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string().lines().next().unwrap(),
            "Arrow error: Cast error: Casting from Date32 to Float64 not supported"
        );
    }

    #[test]
    fn test_log_invalid_value() {
        let arg_field = Field::new("a", DataType::Date32, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Date32Array::from(vec![10]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = LogFunc::new().invoke_with_args(args);
        result.expect_err("expected error");
    }

    #[test]
    fn test_log_scalar_f32_unary() {
        let arg_field = Field::new("a", DataType::Float32, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float32(Some(10.0))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float32, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_scalar_f64_unary() {
        let arg_field = Field::new("a", DataType::Float64, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(10.0))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_scalar_f32() {
        let arg_fields = vec![
            Field::new("a", DataType::Float32, false).into(),
            Field::new("a", DataType::Float32, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float32(Some(2.0))), // base
                ColumnarValue::Scalar(ScalarValue::Float32(Some(32.0))), // num
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float32, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 5.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_scalar_f64() {
        let arg_fields = vec![
            Field::new("a", DataType::Float64, false).into(),
            Field::new("a", DataType::Float64, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(2.0))), // base
                ColumnarValue::Scalar(ScalarValue::Float64(Some(64.0))), // num
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 6.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f64_unary() {
        let arg_field = Field::new("a", DataType::Float64, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    10.0, 100.0, 1000.0, 10000.0,
                ]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 4,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 4);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 3.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f32_unary() {
        let arg_field = Field::new("a", DataType::Float32, false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                    10.0, 100.0, 1000.0, 10000.0,
                ]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 4,
            return_field: Field::new("f", DataType::Float32, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 4);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 3.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f64() {
        let arg_fields = vec![
            Field::new("a", DataType::Float64, false).into(),
            Field::new("a", DataType::Float64, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    2.0, 2.0, 3.0, 5.0, 5.0,
                ]))), // base
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    8.0, 4.0, 81.0, 625.0, -123.0,
                ]))), // num
            ],
            arg_fields,
            number_rows: 5,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 5);
                assert!((floats.value(0) - 3.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 4.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
                assert!(floats.value(4).is_nan());
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_f32() {
        let arg_fields = vec![
            Field::new("a", DataType::Float32, false).into(),
            Field::new("a", DataType::Float32, false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                    2.0, 2.0, 3.0, 5.0,
                ]))), // base
                ColumnarValue::Array(Arc::new(Float32Array::from(vec![
                    8.0, 4.0, 81.0, 625.0,
                ]))), // num
            ],
            arg_fields,
            number_rows: 4,
            return_field: Field::new("f", DataType::Float32, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float32_array(&arr)
                    .expect("failed to convert result to a Float32Array");

                assert_eq!(floats.len(), 4);
                assert!((floats.value(0) - 3.0).abs() < f32::EPSILON);
                assert!((floats.value(1) - 2.0).abs() < f32::EPSILON);
                assert!((floats.value(2) - 4.0).abs() < f32::EPSILON);
                assert!((floats.value(3) - 4.0).abs() < f32::EPSILON);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }
    #[test]
    // Test log() simplification errors
    fn test_log_simplify_errors() {
        let props = ExecutionProps::new();
        let schema =
            Arc::new(DFSchema::new_with_metadata(vec![], HashMap::new()).unwrap());
        let context = SimplifyContext::new(&props).with_schema(schema);
        // Expect 0 args to error
        let _ = LogFunc::new().simplify(vec![], &context).unwrap_err();
        // Expect 3 args to error
        let _ = LogFunc::new()
            .simplify(vec![lit(1), lit(2), lit(3)], &context)
            .unwrap_err();
    }

    #[test]
    // Test that non-simplifiable log() expressions are unchanged after simplification
    fn test_log_simplify_original() {
        let props = ExecutionProps::new();
        let schema =
            Arc::new(DFSchema::new_with_metadata(vec![], HashMap::new()).unwrap());
        let context = SimplifyContext::new(&props).with_schema(schema);
        // One argument with no simplifications
        let result = LogFunc::new().simplify(vec![lit(2)], &context).unwrap();
        let ExprSimplifyResult::Original(args) = result else {
            panic!("Expected ExprSimplifyResult::Original")
        };
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], lit(2));
        // Two arguments with no simplifications
        let result = LogFunc::new()
            .simplify(vec![lit(2), lit(3)], &context)
            .unwrap();
        let ExprSimplifyResult::Original(args) = result else {
            panic!("Expected ExprSimplifyResult::Original")
        };
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], lit(2));
        assert_eq!(args[1], lit(3));
    }

    #[test]
    fn test_log_output_ordering() {
        // [Unordered, Ascending, Descending, Literal]
        let orders = [
            ExprProperties::new_unknown(),
            ExprProperties::new_unknown().with_order(SortProperties::Ordered(
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )),
            ExprProperties::new_unknown().with_order(SortProperties::Ordered(
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            )),
            ExprProperties::new_unknown().with_order(SortProperties::Singleton),
        ];

        let log = LogFunc::new();

        // Test log(num)
        for order in orders.iter().cloned() {
            let result = log.output_ordering(std::slice::from_ref(&order)).unwrap();
            assert_eq!(result, order.sort_properties);
        }

        // Test log(base, num), where `nulls_first` is the same
        let mut results = Vec::with_capacity(orders.len() * orders.len());
        for base_order in orders.iter() {
            for num_order in orders.iter().cloned() {
                let result = log
                    .output_ordering(&[base_order.clone(), num_order])
                    .unwrap();
                results.push(result);
            }
        }
        let expected = [
            // base: Unordered
            SortProperties::Unordered,
            SortProperties::Unordered,
            SortProperties::Unordered,
            SortProperties::Unordered,
            // base: Ascending, num: Unordered
            SortProperties::Unordered,
            // base: Ascending, num: Ascending
            SortProperties::Unordered,
            // base: Ascending, num: Descending
            SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            // base: Ascending, num: Literal
            SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            // base: Descending, num: Unordered
            SortProperties::Unordered,
            // base: Descending, num: Ascending
            SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            // base: Descending, num: Descending
            SortProperties::Unordered,
            // base: Descending, num: Literal
            SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            // base: Literal, num: Unordered
            SortProperties::Unordered,
            // base: Literal, num: Ascending
            SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            // base: Literal, num: Descending
            SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            // base: Literal, num: Literal
            SortProperties::Singleton,
        ];
        assert_eq!(results, expected);

        // Test with different `nulls_first`
        let base_order = ExprProperties::new_unknown().with_order(
            SortProperties::Ordered(SortOptions {
                descending: true,
                nulls_first: true,
            }),
        );
        let num_order = ExprProperties::new_unknown().with_order(
            SortProperties::Ordered(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        );
        assert_eq!(
            log.output_ordering(&[base_order, num_order]).unwrap(),
            SortProperties::Unordered
        );
    }

    #[test]
    fn test_log_scalar_decimal128_unary() {
        let arg_field = Field::new("a", DataType::Decimal128(38, 0), false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(10), 38, 0)), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Decimal128(38, 0), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Decimal128Array");
                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_scalar_decimal128() {
        let arg_fields = vec![
            Field::new("b", DataType::Float64, false).into(),
            Field::new("x", DataType::Decimal128(38, 0), false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(2.0))), // base
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(64), 38, 0)), // num
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 1);
                assert!((floats.value(0) - 6.0).abs() < 1e-10);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_decimal128_unary() {
        let arg_field = Field::new("a", DataType::Decimal128(38, 0), false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(
                    Decimal128Array::from(vec![10, 100, 1000, 10000, 12600, -123])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                )), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 6,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 6);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 3.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
                assert!((floats.value(4) - 4.0).abs() < 1e-10); // Integer rounding
                assert!(floats.value(5).is_nan());
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_decimal128_base_decimal() {
        // Base stays 2 despite scaling
        for base in [
            ScalarValue::Decimal128(Some(i128::from(2)), 38, 0),
            ScalarValue::Decimal128(Some(i128::from(2000)), 38, 3),
        ] {
            let arg_fields = vec![
                Field::new("b", DataType::Decimal128(38, 0), false).into(),
                Field::new("x", DataType::Decimal128(38, 0), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(base), // base
                    ColumnarValue::Scalar(ScalarValue::Decimal128(Some(64), 38, 0)), // num
                ],
                arg_fields,
                number_rows: 1,
                return_field: Field::new("f", DataType::Float64, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = LogFunc::new()
                .invoke_with_args(args)
                .expect("failed to initialize function log");

            match result {
                ColumnarValue::Array(arr) => {
                    let floats = as_float64_array(&arr)
                        .expect("failed to convert result to a Float64Array");

                    assert_eq!(floats.len(), 1);
                    assert!((floats.value(0) - 6.0).abs() < 1e-10);
                }
                ColumnarValue::Scalar(_) => {
                    panic!("Expected an array value")
                }
            }
        }
    }

    #[test]
    fn test_log_decimal128_value_scale() {
        // Value stays 1000 despite scaling
        for value in [
            ScalarValue::Decimal128(Some(i128::from(1000)), 38, 0),
            ScalarValue::Decimal128(Some(i128::from(10000)), 38, 1),
            ScalarValue::Decimal128(Some(i128::from(1000000)), 38, 3),
        ] {
            let arg_fields = vec![
                Field::new("b", DataType::Decimal128(38, 0), false).into(),
                Field::new("x", DataType::Decimal128(38, 0), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(value), // base
                ],
                arg_fields,
                number_rows: 1,
                return_field: Field::new("f", DataType::Float64, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = LogFunc::new()
                .invoke_with_args(args)
                .expect("failed to initialize function log");

            match result {
                ColumnarValue::Array(arr) => {
                    let floats = as_float64_array(&arr)
                        .expect("failed to convert result to a Float64Array");

                    assert_eq!(floats.len(), 1);
                    assert!((floats.value(0) - 3.0).abs() < 1e-10);
                }
                ColumnarValue::Scalar(_) => {
                    panic!("Expected an array value")
                }
            }
        }
    }

    #[test]
    fn test_log_decimal256_unary() {
        let arg_field = Field::new(
            "a",
            DataType::Decimal256(DECIMAL256_MAX_PRECISION, 0),
            false,
        )
        .into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(
                    Decimal256Array::from(vec![
                        Some(i256::from(10)),
                        Some(i256::from(100)),
                        Some(i256::from(1000)),
                        Some(i256::from(10000)),
                        Some(i256::from(12600)),
                        // Slightly lower than i128 max - can calculate
                        Some(i256::from_i128(i128::MAX) - i256::from(1000)),
                        // Give NaN for incorrect inputs, as in f64::log
                        Some(i256::from(-123)),
                    ])
                    .with_precision_and_scale(DECIMAL256_MAX_PRECISION, 0)
                    .unwrap(),
                )), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 7,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function log");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");

                assert_eq!(floats.len(), 7);
                eprintln!("floats {:?}", &floats);
                assert!((floats.value(0) - 1.0).abs() < 1e-10);
                assert!((floats.value(1) - 2.0).abs() < 1e-10);
                assert!((floats.value(2) - 3.0).abs() < 1e-10);
                assert!((floats.value(3) - 4.0).abs() < 1e-10);
                assert!((floats.value(4) - 4.0).abs() < 1e-10); // Integer rounding for float log
                assert!((floats.value(5) - 38.0).abs() < 1e-10);
                assert!(floats.value(6).is_nan());
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_log_decimal128_wrong_base() {
        let arg_fields = vec![
            Field::new("b", DataType::Float64, false).into(),
            Field::new("x", DataType::Decimal128(38, 0), false).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Float64(Some(-2.0))), // base
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(64), 38, 0)), // num
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new().invoke_with_args(args);
        assert!(result.is_err());
        assert_eq!(
            "Arrow error: Compute error: Log base must be greater than 1: -2",
            result.unwrap_err().to_string().lines().next().unwrap()
        );
    }

    #[test]
    fn test_log_decimal256_error() {
        let arg_field = Field::new("a", DataType::Decimal256(38, 0), false).into();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Decimal256Array::from(vec![
                    // Slightly larger than i128
                    Some(i256::from_i128(i128::MAX) + i256::from(1000)),
                ]))), // num
            ],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = LogFunc::new().invoke_with_args(args);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string().lines().next().unwrap(),
            "Arrow error: Not yet implemented: Log of Decimal256 larger than Decimal128 is not yet supported: 170141183460469231731687303715884106727"
        );
    }
}
