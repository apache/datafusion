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

//! Math function: `power()`.
use std::any::Any;

use super::log::LogFunc;

use crate::utils::{calculate_binary_decimal_math, calculate_binary_math};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, Float64Type, Int64Type,
};
use arrow::error::ArrowError;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, plan_datafusion_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::type_coercion::is_decimal;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns a base expression raised to the power of an exponent.",
    syntax_example = "power(base, exponent)",
    sql_example = r#"```sql
> SELECT power(2, 3);
+-------------+
| power(2,3)  |
+-------------+
| 8           |
+-------------+
```"#,
    standard_argument(name = "base", prefix = "Numeric"),
    standard_argument(name = "exponent", prefix = "Exponent numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PowerFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for PowerFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl PowerFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("pow")],
        }
    }
}

/// Binary function to calculate a math power to integer exponent
/// for scaled integer types.
///
/// Formula
/// The power for a scaled integer `b` is
///
/// ```text
/// (b * 10^(-s)) ^ e
/// ```
/// However, the result should be scaled back from scale 0 to scale `s`,
/// which is done by multiplying by `10^s`.
/// At the end, the formula is:
///
/// ```text
///   b^e * 10^(-s * e) * 10^s = b^e / 10^(s * (e-1))
/// ```
/// Example of 2.5 ^ 4 = 39:
///   2.5 is represented as 25 with scale 1
///   The unscaled result is 25^4 = 390625
///   Scale it back to 1: 390625 / 10^4 = 39
///
/// Returns error if base is invalid
fn pow_decimal_int<T>(base: T, scale: i8, exp: i64) -> Result<T, ArrowError>
where
    T: From<i32> + ArrowNativeTypeOp,
{
    let scale: u32 = scale.try_into().map_err(|_| {
        ArrowError::NotYetImplemented(format!(
            "Negative scale is not yet supported value: {scale}"
        ))
    })?;
    if exp == 0 {
        // Edge case to provide 1 as result (10^s with scale)
        let result: T = T::from(10).pow_checked(scale).map_err(|_| {
            ArrowError::ArithmeticOverflow(format!(
                "Cannot make unscale factor for {scale} and {exp}"
            ))
        })?;
        return Ok(result);
    }
    let exp: u32 = exp.try_into().map_err(|_| {
        ArrowError::ArithmeticOverflow(format!("Unsupported exp value: {exp}"))
    })?;
    let powered: T = base.pow_checked(exp).map_err(|_| {
        ArrowError::ArithmeticOverflow(format!("Cannot raise base {base:?} to exp {exp}"))
    })?;
    let unscale_factor: T = T::from(10).pow_checked(scale * (exp - 1)).map_err(|_| {
        ArrowError::ArithmeticOverflow(format!(
            "Cannot make unscale factor for {scale} and {exp}"
        ))
    })?;

    powered.div_checked(unscale_factor)
}

/// Binary function to calculate a math power to float exponent
/// for scaled integer types.
/// Returns error if exponent is negative or non-integer, or base invalid
fn pow_decimal_float<T>(base: T, scale: i8, exp: f64) -> Result<T, ArrowError>
where
    T: From<i32> + ArrowNativeTypeOp,
{
    if !exp.is_finite() || exp.trunc() != exp {
        return Err(ArrowError::ComputeError(format!(
            "Cannot use non-integer exp: {exp}"
        )));
    }
    if exp < 0f64 || exp >= u32::MAX as f64 {
        return Err(ArrowError::ArithmeticOverflow(format!(
            "Unsupported exp value: {exp}"
        )));
    }
    pow_decimal_int(base, scale, exp as i64)
}

impl ScalarUDFImpl for PowerFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "power"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg1, arg2] = take_function_args(self.name(), arg_types)?;

        fn coerced_type_exp(name: &str, data_type: &DataType) -> Result<DataType> {
            match data_type {
                DataType::Null => Ok(DataType::Int64),
                d if d.is_floating() => Ok(DataType::Float64),
                d if d.is_integer() => Ok(DataType::Int64),
                d if is_decimal(d) => Ok(DataType::Float64),
                other => {
                    exec_err!("Unsupported data type {other:?} for {} function", name)
                }
            }
        }

        // Determine the exponent type first, as it affects base coercion
        let exp_type = coerced_type_exp(self.name(), arg2)?;

        // For base coercion: always use Float64 for integer/null bases
        // This matches PostgreSQL behavior and handles negative exponents correctly
        fn coerced_type_base(name: &str, data_type: &DataType) -> Result<DataType> {
            match data_type {
                d if d.is_floating() => Ok(DataType::Float64),
                // Integer and Null bases always coerce to Float64
                // (integer power doesn't support negative exponents, and pow()
                // should return float like PostgreSQL does)
                DataType::Null => Ok(DataType::Float64),
                d if d.is_integer() => Ok(DataType::Float64),
                d if is_decimal(d) => Ok(d.clone()),
                other => {
                    exec_err!("Unsupported data type {other:?} for {} function", name)
                }
            }
        }

        Ok(vec![coerced_type_base(self.name(), arg1)?, exp_type])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let base = &args.args[0].to_array(args.number_rows)?;
        let exponent = &args.args[1];

        let arr: ArrayRef = match (base.data_type(), exponent.data_type()) {
            (DataType::Float64, _) => {
                calculate_binary_math::<Float64Type, Float64Type, Float64Type, _>(
                    &base,
                    exponent,
                    |b, e| Ok(f64::powf(b, e)),
                )?
            }
            (DataType::Decimal32(precision, scale), DataType::Int64) => {
                calculate_binary_decimal_math::<Decimal32Type, Int64Type, Decimal32Type, _>(
                    &base,
                    exponent,
                    |b, e| pow_decimal_int(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal32(precision, scale), DataType::Float64) => {
                calculate_binary_decimal_math::<
                    Decimal32Type,
                    Float64Type,
                    Decimal32Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_float(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal64(precision, scale), DataType::Int64) => {
                calculate_binary_decimal_math::<Decimal64Type, Int64Type, Decimal64Type, _>(
                    &base,
                    exponent,
                    |b, e| pow_decimal_int(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal64(precision, scale), DataType::Float64) => {
                calculate_binary_decimal_math::<
                    Decimal64Type,
                    Float64Type,
                    Decimal64Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_float(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal128(precision, scale), DataType::Int64) => {
                calculate_binary_decimal_math::<
                    Decimal128Type,
                    Int64Type,
                    Decimal128Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_int(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal128(precision, scale), DataType::Float64) => {
                calculate_binary_decimal_math::<
                    Decimal128Type,
                    Float64Type,
                    Decimal128Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_float(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal256(precision, scale), DataType::Int64) => {
                calculate_binary_decimal_math::<
                    Decimal256Type,
                    Int64Type,
                    Decimal256Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_int(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (DataType::Decimal256(precision, scale), DataType::Float64) => {
                calculate_binary_decimal_math::<
                    Decimal256Type,
                    Float64Type,
                    Decimal256Type,
                    _,
                >(
                    &base,
                    exponent,
                    |b, e| pow_decimal_float(b, *scale, e),
                    *precision,
                    *scale,
                )?
            }
            (base_type, exp_type) => {
                return exec_err!(
                    "Unsupported data types for base {base_type:?} and exponent {exp_type:?} for function {}",
                    self.name()
                );
            }
        };
        Ok(ColumnarValue::Array(arr))
    }

    /// Simplify the `power` function by the relevant rules:
    /// 1. Power(a, 0) ===> 0
    /// 2. Power(a, 1) ===> a
    /// 3. Power(a, Log(a, b)) ===> b
    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let exponent = args.pop().ok_or_else(|| {
            plan_datafusion_err!("Expected power to have 2 arguments, got 0")
        })?;
        let base = args.pop().ok_or_else(|| {
            plan_datafusion_err!("Expected power to have 2 arguments, got 1")
        })?;

        let exponent_type = info.get_data_type(&exponent)?;
        match exponent {
            Expr::Literal(value, _)
                if value == ScalarValue::new_zero(&exponent_type)? =>
            {
                Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::new_one(&info.get_data_type(&base)?)?,
                    None,
                )))
            }
            Expr::Literal(value, _) if value == ScalarValue::new_one(&exponent_type)? => {
                Ok(ExprSimplifyResult::Simplified(base))
            }
            Expr::ScalarFunction(ScalarFunction { func, mut args })
                if is_log(&func) && args.len() == 2 && base == args[0] =>
            {
                let b = args.pop().unwrap(); // length checked above
                Ok(ExprSimplifyResult::Simplified(b))
            }
            _ => Ok(ExprSimplifyResult::Original(vec![base, exponent])),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Return true if this function call is a call to `Log`
fn is_log(func: &ScalarUDF) -> bool {
    func.inner().as_any().downcast_ref::<LogFunc>().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array};
    use arrow::datatypes::{DECIMAL128_MAX_SCALE, Field};
    use datafusion_common::cast::{as_decimal128_array, as_float64_array};
    use datafusion_common::config::ConfigOptions;
    use std::sync::Arc;

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        // Enable RUST_LOG logging configuration for test
        let _ = env_logger::try_init();
    }

    #[test]
    fn test_power_f64() {
        let arg_fields = vec![
            Field::new("a", DataType::Float64, true).into(),
            Field::new("a", DataType::Float64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    2.0, 2.0, 3.0, 5.0,
                ]))), // base
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    3.0, 2.0, 4.0, 4.0,
                ]))), // exponent
            ],
            arg_fields,
            number_rows: 4,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");
                assert_eq!(floats.len(), 4);
                assert_eq!(floats.value(0), 8.0);
                assert_eq!(floats.value(1), 4.0);
                assert_eq!(floats.value(2), 81.0);
                assert_eq!(floats.value(3), 625.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_i128() {
        let arg_fields = vec![
            Field::new(
                "a",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            Field::new("a", DataType::Int64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(
                    Decimal128Array::from(vec![2, 2, 3, 5, 0, 5])
                        .with_precision_and_scale(DECIMAL128_MAX_SCALE as u8, 0)
                        .unwrap(),
                )), // base
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![3, 2, 4, 4, 4, 0]))), // exponent
            ],
            arg_fields,
            number_rows: 6,
            return_field: Field::new(
                "f",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_decimal128_array(&arr)
                    .expect("failed to convert result to an array");

                assert_eq!(ints.len(), 6);
                assert_eq!(ints.value(0), i128::from(8));
                assert_eq!(ints.value(1), i128::from(4));
                assert_eq!(ints.value(2), i128::from(81));
                assert_eq!(ints.value(3), i128::from(625));
                assert_eq!(ints.value(4), i128::from(0));
                assert_eq!(ints.value(5), i128::from(1));
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_array_null() {
        let arg_fields = vec![
            Field::new("a", DataType::Float64, true).into(),
            Field::new("a", DataType::Float64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![2.0, 2.0, 2.0]))), // base
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    None,
                    Some(3.0),
                ]))), // exponent
            ],
            arg_fields,
            number_rows: 3,
            return_field: Field::new("f", DataType::Float64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let floats =
                    as_float64_array(&arr).expect("failed to convert result to an array");

                assert_eq!(floats.len(), 3);
                assert!(!floats.is_null(0));
                assert_eq!(floats.value(0), 2.0);
                assert!(floats.is_null(1));
                assert!(!floats.is_null(2));
                assert_eq!(floats.value(2), 8.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_decimal_with_scale() {
        // 2.5 ^ 4 = 39
        // 2.5 is 25 in Decimal128(2, 1) by parsing rules
        // Signature is Decimal128(2, 1) -> Int64 -> Decimal128(2, 1), therefore
        // result is 390 in Decimal128(2, 1) aka 39 in unscaled Decimal128(2, 0)
        let arg_fields = vec![
            Field::new(
                "a",
                DataType::Decimal128(DECIMAL128_MAX_SCALE as u8, 0),
                true,
            )
            .into(),
            Field::new("a", DataType::Int64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(i128::from(25)),
                    2,
                    1,
                )), // base
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))), // exponent
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Decimal128(2, 1), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = PowerFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_decimal128_array(&arr)
                    .expect("failed to convert result to an array");

                assert_eq!(ints.len(), 1);
                assert_eq!(ints.value(0), i128::from(390));
                // Signature stays the same as input
                assert_eq!(*arr.data_type(), DataType::Decimal128(2, 1));
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_pow_decimal128_helper() {
        // Expression: 2.5 ^ 4 = 39.0625
        assert_eq!(pow_decimal_int(25, 1, 4).unwrap(), i128::from(390));
        assert_eq!(pow_decimal_int(2500, 3, 4).unwrap(), i128::from(39062));
        assert_eq!(pow_decimal_int(25000, 4, 4).unwrap(), i128::from(390625));

        // Expression: 25 ^ 4 = 390625
        assert_eq!(pow_decimal_int(25, 0, 4).unwrap(), i128::from(390625));

        // Expressions for edge cases
        assert_eq!(pow_decimal_int(25, 1, 1).unwrap(), i128::from(25));
        assert_eq!(pow_decimal_int(25, 0, 1).unwrap(), i128::from(25));
        assert_eq!(pow_decimal_int(25, 0, 0).unwrap(), i128::from(1));
        assert_eq!(pow_decimal_int(25, 1, 0).unwrap(), i128::from(10));

        assert_eq!(
            pow_decimal_int(25, -1, 4).unwrap_err().to_string(),
            "Not yet implemented: Negative scale is not yet supported value: -1"
        );
    }

    #[test]
    fn test_power_coerce_types() {
        let power_func = PowerFunc::new();

        // Int64 base with Int64 exponent -> base coerced to Float64 (like PostgreSQL)
        // This allows negative exponents to work correctly
        let result = power_func
            .coerce_types(&[DataType::Int64, DataType::Int64])
            .unwrap();
        assert_eq!(result, vec![DataType::Float64, DataType::Int64]);

        // Float64 base with Float64 exponent -> both stay Float64
        let result = power_func
            .coerce_types(&[DataType::Float64, DataType::Float64])
            .unwrap();
        assert_eq!(result, vec![DataType::Float64, DataType::Float64]);

        // Int64 base with Float64 exponent -> base coerced to Float64
        let result = power_func
            .coerce_types(&[DataType::Int64, DataType::Float64])
            .unwrap();
        assert_eq!(result, vec![DataType::Float64, DataType::Float64]);

        // Int32 base with Float32 exponent -> both coerced to Float64
        let result = power_func
            .coerce_types(&[DataType::Int32, DataType::Float32])
            .unwrap();
        assert_eq!(result, vec![DataType::Float64, DataType::Float64]);

        // Null base with Float64 exponent -> base coerced to Float64
        let result = power_func
            .coerce_types(&[DataType::Null, DataType::Float64])
            .unwrap();
        assert_eq!(result, vec![DataType::Float64, DataType::Float64]);

        // Null base with Int64 exponent -> base coerced to Float64 (like PostgreSQL)
        let result = power_func
            .coerce_types(&[DataType::Null, DataType::Int64])
            .unwrap();
        assert_eq!(result, vec![DataType::Float64, DataType::Int64]);
    }
}
