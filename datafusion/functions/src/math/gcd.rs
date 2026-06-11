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

use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::compute::try_binary;
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Int64Type,
};
use std::sync::Arc;

use crate::math::common::{gcd_signed, gcd_signed_int, unsigned_gcd};
use crate::utils::calculate_binary_decimal_math_cast;
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    Result, ScalarValue, exec_err, internal_datafusion_err, plan_err,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_expr_common::type_coercion::binary::decimal_coercion;
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the greatest common divisor of `expression_x` and `expression_y`. Returns 0 if both inputs are zero.",
    syntax_example = "gcd(expression_x, expression_y)",
    sql_example = r#"```sql
> SELECT gcd(48, 18);
+------------+
| gcd(48,18) |
+------------+
| 6          |
+------------+
```"#,
    standard_argument(name = "expression_x", prefix = "First numeric"),
    standard_argument(name = "expression_y", prefix = "Second numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GcdFunc {
    signature: Signature,
}

impl Default for GcdFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GcdFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GcdFunc {
    fn name(&self) -> &str {
        "gcd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg1, arg2] = take_function_args(self.name(), arg_types)?;

        let coerced_type = match (arg1, arg2) {
            (DataType::Null, _) | (_, DataType::Null) => Ok(DataType::Int64),
            (lhs, rhs) if lhs.is_integer() && rhs.is_integer() => Ok(DataType::Int64),
            (lhs, rhs) if lhs.is_decimal() || rhs.is_decimal() => {
                decimal_coercion(lhs, rhs).map(Ok).unwrap_or_else(|| {
                    plan_err!(
                        "Unsupported argument types {lhs:?} and {rhs:?} for function {}",
                        self.name()
                    )
                })
            }
            (lhs, rhs) => {
                plan_err!(
                    "Unsupported argument types {lhs:?} and {rhs:?} for function {}",
                    self.name()
                )
            }
        }?;
        Ok(vec![coerced_type.clone(), coerced_type])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let number_rows = args.number_rows;
        let args: [ColumnarValue; 2] = args.args.try_into().map_err(|_| {
            internal_datafusion_err!("Expected 2 arguments for function gcd")
        })?;

        if args[0].data_type() == DataType::Int64 {
            // Optimized path for both integers
            match args {
                [ColumnarValue::Array(a), ColumnarValue::Array(b)] => {
                    compute_gcd_for_arrays(&a, &b)
                }
                [
                    ColumnarValue::Scalar(ScalarValue::Int64(a)),
                    ColumnarValue::Scalar(ScalarValue::Int64(b)),
                ] => match (a, b) {
                    (Some(a), Some(b)) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                        Some(gcd_signed_int(a, b)?),
                    ))),
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
                },
                [
                    ColumnarValue::Array(a),
                    ColumnarValue::Scalar(ScalarValue::Int64(b)),
                ] => compute_gcd_with_scalar(&a, b),
                [
                    ColumnarValue::Scalar(ScalarValue::Int64(a)),
                    ColumnarValue::Array(b),
                ] => compute_gcd_with_scalar(&b, a),
                _ => exec_err!("Unsupported argument types for function gcd"),
            }
        } else {
            // Decimal path: convert left to array and use generic helper
            let left = args[0].to_array(number_rows)?;
            let right = &args[1];

            let arr: ArrayRef = match (left.data_type(), right.data_type()) {
                (
                    lhs @ DataType::Decimal32(precision, scale),
                    rhs @ DataType::Decimal32(_, _),
                ) if *lhs == rhs => calculate_binary_decimal_math_cast::<
                    Decimal32Type,
                    Decimal32Type,
                    Decimal32Type,
                    _,
                >(
                    &left, right, gcd_signed, *precision, *scale, lhs
                )?,
                (
                    lhs @ DataType::Decimal64(precision, scale),
                    rhs @ DataType::Decimal64(_, _),
                ) if *lhs == rhs => calculate_binary_decimal_math_cast::<
                    Decimal64Type,
                    Decimal64Type,
                    Decimal64Type,
                    _,
                >(
                    &left, right, gcd_signed, *precision, *scale, lhs
                )?,
                (
                    lhs @ DataType::Decimal128(precision, scale),
                    rhs @ DataType::Decimal128(_, _),
                ) if *lhs == rhs => calculate_binary_decimal_math_cast::<
                    Decimal128Type,
                    Decimal128Type,
                    Decimal128Type,
                    _,
                >(
                    &left, right, gcd_signed, *precision, *scale, lhs
                )?,
                (
                    lhs @ DataType::Decimal256(precision, scale),
                    rhs @ DataType::Decimal256(_, _),
                ) if *lhs == rhs => calculate_binary_decimal_math_cast::<
                    Decimal256Type,
                    Decimal256Type,
                    Decimal256Type,
                    _,
                >(
                    &left, right, gcd_signed, *precision, *scale, lhs
                )?,
                (lhs, rhs) => {
                    exec_err!(
                        "Unsupported data types {lhs:?} and {rhs:?} for function {}",
                        self.name()
                    )
                }?,
            };
            Ok(ColumnarValue::Array(arr))
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn compute_gcd_for_arrays(a: &ArrayRef, b: &ArrayRef) -> Result<ColumnarValue> {
    let a = a.as_primitive::<Int64Type>();
    let b = b.as_primitive::<Int64Type>();
    try_binary(a, b, gcd_signed_int)
        .map(|arr: PrimitiveArray<Int64Type>| {
            ColumnarValue::Array(Arc::new(arr) as ArrayRef)
        })
        .map_err(Into::into) // convert ArrowError to DataFusionError
}

fn compute_gcd_with_scalar(arr: &ArrayRef, scalar: Option<i64>) -> Result<ColumnarValue> {
    let prim = arr.as_primitive::<Int64Type>();
    match scalar {
        Some(scalar_value) if scalar_value != 0 && scalar_value != i64::MIN => {
            // The gcd result divides both inputs' absolute values. When the
            // scalar is neither 0 nor i64::MIN, the gcd's absolute value fits
            // in i64, so the cast to i64 below cannot overflow. This allows us
            // to use `unary` instead of `try_unary`, which allows LLVM to
            // vectorize more effectively.
            let sv = scalar_value.unsigned_abs();
            let result: PrimitiveArray<Int64Type> =
                prim.unary(|val| unsigned_gcd(val.unsigned_abs(), sv) as i64);
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        Some(scalar_value) => {
            let result: PrimitiveArray<Int64Type> =
                prim.try_unary(|val| gcd_signed_int(val, scalar_value))?;
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        None => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::math::common::gcd_signed;
    use arrow::array::{Array, Decimal128Array, Int64Array};
    use arrow::datatypes::{DECIMAL128_MAX_PRECISION, Field};
    use arrow_buffer::i256;
    use datafusion_common::ScalarValue;
    use datafusion_common::cast::{as_decimal128_array, as_int64_array};
    use datafusion_common::config::ConfigOptions;
    use std::sync::Arc;

    #[test]
    fn test_i64_array() {
        let arg_fields = vec![
            Field::new("a", DataType::Int64, true).into(),
            Field::new("b", DataType::Int64, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    0, 2, 0, 2, 15, 20,
                ]))),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    0, 0, 2, 3, 10, 1000,
                ]))),
            ],
            arg_fields,
            number_rows: 6,
            return_field: Field::new("f", DataType::Int64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = GcdFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function");

        match result {
            ColumnarValue::Array(arr) => {
                let values =
                    as_int64_array(&arr).expect("failed to convert result to an array");
                assert_eq!(values.len(), 6);
                assert_eq!(values.value(0), 0);
                assert_eq!(values.value(1), 2);
                assert_eq!(values.value(2), 2);
                assert_eq!(values.value(3), 1);
                assert_eq!(values.value(4), 5);
                assert_eq!(values.value(5), 20);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_decimal_scalar() {
        let arg_fields = vec![
            Field::new("a", DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0), true)
                .into(),
            Field::new("b", DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0), true)
                .into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(i128::from(2)),
                    DECIMAL128_MAX_PRECISION,
                    0,
                )),
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(i128::from(3)),
                    DECIMAL128_MAX_PRECISION,
                    0,
                )),
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new(
                "f",
                DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = GcdFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_decimal128_array(&arr)
                    .expect("failed to convert result to an array");

                assert_eq!(ints.len(), 1);
                assert_eq!(ints.value(0), i128::from(1));
                // Signature stays the same as input
                assert_eq!(
                    *arr.data_type(),
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0)
                );
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_decimal_array_scalar() {
        let arg_fields = vec![
            Field::new("a", DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0), true)
                .into(),
            Field::new("b", DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0), true)
                .into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(
                    Decimal128Array::from(vec![2, 15])
                        .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 0)
                        .unwrap(),
                )),
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(i128::from(3)),
                    DECIMAL128_MAX_PRECISION,
                    0,
                )),
            ],
            arg_fields,
            number_rows: 2,
            return_field: Field::new(
                "f",
                DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = GcdFunc::new()
            .invoke_with_args(args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_decimal128_array(&arr)
                    .expect("failed to convert result to an array");

                assert_eq!(ints.len(), 2);
                assert_eq!(ints.value(0), i128::from(1));
                assert_eq!(ints.value(1), i128::from(3));
                // Signature stays the same as input
                assert_eq!(
                    *arr.data_type(),
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0)
                );
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_coercion() {
        let mut coerced = GcdFunc::new()
            .coerce_types(&[DataType::Int64, DataType::Int32])
            .expect("coercion should succeed");
        assert_eq!(coerced, vec![DataType::Int64, DataType::Int64]);

        coerced = GcdFunc::new()
            .coerce_types(&[DataType::Decimal128(10, 2), DataType::Int32])
            .expect("coercion should succeed");

        assert_eq!(
            coerced,
            vec![DataType::Decimal128(12, 2), DataType::Decimal128(12, 2)]
        );

        coerced = GcdFunc::new()
            .coerce_types(&[DataType::Decimal128(10, 2), DataType::Null])
            .expect("coercion should succeed");

        assert_eq!(coerced, vec![DataType::Int64, DataType::Int64]);
    }
}
