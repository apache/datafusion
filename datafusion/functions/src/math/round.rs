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

use crate::utils::{calculate_binary_decimal_math, calculate_binary_math};

use arrow::array::ArrayRef;
use arrow::datatypes::DataType::{
    Decimal32, Decimal64, Decimal128, Decimal256, Float32, Float64,
};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, Float32Type, Float64Type, Int32Type,
};
use arrow::error::ArrowError;
use datafusion_common::types::{
    NativeType, logical_float32, logical_float64, logical_int32,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Rounds a number to the nearest integer.",
    syntax_example = "round(numeric_expression[, decimal_places])",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    argument(
        name = "decimal_places",
        description = "Optional. The number of decimal places to round to. Defaults to 0."
    ),
    sql_example = r#"```sql
> SELECT round(3.14159);
+--------------+
| round(3.14159)|
+--------------+
| 3.0          |
+--------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RoundFunc {
    signature: Signature,
}

impl Default for RoundFunc {
    fn default() -> Self {
        RoundFunc::new()
    }
}

impl RoundFunc {
    pub fn new() -> Self {
        let decimal = Coercion::new_exact(TypeSignatureClass::Decimal);
        let decimal_places = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );
        let float32 = Coercion::new_exact(TypeSignatureClass::Native(logical_float32()));
        let float64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        decimal.clone(),
                        decimal_places.clone(),
                    ]),
                    TypeSignature::Coercible(vec![decimal]),
                    TypeSignature::Coercible(vec![
                        float32.clone(),
                        decimal_places.clone(),
                    ]),
                    TypeSignature::Coercible(vec![float32]),
                    TypeSignature::Coercible(vec![float64.clone(), decimal_places]),
                    TypeSignature::Coercible(vec![float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RoundFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0].clone() {
            Float32 => Float32,
            dt @ Decimal128(_, _)
            | dt @ Decimal256(_, _)
            | dt @ Decimal32(_, _)
            | dt @ Decimal64(_, _) => dt,
            _ => Float64,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.arg_fields.iter().any(|a| a.data_type().is_null()) {
            return ColumnarValue::Scalar(ScalarValue::Null)
                .cast_to(args.return_type(), None);
        }

        let default_decimal_places = ColumnarValue::Scalar(ScalarValue::Int32(Some(0)));
        let decimal_places = if args.args.len() == 2 {
            &args.args[1]
        } else {
            &default_decimal_places
        };

        // Scalar fast path for float and decimal types - avoid array conversion overhead
        if let (ColumnarValue::Scalar(value_scalar), ColumnarValue::Scalar(dp_scalar)) =
            (&args.args[0], decimal_places)
        {
            // Extract decimal places as i32
            // Note: decimal_places is coerced to Int32 by the signature, so the non-Int32
            // arm should be unreachable in normal execution.
            let dp = match dp_scalar {
                ScalarValue::Int32(Some(dp)) => *dp,
                ScalarValue::Int32(None) => {
                    // Return null with correct type for null decimal_places
                    return match value_scalar {
                        ScalarValue::Float32(_) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Float32(None)))
                        }
                        ScalarValue::Decimal128(_, p, s) => Ok(ColumnarValue::Scalar(
                            ScalarValue::Decimal128(None, *p, *s),
                        )),
                        ScalarValue::Decimal256(_, p, s) => Ok(ColumnarValue::Scalar(
                            ScalarValue::Decimal256(None, *p, *s),
                        )),
                        ScalarValue::Decimal64(_, p, s) => Ok(ColumnarValue::Scalar(
                            ScalarValue::Decimal64(None, *p, *s),
                        )),
                        ScalarValue::Decimal32(_, p, s) => Ok(ColumnarValue::Scalar(
                            ScalarValue::Decimal32(None, *p, *s),
                        )),
                        _ => Ok(ColumnarValue::Scalar(ScalarValue::Float64(None))),
                    };
                }
                _ => {
                    return exec_err!(
                        "Internal error: round decimal_places should be Int32, got {:?}",
                        dp_scalar
                    );
                }
            };

            match value_scalar {
                ScalarValue::Float64(Some(v)) => {
                    return round_float(*v, dp)
                        .map(|r| ColumnarValue::Scalar(ScalarValue::Float64(Some(r))))
                        .map_err(DataFusionError::from);
                }
                ScalarValue::Float64(None) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
                }
                ScalarValue::Float32(Some(v)) => {
                    return round_float(*v, dp)
                        .map(|r| ColumnarValue::Scalar(ScalarValue::Float32(Some(r))))
                        .map_err(DataFusionError::from);
                }
                ScalarValue::Float32(None) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float32(None)));
                }
                ScalarValue::Decimal128(Some(v), precision, scale) => {
                    return round_decimal(*v, *scale, dp)
                        .map(|r| {
                            ColumnarValue::Scalar(ScalarValue::Decimal128(
                                Some(r),
                                *precision,
                                *scale,
                            ))
                        })
                        .map_err(DataFusionError::from);
                }
                ScalarValue::Decimal128(None, precision, scale) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        None, *precision, *scale,
                    )));
                }
                ScalarValue::Decimal256(Some(v), precision, scale) => {
                    return round_decimal(*v, *scale, dp)
                        .map(|r| {
                            ColumnarValue::Scalar(ScalarValue::Decimal256(
                                Some(r),
                                *precision,
                                *scale,
                            ))
                        })
                        .map_err(DataFusionError::from);
                }
                ScalarValue::Decimal256(None, precision, scale) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                        None, *precision, *scale,
                    )));
                }
                ScalarValue::Decimal64(Some(v), precision, scale) => {
                    return round_decimal(*v, *scale, dp)
                        .map(|r| {
                            ColumnarValue::Scalar(ScalarValue::Decimal64(
                                Some(r),
                                *precision,
                                *scale,
                            ))
                        })
                        .map_err(DataFusionError::from);
                }
                ScalarValue::Decimal64(None, precision, scale) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Decimal64(
                        None, *precision, *scale,
                    )));
                }
                ScalarValue::Decimal32(Some(v), precision, scale) => {
                    return round_decimal(*v, *scale, dp)
                        .map(|r| {
                            ColumnarValue::Scalar(ScalarValue::Decimal32(
                                Some(r),
                                *precision,
                                *scale,
                            ))
                        })
                        .map_err(DataFusionError::from);
                }
                ScalarValue::Decimal32(None, precision, scale) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Decimal32(
                        None, *precision, *scale,
                    )));
                }
                // All supported scalar types are handled above
                _ => {
                    return exec_err!(
                        "Internal error: unexpected scalar type for round: {:?}",
                        value_scalar
                    );
                }
            }
        }

        round_columnar(&args.args[0], decimal_places, args.number_rows)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // round preserves the order of the first argument
        let value = &input[0];
        let precision = input.get(1);

        if precision
            .map(|r| r.sort_properties.eq(&SortProperties::Singleton))
            .unwrap_or(true)
        {
            Ok(value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn round_columnar(
    value: &ColumnarValue,
    decimal_places: &ColumnarValue,
    number_rows: usize,
) -> Result<ColumnarValue> {
    let value_array = value.to_array(number_rows)?;
    let both_scalars = matches!(value, ColumnarValue::Scalar(_))
        && matches!(decimal_places, ColumnarValue::Scalar(_));

    let arr: ArrayRef = match value_array.data_type() {
        Float64 => {
            let result = calculate_binary_math::<Float64Type, Int32Type, Float64Type, _>(
                value_array.as_ref(),
                decimal_places,
                round_float::<f64>,
            )?;
            result as _
        }
        Float32 => {
            let result = calculate_binary_math::<Float32Type, Int32Type, Float32Type, _>(
                value_array.as_ref(),
                decimal_places,
                round_float::<f32>,
            )?;
            result as _
        }
        Decimal32(precision, scale) => {
            let result = calculate_binary_decimal_math::<
                Decimal32Type,
                Int32Type,
                Decimal32Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| round_decimal(v, *scale, dp),
                *precision,
                *scale,
            )?;
            result as _
        }
        Decimal64(precision, scale) => {
            let result = calculate_binary_decimal_math::<
                Decimal64Type,
                Int32Type,
                Decimal64Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| round_decimal(v, *scale, dp),
                *precision,
                *scale,
            )?;
            result as _
        }
        Decimal128(precision, scale) => {
            let result = calculate_binary_decimal_math::<
                Decimal128Type,
                Int32Type,
                Decimal128Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| round_decimal(v, *scale, dp),
                *precision,
                *scale,
            )?;
            result as _
        }
        Decimal256(precision, scale) => {
            let result = calculate_binary_decimal_math::<
                Decimal256Type,
                Int32Type,
                Decimal256Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| round_decimal(v, *scale, dp),
                *precision,
                *scale,
            )?;
            result as _
        }
        other => exec_err!("Unsupported data type {other:?} for function round")?,
    };

    if both_scalars {
        ScalarValue::try_from_array(&arr, 0).map(ColumnarValue::Scalar)
    } else {
        Ok(ColumnarValue::Array(arr))
    }
}

fn round_float<T>(value: T, decimal_places: i32) -> Result<T, ArrowError>
where
    T: num_traits::Float,
{
    let factor = T::from(10_f64.powi(decimal_places)).ok_or_else(|| {
        ArrowError::ComputeError(format!(
            "Invalid value for decimal places: {decimal_places}"
        ))
    })?;
    Ok((value * factor).round() / factor)
}

fn round_decimal<V: ArrowNativeTypeOp>(
    value: V,
    scale: i8,
    decimal_places: i32,
) -> Result<V, ArrowError> {
    let diff = i64::from(scale) - i64::from(decimal_places);
    if diff <= 0 {
        return Ok(value);
    }

    let diff: u32 = diff.try_into().map_err(|e| {
        ArrowError::ComputeError(format!(
            "Invalid value for decimal places: {decimal_places}: {e}"
        ))
    })?;

    let one = V::ONE;
    let two = V::from_usize(2).ok_or_else(|| {
        ArrowError::ComputeError("Internal error: could not create constant 2".into())
    })?;
    let ten = V::from_usize(10).ok_or_else(|| {
        ArrowError::ComputeError("Internal error: could not create constant 10".into())
    })?;

    let factor = ten.pow_checked(diff).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Overflow while rounding decimal with scale {scale} and decimal places {decimal_places}"
        ))
    })?;

    let mut quotient = value.div_wrapping(factor);
    let remainder = value.mod_wrapping(factor);

    // `factor` is an even number (10^n, n > 0), so `factor / 2` is the tie threshold
    let threshold = factor.div_wrapping(two);
    if remainder >= threshold {
        quotient = quotient.add_checked(one).map_err(|_| {
            ArrowError::ComputeError("Overflow while rounding decimal".into())
        })?;
    } else if remainder <= threshold.neg_wrapping() {
        quotient = quotient.sub_checked(one).map_err(|_| {
            ArrowError::ComputeError("Overflow while rounding decimal".into())
        })?;
    }

    quotient
        .mul_checked(factor)
        .map_err(|_| ArrowError::ComputeError("Overflow while rounding decimal".into()))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array};
    use datafusion_common::DataFusionError;
    use datafusion_common::ScalarValue;
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use datafusion_expr::ColumnarValue;

    fn round_arrays(
        value: ArrayRef,
        decimal_places: Option<ArrayRef>,
    ) -> Result<ArrayRef, DataFusionError> {
        let number_rows = value.len();
        let value = ColumnarValue::Array(value);
        let decimal_places = decimal_places
            .map(ColumnarValue::Array)
            .unwrap_or_else(|| ColumnarValue::Scalar(ScalarValue::Int32(Some(0))));

        let result = super::round_columnar(&value, &decimal_places, number_rows)?;
        match result {
            ColumnarValue::Array(array) => Ok(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1),
        }
    }

    #[test]
    fn test_round_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![125.2345; 10])), // input
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, -1, -2, -3, -4])), // decimal_places
        ];

        let result = round_arrays(Arc::clone(&args[0]), Some(Arc::clone(&args[1])))
            .expect("failed to initialize function round");
        let floats =
            as_float32_array(&result).expect("failed to initialize function round");

        let expected = Float32Array::from(vec![
            125.0, 125.2, 125.23, 125.235, 125.2345, 125.2345, 130.0, 100.0, 0.0, 0.0,
        ]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345; 10])), // input
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, -1, -2, -3, -4])), // decimal_places
        ];

        let result = round_arrays(Arc::clone(&args[0]), Some(Arc::clone(&args[1])))
            .expect("failed to initialize function round");
        let floats =
            as_float64_array(&result).expect("failed to initialize function round");

        let expected = Float64Array::from(vec![
            125.0, 125.2, 125.23, 125.235, 125.2345, 125.2345, 130.0, 100.0, 0.0, 0.0,
        ]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f32_one_input() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![125.2345, 12.345, 1.234, 0.1234])), // input
        ];

        let result = round_arrays(Arc::clone(&args[0]), None)
            .expect("failed to initialize function round");
        let floats =
            as_float32_array(&result).expect("failed to initialize function round");

        let expected = Float32Array::from(vec![125.0, 12.0, 1.0, 0.0]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f64_one_input() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345, 12.345, 1.234, 0.1234])), // input
        ];

        let result = round_arrays(Arc::clone(&args[0]), None)
            .expect("failed to initialize function round");
        let floats =
            as_float64_array(&result).expect("failed to initialize function round");

        let expected = Float64Array::from(vec![125.0, 12.0, 1.0, 0.0]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f32_cast_fail() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345])), // input
            Arc::new(Int64Array::from(vec![2147483648])), // decimal_places
        ];

        let result = round_arrays(Arc::clone(&args[0]), Some(Arc::clone(&args[1])));

        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(DataFusionError::ArrowError(_, _)) | Err(DataFusionError::Execution(_))
        ));
    }
}
