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
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Float32Type,
    Float64Type, Int64Type,
};
use arrow::error::ArrowError;
use arrow_buffer::i256;
use datafusion_common::types::NativeType;
use datafusion_common::{Result, ScalarValue, exec_err};
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
        let integer = Coercion::new_exact(TypeSignatureClass::Integer);
        let float = Coercion::new_implicit(
            TypeSignatureClass::Float,
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![decimal.clone(), integer.clone()]),
                    TypeSignature::Coercible(vec![decimal]),
                    TypeSignature::Coercible(vec![float.clone(), integer]),
                    TypeSignature::Coercible(vec![float]),
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
        if args.args.len() != 1 && args.args.len() != 2 {
            return exec_err!(
                "round function requires one or two arguments, got {}",
                args.args.len()
            );
        }

        if args.arg_fields.iter().any(|a| a.data_type().is_null()) {
            return ColumnarValue::Scalar(ScalarValue::Null)
                .cast_to(args.return_type(), None);
        }

        let default_decimal_places = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));
        let value = &args.args[0];
        let decimal_places = args.args.get(1).unwrap_or(&default_decimal_places);

        round_columnar(value, decimal_places, args.number_rows)
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
    let decimal_places_is_scalar = matches!(decimal_places, ColumnarValue::Scalar(_));
    let value_is_scalar = matches!(value, ColumnarValue::Scalar(_));

    let arr: ArrayRef = match value_array.data_type() {
        Float64 => {
            let result = calculate_binary_math::<Float64Type, Int64Type, Float64Type, _>(
                value_array.as_ref(),
                decimal_places,
                round_float::<f64>,
            )?;
            result as _
        }
        Float32 => {
            let result = calculate_binary_math::<Float32Type, Int64Type, Float32Type, _>(
                value_array.as_ref(),
                decimal_places,
                round_float::<f32>,
            )?;
            result as _
        }
        Decimal32(precision, scale) => {
            let result = calculate_binary_decimal_math::<
                Decimal32Type,
                Int64Type,
                Decimal32Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| round_decimal32(v, *scale, dp),
                *precision,
                *scale,
            )?;
            result as _
        }
        Decimal64(precision, scale) => {
            let result = calculate_binary_decimal_math::<
                Decimal64Type,
                Int64Type,
                Decimal64Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| round_decimal64(v, *scale, dp),
                *precision,
                *scale,
            )?;
            result as _
        }
        Decimal128(precision, scale) => {
            let result = calculate_binary_decimal_math::<
                Decimal128Type,
                Int64Type,
                Decimal128Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| round_decimal128(v, *scale, dp),
                *precision,
                *scale,
            )?;
            result as _
        }
        Decimal256(precision, scale) => {
            let result = calculate_binary_decimal_math::<
                Decimal256Type,
                Int64Type,
                Decimal256Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| round_decimal256(v, *scale, dp),
                *precision,
                *scale,
            )?;
            result as _
        }
        other => exec_err!("Unsupported data type {other:?} for function round")?,
    };

    if value_is_scalar && decimal_places_is_scalar {
        ScalarValue::try_from_array(&arr, 0).map(ColumnarValue::Scalar)
    } else {
        Ok(ColumnarValue::Array(arr))
    }
}

fn round_float<T>(value: T, decimal_places: i64) -> Result<T, ArrowError>
where
    T: num_traits::Float,
{
    let places: i32 = decimal_places.try_into().map_err(|e| {
        ArrowError::ComputeError(format!(
            "Invalid value for decimal places: {decimal_places}: {e}"
        ))
    })?;

    let factor = T::from(10_f64.powi(places)).ok_or_else(|| {
        ArrowError::ComputeError(format!(
            "Invalid value for decimal places: {decimal_places}"
        ))
    })?;
    Ok((value * factor).round() / factor)
}

fn round_decimal32(
    value: i32,
    scale: i8,
    decimal_places: i64,
) -> Result<i32, ArrowError> {
    let rounded =
        round_decimal_i256(i256::from_i128(i128::from(value)), scale, decimal_places)?;
    rounded
        .to_i128()
        .and_then(|v| i32::try_from(v).ok())
        .ok_or_else(|| {
            ArrowError::ComputeError("Overflow while rounding decimal32".into())
        })
}

fn round_decimal64(
    value: i64,
    scale: i8,
    decimal_places: i64,
) -> Result<i64, ArrowError> {
    let rounded =
        round_decimal_i256(i256::from_i128(value.into()), scale, decimal_places)?;
    rounded
        .to_i128()
        .and_then(|v| i64::try_from(v).ok())
        .ok_or_else(|| {
            ArrowError::ComputeError("Overflow while rounding decimal64".into())
        })
}

fn round_decimal128(
    value: i128,
    scale: i8,
    decimal_places: i64,
) -> Result<i128, ArrowError> {
    let rounded = round_decimal_i256(i256::from_i128(value), scale, decimal_places)?;
    rounded.to_i128().ok_or_else(|| {
        ArrowError::ComputeError("Overflow while rounding decimal128".into())
    })
}

fn round_decimal256(
    value: i256,
    scale: i8,
    decimal_places: i64,
) -> Result<i256, ArrowError> {
    round_decimal_i256(value, scale, decimal_places)
}

fn round_decimal_i256(
    value: i256,
    scale: i8,
    decimal_places: i64,
) -> Result<i256, ArrowError> {
    let dp: i32 = decimal_places.try_into().map_err(|e| {
        ArrowError::ComputeError(format!(
            "Invalid value for decimal places: {decimal_places}: {e}"
        ))
    })?;

    let diff = scale as i32 - dp;
    if diff <= 0 {
        return Ok(value);
    }

    let factor = i256::from_i128(10)
        .checked_pow(diff as u32)
        .ok_or_else(|| {
            ArrowError::ComputeError(format!(
                "Overflow while rounding decimal with scale {scale} and decimal places {decimal_places}"
            ))
        })?;

    let mut quotient = value / factor;
    let remainder = value % factor;
    let abs_remainder = remainder.wrapping_abs();

    let threshold = (factor + i256::ONE) / i256::from_i128(2);
    if abs_remainder >= threshold {
        quotient = if value.is_negative() {
            quotient.checked_sub(i256::ONE).ok_or_else(|| {
                ArrowError::ComputeError("Overflow while rounding decimal".into())
            })?
        } else {
            quotient.checked_add(i256::ONE).ok_or_else(|| {
                ArrowError::ComputeError("Overflow while rounding decimal".into())
            })?
        };
    }

    quotient
        .checked_mul(factor)
        .ok_or_else(|| ArrowError::ComputeError("Overflow while rounding decimal".into()))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, AsArray, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
        Int64Array,
    };
    use arrow::datatypes::{Decimal128Type, Decimal256Type};
    use arrow_buffer::i256;
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
            .unwrap_or_else(|| ColumnarValue::Scalar(ScalarValue::Int64(Some(0))));

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

    #[test]
    fn test_round_decimal128_scalar_places() {
        let values = Decimal128Array::from(vec![1739751405458550000000i128])
            .with_precision_and_scale(38, 10)
            .unwrap();

        let decimal_places = Arc::new(Int64Array::from(vec![2])) as ArrayRef;
        let result = round_arrays(Arc::new(values), Some(decimal_places))
            .expect("failed to initialize function round");
        let decimals = result.as_primitive::<Decimal128Type>();

        assert_eq!(decimals.value(0), 1739751405458600000000i128);
        assert_eq!(decimals.precision(), 38);
        assert_eq!(decimals.scale(), 10);
    }

    #[test]
    fn test_round_decimal128_negative_places() {
        let values = Decimal128Array::from(vec![1234555i128])
            .with_precision_and_scale(10, 2)
            .unwrap();

        let decimal_places = Arc::new(Int64Array::from(vec![-1])) as ArrayRef;
        let result = round_arrays(Arc::new(values), Some(decimal_places))
            .expect("failed to initialize function round");
        let decimals = result.as_primitive::<Decimal128Type>();

        // 12345.55 rounded to -1 decimal place => 12350.00
        assert_eq!(decimals.value(0), 1235000i128);
        assert_eq!(decimals.precision(), 10);
        assert_eq!(decimals.scale(), 2);
    }

    #[test]
    fn test_round_decimal256() {
        let values = Decimal256Array::from(vec![Some(i256::from_i128(12345678))])
            .with_precision_and_scale(30, 4)
            .unwrap();

        let decimal_places = Arc::new(Int64Array::from(vec![2])) as ArrayRef;
        let result = round_arrays(Arc::new(values), Some(decimal_places))
            .expect("failed to initialize function round");
        let decimals = result.as_primitive::<Decimal256Type>();

        assert_eq!(decimals.value(0), i256::from_i128(12345700));
        assert_eq!(decimals.precision(), 30);
        assert_eq!(decimals.scale(), 4);
    }
}
