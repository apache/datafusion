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
    Decimal256Type, DecimalType, Float32Type, Float64Type, Int32Type,
};
use arrow::datatypes::{Field, FieldRef};
use arrow::error::ArrowError;
use datafusion_common::types::{
    NativeType, logical_float32, logical_float64, logical_int32,
};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

fn output_scale_for_decimal(precision: u8, input_scale: i8, decimal_places: i32) -> i8 {
    // `decimal_places` controls the maximum output scale, but scale cannot exceed the input scale.
    //
    // For negative-scale decimals, allow further scale reduction to match negative `decimal_places`
    // (e.g. scale -2 rounded to -3 becomes scale -3). This preserves fixed precision by
    // representing the rounded result at a coarser scale.
    if input_scale < 0 {
        // Decimal scales must be within [-precision, precision] and fit in i8. For negative-scale
        // decimals, allow rounding to move the output scale further negative, but cap it at
        // `-precision` (beyond that, the rounded result is always 0).
        let min_scale = -i32::from(precision);
        let new_scale = i32::from(input_scale).min(decimal_places).max(min_scale);
        return new_scale as i8;
    }

    // The `min` ensures the result is always within i8 range because `input_scale` is i8.
    let decimal_places = decimal_places.max(0);
    i32::from(input_scale).min(decimal_places) as i8
}

fn normalize_decimal_places_for_decimal(
    decimal_places: i32,
    precision: u8,
    scale: i8,
) -> Option<i32> {
    if decimal_places >= 0 {
        return Some(decimal_places);
    }

    // For fixed precision decimals, the absolute value is strictly less than 10^(precision - scale).
    // If the rounding position is beyond that (abs(decimal_places) > precision - scale), the
    // rounded result is always 0, and we can avoid overflow in intermediate 10^n computations.
    let max_rounding_pow10 = i64::from(precision) - i64::from(scale);
    if max_rounding_pow10 <= 0 {
        return None;
    }

    let abs_decimal_places = i64::from(decimal_places.unsigned_abs());
    (abs_decimal_places <= max_rounding_pow10).then_some(decimal_places)
}

fn validate_decimal_precision<T: DecimalType>(
    value: T::Native,
    precision: u8,
    scale: i8,
) -> Result<T::Native, ArrowError> {
    T::validate_decimal_precision(value, precision, scale).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Decimal overflow: rounded value exceeds precision {precision}"
        ))
    })?;
    Ok(value)
}

fn calculate_new_precision_scale<T: DecimalType>(
    precision: u8,
    scale: i8,
    decimal_places: Option<i32>,
) -> Result<DataType> {
    if let Some(decimal_places) = decimal_places {
        let new_scale = output_scale_for_decimal(precision, scale, decimal_places);

        // When rounding an integer decimal (scale == 0) to a negative `decimal_places`, a carry can
        // add an extra digit to the integer part (e.g. 99 -> 100 when rounding to -1). This can
        // only happen when the rounding position is within the existing precision.
        let abs_decimal_places = decimal_places.unsigned_abs();
        let new_precision = if scale == 0
            && decimal_places < 0
            && abs_decimal_places <= u32::from(precision)
        {
            precision.saturating_add(1).min(T::MAX_PRECISION)
        } else {
            precision
        };
        Ok(T::TYPE_CONSTRUCTOR(new_precision, new_scale))
    } else {
        let new_precision = precision.saturating_add(1).min(T::MAX_PRECISION);
        Ok(T::TYPE_CONSTRUCTOR(new_precision, scale))
    }
}

fn decimal_places_from_scalar(scalar: &ScalarValue) -> Result<i32> {
    let out_of_range = |value: String| {
        datafusion_common::DataFusionError::Execution(format!(
            "round decimal_places {value} is out of supported i32 range"
        ))
    };
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(i32::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i32::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(*v),
        ScalarValue::Int64(Some(v)) => {
            i32::try_from(*v).map_err(|_| out_of_range(v.to_string()))
        }
        ScalarValue::UInt8(Some(v)) => Ok(i32::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(i32::from(*v)),
        ScalarValue::UInt32(Some(v)) => {
            i32::try_from(*v).map_err(|_| out_of_range(v.to_string()))
        }
        ScalarValue::UInt64(Some(v)) => {
            i32::try_from(*v).map_err(|_| out_of_range(v.to_string()))
        }
        other => exec_err!(
            "Unexpected datatype for decimal_places: {}",
            other.data_type()
        ),
    }
}

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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_field = &args.arg_fields[0];
        let input_type = input_field.data_type();

        // If decimal_places is a scalar literal, we can incorporate it into the output type
        // (scale reduction). Otherwise, keep the input scale as we can't pick a per-row scale.
        //
        // Note: `scalar_arguments` contains the original literal values (pre-coercion), so
        // integer literals may appear as Int64 even though the signature coerces them to Int32.
        let decimal_places: Option<i32> = match args.scalar_arguments.get(1) {
            None => Some(0),    // No dp argument means default to 0
            Some(None) => None, // dp is not a literal (e.g. column)
            Some(Some(scalar)) if scalar.is_null() => Some(0), // null dp => output is null
            Some(Some(scalar)) => Some(decimal_places_from_scalar(scalar)?),
        };

        // Calculate return type based on input type
        // For decimals: reduce scale to decimal_places (reclaims precision for integer part)
        // This matches Spark/DuckDB behavior where ROUND adjusts the scale
        // BUT only if dp is a scalar literal - otherwise keep original scale and add
        // extra precision to accommodate potential carry-over.
        let return_type =
            match input_type {
                Float32 => Float32,
                Decimal32(precision, scale) => calculate_new_precision_scale::<
                    Decimal32Type,
                >(
                    *precision, *scale, decimal_places
                )?,
                Decimal64(precision, scale) => calculate_new_precision_scale::<
                    Decimal64Type,
                >(
                    *precision, *scale, decimal_places
                )?,
                Decimal128(precision, scale) => calculate_new_precision_scale::<
                    Decimal128Type,
                >(
                    *precision, *scale, decimal_places
                )?,
                Decimal256(precision, scale) => calculate_new_precision_scale::<
                    Decimal256Type,
                >(
                    *precision, *scale, decimal_places
                )?,
                _ => Float64,
            };

        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), return_type, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("use return_field_from_args instead")
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

        if let (ColumnarValue::Scalar(value_scalar), ColumnarValue::Scalar(dp_scalar)) =
            (&args.args[0], decimal_places)
        {
            if value_scalar.is_null() || dp_scalar.is_null() {
                return ColumnarValue::Scalar(ScalarValue::Null)
                    .cast_to(args.return_type(), None);
            }

            let dp = if let ScalarValue::Int32(Some(dp)) = dp_scalar {
                *dp
            } else {
                return internal_err!(
                    "Unexpected datatype for decimal_places: {}",
                    dp_scalar.data_type()
                );
            };

            match (value_scalar, args.return_type()) {
                (ScalarValue::Float32(Some(v)), _) => {
                    let rounded = round_float(*v, dp)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::from(rounded)))
                }
                (ScalarValue::Float64(Some(v)), _) => {
                    let rounded = round_float(*v, dp)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::from(rounded)))
                }
                (
                    ScalarValue::Decimal32(Some(v), in_precision, scale),
                    Decimal32(out_precision, out_scale),
                ) => {
                    let rounded =
                        round_decimal_or_zero(*v, *in_precision, *scale, *out_scale, dp)?;
                    let rounded = if *out_precision == Decimal32Type::MAX_PRECISION
                        && *scale == 0
                        && dp < 0
                    {
                        // With scale == 0 and negative dp, rounding can carry into an additional
                        // digit (e.g. 99 -> 100). If we're already at max precision we can't widen
                        // the type, so validate and error rather than producing an invalid decimal.
                        validate_decimal_precision::<Decimal32Type>(
                            rounded,
                            *out_precision,
                            *out_scale,
                        )
                    } else {
                        Ok(rounded)
                    }?;
                    let scalar =
                        ScalarValue::Decimal32(Some(rounded), *out_precision, *out_scale);
                    Ok(ColumnarValue::Scalar(scalar))
                }
                (
                    ScalarValue::Decimal64(Some(v), in_precision, scale),
                    Decimal64(out_precision, out_scale),
                ) => {
                    let rounded =
                        round_decimal_or_zero(*v, *in_precision, *scale, *out_scale, dp)?;
                    let rounded = if *out_precision == Decimal64Type::MAX_PRECISION
                        && *scale == 0
                        && dp < 0
                    {
                        // See Decimal32 branch for details.
                        validate_decimal_precision::<Decimal64Type>(
                            rounded,
                            *out_precision,
                            *out_scale,
                        )
                    } else {
                        Ok(rounded)
                    }?;
                    let scalar =
                        ScalarValue::Decimal64(Some(rounded), *out_precision, *out_scale);
                    Ok(ColumnarValue::Scalar(scalar))
                }
                (
                    ScalarValue::Decimal128(Some(v), in_precision, scale),
                    Decimal128(out_precision, out_scale),
                ) => {
                    let rounded =
                        round_decimal_or_zero(*v, *in_precision, *scale, *out_scale, dp)?;
                    let rounded = if *out_precision == Decimal128Type::MAX_PRECISION
                        && *scale == 0
                        && dp < 0
                    {
                        // See Decimal32 branch for details.
                        validate_decimal_precision::<Decimal128Type>(
                            rounded,
                            *out_precision,
                            *out_scale,
                        )
                    } else {
                        Ok(rounded)
                    }?;
                    let scalar = ScalarValue::Decimal128(
                        Some(rounded),
                        *out_precision,
                        *out_scale,
                    );
                    Ok(ColumnarValue::Scalar(scalar))
                }
                (
                    ScalarValue::Decimal256(Some(v), in_precision, scale),
                    Decimal256(out_precision, out_scale),
                ) => {
                    let rounded =
                        round_decimal_or_zero(*v, *in_precision, *scale, *out_scale, dp)?;
                    let rounded = if *out_precision == Decimal256Type::MAX_PRECISION
                        && *scale == 0
                        && dp < 0
                    {
                        // See Decimal32 branch for details.
                        validate_decimal_precision::<Decimal256Type>(
                            rounded,
                            *out_precision,
                            *out_scale,
                        )
                    } else {
                        Ok(rounded)
                    }?;
                    let scalar = ScalarValue::Decimal256(
                        Some(rounded),
                        *out_precision,
                        *out_scale,
                    );
                    Ok(ColumnarValue::Scalar(scalar))
                }
                (ScalarValue::Null, _) => ColumnarValue::Scalar(ScalarValue::Null)
                    .cast_to(args.return_type(), None),
                (value_scalar, return_type) => {
                    internal_err!(
                        "Unexpected datatype for round(value, decimal_places): value {}, return type {}",
                        value_scalar.data_type(),
                        return_type
                    )
                }
            }
        } else {
            round_columnar(
                &args.args[0],
                decimal_places,
                args.number_rows,
                args.return_type(),
            )
        }
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
    return_type: &DataType,
) -> Result<ColumnarValue> {
    let value_array = value.to_array(number_rows)?;
    let both_scalars = matches!(value, ColumnarValue::Scalar(_))
        && matches!(decimal_places, ColumnarValue::Scalar(_));
    let decimal_places_is_array = matches!(decimal_places, ColumnarValue::Array(_));

    let arr: ArrayRef = match (value_array.data_type(), return_type) {
        (Float64, _) => {
            let result = calculate_binary_math::<Float64Type, Int32Type, Float64Type, _>(
                value_array.as_ref(),
                decimal_places,
                round_float::<f64>,
            )?;
            result as _
        }
        (Float32, _) => {
            let result = calculate_binary_math::<Float32Type, Int32Type, Float32Type, _>(
                value_array.as_ref(),
                decimal_places,
                round_float::<f32>,
            )?;
            result as _
        }
        (Decimal32(input_precision, scale), Decimal32(precision, new_scale)) => {
            // reduce scale to reclaim integer precision
            let result = calculate_binary_decimal_math::<
                Decimal32Type,
                Int32Type,
                Decimal32Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| {
                    let rounded = round_decimal_or_zero(
                        v,
                        *input_precision,
                        *scale,
                        *new_scale,
                        dp,
                    )?;
                    if *precision == Decimal32Type::MAX_PRECISION
                        && (decimal_places_is_array || (*scale == 0 && dp < 0))
                    {
                        // If we're already at max precision, we can't widen the result type. For
                        // dp arrays, or for scale == 0 with negative dp, rounding can overflow the
                        // fixed-precision type. Validate per-row and return an error instead of
                        // producing an invalid decimal that Arrow may display incorrectly.
                        validate_decimal_precision::<Decimal32Type>(
                            rounded, *precision, *new_scale,
                        )
                    } else {
                        Ok(rounded)
                    }
                },
                *precision,
                *new_scale,
            )?;
            result as _
        }
        (Decimal64(input_precision, scale), Decimal64(precision, new_scale)) => {
            let result = calculate_binary_decimal_math::<
                Decimal64Type,
                Int32Type,
                Decimal64Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| {
                    let rounded = round_decimal_or_zero(
                        v,
                        *input_precision,
                        *scale,
                        *new_scale,
                        dp,
                    )?;
                    if *precision == Decimal64Type::MAX_PRECISION
                        && (decimal_places_is_array || (*scale == 0 && dp < 0))
                    {
                        // See Decimal32 branch for details.
                        validate_decimal_precision::<Decimal64Type>(
                            rounded, *precision, *new_scale,
                        )
                    } else {
                        Ok(rounded)
                    }
                },
                *precision,
                *new_scale,
            )?;
            result as _
        }
        (Decimal128(input_precision, scale), Decimal128(precision, new_scale)) => {
            let result = calculate_binary_decimal_math::<
                Decimal128Type,
                Int32Type,
                Decimal128Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| {
                    let rounded = round_decimal_or_zero(
                        v,
                        *input_precision,
                        *scale,
                        *new_scale,
                        dp,
                    )?;
                    if *precision == Decimal128Type::MAX_PRECISION
                        && (decimal_places_is_array || (*scale == 0 && dp < 0))
                    {
                        // See Decimal32 branch for details.
                        validate_decimal_precision::<Decimal128Type>(
                            rounded, *precision, *new_scale,
                        )
                    } else {
                        Ok(rounded)
                    }
                },
                *precision,
                *new_scale,
            )?;
            result as _
        }
        (Decimal256(input_precision, scale), Decimal256(precision, new_scale)) => {
            let result = calculate_binary_decimal_math::<
                Decimal256Type,
                Int32Type,
                Decimal256Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| {
                    let rounded = round_decimal_or_zero(
                        v,
                        *input_precision,
                        *scale,
                        *new_scale,
                        dp,
                    )?;
                    if *precision == Decimal256Type::MAX_PRECISION
                        && (decimal_places_is_array || (*scale == 0 && dp < 0))
                    {
                        // See Decimal32 branch for details.
                        validate_decimal_precision::<Decimal256Type>(
                            rounded, *precision, *new_scale,
                        )
                    } else {
                        Ok(rounded)
                    }
                },
                *precision,
                *new_scale,
            )?;
            result as _
        }
        (other, _) => exec_err!("Unsupported data type {other:?} for function round")?,
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
    input_scale: i8,
    output_scale: i8,
    decimal_places: i32,
) -> Result<V, ArrowError> {
    let diff = i64::from(input_scale) - i64::from(decimal_places);
    if diff <= 0 {
        return Ok(value);
    }

    debug_assert!(diff <= i64::from(u32::MAX));
    let diff = diff as u32;

    let one = V::ONE;
    let two = V::from_usize(2).ok_or_else(|| {
        ArrowError::ComputeError("Internal error: could not create constant 2".into())
    })?;
    let ten = V::from_usize(10).ok_or_else(|| {
        ArrowError::ComputeError("Internal error: could not create constant 10".into())
    })?;

    let factor = ten.pow_checked(diff).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Overflow while rounding decimal with scale {input_scale} and decimal places {decimal_places}"
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

    // `quotient` is the rounded value at scale `decimal_places`. Rescale to the desired
    // `output_scale` (which is always >= `decimal_places` in cases where diff > 0).
    let scale_shift = i64::from(output_scale) - i64::from(decimal_places);
    if scale_shift == 0 {
        return Ok(quotient);
    }

    debug_assert!(scale_shift > 0);
    debug_assert!(scale_shift <= i64::from(u32::MAX));
    let scale_shift = scale_shift as u32;
    let shift_factor = ten.pow_checked(scale_shift).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Overflow while rounding decimal with scale {input_scale} and decimal places {decimal_places}"
        ))
    })?;
    quotient
        .mul_checked(shift_factor)
        .map_err(|_| ArrowError::ComputeError("Overflow while rounding decimal".into()))
}

fn round_decimal_or_zero<V: ArrowNativeTypeOp>(
    value: V,
    precision: u8,
    input_scale: i8,
    output_scale: i8,
    decimal_places: i32,
) -> Result<V, ArrowError> {
    if let Some(dp) =
        normalize_decimal_places_for_decimal(decimal_places, precision, input_scale)
    {
        round_decimal(value, input_scale, output_scale, dp)
    } else {
        V::from_usize(0).ok_or_else(|| {
            ArrowError::ComputeError("Internal error: could not create constant 0".into())
        })
    }
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
        // NOTE: For decimal inputs, the actual ROUND return type can differ from the
        // input type (scale reduction for literal `decimal_places`). These unit tests
        // only exercise Float32/Float64 behavior.
        let return_type = value.data_type().clone();
        let value = ColumnarValue::Array(value);
        let decimal_places = decimal_places
            .map(ColumnarValue::Array)
            .unwrap_or_else(|| ColumnarValue::Scalar(ScalarValue::Int32(Some(0))));

        let result =
            super::round_columnar(&value, &decimal_places, number_rows, &return_type)?;
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
