// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{ArrowNativeTypeOp, AsArray, Decimal128Array};
use arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int64Type,
};
use datafusion_common::types::{NativeType, logical_int32};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};

/// Spark-compatible `ceil` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#ceil>
///
/// Differences with DataFusion ceil:
///  - Spark's ceil returns Int64 for float inputs; DataFusion preserves
///    the input type (Float32→Float32, Float64→Float64)
///  - Spark's ceil on Decimal128(p, s) returns Decimal128(p−s+1, 0), reducing scale
///    to 0; DataFusion preserves the original precision and scale
///  - Spark only supports Decimal128; DataFusion also supports Decimal32/64/256
///  - Spark does not check for decimal overflow; DataFusion errors on overflow
///
/// Two-arg form `ceil(decimal, scale)` matches Spark `RoundCeil`: decimal value, constant
/// Int32 scale, result type from Spark `RoundBase::dataType`. Extreme scales that would
/// wrap or exceed the result precision return an error (Spark: NUMERIC_VALUE_OUT_OF_RANGE).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCeil {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        let scale = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    // ceil(value)
                    TypeSignature::Numeric(1),
                    // ceil(decimal, scale)
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Decimal),
                        scale,
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["ceiling".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        let value_type = args.arg_fields[0].data_type();

        let data_type = if args.arg_fields.len() == 1 {
            self.return_type(std::slice::from_ref(value_type))?
        } else {
            // Spark requires a foldable scale: a non-constant one is a plan-time error there
            // (NON_FOLDABLE_INPUT) rather than something evaluated per row.
            let scale = match args.scalar_arguments.get(1).copied().flatten() {
                Some(ScalarValue::Int32(Some(scale))) => *scale,
                Some(ScalarValue::Int32(None)) => {
                    return plan_err!("Function ceil requires a non-null scale argument");
                }
                _ => {
                    return plan_err!(
                        "Function ceil requires a constant integer scale argument"
                    );
                }
            };

            match value_type {
                DataType::Decimal128(p, s) => DataType::Decimal128(
                    ceil_scaled_precision(*p, *s, scale),
                    ceil_scaled_scale(*s, scale),
                ),
                other => {
                    return plan_err!(
                        "Function ceil does not support a scale argument for {other:?}"
                    );
                }
            }
        };

        Ok(Arc::new(Field::new("ceil", data_type, nullable)))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Decimal128(p, s) => {
                if *s > 0 {
                    Ok(DataType::Decimal128(decimal128_ceil_precision(*p, *s), 0))
                } else {
                    // scale <= 0 means the value is already a whole number
                    // (or represents multiples of 10^(-scale)), so ceil is a no-op
                    Ok(DataType::Decimal128(*p, *s))
                }
            }
            dt if matches!(dt, DataType::Float32 | DataType::Float64)
                || dt.is_integer() =>
            {
                Ok(DataType::Int64)
            }
            other => exec_err!("Unsupported data type {other:?} for function ceil"),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_ceil(&args.args)
    }
}

fn spark_ceil(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match args {
        [input] => match input {
            ColumnarValue::Scalar(value) => spark_ceil_scalar(value),
            ColumnarValue::Array(input) => spark_ceil_array(input),
        },
        [input, scale] => spark_ceil_with_scale(input, scale),
        _ => exec_err!("Function ceil expects one or two arguments"),
    }
}

/// Result scale for `ceil(decimal(p, s), scale)`, per Spark's `RoundBase::dataType`.
#[inline]
fn ceil_scaled_scale(input_scale: i8, scale: i32) -> i8 {
    if scale < 0 {
        0
    } else {
        (input_scale as i32).min(scale) as i8
    }
}

/// Result precision for `ceil(decimal(p, s), scale)`, per Spark's `RoundBase::dataType`.
///
/// Rounding up can carry into a new integral digit (`ceil(9.9, 0)` is `10`), so the integral
/// part always gets one digit more than the input had.
#[inline]
fn ceil_scaled_precision(precision: u8, input_scale: i8, scale: i32) -> u8 {
    let integral_least_num_digits = precision as i32 - input_scale as i32 + 1;
    let new_precision = if scale < 0 {
        integral_least_num_digits.max(-scale + 1)
    } else {
        integral_least_num_digits + (input_scale as i32).min(scale)
    };
    new_precision.clamp(1, 38) as u8
}

/// True when `|value|` fits in `Decimal128(precision, _)` (unscaled absolute value < 10^p).
#[inline]
fn decimal128_fits_precision(value: i128, precision: u8) -> bool {
    match 10_i128.checked_pow(precision as u32) {
        Some(limit) => value > -limit && value < limit,
        // precision == 38: 10^38 does not fit in i128, but every i128 fits Decimal128(38, _).
        None => true,
    }
}

/// Round `value` up to `target_scale` decimal places, where `value` is held at `input_scale`.
///
/// Errors on arithmetic overflow or when the result does not fit the Spark result precision,
/// matching Spark's `NUMERIC_VALUE_OUT_OF_RANGE` for extreme negative scales.
fn decimal128_ceil_to_scale(
    value: i128,
    input_scale: i8,
    input_precision: u8,
    target_scale: i32,
) -> Result<i128> {
    let out_p = ceil_scaled_precision(input_precision, input_scale, target_scale);

    let result = if target_scale >= input_scale as i32 {
        // Already at least as fine as the target; keep the unscaled digits.
        // Spark still caps the *declared* result scale at min(s, scale).
        value
    } else {
        let dropped = (input_scale as i32 - target_scale) as u32;
        let rounded = decimal128_ceil_checked(value, dropped)?;

        if target_scale < 0 {
            // Result is stored at scale 0; restore the trailing zeros for the places
            // below the decimal point that were rounded away into the integer part.
            let Some(factor) = 10_i128.checked_pow((-target_scale) as u32) else {
                return exec_err!(
                    "Decimal overflow in ceil: 10^{} exceeds Decimal128 range",
                    -target_scale
                );
            };
            rounded.checked_mul(factor).ok_or_else(|| {
                (exec_err!("Decimal overflow in ceil while rescaling to scale 0")
                    as Result<()>)
                    .unwrap_err()
            })?
        } else {
            rounded
        }
    };

    if !decimal128_fits_precision(result, out_p) {
        return exec_err!(
            "Decimal overflow in ceil: result does not fit Decimal128({out_p}, {})",
            ceil_scaled_scale(input_scale, target_scale)
        );
    }
    Ok(result)
}

fn spark_ceil_with_scale(
    input: &ColumnarValue,
    scale: &ColumnarValue,
) -> Result<ColumnarValue> {
    let ColumnarValue::Scalar(scale) = scale else {
        return exec_err!("Function ceil requires a constant integer scale argument");
    };
    let ScalarValue::Int32(Some(scale)) = scale else {
        return exec_err!("Function ceil requires a non-null integer scale argument");
    };
    let scale = *scale;

    match input {
        ColumnarValue::Scalar(ScalarValue::Decimal128(value, p, s)) => {
            let out_p = ceil_scaled_precision(*p, *s, scale);
            let out_s = ceil_scaled_scale(*s, scale);
            let result = match value {
                Some(x) => Some(decimal128_ceil_to_scale(*x, *s, *p, scale)?),
                None => None,
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                result, out_p, out_s,
            )))
        }
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Decimal128(p, s) => {
                let (p, s) = (*p, *s);
                let out_p = ceil_scaled_precision(p, s, scale);
                let out_s = ceil_scaled_scale(s, scale);
                let result: Decimal128Array = array
                    .as_primitive::<Decimal128Type>()
                    .try_unary(|x| decimal128_ceil_to_scale(x, s, p, scale))?;
                Ok(ColumnarValue::Array(Arc::new(
                    result.with_data_type(DataType::Decimal128(out_p, out_s)),
                )))
            }
            other => {
                exec_err!("Function ceil does not support a scale argument for {other:?}")
            }
        },
        other => exec_err!(
            "Function ceil does not support a scale argument for {:?}",
            other.data_type()
        ),
    }
}

/// Compute ceil for a single decimal128 value with the given scale (1-arg path).
#[inline]
fn decimal128_ceil(value: i128, scale: u32) -> i128 {
    let div = 10_i128.pow_wrapping(scale);
    let d = value / div;
    let r = value % div;
    if r > 0 { d + 1 } else { d }
}

/// Checked variant used by `ceil(decimal, scale)`. Errors instead of wrapping when
/// `10^scale` itself overflows i128.
fn decimal128_ceil_checked(value: i128, scale: u32) -> Result<i128> {
    let Some(div) = 10_i128.checked_pow(scale) else {
        return exec_err!(
            "Decimal overflow in ceil: 10^{scale} exceeds Decimal128 range"
        );
    };
    let d = value / div;
    let r = value % div;
    if r > 0 {
        d.checked_add(1).ok_or_else(|| {
            (exec_err!("Decimal overflow in ceil") as Result<()>).unwrap_err()
        })
    } else {
        Ok(d)
    }
}

/// Compute the return precision for a decimal128 ceil result.
#[inline]
fn decimal128_ceil_precision(precision: u8, scale: i8) -> u8 {
    ((precision as i64) - (scale as i64) + 1).clamp(1, 38) as u8
}

fn spark_ceil_scalar(value: &ScalarValue) -> Result<ColumnarValue> {
    let result = match value {
        ScalarValue::Float32(v) => ScalarValue::Int64(v.map(|x| x.ceil() as i64)),
        ScalarValue::Float64(v) => ScalarValue::Int64(v.map(|x| x.ceil() as i64)),
        v if v.data_type().is_integer() => v.cast_to(&DataType::Int64)?,
        ScalarValue::Decimal128(v, p, s) if *s > 0 => {
            let new_p = decimal128_ceil_precision(*p, *s);
            ScalarValue::Decimal128(v.map(|x| decimal128_ceil(x, *s as u32)), new_p, 0)
        }
        ScalarValue::Decimal128(_, _, _) => value.clone(),
        other => {
            return exec_err!(
                "Unsupported data type {:?} for function ceil",
                other.data_type()
            );
        }
    };
    Ok(ColumnarValue::Scalar(result))
}

fn spark_ceil_array(input: &Arc<dyn arrow::array::Array>) -> Result<ColumnarValue> {
    let result = match input.data_type() {
        DataType::Float32 => Arc::new(
            input
                .as_primitive::<Float32Type>()
                .unary::<_, Int64Type>(|x| x.ceil() as i64),
        ) as _,
        DataType::Float64 => Arc::new(
            input
                .as_primitive::<Float64Type>()
                .unary::<_, Int64Type>(|x| x.ceil() as i64),
        ) as _,
        dt if dt.is_integer() => arrow::compute::cast(input, &DataType::Int64)?,
        DataType::Decimal128(p, s) if *s > 0 => {
            let new_p = decimal128_ceil_precision(*p, *s);
            let result: Decimal128Array = input
                .as_primitive::<Decimal128Type>()
                .unary(|x| decimal128_ceil(x, *s as u32));
            Arc::new(result.with_data_type(DataType::Decimal128(new_p, 0)))
        }
        DataType::Decimal128(_, _) => Arc::clone(input),
        other => return exec_err!("Unsupported data type {other:?} for function ceil"),
    };

    Ok(ColumnarValue::Array(result))
}

#[cfg(test)]
mod scale_tests {
    use super::*;

    /// Values match the PySpark 3.5.5 results recorded in ceil.slt.
    #[test]
    fn test_scale_matches_pyspark_cases() {
        // ceil(3.1411, 3) -> Decimal('3.142'), decimal(5,3)
        let value = 31411_i128; // 3.1411 at scale 4
        assert_eq!(decimal128_ceil_to_scale(value, 4, 5, 3).unwrap(), 3142);
        assert_eq!(ceil_scaled_precision(5, 4, 3), 5);
        assert_eq!(ceil_scaled_scale(4, 3), 3);

        // ceil(3.1411, -3) -> Decimal('1000'), decimal(4,0)
        assert_eq!(decimal128_ceil_to_scale(value, 4, 5, -3).unwrap(), 1000);
        assert_eq!(ceil_scaled_precision(5, 4, -3), 4);
        assert_eq!(ceil_scaled_scale(4, -3), 0);
    }

    #[test]
    fn test_scale_rounds_toward_positive_infinity() {
        assert_eq!(decimal128_ceil_to_scale(-31411, 4, 5, 3).unwrap(), -3141);
        assert_eq!(decimal128_ceil_to_scale(-31411, 4, 5, 0).unwrap(), -3);
    }

    #[test]
    fn test_scale_noop_when_request_ge_input_scale() {
        assert_eq!(decimal128_ceil_to_scale(31411, 4, 5, 4).unwrap(), 31411);
        assert_eq!(decimal128_ceil_to_scale(31411, 4, 5, 9).unwrap(), 31411);
        assert_eq!(ceil_scaled_scale(4, 9), 4);
        assert_eq!(ceil_scaled_precision(5, 4, 9), 6);
    }

    #[test]
    fn test_scale_exact_value_and_carry() {
        assert_eq!(decimal128_ceil_to_scale(31400, 4, 5, 2).unwrap(), 314);
        assert_eq!(decimal128_ceil_to_scale(99, 1, 2, 0).unwrap(), 10);
        assert_eq!(ceil_scaled_precision(2, 1, 0), 2);
    }

    #[test]
    fn test_scaled_precision_bounds() {
        assert_eq!(ceil_scaled_precision(38, 0, -40), 38);
        assert_eq!(ceil_scaled_precision(38, 2, 30), 38);
        assert_eq!(ceil_scaled_precision(1, 1, 0), 1);
    }

    #[test]
    fn test_scale_errors_on_extreme_negative_scale() {
        // Spark raises NUMERIC_VALUE_OUT_OF_RANGE for these; do not wrap.
        assert!(decimal128_ceil_to_scale(31411, 4, 5, -40).is_err());
        assert!(decimal128_ceil_to_scale(31411, 4, 5, -38).is_err());
        // ceil(10^38 - 1 at scale 0, -1) overflows Decimal128(38, 0)
        let max38 = 10_i128.pow(38) - 1;
        assert!(decimal128_ceil_to_scale(max38, 0, 38, -1).is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Float32Array, Float64Array, Int64Array};
    use datafusion_common::ScalarValue;

    #[test]
    fn test_ceil_float64() {
        let input = Float64Array::from(vec![
            Some(125.2345),
            Some(15.0001),
            Some(0.1),
            Some(-0.9),
            Some(-1.1),
            Some(123.0),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_ceil(&args).unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(
            result,
            &Int64Array::from(vec![
                Some(126),
                Some(16),
                Some(1),
                Some(0),
                Some(-1),
                Some(123),
                None,
            ])
        );
    }

    #[test]
    fn test_ceil_float32() {
        let input = Float32Array::from(vec![
            Some(125.2345f32),
            Some(15.0001f32),
            Some(0.1f32),
            Some(-0.9f32),
            Some(-1.1f32),
            Some(123.0f32),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_ceil(&args).unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(
            result,
            &Int64Array::from(vec![
                Some(126),
                Some(16),
                Some(1),
                Some(0),
                Some(-1),
                Some(123),
                None,
            ])
        );
    }

    #[test]
    fn test_ceil_int64() {
        let input = Int64Array::from(vec![Some(1), Some(-1), None]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_ceil(&args).unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result, &Int64Array::from(vec![Some(1), Some(-1), None]));
    }

    #[test]
    fn test_ceil_decimal128() {
        // Decimal128(10, 2): 150 = 1.50, -150 = -1.50, 100 = 1.00
        let return_type = DataType::Decimal128(9, 0);
        let input = Decimal128Array::from(vec![Some(150), Some(-150), Some(100), None])
            .with_data_type(DataType::Decimal128(10, 2));
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_ceil(&args).unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
        };
        let result = result.as_primitive::<Decimal128Type>();
        let expected = Decimal128Array::from(vec![Some(2), Some(-1), Some(1), None])
            .with_data_type(return_type);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_ceil_float64_scalar() {
        let input = ScalarValue::Float64(Some(-1.1));
        let args = vec![ColumnarValue::Scalar(input)];
        let ColumnarValue::Scalar(result) = spark_ceil(&args).unwrap() else {
            panic!("Expected scalar")
        };
        assert_eq!(result, ScalarValue::Int64(Some(-1)));
    }

    #[test]
    fn test_ceil_float32_scalar() {
        let input = ScalarValue::Float32(Some(125.2345f32));
        let args = vec![ColumnarValue::Scalar(input)];
        let ColumnarValue::Scalar(result) = spark_ceil(&args).unwrap() else {
            panic!("Expected scalar")
        };
        assert_eq!(result, ScalarValue::Int64(Some(126)));
    }

    #[test]
    fn test_ceil_int64_scalar() {
        let input = ScalarValue::Int64(Some(48));
        let args = vec![ColumnarValue::Scalar(input)];
        let ColumnarValue::Scalar(result) = spark_ceil(&args).unwrap() else {
            panic!("Expected scalar")
        };
        assert_eq!(result, ScalarValue::Int64(Some(48)));
    }
}
