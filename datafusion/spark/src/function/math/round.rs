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

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::types::{
    NativeType, logical_float32, logical_float64, logical_int32,
};
use datafusion_common::{Result, ScalarValue, exec_err, not_impl_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};

/// Spark-compatible `round` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#round>
///
/// Rounds the value of `expr` to `scale` decimal places using HALF_UP rounding mode.
/// Returns the same type as the input expression.
///
/// - `round(expr)` rounds to 0 decimal places (default scale = 0)
/// - `round(expr, scale)` rounds to `scale` decimal places
/// - For integer types with negative scale: `round(25, -1)` → `30`
/// - Uses HALF_UP rounding: 2.5 → 3, -2.5 → -3 (away from zero)
///
/// Supported types: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
/// Float16, Float32, Float64, Decimal32, Decimal64, Decimal128, Decimal256
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRound {
    signature: Signature,
}

impl Default for SparkRound {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRound {
    pub fn new() -> Self {
        let decimal = Coercion::new_exact(TypeSignatureClass::Decimal);
        let integer = Coercion::new_exact(TypeSignatureClass::Integer);
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
                    // round(decimal, scale)
                    TypeSignature::Coercible(vec![
                        decimal.clone(),
                        decimal_places.clone(),
                    ]),
                    // round(decimal)
                    TypeSignature::Coercible(vec![decimal]),
                    // round(integer, scale)
                    TypeSignature::Coercible(vec![
                        integer.clone(),
                        decimal_places.clone(),
                    ]),
                    // round(integer)
                    TypeSignature::Coercible(vec![integer]),
                    // round(float32, scale)
                    TypeSignature::Coercible(vec![
                        float32.clone(),
                        decimal_places.clone(),
                    ]),
                    // round(float32)
                    TypeSignature::Coercible(vec![float32]),
                    // round(float64, scale)
                    TypeSignature::Coercible(vec![float64.clone(), decimal_places]),
                    // round(float64)
                    TypeSignature::Coercible(vec![float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkRound {
    fn name(&self) -> &str {
        "round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_round(&args.args, args.config_options.execution.enable_ansi_mode)
    }
}

/// Extract the scale (decimal places) from the second argument.
/// Returns `Some(0)` if no second argument is provided.
/// Returns `None` if the scale argument is NULL (Spark returns NULL for `round(expr, NULL)`).
fn get_scale(args: &[ColumnarValue]) -> Result<Option<i32>> {
    if args.len() < 2 {
        return Ok(Some(0));
    }

    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int8(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Ok(Some(*v)),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
            i32::try_from(*v).map(Some).map_err(|_| {
                (exec_err!("round scale {v} is out of supported i32 range")
                    as Result<(), _>)
                    .unwrap_err()
            })
        }
        ColumnarValue::Scalar(ScalarValue::UInt8(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::UInt16(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => {
            i32::try_from(*v).map(Some).map_err(|_| {
                (exec_err!("round scale {v} is out of supported i32 range")
                    as Result<(), _>)
                    .unwrap_err()
            })
        }
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(v))) => {
            i32::try_from(*v).map(Some).map_err(|_| {
                (exec_err!("round scale {v} is out of supported i32 range")
                    as Result<(), _>)
                    .unwrap_err()
            })
        }
        ColumnarValue::Scalar(sv) if sv.is_null() => Ok(None),
        other => exec_err!("Unsupported type for round scale: {}", other.data_type()),
    }
}

/// Round a floating-point value to the given number of decimal places using
/// HALF_UP rounding mode (ties round away from zero).
///
/// This matches Spark's `RoundBase` behaviour for `FloatType` / `DoubleType`,
/// which internally converts the value to `BigDecimal` and rounds with
/// `RoundingMode.HALF_UP`.
///
/// # Arguments
/// * `value` – the floating-point number to round
/// * `scale` – number of decimal places to keep.
///   - `scale >= 0`: rounds to that many fractional digits
///     (e.g. `round_float(2.345, 2) == 2.35`)
///   - `scale < 0`:  rounds to the left of the decimal point
///     (e.g. `round_float(125.0, -1) == 130.0`)
///
/// # Examples
/// ```text
/// round_float(2.5,  0) →  3.0   // half rounds up
/// round_float(-2.5, 0) → -3.0   // half rounds away from zero
/// round_float(1.4,  0) →  1.0
/// round_float(125.0, -1) → 130.0
/// ```
fn round_float<T: num_traits::Float>(value: T, scale: i32) -> T {
    if scale >= 0 {
        let factor = T::from(10.0f64.powi(scale)).unwrap_or_else(T::infinity);
        if factor.is_infinite() {
            // Very large positive scale — value is already precise enough, return as-is
            return value;
        }
        (value * factor).round() / factor
    } else {
        let factor = T::from(10.0f64.powi(-scale)).unwrap_or_else(T::infinity);
        if factor.is_infinite() {
            // Very large negative scale — any finite value rounds to 0
            return T::zero();
        }
        (value / factor).round() * factor
    }
}

/// Round an integer value to the given scale using HALF_UP rounding mode.
///
/// Only meaningful when `scale` is negative — a non-negative scale leaves
/// the integer unchanged because integers have no fractional part.
///
/// This matches Spark's `RoundBase` behaviour for `ByteType`, `ShortType`,
/// `IntegerType`, and `LongType`, which round to the nearest power-of-ten
/// boundary and return the same integer type.
///
/// In ANSI mode, overflow conditions return an error instead of wrapping.
///
/// # Arguments
/// * `value` – the integer to round (widened to `i64` by callers)
/// * `scale` – rounding position relative to the ones digit.
///   - `scale >= 0`:  returns `value` as-is
///   - `scale == -1`: rounds to the nearest 10
///   - `scale == -2`: rounds to the nearest 100
///   - If `10^|scale|` overflows `i64`, returns `0`
/// * `enable_ansi_mode` – when true, overflow returns an error
///
/// # Examples
/// ```text
/// round_integer(25,   -1, false) →  Ok(30)
/// round_integer(-25,  -1, false) → Ok(-30)
/// round_integer(123,  -1, false) →  Ok(120)
/// round_integer(150,  -2, false) →  Ok(200)
/// round_integer(42,    2, false) →   Ok(42)   // no-op for positive scale
/// round_integer(42,  -10, false) →    Ok(0)   // factor overflows → 0
/// ```
fn round_integer(value: i64, scale: i32, enable_ansi_mode: bool) -> Result<i64> {
    if scale >= 0 {
        return Ok(value);
    }
    let abs_scale = (-scale) as u32;
    let Some(factor) = 10_i64.checked_pow(abs_scale) else {
        return Ok(0);
    };
    let remainder = value % factor;
    let threshold = factor / 2;
    let result = if remainder >= threshold {
        if enable_ansi_mode {
            value
                .checked_sub(remainder)
                .and_then(|v| v.checked_add(factor))
                .ok_or_else(|| {
                    (exec_err!("Int64 overflow on round({value}, {scale})")
                        as Result<(), _>)
                        .unwrap_err()
                })?
        } else {
            value.wrapping_sub(remainder).wrapping_add(factor)
        }
    } else if remainder <= -threshold {
        if enable_ansi_mode {
            value
                .checked_sub(remainder)
                .and_then(|v| v.checked_sub(factor))
                .ok_or_else(|| {
                    (exec_err!("Int64 overflow on round({value}, {scale})")
                        as Result<(), _>)
                        .unwrap_err()
                })?
        } else {
            value.wrapping_sub(remainder).wrapping_sub(factor)
        }
    } else {
        value - remainder
    };
    Ok(result)
}

// ---------------------------------------------------------------------------
// Decimal rounding using ArrowNativeTypeOp (HALF_UP)
// ---------------------------------------------------------------------------

/// Round a decimal value represented as its unscaled integer using HALF_UP
/// rounding mode (ties round away from zero).
///
/// This matches Spark's `RoundBase` behaviour for `DecimalType`, which calls
/// `BigDecimal.setScale(scale, RoundingMode.HALF_UP)`.
///
/// Decimals are stored as `(unscaled_value, precision, scale)` where the real
/// value equals `unscaled_value * 10^(-scale)`.  This function operates on the
/// unscaled integer directly:
///
/// 1. Compute `diff = input_scale - decimal_places`.
///    If `diff <= 0` the requested precision is finer than (or equal to) the
///    stored scale, so nothing needs to be rounded — return as-is.
/// 2. Divide by `10^diff` to shift the rounding boundary into the ones digit.
/// 3. Inspect the remainder to decide whether to round up or down (HALF_UP).
/// 4. Multiply back by `10^diff` so the result is expressed at the original
///    `input_scale`.
///
/// # Arguments
/// * `value`          – unscaled decimal value
/// * `input_scale`    – scale of the incoming decimal
/// * `decimal_places` – number of fractional digits to keep (may be negative)
///
/// # Returns
/// The rounded unscaled value at the same `input_scale`, or an error
/// on overflow.
///
/// # Examples
/// ```text
/// // 2.5 (unscaled 25, scale 1) rounded to 0 places → 3.0 (unscaled 30)
/// round_decimal(25_i128, 1, 0)  → Ok(30)
///
/// // 2.345 (unscaled 2345, scale 3) rounded to 2 places → 2.350 (unscaled 2350)
/// round_decimal(2345_i128, 3, 2) → Ok(2350)
/// ```
fn round_decimal<V: ArrowNativeTypeOp>(
    value: V,
    input_scale: i8,
    decimal_places: i32,
) -> Result<V> {
    let diff = i64::from(input_scale) - i64::from(decimal_places);
    if diff <= 0 {
        // Nothing to round – the requested precision is finer than (or equal to) the
        // stored scale.
        return Ok(value);
    }

    let diff = diff as u32;

    let one = V::ONE;
    let two = V::from_usize(2).ok_or_else(|| {
        (exec_err!("Internal error: could not create constant 2") as Result<(), _>)
            .unwrap_err()
    })?;
    let ten = V::from_usize(10).ok_or_else(|| {
        (exec_err!("Internal error: could not create constant 10") as Result<(), _>)
            .unwrap_err()
    })?;

    let Ok(factor) = ten.pow_checked(diff) else {
        // 10^diff overflows the decimal type — the rounding position is beyond
        // the representable range, so any value rounds to 0.
        // This matches Spark's BigDecimal.setScale behavior where rounding to a
        // scale far beyond the number's magnitude yields 0.
        return Ok(V::ZERO);
    };

    let mut quotient = value.div_wrapping(factor);
    let remainder = value.mod_wrapping(factor);

    // HALF_UP: round away from zero when remainder is exactly half
    let threshold = factor.div_wrapping(two);
    if remainder >= threshold {
        quotient = quotient.add_checked(one).map_err(|_| {
            (exec_err!("Overflow while rounding decimal") as Result<(), _>).unwrap_err()
        })?;
    } else if remainder <= threshold.neg_wrapping() {
        quotient = quotient.sub_checked(one).map_err(|_| {
            (exec_err!("Overflow while rounding decimal") as Result<(), _>).unwrap_err()
        })?;
    }

    // Re-scale the quotient back to `input_scale` so the returned unscaled integer is
    // at the original scale. `factor` is already `10^diff` which is exactly the shift
    // we need.
    quotient.mul_checked(factor).map_err(|_| {
        (exec_err!("Overflow while rounding decimal") as Result<(), _>).unwrap_err()
    })
}

// ---------------------------------------------------------------------------
// Macros for array dispatch
// ---------------------------------------------------------------------------

macro_rules! impl_integer_array_round {
    ($array:expr, $arrow_type:ty, $scale:expr, $enable_ansi_mode:expr) => {{
        let array = $array.as_primitive::<$arrow_type>();
        type Native = <$arrow_type as arrow::datatypes::ArrowPrimitiveType>::Native;
        let result: PrimitiveArray<$arrow_type> = if $enable_ansi_mode {
            array.try_unary(|x| {
                let v = round_integer(x as i64, $scale, true)?;
                Native::try_from(v).map_err(|_| {
                    (exec_err!(
                        "{} overflow on round({x}, {})",
                        stringify!($arrow_type),
                        $scale
                    ) as Result<(), _>)
                        .unwrap_err()
                })
            })?
        } else {
            array.unary(|x| round_integer(x as i64, $scale, false).unwrap() as Native)
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

macro_rules! impl_float_array_round {
    ($array:expr, $arrow_type:ty, $scale:expr) => {{
        let array = $array.as_primitive::<$arrow_type>();
        let result: PrimitiveArray<$arrow_type> = array.unary(|x| round_float(x, $scale));
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

macro_rules! impl_decimal_array_round {
    ($array:expr, $arrow_type:ty, $input_scale:expr, $scale:expr) => {{
        let array = $array.as_primitive::<$arrow_type>();
        let result: PrimitiveArray<$arrow_type> = array
            .try_unary(|x| round_decimal(x, $input_scale, $scale))?
            .with_data_type($array.data_type().clone());
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

// ---------------------------------------------------------------------------
// Core dispatch
// ---------------------------------------------------------------------------

fn spark_round(args: &[ColumnarValue], enable_ansi_mode: bool) -> Result<ColumnarValue> {
    if args.is_empty() || args.len() > 2 {
        return exec_err!("round requires 1 or 2 arguments, got {}", args.len());
    }

    let scale = match get_scale(args)? {
        Some(s) => s,
        None => {
            // NULL scale → return NULL with the same data type as the first argument
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                args[0].data_type(),
            )?));
        }
    };

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null => Ok(args[0].clone()),

            // Integer types
            DataType::Int8 => {
                impl_integer_array_round!(array, Int8Type, scale, enable_ansi_mode)
            }
            DataType::Int16 => {
                impl_integer_array_round!(array, Int16Type, scale, enable_ansi_mode)
            }
            DataType::Int32 => {
                impl_integer_array_round!(array, Int32Type, scale, enable_ansi_mode)
            }
            DataType::Int64 => {
                impl_integer_array_round!(array, Int64Type, scale, enable_ansi_mode)
            }

            // Unsigned integer types
            DataType::UInt8 => {
                impl_integer_array_round!(array, UInt8Type, scale, enable_ansi_mode)
            }
            DataType::UInt16 => {
                impl_integer_array_round!(array, UInt16Type, scale, enable_ansi_mode)
            }
            DataType::UInt32 => {
                impl_integer_array_round!(array, UInt32Type, scale, enable_ansi_mode)
            }
            DataType::UInt64 => {
                let array = array.as_primitive::<UInt64Type>();
                let result: PrimitiveArray<UInt64Type> = array.try_unary(|x| {
                    let v_i64 = i64::try_from(x).map_err(|_| {
                        (exec_err!(
                            "round: UInt64 value {x} exceeds i64::MAX and cannot be rounded"
                        ) as Result<(), _>)
                            .unwrap_err()
                    })?;
                    round_integer(v_i64, scale, enable_ansi_mode)
                        .map(|v| v as u64)
                })?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // Float types
            DataType::Float16 => impl_float_array_round!(array, Float16Type, scale),
            DataType::Float32 => impl_float_array_round!(array, Float32Type, scale),
            DataType::Float64 => impl_float_array_round!(array, Float64Type, scale),

            // Decimal types
            DataType::Decimal32(_, input_scale) => {
                impl_decimal_array_round!(array, Decimal32Type, *input_scale, scale)
            }
            DataType::Decimal64(_, input_scale) => {
                impl_decimal_array_round!(array, Decimal64Type, *input_scale, scale)
            }
            DataType::Decimal128(_, input_scale) => {
                impl_decimal_array_round!(array, Decimal128Type, *input_scale, scale)
            }
            DataType::Decimal256(_, input_scale) => {
                impl_decimal_array_round!(array, Decimal256Type, *input_scale, scale)
            }

            dt => not_impl_err!("Unsupported data type for Spark round(): {dt}"),
        },

        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Null => Ok(args[0].clone()),
            _ if sv.is_null() => Ok(args[0].clone()),

            // Integer scalars
            ScalarValue::Int8(Some(v)) => {
                let r = round_integer(i64::from(*v), scale, enable_ansi_mode)?;
                let result = if enable_ansi_mode {
                    i8::try_from(r).map_err(|_| {
                        (exec_err!("Int8 overflow on round({v}, {scale})")
                            as Result<(), _>)
                            .unwrap_err()
                    })?
                } else {
                    r as i8
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(result))))
            }
            ScalarValue::Int16(Some(v)) => {
                let r = round_integer(i64::from(*v), scale, enable_ansi_mode)?;
                let result = if enable_ansi_mode {
                    i16::try_from(r).map_err(|_| {
                        (exec_err!("Int16 overflow on round({v}, {scale})")
                            as Result<(), _>)
                            .unwrap_err()
                    })?
                } else {
                    r as i16
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(result))))
            }
            ScalarValue::Int32(Some(v)) => {
                let r = round_integer(i64::from(*v), scale, enable_ansi_mode)?;
                let result = if enable_ansi_mode {
                    i32::try_from(r).map_err(|_| {
                        (exec_err!("Int32 overflow on round({v}, {scale})")
                            as Result<(), _>)
                            .unwrap_err()
                    })?
                } else {
                    r as i32
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(result))))
            }
            ScalarValue::Int64(Some(v)) => {
                let result = round_integer(*v, scale, enable_ansi_mode)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(result))))
            }

            // Unsigned integer scalars
            ScalarValue::UInt8(Some(v)) => {
                let r = round_integer(i64::from(*v), scale, enable_ansi_mode)?;
                let result = if enable_ansi_mode {
                    u8::try_from(r).map_err(|_| {
                        (exec_err!("UInt8 overflow on round({v}, {scale})")
                            as Result<(), _>)
                            .unwrap_err()
                    })?
                } else {
                    r as u8
                };
                Ok(ColumnarValue::Scalar(ScalarValue::UInt8(Some(result))))
            }
            ScalarValue::UInt16(Some(v)) => {
                let r = round_integer(i64::from(*v), scale, enable_ansi_mode)?;
                let result = if enable_ansi_mode {
                    u16::try_from(r).map_err(|_| {
                        (exec_err!("UInt16 overflow on round({v}, {scale})")
                            as Result<(), _>)
                            .unwrap_err()
                    })?
                } else {
                    r as u16
                };
                Ok(ColumnarValue::Scalar(ScalarValue::UInt16(Some(result))))
            }
            ScalarValue::UInt32(Some(v)) => {
                let r = round_integer(i64::from(*v), scale, enable_ansi_mode)?;
                let result = if enable_ansi_mode {
                    u32::try_from(r).map_err(|_| {
                        (exec_err!("UInt32 overflow on round({v}, {scale})")
                            as Result<(), _>)
                            .unwrap_err()
                    })?
                } else {
                    r as u32
                };
                Ok(ColumnarValue::Scalar(ScalarValue::UInt32(Some(result))))
            }
            ScalarValue::UInt64(Some(v)) => {
                let v_i64 = i64::try_from(*v).map_err(|_| {
                    (exec_err!(
                        "round: UInt64 value {v} exceeds i64::MAX and cannot be rounded"
                    ) as Result<(), _>)
                        .unwrap_err()
                })?;
                let result = round_integer(v_i64, scale, enable_ansi_mode)?;
                Ok(ColumnarValue::Scalar(ScalarValue::UInt64(Some(
                    result as u64,
                ))))
            }

            // Float scalars
            ScalarValue::Float16(Some(v)) => {
                let result = round_float(*v, scale);
                Ok(ColumnarValue::Scalar(ScalarValue::Float16(Some(result))))
            }
            ScalarValue::Float32(Some(v)) => {
                let result = round_float(*v, scale);
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(result))))
            }
            ScalarValue::Float64(Some(v)) => {
                let result = round_float(*v, scale);
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(result))))
            }

            // Decimal scalars
            ScalarValue::Decimal32(Some(v), precision, input_scale) => {
                let rounded = round_decimal(*v, *input_scale, scale)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal32(
                    Some(rounded),
                    *precision,
                    *input_scale,
                )))
            }
            ScalarValue::Decimal64(Some(v), precision, input_scale) => {
                let rounded = round_decimal(*v, *input_scale, scale)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal64(
                    Some(rounded),
                    *precision,
                    *input_scale,
                )))
            }
            ScalarValue::Decimal128(Some(v), precision, input_scale) => {
                let rounded = round_decimal(*v, *input_scale, scale)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(rounded),
                    *precision,
                    *input_scale,
                )))
            }
            ScalarValue::Decimal256(Some(v), precision, input_scale) => {
                let rounded = round_decimal(*v, *input_scale, scale)?;
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                    Some(rounded),
                    *precision,
                    *input_scale,
                )))
            }

            dt => not_impl_err!("Unsupported data type for Spark round(): {dt}"),
        },
    }
}
