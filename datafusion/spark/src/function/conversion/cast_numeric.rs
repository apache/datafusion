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

use crate::function::conversion::cast_utils::{
    cast_overflow, format_decimal_str, numeric_value_out_of_range, EvalMode,
};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Decimal128Array, Decimal128Builder, Float32Array,
    Float64Array, GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array,
    OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::{
    is_validate_decimal_precision, ArrowPrimitiveType, DataType, Decimal128Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion_common::Result;
use num_traits::{AsPrimitive, ToPrimitive, Zero};
use std::sync::Arc;

// --- Float → String macros ---

macro_rules! cast_float_to_string {
    ($from:expr, $eval_mode:expr, $type:ty, $output_type:ty, $offset_type:ty) => {{
        fn cast<OffsetSize>(from: &dyn Array, _eval_mode: EvalMode) -> Result<ArrayRef>
        where
            OffsetSize: OffsetSizeTrait,
        {
            let array = from.as_any().downcast_ref::<$output_type>().unwrap();

            // If the absolute number is less than 10,000,000 and greater or equal than 0.001,
            // the result is expressed without scientific notation with at least one digit on
            // either side of the decimal point. Otherwise, Spark uses scientific notation.
            const LOWER_SCIENTIFIC_BOUND: $type = 0.001;
            const UPPER_SCIENTIFIC_BOUND: $type = 10000000.0;

            let output_array = array
                .iter()
                .map(|value| match value {
                    Some(value) if value == <$type>::INFINITY => {
                        Ok(Some("Infinity".to_string()))
                    }
                    Some(value) if value == <$type>::NEG_INFINITY => {
                        Ok(Some("-Infinity".to_string()))
                    }
                    Some(value)
                        if (value.abs() < UPPER_SCIENTIFIC_BOUND
                            && value.abs() >= LOWER_SCIENTIFIC_BOUND)
                            || value.abs() == 0.0 =>
                    {
                        let trailing_zero = if value.fract() == 0.0 { ".0" } else { "" };
                        Ok(Some(format!("{value}{trailing_zero}")))
                    }
                    Some(value)
                        if value.abs() >= UPPER_SCIENTIFIC_BOUND
                            || value.abs() < LOWER_SCIENTIFIC_BOUND =>
                    {
                        let formatted = format!("{value:E}");
                        if formatted.contains('.') {
                            Ok(Some(formatted))
                        } else {
                            let prepare_number: Vec<&str> = formatted.split('E').collect();
                            let coefficient = prepare_number[0];
                            let exponent = prepare_number[1];
                            Ok(Some(format!("{coefficient}.0E{exponent}")))
                        }
                    }
                    Some(value) => Ok(Some(value.to_string())),
                    _ => Ok(None),
                })
                .collect::<Result<GenericStringArray<OffsetSize>>>()?;

            Ok(Arc::new(output_array))
        }

        cast::<$offset_type>($from, $eval_mode)
    }};
}

// --- Int → Int narrowing macros ---

macro_rules! cast_int_to_int_macro {
    (
        $array:expr,
        $eval_mode:expr,
        $from_arrow_primitive_type:ty,
        $to_arrow_primitive_type:ty,
        $from_data_type:expr,
        $to_native_type:ty,
        $spark_from_data_type_name:expr,
        $spark_to_data_type_name:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<PrimitiveArray<$from_arrow_primitive_type>>()
            .unwrap();
        let spark_int_literal_suffix = match $from_data_type {
            &DataType::Int64 => "L",
            &DataType::Int16 => "S",
            &DataType::Int8 => "T",
            _ => "",
        };

        let output_array = match $eval_mode {
            EvalMode::Legacy => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        Ok::<Option<$to_native_type>, datafusion_common::DataFusionError>(Some(
                            value as $to_native_type,
                        ))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<PrimitiveArray<$to_arrow_primitive_type>>>(),
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let res = <$to_native_type>::try_from(value);
                        if res.is_err() {
                            Err(cast_overflow(
                                &(value.to_string() + spark_int_literal_suffix),
                                $spark_from_data_type_name,
                                $spark_to_data_type_name,
                            ))
                        } else {
                            Ok::<Option<$to_native_type>, datafusion_common::DataFusionError>(
                                Some(res.unwrap()),
                            )
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<PrimitiveArray<$to_arrow_primitive_type>>>(),
        }?;
        let result: Result<ArrayRef> = Ok(Arc::new(output_array) as ArrayRef);
        result
    }};
}

// --- Float/Decimal → Int macros ---

// When Spark casts to Byte/Short types, it casts to Int first then to Byte/Short.
macro_rules! cast_float_to_int16_down {
    (
        $array:expr,
        $eval_mode:expr,
        $src_array_type:ty,
        $dest_array_type:ty,
        $rust_src_type:ty,
        $rust_dest_type:ty,
        $src_type_str:expr,
        $dest_type_str:expr,
        $format_str:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<$src_array_type>()
            .expect(concat!("Expected a ", stringify!($src_array_type)));

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let is_overflow = value.is_nan() || value.abs() as i32 == i32::MAX;
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!($format_str, value).replace("e", "E"),
                                $src_type_str,
                                $dest_type_str,
                            ));
                        }
                        let i32_value = value as i32;
                        <$rust_dest_type>::try_from(i32_value)
                            .map_err(|_| {
                                cast_overflow(
                                    &format!($format_str, value).replace("e", "E"),
                                    $src_type_str,
                                    $dest_type_str,
                                )
                            })
                            .map(Some)
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let i32_value = value as i32;
                        Ok::<Option<$rust_dest_type>, datafusion_common::DataFusionError>(Some(
                            i32_value as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

macro_rules! cast_float_to_int32_up {
    (
        $array:expr,
        $eval_mode:expr,
        $src_array_type:ty,
        $dest_array_type:ty,
        $rust_src_type:ty,
        $rust_dest_type:ty,
        $src_type_str:expr,
        $dest_type_str:expr,
        $max_dest_val:expr,
        $format_str:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<$src_array_type>()
            .expect(concat!("Expected a ", stringify!($src_array_type)));

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let is_overflow =
                            value.is_nan() || value.abs() as $rust_dest_type == $max_dest_val;
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!($format_str, value).replace("e", "E"),
                                $src_type_str,
                                $dest_type_str,
                            ));
                        }
                        Ok(Some(value as $rust_dest_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        Ok::<Option<$rust_dest_type>, datafusion_common::DataFusionError>(Some(
                            value as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

macro_rules! cast_decimal_to_int16_down {
    (
        $array:expr,
        $eval_mode:expr,
        $dest_array_type:ty,
        $rust_dest_type:ty,
        $dest_type_str:expr,
        $precision:expr,
        $scale:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Expected a Decimal128ArrayType");

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let truncated = value / divisor;
                        let is_overflow = truncated.abs() > i32::MAX.into();
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!(
                                    "{}BD",
                                    format_decimal_str(
                                        &value.to_string(),
                                        $precision as usize,
                                        $scale
                                    )
                                ),
                                &format!("DECIMAL({},{})", $precision, $scale),
                                $dest_type_str,
                            ));
                        }
                        let i32_value = truncated as i32;
                        <$rust_dest_type>::try_from(i32_value)
                            .map_err(|_| {
                                cast_overflow(
                                    &format!(
                                        "{}BD",
                                        format_decimal_str(
                                            &value.to_string(),
                                            $precision as usize,
                                            $scale
                                        )
                                    ),
                                    &format!("DECIMAL({},{})", $precision, $scale),
                                    $dest_type_str,
                                )
                            })
                            .map(Some)
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let i32_value = (value / divisor) as i32;
                        Ok::<Option<$rust_dest_type>, datafusion_common::DataFusionError>(Some(
                            i32_value as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

macro_rules! cast_decimal_to_int32_up {
    (
        $array:expr,
        $eval_mode:expr,
        $dest_array_type:ty,
        $rust_dest_type:ty,
        $dest_type_str:expr,
        $max_dest_val:expr,
        $precision:expr,
        $scale:expr
    ) => {{
        let cast_array = $array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Expected a Decimal128ArrayType");

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let truncated = value / divisor;
                        let is_overflow = truncated.abs() > $max_dest_val.into();
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!(
                                    "{}BD",
                                    format_decimal_str(
                                        &value.to_string(),
                                        $precision as usize,
                                        $scale
                                    )
                                ),
                                &format!("DECIMAL({},{})", $precision, $scale),
                                $dest_type_str,
                            ));
                        }
                        Ok(Some(truncated as $rust_dest_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let truncated = value / divisor;
                        Ok::<Option<$rust_dest_type>, datafusion_common::DataFusionError>(Some(
                            truncated as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

// --- Public functions ---

pub(crate) fn spark_cast_float64_to_utf8<OffsetSize>(
    from: &dyn Array,
    eval_mode: EvalMode,
) -> Result<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    cast_float_to_string!(from, eval_mode, f64, Float64Array, OffsetSize)
}

pub(crate) fn spark_cast_float32_to_utf8<OffsetSize>(
    from: &dyn Array,
    eval_mode: EvalMode,
) -> Result<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    cast_float_to_string!(from, eval_mode, f32, Float32Array, OffsetSize)
}

pub(crate) fn spark_cast_int_to_int(
    array: &dyn Array,
    eval_mode: EvalMode,
    from_type: &DataType,
    to_type: &DataType,
) -> Result<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Int64, DataType::Int32) => cast_int_to_int_macro!(
            array, eval_mode, Int64Type, Int32Type, from_type, i32, "BIGINT", "INT"
        ),
        (DataType::Int64, DataType::Int16) => cast_int_to_int_macro!(
            array, eval_mode, Int64Type, Int16Type, from_type, i16, "BIGINT", "SMALLINT"
        ),
        (DataType::Int64, DataType::Int8) => cast_int_to_int_macro!(
            array, eval_mode, Int64Type, Int8Type, from_type, i8, "BIGINT", "TINYINT"
        ),
        (DataType::Int32, DataType::Int16) => cast_int_to_int_macro!(
            array, eval_mode, Int32Type, Int16Type, from_type, i16, "INT", "SMALLINT"
        ),
        (DataType::Int32, DataType::Int8) => cast_int_to_int_macro!(
            array, eval_mode, Int32Type, Int8Type, from_type, i8, "INT", "TINYINT"
        ),
        (DataType::Int16, DataType::Int8) => cast_int_to_int_macro!(
            array, eval_mode, Int16Type, Int8Type, from_type, i8, "SMALLINT", "TINYINT"
        ),
        _ => unreachable!(
            "{}",
            format!("invalid integer type {to_type} in cast from {from_type}")
        ),
    }
}

pub(crate) fn spark_cast_nonintegral_numeric_to_integral(
    array: &dyn Array,
    eval_mode: EvalMode,
    from_type: &DataType,
    to_type: &DataType,
) -> Result<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Float32, DataType::Int8) => cast_float_to_int16_down!(
            array, eval_mode, Float32Array, Int8Array, f32, i8, "FLOAT", "TINYINT", "{:e}"
        ),
        (DataType::Float32, DataType::Int16) => cast_float_to_int16_down!(
            array, eval_mode, Float32Array, Int16Array, f32, i16, "FLOAT", "SMALLINT", "{:e}"
        ),
        (DataType::Float32, DataType::Int32) => cast_float_to_int32_up!(
            array, eval_mode, Float32Array, Int32Array, f32, i32, "FLOAT", "INT", i32::MAX, "{:e}"
        ),
        (DataType::Float32, DataType::Int64) => cast_float_to_int32_up!(
            array,
            eval_mode,
            Float32Array,
            Int64Array,
            f32,
            i64,
            "FLOAT",
            "BIGINT",
            i64::MAX,
            "{:e}"
        ),
        (DataType::Float64, DataType::Int8) => cast_float_to_int16_down!(
            array,
            eval_mode,
            Float64Array,
            Int8Array,
            f64,
            i8,
            "DOUBLE",
            "TINYINT",
            "{:e}D"
        ),
        (DataType::Float64, DataType::Int16) => cast_float_to_int16_down!(
            array,
            eval_mode,
            Float64Array,
            Int16Array,
            f64,
            i16,
            "DOUBLE",
            "SMALLINT",
            "{:e}D"
        ),
        (DataType::Float64, DataType::Int32) => cast_float_to_int32_up!(
            array,
            eval_mode,
            Float64Array,
            Int32Array,
            f64,
            i32,
            "DOUBLE",
            "INT",
            i32::MAX,
            "{:e}D"
        ),
        (DataType::Float64, DataType::Int64) => cast_float_to_int32_up!(
            array,
            eval_mode,
            Float64Array,
            Int64Array,
            f64,
            i64,
            "DOUBLE",
            "BIGINT",
            i64::MAX,
            "{:e}D"
        ),
        (DataType::Decimal128(precision, scale), DataType::Int8) => {
            cast_decimal_to_int16_down!(
                array, eval_mode, Int8Array, i8, "TINYINT", *precision, *scale
            )
        }
        (DataType::Decimal128(precision, scale), DataType::Int16) => {
            cast_decimal_to_int16_down!(
                array, eval_mode, Int16Array, i16, "SMALLINT", *precision, *scale
            )
        }
        (DataType::Decimal128(precision, scale), DataType::Int32) => {
            cast_decimal_to_int32_up!(
                array,
                eval_mode,
                Int32Array,
                i32,
                "INT",
                i32::MAX,
                *precision,
                *scale
            )
        }
        (DataType::Decimal128(precision, scale), DataType::Int64) => {
            cast_decimal_to_int32_up!(
                array,
                eval_mode,
                Int64Array,
                i64,
                "BIGINT",
                i64::MAX,
                *precision,
                *scale
            )
        }
        _ => unreachable!(
            "{}",
            format!("invalid cast from non-integral numeric type: {from_type} to integral numeric type: {to_type}")
        ),
    }
}

pub(crate) fn spark_cast_decimal_to_boolean(array: &dyn Array) -> Result<ArrayRef> {
    let decimal_array = array.as_primitive::<Decimal128Type>();
    let mut result = BooleanBuilder::with_capacity(decimal_array.len());
    for i in 0..decimal_array.len() {
        if decimal_array.is_null(i) {
            result.append_null()
        } else {
            result.append_value(!decimal_array.value(i).is_zero());
        }
    }
    Ok(Arc::new(result.finish()))
}

pub(crate) fn cast_int_to_decimal128(
    array: &dyn Array,
    eval_mode: EvalMode,
    from_type: &DataType,
    to_type: &DataType,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Int8, DataType::Decimal128(_p, _s)) => {
            cast_int_to_decimal128_internal::<Int8Type>(
                array.as_primitive::<Int8Type>(),
                precision,
                scale,
                eval_mode,
            )
        }
        (DataType::Int16, DataType::Decimal128(_p, _s)) => {
            cast_int_to_decimal128_internal::<Int16Type>(
                array.as_primitive::<Int16Type>(),
                precision,
                scale,
                eval_mode,
            )
        }
        (DataType::Int32, DataType::Decimal128(_p, _s)) => {
            cast_int_to_decimal128_internal::<Int32Type>(
                array.as_primitive::<Int32Type>(),
                precision,
                scale,
                eval_mode,
            )
        }
        (DataType::Int64, DataType::Decimal128(_p, _s)) => {
            cast_int_to_decimal128_internal::<Int64Type>(
                array.as_primitive::<Int64Type>(),
                precision,
                scale,
                eval_mode,
            )
        }
        _ => datafusion_common::internal_err!("Unsupported cast from datatype: {}", from_type),
    }
}

fn cast_int_to_decimal128_internal<T>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> Result<ArrayRef>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    let mut builder = Decimal128Builder::with_capacity(array.len());
    let multiplier = 10_i128.pow(scale as u32);

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let v = array.value(i).into();
            let scaled = v.checked_mul(multiplier);
            match scaled {
                Some(scaled) => {
                    if !is_validate_decimal_precision(scaled, precision) {
                        match eval_mode {
                            EvalMode::Ansi => {
                                return Err(numeric_value_out_of_range(
                                    &v.to_string(),
                                    precision,
                                    scale,
                                ));
                            }
                            EvalMode::Try | EvalMode::Legacy => builder.append_null(),
                        }
                    } else {
                        builder.append_value(scaled);
                    }
                }
                _ => match eval_mode {
                    EvalMode::Ansi => {
                        return Err(numeric_value_out_of_range(
                            &v.to_string(),
                            precision,
                            scale,
                        ))
                    }
                    EvalMode::Legacy | EvalMode::Try => builder.append_null(),
                },
            }
        }
    }
    Ok(Arc::new(
        builder.with_precision_and_scale(precision, scale)?.finish(),
    ))
}

pub(crate) fn cast_float64_to_decimal128(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> Result<ArrayRef> {
    cast_floating_point_to_decimal128::<Float64Type>(array, precision, scale, eval_mode)
}

pub(crate) fn cast_float32_to_decimal128(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> Result<ArrayRef> {
    cast_floating_point_to_decimal128::<Float32Type>(array, precision, scale, eval_mode)
}

fn cast_floating_point_to_decimal128<T: ArrowPrimitiveType>(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> Result<ArrayRef>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
{
    let input = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let mut cast_array = PrimitiveArray::<Decimal128Type>::builder(input.len());

    let mul = 10_f64.powi(scale as i32);

    for i in 0..input.len() {
        if input.is_null(i) {
            cast_array.append_null();
            continue;
        }

        let input_value = input.value(i).as_();
        if let Some(v) = (input_value * mul).round().to_i128()
            && is_validate_decimal_precision(v, precision)
        {
            cast_array.append_value(v);
            continue;
        }

        if eval_mode == EvalMode::Ansi {
            return Err(numeric_value_out_of_range(
                &input_value.to_string(),
                precision,
                scale,
            ));
        }
        cast_array.append_null();
    }

    let res = Arc::new(
        cast_array
            .with_precision_and_scale(precision, scale)?
            .finish(),
    ) as ArrayRef;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cast_int64_to_int32_legacy() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
        ]));
        let result =
            spark_cast_int_to_int(&array, EvalMode::Legacy, &DataType::Int64, &DataType::Int32)
                .unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.value(1), -1); // truncation
        assert_eq!(arr.value(2), 0); // truncation
        assert!(arr.is_null(3));
    }

    #[test]
    fn test_cast_int64_to_int32_ansi_overflow() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MAX)]));
        let result =
            spark_cast_int_to_int(&array, EvalMode::Ansi, &DataType::Int64, &DataType::Int32);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("[CAST_OVERFLOW]"));
    }

    #[test]
    fn test_cast_decimal_to_boolean() {
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(100), Some(0), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let result = spark_cast_decimal_to_boolean(&array).unwrap();
        let bool_arr = result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(bool_arr.value(0)); // 100 != 0 -> true
        assert!(!bool_arr.value(1)); // 0 -> false
        assert!(bool_arr.is_null(2));
    }

    #[test]
    fn test_cast_float_to_string_f64() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(42.0),
            Some(0.001),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
        ]));
        let result = spark_cast_float64_to_utf8::<i32>(&array, EvalMode::Legacy).unwrap();
        let str_arr = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(str_arr.value(0), "42.0");
        assert_eq!(str_arr.value(1), "0.001");
        assert_eq!(str_arr.value(2), "Infinity");
        assert_eq!(str_arr.value(3), "-Infinity");
        assert!(str_arr.is_null(4));
    }

    #[test]
    fn test_cast_float_to_decimal() {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(42.),
            Some(-42.4242415),
            Some(42e-314),
            Some(0.),
            Some(f64::INFINITY),
            Some(f64::NAN),
            None,
        ]));
        let b =
            cast_floating_point_to_decimal128::<Float64Type>(&a, 8, 6, EvalMode::Legacy).unwrap();
        assert_eq!(b.len(), a.len());
        let casted = b.as_primitive::<Decimal128Type>();
        assert_eq!(casted.value(0), 42000000);
        assert_eq!(casted.value(1), -42424242);
        assert_eq!(casted.value(2), 0);
        assert_eq!(casted.value(3), 0);
        assert!(casted.is_null(4)); // Infinity
        assert!(casted.is_null(5)); // NaN
        assert!(casted.is_null(6)); // null
    }
}
