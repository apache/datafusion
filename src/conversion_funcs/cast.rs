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

use crate::timezone;
use crate::utils::array_with_timezone;
use crate::{EvalMode, SparkError, SparkResult};
use arrow::{
    array::{
        cast::AsArray,
        types::{Date32Type, Int16Type, Int32Type, Int8Type},
        Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array,
        GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array, OffsetSizeTrait,
        PrimitiveArray,
    },
    compute::{cast_with_options, take, unary, CastOptions},
    datatypes::{
        ArrowPrimitiveType, Decimal128Type, DecimalType, Float32Type, Float64Type, Int64Type,
        TimestampMicrosecondType,
    },
    error::ArrowError,
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use arrow_array::builder::StringBuilder;
use arrow_array::{DictionaryArray, StringArray, StructArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Timelike};
use datafusion_common::{
    cast::as_generic_string_array, internal_err, Result as DataFusionResult, ScalarValue,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use num::{
    cast::AsPrimitive, integer::div_floor, traits::CheckedNeg, CheckedSub, Integer, Num,
    ToPrimitive,
};
use regex::Regex;
use std::str::FromStr;
use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::Hash,
    num::Wrapping,
    sync::Arc,
};

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");

const MICROS_PER_SECOND: i64 = 1000000;

static CAST_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

struct TimeStampInfo {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    microsecond: u32,
}

impl Default for TimeStampInfo {
    fn default() -> Self {
        TimeStampInfo {
            year: 1,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            microsecond: 0,
        }
    }
}

impl TimeStampInfo {
    pub fn with_year(&mut self, year: i32) -> &mut Self {
        self.year = year;
        self
    }

    pub fn with_month(&mut self, month: u32) -> &mut Self {
        self.month = month;
        self
    }

    pub fn with_day(&mut self, day: u32) -> &mut Self {
        self.day = day;
        self
    }

    pub fn with_hour(&mut self, hour: u32) -> &mut Self {
        self.hour = hour;
        self
    }

    pub fn with_minute(&mut self, minute: u32) -> &mut Self {
        self.minute = minute;
        self
    }

    pub fn with_second(&mut self, second: u32) -> &mut Self {
        self.second = second;
        self
    }

    pub fn with_microsecond(&mut self, microsecond: u32) -> &mut Self {
        self.microsecond = microsecond;
        self
    }
}

#[derive(Debug, Eq)]
pub struct Cast {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub cast_options: SparkCastOptions,
}

impl PartialEq for Cast {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.data_type.eq(&other.data_type)
            && self.cast_options.eq(&other.cast_options)
    }
}

impl Hash for Cast {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.data_type.hash(state);
        self.cast_options.hash(state);
    }
}

/// Determine if Comet supports a cast, taking options such as EvalMode and Timezone into account.
pub fn cast_supported(
    from_type: &DataType,
    to_type: &DataType,
    options: &SparkCastOptions,
) -> bool {
    use DataType::*;

    let from_type = if let Dictionary(_, dt) = from_type {
        dt
    } else {
        from_type
    };

    let to_type = if let Dictionary(_, dt) = to_type {
        dt
    } else {
        to_type
    };

    if from_type == to_type {
        return true;
    }

    match (from_type, to_type) {
        (Boolean, _) => can_cast_from_boolean(to_type, options),
        (UInt8 | UInt16 | UInt32 | UInt64, Int8 | Int16 | Int32 | Int64)
            if options.allow_cast_unsigned_ints =>
        {
            true
        }
        (Int8, _) => can_cast_from_byte(to_type, options),
        (Int16, _) => can_cast_from_short(to_type, options),
        (Int32, _) => can_cast_from_int(to_type, options),
        (Int64, _) => can_cast_from_long(to_type, options),
        (Float32, _) => can_cast_from_float(to_type, options),
        (Float64, _) => can_cast_from_double(to_type, options),
        (Decimal128(p, s), _) => can_cast_from_decimal(p, s, to_type, options),
        (Timestamp(_, None), _) => can_cast_from_timestamp_ntz(to_type, options),
        (Timestamp(_, Some(_)), _) => can_cast_from_timestamp(to_type, options),
        (Utf8 | LargeUtf8, _) => can_cast_from_string(to_type, options),
        (_, Utf8 | LargeUtf8) => can_cast_to_string(from_type, options),
        (Struct(from_fields), Struct(to_fields)) => from_fields
            .iter()
            .zip(to_fields.iter())
            .all(|(a, b)| cast_supported(a.data_type(), b.data_type(), options)),
        _ => false,
    }
}

fn can_cast_from_string(to_type: &DataType, options: &SparkCastOptions) -> bool {
    use DataType::*;
    match to_type {
        Boolean | Int8 | Int16 | Int32 | Int64 | Binary => true,
        Float32 | Float64 => {
            // https://github.com/apache/datafusion-comet/issues/326
            // Does not support inputs ending with 'd' or 'f'. Does not support 'inf'.
            // Does not support ANSI mode.
            options.allow_incompat
        }
        Decimal128(_, _) => {
            // https://github.com/apache/datafusion-comet/issues/325
            // Does not support inputs ending with 'd' or 'f'. Does not support 'inf'.
            // Does not support ANSI mode. Returns 0.0 instead of null if input contains no digits

            options.allow_incompat
        }
        Date32 | Date64 => {
            // https://github.com/apache/datafusion-comet/issues/327
            // Only supports years between 262143 BC and 262142 AD
            options.allow_incompat
        }
        Timestamp(_, _) if options.eval_mode == EvalMode::Ansi => {
            // ANSI mode not supported
            false
        }
        Timestamp(_, Some(tz)) if tz.as_ref() != "UTC" => {
            // Cast will use UTC instead of $timeZoneId
            options.allow_incompat
        }
        Timestamp(_, _) => {
            // https://github.com/apache/datafusion-comet/issues/328
            // Not all valid formats are supported
            options.allow_incompat
        }
        _ => false,
    }
}

fn can_cast_to_string(from_type: &DataType, options: &SparkCastOptions) -> bool {
    use DataType::*;
    match from_type {
        Boolean | Int8 | Int16 | Int32 | Int64 | Date32 | Date64 | Timestamp(_, _) => true,
        Float32 | Float64 => {
            // There can be differences in precision.
            // For example, the input \"1.4E-45\" will produce 1.0E-45 " +
            // instead of 1.4E-45"))
            true
        }
        Decimal128(_, _) => {
            // https://github.com/apache/datafusion-comet/issues/1068
            // There can be formatting differences in some case due to Spark using
            // scientific notation where Comet does not
            true
        }
        Binary => {
            // https://github.com/apache/datafusion-comet/issues/377
            // Only works for binary data representing valid UTF-8 strings
            options.allow_incompat
        }
        Struct(fields) => fields
            .iter()
            .all(|f| can_cast_to_string(f.data_type(), options)),
        _ => false,
    }
}

fn can_cast_from_timestamp_ntz(to_type: &DataType, options: &SparkCastOptions) -> bool {
    use DataType::*;
    match to_type {
        Timestamp(_, _) | Date32 | Date64 | Utf8 => {
            // incompatible
            options.allow_incompat
        }
        _ => {
            // unsupported
            false
        }
    }
}

fn can_cast_from_timestamp(to_type: &DataType, _options: &SparkCastOptions) -> bool {
    use DataType::*;
    match to_type {
        Boolean | Int8 | Int16 => {
            // https://github.com/apache/datafusion-comet/issues/352
            // this seems like an edge case that isn't important for us to support
            false
        }
        Int64 => {
            // https://github.com/apache/datafusion-comet/issues/352
            true
        }
        Date32 | Date64 | Utf8 | Decimal128(_, _) => true,
        _ => {
            // unsupported
            false
        }
    }
}

fn can_cast_from_boolean(to_type: &DataType, _: &SparkCastOptions) -> bool {
    use DataType::*;
    matches!(to_type, Int8 | Int16 | Int32 | Int64 | Float32 | Float64)
}

fn can_cast_from_byte(to_type: &DataType, _: &SparkCastOptions) -> bool {
    use DataType::*;
    matches!(
        to_type,
        Boolean | Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Decimal128(_, _)
    )
}

fn can_cast_from_short(to_type: &DataType, _: &SparkCastOptions) -> bool {
    use DataType::*;
    matches!(
        to_type,
        Boolean | Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Decimal128(_, _)
    )
}

fn can_cast_from_int(to_type: &DataType, options: &SparkCastOptions) -> bool {
    use DataType::*;
    match to_type {
        Boolean | Int8 | Int16 | Int32 | Int64 | Float32 | Float64 | Utf8 => true,
        Decimal128(_, _) => {
            // incompatible: no overflow check
            options.allow_incompat
        }
        _ => false,
    }
}

fn can_cast_from_long(to_type: &DataType, options: &SparkCastOptions) -> bool {
    use DataType::*;
    match to_type {
        Boolean | Int8 | Int16 | Int32 | Int64 | Float32 | Float64 => true,
        Decimal128(_, _) => {
            // incompatible: no overflow check
            options.allow_incompat
        }
        _ => false,
    }
}

fn can_cast_from_float(to_type: &DataType, _: &SparkCastOptions) -> bool {
    use DataType::*;
    matches!(
        to_type,
        Boolean | Int8 | Int16 | Int32 | Int64 | Float64 | Decimal128(_, _)
    )
}

fn can_cast_from_double(to_type: &DataType, _: &SparkCastOptions) -> bool {
    use DataType::*;
    matches!(
        to_type,
        Boolean | Int8 | Int16 | Int32 | Int64 | Float32 | Decimal128(_, _)
    )
}

fn can_cast_from_decimal(
    p1: &u8,
    _s1: &i8,
    to_type: &DataType,
    options: &SparkCastOptions,
) -> bool {
    use DataType::*;
    match to_type {
        Int8 | Int16 | Int32 | Int64 | Float32 | Float64 => true,
        Decimal128(p2, _) => {
            if p2 < p1 {
                // https://github.com/apache/datafusion/issues/13492
                // Incompatible(Some("Casting to smaller precision is not supported"))
                options.allow_incompat
            } else {
                true
            }
        }
        _ => false,
    }
}

macro_rules! cast_utf8_to_int {
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len);
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else if let Some(cast_value) = $cast_method($array.value(i), $eval_mode)? {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        let result: SparkResult<ArrayRef> = Ok(Arc::new(cast_array.finish()) as ArrayRef);
        result
    }};
}
macro_rules! cast_utf8_to_timestamp {
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident, $tz:expr) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len).with_timezone("UTC");
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else if let Ok(Some(cast_value)) =
                $cast_method($array.value(i).trim(), $eval_mode, $tz)
            {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        let result: ArrayRef = Arc::new(cast_array.finish()) as ArrayRef;
        result
    }};
}

macro_rules! cast_float_to_string {
    ($from:expr, $eval_mode:expr, $type:ty, $output_type:ty, $offset_type:ty) => {{

        fn cast<OffsetSize>(
            from: &dyn Array,
            _eval_mode: EvalMode,
        ) -> SparkResult<ArrayRef>
        where
            OffsetSize: OffsetSizeTrait, {
                let array = from.as_any().downcast_ref::<$output_type>().unwrap();

                // If the absolute number is less than 10,000,000 and greater or equal than 0.001, the
                // result is expressed without scientific notation with at least one digit on either side of
                // the decimal point. Otherwise, Spark uses a mantissa followed by E and an
                // exponent. The mantissa has an optional leading minus sign followed by one digit to the
                // left of the decimal point, and the minimal number of digits greater than zero to the
                // right. The exponent has and optional leading minus sign.
                // source: https://docs.databricks.com/en/sql/language-manual/functions/cast.html

                const LOWER_SCIENTIFIC_BOUND: $type = 0.001;
                const UPPER_SCIENTIFIC_BOUND: $type = 10000000.0;

                let output_array = array
                    .iter()
                    .map(|value| match value {
                        Some(value) if value == <$type>::INFINITY => Ok(Some("Infinity".to_string())),
                        Some(value) if value == <$type>::NEG_INFINITY => Ok(Some("-Infinity".to_string())),
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

                            if formatted.contains(".") {
                                Ok(Some(formatted))
                            } else {
                                // `formatted` is already in scientific notation and can be split up by E
                                // in order to add the missing trailing 0 which gets removed for numbers with a fraction of 0.0
                                let prepare_number: Vec<&str> = formatted.split("E").collect();

                                let coefficient = prepare_number[0];

                                let exponent = prepare_number[1];

                                Ok(Some(format!("{coefficient}.0E{exponent}")))
                            }
                        }
                        Some(value) => Ok(Some(value.to_string())),
                        _ => Ok(None),
                    })
                    .collect::<Result<GenericStringArray<OffsetSize>, SparkError>>()?;

                Ok(Arc::new(output_array))
            }

        cast::<$offset_type>($from, $eval_mode)
    }};
}

macro_rules! cast_int_to_int_macro {
    (
        $array: expr,
        $eval_mode:expr,
        $from_arrow_primitive_type: ty,
        $to_arrow_primitive_type: ty,
        $from_data_type: expr,
        $to_native_type: ty,
        $spark_from_data_type_name: expr,
        $spark_to_data_type_name: expr
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
                        Ok::<Option<$to_native_type>, SparkError>(Some(value as $to_native_type))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<PrimitiveArray<$to_arrow_primitive_type>, _>>(),
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
                            Ok::<Option<$to_native_type>, SparkError>(Some(res.unwrap()))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<PrimitiveArray<$to_arrow_primitive_type>, _>>(),
        }?;
        let result: SparkResult<ArrayRef> = Ok(Arc::new(output_array) as ArrayRef);
        result
    }};
}

// When Spark casts to Byte/Short Types, it does not cast directly to Byte/Short.
// It casts to Int first and then to Byte/Short. Because of potential overflows in the Int cast,
// this can cause unexpected Short/Byte cast results. Replicate this behavior.
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
                .collect::<Result<$dest_array_type, _>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let i32_value = value as i32;
                        Ok::<Option<$rust_dest_type>, SparkError>(Some(
                            i32_value as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
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
                .collect::<Result<$dest_array_type, _>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        Ok::<Option<$rust_dest_type>, SparkError>(Some(value as $rust_dest_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

// When Spark casts to Byte/Short Types, it does not cast directly to Byte/Short.
// It casts to Int first and then to Byte/Short. Because of potential overflows in the Int cast,
// this can cause unexpected Short/Byte cast results. Replicate this behavior.
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
            .expect(concat!("Expected a Decimal128ArrayType"));

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let (truncated, decimal) = (value / divisor, (value % divisor).abs());
                        let is_overflow = truncated.abs() > i32::MAX.into();
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!("{}.{}BD", truncated, decimal),
                                &format!("DECIMAL({},{})", $precision, $scale),
                                $dest_type_str,
                            ));
                        }
                        let i32_value = truncated as i32;
                        <$rust_dest_type>::try_from(i32_value)
                            .map_err(|_| {
                                cast_overflow(
                                    &format!("{}.{}BD", truncated, decimal),
                                    &format!("DECIMAL({},{})", $precision, $scale),
                                    $dest_type_str,
                                )
                            })
                            .map(Some)
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let i32_value = (value / divisor) as i32;
                        Ok::<Option<$rust_dest_type>, SparkError>(Some(
                            i32_value as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
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
            .expect(concat!("Expected a Decimal128ArrayType"));

        let output_array = match $eval_mode {
            EvalMode::Ansi => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let (truncated, decimal) = (value / divisor, (value % divisor).abs());
                        let is_overflow = truncated.abs() > $max_dest_val.into();
                        if is_overflow {
                            return Err(cast_overflow(
                                &format!("{}.{}BD", truncated, decimal),
                                &format!("DECIMAL({},{})", $precision, $scale),
                                $dest_type_str,
                            ));
                        }
                        Ok(Some(truncated as $rust_dest_type))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
            _ => cast_array
                .iter()
                .map(|value| match value {
                    Some(value) => {
                        let divisor = 10_i128.pow($scale as u32);
                        let truncated = value / divisor;
                        Ok::<Option<$rust_dest_type>, SparkError>(Some(
                            truncated as $rust_dest_type,
                        ))
                    }
                    None => Ok(None),
                })
                .collect::<Result<$dest_array_type, _>>()?,
        };
        Ok(Arc::new(output_array) as ArrayRef)
    }};
}

impl Cast {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        cast_options: SparkCastOptions,
    ) -> Self {
        Self {
            child,
            data_type,
            cast_options,
        }
    }
}

/// Spark cast options
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SparkCastOptions {
    /// Spark evaluation mode
    pub eval_mode: EvalMode,
    /// When cast from/to timezone related types, we need timezone, which will be resolved with
    /// session local timezone by an analyzer in Spark.
    pub timezone: String,
    /// Allow casts that are supported but not guaranteed to be 100% compatible
    pub allow_incompat: bool,
    /// Support casting unsigned ints to signed ints (used by Parquet SchemaAdapter)
    pub allow_cast_unsigned_ints: bool,
}

impl SparkCastOptions {
    pub fn new(eval_mode: EvalMode, timezone: &str, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: timezone.to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
        }
    }

    pub fn new_without_timezone(eval_mode: EvalMode, allow_incompat: bool) -> Self {
        Self {
            eval_mode,
            timezone: "".to_string(),
            allow_incompat,
            allow_cast_unsigned_ints: false,
        }
    }
}

/// Spark-compatible cast implementation. Defers to DataFusion's cast where that is known
/// to be compatible, and returns an error when a not supported and not DF-compatible cast
/// is requested.
pub fn spark_cast(
    arg: ColumnarValue,
    data_type: &DataType,
    cast_options: &SparkCastOptions,
) -> DataFusionResult<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_array(
            array,
            data_type,
            cast_options,
        )?)),
        ColumnarValue::Scalar(scalar) => {
            // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
            // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
            // here.
            let array = scalar.to_array()?;
            let scalar =
                ScalarValue::try_from_array(&cast_array(array, data_type, cast_options)?, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        }
    }
}

fn cast_array(
    array: ArrayRef,
    to_type: &DataType,
    cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    use DataType::*;
    let array = array_with_timezone(array, cast_options.timezone.clone(), Some(to_type))?;
    let from_type = array.data_type().clone();

    let array = match &from_type {
        Dictionary(key_type, value_type)
            if key_type.as_ref() == &Int32
                && (value_type.as_ref() == &Utf8 || value_type.as_ref() == &LargeUtf8) =>
        {
            let dict_array = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Expected a dictionary array");

            let casted_dictionary = DictionaryArray::<Int32Type>::new(
                dict_array.keys().clone(),
                cast_array(Arc::clone(dict_array.values()), to_type, cast_options)?,
            );

            let casted_result = match to_type {
                Dictionary(_, _) => Arc::new(casted_dictionary.clone()),
                _ => take(casted_dictionary.values().as_ref(), dict_array.keys(), None)?,
            };
            return Ok(spark_cast_postprocess(casted_result, &from_type, to_type));
        }
        _ => array,
    };
    let from_type = array.data_type();
    let eval_mode = cast_options.eval_mode;

    let cast_result = match (from_type, to_type) {
        (Utf8, Boolean) => spark_cast_utf8_to_boolean::<i32>(&array, eval_mode),
        (LargeUtf8, Boolean) => spark_cast_utf8_to_boolean::<i64>(&array, eval_mode),
        (Utf8, Timestamp(_, _)) => {
            cast_string_to_timestamp(&array, to_type, eval_mode, &cast_options.timezone)
        }
        (Utf8, Date32) => cast_string_to_date(&array, to_type, eval_mode),
        (Int64, Int32)
        | (Int64, Int16)
        | (Int64, Int8)
        | (Int32, Int16)
        | (Int32, Int8)
        | (Int16, Int8)
            if eval_mode != EvalMode::Try =>
        {
            spark_cast_int_to_int(&array, eval_mode, from_type, to_type)
        }
        (Utf8, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int::<i32>(to_type, &array, eval_mode)
        }
        (LargeUtf8, Int8 | Int16 | Int32 | Int64) => {
            cast_string_to_int::<i64>(to_type, &array, eval_mode)
        }
        (Float64, Utf8) => spark_cast_float64_to_utf8::<i32>(&array, eval_mode),
        (Float64, LargeUtf8) => spark_cast_float64_to_utf8::<i64>(&array, eval_mode),
        (Float32, Utf8) => spark_cast_float32_to_utf8::<i32>(&array, eval_mode),
        (Float32, LargeUtf8) => spark_cast_float32_to_utf8::<i64>(&array, eval_mode),
        (Float32, Decimal128(precision, scale)) => {
            cast_float32_to_decimal128(&array, *precision, *scale, eval_mode)
        }
        (Float64, Decimal128(precision, scale)) => {
            cast_float64_to_decimal128(&array, *precision, *scale, eval_mode)
        }
        (Float32, Int8)
        | (Float32, Int16)
        | (Float32, Int32)
        | (Float32, Int64)
        | (Float64, Int8)
        | (Float64, Int16)
        | (Float64, Int32)
        | (Float64, Int64)
        | (Decimal128(_, _), Int8)
        | (Decimal128(_, _), Int16)
        | (Decimal128(_, _), Int32)
        | (Decimal128(_, _), Int64)
            if eval_mode != EvalMode::Try =>
        {
            spark_cast_nonintegral_numeric_to_integral(&array, eval_mode, from_type, to_type)
        }
        (Struct(_), Utf8) => Ok(casts_struct_to_string(array.as_struct(), cast_options)?),
        (Struct(_), Struct(_)) => Ok(cast_struct_to_struct(
            array.as_struct(),
            from_type,
            to_type,
            cast_options,
        )?),
        (UInt8 | UInt16 | UInt32 | UInt64, Int8 | Int16 | Int32 | Int64)
            if cast_options.allow_cast_unsigned_ints =>
        {
            Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?)
        }
        _ if is_datafusion_spark_compatible(from_type, to_type, cast_options.allow_incompat) => {
            // use DataFusion cast only when we know that it is compatible with Spark
            Ok(cast_with_options(&array, to_type, &CAST_OPTIONS)?)
        }
        _ => {
            // we should never reach this code because the Scala code should be checking
            // for supported cast operations and falling back to Spark for anything that
            // is not yet supported
            Err(SparkError::Internal(format!(
                "Native cast invoked for unsupported cast from {from_type:?} to {to_type:?}"
            )))
        }
    };
    Ok(spark_cast_postprocess(cast_result?, from_type, to_type))
}

/// Determines if DataFusion supports the given cast in a way that is
/// compatible with Spark
fn is_datafusion_spark_compatible(
    from_type: &DataType,
    to_type: &DataType,
    allow_incompat: bool,
) -> bool {
    if from_type == to_type {
        return true;
    }
    match from_type {
        DataType::Boolean => matches!(
            to_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
        ),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            // note that the cast from Int32/Int64 -> Decimal128 here is actually
            // not compatible with Spark (no overflow checks) but we have tests that
            // rely on this cast working so we have to leave it here for now
            matches!(
                to_type,
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
                    | DataType::Utf8
            )
        }
        DataType::Float32 | DataType::Float64 => matches!(
            to_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        ),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => matches!(
            to_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Utf8 // note that there can be formatting differences
        ),
        DataType::Utf8 if allow_incompat => matches!(
            to_type,
            DataType::Binary | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _)
        ),
        DataType::Utf8 => matches!(to_type, DataType::Binary),
        DataType::Date32 => matches!(to_type, DataType::Utf8),
        DataType::Timestamp(_, _) => {
            matches!(
                to_type,
                DataType::Int64 | DataType::Date32 | DataType::Utf8 | DataType::Timestamp(_, _)
            )
        }
        DataType::Binary => {
            // note that this is not completely Spark compatible because
            // DataFusion only supports binary data containing valid UTF-8 strings
            matches!(to_type, DataType::Utf8)
        }
        _ => false,
    }
}

/// Cast between struct types based on logic in
/// `org.apache.spark.sql.catalyst.expressions.Cast#castStruct`.
fn cast_struct_to_struct(
    array: &StructArray,
    from_type: &DataType,
    to_type: &DataType,
    cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Struct(_), DataType::Struct(to_fields)) => {
            let mut cast_fields: Vec<(Arc<Field>, ArrayRef)> = Vec::with_capacity(to_fields.len());
            for i in 0..to_fields.len() {
                let cast_field = cast_array(
                    Arc::clone(array.column(i)),
                    to_fields[i].data_type(),
                    cast_options,
                )?;
                cast_fields.push((Arc::clone(&to_fields[i]), cast_field));
            }
            Ok(Arc::new(StructArray::from(cast_fields)))
        }
        _ => unreachable!(),
    }
}

fn casts_struct_to_string(
    array: &StructArray,
    spark_cast_options: &SparkCastOptions,
) -> DataFusionResult<ArrayRef> {
    // cast each field to a string
    let string_arrays: Vec<ArrayRef> = array
        .columns()
        .iter()
        .map(|arr| {
            spark_cast(
                ColumnarValue::Array(Arc::clone(arr)),
                &DataType::Utf8,
                spark_cast_options,
            )
            .and_then(|cv| cv.into_array(arr.len()))
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    let string_arrays: Vec<&StringArray> =
        string_arrays.iter().map(|arr| arr.as_string()).collect();
    // build the struct string containing entries in the format `"field_name":field_value`
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 16);
    let mut str = String::with_capacity(array.len() * 16);
    for row_index in 0..array.len() {
        if array.is_null(row_index) {
            builder.append_null();
        } else {
            str.clear();
            let mut any_fields_written = false;
            str.push('{');
            for field in &string_arrays {
                if any_fields_written {
                    str.push_str(", ");
                }
                if field.is_null(row_index) {
                    str.push_str("null");
                } else {
                    str.push_str(field.value(row_index));
                }
                any_fields_written = true;
            }
            str.push('}');
            builder.append_value(&str);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn cast_string_to_int<OffsetSize: OffsetSizeTrait>(
    to_type: &DataType,
    array: &ArrayRef,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<OffsetSize>>()
        .expect("cast_string_to_int expected a string array");

    let cast_array: ArrayRef = match to_type {
        DataType::Int8 => cast_utf8_to_int!(string_array, eval_mode, Int8Type, cast_string_to_i8)?,
        DataType::Int16 => {
            cast_utf8_to_int!(string_array, eval_mode, Int16Type, cast_string_to_i16)?
        }
        DataType::Int32 => {
            cast_utf8_to_int!(string_array, eval_mode, Int32Type, cast_string_to_i32)?
        }
        DataType::Int64 => {
            cast_utf8_to_int!(string_array, eval_mode, Int64Type, cast_string_to_i64)?
        }
        dt => unreachable!(
            "{}",
            format!("invalid integer type {dt} in cast from string")
        ),
    };
    Ok(cast_array)
}

fn cast_string_to_date(
    array: &ArrayRef,
    to_type: &DataType,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .expect("Expected a string array");

    if to_type != &DataType::Date32 {
        unreachable!("Invalid data type {:?} in cast from string", to_type);
    }

    let len = string_array.len();
    let mut cast_array = PrimitiveArray::<Date32Type>::builder(len);

    for i in 0..len {
        let value = if string_array.is_null(i) {
            None
        } else {
            match date_parser(string_array.value(i), eval_mode) {
                Ok(Some(cast_value)) => Some(cast_value),
                Ok(None) => None,
                Err(e) => return Err(e),
            }
        };

        match value {
            Some(cast_value) => cast_array.append_value(cast_value),
            None => cast_array.append_null(),
        }
    }

    Ok(Arc::new(cast_array.finish()) as ArrayRef)
}

fn cast_string_to_timestamp(
    array: &ArrayRef,
    to_type: &DataType,
    eval_mode: EvalMode,
    timezone_str: &str,
) -> SparkResult<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .expect("Expected a string array");

    let tz = &timezone::Tz::from_str(timezone_str).unwrap();

    let cast_array: ArrayRef = match to_type {
        DataType::Timestamp(_, _) => {
            cast_utf8_to_timestamp!(
                string_array,
                eval_mode,
                TimestampMicrosecondType,
                timestamp_parser,
                tz
            )
        }
        _ => unreachable!("Invalid data type {:?} in cast from string", to_type),
    };
    Ok(cast_array)
}

fn cast_float64_to_decimal128(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    cast_floating_point_to_decimal128::<Float64Type>(array, precision, scale, eval_mode)
}

fn cast_float32_to_decimal128(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef> {
    cast_floating_point_to_decimal128::<Float32Type>(array, precision, scale, eval_mode)
}

fn cast_floating_point_to_decimal128<T: ArrowPrimitiveType>(
    array: &dyn Array,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
{
    let input = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let mut cast_array = PrimitiveArray::<Decimal128Type>::builder(input.len());

    let mul = 10_f64.powi(scale as i32);

    for i in 0..input.len() {
        if input.is_null(i) {
            cast_array.append_null();
        } else {
            let input_value = input.value(i).as_();
            let value = (input_value * mul).round().to_i128();

            match value {
                Some(v) => {
                    if Decimal128Type::validate_decimal_precision(v, precision).is_err() {
                        if eval_mode == EvalMode::Ansi {
                            return Err(SparkError::NumericValueOutOfRange {
                                value: input_value.to_string(),
                                precision,
                                scale,
                            });
                        } else {
                            cast_array.append_null();
                        }
                    }
                    cast_array.append_value(v);
                }
                None => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(SparkError::NumericValueOutOfRange {
                            value: input_value.to_string(),
                            precision,
                            scale,
                        });
                    } else {
                        cast_array.append_null();
                    }
                }
            }
        }
    }

    let res = Arc::new(
        cast_array
            .with_precision_and_scale(precision, scale)?
            .finish(),
    ) as ArrayRef;
    Ok(res)
}

fn spark_cast_float64_to_utf8<OffsetSize>(
    from: &dyn Array,
    _eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    cast_float_to_string!(from, _eval_mode, f64, Float64Array, OffsetSize)
}

fn spark_cast_float32_to_utf8<OffsetSize>(
    from: &dyn Array,
    _eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    cast_float_to_string!(from, _eval_mode, f32, Float32Array, OffsetSize)
}

fn spark_cast_int_to_int(
    array: &dyn Array,
    eval_mode: EvalMode,
    from_type: &DataType,
    to_type: &DataType,
) -> SparkResult<ArrayRef> {
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

fn spark_cast_utf8_to_boolean<OffsetSize>(
    from: &dyn Array,
    eval_mode: EvalMode,
) -> SparkResult<ArrayRef>
where
    OffsetSize: OffsetSizeTrait,
{
    let array = from
        .as_any()
        .downcast_ref::<GenericStringArray<OffsetSize>>()
        .unwrap();

    let output_array = array
        .iter()
        .map(|value| match value {
            Some(value) => match value.to_ascii_lowercase().trim() {
                "t" | "true" | "y" | "yes" | "1" => Ok(Some(true)),
                "f" | "false" | "n" | "no" | "0" => Ok(Some(false)),
                _ if eval_mode == EvalMode::Ansi => Err(SparkError::CastInvalidValue {
                    value: value.to_string(),
                    from_type: "STRING".to_string(),
                    to_type: "BOOLEAN".to_string(),
                }),
                _ => Ok(None),
            },
            _ => Ok(None),
        })
        .collect::<Result<BooleanArray, _>>()?;

    Ok(Arc::new(output_array))
}

fn spark_cast_nonintegral_numeric_to_integral(
    array: &dyn Array,
    eval_mode: EvalMode,
    from_type: &DataType,
    to_type: &DataType,
) -> SparkResult<ArrayRef> {
    match (from_type, to_type) {
        (DataType::Float32, DataType::Int8) => cast_float_to_int16_down!(
            array,
            eval_mode,
            Float32Array,
            Int8Array,
            f32,
            i8,
            "FLOAT",
            "TINYINT",
            "{:e}"
        ),
        (DataType::Float32, DataType::Int16) => cast_float_to_int16_down!(
            array,
            eval_mode,
            Float32Array,
            Int16Array,
            f32,
            i16,
            "FLOAT",
            "SMALLINT",
            "{:e}"
        ),
        (DataType::Float32, DataType::Int32) => cast_float_to_int32_up!(
            array,
            eval_mode,
            Float32Array,
            Int32Array,
            f32,
            i32,
            "FLOAT",
            "INT",
            i32::MAX,
            "{:e}"
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
                array, eval_mode, Int8Array, i8, "TINYINT", precision, *scale
            )
        }
        (DataType::Decimal128(precision, scale), DataType::Int16) => {
            cast_decimal_to_int16_down!(
                array, eval_mode, Int16Array, i16, "SMALLINT", precision, *scale
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

/// Equivalent to org.apache.spark.unsafe.types.UTF8String.toByte
fn cast_string_to_i8(str: &str, eval_mode: EvalMode) -> SparkResult<Option<i8>> {
    Ok(cast_string_to_int_with_range_check(
        str,
        eval_mode,
        "TINYINT",
        i8::MIN as i32,
        i8::MAX as i32,
    )?
    .map(|v| v as i8))
}

/// Equivalent to org.apache.spark.unsafe.types.UTF8String.toShort
fn cast_string_to_i16(str: &str, eval_mode: EvalMode) -> SparkResult<Option<i16>> {
    Ok(cast_string_to_int_with_range_check(
        str,
        eval_mode,
        "SMALLINT",
        i16::MIN as i32,
        i16::MAX as i32,
    )?
    .map(|v| v as i16))
}

/// Equivalent to org.apache.spark.unsafe.types.UTF8String.toInt(IntWrapper intWrapper)
fn cast_string_to_i32(str: &str, eval_mode: EvalMode) -> SparkResult<Option<i32>> {
    do_cast_string_to_int::<i32>(str, eval_mode, "INT", i32::MIN)
}

/// Equivalent to org.apache.spark.unsafe.types.UTF8String.toLong(LongWrapper intWrapper)
fn cast_string_to_i64(str: &str, eval_mode: EvalMode) -> SparkResult<Option<i64>> {
    do_cast_string_to_int::<i64>(str, eval_mode, "BIGINT", i64::MIN)
}

fn cast_string_to_int_with_range_check(
    str: &str,
    eval_mode: EvalMode,
    type_name: &str,
    min: i32,
    max: i32,
) -> SparkResult<Option<i32>> {
    match do_cast_string_to_int(str, eval_mode, type_name, i32::MIN)? {
        None => Ok(None),
        Some(v) if v >= min && v <= max => Ok(Some(v)),
        _ if eval_mode == EvalMode::Ansi => Err(invalid_value(str, "STRING", type_name)),
        _ => Ok(None),
    }
}

/// Equivalent to
/// - org.apache.spark.unsafe.types.UTF8String.toInt(IntWrapper intWrapper, boolean allowDecimal)
/// - org.apache.spark.unsafe.types.UTF8String.toLong(LongWrapper longWrapper, boolean allowDecimal)
fn do_cast_string_to_int<
    T: Num + PartialOrd + Integer + CheckedSub + CheckedNeg + From<i32> + Copy,
>(
    str: &str,
    eval_mode: EvalMode,
    type_name: &str,
    min_value: T,
) -> SparkResult<Option<T>> {
    let trimmed_str = str.trim();
    if trimmed_str.is_empty() {
        return none_or_err(eval_mode, type_name, str);
    }
    let len = trimmed_str.len();
    let mut result: T = T::zero();
    let mut negative = false;
    let radix = T::from(10);
    let stop_value = min_value / radix;
    let mut parse_sign_and_digits = true;

    for (i, ch) in trimmed_str.char_indices() {
        if parse_sign_and_digits {
            if i == 0 {
                negative = ch == '-';
                let positive = ch == '+';
                if negative || positive {
                    if i + 1 == len {
                        // input string is just "+" or "-"
                        return none_or_err(eval_mode, type_name, str);
                    }
                    // consume this char
                    continue;
                }
            }

            if ch == '.' {
                if eval_mode == EvalMode::Legacy {
                    // truncate decimal in legacy mode
                    parse_sign_and_digits = false;
                    continue;
                } else {
                    return none_or_err(eval_mode, type_name, str);
                }
            }

            let digit = if ch.is_ascii_digit() {
                (ch as u32) - ('0' as u32)
            } else {
                return none_or_err(eval_mode, type_name, str);
            };

            // We are going to process the new digit and accumulate the result. However, before
            // doing this, if the result is already smaller than the
            // stopValue(Integer.MIN_VALUE / radix), then result * 10 will definitely be
            // smaller than minValue, and we can stop
            if result < stop_value {
                return none_or_err(eval_mode, type_name, str);
            }

            // Since the previous result is greater than or equal to stopValue(Integer.MIN_VALUE /
            // radix), we can just use `result > 0` to check overflow. If result
            // overflows, we should stop
            let v = result * radix;
            let digit = (digit as i32).into();
            match v.checked_sub(&digit) {
                Some(x) if x <= T::zero() => result = x,
                _ => {
                    return none_or_err(eval_mode, type_name, str);
                }
            }
        } else {
            // make sure fractional digits are valid digits but ignore them
            if !ch.is_ascii_digit() {
                return none_or_err(eval_mode, type_name, str);
            }
        }
    }

    if !negative {
        if let Some(neg) = result.checked_neg() {
            if neg < T::zero() {
                return none_or_err(eval_mode, type_name, str);
            }
            result = neg;
        } else {
            return none_or_err(eval_mode, type_name, str);
        }
    }

    Ok(Some(result))
}

/// Either return Ok(None) or Err(SparkError::CastInvalidValue) depending on the evaluation mode
#[inline]
fn none_or_err<T>(eval_mode: EvalMode, type_name: &str, str: &str) -> SparkResult<Option<T>> {
    match eval_mode {
        EvalMode::Ansi => Err(invalid_value(str, "STRING", type_name)),
        _ => Ok(None),
    }
}

#[inline]
fn invalid_value(value: &str, from_type: &str, to_type: &str) -> SparkError {
    SparkError::CastInvalidValue {
        value: value.to_string(),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}

#[inline]
fn cast_overflow(value: &str, from_type: &str, to_type: &str) -> SparkError {
    SparkError::CastOverFlow {
        value: value.to_string(),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}

impl Display for Cast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cast [data_type: {}, timezone: {}, child: {}, eval_mode: {:?}]",
            self.data_type, self.cast_options.timezone, self.child, &self.cast_options.eval_mode
        )
    }
}

impl PhysicalExpr for Cast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        spark_cast(arg, &self.data_type, &self.cast_options)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(Cast::new(
                Arc::clone(&children[0]),
                self.data_type.clone(),
                self.cast_options.clone(),
            ))),
            _ => internal_err!("Cast should have exactly one child"),
        }
    }
}

fn timestamp_parser<T: TimeZone>(
    value: &str,
    eval_mode: EvalMode,
    tz: &T,
) -> SparkResult<Option<i64>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    // Define regex patterns and corresponding parsing functions
    let patterns = &[
        (
            Regex::new(r"^\d{4,5}$").unwrap(),
            parse_str_to_year_timestamp as fn(&str, &T) -> SparkResult<Option<i64>>,
        ),
        (
            Regex::new(r"^\d{4,5}-\d{2}$").unwrap(),
            parse_str_to_month_timestamp,
        ),
        (
            Regex::new(r"^\d{4,5}-\d{2}-\d{2}$").unwrap(),
            parse_str_to_day_timestamp,
        ),
        (
            Regex::new(r"^\d{4,5}-\d{2}-\d{2}T\d{1,2}$").unwrap(),
            parse_str_to_hour_timestamp,
        ),
        (
            Regex::new(r"^\d{4,5}-\d{2}-\d{2}T\d{2}:\d{2}$").unwrap(),
            parse_str_to_minute_timestamp,
        ),
        (
            Regex::new(r"^\d{4,5}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$").unwrap(),
            parse_str_to_second_timestamp,
        ),
        (
            Regex::new(r"^\d{4,5}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}$").unwrap(),
            parse_str_to_microsecond_timestamp,
        ),
        (
            Regex::new(r"^T\d{1,2}$").unwrap(),
            parse_str_to_time_only_timestamp,
        ),
    ];

    let mut timestamp = None;

    // Iterate through patterns and try matching
    for (pattern, parse_func) in patterns {
        if pattern.is_match(value) {
            timestamp = parse_func(value, tz)?;
            break;
        }
    }

    if timestamp.is_none() {
        return if eval_mode == EvalMode::Ansi {
            Err(SparkError::CastInvalidValue {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP".to_string(),
            })
        } else {
            Ok(None)
        };
    }

    match timestamp {
        Some(ts) => Ok(Some(ts)),
        None => Err(SparkError::Internal(
            "Failed to parse timestamp".to_string(),
        )),
    }
}

fn parse_timestamp_to_micros<T: TimeZone>(
    timestamp_info: &TimeStampInfo,
    tz: &T,
) -> SparkResult<Option<i64>> {
    let datetime = tz.with_ymd_and_hms(
        timestamp_info.year,
        timestamp_info.month,
        timestamp_info.day,
        timestamp_info.hour,
        timestamp_info.minute,
        timestamp_info.second,
    );

    // Check if datetime is not None
    let tz_datetime = match datetime.single() {
        Some(dt) => dt
            .with_timezone(tz)
            .with_nanosecond(timestamp_info.microsecond * 1000),
        None => {
            return Err(SparkError::Internal(
                "Failed to parse timestamp".to_string(),
            ));
        }
    };

    let result = match tz_datetime {
        Some(dt) => dt.timestamp_micros(),
        None => {
            return Err(SparkError::Internal(
                "Failed to parse timestamp".to_string(),
            ));
        }
    };

    Ok(Some(result))
}

fn get_timestamp_values<T: TimeZone>(
    value: &str,
    timestamp_type: &str,
    tz: &T,
) -> SparkResult<Option<i64>> {
    let values: Vec<_> = value.split(['T', '-', ':', '.']).collect();
    let year = values[0].parse::<i32>().unwrap_or_default();
    let month = values.get(1).map_or(1, |m| m.parse::<u32>().unwrap_or(1));
    let day = values.get(2).map_or(1, |d| d.parse::<u32>().unwrap_or(1));
    let hour = values.get(3).map_or(0, |h| h.parse::<u32>().unwrap_or(0));
    let minute = values.get(4).map_or(0, |m| m.parse::<u32>().unwrap_or(0));
    let second = values.get(5).map_or(0, |s| s.parse::<u32>().unwrap_or(0));
    let microsecond = values.get(6).map_or(0, |ms| ms.parse::<u32>().unwrap_or(0));

    let mut timestamp_info = TimeStampInfo::default();

    let timestamp_info = match timestamp_type {
        "year" => timestamp_info.with_year(year),
        "month" => timestamp_info.with_year(year).with_month(month),
        "day" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day),
        "hour" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour),
        "minute" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute),
        "second" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute)
            .with_second(second),
        "microsecond" => timestamp_info
            .with_year(year)
            .with_month(month)
            .with_day(day)
            .with_hour(hour)
            .with_minute(minute)
            .with_second(second)
            .with_microsecond(microsecond),
        _ => {
            return Err(SparkError::CastInvalidValue {
                value: value.to_string(),
                from_type: "STRING".to_string(),
                to_type: "TIMESTAMP".to_string(),
            })
        }
    };

    parse_timestamp_to_micros(timestamp_info, tz)
}

fn parse_str_to_year_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "year", tz)
}

fn parse_str_to_month_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "month", tz)
}

fn parse_str_to_day_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "day", tz)
}

fn parse_str_to_hour_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "hour", tz)
}

fn parse_str_to_minute_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "minute", tz)
}

fn parse_str_to_second_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "second", tz)
}

fn parse_str_to_microsecond_timestamp<T: TimeZone>(
    value: &str,
    tz: &T,
) -> SparkResult<Option<i64>> {
    get_timestamp_values(value, "microsecond", tz)
}

fn parse_str_to_time_only_timestamp<T: TimeZone>(value: &str, tz: &T) -> SparkResult<Option<i64>> {
    let values: Vec<&str> = value.split('T').collect();
    let time_values: Vec<u32> = values[1]
        .split(':')
        .map(|v| v.parse::<u32>().unwrap_or(0))
        .collect();

    let datetime = tz.from_utc_datetime(&chrono::Utc::now().naive_utc());
    let timestamp = datetime
        .with_timezone(tz)
        .with_hour(time_values.first().copied().unwrap_or_default())
        .and_then(|dt| dt.with_minute(*time_values.get(1).unwrap_or(&0)))
        .and_then(|dt| dt.with_second(*time_values.get(2).unwrap_or(&0)))
        .and_then(|dt| dt.with_nanosecond(*time_values.get(3).unwrap_or(&0) * 1_000))
        .map(|dt| dt.timestamp_micros())
        .unwrap_or_default();

    Ok(Some(timestamp))
}

//a string to date parser - port of spark's SparkDateTimeUtils#stringToDate.
fn date_parser(date_str: &str, eval_mode: EvalMode) -> SparkResult<Option<i32>> {
    // local functions
    fn get_trimmed_start(bytes: &[u8]) -> usize {
        let mut start = 0;
        while start < bytes.len() && is_whitespace_or_iso_control(bytes[start]) {
            start += 1;
        }
        start
    }

    fn get_trimmed_end(start: usize, bytes: &[u8]) -> usize {
        let mut end = bytes.len() - 1;
        while end > start && is_whitespace_or_iso_control(bytes[end]) {
            end -= 1;
        }
        end + 1
    }

    fn is_whitespace_or_iso_control(byte: u8) -> bool {
        byte.is_ascii_whitespace() || byte.is_ascii_control()
    }

    fn is_valid_digits(segment: i32, digits: usize) -> bool {
        // An integer is able to represent a date within [+-]5 million years.
        let max_digits_year = 7;
        //year (segment 0) can be between 4 to 7 digits,
        //month and day (segment 1 and 2) can be between 1 to 2 digits
        (segment == 0 && digits >= 4 && digits <= max_digits_year)
            || (segment != 0 && digits > 0 && digits <= 2)
    }

    fn return_result(date_str: &str, eval_mode: EvalMode) -> SparkResult<Option<i32>> {
        if eval_mode == EvalMode::Ansi {
            Err(SparkError::CastInvalidValue {
                value: date_str.to_string(),
                from_type: "STRING".to_string(),
                to_type: "DATE".to_string(),
            })
        } else {
            Ok(None)
        }
    }
    // end local functions

    if date_str.is_empty() {
        return return_result(date_str, eval_mode);
    }

    //values of date segments year, month and day defaulting to 1
    let mut date_segments = [1, 1, 1];
    let mut sign = 1;
    let mut current_segment = 0;
    let mut current_segment_value = Wrapping(0);
    let mut current_segment_digits = 0;
    let bytes = date_str.as_bytes();

    let mut j = get_trimmed_start(bytes);
    let str_end_trimmed = get_trimmed_end(j, bytes);

    if j == str_end_trimmed {
        return return_result(date_str, eval_mode);
    }

    //assign a sign to the date
    if bytes[j] == b'-' || bytes[j] == b'+' {
        sign = if bytes[j] == b'-' { -1 } else { 1 };
        j += 1;
    }

    //loop to the end of string until we have processed 3 segments,
    //exit loop on encountering any space ' ' or 'T' after the 3rd segment
    while j < str_end_trimmed && (current_segment < 3 && !(bytes[j] == b' ' || bytes[j] == b'T')) {
        let b = bytes[j];
        if current_segment < 2 && b == b'-' {
            //check for validity of year and month segments if current byte is separator
            if !is_valid_digits(current_segment, current_segment_digits) {
                return return_result(date_str, eval_mode);
            }
            //if valid update corresponding segment with the current segment value.
            date_segments[current_segment as usize] = current_segment_value.0;
            current_segment_value = Wrapping(0);
            current_segment_digits = 0;
            current_segment += 1;
        } else if !b.is_ascii_digit() {
            return return_result(date_str, eval_mode);
        } else {
            //increment value of current segment by the next digit
            let parsed_value = Wrapping((b - b'0') as i32);
            current_segment_value = current_segment_value * Wrapping(10) + parsed_value;
            current_segment_digits += 1;
        }
        j += 1;
    }

    //check for validity of last segment
    if !is_valid_digits(current_segment, current_segment_digits) {
        return return_result(date_str, eval_mode);
    }

    if current_segment < 2 && j < str_end_trimmed {
        // For the `yyyy` and `yyyy-[m]m` formats, entire input must be consumed.
        return return_result(date_str, eval_mode);
    }

    date_segments[current_segment as usize] = current_segment_value.0;

    match NaiveDate::from_ymd_opt(
        sign * date_segments[0],
        date_segments[1] as u32,
        date_segments[2] as u32,
    ) {
        Some(date) => {
            let duration_since_epoch = date
                .signed_duration_since(NaiveDateTime::UNIX_EPOCH.date())
                .num_days();
            Ok(Some(duration_since_epoch.to_i32().unwrap()))
        }
        None => Ok(None),
    }
}

/// This takes for special casting cases of Spark. E.g., Timestamp to Long.
/// This function runs as a post process of the DataFusion cast(). By the time it arrives here,
/// Dictionary arrays are already unpacked by the DataFusion cast() since Spark cannot specify
/// Dictionary as to_type. The from_type is taken before the DataFusion cast() runs in
/// expressions/cast.rs, so it can be still Dictionary.
fn spark_cast_postprocess(array: ArrayRef, from_type: &DataType, to_type: &DataType) -> ArrayRef {
    match (from_type, to_type) {
        (DataType::Timestamp(_, _), DataType::Int64) => {
            // See Spark's `Cast` expression
            unary_dyn::<_, Int64Type>(&array, |v| div_floor(v, MICROS_PER_SECOND)).unwrap()
        }
        (DataType::Dictionary(_, value_type), DataType::Int64)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            // See Spark's `Cast` expression
            unary_dyn::<_, Int64Type>(&array, |v| div_floor(v, MICROS_PER_SECOND)).unwrap()
        }
        (DataType::Timestamp(_, _), DataType::Utf8) => remove_trailing_zeroes(array),
        (DataType::Dictionary(_, value_type), DataType::Utf8)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            remove_trailing_zeroes(array)
        }
        _ => array,
    }
}

/// A fork & modified version of Arrow's `unary_dyn` which is being deprecated
fn unary_dyn<F, T>(array: &ArrayRef, op: F) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    if let Some(d) = array.as_any_dictionary_opt() {
        let new_values = unary_dyn::<F, T>(d.values(), op)?;
        return Ok(Arc::new(d.with_values(Arc::new(new_values))));
    }

    match array.as_primitive_opt::<T>() {
        Some(a) if PrimitiveArray::<T>::is_compatible(a.data_type()) => {
            Ok(Arc::new(unary::<T, F, T>(
                array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap(),
                op,
            )))
        }
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Cannot perform unary operation of type {} on array of type {}",
            T::DATA_TYPE,
            array.data_type()
        ))),
    }
}

/// Remove any trailing zeroes in the string if they occur after in the fractional seconds,
/// to match Spark behavior
/// example:
/// "1970-01-01 05:29:59.900" => "1970-01-01 05:29:59.9"
/// "1970-01-01 05:29:59.990" => "1970-01-01 05:29:59.99"
/// "1970-01-01 05:29:59.999" => "1970-01-01 05:29:59.999"
/// "1970-01-01 05:30:00"     => "1970-01-01 05:30:00"
/// "1970-01-01 05:30:00.001" => "1970-01-01 05:30:00.001"
fn remove_trailing_zeroes(array: ArrayRef) -> ArrayRef {
    let string_array = as_generic_string_array::<i32>(&array).unwrap();
    let result = string_array
        .iter()
        .map(|s| s.map(trim_end))
        .collect::<GenericStringArray<i32>>();
    Arc::new(result) as ArrayRef
}

fn trim_end(s: &str) -> &str {
    if s.rfind('.').is_some() {
        s.trim_end_matches('0')
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimestampMicrosecondType;
    use arrow_array::StringArray;
    use arrow_schema::{Field, Fields, TimeUnit};
    use std::str::FromStr;

    use super::*;

    #[test]
    #[cfg_attr(miri, ignore)] // test takes too long with miri
    fn timestamp_parser_test() {
        let tz = &timezone::Tz::from_str("UTC").unwrap();
        // write for all formats
        assert_eq!(
            timestamp_parser("2020", EvalMode::Legacy, tz).unwrap(),
            Some(1577836800000000) // this is in milliseconds
        );
        assert_eq!(
            timestamp_parser("2020-01", EvalMode::Legacy, tz).unwrap(),
            Some(1577836800000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01", EvalMode::Legacy, tz).unwrap(),
            Some(1577836800000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12", EvalMode::Legacy, tz).unwrap(),
            Some(1577880000000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34", EvalMode::Legacy, tz).unwrap(),
            Some(1577882040000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56", EvalMode::Legacy, tz).unwrap(),
            Some(1577882096000000)
        );
        assert_eq!(
            timestamp_parser("2020-01-01T12:34:56.123456", EvalMode::Legacy, tz).unwrap(),
            Some(1577882096123456)
        );
        assert_eq!(
            timestamp_parser("0100", EvalMode::Legacy, tz).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01", EvalMode::Legacy, tz).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01", EvalMode::Legacy, tz).unwrap(),
            Some(-59011459200000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12", EvalMode::Legacy, tz).unwrap(),
            Some(-59011416000000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34", EvalMode::Legacy, tz).unwrap(),
            Some(-59011413960000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34:56", EvalMode::Legacy, tz).unwrap(),
            Some(-59011413904000000)
        );
        assert_eq!(
            timestamp_parser("0100-01-01T12:34:56.123456", EvalMode::Legacy, tz).unwrap(),
            Some(-59011413903876544)
        );
        assert_eq!(
            timestamp_parser("10000", EvalMode::Legacy, tz).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01", EvalMode::Legacy, tz).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01", EvalMode::Legacy, tz).unwrap(),
            Some(253402300800000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12", EvalMode::Legacy, tz).unwrap(),
            Some(253402344000000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34", EvalMode::Legacy, tz).unwrap(),
            Some(253402346040000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34:56", EvalMode::Legacy, tz).unwrap(),
            Some(253402346096000000)
        );
        assert_eq!(
            timestamp_parser("10000-01-01T12:34:56.123456", EvalMode::Legacy, tz).unwrap(),
            Some(253402346096123456)
        );
        // assert_eq!(
        //     timestamp_parser("T2",  EvalMode::Legacy).unwrap(),
        //     Some(1714356000000000) // this value needs to change everyday.
        // );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // test takes too long with miri
    fn test_cast_string_to_timestamp() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020-01-01T12:34:56.123456"),
            Some("T2"),
            Some("0100-01-01T12:34:56.123456"),
            Some("10000-01-01T12:34:56.123456"),
        ]));
        let tz = &timezone::Tz::from_str("UTC").unwrap();

        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .expect("Expected a string array");

        let eval_mode = EvalMode::Legacy;
        let result = cast_utf8_to_timestamp!(
            &string_array,
            eval_mode,
            TimestampMicrosecondType,
            timestamp_parser,
            tz
        );

        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_cast_dict_string_to_timestamp() -> DataFusionResult<()> {
        // prepare input data
        let keys = Int32Array::from(vec![0, 1]);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020-01-01T12:34:56.123456"),
            Some("T2"),
        ]));
        let dict_array = Arc::new(DictionaryArray::new(keys, values));

        let timezone = "UTC".to_string();
        // test casting string dictionary array to timestamp array
        let cast_options = SparkCastOptions::new(EvalMode::Legacy, &timezone, false);
        let result = cast_array(
            dict_array,
            &DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone().into())),
            &cast_options,
        )?;
        assert_eq!(
            *result.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into()))
        );
        assert_eq!(result.len(), 2);

        Ok(())
    }

    #[test]
    fn date_parser_test() {
        for date in &[
            "2020",
            "2020-01",
            "2020-01-01",
            "02020-01-01",
            "002020-01-01",
            "0002020-01-01",
            "2020-1-1",
            "2020-01-01 ",
            "2020-01-01T",
        ] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
                assert_eq!(date_parser(date, *eval_mode).unwrap(), Some(18262));
            }
        }

        //dates in invalid formats
        for date in &[
            "abc",
            "",
            "not_a_date",
            "3/",
            "3/12",
            "3/12/2020",
            "3/12/2002 T",
            "202",
            "2020-010-01",
            "2020-10-010",
            "2020-10-010T",
            "--262143-12-31",
            "--262143-12-31 ",
        ] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Try] {
                assert_eq!(date_parser(date, *eval_mode).unwrap(), None);
            }
            assert!(date_parser(date, EvalMode::Ansi).is_err());
        }

        for date in &["-3638-5"] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Try, EvalMode::Ansi] {
                assert_eq!(date_parser(date, *eval_mode).unwrap(), Some(-2048160));
            }
        }

        //Naive Date only supports years 262142 AD to 262143 BC
        //returns None for dates out of range supported by Naive Date.
        for date in &[
            "-262144-1-1",
            "262143-01-1",
            "262143-1-1",
            "262143-01-1 ",
            "262143-01-01T ",
            "262143-1-01T 1234",
            "-0973250",
        ] {
            for eval_mode in &[EvalMode::Legacy, EvalMode::Try, EvalMode::Ansi] {
                assert_eq!(date_parser(date, *eval_mode).unwrap(), None);
            }
        }
    }

    #[test]
    fn test_cast_string_to_date() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020"),
            Some("2020-01"),
            Some("2020-01-01"),
            Some("2020-01-01T"),
        ]));

        let result = cast_string_to_date(&array, &DataType::Date32, EvalMode::Legacy).unwrap();

        let date32_array = result
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(date32_array.len(), 4);
        date32_array
            .iter()
            .for_each(|v| assert_eq!(v.unwrap(), 18262));
    }

    #[test]
    fn test_cast_string_array_with_valid_dates() {
        let array_with_invalid_date: ArrayRef = Arc::new(StringArray::from(vec![
            Some("-262143-12-31"),
            Some("\n -262143-12-31 "),
            Some("-262143-12-31T \t\n"),
            Some("\n\t-262143-12-31T\r"),
            Some("-262143-12-31T 123123123"),
            Some("\r\n-262143-12-31T \r123123123"),
            Some("\n -262143-12-31T \n\t"),
        ]));

        for eval_mode in &[EvalMode::Legacy, EvalMode::Try, EvalMode::Ansi] {
            let result =
                cast_string_to_date(&array_with_invalid_date, &DataType::Date32, *eval_mode)
                    .unwrap();

            let date32_array = result
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap();
            assert_eq!(result.len(), 7);
            date32_array
                .iter()
                .for_each(|v| assert_eq!(v.unwrap(), -96464928));
        }
    }

    #[test]
    fn test_cast_string_array_with_invalid_dates() {
        let array_with_invalid_date: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020"),
            Some("2020-01"),
            Some("2020-01-01"),
            //4 invalid dates
            Some("2020-010-01T"),
            Some("202"),
            Some(" 202 "),
            Some("\n 2020-\r8 "),
            Some("2020-01-01T"),
            // Overflows i32
            Some("-4607172990231812908"),
        ]));

        for eval_mode in &[EvalMode::Legacy, EvalMode::Try] {
            let result =
                cast_string_to_date(&array_with_invalid_date, &DataType::Date32, *eval_mode)
                    .unwrap();

            let date32_array = result
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap();
            assert_eq!(
                date32_array.iter().collect::<Vec<_>>(),
                vec![
                    Some(18262),
                    Some(18262),
                    Some(18262),
                    None,
                    None,
                    None,
                    None,
                    Some(18262),
                    None
                ]
            );
        }

        let result =
            cast_string_to_date(&array_with_invalid_date, &DataType::Date32, EvalMode::Ansi);
        match result {
            Err(e) => assert!(
                e.to_string().contains(
                    "[CAST_INVALID_INPUT] The value '2020-010-01T' of the type \"STRING\" cannot be cast to \"DATE\" because it is malformed")
            ),
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_cast_string_as_i8() {
        // basic
        assert_eq!(
            cast_string_to_i8("127", EvalMode::Legacy).unwrap(),
            Some(127_i8)
        );
        assert_eq!(cast_string_to_i8("128", EvalMode::Legacy).unwrap(), None);
        assert!(cast_string_to_i8("128", EvalMode::Ansi).is_err());
        // decimals
        assert_eq!(
            cast_string_to_i8("0.2", EvalMode::Legacy).unwrap(),
            Some(0_i8)
        );
        assert_eq!(
            cast_string_to_i8(".", EvalMode::Legacy).unwrap(),
            Some(0_i8)
        );
        // TRY should always return null for decimals
        assert_eq!(cast_string_to_i8("0.2", EvalMode::Try).unwrap(), None);
        assert_eq!(cast_string_to_i8(".", EvalMode::Try).unwrap(), None);
        // ANSI mode should throw error on decimal
        assert!(cast_string_to_i8("0.2", EvalMode::Ansi).is_err());
        assert!(cast_string_to_i8(".", EvalMode::Ansi).is_err());
    }

    #[test]
    fn test_cast_unsupported_timestamp_to_date() {
        // Since datafusion uses chrono::Datetime internally not all dates representable by TimestampMicrosecondType are supported
        let timestamps: PrimitiveArray<TimestampMicrosecondType> = vec![i64::MAX].into();
        let cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
        let result = cast_array(
            Arc::new(timestamps.with_timezone("Europe/Copenhagen")),
            &DataType::Date32,
            &cast_options,
        );
        assert!(result.is_err())
    }

    #[test]
    fn test_cast_invalid_timezone() {
        let timestamps: PrimitiveArray<TimestampMicrosecondType> = vec![i64::MAX].into();
        let cast_options = SparkCastOptions::new(EvalMode::Legacy, "Not a valid timezone", false);
        let result = cast_array(
            Arc::new(timestamps.with_timezone("Europe/Copenhagen")),
            &DataType::Date32,
            &cast_options,
        );
        assert!(result.is_err())
    }

    #[test]
    fn test_cast_struct_to_utf8() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let c: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, true)), a),
            (Arc::new(Field::new("b", DataType::Utf8, true)), b),
        ]));
        let string_array = cast_array(
            c,
            &DataType::Utf8,
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        let string_array = string_array.as_string::<i32>();
        assert_eq!(5, string_array.len());
        assert_eq!(r#"{1, a}"#, string_array.value(0));
        assert_eq!(r#"{2, b}"#, string_array.value(1));
        assert_eq!(r#"{null, c}"#, string_array.value(2));
        assert_eq!(r#"{4, d}"#, string_array.value(3));
        assert_eq!(r#"{5, e}"#, string_array.value(4));
    }

    #[test]
    fn test_cast_struct_to_struct() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let c: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, true)), a),
            (Arc::new(Field::new("b", DataType::Utf8, true)), b),
        ]));
        // change type of "a" from Int32 to Utf8
        let fields = Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let cast_array = spark_cast(
            ColumnarValue::Array(c),
            &DataType::Struct(fields),
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        if let ColumnarValue::Array(cast_array) = cast_array {
            assert_eq!(5, cast_array.len());
            let a = cast_array.as_struct().column(0).as_string::<i32>();
            assert_eq!("1", a.value(0));
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_cast_struct_to_struct_drop_column() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let c: ArrayRef = Arc::new(StructArray::from(vec![
            (Arc::new(Field::new("a", DataType::Int32, true)), a),
            (Arc::new(Field::new("b", DataType::Utf8, true)), b),
        ]));
        // change type of "a" from Int32 to Utf8 and drop "b"
        let fields = Fields::from(vec![Field::new("a", DataType::Utf8, true)]);
        let cast_array = spark_cast(
            ColumnarValue::Array(c),
            &DataType::Struct(fields),
            &SparkCastOptions::new(EvalMode::Legacy, "UTC", false),
        )
        .unwrap();
        if let ColumnarValue::Array(cast_array) = cast_array {
            assert_eq!(5, cast_array.len());
            let struct_array = cast_array.as_struct();
            assert_eq!(1, struct_array.columns().len());
            let a = struct_array.column(0).as_string::<i32>();
            assert_eq!("1", a.value(0));
        } else {
            unreachable!()
        }
    }
}
