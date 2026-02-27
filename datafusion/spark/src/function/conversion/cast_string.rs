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
    EvalMode, cast_invalid_input, numeric_out_of_range, numeric_value_out_of_range,
};
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Decimal128Builder,
    GenericStringArray, OffsetSizeTrait, PrimitiveArray, PrimitiveBuilder, StringArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Decimal256Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimestampMicrosecondType, i256, is_validate_decimal_precision,
};
use chrono::{DateTime, NaiveDate, TimeZone, Timelike};
use datafusion_common::Result;
use num_traits::Float;
use regex::Regex;
use std::num::Wrapping;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

// Pre-compiled regex patterns for timestamp parsing (improvement over Comet which recompiles)
static RE_YEAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\d{4,5}$").unwrap());
static RE_MONTH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4,5}-\d{2}$").unwrap());
static RE_DAY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4,5}-\d{2}-\d{2}$").unwrap());
static RE_HOUR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4,5}-\d{2}-\d{2}T\d{1,2}$").unwrap());
static RE_MINUTE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4,5}-\d{2}-\d{2}T\d{2}:\d{2}$").unwrap());
static RE_SECOND: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4,5}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$").unwrap());
static RE_MICROSECOND: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\d{4,5}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}$").unwrap()
});
static RE_TIME_ONLY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^T\d{1,2}$").unwrap());

macro_rules! cast_utf8_to_timestamp {
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident, $tz:expr) => {{
        let len = $array.len();
        let mut cast_array =
            PrimitiveArray::<$array_type>::builder(len).with_timezone("UTC");
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

macro_rules! cast_utf8_to_int {
    ($array:expr, $array_type:ty, $parse_fn:expr) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len);
        let parse_fn = $parse_fn;
        if $array.null_count() == 0 {
            for i in 0..len {
                if let Some(cast_value) = parse_fn($array.value(i))? {
                    cast_array.append_value(cast_value);
                } else {
                    cast_array.append_null()
                }
            }
        } else {
            for i in 0..len {
                if $array.is_null(i) {
                    cast_array.append_null()
                } else if let Some(cast_value) = parse_fn($array.value(i))? {
                    cast_array.append_value(cast_value);
                } else {
                    cast_array.append_null()
                }
            }
        }
        let result: Result<ArrayRef> = Ok(Arc::new(cast_array.finish()) as ArrayRef);
        result
    }};
}

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
    fn with_year(&mut self, year: i32) -> &mut Self {
        self.year = year;
        self
    }

    fn with_month(&mut self, month: u32) -> &mut Self {
        self.month = month;
        self
    }

    fn with_day(&mut self, day: u32) -> &mut Self {
        self.day = day;
        self
    }

    fn with_hour(&mut self, hour: u32) -> &mut Self {
        self.hour = hour;
        self
    }

    fn with_minute(&mut self, minute: u32) -> &mut Self {
        self.minute = minute;
        self
    }

    fn with_second(&mut self, second: u32) -> &mut Self {
        self.second = second;
        self
    }

    fn with_microsecond(&mut self, microsecond: u32) -> &mut Self {
        self.microsecond = microsecond;
        self
    }
}

pub(crate) fn is_df_cast_from_string_spark_compatible(to_type: &DataType) -> bool {
    matches!(to_type, DataType::Binary)
}

// --- String → Float ---

pub(crate) fn cast_string_to_float(
    array: &ArrayRef,
    to_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ArrayRef> {
    match to_type {
        DataType::Float32 => {
            cast_string_to_float_impl::<Float32Type>(array, eval_mode, "FLOAT")
        }
        DataType::Float64 => {
            cast_string_to_float_impl::<Float64Type>(array, eval_mode, "DOUBLE")
        }
        _ => datafusion_common::internal_err!(
            "Unsupported cast to float type: {:?}",
            to_type
        ),
    }
}

fn cast_string_to_float_impl<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    eval_mode: EvalMode,
    type_name: &str,
) -> Result<ArrayRef>
where
    T::Native: FromStr + Float,
{
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expected string array".to_string(),
            )
        })?;

    let mut builder = PrimitiveBuilder::<T>::with_capacity(arr.len());

    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            let str_value = arr.value(i).trim();
            match parse_string_to_float(str_value) {
                Some(v) => builder.append_value(v),
                None => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(cast_invalid_input(
                            arr.value(i),
                            "STRING",
                            type_name,
                        ));
                    }
                    builder.append_null();
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn parse_string_to_float<F>(s: &str) -> Option<F>
where
    F: FromStr + Float,
{
    if s.eq_ignore_ascii_case("inf")
        || s.eq_ignore_ascii_case("+inf")
        || s.eq_ignore_ascii_case("infinity")
        || s.eq_ignore_ascii_case("+infinity")
    {
        return Some(F::infinity());
    }
    if s.eq_ignore_ascii_case("-inf") || s.eq_ignore_ascii_case("-infinity") {
        return Some(F::neg_infinity());
    }
    if s.eq_ignore_ascii_case("nan") {
        return Some(F::nan());
    }
    // Remove D/F suffix if present
    let pruned_float_str =
        if s.ends_with('d') || s.ends_with('D') || s.ends_with('f') || s.ends_with('F') {
            &s[..s.len() - 1]
        } else {
            s
        };
    pruned_float_str.parse::<F>().ok()
}

// --- String → Boolean ---

pub(crate) fn spark_cast_utf8_to_boolean<OffsetSize>(
    from: &dyn Array,
    eval_mode: EvalMode,
) -> Result<ArrayRef>
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
                _ if eval_mode == EvalMode::Ansi => {
                    Err(cast_invalid_input(value, "STRING", "BOOLEAN"))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        })
        .collect::<Result<BooleanArray>>()?;

    Ok(Arc::new(output_array))
}

// --- String → Decimal ---

pub(crate) fn cast_string_to_decimal(
    array: &ArrayRef,
    to_type: &DataType,
    precision: &u8,
    scale: &i8,
    eval_mode: EvalMode,
) -> Result<ArrayRef> {
    match to_type {
        DataType::Decimal128(_, _) => {
            cast_string_to_decimal128_impl(array, eval_mode, *precision, *scale)
        }
        DataType::Decimal256(_, _) => {
            cast_string_to_decimal256_impl(array, eval_mode, *precision, *scale)
        }
        _ => datafusion_common::internal_err!(
            "Unexpected type in cast_string_to_decimal: {:?}",
            to_type
        ),
    }
}

fn cast_string_to_decimal128_impl(
    array: &ArrayRef,
    eval_mode: EvalMode,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expected string array".to_string(),
            )
        })?;

    let mut decimal_builder = Decimal128Builder::with_capacity(string_array.len());

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            decimal_builder.append_null();
        } else {
            let str_value = string_array.value(i);
            match parse_string_to_decimal(str_value, precision, scale) {
                Ok(Some(decimal_value)) => {
                    decimal_builder.append_value(decimal_value);
                }
                Ok(None) => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(cast_invalid_input(
                            string_array.value(i),
                            "STRING",
                            &format!("DECIMAL({precision},{scale})"),
                        ));
                    }
                    decimal_builder.append_null();
                }
                Err(e) => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(e);
                    }
                    decimal_builder.append_null();
                }
            }
        }
    }

    Ok(Arc::new(
        decimal_builder
            .with_precision_and_scale(precision, scale)?
            .finish(),
    ))
}

fn cast_string_to_decimal256_impl(
    array: &ArrayRef,
    eval_mode: EvalMode,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Expected string array".to_string(),
            )
        })?;

    let mut decimal_builder =
        PrimitiveBuilder::<Decimal256Type>::with_capacity(string_array.len());

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            decimal_builder.append_null();
        } else {
            let str_value = string_array.value(i);
            match parse_string_to_decimal(str_value, precision, scale) {
                Ok(Some(decimal_value)) => {
                    let i256_value = i256::from_i128(decimal_value);
                    decimal_builder.append_value(i256_value);
                }
                Ok(None) => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(cast_invalid_input(
                            str_value,
                            "STRING",
                            &format!("DECIMAL({precision},{scale})"),
                        ));
                    }
                    decimal_builder.append_null();
                }
                Err(e) => {
                    if eval_mode == EvalMode::Ansi {
                        return Err(e);
                    }
                    decimal_builder.append_null();
                }
            }
        }
    }

    Ok(Arc::new(
        decimal_builder
            .with_precision_and_scale(precision, scale)?
            .finish(),
    ))
}

fn parse_string_to_decimal(
    input_str: &str,
    precision: u8,
    scale: i8,
) -> Result<Option<i128>> {
    let string_bytes = input_str.as_bytes();
    let mut start = 0;
    let mut end = string_bytes.len();

    while start < end && string_bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && string_bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }

    let trimmed = &input_str[start..end];

    if trimmed.is_empty() {
        return Ok(None);
    }

    if trimmed.eq_ignore_ascii_case("inf")
        || trimmed.eq_ignore_ascii_case("+inf")
        || trimmed.eq_ignore_ascii_case("infinity")
        || trimmed.eq_ignore_ascii_case("+infinity")
        || trimmed.eq_ignore_ascii_case("-inf")
        || trimmed.eq_ignore_ascii_case("-infinity")
        || trimmed.eq_ignore_ascii_case("nan")
    {
        return Ok(None);
    }

    let (mantissa, exponent) = parse_decimal_str(trimmed, input_str, precision, scale)?;

    if mantissa == 0 {
        if exponent < -37 {
            return Err(numeric_out_of_range(input_str));
        }
        return Ok(Some(0));
    }

    let target_scale = scale as i32;
    let scale_adjustment = target_scale - exponent;

    let scaled_value = if scale_adjustment >= 0 {
        if scale_adjustment > 38 {
            return Ok(None);
        }
        mantissa.checked_mul(10_i128.pow(scale_adjustment as u32))
    } else {
        let abs_scale_adjustment = (-scale_adjustment) as u32;
        if abs_scale_adjustment > 38 {
            return Ok(Some(0));
        }

        let divisor = 10_i128.pow(abs_scale_adjustment);
        let quotient_opt = mantissa.checked_div(divisor);
        if quotient_opt.is_none() {
            return Ok(None);
        }
        let quotient = quotient_opt.unwrap();
        let remainder = mantissa % divisor;

        let half_divisor = divisor / 2;
        let rounded = if remainder.abs() >= half_divisor {
            if mantissa >= 0 {
                quotient + 1
            } else {
                quotient - 1
            }
        } else {
            quotient
        };
        Some(rounded)
    };

    match scaled_value {
        Some(value) => {
            if is_validate_decimal_precision(value, precision) {
                Ok(Some(value))
            } else {
                Err(numeric_value_out_of_range(trimmed, precision, scale))
            }
        }
        None => Err(numeric_value_out_of_range(trimmed, precision, scale)),
    }
}

fn invalid_decimal_cast(
    value: &str,
    precision: u8,
    scale: i8,
) -> datafusion_common::DataFusionError {
    cast_invalid_input(value, "STRING", &format!("DECIMAL({precision},{scale})"))
}

fn parse_decimal_str(
    s: &str,
    original_str: &str,
    precision: u8,
    scale: i8,
) -> Result<(i128, i32)> {
    if s.is_empty() {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    let (mantissa_str, exponent) =
        if let Some(e_pos) = s.find(|c| ['e', 'E'].contains(&c)) {
            let mantissa_part = &s[..e_pos];
            let exponent_part = &s[e_pos + 1..];
            let exp: i32 = exponent_part
                .parse()
                .map_err(|_| invalid_decimal_cast(original_str, precision, scale))?;
            (mantissa_part, exp)
        } else {
            (s, 0)
        };

    let negative = mantissa_str.starts_with('-');
    let mantissa_str = if negative || mantissa_str.starts_with('+') {
        &mantissa_str[1..]
    } else {
        mantissa_str
    };

    if mantissa_str.starts_with('+') || mantissa_str.starts_with('-') {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    let (integral_part, fractional_part) = match mantissa_str.find('.') {
        Some(dot_pos) => {
            if mantissa_str[dot_pos + 1..].contains('.') {
                return Err(invalid_decimal_cast(original_str, precision, scale));
            }
            (&mantissa_str[..dot_pos], &mantissa_str[dot_pos + 1..])
        }
        None => (mantissa_str, ""),
    };

    if integral_part.is_empty() && fractional_part.is_empty() {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    if !integral_part.is_empty() && !integral_part.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    if !fractional_part.is_empty() && !fractional_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid_decimal_cast(original_str, precision, scale));
    }

    let integral_value: i128 = if integral_part.is_empty() {
        0
    } else {
        integral_part
            .parse()
            .map_err(|_| invalid_decimal_cast(original_str, precision, scale))?
    };

    let fractional_scale = fractional_part.len() as i32;
    let fractional_value: i128 = if fractional_part.is_empty() {
        0
    } else {
        fractional_part
            .parse()
            .map_err(|_| invalid_decimal_cast(original_str, precision, scale))?
    };

    let mantissa = integral_value
        .checked_mul(10_i128.pow(fractional_scale as u32))
        .and_then(|v| v.checked_add(fractional_value))
        .ok_or_else(|| invalid_decimal_cast(original_str, precision, scale))?;

    let final_mantissa = if negative { -mantissa } else { mantissa };
    let final_scale = fractional_scale - exponent;
    Ok((final_mantissa, final_scale))
}

// --- String → Date ---

pub(crate) fn cast_string_to_date(
    array: &ArrayRef,
    to_type: &DataType,
    eval_mode: EvalMode,
) -> Result<ArrayRef> {
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

fn date_parser(date_str: &str, eval_mode: EvalMode) -> Result<Option<i32>> {
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
        let max_digits_year = 7;
        (segment == 0 && digits >= 4 && digits <= max_digits_year)
            || (segment != 0 && digits > 0 && digits <= 2)
    }

    fn return_result(date_str: &str, eval_mode: EvalMode) -> Result<Option<i32>> {
        if eval_mode == EvalMode::Ansi {
            Err(cast_invalid_input(date_str, "STRING", "DATE"))
        } else {
            Ok(None)
        }
    }

    if date_str.is_empty() {
        return return_result(date_str, eval_mode);
    }

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

    if bytes[j] == b'-' || bytes[j] == b'+' {
        sign = if bytes[j] == b'-' { -1 } else { 1 };
        j += 1;
    }

    while j < str_end_trimmed
        && (current_segment < 3 && !(bytes[j] == b' ' || bytes[j] == b'T'))
    {
        let b = bytes[j];
        if current_segment < 2 && b == b'-' {
            if !is_valid_digits(current_segment, current_segment_digits) {
                return return_result(date_str, eval_mode);
            }
            date_segments[current_segment as usize] = current_segment_value.0;
            current_segment_value = Wrapping(0);
            current_segment_digits = 0;
            current_segment += 1;
        } else if !b.is_ascii_digit() {
            return return_result(date_str, eval_mode);
        } else {
            let parsed_value = Wrapping((b - b'0') as i32);
            current_segment_value = current_segment_value * Wrapping(10) + parsed_value;
            current_segment_digits += 1;
        }
        j += 1;
    }

    if !is_valid_digits(current_segment, current_segment_digits) {
        return return_result(date_str, eval_mode);
    }

    if current_segment < 2 && j < str_end_trimmed {
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
                .signed_duration_since(DateTime::UNIX_EPOCH.naive_utc().date())
                .num_days();
            Ok(Some(duration_since_epoch.try_into().unwrap_or(i32::MAX)))
        }
        None => Ok(None),
    }
}

// --- String → Timestamp ---

pub(crate) fn cast_string_to_timestamp<T: TimeZone>(
    array: &ArrayRef,
    to_type: &DataType,
    eval_mode: EvalMode,
    tz: &T,
) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<i32>>()
        .expect("Expected a string array");

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

fn get_timestamp_values<T: TimeZone>(
    value: &str,
    timestamp_type: &str,
    tz: &T,
) -> Result<Option<i64>> {
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
            return Err(cast_invalid_input(value, "STRING", "TIMESTAMP"));
        }
    };
    parse_timestamp_to_micros(timestamp_info, tz)
}

fn parse_timestamp_to_micros<T: TimeZone>(
    timestamp_info: &TimeStampInfo,
    tz: &T,
) -> Result<Option<i64>> {
    let datetime = tz.with_ymd_and_hms(
        timestamp_info.year,
        timestamp_info.month,
        timestamp_info.day,
        timestamp_info.hour,
        timestamp_info.minute,
        timestamp_info.second,
    );

    let tz_datetime = match datetime.single() {
        Some(dt) => dt
            .with_timezone(tz)
            .with_nanosecond(timestamp_info.microsecond * 1000),
        None => {
            return Err(datafusion_common::DataFusionError::Internal(
                "Failed to parse timestamp".to_string(),
            ));
        }
    };

    let result = match tz_datetime {
        Some(dt) => dt.timestamp_micros(),
        None => {
            return Err(datafusion_common::DataFusionError::Internal(
                "Failed to parse timestamp".to_string(),
            ));
        }
    };

    Ok(Some(result))
}

fn parse_str_to_year_timestamp<T: TimeZone>(value: &str, tz: &T) -> Result<Option<i64>> {
    get_timestamp_values(value, "year", tz)
}

fn parse_str_to_month_timestamp<T: TimeZone>(value: &str, tz: &T) -> Result<Option<i64>> {
    get_timestamp_values(value, "month", tz)
}

fn parse_str_to_day_timestamp<T: TimeZone>(value: &str, tz: &T) -> Result<Option<i64>> {
    get_timestamp_values(value, "day", tz)
}

fn parse_str_to_hour_timestamp<T: TimeZone>(value: &str, tz: &T) -> Result<Option<i64>> {
    get_timestamp_values(value, "hour", tz)
}

fn parse_str_to_minute_timestamp<T: TimeZone>(
    value: &str,
    tz: &T,
) -> Result<Option<i64>> {
    get_timestamp_values(value, "minute", tz)
}

fn parse_str_to_second_timestamp<T: TimeZone>(
    value: &str,
    tz: &T,
) -> Result<Option<i64>> {
    get_timestamp_values(value, "second", tz)
}

fn parse_str_to_microsecond_timestamp<T: TimeZone>(
    value: &str,
    tz: &T,
) -> Result<Option<i64>> {
    get_timestamp_values(value, "microsecond", tz)
}

type TimestampPattern<T> = (
    &'static LazyLock<Regex>,
    fn(&str, &T) -> Result<Option<i64>>,
);

fn timestamp_parser<T: TimeZone>(
    value: &str,
    eval_mode: EvalMode,
    tz: &T,
) -> Result<Option<i64>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    let patterns: &[TimestampPattern<T>] = &[
        (&RE_YEAR, parse_str_to_year_timestamp),
        (&RE_MONTH, parse_str_to_month_timestamp),
        (&RE_DAY, parse_str_to_day_timestamp),
        (&RE_HOUR, parse_str_to_hour_timestamp),
        (&RE_MINUTE, parse_str_to_minute_timestamp),
        (&RE_SECOND, parse_str_to_second_timestamp),
        (&RE_MICROSECOND, parse_str_to_microsecond_timestamp),
        (&RE_TIME_ONLY, parse_str_to_time_only_timestamp),
    ];

    let mut timestamp = None;

    for (pattern, parse_func) in patterns {
        if pattern.is_match(value) {
            timestamp = parse_func(value, tz)?;
            break;
        }
    }

    if timestamp.is_none() {
        return if eval_mode == EvalMode::Ansi {
            Err(cast_invalid_input(value, "STRING", "TIMESTAMP"))
        } else {
            Ok(None)
        };
    }

    match timestamp {
        Some(ts) => Ok(Some(ts)),
        None => Err(datafusion_common::DataFusionError::Internal(
            "Failed to parse timestamp".to_string(),
        )),
    }
}

fn parse_str_to_time_only_timestamp<T: TimeZone>(
    value: &str,
    tz: &T,
) -> Result<Option<i64>> {
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

// --- String → Int ---

pub(crate) fn cast_string_to_int<OffsetSize: OffsetSizeTrait>(
    to_type: &DataType,
    array: &ArrayRef,
    eval_mode: EvalMode,
) -> Result<ArrayRef> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<OffsetSize>>()
        .expect("cast_string_to_int expected a string array");

    let cast_array: ArrayRef = match (to_type, eval_mode) {
        (DataType::Int8, EvalMode::Legacy) => {
            cast_utf8_to_int!(string_array, Int8Type, parse_string_to_i8_legacy)?
        }
        (DataType::Int8, EvalMode::Ansi) => {
            cast_utf8_to_int!(string_array, Int8Type, parse_string_to_i8_ansi)?
        }
        (DataType::Int8, EvalMode::Try) => {
            cast_utf8_to_int!(string_array, Int8Type, parse_string_to_i8_try)?
        }
        (DataType::Int16, EvalMode::Legacy) => {
            cast_utf8_to_int!(string_array, Int16Type, parse_string_to_i16_legacy)?
        }
        (DataType::Int16, EvalMode::Ansi) => {
            cast_utf8_to_int!(string_array, Int16Type, parse_string_to_i16_ansi)?
        }
        (DataType::Int16, EvalMode::Try) => {
            cast_utf8_to_int!(string_array, Int16Type, parse_string_to_i16_try)?
        }
        (DataType::Int32, EvalMode::Legacy) => cast_utf8_to_int!(
            string_array,
            Int32Type,
            |s| do_parse_string_to_int_legacy::<i32>(s, i32::MIN)
        )?,
        (DataType::Int32, EvalMode::Ansi) => cast_utf8_to_int!(
            string_array,
            Int32Type,
            |s| do_parse_string_to_int_ansi::<i32>(s, "INT", i32::MIN)
        )?,
        (DataType::Int32, EvalMode::Try) => {
            cast_utf8_to_int!(string_array, Int32Type, |s| do_parse_string_to_int_try::<
                i32,
            >(s, i32::MIN))?
        }
        (DataType::Int64, EvalMode::Legacy) => cast_utf8_to_int!(
            string_array,
            Int64Type,
            |s| do_parse_string_to_int_legacy::<i64>(s, i64::MIN)
        )?,
        (DataType::Int64, EvalMode::Ansi) => cast_utf8_to_int!(
            string_array,
            Int64Type,
            |s| do_parse_string_to_int_ansi::<i64>(s, "BIGINT", i64::MIN)
        )?,
        (DataType::Int64, EvalMode::Try) => {
            cast_utf8_to_int!(string_array, Int64Type, |s| do_parse_string_to_int_try::<
                i64,
            >(s, i64::MIN))?
        }
        (dt, _) => unreachable!(
            "{}",
            format!("invalid integer type {dt} in cast from string")
        ),
    };
    Ok(cast_array)
}

fn finalize_int_result<T>(result: T, negative: bool) -> Option<T>
where
    T: num_traits::CheckedNeg + num_traits::Zero + Copy + PartialOrd,
{
    if negative {
        Some(result)
    } else {
        result.checked_neg().filter(|&n| n >= T::zero())
    }
}

fn do_parse_string_to_int_legacy<T>(str: &str, min_value: T) -> Result<Option<T>>
where
    T: num_traits::CheckedNeg
        + num_traits::Zero
        + Copy
        + PartialOrd
        + std::ops::Mul<Output = T>
        + std::ops::Div<Output = T>
        + From<u8>,
    T: num_traits::CheckedSub,
{
    let trimmed_bytes = str.as_bytes().trim_ascii();

    let (negative, digits) = match parse_sign(trimmed_bytes) {
        Some(result) => result,
        None => return Ok(None),
    };

    let mut result: T = T::zero();
    let radix = T::from(10_u8);
    let stop_value = min_value / radix;

    let mut iter = digits.iter();

    for &ch in iter.by_ref() {
        if ch == b'.' {
            break;
        }

        if !ch.is_ascii_digit() {
            return Ok(None);
        }

        if result < stop_value {
            return Ok(None);
        }
        let v = result * radix;
        let digit: T = T::from(ch - b'0');
        match v.checked_sub(&digit) {
            Some(x) if x <= T::zero() => result = x,
            _ => return Ok(None),
        }
    }

    for &ch in iter {
        if !ch.is_ascii_digit() {
            return Ok(None);
        }
    }

    Ok(finalize_int_result(result, negative))
}

fn do_parse_string_to_int_ansi<T>(
    str: &str,
    type_name: &str,
    min_value: T,
) -> Result<Option<T>>
where
    T: num_traits::CheckedNeg
        + num_traits::Zero
        + Copy
        + PartialOrd
        + std::ops::Mul<Output = T>
        + std::ops::Div<Output = T>
        + From<u8>,
    T: num_traits::CheckedSub,
{
    let error = || Err(cast_invalid_input(str, "STRING", type_name));

    let trimmed_bytes = str.as_bytes().trim_ascii();

    let (negative, digits) = match parse_sign(trimmed_bytes) {
        Some(result) => result,
        None => return error(),
    };

    let mut result: T = T::zero();
    let radix = T::from(10_u8);
    let stop_value = min_value / radix;

    for &ch in digits {
        if ch == b'.' || !ch.is_ascii_digit() {
            return error();
        }

        if result < stop_value {
            return error();
        }
        let v = result * radix;
        let digit: T = T::from(ch - b'0');
        match v.checked_sub(&digit) {
            Some(x) if x <= T::zero() => result = x,
            _ => return error(),
        }
    }

    finalize_int_result(result, negative)
        .map(Some)
        .ok_or_else(|| cast_invalid_input(str, "STRING", type_name))
}

fn do_parse_string_to_int_try<T>(str: &str, min_value: T) -> Result<Option<T>>
where
    T: num_traits::CheckedNeg
        + num_traits::Zero
        + Copy
        + PartialOrd
        + std::ops::Mul<Output = T>
        + std::ops::Div<Output = T>
        + From<u8>,
    T: num_traits::CheckedSub,
{
    let trimmed_bytes = str.as_bytes().trim_ascii();

    let (negative, digits) = match parse_sign(trimmed_bytes) {
        Some(result) => result,
        None => return Ok(None),
    };

    let mut result: T = T::zero();
    let radix = T::from(10_u8);
    let stop_value = min_value / radix;

    for &ch in digits {
        if ch == b'.' || !ch.is_ascii_digit() {
            return Ok(None);
        }

        if result < stop_value {
            return Ok(None);
        }
        let v = result * radix;
        let digit: T = T::from(ch - b'0');
        match v.checked_sub(&digit) {
            Some(x) if x <= T::zero() => result = x,
            _ => return Ok(None),
        }
    }

    Ok(finalize_int_result(result, negative))
}

fn parse_string_to_i8_legacy(str: &str) -> Result<Option<i8>> {
    match do_parse_string_to_int_legacy::<i32>(str, i32::MIN)? {
        Some(v) if v >= i8::MIN as i32 && v <= i8::MAX as i32 => Ok(Some(v as i8)),
        _ => Ok(None),
    }
}

fn parse_string_to_i8_ansi(str: &str) -> Result<Option<i8>> {
    match do_parse_string_to_int_ansi::<i32>(str, "TINYINT", i32::MIN)? {
        Some(v) if v >= i8::MIN as i32 && v <= i8::MAX as i32 => Ok(Some(v as i8)),
        _ => Err(cast_invalid_input(str, "STRING", "TINYINT")),
    }
}

fn parse_string_to_i8_try(str: &str) -> Result<Option<i8>> {
    match do_parse_string_to_int_try::<i32>(str, i32::MIN)? {
        Some(v) if v >= i8::MIN as i32 && v <= i8::MAX as i32 => Ok(Some(v as i8)),
        _ => Ok(None),
    }
}

fn parse_string_to_i16_legacy(str: &str) -> Result<Option<i16>> {
    match do_parse_string_to_int_legacy::<i32>(str, i32::MIN)? {
        Some(v) if v >= i16::MIN as i32 && v <= i16::MAX as i32 => Ok(Some(v as i16)),
        _ => Ok(None),
    }
}

fn parse_string_to_i16_ansi(str: &str) -> Result<Option<i16>> {
    match do_parse_string_to_int_ansi::<i32>(str, "SMALLINT", i32::MIN)? {
        Some(v) if v >= i16::MIN as i32 && v <= i16::MAX as i32 => Ok(Some(v as i16)),
        _ => Err(cast_invalid_input(str, "STRING", "SMALLINT")),
    }
}

fn parse_string_to_i16_try(str: &str) -> Result<Option<i16>> {
    match do_parse_string_to_int_try::<i32>(str, i32::MIN)? {
        Some(v) if v >= i16::MIN as i32 && v <= i16::MAX as i32 => Ok(Some(v as i16)),
        _ => Ok(None),
    }
}

fn parse_sign(bytes: &[u8]) -> Option<(bool, &[u8])> {
    let (&first, rest) = bytes.split_first()?;
    match first {
        b'-' if !rest.is_empty() => Some((true, rest)),
        b'+' if !rest.is_empty() => Some((false, rest)),
        _ => Some((false, bytes)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cast_string_to_i8(str: &str, eval_mode: EvalMode) -> Result<Option<i8>> {
        match eval_mode {
            EvalMode::Legacy => parse_string_to_i8_legacy(str),
            EvalMode::Ansi => parse_string_to_i8_ansi(str),
            EvalMode::Try => parse_string_to_i8_try(str),
        }
    }

    #[test]
    fn test_cast_string_as_i8() {
        assert_eq!(
            cast_string_to_i8("127", EvalMode::Legacy).unwrap(),
            Some(127_i8)
        );
        assert_eq!(cast_string_to_i8("128", EvalMode::Legacy).unwrap(), None);
        assert!(cast_string_to_i8("128", EvalMode::Ansi).is_err());
        assert_eq!(
            cast_string_to_i8("0.2", EvalMode::Legacy).unwrap(),
            Some(0_i8)
        );
        assert_eq!(
            cast_string_to_i8(".", EvalMode::Legacy).unwrap(),
            Some(0_i8)
        );
        assert_eq!(cast_string_to_i8("0.2", EvalMode::Try).unwrap(), None);
        assert_eq!(cast_string_to_i8(".", EvalMode::Try).unwrap(), None);
        assert!(cast_string_to_i8("0.2", EvalMode::Ansi).is_err());
        assert!(cast_string_to_i8(".", EvalMode::Ansi).is_err());
    }

    #[test]
    fn test_cast_string_to_boolean() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("true"),
            Some("false"),
            Some("t"),
            Some("f"),
            Some("yes"),
            Some("no"),
            Some("y"),
            Some("n"),
            Some("1"),
            Some("0"),
            Some("invalid"),
            None,
        ]));

        let result = spark_cast_utf8_to_boolean::<i32>(&array, EvalMode::Legacy).unwrap();
        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(bool_arr.value(0));
        assert!(!bool_arr.value(1));
        assert!(bool_arr.value(2));
        assert!(!bool_arr.value(3));
        assert!(bool_arr.value(4));
        assert!(!bool_arr.value(5));
        assert!(bool_arr.value(6));
        assert!(!bool_arr.value(7));
        assert!(bool_arr.value(8));
        assert!(!bool_arr.value(9));
        assert!(bool_arr.is_null(10)); // "invalid" -> null in Legacy
        assert!(bool_arr.is_null(11)); // null -> null
    }

    #[test]
    fn test_cast_string_to_boolean_ansi_error() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![Some("invalid")]));
        let result = spark_cast_utf8_to_boolean::<i32>(&array, EvalMode::Ansi);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("[CAST_INVALID_INPUT]"));
    }

    #[test]
    fn test_date_parser() {
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
    }

    #[test]
    fn test_cast_string_to_date_array() {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("2020"),
            Some("2020-01"),
            Some("2020-01-01"),
            Some("2020-01-01T"),
        ]));

        let result =
            cast_string_to_date(&array, &DataType::Date32, EvalMode::Legacy).unwrap();

        let date32_array = result
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(date32_array.len(), 4);
        date32_array
            .iter()
            .for_each(|v| assert_eq!(v.unwrap(), 18262));
    }
}
