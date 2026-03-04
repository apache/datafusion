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

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, GenericStringArray, PrimitiveArray,
};
use arrow::compute::unary;
use arrow::datatypes::{DataType, Int64Type, TimeUnit, TimestampMicrosecondType};
use arrow::error::ArrowError;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{DataFusionError, Result, exec_err};
use std::sync::Arc;

/// Spark evaluation modes matching Spark's three cast behaviors.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum EvalMode {
    /// Legacy: default behavior before Spark 4.0. Silently returns NULL on errors.
    Legacy,
    /// Ansi: strict ANSI SQL mode that throws errors on invalid input.
    Ansi,
    /// Try: like Ansi but converts errors to NULL instead of failing.
    Try,
}

/// Simplified Spark cast options for the UDF context.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SparkCastOptions {
    pub eval_mode: EvalMode,
    pub timezone: String,
}

impl SparkCastOptions {
    pub fn new(eval_mode: EvalMode, timezone: &str) -> Self {
        Self {
            eval_mode,
            timezone: timezone.to_string(),
        }
    }
}

pub(crate) const MICROS_PER_SECOND: i64 = 1_000_000;

pub(crate) static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");

/// Parse a Spark SQL type name string into an Arrow DataType.
pub fn parse_spark_datatype(s: &str) -> Result<DataType> {
    let s = s.trim().to_uppercase();
    match s.as_str() {
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "TINYINT" | "BYTE" | "INT1" => Ok(DataType::Int8),
        "SMALLINT" | "SHORT" | "INT2" => Ok(DataType::Int16),
        "INT" | "INTEGER" | "INT4" => Ok(DataType::Int32),
        "BIGINT" | "LONG" | "INT8" => Ok(DataType::Int64),
        "FLOAT" | "REAL" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "STRING" => Ok(DataType::Utf8),
        "BINARY" => Ok(DataType::Binary),
        "DATE" => Ok(DataType::Date32),
        "TIMESTAMP" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        "TIMESTAMP_NTZ" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        _ if s.starts_with("DECIMAL")
            || s.starts_with("DEC")
            || s.starts_with("NUMERIC") =>
        {
            parse_decimal_type(&s)
        }
        _ => exec_err!("Unsupported Spark SQL type: {s}"),
    }
}

fn parse_decimal_type(s: &str) -> Result<DataType> {
    // DECIMAL, DECIMAL(p), DECIMAL(p, s)
    if let Some(paren_start) = s.find('(') {
        let paren_end = s.find(')').ok_or_else(|| {
            DataFusionError::Execution(format!("Invalid decimal type: {s}"))
        })?;
        let inner = &s[paren_start + 1..paren_end];
        let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
        match parts.len() {
            1 => {
                let precision: u8 = parts[0].parse().map_err(|_| {
                    DataFusionError::Execution(format!("Invalid precision: {s}"))
                })?;
                Ok(DataType::Decimal128(precision, 0))
            }
            2 => {
                let precision: u8 = parts[0].parse().map_err(|_| {
                    DataFusionError::Execution(format!("Invalid precision: {s}"))
                })?;
                let scale: i8 = parts[1].parse().map_err(|_| {
                    DataFusionError::Execution(format!("Invalid scale: {s}"))
                })?;
                Ok(DataType::Decimal128(precision, scale))
            }
            _ => exec_err!("Invalid decimal type: {s}"),
        }
    } else {
        // DECIMAL without parameters defaults to DECIMAL(10, 0) in Spark
        Ok(DataType::Decimal128(10, 0))
    }
}

// --- Error helpers ---

/// Creates a DataFusionError with Spark's [CAST_INVALID_INPUT] message format.
#[inline]
pub fn cast_invalid_input(
    value: &str,
    from_type: &str,
    to_type: &str,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"{from_type}\" cannot be cast to \"{to_type}\" \
         because it is malformed. Correct the value as per the syntax, or change its target type. \
         Use `try_cast` to tolerate malformed input and return NULL instead. If necessary \
         set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error."
    ))
}

/// Creates a DataFusionError with Spark's [CAST_OVERFLOW] message format.
#[inline]
pub fn cast_overflow(value: &str, from_type: &str, to_type: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_OVERFLOW] The value {value} of the type \"{from_type}\" cannot be cast to \"{to_type}\" \
         due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead. If necessary \
         set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error."
    ))
}

/// Creates a DataFusionError with Spark's [NUMERIC_VALUE_OUT_OF_RANGE] message format.
#[inline]
pub fn numeric_value_out_of_range(
    value: &str,
    precision: u8,
    scale: i8,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[NUMERIC_VALUE_OUT_OF_RANGE] {value} cannot be represented as Decimal({precision}, {scale}). \
         If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error, and return NULL instead."
    ))
}

/// Creates a DataFusionError for values that exceed the supported numeric range.
#[inline]
pub fn numeric_out_of_range(value: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[NUMERIC_OUT_OF_SUPPORTED_RANGE] The value {value} cannot be interpreted as a numeric \
         since it has more than 38 digits."
    ))
}

// --- Post-processing ---

/// A fork & modified version of Arrow's `unary_dyn`
pub(crate) fn unary_dyn<F, T>(
    array: &ArrayRef,
    op: F,
) -> std::result::Result<ArrayRef, ArrowError>
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

/// Spark-specific post-processing after cast. Handles:
/// - Timestamp -> Int64: divide by MICROS_PER_SECOND
/// - Timestamp -> Utf8: remove trailing zeroes from fractional seconds
pub(crate) fn spark_cast_postprocess(
    array: ArrayRef,
    from_type: &DataType,
    to_type: &DataType,
) -> ArrayRef {
    match (from_type, to_type) {
        (DataType::Timestamp(_, _), DataType::Int64) => {
            unary_dyn::<_, Int64Type>(&array, |v| div_floor(v, MICROS_PER_SECOND))
                .unwrap()
        }
        (DataType::Dictionary(_, value_type), DataType::Int64)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            unary_dyn::<_, Int64Type>(&array, |v| div_floor(v, MICROS_PER_SECOND))
                .unwrap()
        }
        (DataType::Timestamp(_, _), DataType::Utf8) => remove_trailing_zeroes(&array),
        (DataType::Dictionary(_, value_type), DataType::Utf8)
            if matches!(value_type.as_ref(), &DataType::Timestamp(_, _)) =>
        {
            remove_trailing_zeroes(&array)
        }
        _ => array,
    }
}

/// Integer floor division (rounds toward negative infinity).
fn div_floor(a: i64, b: i64) -> i64 {
    let d = a / b;
    let r = a % b;
    if (r != 0) && ((r ^ b) < 0) { d - 1 } else { d }
}

fn remove_trailing_zeroes(array: &ArrayRef) -> ArrayRef {
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

/// Attach timezone metadata to timestamp arrays that are missing it,
/// and handle NTZ (no timezone) to TZ conversions.
pub(crate) fn array_with_timezone(
    array: ArrayRef,
    timezone: &str,
    to_type: Option<&DataType>,
) -> std::result::Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Timestamp(_, None) => {
            // TimestampNTZ: no timezone info
            match to_type {
                Some(DataType::Utf8) | Some(DataType::Date32) => Ok(array),
                Some(DataType::Timestamp(_, Some(_))) => {
                    // Convert NTZ to TZ by adding the session timezone
                    timestamp_ntz_to_timestamp(&array, timezone, Some(timezone))
                }
                _ => Ok(array),
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            if !timezone.is_empty() {
                let ts_array = array.as_primitive::<TimestampMicrosecondType>();
                let array_with_tz = ts_array.clone().with_timezone(timezone.to_string());
                let array = Arc::new(array_with_tz) as ArrayRef;
                match to_type {
                    Some(DataType::Utf8) | Some(DataType::Date32) => {
                        pre_timestamp_cast(&array, timezone)
                    }
                    _ => Ok(array),
                }
            } else {
                Ok(array)
            }
        }
        _ => Ok(array),
    }
}

/// Convert a timestamp without timezone to one with timezone.
fn timestamp_ntz_to_timestamp(
    array: &ArrayRef,
    _from_tz: &str,
    to_tz: Option<&str>,
) -> std::result::Result<ArrayRef, ArrowError> {
    let ts_array = array.as_primitive::<TimestampMicrosecondType>();
    let result = ts_array
        .clone()
        .with_timezone_opt(to_tz.map(|s| Arc::from(s) as Arc<str>));
    Ok(Arc::new(result) as ArrayRef)
}

/// Pre-process timestamp for cast to string/date by setting UTC timezone.
fn pre_timestamp_cast(
    array: &ArrayRef,
    _timezone: &str,
) -> std::result::Result<ArrayRef, ArrowError> {
    let ts_array = array.as_primitive::<TimestampMicrosecondType>();
    let result = ts_array.clone().with_timezone("UTC");
    Ok(Arc::new(result) as ArrayRef)
}

/// Format a decimal value string with proper decimal point placement.
pub(crate) fn format_decimal_str(value_str: &str, precision: usize, scale: i8) -> String {
    let (sign, rest) = match value_str.strip_prefix('-') {
        Some(stripped) => ("-", stripped),
        None => ("", value_str),
    };
    let bound = precision.min(rest.len()) + sign.len();
    let value_str = &value_str[0..bound];

    if scale == 0 {
        value_str.to_string()
    } else if scale < 0 {
        let padding = value_str.len() + scale.unsigned_abs() as usize;
        format!("{value_str:0<padding$}")
    } else if rest.len() > scale as usize {
        let (whole, decimal) = value_str.split_at(value_str.len() - scale as usize);
        format!("{whole}.{decimal}")
    } else {
        format!("{}0.{:0>width$}", sign, rest, width = scale as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_spark_datatype_basic() {
        assert_eq!(parse_spark_datatype("BOOLEAN").unwrap(), DataType::Boolean);
        assert_eq!(parse_spark_datatype("bool").unwrap(), DataType::Boolean);
        assert_eq!(parse_spark_datatype("TINYINT").unwrap(), DataType::Int8);
        assert_eq!(parse_spark_datatype("BYTE").unwrap(), DataType::Int8);
        assert_eq!(parse_spark_datatype("SMALLINT").unwrap(), DataType::Int16);
        assert_eq!(parse_spark_datatype("SHORT").unwrap(), DataType::Int16);
        assert_eq!(parse_spark_datatype("INT").unwrap(), DataType::Int32);
        assert_eq!(parse_spark_datatype("INTEGER").unwrap(), DataType::Int32);
        assert_eq!(parse_spark_datatype("BIGINT").unwrap(), DataType::Int64);
        assert_eq!(parse_spark_datatype("LONG").unwrap(), DataType::Int64);
        assert_eq!(parse_spark_datatype("FLOAT").unwrap(), DataType::Float32);
        assert_eq!(parse_spark_datatype("REAL").unwrap(), DataType::Float32);
        assert_eq!(parse_spark_datatype("DOUBLE").unwrap(), DataType::Float64);
        assert_eq!(parse_spark_datatype("STRING").unwrap(), DataType::Utf8);
        assert_eq!(parse_spark_datatype("BINARY").unwrap(), DataType::Binary);
        assert_eq!(parse_spark_datatype("DATE").unwrap(), DataType::Date32);
        assert_eq!(
            parse_spark_datatype("TIMESTAMP").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            parse_spark_datatype("TIMESTAMP_NTZ").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_parse_spark_datatype_decimal() {
        assert_eq!(
            parse_spark_datatype("DECIMAL").unwrap(),
            DataType::Decimal128(10, 0)
        );
        assert_eq!(
            parse_spark_datatype("DECIMAL(18)").unwrap(),
            DataType::Decimal128(18, 0)
        );
        assert_eq!(
            parse_spark_datatype("DECIMAL(10, 2)").unwrap(),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            parse_spark_datatype("DEC(38, 18)").unwrap(),
            DataType::Decimal128(38, 18)
        );
        assert_eq!(
            parse_spark_datatype("NUMERIC(20, 5)").unwrap(),
            DataType::Decimal128(20, 5)
        );
    }

    #[test]
    fn test_parse_spark_datatype_case_insensitive() {
        assert_eq!(parse_spark_datatype("int").unwrap(), DataType::Int32);
        assert_eq!(parse_spark_datatype("  INT  ").unwrap(), DataType::Int32);
        assert_eq!(parse_spark_datatype("Double").unwrap(), DataType::Float64);
    }

    #[test]
    fn test_parse_spark_datatype_invalid() {
        assert!(parse_spark_datatype("UNKNOWN").is_err());
        assert!(parse_spark_datatype("").is_err());
    }

    #[test]
    fn test_eval_mode_eq() {
        assert_eq!(EvalMode::Legacy, EvalMode::Legacy);
        assert_ne!(EvalMode::Legacy, EvalMode::Ansi);
        assert_ne!(EvalMode::Ansi, EvalMode::Try);
    }

    #[test]
    fn test_div_floor() {
        assert_eq!(div_floor(7, 2), 3);
        assert_eq!(div_floor(-7, 2), -4);
        assert_eq!(div_floor(7, -2), -4);
        assert_eq!(div_floor(-7, -2), 3);
        assert_eq!(div_floor(6, 2), 3);
        assert_eq!(div_floor(0, 1), 0);
    }

    #[test]
    fn test_format_decimal_str() {
        assert_eq!(format_decimal_str("12345", 5, 2), "123.45");
        assert_eq!(format_decimal_str("-1", 1, 3), "-0.001");
        assert_eq!(format_decimal_str("42", 2, 0), "42");
    }
}
