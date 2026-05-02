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

use arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondBuilder};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, FieldRef, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, TimeUnit,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::types::{
    logical_float32, logical_float64, logical_int8, logical_int16, logical_int32,
    logical_int64, logical_string,
};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{Coercion, TypeSignatureClass};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use std::sync::Arc;

const MICROS_PER_SECOND: i64 = 1_000_000;

/// Convert integer seconds to microseconds with saturating overflow behavior
#[inline]
fn secs_to_micros(secs: i64) -> i64 {
    secs.saturating_mul(MICROS_PER_SECOND)
}

/// Convert float seconds to microseconds
/// Returns None for NaN/Infinity in non-ANSI mode, error in ANSI mode
/// Saturates to i64::MAX/MIN for overflow
#[inline]
fn float_secs_to_micros(val: f64, enable_ansi_mode: bool) -> Result<Option<i64>> {
    if val.is_nan() || val.is_infinite() {
        if enable_ansi_mode {
            let display_val = if val.is_nan() {
                "NaN"
            } else if val.is_sign_positive() {
                "Infinity"
            } else {
                "-Infinity"
            };
            return exec_err!("Cannot cast {} to TIMESTAMP", display_val);
        }
        return Ok(None);
    }
    let micros = val * MICROS_PER_SECOND as f64;

    // Bounds check for i64 range.
    // Note on precision: i64::MIN (-2^63) is exactly representable in f64,
    // but i64::MAX (2^63 - 1) is not - it rounds up to 2^63 (i64::MAX + 1).
    // We use strict `<` for the upper bound to reject values >= 2^63,
    // which correctly handles the precision loss edge case.
    if micros >= i64::MIN as f64 && micros < i64::MAX as f64 {
        Ok(Some(micros as i64))
    } else {
        if enable_ansi_mode {
            return exec_err!("Overflow casting {} to TIMESTAMP", val);
        }
        // Saturate to i64::MAX or i64::MIN like Spark does for overflow
        if micros.is_sign_negative() {
            Ok(Some(i64::MIN))
        } else {
            Ok(Some(i64::MAX))
        }
    }
}

/// Spark-compatible `cast` function for type conversions
///
/// This implements Spark's CAST expression with a target type parameter
///
/// # Usage
/// ```sql
/// SELECT spark_cast(value, 'timestamp')
/// ```
///
/// # Currently supported conversions
/// - Int8/Int16/Int32/Int64/Float32/Float64 -> Timestamp (target_type = 'timestamp')
///
/// The integer value is interpreted as seconds since the Unix epoch (1970-01-01 00:00:00 UTC)
/// and converted to a timestamp with microsecond precision (matches spark's spec). Same is the case
/// with Float but with higher precision to support micro / nanoseconds.
///
/// # Overflow behavior
/// Uses saturating multiplication to handle overflow - values that would overflow
/// i64 when multiplied by 1,000,000 are clamped to i64::MAX or i64::MIN
///
/// # References
/// - <https://spark.apache.org/docs/latest/api/sql/index.html#cast>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCast {
    signature: Signature,
    timezone: Option<Arc<str>>,
}

impl Default for SparkCast {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCast {
    pub fn new() -> Self {
        Self::new_with_config(&ConfigOptions::default())
    }

    pub fn new_with_config(config: &ConfigOptions) -> Self {
        // First arg: value to cast
        // Second arg: target datatype as Utf8 string literal (ex : 'timestamp')
        let string_arg =
            Coercion::new_exact(TypeSignatureClass::Native(logical_string()));

        // Supported input types: signed integers and floats
        let input_type_signatures = [
            logical_int8(),
            logical_int16(),
            logical_int32(),
            logical_int64(),
            logical_float32(),
            logical_float64(),
        ]
        .map(|input_type| {
            TypeSignature::Coercible(vec![
                Coercion::new_exact(TypeSignatureClass::Native(input_type)),
                string_arg.clone(),
            ])
        });

        Self {
            signature: Signature::new(
                TypeSignature::OneOf(Vec::from(input_type_signatures)),
                Volatility::Stable,
            ),
            timezone: config
                .execution
                .time_zone
                .as_ref()
                .map(|tz| Arc::from(tz.as_str()))
                .or_else(|| Some(Arc::from("UTC"))),
        }
    }
}

/// Parse target type string into a DataType
fn parse_target_type(type_str: &str, timezone: Option<Arc<str>>) -> Result<DataType> {
    match type_str.to_lowercase().as_str() {
        // further data type support in future
        "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, timezone)),
        other => exec_err!(
            "Unsupported spark_cast target type '{}'. Supported types: timestamp",
            other
        ),
    }
}

/// Extract target type string from scalar arguments
fn get_target_type_from_scalar_args(
    scalar_args: &[Option<&ScalarValue>],
    timezone: Option<Arc<str>>,
) -> Result<DataType> {
    let type_arg = scalar_args.get(1).and_then(|opt| *opt);

    match type_arg {
        Some(ScalarValue::Utf8(Some(s)))
        | Some(ScalarValue::LargeUtf8(Some(s)))
        | Some(ScalarValue::Utf8View(Some(s))) => parse_target_type(s, timezone),
        _ => exec_err!(
            "spark_cast requires second argument to be a string of target data type ex: timestamp"
        ),
    }
}

fn cast_int_to_timestamp<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    timezone: Option<Arc<str>>,
) -> Result<ArrayRef>
where
    T::Native: Into<i64>,
{
    let arr = array.as_primitive::<T>();
    let mut builder = TimestampMicrosecondBuilder::with_capacity(arr.len());

    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            // spark saturates to i64 min/max
            let micros = secs_to_micros(arr.value(i).into());
            builder.append_value(micros);
        }
    }

    Ok(Arc::new(builder.finish().with_timezone_opt(timezone)))
}

/// Cast float to timestamp
/// Float value represents seconds (with fractional part) since Unix epoch
/// NaN and Infinity: error in ANSI mode, NULL in non-ANSI mode
fn cast_float_to_timestamp<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    timezone: Option<Arc<str>>,
    enable_ansi_mode: bool,
) -> Result<ArrayRef>
where
    T::Native: Into<f64>,
{
    let arr = array.as_primitive::<T>();
    let mut builder = TimestampMicrosecondBuilder::with_capacity(arr.len());

    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            let val: f64 = arr.value(i).into();
            match float_secs_to_micros(val, enable_ansi_mode)? {
                Some(micros) => builder.append_value(micros),
                None => builder.append_null(),
            }
        }
    }

    Ok(Arc::new(builder.finish().with_timezone_opt(timezone)))
}

impl ScalarUDFImpl for SparkCast {
    fn name(&self) -> &str {
        "spark_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        Some(ScalarUDF::from(Self::new_with_config(config)))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let return_type = get_target_type_from_scalar_args(
            args.scalar_arguments,
            self.timezone.clone(),
        )?;
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let enable_ansi_mode = args.config_options.execution.enable_ansi_mode;
        let target_type = args.return_field.data_type();
        match target_type {
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                cast_to_timestamp(&args.args[0], tz.clone(), enable_ansi_mode)
            }
            other => exec_err!("Unsupported spark_cast target type: {:?}", other),
        }
    }
}

/// Cast value to timestamp internal function
fn cast_to_timestamp(
    input: &ColumnarValue,
    timezone: Option<Arc<str>>,
    enable_ansi_mode: bool,
) -> Result<ColumnarValue> {
    match input {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null => Ok(ColumnarValue::Array(Arc::new(
                arrow::array::TimestampMicrosecondArray::new_null(array.len())
                    .with_timezone_opt(timezone),
            ))),
            DataType::Int8 => Ok(ColumnarValue::Array(
                cast_int_to_timestamp::<Int8Type>(array, timezone)?,
            )),
            DataType::Int16 => Ok(ColumnarValue::Array(cast_int_to_timestamp::<
                Int16Type,
            >(array, timezone)?)),
            DataType::Int32 => Ok(ColumnarValue::Array(cast_int_to_timestamp::<
                Int32Type,
            >(array, timezone)?)),
            DataType::Int64 => Ok(ColumnarValue::Array(cast_int_to_timestamp::<
                Int64Type,
            >(array, timezone)?)),
            DataType::Float32 => Ok(ColumnarValue::Array(cast_float_to_timestamp::<
                Float32Type,
            >(
                array,
                timezone,
                enable_ansi_mode,
            )?)),
            DataType::Float64 => Ok(ColumnarValue::Array(cast_float_to_timestamp::<
                Float64Type,
            >(
                array,
                timezone,
                enable_ansi_mode,
            )?)),
            other => exec_err!("Unsupported cast from {:?} to timestamp", other),
        },
        ColumnarValue::Scalar(scalar) => {
            let micros = match scalar {
                ScalarValue::Null
                | ScalarValue::Int8(None)
                | ScalarValue::Int16(None)
                | ScalarValue::Int32(None)
                | ScalarValue::Int64(None)
                | ScalarValue::Float32(None)
                | ScalarValue::Float64(None) => None,
                ScalarValue::Int8(Some(v)) => Some(secs_to_micros((*v).into())),
                ScalarValue::Int16(Some(v)) => Some(secs_to_micros((*v).into())),
                ScalarValue::Int32(Some(v)) => Some(secs_to_micros((*v).into())),
                ScalarValue::Int64(Some(v)) => Some(secs_to_micros(*v)),
                ScalarValue::Float32(Some(v)) => {
                    float_secs_to_micros(*v as f64, enable_ansi_mode)?
                }
                ScalarValue::Float64(Some(v)) => {
                    float_secs_to_micros(*v, enable_ansi_mode)?
                }
                other => {
                    return exec_err!("Unsupported cast from {:?} to timestamp", other);
                }
            };
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                micros, timezone,
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    };
    use arrow::datatypes::TimestampMicrosecondType;

    // helpers to make testing easier
    fn make_args(input: ColumnarValue, target_type: &str) -> ScalarFunctionArgs {
        make_args_with_timezone(input, target_type, Some("UTC"))
    }

    fn make_args_with_timezone(
        input: ColumnarValue,
        target_type: &str,
        timezone: Option<&str>,
    ) -> ScalarFunctionArgs {
        let return_field = Arc::new(Field::new(
            "result",
            DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from(timezone.unwrap())),
            ),
            true,
        ));
        let mut config = ConfigOptions::default();
        if let Some(tz) = timezone {
            config.execution.time_zone = Some(tz.to_string());
        }
        ScalarFunctionArgs {
            args: vec![
                input,
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(target_type.to_string()))),
            ],
            arg_fields: vec![],
            number_rows: 0,
            return_field,
            config_options: Arc::new(config),
        }
    }

    fn assert_scalar_timestamp(result: ColumnarValue, expected: i64) {
        assert_scalar_timestamp_with_tz(result, expected, "UTC");
    }

    fn assert_scalar_timestamp_with_tz(
        result: ColumnarValue,
        expected: i64,
        expected_tz: &str,
    ) {
        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(val),
                Some(tz),
            )) => {
                assert_eq!(val, expected);
                assert_eq!(tz.as_ref(), expected_tz);
            }
            _ => {
                panic!(
                    "Expected scalar timestamp with value {expected} and {expected_tz} timezone"
                )
            }
        }
    }

    fn assert_scalar_null(result: ColumnarValue) {
        assert_scalar_null_with_tz(result, "UTC");
    }

    fn assert_scalar_null_with_tz(result: ColumnarValue, expected_tz: &str) {
        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(None, Some(tz))) => {
                assert_eq!(tz.as_ref(), expected_tz);
            }
            _ => panic!("Expected null scalar timestamp with {expected_tz} timezone"),
        }
    }

    #[test]
    fn test_cast_int8_array_to_timestamp() {
        let array: ArrayRef = Arc::new(Int8Array::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(127),
            Some(-128),
            None,
        ]));

        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Array(array), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(ts_array.value(0), 0);
                assert_eq!(ts_array.value(1), 1_000_000);
                assert_eq!(ts_array.value(2), -1_000_000);
                assert_eq!(ts_array.value(3), 127_000_000);
                assert_eq!(ts_array.value(4), -128_000_000);
                assert!(ts_array.is_null(5));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_int16_array_to_timestamp() {
        let array: ArrayRef = Arc::new(Int16Array::from(vec![
            Some(0),
            Some(32767),
            Some(-32768),
            None,
        ]));

        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Array(array), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(ts_array.value(0), 0);
                assert_eq!(ts_array.value(1), 32_767_000_000);
                assert_eq!(ts_array.value(2), -32_768_000_000);
                assert!(ts_array.is_null(3));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_int32_array_to_timestamp() {
        let array: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(0), Some(1704067200), None]));

        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Array(array), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(ts_array.value(0), 0);
                assert_eq!(ts_array.value(1), 1_704_067_200_000_000);
                assert!(ts_array.is_null(2));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_int64_array_overflow() {
        let array: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)]));

        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Array(array), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                // saturating_mul clamps to i64::MAX/MIN
                assert_eq!(ts_array.value(0), i64::MAX);
                assert_eq!(ts_array.value(1), i64::MIN);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_int64_array_to_timestamp() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(0),
            Some(1704067200),
            Some(-86400),
            None,
        ]));

        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Array(array), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(ts_array.value(0), 0);
                assert_eq!(ts_array.value(1), 1_704_067_200_000_000);
                assert_eq!(ts_array.value(2), -86_400_000_000); // -1 day
                assert!(ts_array.is_null(3));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_scalar_int8() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Int8(Some(100))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_timestamp(result, 100_000_000);
    }

    #[test]
    fn test_cast_scalar_int16() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Int16(Some(100))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_timestamp(result, 100_000_000);
    }

    #[test]
    fn test_cast_scalar_int32() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1704067200))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_timestamp(result, 1_704_067_200_000_000);
    }

    #[test]
    fn test_cast_scalar_int64() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1704067200))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_timestamp(result, 1_704_067_200_000_000);
    }

    #[test]
    fn test_cast_scalar_negative() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Int32(Some(-86400))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        // -86400 seconds = -1 day before epoch
        assert_scalar_timestamp(result, -86_400_000_000);
    }

    #[test]
    fn test_cast_scalar_null() {
        let cast = SparkCast::new();
        let args =
            make_args(ColumnarValue::Scalar(ScalarValue::Int64(None)), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_null(result);
    }

    #[test]
    fn test_cast_scalar_int64_overflow() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        // saturating_mul clamps to i64::MAX
        assert_scalar_timestamp(result, i64::MAX);
    }

    #[test]
    fn test_unsupported_target_type() {
        let cast = SparkCast::new();
        // invoke_with_args uses return_field which would be set correctly by planning
        // For this test, we need to check return_field_from_args
        let arg_fields: Vec<FieldRef> =
            vec![Arc::new(Field::new("a", DataType::Int64, true))];
        let target_type = ScalarValue::Utf8(Some("string".to_string()));
        let scalar_arguments: Vec<Option<&ScalarValue>> = vec![None, Some(&target_type)];
        let return_field_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        };
        let result = cast.return_field_from_args(return_field_args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported spark_cast target type")
        );
    }

    #[test]
    fn test_unsupported_source_type() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024-01-01".to_string()))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported cast from")
        );
    }

    #[test]
    fn test_cast_null_to_timestamp() {
        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Scalar(ScalarValue::Null), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_null(result);
    }

    #[test]
    fn test_cast_null_array_to_timestamp() {
        let array: ArrayRef = Arc::new(arrow::array::NullArray::new(3));

        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Array(array), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(ts_array.len(), 3);
                assert!(ts_array.is_null(0));
                assert!(ts_array.is_null(1));
                assert!(ts_array.is_null(2));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_int_to_timestamp_with_timezones() {
        // Test with various timezones like Comet does
        let timezones = [
            "UTC",
            "America/New_York",
            "America/Los_Angeles",
            "Europe/London",
            "Asia/Tokyo",
            "Australia/Sydney",
        ];

        let cast = SparkCast::new();
        let test_value: i64 = 1704067200; // 2024-01-01 00:00:00 UTC
        let expected_micros = test_value * MICROS_PER_SECOND;

        for tz in timezones {
            // scalar
            let args = make_args_with_timezone(
                ColumnarValue::Scalar(ScalarValue::Int64(Some(test_value))),
                "timestamp",
                Some(tz),
            );
            let result = cast.invoke_with_args(args).unwrap();
            assert_scalar_timestamp_with_tz(result, expected_micros, tz);

            // array input
            let array: ArrayRef =
                Arc::new(Int64Array::from(vec![Some(test_value), None]));
            let args = make_args_with_timezone(
                ColumnarValue::Array(array),
                "timestamp",
                Some(tz),
            );
            let result = cast.invoke_with_args(args).unwrap();

            match result {
                ColumnarValue::Array(result_array) => {
                    let ts_array =
                        result_array.as_primitive::<TimestampMicrosecondType>();
                    assert_eq!(ts_array.value(0), expected_micros);
                    assert!(ts_array.is_null(1));
                    assert_eq!(ts_array.timezone(), Some(tz));
                }
                _ => panic!("Expected array result for timezone {tz}"),
            }
        }
    }

    #[test]
    fn test_cast_int_to_timestamp_default_timezone() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        // Defaults to UTC
        assert_scalar_timestamp_with_tz(result, 0, "UTC");
    }

    fn make_args_with_ansi_mode(
        input: ColumnarValue,
        target_type: &str,
        enable_ansi_mode: bool,
    ) -> ScalarFunctionArgs {
        let return_field = Arc::new(Field::new(
            "result",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        ));
        let mut config = ConfigOptions::default();
        config.execution.time_zone = Some("UTC".to_string());
        config.execution.enable_ansi_mode = enable_ansi_mode;
        ScalarFunctionArgs {
            args: vec![
                input,
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(target_type.to_string()))),
            ],
            arg_fields: vec![],
            number_rows: 0,
            return_field,
            config_options: Arc::new(config),
        }
    }

    #[test]
    fn test_cast_float64_array_to_timestamp() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(1.5),
            Some(-1.5),
            Some(1704067200.123456),
            None,
        ]));

        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Array(array), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(ts_array.value(0), 0);
                assert_eq!(ts_array.value(1), 1_500_000); // 1.5 seconds
                assert_eq!(ts_array.value(2), -1_500_000); // -1.5 seconds
                assert_eq!(ts_array.value(3), 1_704_067_200_123_456); // with fractional
                assert!(ts_array.is_null(4));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_float32_array_to_timestamp() {
        let array: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(0.0f32),
            Some(1.5f32),
            Some(-1.5f32),
            None,
        ]));

        let cast = SparkCast::new();
        let args = make_args(ColumnarValue::Array(array), "timestamp");
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(ts_array.value(0), 0);
                assert_eq!(ts_array.value(1), 1_500_000); // 1.5 seconds
                assert_eq!(ts_array.value(2), -1_500_000); // -1.5 seconds
                assert!(ts_array.is_null(3));
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_scalar_float64() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(1.5))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_timestamp(result, 1_500_000);
    }

    #[test]
    fn test_cast_scalar_float32() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Float32(Some(1.5f32))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_timestamp(result, 1_500_000);
    }

    #[test]
    fn test_cast_float_nan_non_ansi_mode() {
        // In non-ANSI mode, NaN should return NULL
        let cast = SparkCast::new();
        let args = make_args_with_ansi_mode(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NAN))),
            "timestamp",
            false,
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_null(result);
    }

    #[test]
    fn test_cast_float_infinity_non_ansi_mode() {
        // In non-ANSI mode, Infinity should return NULL
        let cast = SparkCast::new();

        // Positive infinity
        let args = make_args_with_ansi_mode(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::INFINITY))),
            "timestamp",
            false,
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_null(result);

        // Negative infinity
        let args = make_args_with_ansi_mode(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NEG_INFINITY))),
            "timestamp",
            false,
        );
        let result = cast.invoke_with_args(args).unwrap();
        assert_scalar_null(result);
    }

    #[test]
    fn test_cast_float_nan_ansi_mode() {
        // In ANSI mode, NaN should error
        let cast = SparkCast::new();
        let args = make_args_with_ansi_mode(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NAN))),
            "timestamp",
            true,
        );
        let result = cast.invoke_with_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot cast NaN"));
    }

    #[test]
    fn test_cast_float_infinity_ansi_mode() {
        // In ANSI mode, Infinity should error
        let cast = SparkCast::new();
        let args = make_args_with_ansi_mode(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::INFINITY))),
            "timestamp",
            true,
        );
        let result = cast.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot cast Infinity")
        );
    }

    #[test]
    fn test_cast_float_overflow_non_ansi_mode() {
        // Value too large to fit in i64 microseconds - should saturate to i64::MAX like Spark
        let cast = SparkCast::new();
        let large_value = 1e19; // Way too large for i64 microseconds
        let args = make_args_with_ansi_mode(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(large_value))),
            "timestamp",
            false,
        );
        let result = cast.invoke_with_args(args).unwrap();
        // Spark saturates overflow to i64::MAX
        assert_scalar_timestamp(result, i64::MAX);
    }

    #[test]
    fn test_cast_float_negative_overflow_non_ansi_mode() {
        // Large negative value - should saturate to i64::MIN like Spark
        let cast = SparkCast::new();
        let large_value = -1e19; // Way too large negative for i64 microseconds
        let args = make_args_with_ansi_mode(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(large_value))),
            "timestamp",
            false,
        );
        let result = cast.invoke_with_args(args).unwrap();
        // Spark saturates negative overflow to i64::MIN
        assert_scalar_timestamp(result, i64::MIN);
    }

    #[test]
    fn test_cast_float_overflow_ansi_mode() {
        // Value too large to fit in i64 microseconds - should error in ANSI mode
        let cast = SparkCast::new();
        let large_value = 1e19; // Way too large for i64 microseconds
        let args = make_args_with_ansi_mode(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(large_value))),
            "timestamp",
            true,
        );
        let result = cast.invoke_with_args(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Overflow"));
    }

    #[test]
    fn test_cast_float_array_with_nan_and_infinity() {
        // Array with NaN and Infinity in non-ANSI mode
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(2.0),
        ]));

        let cast = SparkCast::new();
        let args =
            make_args_with_ansi_mode(ColumnarValue::Array(array), "timestamp", false);
        let result = cast.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Array(result_array) => {
                let ts_array = result_array.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(ts_array.value(0), 1_000_000);
                assert!(ts_array.is_null(1)); // NaN -> NULL
                assert!(ts_array.is_null(2)); // Infinity -> NULL
                assert!(ts_array.is_null(3)); // -Infinity -> NULL
                assert_eq!(ts_array.value(4), 2_000_000);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_cast_float_negative_values() {
        let cast = SparkCast::new();
        let args = make_args(
            ColumnarValue::Scalar(ScalarValue::Float64(Some(-86400.5))),
            "timestamp",
        );
        let result = cast.invoke_with_args(args).unwrap();
        // -86400.5 seconds = -86400500000 microseconds (1 day and 0.5 seconds before epoch)
        assert_scalar_timestamp(result, -86_400_500_000);
    }
}
