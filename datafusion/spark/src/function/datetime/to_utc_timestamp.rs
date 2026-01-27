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
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, AsArray, PrimitiveBuilder, StringArrayType};
use arrow::datatypes::TimeUnit;
use arrow::datatypes::{
    ArrowTimestampType, DataType, Field, FieldRef, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use chrono::{DateTime, Offset, TimeZone};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    Result, exec_datafusion_err, exec_err, internal_datafusion_err, internal_err,
};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// Apache Spark `to_utc_timestamp` function.
///
/// Interprets the given timestamp in the provided timezone and then converts it to UTC.
///
/// Timestamp in Apache Spark represents number of microseconds from the Unix epoch, which is not
/// timezone-agnostic. So in Apache Spark this function just shift the timestamp value from the given
/// timezone to UTC timezone.
///
/// See <https://spark.apache.org/docs/latest/api/sql/index.html#to_utc_timestamp>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToUtcTimestamp {
    signature: Signature,
}

impl Default for SparkToUtcTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkToUtcTimestamp {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Timestamp,
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::Timestamp(TimeUnit::Microsecond, None),
                    ),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkToUtcTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_utc_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new(
            self.name(),
            args.arg_fields[0].data_type().clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(to_utc_timestamp, vec![])(&args.args)
    }
}

fn to_utc_timestamp(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [timestamp, timezone] = take_function_args("to_utc_timestamp", args)?;

    match timestamp.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
            process_timestamp_with_tz_array::<TimestampNanosecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
            process_timestamp_with_tz_array::<TimestampMicrosecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
            process_timestamp_with_tz_array::<TimestampMillisecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        DataType::Timestamp(TimeUnit::Second, tz_opt) => {
            process_timestamp_with_tz_array::<TimestampSecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        ts_type => {
            exec_err!("`to_utc_timestamp`: unsupported argument types: {ts_type}")
        }
    }
}

fn process_timestamp_with_tz_array<T: ArrowTimestampType>(
    ts_array: &ArrayRef,
    tz_array: &ArrayRef,
    tz_opt: Option<Arc<str>>,
) -> Result<ArrayRef> {
    match tz_array.data_type() {
        DataType::Utf8 => {
            process_arrays::<T, _>(tz_opt, ts_array, tz_array.as_string::<i32>())
        }
        DataType::LargeUtf8 => {
            process_arrays::<T, _>(tz_opt, ts_array, tz_array.as_string::<i64>())
        }
        DataType::Utf8View => {
            process_arrays::<T, _>(tz_opt, ts_array, tz_array.as_string_view())
        }
        other => {
            exec_err!("`to_utc_timestamp`: timezone must be a string type, got {other}")
        }
    }
}

fn process_arrays<'a, T: ArrowTimestampType, S>(
    return_tz_opt: Option<Arc<str>>,
    ts_array: &ArrayRef,
    tz_array: &'a S,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
{
    let ts_primitive = ts_array.as_primitive::<T>();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(ts_array.len());

    for (ts_opt, tz_opt) in ts_primitive.iter().zip(tz_array.iter()) {
        match (ts_opt, tz_opt) {
            (Some(ts), Some(tz_str)) => {
                let tz: Tz = tz_str.parse().map_err(|e| {
                    exec_datafusion_err!(
                        "`to_utc_timestamp`: invalid timezone '{tz_str}': {e}"
                    )
                })?;
                let val = adjust_to_utc_time::<T>(ts, tz)?;
                builder.append_value(val);
            }
            _ => builder.append_null(),
        }
    }

    builder = builder.with_timezone_opt(return_tz_opt);
    Ok(Arc::new(builder.finish()))
}

fn adjust_to_utc_time<T: ArrowTimestampType>(ts: i64, tz: Tz) -> Result<i64> {
    let dt = match T::UNIT {
        TimeUnit::Nanosecond => Some(DateTime::from_timestamp_nanos(ts)),
        TimeUnit::Microsecond => DateTime::from_timestamp_micros(ts),
        TimeUnit::Millisecond => DateTime::from_timestamp_millis(ts),
        TimeUnit::Second => DateTime::from_timestamp(ts, 0),
    }
    .ok_or_else(|| internal_datafusion_err!("Invalid timestamp"))?;
    let naive_dt = dt.naive_utc();

    let offset_seconds = tz
        .offset_from_utc_datetime(&naive_dt)
        .fix()
        .local_minus_utc() as i64;

    let offset_in_unit = match T::UNIT {
        TimeUnit::Nanosecond => offset_seconds.checked_mul(1_000_000_000),
        TimeUnit::Microsecond => offset_seconds.checked_mul(1_000_000),
        TimeUnit::Millisecond => offset_seconds.checked_mul(1_000),
        TimeUnit::Second => Some(offset_seconds),
    }
    .ok_or_else(|| internal_datafusion_err!("Offset overflow"))?;

    ts.checked_sub(offset_in_unit).ok_or_else(|| {
        internal_datafusion_err!("Timestamp overflow during timezone adjustment")
    })
}
