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

//! Spark-compatible `from_utc_timestamp` function.
//!
//! This function interprets a timestamp as UTC and converts it to a specified timezone.
//! Unlike DataFusion's `to_local_time` which strips the timezone from the result,
//! `from_utc_timestamp` preserves the original timezone annotation on the output.
//!
//! # How it works
//!
//! The function reuses [`adjust_to_local_time`] from DataFusion's `to_local_time` module
//! to perform the actual timestamp adjustment. The key insight is that both functions
//! need to add the timezone offset to the underlying UTC timestamp value.
//!
//! For example, given a timestamp `2024-01-15T10:00:00Z` (UTC) and target timezone
//! `America/New_York` (UTC-5 in winter):
//!
//! 1. The input timestamp is stored as a UTC value (e.g., `1705312800` seconds)
//! 2. `adjust_to_local_time` calculates the offset for `America/New_York` (-5 hours)
//! 3. The offset is added to get the local time value (`1705312800 + (-18000)`)
//! 4. The result represents `2024-01-15T05:00:00` in the target timezone

use std::any::Any;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, AsArray, PrimitiveBuilder, StringArrayType};
use arrow::datatypes::TimeUnit;
use arrow::datatypes::{
    ArrowTimestampType, DataType, Field, FieldRef, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};
use datafusion_functions::datetime::to_local_time::adjust_to_local_time;
use datafusion_functions::utils::make_scalar_function;

/// Spark `from_utc_timestamp` function.
///
/// Interprets the given timestamp as UTC and converts it to the given timezone.
///
/// See <https://spark.apache.org/docs/latest/api/sql/index.html#from_utc_timestamp>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFromUtcTimestamp {
    signature: Signature,
}

impl Default for SparkFromUtcTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFromUtcTimestamp {
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

impl ScalarUDFImpl for SparkFromUtcTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "from_utc_timestamp"
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
        make_scalar_function(spark_from_utc_timestamp, vec![])(&args.args)
    }
}

fn spark_from_utc_timestamp(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [timestamp, timezone] = take_function_args("from_utc_timestamp", args)?;

    match (timestamp.data_type(), timezone.data_type()) {
        (DataType::Timestamp(TimeUnit::Nanosecond, tz_opt), _) => {
            process_timestamp_with_tz_array::<TimestampNanosecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        (DataType::Timestamp(TimeUnit::Microsecond, tz_opt), _) => {
            process_timestamp_with_tz_array::<TimestampMicrosecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        (DataType::Timestamp(TimeUnit::Millisecond, tz_opt), _) => {
            process_timestamp_with_tz_array::<TimestampMillisecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        (DataType::Timestamp(TimeUnit::Second, tz_opt), _) => {
            process_timestamp_with_tz_array::<TimestampSecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        (ts_type, _) => {
            exec_err!("`from_utc_timestamp`: unsupported argument types: {ts_type}")
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
            exec_err!("`from_utc_timestamp`: timezone must be a string type, got {other}")
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
                        "`from_utc_timestamp`: invalid timezone '{tz_str}': {e}"
                    )
                })?;
                let val = adjust_to_local_time::<T>(ts, tz)?;
                builder.append_value(val);
            }
            _ => builder.append_null(),
        }
    }

    builder = builder.with_timezone_opt(return_tz_opt);
    Ok(Arc::new(builder.finish()))
}
