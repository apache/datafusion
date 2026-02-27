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

use crate::function::conversion::cast_utils::{SparkCastOptions, MICROS_PER_SECOND};
use arrow::array::{
    Array, ArrayRef, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{
    DataType, Date32Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use chrono::{NaiveDate, TimeZone};
use datafusion_common::Result;
use std::str::FromStr;
use std::sync::Arc;

macro_rules! cast_int_to_timestamp_impl {
    ($array:expr, $builder:expr, $primitive_type:ty) => {{
        let arr = $array.as_primitive::<$primitive_type>();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                $builder.append_null();
            } else {
                // saturating_mul limits to i64::MIN/MAX on overflow instead of panicking,
                // matching Spark behavior (irrespective of EvalMode)
                let micros = (arr.value(i) as i64).saturating_mul(MICROS_PER_SECOND);
                $builder.append_value(micros);
            }
        }
    }};
}

/// Cast integer types (Int8/16/32/64) to Timestamp(Microsecond) by treating
/// the integer as seconds since epoch.
pub(crate) fn cast_int_to_timestamp(
    array_ref: &ArrayRef,
    target_tz: &Option<Arc<str>>,
) -> Result<ArrayRef> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(array_ref.len());

    match array_ref.data_type() {
        DataType::Int8 => cast_int_to_timestamp_impl!(array_ref, builder, Int8Type),
        DataType::Int16 => cast_int_to_timestamp_impl!(array_ref, builder, Int16Type),
        DataType::Int32 => cast_int_to_timestamp_impl!(array_ref, builder, Int32Type),
        DataType::Int64 => cast_int_to_timestamp_impl!(array_ref, builder, Int64Type),
        dt => {
            return datafusion_common::internal_err!(
                "Unsupported type for cast_int_to_timestamp: {:?}",
                dt
            )
        }
    }

    Ok(Arc::new(
        builder.finish().with_timezone_opt(target_tz.clone()),
    ) as ArrayRef)
}

/// Cast Date32 to Timestamp(Microsecond) with timezone awareness.
/// Converts a date (days since epoch) to a timestamp at midnight in the
/// session timezone, then stores as UTC microseconds.
pub(crate) fn cast_date_to_timestamp(
    array_ref: &ArrayRef,
    cast_options: &SparkCastOptions,
    target_tz: &Option<Arc<str>>,
) -> Result<ArrayRef> {
    let tz_str = if cast_options.timezone.is_empty() {
        "UTC"
    } else {
        cast_options.timezone.as_str()
    };
    let tz: arrow::array::timezone::Tz = tz_str.parse().map_err(|e| {
        datafusion_common::DataFusionError::Internal(format!("Failed to parse timezone: {e}"))
    })?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date_array = array_ref.as_primitive::<Date32Type>();

    let mut builder = TimestampMicrosecondBuilder::with_capacity(date_array.len());

    for date in date_array.iter() {
        match date {
            Some(date) => {
                let naive_date = epoch + chrono::Duration::days(date as i64);
                let local_midnight = naive_date.and_hms_opt(0, 0, 0).unwrap();
                let local_midnight_in_microsec = tz
                    .from_local_datetime(&local_midnight)
                    .earliest()
                    .map(|dt| dt.timestamp_micros())
                    // fallback to UTC if DST ambiguity
                    .unwrap_or((date as i64) * 86_400 * 1_000_000);
                builder.append_value(local_midnight_in_microsec);
            }
            None => {
                builder.append_null();
            }
        }
    }
    Ok(Arc::new(
        builder.finish().with_timezone_opt(target_tz.clone()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Int8Array, Int16Array, Int32Array, Int64Array};
    use arrow::datatypes::{TimestampMicrosecondType, TimeUnit};

    #[test]
    fn test_cast_int_to_timestamp() {
        let timezones: [Option<Arc<str>>; 3] = [
            Some(Arc::from("UTC")),
            Some(Arc::from("America/New_York")),
            None,
        ];

        for tz in &timezones {
            let int8_array: ArrayRef = Arc::new(Int8Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                None,
            ]));

            let result = cast_int_to_timestamp(&int8_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000);
            assert_eq!(ts_array.value(2), -1_000_000);
            assert!(ts_array.is_null(3));
        }
    }

    #[test]
    fn test_cast_int64_overflow() {
        let int64_array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(i64::MAX),
            Some(i64::MIN),
        ]));
        let result =
            cast_int_to_timestamp(&int64_array, &Some(Arc::from("UTC"))).unwrap();
        let ts_array = result.as_primitive::<TimestampMicrosecondType>();
        // saturating_mul should cap at i64::MAX/MIN
        assert_eq!(ts_array.value(0), i64::MAX);
        assert_eq!(ts_array.value(1), i64::MIN);
    }

    #[test]
    fn test_cast_date_to_timestamp_utc() {
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(0),      // epoch
            Some(19723),  // 2024-01-01
            None,
        ]));
        let opts = SparkCastOptions::new(EvalMode::Legacy, "UTC");
        let result = cast_date_to_timestamp(
            &dates,
            &opts,
            &Some(Arc::from("UTC")),
        )
        .unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), 0);
        assert_eq!(ts.value(1), 1704067200000000i64);
        assert!(ts.is_null(2));
    }

    use crate::function::conversion::cast_utils::EvalMode;
}
