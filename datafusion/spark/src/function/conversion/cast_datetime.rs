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

use crate::function::conversion::cast_utils::{MICROS_PER_SECOND, SparkCastOptions};
use arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, Date32Type, Int8Type, Int16Type, Int32Type, Int64Type};
use chrono::{NaiveDate, TimeZone};
use datafusion_common::Result;
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
            );
        }
    }

    Ok(Arc::new(builder.finish().with_timezone_opt(target_tz.clone())) as ArrayRef)
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
        datafusion_common::DataFusionError::Internal(format!(
            "Failed to parse timezone: {e}"
        ))
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
    use arrow::array::{
        Date32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    };
    use arrow::datatypes::TimestampMicrosecondType;

    use crate::function::conversion::cast_utils::EvalMode;

    #[test]
    fn test_cast_int_to_timestamp() {
        let timezones: [Option<Arc<str>>; 3] = [
            Some(Arc::from("UTC")),
            Some(Arc::from("America/New_York")),
            None,
        ];

        for tz in &timezones {
            let int8_array: ArrayRef =
                Arc::new(Int8Array::from(vec![Some(0), Some(1), Some(-1), None]));

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
        let int64_array: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)]));
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
            Some(0),     // epoch
            Some(19723), // 2024-01-01
            None,
        ]));
        let opts = SparkCastOptions::new(EvalMode::Legacy, "UTC");
        let result =
            cast_date_to_timestamp(&dates, &opts, &Some(Arc::from("UTC"))).unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), 0);
        assert_eq!(ts.value(1), 1704067200000000i64);
        assert!(ts.is_null(2));
    }

    #[test]
    fn test_cast_int_to_timestamp_all_types() {
        let timezones: [Option<Arc<str>>; 6] = [
            Some(Arc::from("UTC")),
            Some(Arc::from("America/New_York")),
            Some(Arc::from("America/Los_Angeles")),
            Some(Arc::from("Europe/London")),
            Some(Arc::from("Asia/Tokyo")),
            Some(Arc::from("Australia/Sydney")),
        ];

        for tz in &timezones {
            // Int8 with boundary values
            let int8_array: ArrayRef = Arc::new(Int8Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(127),
                Some(-128),
                None,
            ]));
            let result = cast_int_to_timestamp(&int8_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000);
            assert_eq!(ts_array.value(2), -1_000_000);
            assert_eq!(ts_array.value(3), 127_000_000);
            assert_eq!(ts_array.value(4), -128_000_000);
            assert!(ts_array.is_null(5));
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));

            // Int16 with boundary values
            let int16_array: ArrayRef = Arc::new(Int16Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(32767),
                Some(-32768),
                None,
            ]));
            let result = cast_int_to_timestamp(&int16_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000);
            assert_eq!(ts_array.value(2), -1_000_000);
            assert_eq!(ts_array.value(3), 32_767_000_000_i64);
            assert_eq!(ts_array.value(4), -32_768_000_000_i64);
            assert!(ts_array.is_null(5));
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));

            // Int32 with a realistic epoch seconds value
            let int32_array: ArrayRef = Arc::new(Int32Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(1704067200), // 2024-01-01
                None,
            ]));
            let result = cast_int_to_timestamp(&int32_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000);
            assert_eq!(ts_array.value(2), -1_000_000);
            assert_eq!(ts_array.value(3), 1_704_067_200_000_000_i64);
            assert!(ts_array.is_null(4));
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));

            // Int64 with MAX/MIN (overflow-saturating)
            let int64_array: ArrayRef = Arc::new(Int64Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(i64::MAX),
                Some(i64::MIN),
            ]));
            let result = cast_int_to_timestamp(&int64_array, tz).unwrap();
            let ts_array = result.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(ts_array.value(0), 0);
            assert_eq!(ts_array.value(1), 1_000_000_i64);
            assert_eq!(ts_array.value(2), -1_000_000_i64);
            assert_eq!(ts_array.value(3), i64::MAX);
            assert_eq!(ts_array.value(4), i64::MIN);
            assert_eq!(ts_array.timezone(), tz.as_ref().map(|s| s.as_ref()));
        }
    }

    #[test]
    fn test_cast_date_to_timestamp_dst() {
        // epoch, 2024-01-01, 2024-03-11 (DST spring-forward in US)
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(19723),
            Some(19793),
            None,
        ]));

        let non_dst_date = 1704067200000000_i64;
        let dst_date = 1710115200000000_i64;
        let seven_hours_ts = 25200000000_i64;
        let eight_hours_ts = 28800000000_i64;

        // America/Los_Angeles: DST-aware
        let opts = SparkCastOptions::new(EvalMode::Legacy, "America/Los_Angeles");
        let result =
            cast_date_to_timestamp(&dates, &opts, &Some(Arc::from("UTC"))).unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), eight_hours_ts);
        assert_eq!(ts.value(1), non_dst_date + eight_hours_ts);
        // DST: spring forward -> only 7 hours offset
        assert_eq!(ts.value(2), dst_date + seven_hours_ts);
        assert!(ts.is_null(3));

        // America/Phoenix: no DST, always 7 hours offset
        let opts = SparkCastOptions::new(EvalMode::Legacy, "America/Phoenix");
        let result =
            cast_date_to_timestamp(&dates, &opts, &Some(Arc::from("UTC"))).unwrap();
        let ts = result.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(ts.value(0), seven_hours_ts);
        assert_eq!(ts.value(1), non_dst_date + seven_hours_ts);
        assert_eq!(ts.value(2), dst_date + seven_hours_ts);
        assert!(ts.is_null(3));
    }
}
