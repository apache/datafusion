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

use arrow::array::temporal_conversions::NANOSECONDS;
use arrow::array::types::{
    ArrowTimestampType, IntervalDayTimeType, IntervalMonthDayNanoType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::DataType::{Time32, Time64, Timestamp};
use arrow::datatypes::IntervalUnit::{DayTime, MonthDayNano};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{
    DataType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit,
};
use arrow::error::ArrowError;
use arrow::temporal_conversions::NANOSECONDS_IN_DAY;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{Result, ScalarValue, exec_err, not_impl_err, plan_err};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TIMEZONE_WILDCARD, Volatility,
};
use datafusion_macros::user_doc;

use chrono::{DateTime, Datelike, Duration, Months, TimeDelta, Utc};

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Calculates time intervals and returns the start of the interval nearest to the specified timestamp. Use `date_bin` to downsample time series data by grouping rows into time-based "bins" or "windows" and applying an aggregate or selector function to each window.

For example, if you "bin" or "window" data into 15 minute intervals, an input timestamp of `2023-01-01T18:18:18Z` will be updated to the start time of the 15 minute bin it is in: `2023-01-01T18:15:00Z`.
"#,
    syntax_example = "date_bin(interval, expression, origin-timestamp)",
    sql_example = r#"```sql
-- Bin the timestamp into 1 day intervals
> SELECT date_bin(interval '1 day', time) as bin
FROM VALUES ('2023-01-01T18:18:18Z'), ('2023-01-03T19:00:03Z')  t(time);
+---------------------+
| bin                 |
+---------------------+
| 2023-01-01T00:00:00 |
| 2023-01-03T00:00:00 |
+---------------------+
2 row(s) fetched.

-- Bin the timestamp into 1 day intervals starting at 3AM on  2023-01-01
> SELECT date_bin(interval '1 day', time,  '2023-01-01T03:00:00') as bin
FROM VALUES ('2023-01-01T18:18:18Z'), ('2023-01-03T19:00:03Z')  t(time);
+---------------------+
| bin                 |
+---------------------+
| 2023-01-01T03:00:00 |
| 2023-01-03T03:00:00 |
+---------------------+
2 row(s) fetched.

-- Bin the time into 15 minute intervals starting at 1 min
>  SELECT date_bin(interval '15 minutes', time, TIME '00:01:00') as bin
FROM VALUES (TIME '02:18:18'), (TIME '19:00:03')  t(time);
+----------+
| bin      |
+----------+
| 02:16:00 |
| 18:46:00 |
+----------+
2 row(s) fetched.
```"#,
    argument(name = "interval", description = "Bin interval."),
    argument(
        name = "expression",
        description = "Time expression to operate on. Can be a constant, column, or function."
    ),
    argument(
        name = "origin-timestamp",
        description = r#"Optional. Starting point used to determine bin boundaries. If not specified defaults 1970-01-01T00:00:00Z (the UNIX epoch in UTC). The following intervals are supported:

    - nanoseconds
    - microseconds
    - milliseconds
    - seconds
    - minutes
    - hours
    - days
    - weeks
    - months
    - years
    - century
"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DateBinFunc {
    signature: Signature,
}

impl Default for DateBinFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DateBinFunc {
    pub fn new() -> Self {
        let base_sig = |array_type: TimeUnit| {
            let mut v = vec![
                Exact(vec![
                    DataType::Interval(MonthDayNano),
                    Timestamp(array_type, None),
                    Timestamp(Nanosecond, None),
                ]),
                Exact(vec![
                    DataType::Interval(MonthDayNano),
                    Timestamp(array_type, Some(TIMEZONE_WILDCARD.into())),
                    Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                ]),
                Exact(vec![
                    DataType::Interval(DayTime),
                    Timestamp(array_type, None),
                    Timestamp(Nanosecond, None),
                ]),
                Exact(vec![
                    DataType::Interval(DayTime),
                    Timestamp(array_type, Some(TIMEZONE_WILDCARD.into())),
                    Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                ]),
                Exact(vec![
                    DataType::Interval(MonthDayNano),
                    Timestamp(array_type, None),
                ]),
                Exact(vec![
                    DataType::Interval(MonthDayNano),
                    Timestamp(array_type, Some(TIMEZONE_WILDCARD.into())),
                ]),
                Exact(vec![
                    DataType::Interval(DayTime),
                    Timestamp(array_type, None),
                ]),
                Exact(vec![
                    DataType::Interval(DayTime),
                    Timestamp(array_type, Some(TIMEZONE_WILDCARD.into())),
                ]),
            ];

            match array_type {
                Second | Millisecond => {
                    v.append(&mut vec![
                        Exact(vec![
                            DataType::Interval(MonthDayNano),
                            Time32(array_type),
                            Time32(array_type),
                        ]),
                        Exact(vec![DataType::Interval(MonthDayNano), Time32(array_type)]),
                        Exact(vec![
                            DataType::Interval(DayTime),
                            Time32(array_type),
                            Time32(array_type),
                        ]),
                        Exact(vec![DataType::Interval(DayTime), Time32(array_type)]),
                    ]);
                }
                Microsecond | Nanosecond => {
                    v.append(&mut vec![
                        Exact(vec![
                            DataType::Interval(DayTime),
                            Time64(array_type),
                            Time64(array_type),
                        ]),
                        Exact(vec![DataType::Interval(DayTime), Time64(array_type)]),
                        Exact(vec![
                            DataType::Interval(MonthDayNano),
                            Time64(array_type),
                            Time64(array_type),
                        ]),
                        Exact(vec![DataType::Interval(MonthDayNano), Time64(array_type)]),
                    ]);
                }
            }

            v
        };

        let full_sig = [Nanosecond, Microsecond, Millisecond, Second]
            .into_iter()
            .map(base_sig)
            .collect::<Vec<_>>()
            .concat();

        Self {
            signature: Signature::one_of(full_sig, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateBinFunc {
    fn name(&self) -> &str {
        "date_bin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[1] {
            Timestamp(tu, tz_opt) => Ok(Timestamp(*tu, tz_opt.clone())),
            Time32(tu) => Ok(Time32(*tu)),
            Time64(tu) => Ok(Time64(*tu)),
            _ => plan_err!(
                "The date_bin function can only accept timestamp or time as the second arg."
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        if args.len() == 2 {
            let origin = match args[1].data_type() {
                Time32(Second) => {
                    ColumnarValue::Scalar(ScalarValue::Time32Second(Some(0)))
                }
                Time32(Millisecond) => {
                    ColumnarValue::Scalar(ScalarValue::Time32Millisecond(Some(0)))
                }
                Time64(Microsecond) => {
                    ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(0)))
                }
                Time64(Nanosecond) => {
                    ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(0)))
                }
                _ => {
                    // Default to unix EPOCH
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(0),
                        Some("+00:00".into()),
                    ))
                }
            };
            date_bin_impl(&args[0], &args[1], &origin)
        } else if args.len() == 3 {
            date_bin_impl(&args[0], &args[1], &args[2])
        } else {
            exec_err!("DATE_BIN expected two or three arguments")
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // The DATE_BIN function preserves the order of its second argument.
        let step = &input[0];
        let date_value = &input[1];
        let reference = input.get(2);

        if step.sort_properties.eq(&SortProperties::Singleton)
            && reference
                .map(|r| r.sort_properties.eq(&SortProperties::Singleton))
                .unwrap_or(true)
        {
            Ok(date_value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_SEC: i64 = NANOSECONDS;
/// Function type for binning timestamps into intervals
///
/// Arguments:
/// * `stride` - Interval width (nanoseconds for time-based, months for month-based)
/// * `source` - Timestamp to bin (nanoseconds since epoch)
/// * `origin` - Origin timestamp (nanoseconds since epoch)
///
/// Returns: Binned timestamp in nanoseconds, or error if out of range
type BinFunction = fn(i64, i64, i64) -> Result<i64>;
enum Interval {
    Nanoseconds(i64),
    Months(i64),
}

impl Interval {
    /// Returns (`stride_nanos`, `fn`) where
    ///
    /// 1. `stride_nanos` is a width, in nanoseconds
    /// 2. `fn` is a function that takes (stride_nanos, source, origin)
    ///
    /// `source` is the timestamp being binned
    ///
    /// `origin`  is the time, in nanoseconds, where windows are measured from
    fn bin_fn(&self) -> (i64, BinFunction) {
        match self {
            Interval::Nanoseconds(nanos) => (*nanos, date_bin_nanos_interval),
            Interval::Months(months) => (*months, date_bin_months_interval),
        }
    }
}

// return time in nanoseconds that the source timestamp falls into based on the stride and origin
fn date_bin_nanos_interval(stride_nanos: i64, source: i64, origin: i64) -> Result<i64> {
    let time_diff = source.checked_sub(origin).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "date_bin source timestamp {source} - origin {origin} overflows i64"
        ))
    })?;

    // distance from origin to bin
    let time_delta = compute_distance(time_diff, stride_nanos)?;

    origin.checked_add(time_delta).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "date_bin origin {origin} + delta {time_delta} overflows i64"
        ))
        .into()
    })
}

// distance from origin to bin
fn compute_distance(time_diff: i64, stride: i64) -> Result<i64> {
    let remainder = time_diff.checked_rem(stride).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "date_bin compute_distance time_diff {time_diff} % stride {stride} overflows i64"
        ))
    })?;
    let time_delta = time_diff.checked_sub(remainder).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "date_bin compute_distance time_diff {time_diff} - remainder {remainder} overflows i64"
        ))
    })?;

    if time_diff < 0 && stride > 1 && time_delta != time_diff {
        // The origin is later than the source timestamp, round down to the previous bin
        time_delta.checked_sub(stride).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "date_bin compute_distance time_delta {time_delta} - stride {stride} overflows i64"
            ))
            .into()
        })
    } else {
        Ok(time_delta)
    }
}

// return time in nanoseconds that the source timestamp falls into based on the stride and origin
fn date_bin_months_interval(stride_months: i64, source: i64, origin: i64) -> Result<i64> {
    // convert source and origin to DateTime<Utc>
    let source_date = to_utc_date_time(source)?;
    let origin_date = to_utc_date_time(origin)?;

    // calculate the number of months between the source and origin
    let month_diff = (source_date.year() - origin_date.year()) * 12
        + source_date.month() as i32
        - origin_date.month() as i32;

    // distance from origin to bin
    let month_delta = compute_distance(month_diff as i64, stride_months)?;

    let mut bin_time = if month_delta < 0 {
        match origin_date
            .checked_sub_months(Months::new(month_delta.unsigned_abs() as u32))
        {
            Some(dt) => dt,
            None => return exec_err!("DATE_BIN month subtraction out of range"),
        }
    } else {
        match origin_date.checked_add_months(Months::new(month_delta as u32)) {
            Some(dt) => dt,
            None => return exec_err!("DATE_BIN month addition out of range"),
        }
    };

    // If origin is not midnight of first date of the month, the bin_time may be larger than the source
    // In this case, we need to move back to previous bin
    if bin_time > source_date {
        let month_delta = month_delta - stride_months;
        bin_time = if month_delta < 0 {
            match origin_date
                .checked_sub_months(Months::new(month_delta.unsigned_abs() as u32))
            {
                Some(dt) => dt,
                None => return exec_err!("DATE_BIN month subtraction out of range"),
            }
        } else {
            match origin_date.checked_add_months(Months::new(month_delta as u32)) {
                Some(dt) => dt,
                None => return exec_err!("DATE_BIN month addition out of range"),
            }
        };
    }
    match bin_time.timestamp_nanos_opt() {
        Some(nanos) => Ok(nanos),
        None => exec_err!("DATE_BIN result timestamp out of range"),
    }
}

fn to_utc_date_time(nanos: i64) -> Result<DateTime<Utc>> {
    // Keep negative sub-second values normalized as seconds + non-negative nanos.
    let secs = nanos.div_euclid(NANOS_PER_SEC);
    let nsec = nanos.rem_euclid(NANOS_PER_SEC) as u32;
    match DateTime::from_timestamp(secs, nsec) {
        Some(dt) => Ok(dt),
        None => exec_err!("Invalid timestamp value"),
    }
}

fn timestamp_scale<T: ArrowTimestampType>() -> i64 {
    match T::UNIT {
        Nanosecond => 1,
        Microsecond => NANOS_PER_MICRO,
        Millisecond => NANOS_PER_MILLI,
        Second => NANOSECONDS,
    }
}

// Scale to nanoseconds and report overflow as a normal error.
fn checked_scale_to_nanos(x: i64, scale: i64) -> Result<i64> {
    match x.checked_mul(scale) {
        Some(scaled) => Ok(scaled),
        None => exec_err!("date_bin timestamp value {x} * scale {scale} overflows i64"),
    }
}

fn validate_time_stride(stride: &Interval) -> Result<()> {
    match stride {
        Interval::Months(m) if *m > 0 => {
            exec_err!("DATE_BIN stride for TIME input must be less than 1 day")
        }
        Interval::Nanoseconds(ns) if *ns >= NANOSECONDS_IN_DAY => {
            exec_err!("DATE_BIN stride for TIME input must be less than 1 day")
        }
        _ => Ok(()),
    }
}

// Supported intervals:
//  1. IntervalDayTime: this means that the stride is in days, hours, minutes, seconds and milliseconds
//     We will assume month interval won't be converted into this type
//     TODO (my next PR): without `INTERVAL` keyword, the stride was converted into ScalarValue::IntervalDayTime somewhere
//             for month interval. I need to find that and make it ScalarValue::IntervalMonthDayNano instead
// 2. IntervalMonthDayNano
fn date_bin_impl(
    stride: &ColumnarValue,
    array: &ColumnarValue,
    origin: &ColumnarValue,
) -> Result<ColumnarValue> {
    let stride = match stride {
        ColumnarValue::Scalar(s) if s.is_null() => {
            // NULL stride -> NULL result (standard SQL NULL propagation)
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                array.data_type(),
            )?));
        }
        ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(v))) => {
            let (days, ms) = IntervalDayTimeType::to_parts(*v);
            let nanos = (TimeDelta::try_days(days as i64).unwrap()
                + TimeDelta::try_milliseconds(ms as i64).unwrap())
            .num_nanoseconds();

            match nanos {
                Some(v) => Interval::Nanoseconds(v),
                _ => return exec_err!("DATE_BIN stride argument is too large"),
            }
        }
        ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(v))) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);

            // If interval is months, its origin must be midnight of first date of the month
            if months != 0 {
                // Return error if days or nanos is not zero
                if days != 0 || nanos != 0 {
                    return not_impl_err!(
                        "DATE_BIN stride does not support combination of month, day and nanosecond intervals"
                    );
                } else {
                    Interval::Months(months as i64)
                }
            } else {
                let nanos = (TimeDelta::try_days(days as i64).unwrap()
                    + Duration::nanoseconds(nanos))
                .num_nanoseconds();
                match nanos {
                    Some(v) => Interval::Nanoseconds(v),
                    _ => return exec_err!("DATE_BIN stride argument is too large"),
                }
            }
        }
        ColumnarValue::Scalar(v) => {
            return exec_err!(
                "DATE_BIN expects stride argument to be an INTERVAL but got {}",
                v.data_type()
            );
        }
        ColumnarValue::Array(_) => {
            return not_impl_err!(
                "DATE_BIN only supports literal values for the stride argument, not arrays"
            );
        }
    };

    let (origin, is_time) = match origin {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => {
            (*v, false)
        }
        ColumnarValue::Scalar(ScalarValue::Time32Millisecond(Some(v))) => {
            validate_time_stride(&stride)?;
            // TIME origins can come from reinterpret casts, so scale defensively.
            (checked_scale_to_nanos(*v as i64, NANOS_PER_MILLI)?, true)
        }
        ColumnarValue::Scalar(ScalarValue::Time32Second(Some(v))) => {
            validate_time_stride(&stride)?;
            (checked_scale_to_nanos(*v as i64, NANOS_PER_SEC)?, true)
        }
        ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(v))) => {
            validate_time_stride(&stride)?;
            (checked_scale_to_nanos(*v, NANOS_PER_MICRO)?, true)
        }
        ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(v))) => {
            validate_time_stride(&stride)?;
            (*v, true)
        }
        ColumnarValue::Scalar(v) => {
            return exec_err!(
                "DATE_BIN expects origin argument to be a TIMESTAMP with nanosecond precision or a TIME but got {}",
                v.data_type()
            );
        }
        ColumnarValue::Array(_) => {
            return not_impl_err!(
                "DATE_BIN only supports literal values for the origin argument, not arrays"
            );
        }
    };

    let (stride, stride_fn) = stride.bin_fn();

    // Return error if stride is 0
    if stride == 0 {
        return exec_err!("DATE_BIN stride must be non-zero");
    }

    fn transform_scalar_with_stride<T: ArrowTimestampType>(
        value: Option<i64>,
        origin: i64,
        stride: i64,
        stride_fn: BinFunction,
    ) -> Option<i64> {
        let scale = timestamp_scale::<T>();
        value
            .and_then(|val| val.checked_mul(scale))
            .and_then(|scaled| stride_fn(stride, scaled, origin).ok())
            .map(|binned| binned / scale)
    }

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                transform_scalar_with_stride::<TimestampNanosecondType>(
                    *v, origin, stride, stride_fn,
                ),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                transform_scalar_with_stride::<TimestampMicrosecondType>(
                    *v, origin, stride, stride_fn,
                ),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                transform_scalar_with_stride::<TimestampMillisecondType>(
                    *v, origin, stride, stride_fn,
                ),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampSecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                transform_scalar_with_stride::<TimestampSecondType>(
                    *v, origin, stride, stride_fn,
                ),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::Time32Millisecond(v)) => {
            if !is_time {
                return exec_err!("DATE_BIN with Time32 source requires Time32 origin");
            }
            let result = v
                .and_then(|x| (x as i64).checked_mul(NANOS_PER_MILLI))
                .and_then(|scaled| stride_fn(stride, scaled, origin).ok())
                .map(|binned| ((binned % NANOSECONDS_IN_DAY) / NANOS_PER_MILLI) as i32);
            ColumnarValue::Scalar(ScalarValue::Time32Millisecond(result))
        }
        ColumnarValue::Scalar(ScalarValue::Time32Second(v)) => {
            if !is_time {
                return exec_err!("DATE_BIN with Time32 source requires Time32 origin");
            }
            let result = v
                .and_then(|x| (x as i64).checked_mul(NANOS_PER_SEC))
                .and_then(|scaled| stride_fn(stride, scaled, origin).ok())
                .map(|binned| ((binned % NANOSECONDS_IN_DAY) / NANOS_PER_SEC) as i32);
            ColumnarValue::Scalar(ScalarValue::Time32Second(result))
        }
        ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(v)) => {
            if !is_time {
                return exec_err!("DATE_BIN with Time64 source requires Time64 origin");
            }
            let result = v.and_then(|x| {
                stride_fn(stride, x, origin)
                    .map(|binned| binned % NANOSECONDS_IN_DAY)
                    .ok()
            });
            ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(result))
        }
        ColumnarValue::Scalar(ScalarValue::Time64Microsecond(v)) => {
            if !is_time {
                return exec_err!("DATE_BIN with Time64 source requires Time64 origin");
            }
            let result = v
                .and_then(|x| x.checked_mul(NANOS_PER_MICRO))
                .and_then(|scaled| stride_fn(stride, scaled, origin).ok())
                .map(|binned| (binned % NANOSECONDS_IN_DAY) / NANOS_PER_MICRO);
            ColumnarValue::Scalar(ScalarValue::Time64Microsecond(result))
        }
        ColumnarValue::Array(array) => {
            fn transform_array_with_stride<T>(
                origin: i64,
                stride: i64,
                stride_fn: BinFunction,
                array: &ArrayRef,
                tz_opt: &Option<Arc<str>>,
            ) -> Result<ColumnarValue>
            where
                T: ArrowTimestampType,
            {
                let array = as_primitive_array::<T>(array)?;
                let scale = timestamp_scale::<T>();

                // Per-row errors become NULL, matching scalar behavior.
                let result: PrimitiveArray<T> = array.unary_opt(|val| {
                    val.checked_mul(scale)
                        .and_then(|scaled| stride_fn(stride, scaled, origin).ok())
                        .map(|binned| binned / scale)
                });

                let array = result.with_timezone_opt(tz_opt.clone());
                Ok(ColumnarValue::Array(Arc::new(array)))
            }

            match array.data_type() {
                Timestamp(Nanosecond, tz_opt) => {
                    transform_array_with_stride::<TimestampNanosecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                Timestamp(Microsecond, tz_opt) => {
                    transform_array_with_stride::<TimestampMicrosecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                Timestamp(Millisecond, tz_opt) => {
                    transform_array_with_stride::<TimestampMillisecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                Timestamp(Second, tz_opt) => {
                    transform_array_with_stride::<TimestampSecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                Time32(Millisecond) => {
                    if !is_time {
                        return exec_err!(
                            "DATE_BIN with Time32 source requires Time32 origin"
                        );
                    }
                    let array = array.as_primitive::<Time32MillisecondType>();
                    let result: PrimitiveArray<Time32MillisecondType> =
                        array.unary_opt(|x| {
                            (x as i64)
                                .checked_mul(NANOS_PER_MILLI)
                                .and_then(|scaled| stride_fn(stride, scaled, origin).ok())
                                .map(|binned| {
                                    ((binned % NANOSECONDS_IN_DAY) / NANOS_PER_MILLI)
                                        as i32
                                })
                        });
                    ColumnarValue::Array(Arc::new(result))
                }
                Time32(Second) => {
                    if !is_time {
                        return exec_err!(
                            "DATE_BIN with Time32 source requires Time32 origin"
                        );
                    }
                    let array = array.as_primitive::<Time32SecondType>();
                    let result: PrimitiveArray<Time32SecondType> = array.unary_opt(|x| {
                        (x as i64)
                            .checked_mul(NANOS_PER_SEC)
                            .and_then(|scaled| stride_fn(stride, scaled, origin).ok())
                            .map(|binned| {
                                ((binned % NANOSECONDS_IN_DAY) / NANOS_PER_SEC) as i32
                            })
                    });
                    ColumnarValue::Array(Arc::new(result))
                }
                Time64(Microsecond) => {
                    if !is_time {
                        return exec_err!(
                            "DATE_BIN with Time64 source requires Time64 origin"
                        );
                    }
                    let array = array.as_primitive::<Time64MicrosecondType>();
                    let result: PrimitiveArray<Time64MicrosecondType> =
                        array.unary_opt(|x| {
                            x.checked_mul(NANOS_PER_MICRO)
                                .and_then(|scaled| stride_fn(stride, scaled, origin).ok())
                                .map(|binned| {
                                    (binned % NANOSECONDS_IN_DAY) / NANOS_PER_MICRO
                                })
                        });
                    ColumnarValue::Array(Arc::new(result))
                }
                Time64(Nanosecond) => {
                    if !is_time {
                        return exec_err!(
                            "DATE_BIN with Time64 source requires Time64 origin"
                        );
                    }
                    let array = array.as_primitive::<Time64NanosecondType>();
                    let result: PrimitiveArray<Time64NanosecondType> =
                        array.unary_opt(|x| {
                            stride_fn(stride, x, origin)
                                .map(|binned_nanos| binned_nanos % (NANOSECONDS_IN_DAY))
                                .ok()
                        });
                    ColumnarValue::Array(Arc::new(result))
                }
                _ => {
                    return exec_err!(
                        "DATE_BIN expects source argument to be a TIMESTAMP or TIME but got {}",
                        array.data_type()
                    );
                }
            }
        }
        _ => {
            return exec_err!(
                "DATE_BIN expects source argument to be a TIMESTAMP or TIME scalar or array"
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datetime::date_bin::{DateBinFunc, date_bin_nanos_interval};
    use arrow::array::types::TimestampNanosecondType;
    use arrow::array::{Array, IntervalDayTimeArray, TimestampNanosecondArray};
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};

    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use chrono::TimeDelta;
    use datafusion_common::config::ConfigOptions;

    fn invoke_date_bin_with_args(
        args: Vec<ColumnarValue>,
        number_rows: usize,
        return_field: &FieldRef,
    ) -> Result<ColumnarValue, DataFusionError> {
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type(), true).into())
            .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::clone(return_field),
            config_options: Arc::new(ConfigOptions::default()),
        };
        DateBinFunc::new().invoke_with_args(args)
    }

    fn assert_null_scalar(value: ColumnarValue, expected_type: DataType) {
        let ColumnarValue::Scalar(value) = value else {
            panic!("expected scalar, got {value:?}");
        };
        assert_eq!(value.data_type(), expected_type);
        assert!(value.is_null(), "expected NULL, got {value:?}");
    }

    fn assert_array_null_then_valid(value: ColumnarValue, expected_type: DataType) {
        let ColumnarValue::Array(array) = value else {
            panic!("expected array, got {value:?}");
        };
        assert_eq!(array.data_type(), &expected_type);
        assert!(array.is_null(0), "expected NULL at row 0");
        assert!(array.is_valid(1), "expected valid value at row 1");
    }

    fn assert_overflow_error(result: Result<ColumnarValue, DataFusionError>) {
        let err = result.expect_err("expected overflow error");
        assert!(
            err.strip_backtrace().contains("overflows i64"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_date_bin() {
        let return_field = &Arc::new(Field::new(
            "f",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ));

        let mut args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert!(res.is_ok());

        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let batch_len = timestamps.len();
        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Array(timestamps),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, batch_len, return_field);
        assert!(res.is_ok());

        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert!(res.is_ok());

        // stride supports month-day-nano
        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano {
                    months: 0,
                    days: 0,
                    nanoseconds: 1,
                },
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert!(res.is_ok());

        //
        // Fallible test cases
        //

        // invalid number of arguments
        args = vec![ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(
            IntervalDayTime {
                days: 0,
                milliseconds: 1,
            },
        )))];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expected two or three arguments"
        );

        // stride: invalid type
        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expects stride argument to be an INTERVAL but got Interval(YearMonth)"
        );

        // stride: invalid value

        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 0,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];

        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride must be non-zero"
        );

        // stride: overflow of day-time interval
        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(
                IntervalDayTime::MAX,
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // stride: overflow of month-day-nano interval
        args = vec![
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(0, i32::MAX, 1)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // stride: month intervals
        args = vec![
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(1, 1, 1)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN stride does not support combination of month, day and nanosecond intervals"
        );

        // origin: invalid type
        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expects origin argument to be a TIMESTAMP with nanosecond precision or a TIME but got Timestamp(µs)"
        );

        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert!(res.is_ok());

        // unsupported array type for stride
        let intervals = Arc::new(
            (1..6)
                .map(|x| {
                    Some(IntervalDayTime {
                        days: 0,
                        milliseconds: x,
                    })
                })
                .collect::<IntervalDayTimeArray>(),
        );
        args = vec![
            ColumnarValue::Array(intervals),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];
        let res = invoke_date_bin_with_args(args, 1, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN only supports literal values for the stride argument, not arrays"
        );

        // unsupported array type for origin
        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let batch_len = timestamps.len();
        args = vec![
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Array(timestamps),
        ];
        let res = invoke_date_bin_with_args(args, batch_len, return_field);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN only supports literal values for the origin argument, not arrays"
        );
    }

    #[test]
    fn test_date_bin_timezones() {
        let cases = [
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("+00".into()),
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                None,
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("-02".into()),
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T01:00:00+05",
                    "2020-09-08T02:00:00+05",
                    "2020-09-08T03:00:00+05",
                    "2020-09-08T04:00:00+05",
                ],
                Some("+05".into()),
                "1970-01-01T00:00:00+05",
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T01:00:00+08",
                    "2020-09-08T02:00:00+08",
                    "2020-09-08T03:00:00+08",
                    "2020-09-08T04:00:00+08",
                ],
                Some("+08".into()),
                "1970-01-01T00:00:00+08",
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                ],
            ),
        ];

        cases
            .iter()
            .for_each(|(original, tz_opt, origin, expected)| {
                let input = original
                    .iter()
                    .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                    .collect::<TimestampNanosecondArray>()
                    .with_timezone_opt(tz_opt.clone());
                let right = expected
                    .iter()
                    .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                    .collect::<TimestampNanosecondArray>()
                    .with_timezone_opt(tz_opt.clone());
                let batch_len = input.len();
                let args = vec![
                    ColumnarValue::Scalar(ScalarValue::new_interval_dt(1, 0)),
                    ColumnarValue::Array(Arc::new(input)),
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(string_to_timestamp_nanos(origin).unwrap()),
                        tz_opt.clone(),
                    )),
                ];
                let return_field = &Arc::new(Field::new(
                    "f",
                    DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone()),
                    true,
                ));
                let result =
                    invoke_date_bin_with_args(args, batch_len, return_field).unwrap();

                if let ColumnarValue::Array(result) = result {
                    assert_eq!(
                        result.data_type(),
                        &DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
                    );
                    let left = arrow::array::cast::as_primitive_array::<
                        TimestampNanosecondType,
                    >(&result);
                    assert_eq!(left, &right);
                } else {
                    panic!("unexpected column type");
                }
            });
    }

    #[test]
    fn test_date_bin_single() {
        let cases = [
            (
                (
                    TimeDelta::try_minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    TimeDelta::try_minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    TimeDelta::try_minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2005-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    TimeDelta::try_hours(1),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    TimeDelta::try_seconds(10),
                    "2004-04-09T02:03:11.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:03:10Z",
            ),
        ];

        cases
            .iter()
            .for_each(|((stride, source, origin), expected)| {
                let stride = stride.unwrap();
                let stride1 = stride.num_nanoseconds().unwrap();
                let source1 = string_to_timestamp_nanos(source).unwrap();
                let origin1 = string_to_timestamp_nanos(origin).unwrap();

                let expected1 = string_to_timestamp_nanos(expected).unwrap();
                let result = date_bin_nanos_interval(stride1, source1, origin1).unwrap();
                assert_eq!(result, expected1, "{source} = {expected}");
            })
    }

    #[test]
    fn test_date_bin_before_epoch() {
        let cases = [
            (
                (TimeDelta::try_minutes(15), "1969-12-31T23:44:59.999999999"),
                "1969-12-31T23:30:00",
            ),
            (
                (TimeDelta::try_minutes(15), "1969-12-31T23:45:00"),
                "1969-12-31T23:45:00",
            ),
            (
                (TimeDelta::try_minutes(15), "1969-12-31T23:45:00.000000001"),
                "1969-12-31T23:45:00",
            ),
        ];

        cases.iter().for_each(|((stride, source), expected)| {
            let stride = stride.unwrap();
            let stride1 = stride.num_nanoseconds().unwrap();
            let source1 = string_to_timestamp_nanos(source).unwrap();

            let expected1 = string_to_timestamp_nanos(expected).unwrap();
            let result = date_bin_nanos_interval(stride1, source1, 0).unwrap();
            assert_eq!(result, expected1, "{source} = {expected}");
        })
    }

    #[test]
    fn test_date_bin_out_of_range() {
        let return_field = &Arc::new(Field::new(
            "f",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ));
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(1637426858, 0, 0)),
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                Some(1040292460),
                None,
            )),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(string_to_timestamp_nanos("1984-01-07 00:00:00").unwrap()),
                None,
            )),
        ];

        let result = invoke_date_bin_with_args(args, 1, return_field);
        assert!(result.is_ok());
        if let ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(val, _)) =
            result.unwrap()
        {
            assert!(val.is_none(), "Expected None for out of range operation");
        }
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(1637426858, 0, 0)),
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                Some(-1040292460),
                None,
            )),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(string_to_timestamp_nanos("1984-01-07 00:00:00").unwrap()),
                None,
            )),
        ];

        let result = invoke_date_bin_with_args(args, 1, return_field);
        assert!(result.is_ok());
        if let ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(val, _)) =
            result.unwrap()
        {
            assert!(val.is_none(), "Expected None for out of range operation");
        }
    }

    #[test]
    fn test_date_bin_compute_distance_i64_min() {
        // Regression for #22215: date_bin_nanos_interval on a source near i64::MIN
        // previously panicked inside compute_distance with "attempt to subtract with overflow".
        // Now it must return a normal Err that the scalar pipeline maps to NULL.
        let result = date_bin_nanos_interval(3, i64::MIN, 0);
        assert!(
            result.is_err(),
            "expected Err for source=i64::MIN, got {result:?}"
        );

        let return_field = &Arc::new(Field::new(
            "f",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ));
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(0, 0, 3)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(i64::MIN), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(0), None)),
        ];
        let result = invoke_date_bin_with_args(args, 1, return_field);
        assert!(result.is_ok(), "expected Ok with NULL, got {result:?}");
        if let ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(val, _)) =
            result.unwrap()
        {
            assert!(
                val.is_none(),
                "Expected None for compute_distance overflow, got {val:?}"
            );
        } else {
            panic!("Expected TimestampNanosecond scalar");
        }
    }

    #[test]
    fn test_date_bin_scale_overflow_returns_null() {
        // Scaling non-nanosecond timestamps to nanoseconds can overflow.
        use arrow::array::{
            ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampSecondArray,
        };

        let scalar_cases = [
            ScalarValue::TimestampSecond(Some(i64::MAX), None),
            ScalarValue::TimestampMillisecond(Some(i64::MAX), None),
            ScalarValue::TimestampMicrosecond(Some(i64::MAX), None),
        ];
        for source in scalar_cases {
            let expected_type = source.data_type();
            let return_field = Arc::new(Field::new("f", expected_type.clone(), true));
            let args = vec![
                ColumnarValue::Scalar(ScalarValue::new_interval_dt(1, 0)),
                ColumnarValue::Scalar(source),
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(0), None)),
            ];
            let result = invoke_date_bin_with_args(args, 1, &return_field)
                .unwrap_or_else(|e| panic!("expected Ok for {expected_type}, got {e:?}"));
            assert_null_scalar(result, expected_type);
        }

        let array_cases: Vec<ArrayRef> = vec![
            Arc::new(TimestampSecondArray::from(vec![Some(i64::MAX), Some(0)])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(i64::MAX),
                Some(0),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(i64::MAX),
                Some(0),
            ])),
        ];
        for array in array_cases {
            let dt = array.data_type().clone();
            let return_field = Arc::new(Field::new("f", dt.clone(), true));
            let args = vec![
                ColumnarValue::Scalar(ScalarValue::new_interval_dt(1, 0)),
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(0), None)),
            ];
            let result = invoke_date_bin_with_args(args, 2, &return_field)
                .unwrap_or_else(|e| panic!("expected Ok for {dt:?}, got {e:?}"));
            assert_array_null_then_valid(result, dt);
        }
    }

    #[test]
    fn test_date_bin_time64_micro_overflow_handling() {
        // Time64(Microsecond) can hold out-of-range values after reinterpret casts.
        use arrow::array::Time64MicrosecondArray;

        let data_type = DataType::Time64(TimeUnit::Microsecond);
        let return_field = &Arc::new(Field::new("f", data_type.clone(), true));
        let stride = || ColumnarValue::Scalar(ScalarValue::new_interval_dt(0, 1000));
        let origin = || ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(0)));

        // Out-of-range source values are per-row data, so they become NULL.
        let args = vec![
            stride(),
            ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(i64::MAX))),
            origin(),
        ];
        let result = invoke_date_bin_with_args(args, 1, return_field).unwrap();
        assert_null_scalar(result, data_type.clone());

        let array = Arc::new(Time64MicrosecondArray::from(vec![Some(i64::MAX), Some(0)]));
        let args = vec![stride(), ColumnarValue::Array(array), origin()];
        let result = invoke_date_bin_with_args(args, 2, return_field).unwrap();
        assert_array_null_then_valid(result, data_type);

        let bad_origin =
            || ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(i64::MAX)));

        // Out-of-range origins are shared inputs, so they return an error.
        let args = vec![
            stride(),
            ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(0))),
            bad_origin(),
        ];
        assert_overflow_error(invoke_date_bin_with_args(args, 1, return_field));

        let array = Arc::new(Time64MicrosecondArray::from(vec![Some(0), Some(1)]));
        let args = vec![stride(), ColumnarValue::Array(array), bad_origin()];
        assert_overflow_error(invoke_date_bin_with_args(args, 2, return_field));
    }

    #[test]
    fn test_date_bin_compute_distance_rem_overflow() {
        // Regression for #22215: `time_diff % stride` panics with "attempt to
        // calculate the remainder with overflow" when `time_diff == i64::MIN`
        // and `stride == -1`. Now it must return a normal Err that the scalar
        // pipeline maps to NULL.
        let result = date_bin_nanos_interval(-1, i64::MIN, 0);
        assert!(
            result.is_err(),
            "expected Err for time_diff=i64::MIN, stride=-1, got {result:?}"
        );
    }
}
