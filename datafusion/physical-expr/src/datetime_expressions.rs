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

//! DateTime expressions

use arrow::{
    array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait, PrimitiveArray},
    compute::kernels::cast_utils::string_to_timestamp_nanos,
    datatypes::{
        ArrowPrimitiveType, DataType, IntervalDayTimeType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow::{
    array::{
        Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    },
    compute::kernels::temporal,
    datatypes::TimeUnit,
    temporal_conversions::timestamp_ns_to_datetime,
};
use chrono::prelude::*;
use chrono::Duration;
use datafusion_common::{DataFusionError, Result};
use datafusion_common::{ScalarType, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::borrow::Borrow;
use std::sync::Arc;

/// given a function `op` that maps a `&str` to a Result of an arrow native type,
/// returns a `PrimitiveArray` after the application
/// of the function to `args[0]`.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not 1 or
/// * the first argument is not castable to a `GenericStringArray` or
/// * the function `op` errors
pub(crate) fn unary_string_to_primitive_function<'a, T, O, F>(
    args: &[&'a dyn Array],
    op: F,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    T: OffsetSizeTrait,
    F: Fn(&'a str) -> Result<O::Native>,
{
    if args.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name,
        )));
    }

    let array = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal("failed to downcast to string".to_string())
        })?;

    // first map is the iterator, second is for the `Option<_>`
    array
        .iter()
        .map(|x| x.map(op.borrow()).transpose())
        .collect()
}

// given an function that maps a `&str` to a arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
fn handle<'a, O, F, S>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&'a str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i32, O, _>(&[a.as_ref()], op, name)?,
            ))),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i64, O, _>(&[a.as_ref()], op, name)?,
            ))),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, name,
            ))),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, name
            ))),
        },
    }
}

/// Calls string_to_timestamp_nanos and converts the error type
fn string_to_timestamp_nanos_shim(s: &str) -> Result<i64> {
    string_to_timestamp_nanos(s).map_err(|e| e.into())
}

/// to_timestamp SQL function
pub fn to_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampNanosecondType, _, TimestampNanosecondType>(
        args,
        string_to_timestamp_nanos_shim,
        "to_timestamp",
    )
}

/// to_timestamp_millis SQL function
pub fn to_timestamp_millis(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampMillisecondType, _, TimestampMillisecondType>(
        args,
        |s| string_to_timestamp_nanos_shim(s).map(|n| n / 1_000_000),
        "to_timestamp_millis",
    )
}

/// to_timestamp_micros SQL function
pub fn to_timestamp_micros(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampMicrosecondType, _, TimestampMicrosecondType>(
        args,
        |s| string_to_timestamp_nanos_shim(s).map(|n| n / 1_000),
        "to_timestamp_micros",
    )
}

/// to_timestamp_seconds SQL function
pub fn to_timestamp_seconds(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampSecondType, _, TimestampSecondType>(
        args,
        |s| string_to_timestamp_nanos_shim(s).map(|n| n / 1_000_000_000),
        "to_timestamp_seconds",
    )
}

/// Create an implementation of `now()` that always returns the
/// specified timestamp.
///
/// The semantics of `now()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_now(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let now_ts = Some(now_ts.timestamp_nanos());
    move |_arg| {
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            now_ts,
            Some("UTC".to_owned()),
        )))
    }
}

fn quarter_month(date: &NaiveDateTime) -> u32 {
    1 + 3 * ((date.month() - 1) / 3)
}

fn date_trunc_single(granularity: &str, value: i64) -> Result<i64> {
    let value = timestamp_ns_to_datetime(value).with_nanosecond(0);
    let value = match granularity {
        "second" => value,
        "minute" => value.and_then(|d| d.with_second(0)),
        "hour" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0)),
        "day" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0)),
        "week" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .map(|d| d - Duration::seconds(60 * 60 * 24 * d.weekday() as i64)),
        "month" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0)),
        "quarter" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month(quarter_month(&d))),
        "year" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month0(0)),
        unsupported => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported date_trunc granularity: {}",
                unsupported
            )));
        }
    };
    // `with_x(0)` are infalible because `0` are always a valid
    Ok(value.unwrap().timestamp_nanos())
}

/// date_trunc SQL function
pub fn date_trunc(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (granularity, array) = (&args[0], &args[1]);

    let granularity =
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = granularity {
            v
        } else {
            return Err(DataFusionError::Execution(
                "Granularity of `date_trunc` must be non-null scalar Utf8".to_string(),
            ));
        };

    let f = |x: Option<i64>| x.map(|x| date_trunc_single(granularity, x)).transpose();

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                (f)(*v)?,
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Array(array) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let array = array
                .iter()
                .map(f)
                .collect::<Result<TimestampNanosecondArray>>()?;

            ColumnarValue::Array(Arc::new(array))
        }
        _ => {
            return Err(DataFusionError::Execution(
                "array of `date_trunc` must be non-null scalar Utf8".to_string(),
            ));
        }
    })
}

fn date_bin_single(stride: i64, source: i64, origin: i64) -> i64 {
    let time_diff = source - origin;
    // distance to bin
    let time_delta = time_diff - (time_diff % stride);

    let time_delta = if time_diff < 0 && stride > 1 {
        // The origin is later than the source timestamp, round down to the previous bin
        time_delta - stride
    } else {
        time_delta
    };

    origin + time_delta
}

/// DATE_BIN sql function
pub fn date_bin(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return Err(DataFusionError::Execution(
            "DATE_BIN expected three arguments".to_string(),
        ));
    }

    let (stride, array, origin) = (&args[0], &args[1], &args[2]);

    let stride = match stride {
        ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(v))) => {
            let (days, ms) = IntervalDayTimeType::to_parts(*v);
            let nanos = (Duration::days(days as i64) + Duration::milliseconds(ms as i64))
                .num_nanoseconds();
            match nanos {
                Some(v) => v,
                _ => {
                    return Err(DataFusionError::Execution(
                        "DATE_BIN stride argument is too large".to_string(),
                    ))
                }
            }
        }
        ColumnarValue::Scalar(v) => {
            return Err(DataFusionError::Execution(format!(
                "DATE_BIN expects stride argument to be an INTERVAL but got {}",
                v.get_datatype()
            )))
        }
        ColumnarValue::Array(_) => return Err(DataFusionError::NotImplemented(
            "DATE_BIN only supports literal values for the stride argument, not arrays"
                .to_string(),
        )),
    };

    let origin = match origin {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => *v,
        ColumnarValue::Scalar(v) => {
            return Err(DataFusionError::Execution(format!(
                "DATE_BIN expects origin argument to be a TIMESTAMP but got {}",
                v.get_datatype()
            )))
        }
        ColumnarValue::Array(_) => return Err(DataFusionError::NotImplemented(
            "DATE_BIN only supports literal values for the origin argument, not arrays"
                .to_string(),
        )),
    };

    let f = |x: Option<i64>| x.map(|x| date_bin_single(stride, x, origin));

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(f(*v), tz_opt.clone()))
        }
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .iter()
                    .map(f)
                    .collect::<TimestampNanosecondArray>();

                ColumnarValue::Array(Arc::new(array))
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "DATE_BIN expects source argument to be a TIMESTAMP but got {}",
                    array.data_type()
                )))
            }
        },
        _ => {
            return Err(DataFusionError::Execution(
                "DATE_BIN expects source argument to be a TIMESTAMP scalar or array"
                    .to_string(),
            ));
        }
    })
}

macro_rules! extract_date_part {
    ($ARRAY: expr, $FN:expr) => {
        match $ARRAY.data_type() {
            DataType::Date32 => {
                let array = $ARRAY.as_any().downcast_ref::<Date32Array>().unwrap();
                Ok($FN(array)?)
            }
            DataType::Date64 => {
                let array = $ARRAY.as_any().downcast_ref::<Date64Array>().unwrap();
                Ok($FN(array)?)
            }
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => {
                    let array = $ARRAY
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    Ok($FN(array)?)
                }
                TimeUnit::Millisecond => {
                    let array = $ARRAY
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    Ok($FN(array)?)
                }
                TimeUnit::Microsecond => {
                    let array = $ARRAY
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    Ok($FN(array)?)
                }
                TimeUnit::Nanosecond => {
                    let array = $ARRAY
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    Ok($FN(array)?)
                }
            },
            datatype => Err(DataFusionError::Internal(format!(
                "Extract does not support datatype {:?}",
                datatype
            ))),
        }
    };
}

/// DATE_PART SQL function
pub fn date_part(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(
            "Expected two arguments in DATE_PART".to_string(),
        ));
    }
    let (date_part, array) = (&args[0], &args[1]);

    let date_part = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = date_part {
        v
    } else {
        return Err(DataFusionError::Execution(
            "First argument of `DATE_PART` must be non-null scalar Utf8".to_string(),
        ));
    };

    let is_scalar = matches!(array, ColumnarValue::Scalar(_));

    let array = match array {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array(),
    };

    let arr = match date_part.to_lowercase().as_str() {
        "year" => extract_date_part!(array, temporal::year),
        "quarter" => extract_date_part!(array, temporal::quarter),
        "month" => extract_date_part!(array, temporal::month),
        "week" => extract_date_part!(array, temporal::week),
        "day" => extract_date_part!(array, temporal::day),
        "doy" => extract_date_part!(array, temporal::doy),
        "dow" => extract_date_part!(array, temporal::num_days_from_sunday),
        "hour" => extract_date_part!(array, temporal::hour),
        "minute" => extract_date_part!(array, temporal::minute),
        "second" => extract_date_part!(array, temporal::second),
        _ => Err(DataFusionError::Execution(format!(
            "Date part '{}' not supported",
            date_part
        ))),
    }?;

    Ok(if is_scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(
            &(Arc::new(arr) as ArrayRef),
            0,
        )?)
    } else {
        ColumnarValue::Array(Arc::new(arr))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, IntervalDayTimeArray, StringBuilder};

    use super::*;

    #[test]
    fn to_timestamp_arrays_and_nulls() -> Result<()> {
        // ensure that arrow array implementation is wired up and handles nulls correctly

        let mut string_builder = StringBuilder::with_capacity(2, 1024);
        let mut ts_builder = TimestampNanosecondArray::builder(2);

        string_builder.append_value("2020-09-08T13:42:29.190855Z");
        ts_builder.append_value(1599572549190855000);

        string_builder.append_null();
        ts_builder.append_null();
        let expected_timestamps = &ts_builder.finish() as &dyn Array;

        let string_array =
            ColumnarValue::Array(Arc::new(string_builder.finish()) as ArrayRef);
        let parsed_timestamps = to_timestamp(&[string_array])
            .expect("that to_timestamp parsed values without error");
        if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
            assert_eq!(parsed_array.len(), 2);
            assert_eq!(expected_timestamps, parsed_array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }
        Ok(())
    }

    #[test]
    fn date_trunc_test() {
        let cases = vec![
            (
                "2020-09-08T13:42:29.190855Z",
                "second",
                "2020-09-08T13:42:29.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "minute",
                "2020-09-08T13:42:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "hour",
                "2020-09-08T13:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "day",
                "2020-09-08T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "week",
                "2020-09-07T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "month",
                "2020-09-01T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "year",
                "2020-01-01T00:00:00.000000Z",
            ),
            // week
            (
                "2021-01-01T13:42:29.190855Z",
                "week",
                "2020-12-28T00:00:00.000000Z",
            ),
            (
                "2020-01-01T13:42:29.190855Z",
                "week",
                "2019-12-30T00:00:00.000000Z",
            ),
            // quarter
            (
                "2020-01-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-02-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-03-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-04-01T13:42:29.190855Z",
                "quarter",
                "2020-04-01T00:00:00.000000Z",
            ),
            (
                "2020-08-01T13:42:29.190855Z",
                "quarter",
                "2020-07-01T00:00:00.000000Z",
            ),
            (
                "2020-11-01T13:42:29.190855Z",
                "quarter",
                "2020-10-01T00:00:00.000000Z",
            ),
            (
                "2020-12-01T13:42:29.190855Z",
                "quarter",
                "2020-10-01T00:00:00.000000Z",
            ),
        ];

        cases.iter().for_each(|(original, granularity, expected)| {
            let left = string_to_timestamp_nanos(original).unwrap();
            let right = string_to_timestamp_nanos(expected).unwrap();
            let result = date_trunc_single(granularity, left).unwrap();
            assert_eq!(result, right, "{} = {}", original, expected);
        });
    }

    #[test]
    fn test_date_bin_single() {
        use chrono::Duration;

        let cases = vec![
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2005-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    Duration::hours(1),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    Duration::seconds(10),
                    "2004-04-09T02:03:11.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:03:10Z",
            ),
        ];

        cases
            .iter()
            .for_each(|((stride, source, origin), expected)| {
                let stride1 = stride.num_nanoseconds().unwrap();
                let source1 = string_to_timestamp_nanos(source).unwrap();
                let origin1 = string_to_timestamp_nanos(origin).unwrap();

                let expected1 = string_to_timestamp_nanos(expected).unwrap();
                let result = date_bin_single(stride1, source1, origin1);
                assert_eq!(result, expected1, "{} = {}", source, expected);
            })
    }

    #[test]
    fn test_date_bin() {
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Array(timestamps),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN expected three arguments"
        );

        // stride: invalid type
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN expects stride argument to be an INTERVAL but got Interval(MonthDayNano)"
        );

        // stride: overflow
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(i64::MAX))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // origin: invalid type
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN expects origin argument to be a TIMESTAMP but got Timestamp(Microsecond, None)"
        );

        // source: invalid scalar type
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN expects source argument to be a TIMESTAMP scalar or array"
        );

        let timestamps =
            Arc::new((1..6).map(Some).collect::<TimestampMicrosecondArray>());
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Array(timestamps),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN expects source argument to be a TIMESTAMP but got Timestamp(Microsecond, None)"
        );

        // unsupported array type for stride
        let intervals = Arc::new((1..6).map(Some).collect::<IntervalDayTimeArray>());
        let res = date_bin(&[
            ColumnarValue::Array(intervals),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "This feature is not implemented: DATE_BIN only supports literal values for the stride argument, not arrays"
        );

        // unsupported array type for origin
        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Array(timestamps),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "This feature is not implemented: DATE_BIN only supports literal values for the origin argument, not arrays"
        );
    }

    #[test]
    fn to_timestamp_invalid_input_type() -> Result<()> {
        // pass the wrong type of input array to to_timestamp and test
        // that we get an error.

        let mut builder = Int64Array::builder(1);
        builder.append_value(1);
        let int64array = ColumnarValue::Array(Arc::new(builder.finish()));

        let expected_err =
            "Internal error: Unsupported data type Int64 for function to_timestamp";
        match to_timestamp(&[int64array]) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{}'. Actual error '{}'",
                    expected_err,
                    e
                );
            }
        }
        Ok(())
    }
}
