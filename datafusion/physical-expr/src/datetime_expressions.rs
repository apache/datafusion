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

use std::sync::Arc;

use arrow::datatypes::TimeUnit;
use arrow::util::display::{ArrayFormatter, DurationFormat, FormatOptions};
use arrow::{
    array::{Array, ArrayRef, PrimitiveArray},
    datatypes::DataType,
};
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Int32Type};
use arrow_array::StringArray;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

/// Create an implementation of `now()` that always returns the
/// specified timestamp.
///
/// The semantics of `now()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_now(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let now_ts = now_ts.timestamp_nanos_opt();
    move |_arg| {
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            now_ts,
            Some("+00:00".into()),
        )))
    }
}

/// Create an implementation of `current_date()` that always returns the
/// specified current date.
///
/// The semantics of `current_date()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_current_date(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let days = Some(
        now_ts.num_days_from_ce()
            - NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .num_days_from_ce(),
    );
    move |_arg| Ok(ColumnarValue::Scalar(ScalarValue::Date32(days)))
}

/// Create an implementation of `current_time()` that always returns the
/// specified current time.
///
/// The semantics of `current_time()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_current_time(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let nano = now_ts.timestamp_nanos_opt().map(|ts| ts % 86400000000000);
    move |_arg| Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(nano)))
}

/// Returns a string representation of a date, time, timestamp or duration based
/// on a Chrono pattern.
///
/// The syntax for the patterns can be found at
/// <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>
///
/// # Examples
///
/// ```ignore
/// # use chrono::prelude::*;
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # use datafusion_common::ScalarValue::TimestampNanosecond;
/// # use std::sync::Arc;
/// # use arrow_array::{Date32Array, RecordBatch, StringArray};
/// # use arrow_schema::{DataType, Field, Schema};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("values", DataType::Date32, false),
///     Field::new("patterns", DataType::Utf8, false),
/// ]));
///
/// let batch = RecordBatch::try_new(
///     schema,
///     vec![
///         Arc::new(Date32Array::from(vec![
///             18506,
///             18507,
///             18508,
///             18509,
///         ])),
///         Arc::new(StringArray::from(vec![
///             "%Y-%m-%d",
///             "%Y:%m:%d",
///             "%Y%m%d",
///             "%d-%m-%Y",
///         ])),
///     ],
/// )?;
///
/// let ctx = SessionContext::new();
/// ctx.register_batch("t", batch)?;
/// let df = ctx.table("t").await?;
///
/// // use the to_char function to convert col 'values',
/// // to strings using patterns in col 'patterns'
/// let df = df.with_column(
///     "date_str",
///     to_char(col("values"), col("patterns"))
/// )?;
/// // Note that providing a scalar value for the pattern
/// // is more performant
/// let df = df.with_column(
///     "date_str2",
///     to_char(col("values"), lit("%d-%m-%Y"))
/// )?;
/// // literals can be used as well with dataframe calls
/// let timestamp = "2026-07-08T09:10:11"
///     .parse::<NaiveDateTime>()
///     .unwrap()
///     .with_nanosecond(56789)
///     .unwrap()
///     .timestamp_nanos_opt()
///     .unwrap();
/// let df = df.with_column(
///     "timestamp_str",
///     to_char(lit(TimestampNanosecond(Some(timestamp), None)), lit("%d-%m-%Y %H:%M:%S"))
/// )?;
///
/// df.show().await?;
///
/// # Ok(())
/// # }
/// ```
pub fn to_char(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!("to_char function requires 2 arguments, got {}", args.len());
    }

    match &args[1] {
        // null format, use default formats
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Null) => {
            _to_char_scalar(args[0].clone(), None)
        }
        // constant format
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(format))) => {
            // invoke to_char_scalar with the known string, without converting to array
            _to_char_scalar(args[0].clone(), Some(format))
        }
        ColumnarValue::Array(_) => _to_char_array(args),
        _ => {
            exec_err!(
                "Format for `to_char` must be non-null Utf8, received {:?}",
                args[1].data_type()
            )
        }
    }
}

fn _build_format_options<'a>(
    data_type: &DataType,
    format: Option<&'a str>,
) -> Result<FormatOptions<'a>, Result<ColumnarValue>> {
    let Some(format) = format else {
        return Ok(FormatOptions::new());
    };
    let format_options = match data_type {
        DataType::Date32 => FormatOptions::new().with_date_format(Some(format)),
        DataType::Date64 => FormatOptions::new().with_datetime_format(Some(format)),
        DataType::Time32(_) => FormatOptions::new().with_time_format(Some(format)),
        DataType::Time64(_) => FormatOptions::new().with_time_format(Some(format)),
        DataType::Timestamp(_, _) => FormatOptions::new()
            .with_timestamp_format(Some(format))
            .with_timestamp_tz_format(Some(format)),
        DataType::Duration(_) => FormatOptions::new().with_duration_format(
            if "ISO8601".eq_ignore_ascii_case(format) {
                DurationFormat::ISO8601
            } else {
                DurationFormat::Pretty
            },
        ),
        other => {
            return Err(exec_err!(
                "to_char only supports date, time, timestamp and duration data types, received {other:?}"
            ));
        }
    };
    Ok(format_options)
}

/// Special version when arg\[1] is a scalar
fn _to_char_scalar(
    expression: ColumnarValue,
    format: Option<&str>,
) -> Result<ColumnarValue> {
    // it's possible that the expression is a scalar however because
    // of the implementation in arrow-rs we need to convert it to an array
    let data_type = &expression.data_type();
    let is_scalar_expression = matches!(&expression, ColumnarValue::Scalar(_));
    let array = expression.into_array(1)?;
    let format_options = match _build_format_options(data_type, format) {
        Ok(value) => value,
        Err(value) => return value,
    };

    let formatter = ArrayFormatter::try_new(array.as_ref(), &format_options)?;
    let formatted: Result<Vec<_>, arrow_schema::ArrowError> = (0..array.len())
        .map(|i| formatter.value(i).try_to_string())
        .collect();

    if let Ok(formatted) = formatted {
        if is_scalar_expression {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                formatted.first().unwrap().to_string(),
            ))))
        } else {
            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(formatted)) as ArrayRef
            ))
        }
    } else {
        exec_err!("{}", formatted.unwrap_err())
    }
}

fn _to_char_array(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let mut results: Vec<String> = vec![];
    let format_array = arrays[1].as_string::<i32>();
    let data_type = arrays[0].data_type();

    for idx in 0..arrays[0].len() {
        let format = if format_array.is_null(idx) {
            None
        } else {
            Some(format_array.value(idx))
        };
        let format_options = match _build_format_options(data_type, format) {
            Ok(value) => value,
            Err(value) => return value,
        };
        // this isn't ideal but this can't use ValueFormatter as it isn't independent
        // from ArrayFormatter
        let formatter = ArrayFormatter::try_new(arrays[0].as_ref(), &format_options)?;
        let result = formatter.value(idx).try_to_string();
        match result {
            Ok(value) => results.push(value),
            Err(e) => return exec_err!("{}", e),
        }
    }

    match args[0] {
        ColumnarValue::Array(_) => Ok(ColumnarValue::Array(Arc::new(StringArray::from(
            results,
        )) as ArrayRef)),
        ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            results.first().unwrap().to_string(),
        )))),
    }
}

/// make_date(year, month, day) SQL function implementation
pub fn make_date(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return exec_err!(
            "make_date function requires 3 arguments, got {}",
            args.len()
        );
    }

    // first, identify if any of the arguments is an Array. If yes, store its `len`,
    // as any scalar will need to be converted to an array of len `len`.
    let len = args
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        });

    let is_scalar = len.is_none();
    let array_size = if is_scalar { 1 } else { len.unwrap() };

    let years = args[0].cast_to(&DataType::Int32, None)?;
    let months = args[1].cast_to(&DataType::Int32, None)?;
    let days = args[2].cast_to(&DataType::Int32, None)?;

    // since the epoch for the date32 datatype is the unix epoch
    // we need to subtract the unix epoch from the current date
    // note this can result in a negative value
    let unix_days_from_ce = NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .num_days_from_ce();

    let mut builder: PrimitiveBuilder<Date32Type> = PrimitiveArray::builder(array_size);

    let construct_date_fn = |builder: &mut PrimitiveBuilder<Date32Type>,
                             year: i32,
                             month: i32,
                             day: i32,
                             unix_days_from_ce: i32|
     -> Result<()> {
        let Ok(m) = u32::try_from(month) else {
            return exec_err!("Month value '{month:?}' is out of range");
        };
        let Ok(d) = u32::try_from(day) else {
            return exec_err!("Day value '{day:?}' is out of range");
        };

        let date = NaiveDate::from_ymd_opt(year, m, d);

        match date {
            Some(d) => builder.append_value(d.num_days_from_ce() - unix_days_from_ce),
            None => return exec_err!("Unable to parse date from {year}, {month}, {day}"),
        };
        Ok(())
    };

    let scalar_value_fn = |col: &ColumnarValue| -> Result<i32> {
        let ColumnarValue::Scalar(s) = col else {
            return exec_err!("Expected scalar value");
        };
        let ScalarValue::Int32(Some(i)) = s else {
            return exec_err!("Unable to parse date from null/empty value");
        };
        Ok(*i)
    };

    // For scalar only columns the operation is faster without using the PrimitiveArray
    if is_scalar {
        construct_date_fn(
            &mut builder,
            scalar_value_fn(&years)?,
            scalar_value_fn(&months)?,
            scalar_value_fn(&days)?,
            unix_days_from_ce,
        )?;
    } else {
        let to_primitive_array = |col: &ColumnarValue,
                                  scalar_count: usize|
         -> Result<PrimitiveArray<Int32Type>> {
            match col {
                ColumnarValue::Array(a) => Ok(a.as_primitive::<Int32Type>().to_owned()),
                _ => {
                    let v = scalar_value_fn(col).unwrap();
                    Ok(PrimitiveArray::<Int32Type>::from_value(v, scalar_count))
                }
            }
        };

        let years = to_primitive_array(&years, array_size).unwrap();
        let months = to_primitive_array(&months, array_size).unwrap();
        let days = to_primitive_array(&days, array_size).unwrap();
        for i in 0..array_size {
            construct_date_fn(
                &mut builder,
                years.value(i),
                months.value(i),
                days.value(i),
                unix_days_from_ce,
            )?;
        }
    }

    let arr = builder.finish();

    if is_scalar {
        // If all inputs are scalar, keeps output as scalar
        Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(
            arr.value(0),
        ))))
    } else {
        Ok(ColumnarValue::Array(Arc::new(arr)))
    }
}

/// from_unixtime() SQL function implementation
pub fn from_unixtime_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return exec_err!(
            "from_unixtime function requires 1 argument, got {}",
            args.len()
        );
    }

    match args[0].data_type() {
        DataType::Int64 => {
            args[0].cast_to(&DataType::Timestamp(TimeUnit::Second, None), None)
        }
        other => {
            exec_err!(
                "Unsupported data type {:?} for function from_unixtime",
                other
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow_array::{
        Date32Array, Date64Array, Int32Array, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        UInt32Array,
    };
    use chrono::{NaiveDateTime, Timelike};

    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn test_make_date() {
        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2024))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(14))),
        ])
        .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2024))),
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(14))),
        ])
        .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("2024".to_string()))),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("1".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("14".to_string()))),
        ])
        .expect("that make_date parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Date32(date)) = res {
            assert_eq!(19736, date.unwrap());
        } else {
            panic!("Expected a scalar value")
        }

        let years = Arc::new((2021..2025).map(Some).collect::<Int64Array>());
        let months = Arc::new((1..5).map(Some).collect::<Int32Array>());
        let days = Arc::new((11..15).map(Some).collect::<UInt32Array>());
        let res = make_date(&[
            ColumnarValue::Array(years),
            ColumnarValue::Array(months),
            ColumnarValue::Array(days),
        ])
        .expect("that make_date parsed values without error");

        if let ColumnarValue::Array(array) = res {
            assert_eq!(array.len(), 4);
            let mut builder = Date32Array::builder(4);
            builder.append_value(18_638);
            builder.append_value(19_035);
            builder.append_value(19_429);
            builder.append_value(19_827);
            assert_eq!(&builder.finish() as &dyn Array, array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let res = make_date(&[ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: make_date function requires 3 arguments, got 1"
        );

        // invalid type
        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Casting from Interval(YearMonth) to Int32 not supported"
        );

        // overflow of month
        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2023))),
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(u64::MAX))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(22))),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Can't cast value 18446744073709551615 to type Int32"
        );

        // overflow of day
        let res = make_date(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2023))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(22))),
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(u32::MAX))),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Arrow error: Cast error: Can't cast value 4294967295 to type Int32"
        );
    }

    #[test]
    fn test_to_char() {
        let date = "2020-01-02T03:04:05"
            .parse::<NaiveDateTime>()
            .unwrap()
            .with_nanosecond(12345)
            .unwrap();
        let date2 = "2026-07-08T09:10:11"
            .parse::<NaiveDateTime>()
            .unwrap()
            .with_nanosecond(56789)
            .unwrap();

        let scalar_data = vec![
            (
                ScalarValue::Date32(Some(18506)),
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                "2020::09::01".to_string(),
            ),
            (
                ScalarValue::Date64(Some(date.and_utc().timestamp_millis())),
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                "2020::01::02".to_string(),
            ),
            (
                ScalarValue::Time32Second(Some(31851)),
                ScalarValue::Utf8(Some("%H-%M-%S".to_string())),
                "08-50-51".to_string(),
            ),
            (
                ScalarValue::Time32Millisecond(Some(18506000)),
                ScalarValue::Utf8(Some("%H-%M-%S".to_string())),
                "05-08-26".to_string(),
            ),
            (
                ScalarValue::Time64Microsecond(Some(12344567000)),
                ScalarValue::Utf8(Some("%H-%M-%S %f".to_string())),
                "03-25-44 567000000".to_string(),
            ),
            (
                ScalarValue::Time64Nanosecond(Some(12344567890000)),
                ScalarValue::Utf8(Some("%H-%M-%S %f".to_string())),
                "03-25-44 567890000".to_string(),
            ),
            (
                ScalarValue::TimestampSecond(Some(date.and_utc().timestamp()), None),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H".to_string())),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMillisecond(
                    Some(date.and_utc().timestamp_millis()),
                    None,
                ),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H".to_string())),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMicrosecond(
                    Some(date.and_utc().timestamp_micros()),
                    None,
                ),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H %f".to_string())),
                "2020::01::02 05::04::03 000012000".to_string(),
            ),
            (
                ScalarValue::TimestampNanosecond(
                    Some(date.and_utc().timestamp_nanos_opt().unwrap()),
                    None,
                ),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H %f".to_string())),
                "2020::01::02 05::04::03 000012345".to_string(),
            ),
        ];

        for (value, format, expected) in scalar_data {
            let result =
                to_char(&[ColumnarValue::Scalar(value), ColumnarValue::Scalar(format)])
                    .expect("that to_char parsed values without error");

            if let ColumnarValue::Scalar(ScalarValue::Utf8(date)) = result {
                assert_eq!(expected, date.unwrap());
            } else {
                panic!("Expected a scalar value")
            }
        }

        let scalar_array_data = vec![
            (
                ScalarValue::Date32(Some(18506)),
                StringArray::from(vec!["%Y::%m::%d".to_string()]),
                "2020::09::01".to_string(),
            ),
            (
                ScalarValue::Date64(Some(date.and_utc().timestamp_millis())),
                StringArray::from(vec!["%Y::%m::%d".to_string()]),
                "2020::01::02".to_string(),
            ),
            (
                ScalarValue::Time32Second(Some(31851)),
                StringArray::from(vec!["%H-%M-%S".to_string()]),
                "08-50-51".to_string(),
            ),
            (
                ScalarValue::Time32Millisecond(Some(18506000)),
                StringArray::from(vec!["%H-%M-%S".to_string()]),
                "05-08-26".to_string(),
            ),
            (
                ScalarValue::Time64Microsecond(Some(12344567000)),
                StringArray::from(vec!["%H-%M-%S %f".to_string()]),
                "03-25-44 567000000".to_string(),
            ),
            (
                ScalarValue::Time64Nanosecond(Some(12344567890000)),
                StringArray::from(vec!["%H-%M-%S %f".to_string()]),
                "03-25-44 567890000".to_string(),
            ),
            (
                ScalarValue::TimestampSecond(Some(date.and_utc().timestamp()), None),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H".to_string()]),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMillisecond(
                    Some(date.and_utc().timestamp_millis()),
                    None,
                ),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H".to_string()]),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMicrosecond(
                    Some(date.and_utc().timestamp_micros()),
                    None,
                ),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H %f".to_string()]),
                "2020::01::02 05::04::03 000012000".to_string(),
            ),
            (
                ScalarValue::TimestampNanosecond(
                    Some(date.and_utc().timestamp_nanos_opt().unwrap()),
                    None,
                ),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H %f".to_string()]),
                "2020::01::02 05::04::03 000012345".to_string(),
            ),
        ];

        for (value, format, expected) in scalar_array_data {
            let result = to_char(&[
                ColumnarValue::Scalar(value),
                ColumnarValue::Array(Arc::new(format) as ArrayRef),
            ])
            .expect("that to_char parsed values without error");

            if let ColumnarValue::Scalar(ScalarValue::Utf8(date)) = result {
                assert_eq!(expected, date.unwrap());
            } else {
                panic!("Expected a scalar value")
            }
        }

        let array_scalar_data = vec![
            (
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                StringArray::from(vec!["2020::09::01", "2020::09::02"]),
            ),
            (
                Arc::new(Date64Array::from(vec![
                    date.and_utc().timestamp_millis(),
                    date2.and_utc().timestamp_millis(),
                ])) as ArrayRef,
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                StringArray::from(vec!["2020::01::02", "2026::07::08"]),
            ),
        ];

        let array_array_data = vec![
            (
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d", "%d::%m::%Y"]),
                StringArray::from(vec!["2020::09::01", "02::09::2020"]),
            ),
            (
                Arc::new(Date64Array::from(vec![
                    date.and_utc().timestamp_millis(),
                    date2.and_utc().timestamp_millis(),
                ])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d", "%d::%m::%Y"]),
                StringArray::from(vec!["2020::01::02", "08::07::2026"]),
            ),
            (
                Arc::new(Time32MillisecondArray::from(vec![1850600, 1860700]))
                    as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["00:30:50", "00::31::00"]),
            ),
            (
                Arc::new(Time32SecondArray::from(vec![18506, 18507])) as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["05:08:26", "05::08::27"]),
            ),
            (
                Arc::new(Time64MicrosecondArray::from(vec![12344567000, 22244567000]))
                    as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["03:25:44", "06::10::44"]),
            ),
            (
                Arc::new(Time64NanosecondArray::from(vec![
                    1234456789000,
                    2224456789000,
                ])) as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["00:20:34", "00::37::04"]),
            ),
            (
                Arc::new(TimestampSecondArray::from(vec![
                    date.and_utc().timestamp(),
                    date2.and_utc().timestamp(),
                ])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H", "%d::%m::%Y %S-%M-%H"]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03",
                    "08::07::2026 11-10-09",
                ]),
            ),
            (
                Arc::new(TimestampMillisecondArray::from(vec![
                    date.and_utc().timestamp_millis(),
                    date2.and_utc().timestamp_millis(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000000000",
                    "08::07::2026 11-10-09 000000000",
                ]),
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![
                    date.and_utc().timestamp_micros(),
                    date2.and_utc().timestamp_micros(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000012000",
                    "08::07::2026 11-10-09 000056000",
                ]),
            ),
            (
                Arc::new(TimestampNanosecondArray::from(vec![
                    date.and_utc().timestamp_nanos_opt().unwrap(),
                    date2.and_utc().timestamp_nanos_opt().unwrap(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000012345",
                    "08::07::2026 11-10-09 000056789",
                ]),
            ),
        ];

        for (value, format, expected) in array_scalar_data {
            let result = to_char(&[
                ColumnarValue::Array(value as ArrayRef),
                ColumnarValue::Scalar(format),
            ])
            .expect("that to_char parsed values without error");

            if let ColumnarValue::Array(result) = result {
                assert_eq!(result.len(), 2);
                assert_eq!(&expected as &dyn Array, result.as_ref());
            } else {
                panic!("Expected an array value")
            }
        }

        for (value, format, expected) in array_array_data {
            let result = to_char(&[
                ColumnarValue::Array(value),
                ColumnarValue::Array(Arc::new(format) as ArrayRef),
            ])
            .expect("that to_char parsed values without error");

            if let ColumnarValue::Array(result) = result {
                assert_eq!(result.len(), 2);
                assert_eq!(&expected as &dyn Array, result.as_ref());
            } else {
                panic!("Expected an array value")
            }
        }

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let result = to_char(&[ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))]);
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: to_char function requires 2 arguments, got 1"
        );

        // invalid type
        let result = to_char(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: Format for `to_char` must be non-null Utf8, received Timestamp(Nanosecond, None)"
        );
    }
}
