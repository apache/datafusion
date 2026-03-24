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

use arrow::array::builder::StringBuilder;
use arrow::array::cast::AsArray;
use arrow::array::{Array, ArrayRef};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{
    Date32, Date64, Duration, Time32, Time64, Timestamp, Utf8,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::util::display::{ArrayFormatter, DurationFormat, FormatOptions};
use datafusion_common::{Result, ScalarValue, exec_err, utils::take_function_args};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TIMEZONE_WILDCARD, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Returns a string representation of a date, time, timestamp or duration based on a [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html). Unlike the PostgreSQL equivalent of this function numerical formatting is not supported.",
    syntax_example = "to_char(expression, format)",
    sql_example = r#"```sql
> select to_char('2023-03-01'::date, '%d-%m-%Y');
+----------------------------------------------+
| to_char(Utf8("2023-03-01"),Utf8("%d-%m-%Y")) |
+----------------------------------------------+
| 01-03-2023                                   |
+----------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    argument(
        name = "expression",
        description = "Expression to operate on. Can be a constant, column, or function that results in a date, time, timestamp or duration."
    ),
    argument(
        name = "format",
        description = "A [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) string to use to convert the expression."
    ),
    argument(
        name = "day",
        description = "Day to use when making the date. Can be a constant, column or function, and any combination of arithmetic operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToCharFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ToCharFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToCharFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Date32, Utf8]),
                    Exact(vec![Date64, Utf8]),
                    Exact(vec![Time64(Nanosecond), Utf8]),
                    Exact(vec![Time64(Microsecond), Utf8]),
                    Exact(vec![Time32(Millisecond), Utf8]),
                    Exact(vec![Time32(Second), Utf8]),
                    Exact(vec![
                        Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                        Utf8,
                    ]),
                    Exact(vec![Timestamp(Nanosecond, None), Utf8]),
                    Exact(vec![
                        Timestamp(Microsecond, Some(TIMEZONE_WILDCARD.into())),
                        Utf8,
                    ]),
                    Exact(vec![Timestamp(Microsecond, None), Utf8]),
                    Exact(vec![
                        Timestamp(Millisecond, Some(TIMEZONE_WILDCARD.into())),
                        Utf8,
                    ]),
                    Exact(vec![Timestamp(Millisecond, None), Utf8]),
                    Exact(vec![
                        Timestamp(Second, Some(TIMEZONE_WILDCARD.into())),
                        Utf8,
                    ]),
                    Exact(vec![Timestamp(Second, None), Utf8]),
                    Exact(vec![Duration(Nanosecond), Utf8]),
                    Exact(vec![Duration(Microsecond), Utf8]),
                    Exact(vec![Duration(Millisecond), Utf8]),
                    Exact(vec![Duration(Second), Utf8]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("date_format")],
        }
    }
}

impl ScalarUDFImpl for ToCharFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        let [date_time, format] = take_function_args(self.name(), &args)?;

        match format {
            ColumnarValue::Scalar(ScalarValue::Null | ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(fmt))) => {
                to_char_scalar(date_time, fmt)
            }
            ColumnarValue::Array(_) => to_char_array(&args),
            _ => exec_err!(
                "Format for `to_char` must be non-null Utf8, received {}",
                format.data_type()
            ),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn build_format_options<'a>(
    data_type: &DataType,
    format: &'a str,
) -> Result<FormatOptions<'a>> {
    let format_options = match data_type {
        Date32 => FormatOptions::new()
            .with_date_format(Some(format))
            .with_datetime_format(Some(format)),
        Date64 => FormatOptions::new().with_datetime_format(Some(format)),
        Time32(_) => FormatOptions::new().with_time_format(Some(format)),
        Time64(_) => FormatOptions::new().with_time_format(Some(format)),
        Timestamp(_, _) => FormatOptions::new()
            .with_timestamp_format(Some(format))
            .with_timestamp_tz_format(Some(format)),
        Duration(_) => FormatOptions::new().with_duration_format(
            if "ISO8601".eq_ignore_ascii_case(format) {
                DurationFormat::ISO8601
            } else {
                DurationFormat::Pretty
            },
        ),
        other => {
            return exec_err!(
                "to_char only supports date, time, timestamp and duration data types, received {other:?}"
            );
        }
    };
    Ok(format_options)
}

/// Formats `expression` using a constant `format` string.
fn to_char_scalar(expression: &ColumnarValue, format: &str) -> Result<ColumnarValue> {
    // ArrayFormatter requires an array, so scalar expressions must be
    // converted to a 1-element array first.
    let data_type = &expression.data_type();
    let is_scalar_expression = matches!(&expression, ColumnarValue::Scalar(_));
    let array = expression.to_array(1)?;

    let format_options = build_format_options(data_type, format)?;
    let formatter = ArrayFormatter::try_new(array.as_ref(), &format_options)?;

    // Pad the preallocated capacity a bit because format specifiers often
    // expand the string (e.g., %Y -> "2026")
    let fmt_len = format.len() + 10;
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * fmt_len);

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            // Write directly into the builder's internal buffer, then
            // commit the value with append_value("").
            match formatter.value(i).write(&mut builder) {
                Ok(()) => builder.append_value(""),
                // Arrow's Date32 formatter only handles date specifiers
                // (%Y, %m, %d, ...). Format strings with time specifiers
                // (%H, %M, %S, ...) cause it to fail. When this happens,
                // we retry by casting to Date64, whose datetime formatter
                // handles both date and time specifiers (with zero for
                // the time components).
                Err(_) if data_type == &Date32 => {
                    return to_char_scalar(&expression.cast_to(&Date64, None)?, format);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    let result = builder.finish();
    if is_scalar_expression {
        let val = result.is_valid(0).then(|| result.value(0).to_string());
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(val)))
    } else {
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }
}

fn to_char_array(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let data_array = &arrays[0];
    let format_array = arrays[1].as_string::<i32>();
    let data_type = data_array.data_type();

    // Arbitrary guess for the length of a typical formatted datetime string
    let fmt_len = 30;
    let mut builder =
        StringBuilder::with_capacity(data_array.len(), data_array.len() * fmt_len);
    let mut buffer = String::with_capacity(fmt_len);

    for idx in 0..data_array.len() {
        if format_array.is_null(idx) || data_array.is_null(idx) {
            builder.append_null();
            continue;
        }

        let format = format_array.value(idx);
        let format_options = build_format_options(data_type, format)?;
        let formatter = ArrayFormatter::try_new(data_array.as_ref(), &format_options)?;

        buffer.clear();

        // We'd prefer to write directly to the StringBuilder's internal buffer,
        // but the write might fail, and there's no easy way to ensure a partial
        // write is removed from the buffer. So instead we write to a temporary
        // buffer and `append_value` on success.
        match formatter.value(idx).write(&mut buffer) {
            Ok(()) => builder.append_value(&buffer),
            // Retry with Date64 (see comment in to_char_scalar).
            Err(_) if data_type == &Date32 => {
                buffer.clear();
                let date64_value = cast(&data_array.slice(idx, 1), &Date64)?;
                let retry_fmt =
                    ArrayFormatter::try_new(date64_value.as_ref(), &format_options)?;
                retry_fmt.value(0).write(&mut buffer)?;
                builder.append_value(&buffer);
            }
            Err(e) => return Err(e.into()),
        }
    }

    let result = builder.finish();
    match args[0] {
        ColumnarValue::Scalar(_) => {
            let val = result.is_valid(0).then(|| result.value(0).to_string());
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(val)))
        }
        ColumnarValue::Array(_) => Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef)),
    }
}

#[cfg(test)]
mod tests {
    use crate::datetime::to_char::ToCharFunc;
    use arrow::array::{
        Array, ArrayRef, Date32Array, Date64Array, StringArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use chrono::{NaiveDateTime, Timelike};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_array_array() {
        let array_array_data = vec![(
            Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
            StringArray::from(vec!["%Y::%m::%d", "%Y::%m::%d %S::%M::%H %f"]),
            StringArray::from(vec!["2020::09::01", "2020::09::02 00::00::00 000000000"]),
        )];

        for (value, format, expected) in array_array_data {
            let batch_len = value.len();
            let value_data_type = value.data_type().clone();
            let format_data_type = format.data_type().clone();

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(value),
                    ColumnarValue::Array(Arc::new(format) as ArrayRef),
                ],
                arg_fields: vec![
                    Field::new("a", value_data_type, true).into(),
                    Field::new("b", format_data_type, true).into(),
                ],
                number_rows: batch_len,
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::clone(&Arc::new(ConfigOptions::default())),
            };
            let result = ToCharFunc::new()
                .invoke_with_args(args)
                .expect("that to_char parsed values without error");

            if let ColumnarValue::Array(result) = result {
                assert_eq!(result.len(), 2);
                assert_eq!(&expected as &dyn Array, result.as_ref());
            } else {
                panic!("Expected an array value")
            }
        }
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
                ScalarValue::Date32(Some(18506)),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H %f".to_string())),
                "2020::09::01 00::00::00 000000000".to_string(),
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
            let arg_fields = vec![
                Field::new("a", value.data_type(), false).into(),
                Field::new("a", format.data_type(), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(value), ColumnarValue::Scalar(format)],
                arg_fields,
                number_rows: 1,
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = ToCharFunc::new()
                .invoke_with_args(args)
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
                ScalarValue::Date32(Some(18506)),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H %f".to_string()]),
                "2020::09::01 00::00::00 000000000".to_string(),
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
            let batch_len = format.len();
            let arg_fields = vec![
                Field::new("a", value.data_type(), false).into(),
                Field::new("a", format.data_type().to_owned(), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(value),
                    ColumnarValue::Array(Arc::new(format) as ArrayRef),
                ],
                arg_fields,
                number_rows: batch_len,
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = ToCharFunc::new()
                .invoke_with_args(args)
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
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H %f".to_string())),
                StringArray::from(vec![
                    "2020::09::01 00::00::00 000000000",
                    "2020::09::02 00::00::00 000000000",
                ]),
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
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%Y::%m::%d %S::%M::%H %f",
                ]),
                StringArray::from(vec![
                    "2020::09::01 00::00::00 000000000",
                    "2020::09::02 00::00::00 000000000",
                ]),
            ),
            (
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d", "%Y::%m::%d %S::%M::%H %f"]),
                StringArray::from(vec![
                    "2020::09::01",
                    "2020::09::02 00::00::00 000000000",
                ]),
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
            let batch_len = value.len();
            let arg_fields = vec![
                Field::new("a", value.data_type().clone(), false).into(),
                Field::new("a", format.data_type(), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(value as ArrayRef),
                    ColumnarValue::Scalar(format),
                ],
                arg_fields,
                number_rows: batch_len,
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = ToCharFunc::new()
                .invoke_with_args(args)
                .expect("that to_char parsed values without error");

            if let ColumnarValue::Array(result) = result {
                assert_eq!(result.len(), 2);
                assert_eq!(&expected as &dyn Array, result.as_ref());
            } else {
                panic!("Expected an array value")
            }
        }

        for (value, format, expected) in array_array_data {
            let batch_len = value.len();
            let arg_fields = vec![
                Field::new("a", value.data_type().clone(), false).into(),
                Field::new("a", format.data_type().clone(), false).into(),
            ];
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(value),
                    ColumnarValue::Array(Arc::new(format) as ArrayRef),
                ],
                arg_fields,
                number_rows: batch_len,
                return_field: Field::new("f", DataType::Utf8, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let result = ToCharFunc::new()
                .invoke_with_args(args)
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
        let arg_field = Field::new("a", DataType::Int32, true).into();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = ToCharFunc::new().invoke_with_args(args);
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: to_char function requires 2 arguments, got 1"
        );

        // invalid type
        let arg_fields = vec![
            Field::new("a", DataType::Utf8, true).into(),
            Field::new("a", DataType::Timestamp(TimeUnit::Nanosecond, None), true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ],
            arg_fields,
            number_rows: 1,
            return_field: Field::new("f", DataType::Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = ToCharFunc::new().invoke_with_args(args);
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: Format for `to_char` must be non-null Utf8, received Timestamp(ns)"
        );
    }
}
