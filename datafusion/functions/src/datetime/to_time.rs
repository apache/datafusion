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

use crate::datetime::common::*;
use arrow::array::builder::PrimitiveBuilder;
use arrow::array::cast::AsArray;
use arrow::array::temporal_conversions::time_to_time64ns;
use arrow::array::types::Time64NanosecondType;
use arrow::array::{Array, PrimitiveArray, StringArrayType};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::*;
use chrono::NaiveTime;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

/// Default time formats to try when parsing without an explicit format
const DEFAULT_TIME_FORMATS: &[&str] = &[
    "%H:%M:%S%.f", // 12:30:45.123456789
    "%H:%M:%S",    // 12:30:45
    "%H:%M",       // 12:30
];

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r"Converts a value to a time (`HH:MM:SS.nnnnnnnnn`).
Supports strings and timestamps as input.
Strings are parsed as `HH:MM:SS`, `HH:MM:SS.nnnnnnnnn`, or `HH:MM` if no [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)s are provided.
Timestamps will have the time portion extracted.
Returns the corresponding time.

Note: `to_time` returns Time64(Nanosecond), which represents the time of day in nanoseconds since midnight.",
    syntax_example = "to_time('12:30:45', '%H:%M:%S')",
    sql_example = r#"```sql
> select to_time('12:30:45');
+---------------------------+
| to_time(Utf8("12:30:45")) |
+---------------------------+
| 12:30:45                  |
+---------------------------+
> select to_time('12-30-45', '%H-%M-%S');
+--------------------------------------------+
| to_time(Utf8("12-30-45"),Utf8("%H-%M-%S")) |
+--------------------------------------------+
| 12:30:45                                   |
+--------------------------------------------+
> select to_time('2024-01-15 14:30:45'::timestamp);
+--------------------------------------------------+
| to_time(Utf8("2024-01-15 14:30:45"))             |
+--------------------------------------------------+
| 14:30:45                                         |
+--------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    standard_argument(name = "expression", prefix = "String or Timestamp"),
    argument(
        name = "format_n",
        description = r"Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimeFunc {
    signature: Signature,
}

impl Default for ToTimeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToTimeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToTimeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_time"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Time64(arrow::datatypes::TimeUnit::Nanosecond))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = args.args;
        if args.is_empty() {
            return exec_err!("to_time function requires 1 or more arguments, got 0");
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(&args, "to_time")?;
        }

        match args[0].data_type() {
            Utf8View | LargeUtf8 | Utf8 => string_to_time(&args),
            Null => Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(None))),
            // Support timestamp input by extracting time portion using Arrow cast
            Timestamp(_, _) => timestamp_to_time(&args[0]),
            other => {
                exec_err!("Unsupported data type {} for function to_time", other)
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Convert string arguments to time (standalone function, not a method on ToTimeFunc)
fn string_to_time(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let formats = collect_formats(args)?;

    match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Utf8(s))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(s))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(s)) => {
            let result = s
                .as_ref()
                .map(|s| parse_time_with_formats(s, &formats))
                .transpose()?;
            Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(result)))
        }
        ColumnarValue::Array(array) => {
            let result = match array.data_type() {
                Utf8 => parse_time_array(&array.as_string::<i32>(), &formats)?,
                LargeUtf8 => parse_time_array(&array.as_string::<i64>(), &formats)?,
                Utf8View => parse_time_array(&array.as_string_view(), &formats)?,
                other => return exec_err!("Unsupported type for to_time: {other}"),
            };
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        other => exec_err!("Unsupported argument for to_time: {other:?}"),
    }
}

/// Collect format strings from arguments, erroring on non-scalar inputs
fn collect_formats(args: &[ColumnarValue]) -> Result<Vec<&str>> {
    if args.len() <= 1 {
        return Ok(DEFAULT_TIME_FORMATS.to_vec());
    }

    let mut formats = Vec::with_capacity(args.len() - 1);
    for (i, arg) in args[1..].iter().enumerate() {
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => {
                formats.push(s.as_str());
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => {
                // Skip null format strings
            }
            ColumnarValue::Array(_) => {
                return exec_err!(
                    "to_time format argument {} must be a scalar, not an array",
                    i + 2 // argument position (1-indexed, +1 for the first arg)
                );
            }
            other => {
                return exec_err!(
                    "to_time format argument {} has unsupported type: {:?}",
                    i + 2,
                    other.data_type()
                );
            }
        }
    }
    Ok(formats)
}

/// Extract time portion from timestamp using Arrow cast kernel
fn timestamp_to_time(arg: &ColumnarValue) -> Result<ColumnarValue> {
    arg.cast_to(&Time64(arrow::datatypes::TimeUnit::Nanosecond), None)
}

/// Parse time array using the provided formats
fn parse_time_array<'a, A: StringArrayType<'a>>(
    array: &A,
    formats: &[&str],
) -> Result<PrimitiveArray<Time64NanosecondType>> {
    let mut builder: PrimitiveBuilder<Time64NanosecondType> =
        PrimitiveArray::builder(array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let s = array.value(i);
            let nanos = parse_time_with_formats(s, formats)?;
            builder.append_value(nanos);
        }
    }

    Ok(builder.finish())
}

/// Parse time string using provided formats
fn parse_time_with_formats(s: &str, formats: &[&str]) -> Result<i64> {
    for format in formats {
        if let Ok(time) = NaiveTime::parse_from_str(s, format) {
            // Use Arrow's time_to_time64ns function instead of custom implementation
            return Ok(time_to_time64ns(time));
        }
    }
    exec_err!(
        "Error parsing '{}' as time. Tried formats: {:?}",
        s,
        formats
    )
}

#[cfg(test)]
mod tests {
    use super::ToTimeFunc;
    use arrow::array::{
        Array, GenericStringArray, StringViewArray, Time64NanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    fn invoke_to_time_with_args(
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<ColumnarValue, DataFusionError> {
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type(), true).into())
            .collect::<Vec<_>>();

        let args = datafusion_expr::ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Field::new("f", DataType::Time64(TimeUnit::Nanosecond), true)
                .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        ToTimeFunc::new().invoke_with_args(args)
    }

    #[test]
    fn test_to_time_basic() {
        // Basic time parsing - more comprehensive tests are in SLT
        let sv = ScalarValue::Utf8(Some("12:30:45".to_string()));
        let result = invoke_to_time_with_args(vec![ColumnarValue::Scalar(sv)], 1);

        match result {
            Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(val)))) => {
                // 12*3600 + 30*60 + 45 = 45045 seconds = 45045000000000 nanos
                assert_eq!(val, 45_045_000_000_000);
            }
            other => panic!("Unexpected result: {other:?}"),
        }
    }

    #[test]
    fn test_to_time_array() {
        let time_array = GenericStringArray::<i32>::from(vec!["12:30:45", "23:59:59"]);
        let batch_len = time_array.len();
        let result = invoke_to_time_with_args(
            vec![ColumnarValue::Array(Arc::new(time_array))],
            batch_len,
        );

        match result {
            Ok(ColumnarValue::Array(a)) => {
                assert_eq!(a.len(), 2);

                let time_array =
                    a.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
                assert_eq!(time_array.value(0), 45_045_000_000_000); // 12:30:45
                assert_eq!(time_array.value(1), 86_399_000_000_000); // 23:59:59
            }
            other => panic!("Expected Array result, got {other:?}"),
        }
    }

    #[test]
    fn test_to_time_null_input() {
        let null_scalar = ScalarValue::Utf8(None);
        let result =
            invoke_to_time_with_args(vec![ColumnarValue::Scalar(null_scalar)], 1);

        match result {
            Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(None))) => {
                // Expected: null input results in null output
            }
            other => panic!("Expected null Time result, got {other:?}"),
        }
    }

    #[test]
    fn test_to_time_string_view() {
        let time_array = StringViewArray::from(vec!["08:15:30"]);
        let batch_len = time_array.len();
        let result = invoke_to_time_with_args(
            vec![ColumnarValue::Array(Arc::new(time_array))],
            batch_len,
        );

        match result {
            Ok(ColumnarValue::Array(a)) => {
                assert_eq!(a.len(), 1);
                let time_array =
                    a.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
                // 8*3600 + 15*60 + 30 = 29730 seconds
                assert_eq!(time_array.value(0), 29_730_000_000_000);
            }
            other => panic!("Expected Array result, got {other:?}"),
        }
    }

    #[test]
    fn test_to_time_invalid_string() {
        let invalid_scalar = ScalarValue::Utf8(Some("not_a_time".to_string()));
        let result =
            invoke_to_time_with_args(vec![ColumnarValue::Scalar(invalid_scalar)], 1);

        assert!(result.is_err());
    }

    #[test]
    fn test_to_time_format_array_error() {
        // Format argument as array should error, not be silently ignored
        let time_scalar = ScalarValue::Utf8(Some("12:30:45".to_string()));
        let format_array = GenericStringArray::<i32>::from(vec!["%H:%M:%S"]);

        let result = invoke_to_time_with_args(
            vec![
                ColumnarValue::Scalar(time_scalar),
                ColumnarValue::Array(Arc::new(format_array)),
            ],
            1,
        );

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("must be a scalar"),
            "Expected error about scalar format, got: {err_msg}"
        );
    }
}
