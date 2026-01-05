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

use crate::datetime::common::*;
use arrow::array::Float64Array;
use arrow::array::timezone::Tz;
use arrow::datatypes::DataType::*;
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{
    ArrowTimestampType, DataType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{Result, ScalarType, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000000<TZ>`) in the session time zone. Supports strings,
integer, unsigned integer, and double types as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00')
if no [Chrono formats] are provided. Strings that parse without a time zone are treated as if they are in the
session time zone, or UTC if no session time zone is set.
Integers, unsigned integers, and doubles are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00Z`).

Note: `to_timestamp` returns `Timestamp(ns, TimeZone)` where the time zone is the session time zone. The supported range
for integer input is between`-9223372037` and `9223372036`. Supported range for string input is between
`1677-09-21T00:12:44.0` and `2262-04-11T23:47:16.0`. Please use `to_timestamp_seconds`
for the input outside of supported bounds.

The session time zone can be set using the statement `SET TIMEZONE = 'desired time zone'`.
The time zone can be a value like +00:00, 'Europe/London' etc.
"#,
    syntax_example = "to_timestamp(expression[, ..., format_n])",
    sql_example = r#"```sql
> select to_timestamp('2023-01-31T09:26:56.123456789-05:00');
+-----------------------------------------------------------+
| to_timestamp(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-----------------------------------------------------------+
| 2023-01-31T14:26:56.123456789                             |
+-----------------------------------------------------------+
> select to_timestamp('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+--------------------------------------------------------------------------------------------------------+
| to_timestamp(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+--------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456789                                                                          |
+--------------------------------------------------------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    argument(
        name = "expression",
        description = "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "format_n",
        description = r#"
Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression.
Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully
parse the expression an error will be returned. Note: parsing of named timezones (e.g. 'America/New_York') using %Z is
only supported at the end of the string preceded by a space.
"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimestampFunc {
    signature: Signature,
    timezone: Option<Arc<str>>,
}

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Converts a value to a timestamp (`YYYY-MM-DDT00:00:00<TZ>`) in the session time zone. Supports strings,
integer, unsigned integer, and double types as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00')
if no [Chrono formats] are provided. Strings that parse without a time zone are treated as if they are in the
session time zone, or UTC if no session time zone is set.
Integers, unsigned integers, and doubles are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00Z`).

The session time zone can be set using the statement `SET TIMEZONE = 'desired time zone'`.
The time zone can be a value like +00:00, 'Europe/London' etc.
"#,
    syntax_example = "to_timestamp_seconds(expression[, ..., format_n])",
    sql_example = r#"```sql
> select to_timestamp_seconds('2023-01-31T09:26:56.123456789-05:00');
+-------------------------------------------------------------------+
| to_timestamp_seconds(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-------------------------------------------------------------------+
| 2023-01-31T14:26:56                                               |
+-------------------------------------------------------------------+
> select to_timestamp_seconds('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+----------------------------------------------------------------------------------------------------------------+
| to_timestamp_seconds(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+----------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00                                                                                            |
+----------------------------------------------------------------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    argument(
        name = "expression",
        description = "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "format_n",
        description = r#"
Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression.
Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully
parse the expression an error will be returned. Note: parsing of named timezones (e.g. 'America/New_York') using %Z is
only supported at the end of the string preceded by a space.
"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimestampSecondsFunc {
    signature: Signature,
    timezone: Option<Arc<str>>,
}

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000<TZ>`) in the session time zone. Supports strings,
integer, unsigned integer, and double types as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00')
if no [Chrono formats] are provided. Strings that parse without a time zone are treated as if they are in the
session time zone, or UTC if no session time zone is set.
Integers, unsigned integers, and doubles are interpreted as milliseconds since the unix epoch (`1970-01-01T00:00:00Z`).

The session time zone can be set using the statement `SET TIMEZONE = 'desired time zone'`.
The time zone can be a value like +00:00, 'Europe/London' etc.
"#,
    syntax_example = "to_timestamp_millis(expression[, ..., format_n])",
    sql_example = r#"```sql
> select to_timestamp_millis('2023-01-31T09:26:56.123456789-05:00');
+------------------------------------------------------------------+
| to_timestamp_millis(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+------------------------------------------------------------------+
| 2023-01-31T14:26:56.123                                          |
+------------------------------------------------------------------+
> select to_timestamp_millis('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+---------------------------------------------------------------------------------------------------------------+
| to_timestamp_millis(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+---------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123                                                                                       |
+---------------------------------------------------------------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    argument(
        name = "expression",
        description = "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "format_n",
        description = r#"
Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression.
Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully
parse the expression an error will be returned. Note: parsing of named timezones (e.g. 'America/New_York') using %Z is
only supported at the end of the string preceded by a space.
"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimestampMillisFunc {
    signature: Signature,
    timezone: Option<Arc<str>>,
}

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000000<TZ>`) in the session time zone. Supports strings,
integer, unsigned integer, and double types as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00')
if no [Chrono formats] are provided. Strings that parse without a time zone are treated as if they are in the
session time zone, or UTC if no session time zone is set.
Integers, unsigned integers, and doubles are interpreted as microseconds since the unix epoch (`1970-01-01T00:00:00Z`).

The session time zone can be set using the statement `SET TIMEZONE = 'desired time zone'`.
The time zone can be a value like +00:00, 'Europe/London' etc.
"#,
    syntax_example = "to_timestamp_micros(expression[, ..., format_n])",
    sql_example = r#"```sql
> select to_timestamp_micros('2023-01-31T09:26:56.123456789-05:00');
+------------------------------------------------------------------+
| to_timestamp_micros(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+------------------------------------------------------------------+
| 2023-01-31T14:26:56.123456                                       |
+------------------------------------------------------------------+
> select to_timestamp_micros('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+---------------------------------------------------------------------------------------------------------------+
| to_timestamp_micros(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+---------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456                                                                                    |
+---------------------------------------------------------------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    argument(
        name = "expression",
        description = "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "format_n",
        description = r#"
Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression.
Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully
parse the expression an error will be returned. Note: parsing of named timezones (e.g. 'America/New_York') using %Z is
only supported at the end of the string preceded by a space.
"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimestampMicrosFunc {
    signature: Signature,
    timezone: Option<Arc<str>>,
}

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000000000<TZ>`) in the session time zone. Supports strings,
integer, unsigned integer, and double types as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00')
if no [Chrono formats] are provided. Strings that parse without a time zone are treated as if they are in the
session time zone. Integers, unsigned integers, and doubles are interpreted as nanoseconds since the unix epoch (`1970-01-01T00:00:00Z`).

The session time zone can be set using the statement `SET TIMEZONE = 'desired time zone'`.
The time zone can be a value like +00:00, 'Europe/London' etc.
"#,
    syntax_example = "to_timestamp_nanos(expression[, ..., format_n])",
    sql_example = r#"```sql
> select to_timestamp_nanos('2023-01-31T09:26:56.123456789-05:00');
+-----------------------------------------------------------------+
| to_timestamp_nanos(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-----------------------------------------------------------------+
| 2023-01-31T14:26:56.123456789                                   |
+-----------------------------------------------------------------+
> select to_timestamp_nanos('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+--------------------------------------------------------------------------------------------------------------+
| to_timestamp_nanos(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+--------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456789                                                                                |
+---------------------------------------------------------------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    argument(
        name = "expression",
        description = "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "format_n",
        description = r#"
Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression.
Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully
parse the expression an error will be returned. Note: parsing of named timezones (e.g. 'America/New_York') using %Z is
only supported at the end of the string preceded by a space.
"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToTimestampNanosFunc {
    signature: Signature,
    timezone: Option<Arc<str>>,
}

/// Macro to generate boilerplate constructors and config methods for ToTimestamp* functions.
/// Generates: Default impl, deprecated new(), new_with_config(), and extracts timezone from ConfigOptions.
macro_rules! impl_to_timestamp_constructors {
    ($func:ty) => {
        impl Default for $func {
            fn default() -> Self {
                Self::new_with_config(&ConfigOptions::default())
            }
        }

        impl $func {
            #[deprecated(since = "52.0.0", note = "use `new_with_config` instead")]
            /// Deprecated constructor retained for backwards compatibility.
            ///
            /// Prefer `new_with_config` which allows specifying the
            /// timezone via [`ConfigOptions`]. This helper now mirrors the
            /// canonical default offset (None) provided by `ConfigOptions::default()`.
            pub fn new() -> Self {
                Self::new_with_config(&ConfigOptions::default())
            }

            pub fn new_with_config(config: &ConfigOptions) -> Self {
                Self {
                    signature: Signature::variadic_any(Volatility::Immutable),
                    timezone: config
                        .execution
                        .time_zone
                        .as_ref()
                        .map(|tz| Arc::from(tz.as_str())),
                }
            }
        }
    };
}

impl_to_timestamp_constructors!(ToTimestampFunc);
impl_to_timestamp_constructors!(ToTimestampSecondsFunc);
impl_to_timestamp_constructors!(ToTimestampMillisFunc);
impl_to_timestamp_constructors!(ToTimestampMicrosFunc);
impl_to_timestamp_constructors!(ToTimestampNanosFunc);

/// to_timestamp SQL function
///
/// Note: `to_timestamp` returns `Timestamp(Nanosecond)` though its arguments are interpreted as **seconds**.
/// The supported range for integer input is between `-9223372037` and `9223372036`.
/// Supported range for string input is between `1677-09-21T00:12:44.0` and `2262-04-11T23:47:16.0`.
/// Please use `to_timestamp_seconds` for the input outside of supported bounds.
/// Macro to generate the with_updated_config method for ToTimestamp* functions.
macro_rules! impl_with_updated_config {
    () => {
        fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
            Some(Self::new_with_config(config).into())
        }
    };
}

impl ScalarUDFImpl for ToTimestampFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Timestamp(Nanosecond, self.timezone.clone()))
    }

    impl_with_updated_config!();

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let datafusion_expr::ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return exec_err!(
                "to_timestamp function requires 1 or more arguments, got {}",
                args.len()
            );
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(&args, "to_timestamp")?;
        }

        let tz = self.timezone.clone();

        match args[0].data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => args[0]
                .cast_to(&Timestamp(Second, None), None)?
                .cast_to(&Timestamp(Nanosecond, tz), None),
            Null | Timestamp(_, _) => args[0].cast_to(&Timestamp(Nanosecond, tz), None),
            Float32 | Float64 => {
                let arg = args[0].cast_to(&Float64, None)?;
                let rescaled = arrow::compute::kernels::numeric::mul(
                    &arg.to_array(1)?,
                    &arrow::array::Scalar::new(Float64Array::from(vec![
                        1_000_000_000f64,
                    ])),
                )?;
                Ok(ColumnarValue::Array(arrow::compute::cast_with_options(
                    &rescaled,
                    &Timestamp(Nanosecond, tz),
                    &DEFAULT_CAST_OPTIONS,
                )?))
            }
            Utf8View | LargeUtf8 | Utf8 => {
                to_timestamp_impl::<TimestampNanosecondType>(&args, "to_timestamp", &tz)
            }
            Decimal128(_, _) => {
                match &args[0] {
                    ColumnarValue::Scalar(ScalarValue::Decimal128(
                        Some(value),
                        _,
                        scale,
                    )) => {
                        // Convert decimal to seconds and nanoseconds
                        let scale_factor = 10_i128.pow(*scale as u32);
                        let seconds = value / scale_factor;
                        let fraction = value % scale_factor;
                        let nanos = (fraction * 1_000_000_000) / scale_factor;
                        let timestamp_nanos = seconds * 1_000_000_000 + nanos;

                        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                            Some(timestamp_nanos as i64),
                            tz,
                        )))
                    }
                    _ => exec_err!("Invalid decimal value"),
                }
            }
            other => {
                exec_err!("Unsupported data type {other} for function to_timestamp")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

impl ScalarUDFImpl for ToTimestampSecondsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_timestamp_seconds"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Timestamp(Second, self.timezone.clone()))
    }

    impl_with_updated_config!();

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let datafusion_expr::ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return exec_err!(
                "to_timestamp_seconds function requires 1 or more arguments, got {}",
                args.len()
            );
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(&args, "to_timestamp")?;
        }

        let tz = self.timezone.clone();

        match args[0].data_type() {
            Null
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Timestamp(_, _)
            | Decimal128(_, _) => args[0].cast_to(&Timestamp(Second, tz), None),
            Float32 | Float64 => args[0]
                .cast_to(&Int64, None)?
                .cast_to(&Timestamp(Second, tz), None),
            Utf8View | LargeUtf8 | Utf8 => to_timestamp_impl::<TimestampSecondType>(
                &args,
                "to_timestamp_seconds",
                &self.timezone,
            ),
            other => {
                exec_err!(
                    "Unsupported data type {} for function to_timestamp_seconds",
                    other
                )
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

impl ScalarUDFImpl for ToTimestampMillisFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_timestamp_millis"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Timestamp(Millisecond, self.timezone.clone()))
    }

    impl_with_updated_config!();

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let datafusion_expr::ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return exec_err!(
                "to_timestamp_millis function requires 1 or more arguments, got {}",
                args.len()
            );
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(&args, "to_timestamp")?;
        }

        match args[0].data_type() {
            Null
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Timestamp(_, _)
            | Decimal128(_, _) => {
                args[0].cast_to(&Timestamp(Millisecond, self.timezone.clone()), None)
            }
            Float32 | Float64 => args[0]
                .cast_to(&Int64, None)?
                .cast_to(&Timestamp(Millisecond, self.timezone.clone()), None),
            Utf8View | LargeUtf8 | Utf8 => to_timestamp_impl::<TimestampMillisecondType>(
                &args,
                "to_timestamp_millis",
                &self.timezone,
            ),
            other => {
                exec_err!(
                    "Unsupported data type {} for function to_timestamp_millis",
                    other
                )
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

impl ScalarUDFImpl for ToTimestampMicrosFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_timestamp_micros"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Timestamp(Microsecond, self.timezone.clone()))
    }

    impl_with_updated_config!();

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let datafusion_expr::ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return exec_err!(
                "to_timestamp_micros function requires 1 or more arguments, got {}",
                args.len()
            );
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(&args, "to_timestamp")?;
        }

        match args[0].data_type() {
            Null
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Timestamp(_, _)
            | Decimal128(_, _) => {
                args[0].cast_to(&Timestamp(Microsecond, self.timezone.clone()), None)
            }
            Float32 | Float64 => args[0]
                .cast_to(&Int64, None)?
                .cast_to(&Timestamp(Microsecond, self.timezone.clone()), None),
            Utf8View | LargeUtf8 | Utf8 => to_timestamp_impl::<TimestampMicrosecondType>(
                &args,
                "to_timestamp_micros",
                &self.timezone,
            ),
            other => {
                exec_err!(
                    "Unsupported data type {} for function to_timestamp_micros",
                    other
                )
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

impl ScalarUDFImpl for ToTimestampNanosFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_timestamp_nanos"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Timestamp(Nanosecond, self.timezone.clone()))
    }

    impl_with_updated_config!();

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let datafusion_expr::ScalarFunctionArgs { args, .. } = args;

        if args.is_empty() {
            return exec_err!(
                "to_timestamp_nanos function requires 1 or more arguments, got {}",
                args.len()
            );
        }

        // validate that any args after the first one are Utf8
        if args.len() > 1 {
            validate_data_types(&args, "to_timestamp")?;
        }

        match args[0].data_type() {
            Null
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Timestamp(_, _)
            | Decimal128(_, _) => {
                args[0].cast_to(&Timestamp(Nanosecond, self.timezone.clone()), None)
            }
            Float32 | Float64 => args[0]
                .cast_to(&Int64, None)?
                .cast_to(&Timestamp(Nanosecond, self.timezone.clone()), None),
            Utf8View | LargeUtf8 | Utf8 => to_timestamp_impl::<TimestampNanosecondType>(
                &args,
                "to_timestamp_nanos",
                &self.timezone,
            ),
            other => {
                exec_err!(
                    "Unsupported data type {} for function to_timestamp_nanos",
                    other
                )
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn to_timestamp_impl<T: ArrowTimestampType + ScalarType<i64>>(
    args: &[ColumnarValue],
    name: &str,
    timezone: &Option<Arc<str>>,
) -> Result<ColumnarValue> {
    let factor = match T::UNIT {
        Second => 1_000_000_000,
        Millisecond => 1_000_000,
        Microsecond => 1_000,
        Nanosecond => 1,
    };

    let tz = match timezone.clone() {
        Some(tz) => Some(tz.parse::<Tz>()?),
        None => None,
    };

    match args.len() {
        1 => handle::<T, _>(
            args,
            move |s| string_to_timestamp_nanos_with_timezone(&tz, s).map(|n| n / factor),
            name,
            &Timestamp(T::UNIT, timezone.clone()),
        ),
        n if n >= 2 => handle_multiple::<T, _, _>(
            args,
            move |s, format| {
                string_to_timestamp_nanos_formatted_with_timezone(&tz, s, format)
            },
            |n| n / factor,
            name,
            &Timestamp(T::UNIT, timezone.clone()),
        ),
        _ => exec_err!("Unsupported 0 argument count for function {name}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::types::Int64Type;
    use arrow::array::{
        Array, PrimitiveArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::array::{ArrayRef, Int64Array, StringBuilder};
    use arrow::datatypes::{Field, TimeUnit};
    use chrono::{DateTime, FixedOffset, Utc};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{DataFusionError, ScalarValue, assert_contains};
    use datafusion_expr::{ScalarFunctionArgs, ScalarFunctionImplementation};

    use super::*;

    fn to_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let timezone: Option<Arc<str>> = Some("UTC".into());
        to_timestamp_impl::<TimestampNanosecondType>(args, "to_timestamp", &timezone)
    }

    /// to_timestamp_millis SQL function
    fn to_timestamp_millis(args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let timezone: Option<Arc<str>> = Some("UTC".into());
        to_timestamp_impl::<TimestampMillisecondType>(
            args,
            "to_timestamp_millis",
            &timezone,
        )
    }

    /// to_timestamp_micros SQL function
    fn to_timestamp_micros(args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let timezone: Option<Arc<str>> = Some("UTC".into());
        to_timestamp_impl::<TimestampMicrosecondType>(
            args,
            "to_timestamp_micros",
            &timezone,
        )
    }

    /// to_timestamp_nanos SQL function
    fn to_timestamp_nanos(args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let timezone: Option<Arc<str>> = Some("UTC".into());
        to_timestamp_impl::<TimestampNanosecondType>(
            args,
            "to_timestamp_nanos",
            &timezone,
        )
    }

    /// to_timestamp_seconds SQL function
    fn to_timestamp_seconds(args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let timezone: Option<Arc<str>> = Some("UTC".into());
        to_timestamp_impl::<TimestampSecondType>(args, "to_timestamp_seconds", &timezone)
    }

    fn udfs_and_timeunit() -> Vec<(Box<dyn ScalarUDFImpl>, TimeUnit)> {
        let udfs: Vec<(Box<dyn ScalarUDFImpl>, TimeUnit)> = vec![
            (
                Box::new(ToTimestampFunc::new_with_config(&ConfigOptions::default())),
                Nanosecond,
            ),
            (
                Box::new(ToTimestampSecondsFunc::new_with_config(
                    &ConfigOptions::default(),
                )),
                Second,
            ),
            (
                Box::new(ToTimestampMillisFunc::new_with_config(
                    &ConfigOptions::default(),
                )),
                Millisecond,
            ),
            (
                Box::new(ToTimestampMicrosFunc::new_with_config(
                    &ConfigOptions::default(),
                )),
                Microsecond,
            ),
            (
                Box::new(ToTimestampNanosFunc::new_with_config(
                    &ConfigOptions::default(),
                )),
                Nanosecond,
            ),
        ];
        udfs
    }

    fn validate_expected_error(
        options: &mut ConfigOptions,
        args: ScalarFunctionArgs,
        expected_err: &str,
    ) {
        let udfs = udfs_and_timeunit();

        for (udf, _) in udfs {
            match udf
                .with_updated_config(options)
                .unwrap()
                .invoke_with_args(args.clone())
            {
                Ok(_) => panic!("Expected error but got success"),
                Err(e) => {
                    assert!(
                        e.to_string().contains(expected_err),
                        "Can not find expected error '{expected_err}'. Actual error '{e}'"
                    );
                }
            }
        }
    }

    #[test]
    fn to_timestamp_arrays_and_nulls() -> Result<()> {
        // ensure that arrow array implementation is wired up and handles nulls correctly

        let mut string_builder = StringBuilder::with_capacity(2, 1024);
        let mut ts_builder = TimestampNanosecondArray::builder(2);

        string_builder.append_value("2020-09-08T13:42:29.190855");
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
    fn to_timestamp_with_formats_arrays_and_nulls() -> Result<()> {
        // ensure that arrow array implementation is wired up and handles nulls correctly

        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);
        let mut format1_builder = StringBuilder::with_capacity(2, 1024);
        let mut format2_builder = StringBuilder::with_capacity(2, 1024);
        let mut format3_builder = StringBuilder::with_capacity(2, 1024);
        let mut ts_builder = TimestampNanosecondArray::builder(2);

        date_string_builder.append_null();
        format1_builder.append_null();
        format2_builder.append_null();
        format3_builder.append_null();
        ts_builder.append_null();

        date_string_builder.append_value("2020-09-08T13:42:29.19085Z");
        format1_builder.append_value("%s");
        format2_builder.append_value("%c");
        format3_builder.append_value("%+");
        ts_builder.append_value(1599572549190850000);

        let expected_timestamps = &ts_builder.finish() as &dyn Array;

        let string_array = [
            ColumnarValue::Array(Arc::new(date_string_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format1_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format2_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format3_builder.finish()) as ArrayRef),
        ];
        let parsed_timestamps = to_timestamp(&string_array)
            .expect("that to_timestamp with format args parsed values without error");
        if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
            assert_eq!(parsed_array.len(), 2);
            assert_eq!(expected_timestamps, parsed_array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_respects_execution_timezone() -> Result<()> {
        let udfs = udfs_and_timeunit();

        let mut options = ConfigOptions::default();
        options.execution.time_zone = Some("-05:00".to_string());

        let time_zone: Option<Arc<str>> = options
            .execution
            .time_zone
            .as_ref()
            .map(|tz| Arc::from(tz.as_str()));

        for (udf, time_unit) in udfs {
            let field = Field::new("arg", Utf8, true).into();

            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "2020-09-08T13:42:29".to_string(),
                )))],
                arg_fields: vec![field],
                number_rows: 1,
                return_field: Field::new(
                    "f",
                    Timestamp(time_unit, Some("-05:00".into())),
                    true,
                )
                .into(),
                config_options: Arc::new(options.clone()),
            };

            let result = udf
                .with_updated_config(&options.clone())
                .unwrap()
                .invoke_with_args(args)?;
            let result = match time_unit {
                Second => {
                    let ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                        Some(value),
                        tz,
                    )) = result
                    else {
                        panic!("expected scalar timestamp");
                    };

                    assert_eq!(tz, time_zone);

                    value
                }
                Millisecond => {
                    let ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                        Some(value),
                        tz,
                    )) = result
                    else {
                        panic!("expected scalar timestamp");
                    };

                    assert_eq!(tz, time_zone);

                    value
                }
                Microsecond => {
                    let ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        Some(value),
                        tz,
                    )) = result
                    else {
                        panic!("expected scalar timestamp");
                    };

                    assert_eq!(tz, time_zone);

                    value
                }
                Nanosecond => {
                    let ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(value),
                        tz,
                    )) = result
                    else {
                        panic!("expected scalar timestamp");
                    };

                    assert_eq!(tz, time_zone);

                    value
                }
            };

            let scale = match time_unit {
                Second => 1_000_000_000,
                Millisecond => 1_000_000,
                Microsecond => 1_000,
                Nanosecond => 1,
            };

            let offset = FixedOffset::west_opt(5 * 3600).unwrap();
            let result = Some(
                DateTime::<Utc>::from_timestamp_nanos(result * scale)
                    .with_timezone(&offset)
                    .to_string(),
            );

            assert_eq!(result, Some("2020-09-08 13:42:29 -05:00".to_string()));
        }

        Ok(())
    }

    #[test]
    fn to_timestamp_formats_respects_execution_timezone() -> Result<()> {
        let udfs = udfs_and_timeunit();

        let mut options = ConfigOptions::default();
        options.execution.time_zone = Some("-05:00".to_string());

        let time_zone: Option<Arc<str>> = options
            .execution
            .time_zone
            .as_ref()
            .map(|tz| Arc::from(tz.as_str()));

        let expr_field = Field::new("arg", Utf8, true).into();
        let format_field: Arc<Field> = Field::new("fmt", Utf8, true).into();

        for (udf, time_unit) in udfs {
            for (value, format, expected_str) in [
                (
                    "2020-09-08 09:42:29 -05:00",
                    "%Y-%m-%d %H:%M:%S %z",
                    Some("2020-09-08 09:42:29 -05:00"),
                ),
                (
                    "2020-09-08T13:42:29Z",
                    "%+",
                    Some("2020-09-08 08:42:29 -05:00"),
                ),
                (
                    "2020-09-08 13:42:29 UTC",
                    "%Y-%m-%d %H:%M:%S %Z",
                    Some("2020-09-08 08:42:29 -05:00"),
                ),
                (
                    "+0000 2024-01-01 12:00:00",
                    "%z %Y-%m-%d %H:%M:%S",
                    Some("2024-01-01 07:00:00 -05:00"),
                ),
                (
                    "20200908134229+0100",
                    "%Y%m%d%H%M%S%z",
                    Some("2020-09-08 07:42:29 -05:00"),
                ),
                (
                    "2020-09-08+0230 13:42",
                    "%Y-%m-%d%z %H:%M",
                    Some("2020-09-08 06:12:00 -05:00"),
                ),
            ] {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string()))),
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                            format.to_string(),
                        ))),
                    ],
                    arg_fields: vec![Arc::clone(&expr_field), Arc::clone(&format_field)],
                    number_rows: 1,
                    return_field: Field::new(
                        "f",
                        Timestamp(time_unit, Some("-05:00".into())),
                        true,
                    )
                    .into(),
                    config_options: Arc::new(options.clone()),
                };
                let result = udf
                    .with_updated_config(&options.clone())
                    .unwrap()
                    .invoke_with_args(args)?;
                let result = match time_unit {
                    Second => {
                        let ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                            Some(value),
                            tz,
                        )) = result
                        else {
                            panic!("expected scalar timestamp");
                        };

                        assert_eq!(tz, time_zone);

                        value
                    }
                    Millisecond => {
                        let ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                            Some(value),
                            tz,
                        )) = result
                        else {
                            panic!("expected scalar timestamp");
                        };

                        assert_eq!(tz, time_zone);

                        value
                    }
                    Microsecond => {
                        let ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                            Some(value),
                            tz,
                        )) = result
                        else {
                            panic!("expected scalar timestamp");
                        };

                        assert_eq!(tz, time_zone);

                        value
                    }
                    Nanosecond => {
                        let ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                            Some(value),
                            tz,
                        )) = result
                        else {
                            panic!("expected scalar timestamp");
                        };

                        assert_eq!(tz, time_zone);

                        value
                    }
                };

                let scale = match time_unit {
                    Second => 1_000_000_000,
                    Millisecond => 1_000_000,
                    Microsecond => 1_000,
                    Nanosecond => 1,
                };
                let offset = FixedOffset::west_opt(5 * 3600).unwrap();
                let result = Some(
                    DateTime::<Utc>::from_timestamp_nanos(result * scale)
                        .with_timezone(&offset)
                        .to_string(),
                );

                assert_eq!(result, expected_str.map(|s| s.to_string()));
            }
        }

        Ok(())
    }

    #[test]
    fn to_timestamp_invalid_execution_timezone_behavior() -> Result<()> {
        let field: Arc<Field> = Field::new("arg", Utf8, true).into();
        let return_field: Arc<Field> =
            Field::new("f", Timestamp(Nanosecond, None), true).into();

        let mut options = ConfigOptions::default();
        options.execution.time_zone = Some("Invalid/Timezone".to_string());

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "2020-09-08T13:42:29Z".to_string(),
            )))],
            arg_fields: vec![Arc::clone(&field)],
            number_rows: 1,
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(options.clone()),
        };

        let expected_err =
            "Invalid timezone \"Invalid/Timezone\": failed to parse timezone";

        validate_expected_error(&mut options, args, expected_err);

        Ok(())
    }

    #[test]
    fn to_timestamp_formats_invalid_execution_timezone_behavior() -> Result<()> {
        let expr_field: Arc<Field> = Field::new("arg", Utf8, true).into();
        let format_field: Arc<Field> = Field::new("fmt", Utf8, true).into();
        let return_field: Arc<Field> =
            Field::new("f", Timestamp(Nanosecond, None), true).into();

        let mut options = ConfigOptions::default();
        options.execution.time_zone = Some("Invalid/Timezone".to_string());

        let expected_err =
            "Invalid timezone \"Invalid/Timezone\": failed to parse timezone";

        let make_args = |value: &str, format: &str| ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(format.to_string()))),
            ],
            arg_fields: vec![Arc::clone(&expr_field), Arc::clone(&format_field)],
            number_rows: 1,
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(options.clone()),
        };

        for (value, format, _expected_str) in [
            (
                "2020-09-08 09:42:29 -05:00",
                "%Y-%m-%d %H:%M:%S %z",
                Some("2020-09-08 09:42:29 -05:00"),
            ),
            (
                "2020-09-08T13:42:29Z",
                "%+",
                Some("2020-09-08 08:42:29 -05:00"),
            ),
            (
                "2020-09-08 13:42:29 +0000",
                "%Y-%m-%d %H:%M:%S %z",
                Some("2020-09-08 08:42:29 -05:00"),
            ),
            (
                "+0000 2024-01-01 12:00:00",
                "%z %Y-%m-%d %H:%M:%S",
                Some("2024-01-01 07:00:00 -05:00"),
            ),
            (
                "20200908134229+0100",
                "%Y%m%d%H%M%S%z",
                Some("2020-09-08 07:42:29 -05:00"),
            ),
            (
                "2020-09-08+0230 13:42",
                "%Y-%m-%d%z %H:%M",
                Some("2020-09-08 06:12:00 -05:00"),
            ),
        ] {
            let args = make_args(value, format);
            validate_expected_error(&mut options.clone(), args, expected_err);
        }

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "2020-09-08T13:42:29".to_string(),
                ))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "%Y-%m-%dT%H:%M:%S".to_string(),
                ))),
            ],
            arg_fields: vec![Arc::clone(&expr_field), Arc::clone(&format_field)],
            number_rows: 1,
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(options.clone()),
        };

        validate_expected_error(&mut options.clone(), args, expected_err);

        Ok(())
    }

    #[test]
    fn to_timestamp_invalid_input_type() -> Result<()> {
        // pass the wrong type of input array to to_timestamp and test
        // that we get an error.

        let mut builder = Int64Array::builder(1);
        builder.append_value(1);
        let int64array = ColumnarValue::Array(Arc::new(builder.finish()));

        let expected_err =
            "Execution error: Unsupported data type Int64 for function to_timestamp";
        match to_timestamp(&[int64array]) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_with_formats_invalid_input_type() -> Result<()> {
        // pass the wrong type of input array to to_timestamp and test
        // that we get an error.

        let mut builder = Int64Array::builder(1);
        builder.append_value(1);
        let int64array = [
            ColumnarValue::Array(Arc::new(builder.finish())),
            ColumnarValue::Array(Arc::new(builder.finish())),
        ];

        let expected_err =
            "Execution error: Unsupported data type Int64 for function to_timestamp";
        match to_timestamp(&int64array) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_with_unparsable_data() -> Result<()> {
        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);

        date_string_builder.append_null();

        date_string_builder.append_value("2020-09-08 - 13:42:29.19085Z");

        let string_array =
            ColumnarValue::Array(Arc::new(date_string_builder.finish()) as ArrayRef);

        let expected_err = "Arrow error: Parser error: Error parsing timestamp from '2020-09-08 - 13:42:29.19085Z': error parsing time";
        match to_timestamp(&[string_array]) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_with_invalid_tz() -> Result<()> {
        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);

        date_string_builder.append_null();

        date_string_builder.append_value("2020-09-08T13:42:29ZZ");

        let string_array =
            ColumnarValue::Array(Arc::new(date_string_builder.finish()) as ArrayRef);

        let expected_err = "Arrow error: Parser error: Invalid timezone \"ZZ\": failed to parse timezone";
        match to_timestamp(&[string_array]) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn to_timestamp_with_no_matching_formats() -> Result<()> {
        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);
        let mut format1_builder = StringBuilder::with_capacity(2, 1024);
        let mut format2_builder = StringBuilder::with_capacity(2, 1024);
        let mut format3_builder = StringBuilder::with_capacity(2, 1024);

        date_string_builder.append_null();
        format1_builder.append_null();
        format2_builder.append_null();
        format3_builder.append_null();

        date_string_builder.append_value("2020-09-08T13:42:29.19085Z");
        format1_builder.append_value("%s");
        format2_builder.append_value("%c");
        format3_builder.append_value("%H:%M:%S");

        let string_array = [
            ColumnarValue::Array(Arc::new(date_string_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format1_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format2_builder.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(format3_builder.finish()) as ArrayRef),
        ];

        let expected_err = "Execution error: Error parsing timestamp from '2020-09-08T13:42:29.19085Z' using format '%H:%M:%S': input contains invalid characters";
        match to_timestamp(&string_array) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn string_to_timestamp_formatted() {
        // Explicit timezone
        assert_eq!(
            1599572549190855000,
            parse_timestamp_formatted("2020-09-08T13:42:29.190855+00:00", "%+").unwrap()
        );
        assert_eq!(
            1599572549190855000,
            parse_timestamp_formatted("2020-09-08T13:42:29.190855Z", "%+").unwrap()
        );
        assert_eq!(
            1599572549000000000,
            parse_timestamp_formatted("2020-09-08T13:42:29Z", "%+").unwrap()
        ); // no fractional part
        assert_eq!(
            1599590549190855000,
            parse_timestamp_formatted("2020-09-08T13:42:29.190855-05:00", "%+").unwrap()
        );
        assert_eq!(
            1599590549000000000,
            parse_timestamp_formatted("1599590549", "%s").unwrap()
        );
        assert_eq!(
            1599572549000000000,
            parse_timestamp_formatted("09-08-2020 13/42/29", "%m-%d-%Y %H/%M/%S")
                .unwrap()
        );
        assert_eq!(
            1642896000000000000,
            parse_timestamp_formatted("2022-01-23", "%Y-%m-%d").unwrap()
        );
    }

    fn parse_timestamp_formatted(s: &str, format: &str) -> Result<i64, DataFusionError> {
        let result = string_to_timestamp_nanos_formatted_with_timezone(
            &Some("UTC".parse()?),
            s,
            format,
        );
        if let Err(e) = &result {
            eprintln!("Error parsing timestamp '{s}' using format '{format}': {e:?}");
        }
        result
    }

    #[test]
    fn string_to_timestamp_formatted_invalid() {
        // Test parsing invalid formats
        let cases = [
            ("", "%Y%m%d %H%M%S", "premature end of input"),
            ("SS", "%c", "premature end of input"),
            ("Wed, 18 Feb 2015 23:16:09 GMT", "", "trailing input"),
            (
                "Wed, 18 Feb 2015 23:16:09 GMT",
                "%XX",
                "input contains invalid characters",
            ),
            (
                "Wed, 18 Feb 2015 23:16:09 GMT",
                "%Y%m%d %H%M%S",
                "input contains invalid characters",
            ),
        ];

        for (s, f, ctx) in cases {
            let expected = format!(
                "Execution error: Error parsing timestamp from '{s}' using format '{f}': {ctx}"
            );
            let actual = string_to_datetime_formatted(&Utc, s, f)
                .unwrap_err()
                .strip_backtrace();
            assert_eq!(actual, expected)
        }
    }

    #[test]
    fn string_to_timestamp_invalid_arguments() {
        // Test parsing invalid formats
        let cases = [
            ("", "%Y%m%d %H%M%S", "premature end of input"),
            ("SS", "%c", "premature end of input"),
            ("Wed, 18 Feb 2015 23:16:09 GMT", "", "trailing input"),
            (
                "Wed, 18 Feb 2015 23:16:09 GMT",
                "%XX",
                "input contains invalid characters",
            ),
            (
                "Wed, 18 Feb 2015 23:16:09 GMT",
                "%Y%m%d %H%M%S",
                "input contains invalid characters",
            ),
        ];

        for (s, f, ctx) in cases {
            let expected = format!(
                "Execution error: Error parsing timestamp from '{s}' using format '{f}': {ctx}"
            );
            let actual = string_to_datetime_formatted(&Utc, s, f)
                .unwrap_err()
                .strip_backtrace();
            assert_eq!(actual, expected)
        }
    }

    #[test]
    fn test_no_tz() {
        let udfs: Vec<Box<dyn ScalarUDFImpl>> = vec![
            Box::new(ToTimestampFunc::new_with_config(&ConfigOptions::default())),
            Box::new(ToTimestampSecondsFunc::new_with_config(
                &ConfigOptions::default(),
            )),
            Box::new(ToTimestampMillisFunc::new_with_config(
                &ConfigOptions::default(),
            )),
            Box::new(ToTimestampNanosFunc::new_with_config(
                &ConfigOptions::default(),
            )),
            Box::new(ToTimestampSecondsFunc::new_with_config(
                &ConfigOptions::default(),
            )),
        ];

        let mut nanos_builder = TimestampNanosecondArray::builder(2);
        let mut millis_builder = TimestampMillisecondArray::builder(2);
        let mut micros_builder = TimestampMicrosecondArray::builder(2);
        let mut sec_builder = TimestampSecondArray::builder(2);

        nanos_builder.append_value(1599572549190850000);
        millis_builder.append_value(1599572549190);
        micros_builder.append_value(1599572549190850);
        sec_builder.append_value(1599572549);

        let nanos_timestamps =
            Arc::new(nanos_builder.finish().with_timezone("UTC")) as ArrayRef;
        let millis_timestamps =
            Arc::new(millis_builder.finish().with_timezone("UTC")) as ArrayRef;
        let micros_timestamps =
            Arc::new(micros_builder.finish().with_timezone("UTC")) as ArrayRef;
        let sec_timestamps =
            Arc::new(sec_builder.finish().with_timezone("UTC")) as ArrayRef;

        let arrays = &[
            ColumnarValue::Array(Arc::clone(&nanos_timestamps)),
            ColumnarValue::Array(Arc::clone(&millis_timestamps)),
            ColumnarValue::Array(Arc::clone(&micros_timestamps)),
            ColumnarValue::Array(Arc::clone(&sec_timestamps)),
        ];

        for udf in &udfs {
            for array in arrays {
                let rt = udf.return_type(&[array.data_type()]).unwrap();
                let arg_field = Field::new("arg", array.data_type().clone(), true).into();
                assert!(matches!(rt, Timestamp(_, None)));
                let args = ScalarFunctionArgs {
                    args: vec![array.clone()],
                    arg_fields: vec![arg_field],
                    number_rows: 4,
                    return_field: Field::new("f", rt, true).into(),
                    config_options: Arc::new(ConfigOptions::default()),
                };
                let res = udf
                    .invoke_with_args(args)
                    .expect("that to_timestamp parsed values without error");
                let array = match res {
                    ColumnarValue::Array(res) => res,
                    _ => panic!("Expected a columnar array"),
                };
                let ty = array.data_type();
                assert!(matches!(ty, Timestamp(_, None)));
            }
        }

        let mut nanos_builder = TimestampNanosecondArray::builder(2);
        let mut millis_builder = TimestampMillisecondArray::builder(2);
        let mut micros_builder = TimestampMicrosecondArray::builder(2);
        let mut sec_builder = TimestampSecondArray::builder(2);
        let mut i64_builder = Int64Array::builder(2);

        nanos_builder.append_value(1599572549190850000);
        millis_builder.append_value(1599572549190);
        micros_builder.append_value(1599572549190850);
        sec_builder.append_value(1599572549);
        i64_builder.append_value(1599572549);

        let nanos_timestamps = Arc::new(nanos_builder.finish()) as ArrayRef;
        let millis_timestamps = Arc::new(millis_builder.finish()) as ArrayRef;
        let micros_timestamps = Arc::new(micros_builder.finish()) as ArrayRef;
        let sec_timestamps = Arc::new(sec_builder.finish()) as ArrayRef;
        let i64_timestamps = Arc::new(i64_builder.finish()) as ArrayRef;

        let arrays = &[
            ColumnarValue::Array(Arc::clone(&nanos_timestamps)),
            ColumnarValue::Array(Arc::clone(&millis_timestamps)),
            ColumnarValue::Array(Arc::clone(&micros_timestamps)),
            ColumnarValue::Array(Arc::clone(&sec_timestamps)),
            ColumnarValue::Array(Arc::clone(&i64_timestamps)),
        ];

        for udf in &udfs {
            for array in arrays {
                let rt = udf.return_type(&[array.data_type()]).unwrap();
                assert!(matches!(rt, Timestamp(_, None)));
                let arg_field = Field::new("arg", array.data_type().clone(), true).into();
                let args = ScalarFunctionArgs {
                    args: vec![array.clone()],
                    arg_fields: vec![arg_field],
                    number_rows: 5,
                    return_field: Field::new("f", rt, true).into(),
                    config_options: Arc::new(ConfigOptions::default()),
                };
                let res = udf
                    .invoke_with_args(args)
                    .expect("that to_timestamp parsed values without error");
                let array = match res {
                    ColumnarValue::Array(res) => res,
                    _ => panic!("Expected a columnar array"),
                };
                let ty = array.data_type();
                assert!(matches!(ty, Timestamp(_, None)));
            }
        }
    }

    #[test]
    fn test_to_timestamp_arg_validation() {
        let mut date_string_builder = StringBuilder::with_capacity(2, 1024);
        date_string_builder.append_value("2020-09-08T13:42:29.19085Z");

        let data = date_string_builder.finish();

        let funcs: Vec<(ScalarFunctionImplementation, TimeUnit)> = vec![
            (Arc::new(to_timestamp), Nanosecond),
            (Arc::new(to_timestamp_micros), Microsecond),
            (Arc::new(to_timestamp_millis), Millisecond),
            (Arc::new(to_timestamp_nanos), Nanosecond),
            (Arc::new(to_timestamp_seconds), Second),
        ];

        let mut nanos_builder = TimestampNanosecondArray::builder(2);
        let mut millis_builder = TimestampMillisecondArray::builder(2);
        let mut micros_builder = TimestampMicrosecondArray::builder(2);
        let mut sec_builder = TimestampSecondArray::builder(2);

        nanos_builder.append_value(1599572549190850000);
        millis_builder.append_value(1599572549190);
        micros_builder.append_value(1599572549190850);
        sec_builder.append_value(1599572549);

        let nanos_expected_timestamps = &nanos_builder.finish() as &dyn Array;
        let millis_expected_timestamps = &millis_builder.finish() as &dyn Array;
        let micros_expected_timestamps = &micros_builder.finish() as &dyn Array;
        let sec_expected_timestamps = &sec_builder.finish() as &dyn Array;

        for (func, time_unit) in funcs {
            // test UTF8
            let string_array = [
                ColumnarValue::Array(Arc::new(data.clone()) as ArrayRef),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%s".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%c".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("%+".to_string()))),
            ];
            let parsed_timestamps = func(&string_array)
                .expect("that to_timestamp with format args parsed values without error");
            if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
                assert_eq!(parsed_array.len(), 1);
                match time_unit {
                    Nanosecond => {
                        assert_eq!(nanos_expected_timestamps, parsed_array.as_ref())
                    }
                    Millisecond => {
                        assert_eq!(millis_expected_timestamps, parsed_array.as_ref())
                    }
                    Microsecond => {
                        assert_eq!(micros_expected_timestamps, parsed_array.as_ref())
                    }
                    Second => {
                        assert_eq!(sec_expected_timestamps, parsed_array.as_ref())
                    }
                };
            } else {
                panic!("Expected a columnar array")
            }

            // test LargeUTF8
            let string_array = [
                ColumnarValue::Array(Arc::new(data.clone()) as ArrayRef),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("%s".to_string()))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("%c".to_string()))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some("%+".to_string()))),
            ];
            let parsed_timestamps = func(&string_array)
                .expect("that to_timestamp with format args parsed values without error");
            if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
                assert_eq!(parsed_array.len(), 1);
                assert!(matches!(parsed_array.data_type(), Timestamp(_, None)));

                match time_unit {
                    Nanosecond => {
                        assert_eq!(nanos_expected_timestamps, parsed_array.as_ref())
                    }
                    Millisecond => {
                        assert_eq!(millis_expected_timestamps, parsed_array.as_ref())
                    }
                    Microsecond => {
                        assert_eq!(micros_expected_timestamps, parsed_array.as_ref())
                    }
                    Second => {
                        assert_eq!(sec_expected_timestamps, parsed_array.as_ref())
                    }
                };
            } else {
                panic!("Expected a columnar array")
            }

            // test other types
            let string_array = [
                ColumnarValue::Array(Arc::new(data.clone()) as ArrayRef),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
            ];

            let expected = "Unsupported data type Int32 for function".to_string();
            let actual = func(&string_array).unwrap_err().to_string();
            assert_contains!(actual, expected);

            // test other types
            let string_array = [
                ColumnarValue::Array(Arc::new(data.clone()) as ArrayRef),
                ColumnarValue::Array(Arc::new(PrimitiveArray::<Int64Type>::new(
                    vec![1i64].into(),
                    None,
                )) as ArrayRef),
            ];

            let expected = "Unsupported data type".to_string();
            let actual = func(&string_array).unwrap_err().to_string();
            assert_contains!(actual, expected);
        }
    }
}
