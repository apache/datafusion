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

use arrow::datatypes::DataType::{Int64, Timestamp, Utf8};
use arrow::datatypes::TimeUnit::Second;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Converts an integer to a timestamp with second precision (`Timestamp(Second)`).
The integer is interpreted as the number of seconds since the unix epoch
(`1970-01-01T00:00:00Z`).

If the optional `timezone` argument is omitted, the timestamp is returned in the
session time zone (`datafusion.execution.time_zone`), which is unset (i.e.
timezone-naive) by default."#,
    syntax_example = "from_unixtime(expression[, timezone])",
    sql_example = r#"```sql
> select from_unixtime(1599572549, 'America/New_York');
+-----------------------------------------------------------+
| from_unixtime(Int64(1599572549),Utf8("America/New_York")) |
+-----------------------------------------------------------+
| 2020-09-08T09:42:29-04:00                                 |
+-----------------------------------------------------------+

-- Without an explicit timezone the session time zone is used
> SET datafusion.execution.time_zone = 'America/New_York';
> select from_unixtime(1599572549);
+----------------------------------+
| from_unixtime(Int64(1599572549)) |
+----------------------------------+
| 2020-09-08T09:42:29-04:00        |
+----------------------------------+
```"#,
    standard_argument(name = "expression",),
    argument(
        name = "timezone",
        description = "Optional timezone to use when converting the integer to a timestamp. If not provided, the session time zone (`datafusion.execution.time_zone`) is used, which is unset (timezone-naive) by default."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FromUnixtimeFunc {
    signature: Signature,
    /// Timezone from `datafusion.execution.time_zone`, used by the
    /// single-argument form. The two-argument form always uses the timezone
    /// given explicitly as the second argument.
    timezone: Option<Arc<str>>,
}

impl Default for FromUnixtimeFunc {
    fn default() -> Self {
        Self::new_with_config(&ConfigOptions::default())
    }
}

impl FromUnixtimeFunc {
    #[deprecated(since = "56.0.0", note = "use `new_with_config` instead")]
    /// Deprecated constructor retained for backwards compatibility.
    ///
    /// Prefer [`FromUnixtimeFunc::new_with_config`], which picks up the session
    /// time zone from [`ConfigOptions`]. This helper mirrors the canonical
    /// default (no timezone) provided by `ConfigOptions::default()`.
    pub fn new() -> Self {
        Self::new_with_config(&ConfigOptions::default())
    }

    pub fn new_with_config(config: &ConfigOptions) -> Self {
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Int64, Utf8]), Exact(vec![Int64])],
                Volatility::Immutable,
            ),
            timezone: config
                .execution
                .time_zone
                .as_ref()
                .map(|tz| Arc::from(tz.as_str())),
        }
    }
}

impl ScalarUDFImpl for FromUnixtimeFunc {
    fn name(&self) -> &str {
        "from_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        Some(Self::new_with_config(config).into())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Length check handled in the signature
        debug_assert!(matches!(args.scalar_arguments.len(), 1 | 2));

        if args.scalar_arguments.len() == 1 {
            Ok(
                Field::new(self.name(), Timestamp(Second, self.timezone.clone()), true)
                    .into(),
            )
        } else {
            args.scalar_arguments[1]
                .and_then(|sv| {
                    sv.try_as_str()
                        .flatten()
                        .filter(|s| !s.is_empty())
                        .map(|tz| {
                            Field::new(
                                self.name(),
                                Timestamp(Second, Some(Arc::from(tz.to_string()))),
                                true,
                            )
                        })
                })
                .map(Arc::new)
                .map_or_else(
                    || {
                        exec_err!(
                            "{} requires its second argument to be a constant string",
                            self.name()
                        )
                    },
                    Ok,
                )
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("call return_field_from_args instead")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        let len = args.len();
        if len != 1 && len != 2 {
            return exec_err!(
                "from_unixtime function requires 1 or 2 argument, got {}",
                args.len()
            );
        }

        if args[0].data_type() != Int64 {
            return exec_err!(
                "Unsupported data type {} for function from_unixtime",
                args[0].data_type()
            );
        }

        match len {
            1 => args[0].cast_to(&Timestamp(Second, self.timezone.clone()), None),
            2 => match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz))) => args[0]
                    .cast_to(&Timestamp(Second, Some(Arc::from(tz.to_string()))), None),
                _ => {
                    exec_err!(
                        "Unsupported data type {} for function from_unixtime",
                        args[1].data_type()
                    )
                }
            },
            _ => unreachable!(),
        }
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        // The optional timezone argument must be a constant string and only
        // affects the display metadata, not the stored epoch value, so the
        // output ordering follows the first argument.
        Ok(inputs[0].sort_properties)
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        Ok(true)
    }

    fn strictly_order_preserving(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        // `from_unixtime` stores the input's exact `Int64` value as a
        // `Timestamp(Second)`: the mapping is one-to-one, order-preserving,
        // and maps nulls to nulls.
        Ok(true)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod test {
    use crate::datetime::from_unixtime::FromUnixtimeFunc;
    use arrow::datatypes::TimeUnit::Second;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use datafusion_common::ScalarValue;
    use datafusion_common::ScalarValue::Int64;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    };
    use std::sync::Arc;

    #[test]
    fn test_without_timezone() {
        let arg_field = Arc::new(Field::new("a", DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(Int64(Some(1729900800)))],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field: Field::new("f", DataType::Timestamp(Second, None), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = FromUnixtimeFunc::default().invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(sec), None)) => {
                assert_eq!(sec, 1729900800);
            }
            _ => panic!("Expected scalar value"),
        }
    }

    /// The single argument form reports (and produces) the session time zone
    /// configured via `datafusion.execution.time_zone`.
    #[test]
    fn test_session_timezone_is_used_without_explicit_timezone() {
        let mut options = ConfigOptions::default();
        options.execution.time_zone = Some("America/Denver".to_string());

        let func = FromUnixtimeFunc::new_with_config(&options);

        let arg_field: FieldRef = Field::new("a", DataType::Int64, true).into();
        let scalar_arguments = vec![None];
        let return_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg_field),
                scalar_arguments: &scalar_arguments,
            })
            .unwrap();
        assert_eq!(
            return_field.data_type(),
            &DataType::Timestamp(Second, Some(Arc::from("America/Denver")))
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(Int64(Some(1729900800)))],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(options),
        };
        let result = func.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(sec), Some(tz))) => {
                assert_eq!(sec, 1729900800);
                assert_eq!(tz.as_ref(), "America/Denver");
            }
            other => panic!("Expected timezone aware scalar value, got {other:?}"),
        }
    }

    /// An explicit second argument wins over the session time zone.
    #[test]
    fn test_explicit_timezone_overrides_session_timezone() {
        let mut options = ConfigOptions::default();
        options.execution.time_zone = Some("America/Denver".to_string());

        let func = FromUnixtimeFunc::new_with_config(&options);

        let arg_fields: Vec<FieldRef> = vec![
            Field::new("a", DataType::Int64, true).into(),
            Field::new("b", DataType::Utf8, true).into(),
        ];
        let tz_arg = ScalarValue::Utf8(Some("+08:00".to_string()));
        let scalar_arguments = vec![None, Some(&tz_arg)];
        let return_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
            .unwrap();
        assert_eq!(
            return_field.data_type(),
            &DataType::Timestamp(Second, Some(Arc::from("+08:00")))
        );

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(Int64(Some(1729900800))),
                ColumnarValue::Scalar(tz_arg.clone()),
            ],
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Arc::new(options),
        };
        let result = func.invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(sec), Some(tz))) => {
                assert_eq!(sec, 1729900800);
                assert_eq!(tz.as_ref(), "+08:00");
            }
            other => panic!("Expected timezone aware scalar value, got {other:?}"),
        }
    }

    #[test]
    fn test_with_timezone() {
        let arg_fields = vec![
            Field::new("a", DataType::Int64, true).into(),
            Field::new("a", DataType::Utf8, true).into(),
        ];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(Int64(Some(1729900800))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "America/New_York".to_string(),
                ))),
            ],
            arg_fields,
            number_rows: 2,
            return_field: Field::new(
                "f",
                DataType::Timestamp(Second, Some(Arc::from("America/New_York"))),
                true,
            )
            .into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = FromUnixtimeFunc::default().invoke_with_args(args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(sec), Some(tz))) => {
                assert_eq!(sec, 1729900800);
                assert_eq!(tz.to_string(), "America/New_York");
            }
            _ => panic!("Expected scalar value"),
        }
    }
}
