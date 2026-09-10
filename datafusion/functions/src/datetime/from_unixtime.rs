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

use arrow::array::AsArray;
use arrow::array::timezone::Tz;
use arrow::compute::kernels::aggregate::{max, min};
use arrow::datatypes::DataType::{Int64, Timestamp, Utf8};
use arrow::datatypes::Int64Type;
use arrow::datatypes::TimeUnit::Second;
use arrow::datatypes::{DataType, Field, FieldRef};
use chrono::{DateTime, NaiveDateTime, Offset, TimeDelta, TimeZone};
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

        let timezone = match len {
            1 => self.timezone.clone(),
            2 => match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz))) => {
                    Some(Arc::<str>::from(tz.as_str()))
                }
                _ => {
                    return exec_err!(
                        "Unsupported data type {} for function from_unixtime",
                        args[1].data_type()
                    );
                }
            },
            _ => unreachable!(),
        };

        validate_local_datetime_range(&args[0], timezone.as_deref())?;

        args[0].cast_to(&Timestamp(Second, timezone), None)
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

/// An upper bound on the magnitude of any time zone's UTC offset, rounded up to
/// a whole day. Real offsets never exceed 15 hours (including the historical
/// local mean time offsets used before a zone's first transition).
const MAX_TIMEZONE_OFFSET_SECONDS: i64 = 24 * 60 * 60;

/// The range of `Timestamp(Second)` values whose local date and time is
/// representable in *every* time zone.
///
/// Values inside this range never need a per-value check: shifting them by any
/// possible UTC offset keeps them inside [`NaiveDateTime`]'s range.
fn always_representable_range() -> (i64, i64) {
    (
        NaiveDateTime::MIN.and_utc().timestamp() + MAX_TIMEZONE_OFFSET_SECONDS,
        NaiveDateTime::MAX.and_utc().timestamp() - MAX_TIMEZONE_OFFSET_SECONDS,
    )
}

/// Returns an error if any value in `values` has no representable local date
/// and time in `timezone`.
///
/// A `Timestamp(Second, Some(tz))` is *rendered* by shifting the UTC instant by
/// the zone's offset. Arrow does that with `DateTime::naive_local`, which
/// **panics** when the shifted value falls outside [`NaiveDateTime`]'s range,
/// so such values have to be rejected before they are produced: the panic would
/// otherwise happen far away from `from_unixtime`, when the value is formatted.
///
/// See <https://github.com/apache/datafusion/issues/16594>.
fn validate_local_datetime_range(
    values: &ColumnarValue,
    timezone: Option<&str>,
) -> Result<()> {
    // A timezone naive timestamp is never shifted, so it can only be out of
    // range if the UTC instant itself is, which the cast already rejects.
    let Some(timezone) = timezone else {
        return Ok(());
    };

    let (safe_min, safe_max) = always_representable_range();

    let (value_min, value_max) = match values {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => (*value, *value),
        // NULL (and anything else the cast will reject) needs no check.
        ColumnarValue::Scalar(_) => return Ok(()),
        ColumnarValue::Array(array) => {
            let array = array.as_primitive::<Int64Type>();
            match (min(array), max(array)) {
                (Some(value_min), Some(value_max)) => (value_min, value_max),
                // All null.
                _ => return Ok(()),
            }
        }
    };

    // Fast path: no value can overflow, whatever the zone's offset is.
    if value_min >= safe_min && value_max <= safe_max {
        return Ok(());
    }

    let Ok(tz) = timezone.parse::<Tz>() else {
        // An unparseable time zone is reported by the cast below.
        return Ok(());
    };

    let check = |seconds: i64| -> Result<()> {
        if seconds >= safe_min && seconds <= safe_max {
            return Ok(());
        }
        let Some(utc) = DateTime::from_timestamp(seconds, 0) else {
            // Not representable as an instant at all: the cast reports this.
            return Ok(());
        };
        let utc = utc.naive_utc();
        let offset = tz.offset_from_utc_datetime(&utc).fix().local_minus_utc();
        if utc
            .checked_add_signed(TimeDelta::seconds(i64::from(offset)))
            .is_none()
        {
            return exec_err!(
                "Cannot convert {seconds} to a timestamp in timezone \"{timezone}\" \
                 for function from_unixtime: the local date and time is outside the \
                 supported range"
            );
        }
        Ok(())
    };

    match values {
        ColumnarValue::Scalar(_) => check(value_min),
        ColumnarValue::Array(array) => {
            // Only the values near the limits do any real work; `check` returns
            // immediately for everything inside the always representable range.
            for value in array.as_primitive::<Int64Type>().iter().flatten() {
                check(value)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::datetime::from_unixtime::FromUnixtimeFunc;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::TimeUnit::Second;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_common::ScalarValue::Int64;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    };
    use std::sync::Arc;

    /// The last second since the epoch that `chrono::NaiveDateTime` can
    /// represent (`+262142-12-31T23:59:59`).
    const MAX_UTC_SECONDS: i64 = 8210266876799;
    /// The first second since the epoch that `chrono::NaiveDateTime` can
    /// represent (`-262143-01-01T00:00:00`).
    const MIN_UTC_SECONDS: i64 = -8334601228800;

    /// Invoke the single argument form with `datafusion.execution.time_zone`
    /// set to `timezone`.
    fn from_unixtime_session_tz(
        seconds: i64,
        timezone: Option<&str>,
    ) -> Result<ColumnarValue> {
        let mut options = ConfigOptions::default();
        options.execution.time_zone = timezone.map(str::to_string);
        let func = FromUnixtimeFunc::new_with_config(&options);

        let arg_field: FieldRef = Field::new("a", DataType::Int64, true).into();
        let return_field = Field::new(
            "f",
            DataType::Timestamp(Second, timezone.map(Arc::from)),
            true,
        )
        .into();
        func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(Int64(Some(seconds)))],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field,
            config_options: Arc::new(options),
        })
    }

    /// Invoke the two argument form with an explicit `timezone`.
    fn from_unixtime_explicit_tz(
        values: ColumnarValue,
        timezone: &str,
    ) -> Result<ColumnarValue> {
        let number_rows = match &values {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let arg_fields: Vec<FieldRef> = vec![
            Field::new("a", DataType::Int64, true).into(),
            Field::new("b", DataType::Utf8, true).into(),
        ];
        let return_field = Field::new(
            "f",
            DataType::Timestamp(Second, Some(Arc::from(timezone))),
            true,
        )
        .into();
        FromUnixtimeFunc::default().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                values,
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(timezone.to_string()))),
            ],
            arg_fields,
            number_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    /// Render the result the way a client would.
    ///
    /// The out of range local date and time panics when the value is
    /// *formatted*, not when it is produced, so every bounds test has to go
    /// through a formatter to be meaningful.
    fn display(value: ColumnarValue) -> String {
        let array = value.to_array(1).unwrap();
        let options = FormatOptions::default();
        let formatter = ArrayFormatter::try_new(array.as_ref(), &options).unwrap();
        formatter.value(0).to_string()
    }

    /// The single argument form used to silently produce values that panicked
    /// downstream once a session time zone was set.
    ///
    /// `America/New_York` is at -04:56:02 (local mean time) that far in the
    /// past, so the local date and time runs out one offset earlier than the
    /// UTC instant does.
    #[test]
    fn test_named_timezone_lower_bound() {
        let last_ok = MIN_UTC_SECONDS + 17762;
        assert_eq!(last_ok, -8334601211038);

        let value = from_unixtime_session_tz(last_ok, Some("America/New_York")).unwrap();
        assert_eq!(display(value), "-262143-01-01T00:00:00-04:56");

        let err = from_unixtime_session_tz(last_ok - 1, Some("America/New_York"))
            .expect_err("expected an out of range error, not a value that panics");
        assert!(
            err.message().contains("outside the supported range"),
            "unexpected error: {err}"
        );
    }

    /// The same bound in the other direction: a zone that is *ahead* of UTC
    /// runs out of local date and time before the UTC instant does.
    ///
    /// This is the two argument form, which panicked on `main` as well
    /// (<https://github.com/apache/datafusion/issues/16594>).
    #[test]
    fn test_named_timezone_upper_bound() {
        // +09:00, with no daylight saving.
        let last_ok = MAX_UTC_SECONDS - 32400;

        let value = from_unixtime_explicit_tz(
            ColumnarValue::Scalar(Int64(Some(last_ok))),
            "Asia/Tokyo",
        )
        .unwrap();
        assert_eq!(display(value), "+262142-12-31T23:59:59+09:00");

        let err = from_unixtime_explicit_tz(
            ColumnarValue::Scalar(Int64(Some(last_ok + 1))),
            "Asia/Tokyo",
        )
        .expect_err("expected an out of range error, not a value that panics");
        assert!(
            err.message().contains("outside the supported range"),
            "unexpected error: {err}"
        );
    }

    /// Fixed offset zones hit exactly the same bound.
    #[test]
    fn test_fixed_offset_upper_bound() {
        let last_ok = MAX_UTC_SECONDS - 28800;

        let value = from_unixtime_session_tz(last_ok, Some("+08:00")).unwrap();
        assert_eq!(display(value), "+262142-12-31T23:59:59+08:00");

        let err = from_unixtime_session_tz(last_ok + 1, Some("+08:00"))
            .expect_err("expected an out of range error, not a value that panics");
        assert!(
            err.message().contains("outside the supported range"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_fixed_offset_lower_bound() {
        let last_ok = MIN_UTC_SECONDS + 28800;

        let value = from_unixtime_explicit_tz(
            ColumnarValue::Scalar(Int64(Some(last_ok))),
            "-08:00",
        )
        .unwrap();
        assert_eq!(display(value), "-262143-01-01T00:00:00-08:00");

        let err = from_unixtime_explicit_tz(
            ColumnarValue::Scalar(Int64(Some(last_ok - 1))),
            "-08:00",
        )
        .expect_err("expected an out of range error, not a value that panics");
        assert!(
            err.message().contains("outside the supported range"),
            "unexpected error: {err}"
        );
    }

    /// Array input is checked too, not just the constant folded scalar case.
    #[test]
    fn test_array_input_is_bounds_checked() {
        let array: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(0), None, Some(MIN_UTC_SECONDS)]));
        let err =
            from_unixtime_explicit_tz(ColumnarValue::Array(array), "America/New_York")
                .expect_err("expected an out of range error, not a value that panics");
        assert!(
            err.message().contains("outside the supported range"),
            "unexpected error: {err}"
        );

        // In range values still work.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(0), None]));
        let value =
            from_unixtime_explicit_tz(ColumnarValue::Array(array), "America/New_York")
                .unwrap();
        assert_eq!(display(value), "1969-12-31T19:00:00-05:00");
    }

    /// Without a time zone the value is never shifted, so the full
    /// `NaiveDateTime` range stays usable, exactly as before this bound check.
    #[test]
    fn test_timezone_naive_extremes_are_still_accepted() {
        let value = from_unixtime_session_tz(MIN_UTC_SECONDS, None).unwrap();
        assert_eq!(display(value), "-262143-01-01T00:00:00");

        let value = from_unixtime_session_tz(MAX_UTC_SECONDS, None).unwrap();
        assert_eq!(display(value), "+262142-12-31T23:59:59");
    }

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
