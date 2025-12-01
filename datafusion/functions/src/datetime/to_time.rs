use std::any::Any;
use std::sync::Arc;

use crate::datetime::common::*;
use arrow::array::cast::AsArray;
use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::DataType::*;
use arrow::datatypes::Time64NanosecondType;
use arrow::datatypes::TimeUnit::Nanosecond;
use arrow::datatypes::DataType;
use chrono::{DateTime, NaiveTime, Timelike};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"
Converts a value to a time (`HH:MM:SS.nnnnnnnnn`). Supports strings as input. Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00' or '05:44:00') if no [Chrono formats] are provided. If a full timestamp is provided, only the time component is extracted. Returns the corresponding time.
"#,
    syntax_example = "to_time(expression[, ..., format_n])",
    sql_example = r#"```sql
> select to_time('14:30:45');
+-------------------+
|| to_time(Utf8("14:30:45")) |
+-------------------+
|| 14:30:45          |
+-------------------+
> select to_time('2023-01-31T14:30:45');
+-----------------------------------+
|| to_time(Utf8("2023-01-31T14:30:45")) |
+-----------------------------------+
|| 14:30:45                          |
+-----------------------------------+
> select to_time('14:30:45.123456789', '%H:%M:%S%.f');
+---------------------------------------------------+
|| to_time(Utf8("14:30:45.123456789"),Utf8("%H:%M:%S%.f")) |
+---------------------------------------------------+
|| 14:30:45.123456789                                |
+---------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/date_time.rs)
"#,
    argument(
        name = "expression",
        description = "Expression to operate on. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "format_n",
        description = "Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order they appear with the first successful one being returned. If none of the formats successfully parse the expression an error will be returned."
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
        Ok(Time64(Nanosecond))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = args.args;
        if args.is_empty() {
            return exec_err!(
                "to_time function requires 1 or more arguments, got {}",
                args.len()
            );
        }

        if args.len() > 1 {
            validate_data_types(&args, "to_time")?;
        }

        match args[0].data_type() {
            Null => Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(None))),
            Time64(_) => args[0].cast_to(&Time64(Nanosecond), None),
            Utf8View | LargeUtf8 | Utf8 => to_time_impl(&args, "to_time"),
            other => {
                exec_err!("Unsupported data type {other} for function to_time")
            }
        }
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn to_time_impl(args: &[ColumnarValue], name: &str) -> Result<ColumnarValue> {
    let extract_time_nanos = |nanos: i64| -> i64 {
        let secs = nanos / 1_000_000_000;
        let nsecs = (nanos % 1_000_000_000) as u32;
        if let Some(dt) = DateTime::from_timestamp(secs, nsecs) {
            let hour = dt.hour() as i64;
            let minute = dt.minute() as i64;
            let second = dt.second() as i64;
            let nanosecond = dt.nanosecond() as i64;
            (hour * 3600 + minute * 60 + second) * 1_000_000_000 + nanosecond
        } else {
            0
        }
    };

    let parse_time_string = |s: &str| -> Result<i64> {
        if let Ok(time) = NaiveTime::parse_from_str(s, "%H:%M:%S") {
            let nanos = (time.hour() as i64 * 3600
                + time.minute() as i64 * 60
                + time.second() as i64)
                * 1_000_000_000
                + time.nanosecond() as i64;
            return Ok(nanos);
        }
        if let Ok(time) = NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
            let nanos = (time.hour() as i64 * 3600
                + time.minute() as i64 * 60
                + time.second() as i64)
                * 1_000_000_000
                + time.nanosecond() as i64;
            return Ok(nanos);
        }
        string_to_timestamp_nanos_shim(s).map(extract_time_nanos)
    };

    match args.len() {
        1 => {
            match &args[0] {
                ColumnarValue::Array(a) => {
                    let nanos_array = match a.data_type() {
                        Utf8View => {
                            let array = a.as_string_view();
                            let mut builder = PrimitiveArray::<Time64NanosecondType>::builder(array.len());
                            for i in 0..array.len() {
                                if array.is_null(i) {
                                    builder.append_null();
                                } else {
                                    let s = array.value(i);
                                    match parse_time_string(s) {
                                        Ok(nanos) => builder.append_value(nanos),
                                        Err(e) => return Err(e),
                                    }
                                }
                            }
                            builder.finish()
                        }
                        LargeUtf8 => {
                            let array = a.as_string::<i64>();
                            let mut builder = PrimitiveArray::<Time64NanosecondType>::builder(array.len());
                            for i in 0..array.len() {
                                if array.is_null(i) {
                                    builder.append_null();
                                } else {
                                    let s = array.value(i);
                                    match parse_time_string(s) {
                                        Ok(nanos) => builder.append_value(nanos),
                                        Err(e) => return Err(e),
                                    }
                                }
                            }
                            builder.finish()
                        }
                        Utf8 => {
                            let array = a.as_string::<i32>();
                            let mut builder = PrimitiveArray::<Time64NanosecondType>::builder(array.len());
                            for i in 0..array.len() {
                                if array.is_null(i) {
                                    builder.append_null();
                                } else {
                                    let s = array.value(i);
                                    match parse_time_string(s) {
                                        Ok(nanos) => builder.append_value(nanos),
                                        Err(e) => return Err(e),
                                    }
                                }
                            }
                            builder.finish()
                        }
                        other => return exec_err!("Unsupported data type {other:?} for function {name}"),
                    };
                    Ok(ColumnarValue::Array(Arc::new(nanos_array)))
                }
                ColumnarValue::Scalar(scalar) => {
                    match scalar {
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                            let nanos = parse_time_string(s)?;
                            Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(nanos))))
                        }
                        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(None)))
                        }
                        _ => exec_err!("Unsupported data type {scalar:?} for function {name}"),
                    }
                }
            }
        }
        n if n >= 2 => {
            let mut formats = Vec::new();
            for arg in &args[1..] {
                match arg {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(f))) | 
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(f))) => {
                        formats.push(f.as_str());
                    }
                    _ => {}
                }
            }
            if formats.is_empty() {
                return exec_err!("to_time with formats requires at least one format string");
            }
            match &args[0] {
                ColumnarValue::Array(a) => {
                    let mut builder = PrimitiveArray::<Time64NanosecondType>::builder(a.len());
                    for i in 0..a.len() {
                        if a.is_null(i) {
                            builder.append_null();
                        } else {
                            let s = match a.data_type() {
                                Utf8View => a.as_string_view().value(i),
                                LargeUtf8 => a.as_string::<i64>().value(i),
                                Utf8 => a.as_string::<i32>().value(i),
                                _ => return exec_err!("Unsupported data type for function {name}"),
                            };
                            let mut parsed = false;
                            for format in formats.iter() {
                                match string_to_timestamp_nanos_formatted(s, format) {
                                    Ok(nanos) => {
                                        builder.append_value(extract_time_nanos(nanos));
                                        parsed = true;
                                        break;
                                    }
                                    Err(_) => continue,
                                }
                            }
                            if !parsed {
                                return exec_err!("Unable to parse time from '{s}' using provided formats");
                            }
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                }
                ColumnarValue::Scalar(scalar) => {
                    let s = match scalar {
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s,
                        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
                            return Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(None)));
                        }
                        _ => return exec_err!("Unsupported data type {scalar:?} for function {name}"),
                    };
                    for format in formats.iter() {
                        if let Ok(nanos) = string_to_timestamp_nanos_formatted(s, format) {
                            return Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(Some(extract_time_nanos(nanos)))));
                        }
                    }
                    exec_err!("Unable to parse time from '{s}' using provided formats")
                }
            }
        }
        _ => exec_err!("Unsupported 0 argument count for function {name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
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
            return_field: Field::new("f", Time64(Nanosecond), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        ToTimeFunc::new().invoke_with_args(args)
    }

    #[test]
    fn test_to_time() {
        let res = invoke_to_time_with_args(
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "14:30:45".to_string(),
            )))],
            1,
        )
        .expect("that to_time parsed values without error");

        if let ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(time)) = res {
            let expected = 14 * 3600 + 30 * 60 + 45;
            assert_eq!(Some(expected * 1_000_000_000), time);
        } else {
            panic!("Expected a scalar value")
        }

        let res = invoke_to_time_with_args(
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "2023-01-31T14:30:45".to_string(),
            )))],
            1,
        )
        .expect("that to_time parsed timestamp without error");

        if let ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(time)) = res {
            let expected = 14 * 3600 + 30 * 60 + 45;
            assert_eq!(Some(expected * 1_000_000_000), time);
        } else {
            panic!("Expected a scalar value")
        }
    }

    #[test]
    fn test_to_time_null() {
        let res = invoke_to_time_with_args(
            vec![ColumnarValue::Scalar(ScalarValue::Null)],
            1,
        )
        .expect("that to_time handled null without error");

        assert!(matches!(res, ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(None))));
    }
}

