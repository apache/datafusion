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

use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

/// `timezone(zone, expression)`: the function form of the SQL operator
/// `expression AT TIME ZONE zone`, with the semantics of PostgreSQL and DuckDB.
///
/// The operator always returns the *other* kind of timestamp:
///
/// | Input type | Result | Computed as |
/// | --- | --- | --- |
/// | `Timestamp(unit, Some(_))` (aware) | the wall clock of the instant in `zone`, as `Timestamp(unit, None)` | `to_local_time(CAST(expression AS Timestamp(unit, Some(zone))))` |
/// | `Timestamp(unit, None)` (naive) | the value read as a wall clock in `zone`, as `Timestamp(unit, Some(zone))` | `CAST(expression AS Timestamp(unit, Some(zone)))` |
/// | any other type | as for a naive input | `CAST(expression AS Timestamp(ns, Some(zone)))` |
///
/// The choice depends only on the type of `expression`, and it is made every
/// time that type is read: [`ScalarUDFImpl::return_field_from_args`] for the
/// plan, and [`ScalarUDFImpl::invoke_with_args`] for the data. The function is
/// deliberately never rewritten into the `CAST` form during planning. A rewrite
/// would fix the choice from the type at that moment, and that type is not
/// always final: the SQL planner runs before type coercion, `PREPARE` runs the
/// optimizer without the analyzer, and an untyped placeholder has no type
/// until `EXECUTE`.
///
/// `zone` must be a constant, because it determines the result type.
#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r#"Converts a timestamp to another timezone. This is the function form of `expression AT TIME ZONE zone`, and it follows PostgreSQL and DuckDB:

- A timestamp **with** a timezone is an instant. The result is the wall-clock time of that instant in `zone`, as a timestamp **without** a timezone.
- A timestamp **without** a timezone is a wall-clock time. The result is the instant at which that wall-clock time occurs in `zone`, as a timestamp **with** the timezone `zone`.
- Any other type (for example a string) is converted as for a timestamp without a timezone.

The result keeps the time unit of a timestamp input."#,
    syntax_example = "timezone(zone, expression)",
    sql_example = r#"```sql
> SET datafusion.execution.time_zone = 'UTC';
> SELECT timezone('America/Denver', '2024-01-01T12:00:00Z'::timestamptz) AS denver_wall_clock;
+---------------------+
| denver_wall_clock   |
+---------------------+
| 2024-01-01T05:00:00 |
+---------------------+

> SELECT timezone('America/Denver', '2024-01-01T12:00:00'::timestamp) AS denver_instant;
+---------------------------+
| denver_instant            |
+---------------------------+
| 2024-01-01T12:00:00-07:00 |
+---------------------------+
```"#,
    argument(
        name = "zone",
        description = "Target timezone, as a constant string. An IANA timezone name such as `'America/Denver'`, or a fixed offset such as `'+05:30'`."
    ),
    argument(
        name = "expression",
        description = "Timestamp expression to convert. Can be a constant, column, or function."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimezoneFunc {
    signature: Signature,
}

impl Default for TimezoneFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TimezoneFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Any),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

/// The two steps that `timezone(zone, <input_type>)` does: the target type of
/// the `CAST`, and whether `to_local_time` follows it.
fn lowering(input_type: &DataType, zone: &str) -> (DataType, bool) {
    let (unit, input_is_aware) = match input_type {
        DataType::Timestamp(unit, tz) => (*unit, tz.is_some()),
        DataType::Dictionary(_, value_type) => match value_type.as_ref() {
            DataType::Timestamp(unit, tz) => (*unit, tz.is_some()),
            _ => (TimeUnit::Nanosecond, false),
        },
        _ => (TimeUnit::Nanosecond, false),
    };
    (DataType::Timestamp(unit, Some(zone.into())), input_is_aware)
}

fn zone_from_scalar(name: &str, zone: Option<&ScalarValue>) -> Result<String> {
    match zone.and_then(|s| s.try_as_str().flatten()) {
        Some(s) if !s.is_empty() => Ok(s.to_string()),
        _ => exec_err!(
            "{name} requires its first argument (the timezone) to be a constant, non-empty string"
        ),
    }
}

impl ScalarUDFImpl for TimezoneFunc {
    fn name(&self) -> &str {
        "timezone"
    }

    /// Name the column with the SQL operator syntax, so that
    /// `x AT TIME ZONE 'tz'` names its column `x AT TIME ZONE 'tz'`.
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [zone, input] = take_function_args(self.name(), args)?;
        let zone = match zone {
            Expr::Literal(sv, _) => match sv.try_as_str().flatten() {
                Some(s) => format!("'{s}'"),
                None => zone.schema_name().to_string(),
            },
            _ => zone.schema_name().to_string(),
        };
        Ok(format!("{} AT TIME ZONE {zone}", input.schema_name()))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [_, input] = take_function_args(self.name(), args.arg_fields)?;
        let zone = zone_from_scalar(self.name(), args.scalar_arguments[0])?;
        let (cast_type, strip_timezone) = lowering(input.data_type(), &zone);
        let return_type = match cast_type {
            DataType::Timestamp(unit, _) if strip_timezone => {
                DataType::Timestamp(unit, None)
            }
            other => other,
        };
        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            input.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [zone, input] = take_function_args(self.name(), &args.args)?;
        let zone = match zone {
            ColumnarValue::Scalar(zone) => zone_from_scalar(self.name(), Some(zone))?,
            ColumnarValue::Array(_) => zone_from_scalar(self.name(), None)?,
        };
        let (cast_type, strip_timezone) = lowering(&input.data_type(), &zone);
        let cast = input.cast_to(&cast_type, None)?;
        if strip_timezone {
            super::to_local_time::to_local_time(&cast)
        } else {
            Ok(cast)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    use super::TimezoneFunc;

    #[test]
    fn invoke() {
        // 2024-01-01T12:00:00Z
        let micros = 1_704_110_400_000_000;
        let cases = [
            // aware input -> naive wall clock in Denver (05:00)
            (
                Some("UTC"),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                micros - 7 * 3_600_000_000,
            ),
            // naive input -> the instant of 12:00 in Denver (19:00 UTC)
            (
                None,
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/Denver".into())),
                micros + 7 * 3_600_000_000,
            ),
        ];
        for (input_tz, expected_type, expected_value) in cases {
            let input: ArrayRef = Arc::new(
                TimestampMicrosecondArray::from(vec![micros]).with_timezone_opt(input_tz),
            );
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::from("America/Denver")),
                    ColumnarValue::Array(Arc::clone(&input)),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("zone", DataType::Utf8, false)),
                    Arc::new(Field::new("input", input.data_type().clone(), false)),
                ],
                number_rows: 1,
                return_field: Arc::new(Field::new("f", expected_type.clone(), false)),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let ColumnarValue::Array(result) =
                TimezoneFunc::new().invoke_with_args(args).unwrap()
            else {
                panic!("expected an array");
            };
            assert_eq!(result.data_type(), &expected_type);
            let result = result
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            assert_eq!(result.value(0), expected_value);
        }
    }
}
