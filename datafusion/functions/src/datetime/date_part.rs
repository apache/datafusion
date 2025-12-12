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
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array};
use arrow::compute::kernels::cast_utils::IntervalUnit;
use arrow::compute::{DatePart, binary, date_part};
use arrow::datatypes::DataType::{
    Date32, Date64, Duration, Interval, Time32, Time64, Timestamp,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use arrow::temporal_conversions::{
    MICROSECONDS_IN_DAY, MILLISECONDS_IN_DAY, NANOSECONDS_IN_DAY, SECONDS_IN_DAY,
};
use chrono::{Datelike, NaiveDate};
use datafusion_common::types::{NativeType, logical_date};

use datafusion_common::{
    Result, ScalarValue,
    cast::{
        as_date32_array, as_date64_array, as_int32_array, as_time32_millisecond_array,
        as_time32_second_array, as_time64_microsecond_array, as_time64_nanosecond_array,
        as_timestamp_microsecond_array, as_timestamp_millisecond_array,
        as_timestamp_nanosecond_array, as_timestamp_second_array,
    },
    exec_err, internal_err, not_impl_err,
    types::logical_string,
    utils::take_function_args,
};
use datafusion_expr::simplify::SimplifyInfo;
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_expr::{Expr, interval_arithmetic};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Returns the specified part of the date as an integer.",
    syntax_example = "date_part(part, expression)",
    alternative_syntax = "extract(field FROM source)",
    argument(
        name = "part",
        description = r#"Part of the date to return. The following date parts are supported:
        
    - year
    - quarter (emits value in inclusive range [1, 4] based on which quartile of the year the date is in)
    - month
    - week (week of the year)
    - day (day of the month)
    - hour
    - minute
    - second
    - millisecond
    - microsecond
    - nanosecond
    - dow (day of the week where Sunday is 0)
    - doy (day of the year)
    - epoch (seconds since Unix epoch)
    - isodow (day of the week where Monday is 0)
"#
    ),
    argument(
        name = "expression",
        description = "Time expression to operate on. Can be a constant, column, or function."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DatePartFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for DatePartFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DatePartFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_implicit(
                            TypeSignatureClass::Timestamp,
                            // Not consistent with Postgres and DuckDB but to avoid regression we implicit cast string to timestamp
                            vec![TypeSignatureClass::Native(logical_string())],
                            NativeType::Timestamp(Nanosecond, None),
                        ),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Time),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Interval),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Duration),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datepart")],
        }
    }
}

impl ScalarUDFImpl for DatePartFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;

        field
            .and_then(|sv| {
                sv.try_as_str()
                    .flatten()
                    .filter(|s| !s.is_empty())
                    .map(|part| {
                        if is_epoch(part) {
                            Field::new(self.name(), DataType::Float64, true)
                        } else {
                            Field::new(self.name(), DataType::Int32, true)
                        }
                    })
            })
            .map(Arc::new)
            .map_or_else(
                || exec_err!("{} requires non-empty constant string", self.name()),
                Ok,
            )
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = args.args;
        let [part, array] = take_function_args(self.name(), args)?;

        let part = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = part {
            v
        } else if let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) = part {
            v
        } else {
            return exec_err!(
                "First argument of `DATE_PART` must be non-null scalar Utf8"
            );
        };

        let is_scalar = matches!(array, ColumnarValue::Scalar(_));

        let array = match array {
            ColumnarValue::Array(array) => Arc::clone(&array),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        let part_trim = part_normalization(&part);

        // using IntervalUnit here means we hand off all the work of supporting plurals (like "seconds")
        // and synonyms ( like "ms,msec,msecond,millisecond") to Arrow
        let arr = if let Ok(interval_unit) = IntervalUnit::from_str(part_trim) {
            match interval_unit {
                IntervalUnit::Year => date_part(array.as_ref(), DatePart::Year)?,
                IntervalUnit::Month => date_part(array.as_ref(), DatePart::Month)?,
                IntervalUnit::Week => date_part(array.as_ref(), DatePart::Week)?,
                IntervalUnit::Day => date_part(array.as_ref(), DatePart::Day)?,
                IntervalUnit::Hour => date_part(array.as_ref(), DatePart::Hour)?,
                IntervalUnit::Minute => date_part(array.as_ref(), DatePart::Minute)?,
                IntervalUnit::Second => seconds_as_i32(array.as_ref(), Second)?,
                IntervalUnit::Millisecond => seconds_as_i32(array.as_ref(), Millisecond)?,
                IntervalUnit::Microsecond => seconds_as_i32(array.as_ref(), Microsecond)?,
                IntervalUnit::Nanosecond => seconds_as_i32(array.as_ref(), Nanosecond)?,
                // century and decade are not supported by `DatePart`, although they are supported in postgres
                _ => return exec_err!("Date part '{part}' not supported"),
            }
        } else {
            // special cases that can be extracted (in postgres) but are not interval units
            match part_trim.to_lowercase().as_str() {
                "qtr" | "quarter" => date_part(array.as_ref(), DatePart::Quarter)?,
                "doy" => date_part(array.as_ref(), DatePart::DayOfYear)?,
                "dow" => date_part(array.as_ref(), DatePart::DayOfWeekSunday0)?,
                "isodow" => date_part(array.as_ref(), DatePart::DayOfWeekMonday0)?,
                "epoch" => epoch(array.as_ref())?,
                _ => return exec_err!("Date part '{part}' not supported"),
            }
        };

        Ok(if is_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(arr.as_ref(), 0)?)
        } else {
            ColumnarValue::Array(arr)
        })
    }

    // Only casting the year is supported since pruning other IntervalUnit is not possible
    // date_part(col, YEAR) = 2024 => col >= '2024-01-01' and col < '2025-01-01'
    // But for anything less than YEAR simplifying is not possible without specifying the bigger interval
    // date_part(col, MONTH) = 1 => col = '2023-01-01' or col = '2024-01-01' or ... or col = '3000-01-01'
    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &dyn SimplifyInfo,
    ) -> Result<Option<interval_arithmetic::Interval>> {
        let [part, col_expr] = take_function_args(self.name(), args)?;

        // Get the interval unit from the part argument
        let interval_unit = part
            .as_literal()
            .and_then(|sv| sv.try_as_str().flatten())
            .map(part_normalization)
            .and_then(|s| IntervalUnit::from_str(s).ok());

        // only support extracting year
        match interval_unit {
            Some(IntervalUnit::Year) => (),
            _ => return Ok(None),
        }

        // Check if the argument is a literal (e.g. date_part(YEAR, col) = 2024)
        let Some(argument_literal) = lit_expr.as_literal() else {
            return Ok(None);
        };

        // Extract i32 year from Scalar value
        let year = match argument_literal {
            ScalarValue::Int32(Some(y)) => *y,
            _ => return Ok(None),
        };

        // Can only extract year from Date32/64 and Timestamp column
        let target_type = match info.get_data_type(col_expr)? {
            Date32 | Date64 | Timestamp(_, _) => &info.get_data_type(col_expr)?,
            _ => return Ok(None),
        };

        // Compute the Interval bounds
        let start_time =
            NaiveDate::from_ymd_opt(year, 1, 1).expect("Expect computed start time");
        let end_time = start_time
            .with_year(year + 1)
            .expect("Expect computed end time");

        // Convert to ScalarValues
        let lower = date_to_scalar(start_time, target_type)
            .expect("Expect preimage interval lower bound");
        let upper = date_to_scalar(end_time, target_type)
            .expect("Expect preimage interval upper bound");
        Ok(Some(interval_arithmetic::Interval::try_new(lower, upper)?))
    }

    fn column_expr(&self, args: &[Expr]) -> Option<Expr> {
        Some(args[1].clone())
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn date_to_scalar(date: NaiveDate, target_type: &DataType) -> Option<ScalarValue> {
    let days = date
        .signed_duration_since(NaiveDate::from_epoch_days(0)?)
        .num_days();

    Some(match target_type {
        Date32 => ScalarValue::Date32(Some(days as i32)),
        Date64 => ScalarValue::Date64(Some(days * MILLISECONDS_IN_DAY)),
        Timestamp(unit, tz) => match unit {
            Second => {
                ScalarValue::TimestampSecond(Some(days * SECONDS_IN_DAY), tz.clone())
            }
            Millisecond => ScalarValue::TimestampMillisecond(
                Some(days * MILLISECONDS_IN_DAY),
                tz.clone(),
            ),
            Microsecond => ScalarValue::TimestampMicrosecond(
                Some(days * MICROSECONDS_IN_DAY),
                tz.clone(),
            ),
            Nanosecond => ScalarValue::TimestampNanosecond(
                Some(days * NANOSECONDS_IN_DAY),
                tz.clone(),
            ),
        },
        _ => return None,
    })
}

fn is_epoch(part: &str) -> bool {
    let part = part_normalization(part);
    matches!(part.to_lowercase().as_str(), "epoch")
}

// Try to remove quote if exist, if the quote is invalid, return original string and let the downstream function handle the error
fn part_normalization(part: &str) -> &str {
    part.strip_prefix(|c| c == '\'' || c == '\"')
        .and_then(|s| s.strip_suffix(|c| c == '\'' || c == '\"'))
        .unwrap_or(part)
}

/// Invoke [`date_part`] on an `array` (e.g. Timestamp) and convert the
/// result to a total number of seconds, milliseconds, microseconds or
/// nanoseconds
fn seconds_as_i32(array: &dyn Array, unit: TimeUnit) -> Result<ArrayRef> {
    // Nanosecond is neither supported in Postgres nor DuckDB, to avoid dealing
    // with overflow and precision issue we don't support nanosecond
    if unit == Nanosecond {
        return not_impl_err!("Date part {unit:?} not supported");
    }

    let conversion_factor = match unit {
        Second => 1_000_000_000,
        Millisecond => 1_000_000,
        Microsecond => 1_000,
        Nanosecond => 1,
    };

    let second_factor = match unit {
        Second => 1,
        Millisecond => 1_000,
        Microsecond => 1_000_000,
        Nanosecond => 1_000_000_000,
    };

    let secs = date_part(array, DatePart::Second)?;
    // This assumes array is primitive and not a dictionary
    let secs = as_int32_array(secs.as_ref())?;
    let subsecs = date_part(array, DatePart::Nanosecond)?;
    let subsecs = as_int32_array(subsecs.as_ref())?;

    // Special case where there are no nulls.
    if subsecs.null_count() == 0 {
        let r: Int32Array = binary(secs, subsecs, |secs, subsecs| {
            secs * second_factor + (subsecs % 1_000_000_000) / conversion_factor
        })?;
        Ok(Arc::new(r))
    } else {
        // Nulls in secs are preserved, nulls in subsecs are treated as zero to account for the case
        // where the number of nanoseconds overflows.
        let r: Int32Array = secs
            .iter()
            .zip(subsecs)
            .map(|(secs, subsecs)| {
                secs.map(|secs| {
                    let subsecs = subsecs.unwrap_or(0);
                    secs * second_factor + (subsecs % 1_000_000_000) / conversion_factor
                })
            })
            .collect();
        Ok(Arc::new(r))
    }
}

/// Invoke [`date_part`] on an `array` (e.g. Timestamp) and convert the
/// result to a total number of seconds, milliseconds, microseconds or
/// nanoseconds
///
/// Given epoch return f64, this is a duplicated function to optimize for f64 type
fn seconds(array: &dyn Array, unit: TimeUnit) -> Result<ArrayRef> {
    let sf = match unit {
        Second => 1_f64,
        Millisecond => 1_000_f64,
        Microsecond => 1_000_000_f64,
        Nanosecond => 1_000_000_000_f64,
    };
    let secs = date_part(array, DatePart::Second)?;
    // This assumes array is primitive and not a dictionary
    let secs = as_int32_array(secs.as_ref())?;
    let subsecs = date_part(array, DatePart::Nanosecond)?;
    let subsecs = as_int32_array(subsecs.as_ref())?;

    // Special case where there are no nulls.
    if subsecs.null_count() == 0 {
        let r: Float64Array = binary(secs, subsecs, |secs, subsecs| {
            (secs as f64 + ((subsecs % 1_000_000_000) as f64 / 1_000_000_000_f64)) * sf
        })?;
        Ok(Arc::new(r))
    } else {
        // Nulls in secs are preserved, nulls in subsecs are treated as zero to account for the case
        // where the number of nanoseconds overflows.
        let r: Float64Array = secs
            .iter()
            .zip(subsecs)
            .map(|(secs, subsecs)| {
                secs.map(|secs| {
                    let subsecs = subsecs.unwrap_or(0);
                    (secs as f64 + ((subsecs % 1_000_000_000) as f64 / 1_000_000_000_f64))
                        * sf
                })
            })
            .collect();
        Ok(Arc::new(r))
    }
}

fn epoch(array: &dyn Array) -> Result<ArrayRef> {
    const SECONDS_IN_A_DAY: f64 = 86400_f64;

    let f: Float64Array = match array.data_type() {
        Timestamp(Second, _) => as_timestamp_second_array(array)?.unary(|x| x as f64),
        Timestamp(Millisecond, _) => {
            as_timestamp_millisecond_array(array)?.unary(|x| x as f64 / 1_000_f64)
        }
        Timestamp(Microsecond, _) => {
            as_timestamp_microsecond_array(array)?.unary(|x| x as f64 / 1_000_000_f64)
        }
        Timestamp(Nanosecond, _) => {
            as_timestamp_nanosecond_array(array)?.unary(|x| x as f64 / 1_000_000_000_f64)
        }
        Date32 => as_date32_array(array)?.unary(|x| x as f64 * SECONDS_IN_A_DAY),
        Date64 => as_date64_array(array)?.unary(|x| x as f64 / 1_000_f64),
        Time32(Second) => as_time32_second_array(array)?.unary(|x| x as f64),
        Time32(Millisecond) => {
            as_time32_millisecond_array(array)?.unary(|x| x as f64 / 1_000_f64)
        }
        Time64(Microsecond) => {
            as_time64_microsecond_array(array)?.unary(|x| x as f64 / 1_000_000_f64)
        }
        Time64(Nanosecond) => {
            as_time64_nanosecond_array(array)?.unary(|x| x as f64 / 1_000_000_000_f64)
        }
        Interval(_) | Duration(_) => return seconds(array, Second),
        d => return exec_err!("Cannot convert {d:?} to epoch"),
    };
    Ok(Arc::new(f))
}

#[cfg(test)]
mod tests {
    use crate::datetime::expr_fn;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
    use datafusion_expr::expr_fn::col;
    use datafusion_expr::or;
    use datafusion_expr::{
        Expr, and, execution_props::ExecutionProps, lit, simplify::SimplifyContext,
    };
    use datafusion_optimizer::simplify_expressions::ExprSimplifier;
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn test_preimage_date_part_date32_eq() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
        let expr_lt = expr_fn::date_part(lit("year"), col("date32")).eq(lit(2024i32));
        let expected = and(
            col("date32").gt_eq(lit(ScalarValue::Date32(Some(19723)))),
            col("date32").lt(lit(ScalarValue::Date32(Some(20089)))),
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_date64_not_eq() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) <> 2024 -> c1 < 2024-01-01 AND c1 >= 2025-01-01
        let expr_lt = expr_fn::date_part(lit("year"), col("date64")).not_eq(lit(2024i32));
        let expected = or(
            col("date64").lt(lit(ScalarValue::Date64(Some(19723 * 86_400_000)))),
            col("date64").gt_eq(lit(ScalarValue::Date64(Some(20089 * 86_400_000)))),
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_lt() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_nano_none")).lt(lit(2024i32));
        let expected = col("ts_nano_none").lt(lit(ScalarValue::TimestampNanosecond(
            Some(19723 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_utc_gt() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_nano_utc")).gt(lit(2024i32));
        let expected = col("ts_nano_utc").gt_eq(lit(ScalarValue::TimestampNanosecond(
            Some(20089 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_sec_est_gt_eq() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_sec_est")).gt_eq(lit(2024i32));
        let expected = col("ts_sec_est").gt_eq(lit(ScalarValue::TimestampSecond(
            Some(19723 * 86_400),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_sec_est_lt_eq() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_mic_pt")).lt_eq(lit(2024i32));
        let expected = col("ts_mic_pt").lt(lit(ScalarValue::TimestampMicrosecond(
            Some(20089 * 86_400_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_lt_swap() {
        let schema = expr_test_schema();
        let expr_lt =
            lit(2024i32).gt(expr_fn::date_part(lit("year"), col("ts_nano_none")));
        let expected = col("ts_nano_none").lt(lit(ScalarValue::TimestampNanosecond(
            Some(19723 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    // Should not simplify
    fn test_preimage_date_part_not_year_date32_eq() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
        let expr_lt = expr_fn::date_part(lit("month"), col("date32")).eq(lit(1i32));
        let expected = expr_fn::date_part(lit("month"), col("date32")).eq(lit(1i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let props = ExecutionProps::new();
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&props).with_schema(Arc::clone(schema)),
        );

        simplifier.simplify(expr).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualified_fields(
                vec![
                    Field::new("date32", DataType::Date32, false),
                    Field::new("date64", DataType::Date64, false),
                    Field::new("ts_nano_none", timestamp_nano_none_type(), false),
                    Field::new("ts_nano_utc", timestamp_nano_utc_type(), false),
                    Field::new("ts_sec_est", timestamp_sec_est_type(), false),
                    Field::new("ts_mic_pt", timestamp_mic_pt_type(), false),
                ]
                .into(),
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    // this is the type that now() returns
    fn timestamp_nano_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Nanosecond, utc)
    }

    fn timestamp_sec_est_type() -> DataType {
        let est = Some("-5:00".into());
        DataType::Timestamp(TimeUnit::Second, est)
    }

    fn timestamp_mic_pt_type() -> DataType {
        let pt = Some("-8::00".into());
        DataType::Timestamp(TimeUnit::Microsecond, pt)
    }
}
