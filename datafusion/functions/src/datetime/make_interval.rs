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

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, IntervalMonthDayNanoBuilder};
use arrow::datatypes::DataType::Interval;
use arrow::datatypes::IntervalUnit::MonthDayNano;
use arrow::datatypes::{DataType, IntervalMonthDayNano};
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = "Construct an INTERVAL (MonthDayNano) from component parts. Missing arguments default to 0; if any provided argument is NULL on a row, the result is NULL.",
    syntax_example = "make_interval([years[, months[, weeks[, days[, hours[, mins[, secs]]]]]])",
    sql_example = r#"```sql
-- Inline example without creating a table
> SELECT
      y, m, w, d, h, mi, s,
      make_interval(y, m, w, d, h, mi, s) AS interval
    FROM VALUES
      (1,   1,   1,   1,   1,   1,   1.0)
    AS v(y, m, w, d, h, mi, s);
+---+---+---+---+---+----+-----+--------------------------------------------------------------------------+
| y | m | w | d | h | mi | s   | interval                                                                 |
+---+---+---+---+---+----+-----+--------------------------------------------------------------------------+
| 1 | 1 | 1 | 1 | 1 | 1  | 1.0 | IntervalMonthDayNano { months: 13, days: 8, nanoseconds: 3661000000000 } |
+---+---+---+---+---+----+-----+--------------------------------------------------------------------------+
```"#,
    argument(
        name = "years",
        description = "Years to use when making the interval. Optional; defaults to 0. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "months",
        description = "Months to use when making the interval. Optional; defaults to 0. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "weeks",
        description = "Weeks to use when making the interval. Optional; defaults to 0. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "days",
        description = "Days to use when making the interval. Optional; defaults to 0. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "hours",
        description = "Hours to use when making the interval. Optional; defaults to 0. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "mins",
        description = "Minutes to use when making the interval. Optional; defaults to 0. Can be a constant, column or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "secs",
        description = "Seconds to use when making the interval (may be fractional). Optional; defaults to 0. Must be finite (not NaN/±Inf). Can be a constant, column or function, and any combination of arithmetic operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MakeIntervalFunc {
    signature: Signature,
}

impl Default for MakeIntervalFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeIntervalFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MakeIntervalFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Interval(MonthDayNano))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(make_interval_kernel, vec![])(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let length = arg_types.len();
        match length {
            0 => {
                exec_err!(
                    "make_interval expects between 1 and 7, got {}",
                    arg_types.len()
                )
            }
            x if x > 7 => {
                exec_err!(
                    "make_interval expects between 1 and 7, got {}",
                    arg_types.len()
                )
            }
            _ => Ok((0..arg_types.len())
                .map(|i| {
                    if i == 6 {
                        DataType::Float64
                    } else {
                        DataType::Int32
                    }
                })
                .collect()),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn make_interval_kernel(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    use arrow::array::AsArray;
    use arrow::datatypes::{Float64Type, Int32Type};

    if args.is_empty() || args.len() > 7 {
        return exec_err!("make_interval expects between 1 and 7, got {}", args.len());
    }

    let n_rows = args[0].len();
    debug_assert!(args.iter().all(|a| a.len() == n_rows));

    let years = args.first().map(|a| a.as_primitive::<Int32Type>());
    let months = args.get(1).map(|a| a.as_primitive::<Int32Type>());
    let weeks = args.get(2).map(|a| a.as_primitive::<Int32Type>());
    let days = args.get(3).map(|a| a.as_primitive::<Int32Type>());
    let hours = args.get(4).map(|a| a.as_primitive::<Int32Type>());
    let mins = args.get(5).map(|a| a.as_primitive::<Int32Type>());
    let secs = args.get(6).map(|a| a.as_primitive::<Float64Type>());

    let mut builder = IntervalMonthDayNanoBuilder::with_capacity(n_rows);

    for i in 0..n_rows {
        // if one column is NULL → result NULL
        let any_null_present = years.as_ref().is_some_and(|a| a.is_null(i))
            || months.as_ref().is_some_and(|a| a.is_null(i))
            || weeks.as_ref().is_some_and(|a| a.is_null(i))
            || days.as_ref().is_some_and(|a| a.is_null(i))
            || hours.as_ref().is_some_and(|a| a.is_null(i))
            || mins.as_ref().is_some_and(|a| a.is_null(i))
            || secs.as_ref().is_some_and(|a| {
                a.is_null(i) || a.value(i).is_infinite() || a.value(i).is_nan()
            });

        if any_null_present {
            builder.append_null();
            continue;
        }

        // default values 0 or 0.0
        let y = years.as_ref().map_or(0, |a| a.value(i));
        let mo = months.as_ref().map_or(0, |a| a.value(i));
        let w = weeks.as_ref().map_or(0, |a| a.value(i));
        let d = days.as_ref().map_or(0, |a| a.value(i));
        let h = hours.as_ref().map_or(0, |a| a.value(i));
        let mi = mins.as_ref().map_or(0, |a| a.value(i));
        let s = secs.as_ref().map_or(0.0, |a| a.value(i));

        let val = make_interval_month_day_nano(y, mo, w, d, h, mi, s)?;
        builder.append_value(val);
    }

    Ok(Arc::new(builder.finish()))
}

pub fn make_interval_month_day_nano(
    year: i32,
    month: i32,
    week: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> Result<IntervalMonthDayNano> {
    use datafusion_common::DataFusionError;

    if !sec.is_finite() {
        return Err(DataFusionError::Execution("seconds is NaN/Inf".into()));
    }

    let months = year
        .checked_mul(12)
        .and_then(|v| v.checked_add(month))
        .ok_or_else(|| DataFusionError::Execution("months overflow".into()))?;

    let total_days = week
        .checked_mul(7)
        .and_then(|v| v.checked_add(day))
        .ok_or_else(|| DataFusionError::Execution("days overflow".into()))?;

    let hours_nanos = (hour as i64)
        .checked_mul(3_600_000_000_000)
        .ok_or_else(|| DataFusionError::Execution("hours to nanos overflow".into()))?;
    let mins_nanos = (min as i64)
        .checked_mul(60_000_000_000)
        .ok_or_else(|| DataFusionError::Execution("minutes to nanos overflow".into()))?;

    let sec_int = sec.trunc() as i64;
    let frac = sec - sec.trunc();
    let mut frac_nanos = (frac * 1_000_000_000.0).round() as i64;

    if frac_nanos.abs() >= 1_000_000_000 {
        if frac_nanos > 0 {
            frac_nanos -= 1_000_000_000;
        } else {
            frac_nanos += 1_000_000_000;
        }
    }

    let secs_nanos = sec_int
        .checked_mul(1_000_000_000)
        .ok_or_else(|| DataFusionError::Execution("seconds to nanos overflow".into()))?;

    let total_nanos = hours_nanos
        .checked_add(mins_nanos)
        .and_then(|v| v.checked_add(secs_nanos))
        .and_then(|v| v.checked_add(frac_nanos))
        .ok_or_else(|| DataFusionError::Execution("sum nanos overflow".into()))?;

    Ok(IntervalMonthDayNano::new(months, total_days, total_nanos))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, Int32Array, IntervalMonthDayNanoArray};
    use datafusion_common::Result;

    use super::*;
    fn run_make_interval_month_day_nano(arrs: Vec<ArrayRef>) -> Result<ArrayRef> {
        make_interval_kernel(&arrs)
    }

    #[test]
    fn nulls_propagate_per_row() {
        let year = Arc::new(Int32Array::from(vec![
            None,
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let month = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let week = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let day = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let hour = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ]));
        let min = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            None,
            Some(7),
            Some(8),
            Some(9),
        ]));
        let sec = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(4.0),
            Some(5.0),
            Some(6.0),
            None,
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]));

        let out = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, min, sec,
        ])
        .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected IntervalMonthDayNano".into())
            })
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }

    #[test]
    fn error_months_overflow_should_be_null() {
        // months = year*12 + month → overflow -> NULL
        let year = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let err = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, min, sec,
        ])
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("months overflow"), "{msg}");
    }

    #[test]
    fn happy_path_all_present_single_row() {
        // 1y 2m 3w 4d 5h 6m 7.25s
        let year = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(4)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(5)])) as ArrayRef;
        let mins = Arc::new(Int32Array::from(vec![Some(6)])) as ArrayRef;
        let secs = Arc::new(Float64Array::from(vec![Some(7.25)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, mins, secs,
        ])
        .unwrap();
        assert_eq!(out.data_type(), &Interval(MonthDayNano));

        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out.null_count(), 0);

        let v: IntervalMonthDayNano = out.value(0);
        assert_eq!(v.months, 12 + 2); // 14
        assert_eq!(v.days, 3 * 7 + 4); // 25
        let expected_nanos = (5_i64 * 3600 + 6 * 60 + 7) * 1_000_000_000 + 250_000_000;
        assert_eq!(v.nanoseconds, expected_nanos);
    }

    #[test]
    fn negative_components_and_fractional_seconds() {
        // -1y -2m  -1w -1d  -1h -1m  -1.5s
        let year = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(-2)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let mins = Arc::new(Int32Array::from(vec![Some(-1)])) as ArrayRef;
        let secs = Arc::new(Float64Array::from(vec![Some(-1.5)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, mins, secs,
        ])
        .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out.null_count(), 0);
        let v = out.value(0);

        assert_eq!(v.months, -12 + (-2)); // -14
        assert_eq!(v.days, -7 + (-1)); // -8

        // -(1h + 1m + 1.5s) en nanos
        let expected_nanos = -((3600_i64 + 60 + 1) * 1_000_000_000 + 500_000_000);
        assert_eq!(v.nanoseconds, expected_nanos);
    }
}
