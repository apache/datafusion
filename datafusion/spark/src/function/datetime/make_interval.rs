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

use arrow::array::{Array, ArrayRef, IntervalMonthDayNanoBuilder, PrimitiveArray};
use arrow::datatypes::DataType::Interval;
use arrow::datatypes::IntervalUnit::MonthDayNano;
use arrow::datatypes::{DataType, Field, FieldRef, IntervalMonthDayNano};
use datafusion_common::config::ConfigOptions;
use datafusion_common::types::{NativeType, logical_float64, logical_int32};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_err, plan_datafusion_err,
};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeInterval {
    signature: Signature,
    /// Mirrors `spark.sql.ansi.enabled` / `enable_ansi_mode`.
    /// When true (failOnError=true in Spark) arithmetic overflow returns an error;
    /// when false (default) it returns NULL instead.
    ansi_mode: bool,
}

impl Default for SparkMakeInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeInterval {
    pub fn new() -> Self {
        Self::new_with_config(&ConfigOptions::default())
    }

    pub fn new_with_config(config: &ConfigOptions) -> Self {
        let int32 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );

        let float64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );

        let variants = vec![
            TypeSignature::Nullary,
            // year
            TypeSignature::Coercible(vec![int32.clone()]),
            // year, month
            TypeSignature::Coercible(vec![int32.clone(), int32.clone()]),
            // year, month, week
            TypeSignature::Coercible(vec![int32.clone(), int32.clone(), int32.clone()]),
            // year, month, week, day
            TypeSignature::Coercible(vec![
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
            ]),
            // year, month, week, day, hour
            TypeSignature::Coercible(vec![
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
            ]),
            // year, month, week, day, hour, minute
            TypeSignature::Coercible(vec![
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
            ]),
            // year, month, week, day, hour, minute, second
            TypeSignature::Coercible(vec![
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                int32.clone(),
                float64.clone(),
            ]),
        ];

        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
            ansi_mode: config.execution.enable_ansi_mode,
        }
    }
}

impl ScalarUDFImpl for SparkMakeInterval {
    fn name(&self) -> &str {
        "make_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // return_field_from_args is the authoritative implementation
        Ok(Interval(MonthDayNano))
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        Some(ScalarUDF::from(Self::new_with_config(config)))
    }

    /// Spark nullability rule (mirrors `failOnError` in Spark source):
    ///   - nullary call → never null (always returns zero interval)
    ///   - ANSI mode on  → nullable only when any input field is nullable
    ///   - ANSI mode off → always nullable (overflow silently produces NULL)
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let ansi_mode = args.config_options.execution.enable_ansi_mode;
        let nullable = if args.arg_fields.is_empty() {
            false
        } else if ansi_mode {
            args.arg_fields.iter().any(|f| f.is_nullable())
        } else {
            true
        };
        Ok(Arc::new(Field::new(
            self.name(),
            Interval(MonthDayNano),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                Some(IntervalMonthDayNano::new(0, 0, 0)),
            )));
        }
        let ansi_mode = self.ansi_mode;
        make_scalar_function(move |cols| make_interval_kernel(cols, ansi_mode), vec![])(
            &args.args,
        )
    }
}

fn make_interval_kernel(
    args: &[ArrayRef],
    ansi_mode: bool,
) -> Result<ArrayRef, DataFusionError> {
    use arrow::array::AsArray;
    use arrow::datatypes::{Float64Type, Int32Type};

    let n_rows = args[0].len();

    let years = args[0]
        .as_primitive_opt::<Int32Type>()
        .ok_or_else(|| plan_datafusion_err!("make_interval arg[0] must be Int32"))?;
    let months = args
        .get(1)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[1] must be Int32")
            })
        })
        .transpose()?;
    let weeks = args
        .get(2)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[2] must be Int32")
            })
        })
        .transpose()?;
    let days: Option<&PrimitiveArray<Int32Type>> = args
        .get(3)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[3] must be Int32")
            })
        })
        .transpose()?;
    let hours: Option<&PrimitiveArray<Int32Type>> = args
        .get(4)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[4] must be Int32")
            })
        })
        .transpose()?;
    let mins: Option<&PrimitiveArray<Int32Type>> = args
        .get(5)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[5] must be Int32")
            })
        })
        .transpose()?;
    let secs: Option<&PrimitiveArray<Float64Type>> = args
        .get(6)
        .map(|a| {
            a.as_primitive_opt::<Float64Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[6] must be Float64")
            })
        })
        .transpose()?;

    let mut builder = IntervalMonthDayNanoBuilder::with_capacity(n_rows);

    for i in 0..n_rows {
        // if one column is NULL → result NULL
        let any_null_present = years.is_null(i)
            || months.as_ref().is_some_and(|a| a.is_null(i))
            || weeks.as_ref().is_some_and(|a| a.is_null(i))
            || days.as_ref().is_some_and(|a| a.is_null(i))
            || hours.as_ref().is_some_and(|a| a.is_null(i))
            || mins.as_ref().is_some_and(|a| a.is_null(i))
            || secs
                .as_ref()
                .is_some_and(|a| a.is_null(i) || !a.value(i).is_finite());

        if any_null_present {
            builder.append_null();
            continue;
        }

        // default values 0 or 0.0
        let y = years.value(i);
        let mo = months.as_ref().map_or(0, |a| a.value(i));
        let w = weeks.as_ref().map_or(0, |a| a.value(i));
        let d = days.as_ref().map_or(0, |a| a.value(i));
        let h = hours.as_ref().map_or(0, |a| a.value(i));
        let mi = mins.as_ref().map_or(0, |a| a.value(i));
        let s = secs.as_ref().map_or(0.0, |a| a.value(i));

        match make_interval_month_day_nano(y, mo, w, d, h, mi, s) {
            Some(v) => builder.append_value(v),
            None => {
                if ansi_mode {
                    return exec_err!(
                        "Arithmetic overflow in make_interval: result does not fit in IntervalMonthDayNano"
                    );
                }
                builder.append_null();
                continue;
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn make_interval_month_day_nano(
    year: i32,
    month: i32,
    week: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: f64,
) -> Option<IntervalMonthDayNano> {
    // checks if overflow
    let months = year.checked_mul(12).and_then(|v| v.checked_add(month))?;
    let total_days = week.checked_mul(7).and_then(|v| v.checked_add(day))?;

    let hours_nanos = (hour as i64).checked_mul(3_600_000_000_000)?;
    let mins_nanos = (min as i64).checked_mul(60_000_000_000)?;

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

    let secs_nanos = sec_int.checked_mul(1_000_000_000)?;

    let total_nanos = hours_nanos
        .checked_add(mins_nanos)
        .and_then(|v| v.checked_add(secs_nanos))
        .and_then(|v| v.checked_add(frac_nanos))?;

    Some(IntervalMonthDayNano::new(months, total_days, total_nanos))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, Int32Array, IntervalMonthDayNanoArray};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{
        Result, assert_eq_or_internal_err, internal_datafusion_err, internal_err,
    };

    use super::*;
    fn run_make_interval_month_day_nano(arrs: Vec<ArrayRef>) -> Result<ArrayRef> {
        make_interval_kernel(&arrs, false)
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
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }

    #[test]
    fn error_months_overflow_should_be_null() {
        // months = year*12 + month → NULL
        let year = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, min, sec,
        ])
        .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }
    #[test]
    fn error_days_overflow_should_be_null() {
        // months = year*12 + month →  NULL
        let year = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, min, sec,
        ])
        .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }
    #[test]
    fn error_min_overflow_should_be_null() {
        let year = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, min, sec,
        ])
        .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
    }
    #[test]
    fn error_sec_overflow_should_be_null() {
        let year = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(f64::MAX)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, min, sec,
        ])
        .unwrap();
        let out = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| internal_datafusion_err!("expected IntervalMonthDayNano"))
            .unwrap();

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
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

    fn invoke_make_interval_with_args(
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<ColumnarValue, DataFusionError> {
        invoke_make_interval_with_config(args, number_rows, &ConfigOptions::default())
    }

    fn invoke_make_interval_with_config(
        args: Vec<ColumnarValue>,
        number_rows: usize,
        config: &ConfigOptions,
    ) -> Result<ColumnarValue, DataFusionError> {
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type(), true).into())
            .collect::<Vec<_>>();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Field::new("f", Interval(MonthDayNano), true).into(),
            config_options: Arc::new(config.clone()),
        };
        SparkMakeInterval::new_with_config(config).invoke_with_args(args)
    }

    #[test]
    fn zero_args_returns_zero_seconds() -> Result<()> {
        let number_rows = 2;
        let res: ColumnarValue = invoke_make_interval_with_args(vec![], number_rows)?;

        match res {
            ColumnarValue::Array(arr) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        internal_datafusion_err!("expected IntervalMonthDayNanoArray")
                    })?;
                assert_eq_or_internal_err!(
                    arr.len(),
                    number_rows,
                    "expected array length {number_rows}"
                );
                for i in 0..number_rows {
                    let iv = arr.value(i);
                    assert_eq_or_internal_err!(
                        (iv.months, iv.days, iv.nanoseconds),
                        (0, 0, 0),
                        "row {i}: expected (0,0,0), got ({},{},{})",
                        iv.months,
                        iv.days,
                        iv.nanoseconds
                    );
                }
            }
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(iv))) => {
                assert_eq_or_internal_err!(
                    (iv.months, iv.days, iv.nanoseconds),
                    (0, 0, 0),
                    "expected scalar 0s, got ({},{},{})",
                    iv.months,
                    iv.days,
                    iv.nanoseconds
                );
            }
            other => {
                return internal_err!(
                    "expected Array or Scalar IntervalMonthDayNano, got {other:?}"
                );
            }
        }

        Ok(())
    }

    // --- nullability / return_field_from_args tests ---

    fn make_ansi_config() -> ConfigOptions {
        let mut cfg = ConfigOptions::default();
        cfg.execution.enable_ansi_mode = true;
        cfg
    }

    #[test]
    fn return_field_nullary_is_not_nullable() {
        let udf = SparkMakeInterval::new();
        let field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[],
                scalar_arguments: &[],
                config_options: &ConfigOptions::default(),
            })
            .unwrap();
        assert!(!field.is_nullable(), "nullary call must not be nullable");
    }

    #[test]
    fn return_field_non_ansi_always_nullable() {
        // Even with all non-null inputs, non-ANSI mode is always nullable
        // because overflow silently returns NULL.
        let udf = SparkMakeInterval::new(); // ansi_mode = false
        let non_null_field: FieldRef = Arc::new(Field::new("x", DataType::Int32, false));
        let field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[non_null_field],
                scalar_arguments: &[None],
                config_options: &ConfigOptions::default(),
            })
            .unwrap();
        assert!(field.is_nullable(), "non-ANSI must always be nullable");
    }

    #[test]
    fn return_field_ansi_mode_not_nullable_when_inputs_not_null() {
        // ANSI mode: no overflow → null; nullable only if inputs are nullable.
        // config_options carries ansi mode to return_field_from_args.
        let udf = SparkMakeInterval::new_with_config(&make_ansi_config());
        let non_null_field: FieldRef = Arc::new(Field::new("x", DataType::Int32, false));
        let field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[non_null_field],
                scalar_arguments: &[None],
                config_options: &make_ansi_config(),
            })
            .unwrap();
        assert!(
            !field.is_nullable(),
            "ANSI mode with non-null inputs must not be nullable"
        );
    }

    #[test]
    fn return_field_ansi_mode_nullable_when_any_input_nullable() {
        let udf = SparkMakeInterval::new_with_config(&make_ansi_config());
        let nullable_field: FieldRef = Arc::new(Field::new("x", DataType::Int32, true));
        let field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[nullable_field],
                scalar_arguments: &[None],
                config_options: &make_ansi_config(),
            })
            .unwrap();
        assert!(
            field.is_nullable(),
            "ANSI mode with nullable inputs must be nullable"
        );
    }

    // --- ANSI mode overflow error tests ---

    #[test]
    fn ansi_mode_overflow_returns_error() {
        let ansi_cfg = make_ansi_config();
        let year = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(i32::MAX)])));
        let month = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1)])));
        let week = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(0)])));
        let day = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(0)])));
        let hour = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(0)])));
        let min = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(0)])));
        let sec = ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(0.0)])));

        let result = invoke_make_interval_with_config(
            vec![year, month, week, day, hour, min, sec],
            1,
            &ansi_cfg,
        );
        assert!(
            result.is_err(),
            "ANSI mode overflow must return an error, not NULL"
        );
    }

    #[test]
    fn non_ansi_overflow_returns_null() {
        // Existing behavior must be preserved: overflow → NULL in non-ANSI mode.
        let year = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;
        let month = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let week = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let day = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let hour = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let min = Arc::new(Int32Array::from(vec![Some(0)])) as ArrayRef;
        let sec = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;

        let out = run_make_interval_month_day_nano(vec![
            year, month, week, day, hour, min, sec,
        ])
        .unwrap();
        assert_eq!(out.null_count(), 1, "non-ANSI overflow must produce NULL");
    }
}
