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

use arrow::array::{
    Array, ArrayRef, AsArray, DurationMicrosecondBuilder, PrimitiveArray,
};
use arrow::datatypes::TimeUnit::Microsecond;
use arrow::datatypes::{DataType, Float64Type, Int32Type};
use datafusion_common::{
    exec_err, plan_datafusion_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeDtInterval {
    signature: Signature,
}

impl Default for SparkMakeDtInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeDtInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeDtInterval {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_dt_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Note the return type is `DataType::Duration(TimeUnit::Microsecond)` and not `DataType::Interval(DayTime)` as you might expect.
    /// This is because `DataType::Interval(DayTime)` has precision only to the millisecond, whilst Spark's `DayTimeIntervalType` has
    /// precision to the microsecond. We use `DataType::Duration(TimeUnit::Microsecond)` in order to not lose any precision. See the
    /// [Sail compatibility doc] for reference.
    ///
    /// [Sail compatibility doc]: https://github.com/lakehq/sail/blob/dc5368daa24d40a7758a299e1ba8fc985cb29108/docs/guide/dataframe/data-types/compatibility.md?plain=1#L260
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Duration(Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::DurationMicrosecond(
                Some(0),
            )));
        }
        make_scalar_function(make_dt_interval_kernel, vec![])(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() > 4 {
            return exec_err!(
                "make_dt_interval expects between 0 and 4 arguments, got {}",
                arg_types.len()
            );
        }

        Ok((0..arg_types.len())
            .map(|i| {
                if i == 3 {
                    DataType::Float64
                } else {
                    DataType::Int32
                }
            })
            .collect())
    }
}

fn make_dt_interval_kernel(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    let n_rows = args[0].len();
    let days = args[0]
        .as_primitive_opt::<Int32Type>()
        .ok_or_else(|| plan_datafusion_err!("make_dt_interval arg[0] must be Int32"))?;
    let hours: Option<&PrimitiveArray<Int32Type>> = args
        .get(1)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[1] must be Int32")
            })
        })
        .transpose()?;
    let mins: Option<&PrimitiveArray<Int32Type>> = args
        .get(2)
        .map(|a| {
            a.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[2] must be Int32")
            })
        })
        .transpose()?;
    let secs: Option<&PrimitiveArray<Float64Type>> = args
        .get(3)
        .map(|a| {
            a.as_primitive_opt::<Float64Type>().ok_or_else(|| {
                plan_datafusion_err!("make_dt_interval arg[3] must be Float64")
            })
        })
        .transpose()?;
    let mut builder = DurationMicrosecondBuilder::with_capacity(n_rows);

    for i in 0..n_rows {
        // if one column is NULL → result NULL
        let any_null_present = days.is_null(i)
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
        let d = days.value(i);
        let h = hours.as_ref().map_or(0, |a| a.value(i));
        let mi = mins.as_ref().map_or(0, |a| a.value(i));
        let s = secs.as_ref().map_or(0.0, |a| a.value(i));

        match make_interval_dt_nano(d, h, mi, s) {
            Some(v) => builder.append_value(v),
            None => {
                builder.append_null();
                continue;
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}
fn make_interval_dt_nano(day: i32, hour: i32, min: i32, sec: f64) -> Option<i64> {
    const HOURS_PER_DAY: i32 = 24;
    const MINS_PER_HOUR: i32 = 60;
    const SECS_PER_MINUTE: i64 = 60;
    const MICROS_PER_SEC: i64 = 1_000_000;

    let total_hours: i32 = day
        .checked_mul(HOURS_PER_DAY)
        .and_then(|v| v.checked_add(hour))?;

    let total_mins: i32 = total_hours
        .checked_mul(MINS_PER_HOUR)
        .and_then(|v| v.checked_add(min))?;

    let mut sec_whole: i64 = sec.trunc() as i64;
    let sec_frac: f64 = sec - (sec_whole as f64);
    let mut frac_us: i64 = (sec_frac * (MICROS_PER_SEC as f64)).round() as i64;

    if frac_us.abs() >= MICROS_PER_SEC {
        if frac_us > 0 {
            frac_us -= MICROS_PER_SEC;
            sec_whole = sec_whole.checked_add(1)?;
        } else {
            frac_us += MICROS_PER_SEC;
            sec_whole = sec_whole.checked_sub(1)?;
        }
    }

    let total_secs: i64 = (total_mins as i64)
        .checked_mul(SECS_PER_MINUTE)
        .and_then(|v| v.checked_add(sec_whole))?;

    let total_us = total_secs
        .checked_mul(MICROS_PER_SEC)
        .and_then(|v| v.checked_add(frac_us))?;

    Some(total_us)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{DurationMicrosecondArray, Float64Array, Int32Array};
    use arrow::datatypes::DataType::Duration;
    use arrow::datatypes::Field;
    use arrow::datatypes::TimeUnit::Microsecond;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

    use super::*;

    fn run_make_dt_interval(arrs: Vec<ArrayRef>) -> Result<ArrayRef> {
        make_dt_interval_kernel(&arrs)
    }

    #[test]
    fn nulls_propagate_per_row() -> Result<()> {
        let days = Arc::new(Int32Array::from(vec![
            None,
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
        ])) as ArrayRef;

        let hours = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
        ])) as ArrayRef;

        let mins = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
            Some(6),
            Some(7),
        ])) as ArrayRef;

        let secs = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            None,
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ])) as ArrayRef;

        let out = run_make_dt_interval(vec![days, hours, mins, secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected DurationMicrosecondArray".into())
            })?;

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }
        Ok(())
    }

    #[test]
    fn error_months_overflow_should_be_null() -> Result<()> {
        // months = year*12 + month → NULL

        let days = Arc::new(Int32Array::from(vec![Some(i32::MAX)])) as ArrayRef;

        let hours = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;

        let mins = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;

        let secs = Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef;

        let out = run_make_dt_interval(vec![days, hours, mins, secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected DurationMicrosecondArray".into())
            })?;

        for i in 0..out.len() {
            assert!(out.is_null(i), "row {i} should be NULL");
        }

        Ok(())
    }

    fn invoke_make_dt_interval_with_args(
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<ColumnarValue, DataFusionError> {
        let arg_fields = args
            .iter()
            .map(|arg| Field::new("a", arg.data_type(), true).into())
            .collect::<Vec<_>>();
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Field::new("f", Duration(Microsecond), true).into(),
            config_options: Arc::new(Default::default()),
        };
        SparkMakeDtInterval::new().invoke_with_args(args)
    }

    #[test]
    fn zero_args_returns_zero_duration() -> Result<()> {
        let number_rows: usize = 3;

        let res: ColumnarValue = invoke_make_dt_interval_with_args(vec![], number_rows)?;
        let arr = res.into_array(number_rows)?;
        let arr = arr
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected DurationMicrosecondArray".into())
            })?;

        assert_eq!(arr.len(), number_rows);
        for i in 0..number_rows {
            assert!(!arr.is_null(i));
            assert_eq!(arr.value(i), 0_i64);
        }
        Ok(())
    }

    #[test]
    fn one_day_minus_24_hours_equals_zero() -> Result<()> {
        let arr_days = Arc::new(Int32Array::from(vec![Some(1), Some(-1)])) as ArrayRef;
        let arr_hours = Arc::new(Int32Array::from(vec![Some(-24), Some(24)])) as ArrayRef;
        let arr_mins = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let arr_secs =
            Arc::new(Float64Array::from(vec![Some(0.0), Some(0.0)])) as ArrayRef;

        let out = run_make_dt_interval(vec![arr_days, arr_hours, arr_mins, arr_secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected DurationMicrosecondArray".into())
            })?;

        assert_eq!(out.len(), 2);
        assert_eq!(out.null_count(), 0);
        assert_eq!(out.value(0), 0_i64);
        assert_eq!(out.value(1), 0_i64);
        Ok(())
    }

    #[test]
    fn one_hour_minus_60_mins_equals_zero() -> Result<()> {
        let arr_days = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let arr_hours = Arc::new(Int32Array::from(vec![Some(-1), Some(1)])) as ArrayRef;
        let arr_mins = Arc::new(Int32Array::from(vec![Some(60), Some(-60)])) as ArrayRef;
        let arr_secs =
            Arc::new(Float64Array::from(vec![Some(0.0), Some(0.0)])) as ArrayRef;

        let out = run_make_dt_interval(vec![arr_days, arr_hours, arr_mins, arr_secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected DurationMicrosecondArray".into())
            })?;

        assert_eq!(out.len(), 2);
        assert_eq!(out.null_count(), 0);
        assert_eq!(out.value(0), 0_i64);
        assert_eq!(out.value(1), 0_i64);
        Ok(())
    }

    #[test]
    fn one_mins_minus_60_secs_equals_zero() -> Result<()> {
        let arr_days = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let arr_hours = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let arr_mins = Arc::new(Int32Array::from(vec![Some(-1), Some(1)])) as ArrayRef;
        let arr_secs =
            Arc::new(Float64Array::from(vec![Some(60.0), Some(-60.0)])) as ArrayRef;

        let out = run_make_dt_interval(vec![arr_days, arr_hours, arr_mins, arr_secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected DurationMicrosecondArray".into())
            })?;

        assert_eq!(out.len(), 2);
        assert_eq!(out.null_count(), 0);
        assert_eq!(out.value(0), 0_i64);
        assert_eq!(out.value(1), 0_i64);
        Ok(())
    }

    #[test]
    fn frac_carries_up_to_next_second_positive() -> Result<()> {
        // 0.9999995s → 1_000_000 µs (carry a +1s)
        let days = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let hours = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let mins = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let secs = Arc::new(Float64Array::from(vec![
            Some(0.999_999_5),
            Some(0.999_999_4),
        ])) as ArrayRef;

        let out = run_make_dt_interval(vec![days, hours, mins, secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected DurationMicrosecondArray".into())
            })?;

        assert_eq!(out.len(), 2);
        assert_eq!(out.value(0), 1_000_000);
        assert_eq!(out.value(1), 999_999);
        Ok(())
    }

    #[test]
    fn frac_carries_down_to_prev_second_negative() -> Result<()> {
        // -0.9999995s → -1_000_000 µs (carry a −1s)
        let days = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let hours = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let mins = Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef;
        let secs = Arc::new(Float64Array::from(vec![
            Some(-0.999_999_5),
            Some(-0.999_999_4),
        ])) as ArrayRef;

        let out = run_make_dt_interval(vec![days, hours, mins, secs])?;
        let out = out
            .as_any()
            .downcast_ref::<DurationMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("expected DurationMicrosecondArray".into())
            })?;

        assert_eq!(out.len(), 2);
        assert_eq!(out.value(0), -1_000_000);
        assert_eq!(out.value(1), -999_999);
        Ok(())
    }

    #[test]
    fn no_more_than_4_params() -> Result<()> {
        let udf = SparkMakeDtInterval::new();

        let arg_types = vec![
            DataType::Int32,
            DataType::Int32,
            DataType::Int32,
            DataType::Float64,
            DataType::Int32,
        ];

        let res = udf.coerce_types(&arg_types);

        assert!(
            matches!(res, Err(DataFusionError::Execution(_))),
            "make_dt_interval expects between 0 and 4 arguments, got 5"
        );

        Ok(())
    }
}
