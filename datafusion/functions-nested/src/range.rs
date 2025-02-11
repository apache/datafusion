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

//! [`ScalarUDFImpl`] definitions for range and gen_series functions.

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, Int64Array, ListArray, ListBuilder};
use arrow::datatypes::{DataType, Field};
use arrow_array::builder::{Date32Builder, TimestampNanosecondBuilder};
use arrow_array::temporal_conversions::as_datetime_with_timezone;
use arrow_array::timezone::Tz;
use arrow_array::types::{
    Date32Type, IntervalMonthDayNanoType, TimestampNanosecondType as TSNT,
};
use arrow_array::{NullArray, TimestampNanosecondArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer};
use arrow_schema::DataType::*;
use arrow_schema::IntervalUnit::MonthDayNano;
use arrow_schema::TimeUnit::Nanosecond;
use datafusion_common::cast::{
    as_date32_array, as_int64_array, as_interval_mdn_array, as_timestamp_nanosecond_array,
};
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, not_impl_datafusion_err, Result,
};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_ARRAY;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use itertools::Itertools;
use std::any::Any;
use std::cmp::Ordering;
use std::iter::from_fn;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

make_udf_expr_and_func!(
    Range,
    range,
    start stop step,
    "create a list of values in the range between start and stop",
    range_udf
);
#[derive(Debug)]
pub(super) struct Range {
    signature: Signature,
    aliases: Vec<String>,
}
impl Range {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}
impl ScalarUDFImpl for Range {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "range"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        arg_types
            .iter()
            .map(|arg_type| match arg_type {
                Null => Ok(Null),
                Int8 => Ok(Int64),
                Int16 => Ok(Int64),
                Int32 => Ok(Int64),
                Int64 => Ok(Int64),
                UInt8 => Ok(Int64),
                UInt16 => Ok(Int64),
                UInt32 => Ok(Int64),
                UInt64 => Ok(Int64),
                Timestamp(_, tz) => Ok(Timestamp(Nanosecond, tz.clone())),
                Date32 => Ok(Date32),
                Date64 => Ok(Date32),
                Utf8 => Ok(Date32),
                LargeUtf8 => Ok(Date32),
                Utf8View => Ok(Date32),
                Interval(_) => Ok(Interval(MonthDayNano)),
                _ => exec_err!("Unsupported DataType"),
            })
            .try_collect()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.iter().any(|t| t.is_null()) {
            Ok(Null)
        } else {
            Ok(List(Arc::new(Field::new(
                "item",
                arg_types[0].clone(),
                true,
            ))))
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.iter().any(|arg| arg.data_type().is_null()) {
            return Ok(ColumnarValue::Array(Arc::new(NullArray::new(1))));
        }
        match args[0].data_type() {
            Int64 => make_scalar_function(|args| gen_range_inner(args, false))(args),
            Date32 => make_scalar_function(|args| gen_range_date(args, false))(args),
            Timestamp(_, _) => {
                make_scalar_function(|args| gen_range_timestamp(args, false))(args)
            }
            dt => {
                exec_err!("unsupported type for RANGE. Expected Int64, Date32 or Timestamp, got: {dt}")
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_range_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_range_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Returns an Arrow array between start and stop with step. The range start..end contains all values with start <= x < end. It is empty if start >= end. Step cannot be 0.",
            )
            .with_syntax_example("range(start, stop, step)")
            .with_sql_example(
                r#"```sql
> select range(2, 10, 3);
+-----------------------------------+
| range(Int64(2),Int64(10),Int64(3))|
+-----------------------------------+
| [2, 5, 8]                         |
+-----------------------------------+

> select range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);
+--------------------------------------------------------------+
| range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH) |
+--------------------------------------------------------------+
| [1992-09-01, 1992-10-01, 1992-11-01, 1992-12-01, 1993-01-01, 1993-02-01] |
+--------------------------------------------------------------+
```"#,
            )
            .with_argument(
                "start",
                "Start of the range. Ints, timestamps, dates or string types that can be coerced to Date32 are supported.",
            )
            .with_argument(
                "end",
                "End of the range (not included). Type must be the same as start.",
            )
            .with_argument(
                "step",
                "Increase by step (cannot be 0). Steps less than a day are supported only for timestamp ranges.",
            )
            .build()
            .unwrap()
    })
}

make_udf_expr_and_func!(
    GenSeries,
    gen_series,
    start stop step,
    "create a list of values in the range between start and stop, include upper bound",
    gen_series_udf
);
#[derive(Debug)]
pub(super) struct GenSeries {
    signature: Signature,
    aliases: Vec<String>,
}
impl GenSeries {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}
impl ScalarUDFImpl for GenSeries {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "generate_series"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        arg_types
            .iter()
            .map(|arg_type| match arg_type {
                Null => Ok(Null),
                Int8 => Ok(Int64),
                Int16 => Ok(Int64),
                Int32 => Ok(Int64),
                Int64 => Ok(Int64),
                UInt8 => Ok(Int64),
                UInt16 => Ok(Int64),
                UInt32 => Ok(Int64),
                UInt64 => Ok(Int64),
                Timestamp(_, tz) => Ok(Timestamp(Nanosecond, tz.clone())),
                Date32 => Ok(Date32),
                Date64 => Ok(Date32),
                Utf8 => Ok(Date32),
                LargeUtf8 => Ok(Date32),
                Utf8View => Ok(Date32),
                Interval(_) => Ok(Interval(MonthDayNano)),
                _ => exec_err!("Unsupported DataType"),
            })
            .try_collect()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.iter().any(|t| t.is_null()) {
            Ok(Null)
        } else {
            Ok(List(Arc::new(Field::new(
                "item",
                arg_types[0].clone(),
                true,
            ))))
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.iter().any(|arg| arg.data_type().is_null()) {
            return Ok(ColumnarValue::Array(Arc::new(NullArray::new(1))));
        }
        match args[0].data_type() {
            Int64 => make_scalar_function(|args| gen_range_inner(args, true))(args),
            Date32 => make_scalar_function(|args| gen_range_date(args, true))(args),
            Timestamp(_, _) => {
                make_scalar_function(|args| gen_range_timestamp(args, true))(args)
            }
            dt => {
                exec_err!(
                    "unsupported type for GENERATE_SERIES. Expected Int64, Date32 or Timestamp, got: {}",
                    dt
                )
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_generate_series_doc())
    }
}

static GENERATE_SERIES_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_generate_series_doc() -> &'static Documentation {
    GENERATE_SERIES_DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_ARRAY)
            .with_description(
                "Similar to the range function, but it includes the upper bound.",
            )
            .with_syntax_example("generate_series(start, stop, step)")
            .with_sql_example(
                r#"```sql
> select generate_series(1,3);
+------------------------------------+
| generate_series(Int64(1),Int64(3)) |
+------------------------------------+
| [1, 2, 3]                          |
+------------------------------------+
```"#,
            )
            .with_argument(
                "start",
                "start of the series. Ints, timestamps, dates or string types that can be coerced to Date32 are supported.",
            )
            .with_argument(
                "end",
                "end of the series (included). Type must be the same as start.",
            )
            .with_argument(
                "step",
                "increase by step (can not be 0). Steps less than a day are supported only for timestamp ranges.",
            )
            .build()
            .unwrap()
    })
}

/// Generates an array of integers from start to stop with a given step.
///
/// This function takes 1 to 3 ArrayRefs as arguments, representing start, stop, and step values.
/// It returns a `Result<ArrayRef>` representing the resulting ListArray after the operation.
///
/// # Arguments
///
/// * `args` - An array of 1 to 3 ArrayRefs representing start, stop, and step(step value can not be zero.) values.
///
/// # Examples
///
/// gen_range(3) => [0, 1, 2]
/// gen_range(1, 4) => [1, 2, 3]
/// gen_range(1, 7, 2) => [1, 3, 5]
pub(super) fn gen_range_inner(
    args: &[ArrayRef],
    include_upper: bool,
) -> Result<ArrayRef> {
    let (start_array, stop_array, step_array) = match args.len() {
        1 => (None, as_int64_array(&args[0])?, None),
        2 => (
            Some(as_int64_array(&args[0])?),
            as_int64_array(&args[1])?,
            None,
        ),
        3 => (
            Some(as_int64_array(&args[0])?),
            as_int64_array(&args[1])?,
            Some(as_int64_array(&args[2])?),
        ),
        _ => return exec_err!("gen_range expects 1 to 3 arguments"),
    };

    let mut values = vec![];
    let mut offsets = vec![0];
    let mut valid = BooleanBufferBuilder::new(stop_array.len());
    for (idx, stop) in stop_array.iter().enumerate() {
        match retrieve_range_args(start_array, stop, step_array, idx) {
            Some((_, _, 0)) => {
                return exec_err!(
                    "step can't be 0 for function {}(start [, stop, step])",
                    if include_upper {
                        "generate_series"
                    } else {
                        "range"
                    }
                );
            }
            Some((start, stop, step)) => {
                // Below, we utilize `usize` to represent steps.
                // On 32-bit targets, the absolute value of `i64` may fail to fit into `usize`.
                let step_abs = usize::try_from(step.unsigned_abs()).map_err(|_| {
                    not_impl_datafusion_err!("step {} can't fit into usize", step)
                })?;
                values.extend(
                    gen_range_iter(start, stop, step < 0, include_upper)
                        .step_by(step_abs),
                );
                offsets.push(values.len() as i32);
                valid.append(true);
            }
            // If any of the arguments is NULL, append a NULL value to the result.
            None => {
                offsets.push(values.len() as i32);
                valid.append(false);
            }
        };
    }
    let arr = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", Int64, true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(Int64Array::from(values)),
        Some(NullBuffer::new(valid.finish())),
    )?);
    Ok(arr)
}

/// Get the (start, stop, step) args for the range and generate_series function.
/// If any of the arguments is NULL, returns None.
fn retrieve_range_args(
    start_array: Option<&Int64Array>,
    stop: Option<i64>,
    step_array: Option<&Int64Array>,
    idx: usize,
) -> Option<(i64, i64, i64)> {
    // Default start value is 0 if not provided
    let start =
        start_array.map_or(Some(0), |arr| arr.is_valid(idx).then(|| arr.value(idx)))?;
    let stop = stop?;
    // Default step value is 1 if not provided
    let step =
        step_array.map_or(Some(1), |arr| arr.is_valid(idx).then(|| arr.value(idx)))?;
    Some((start, stop, step))
}

/// Returns an iterator of i64 values from start to stop
fn gen_range_iter(
    start: i64,
    stop: i64,
    decreasing: bool,
    include_upper: bool,
) -> Box<dyn Iterator<Item = i64>> {
    match (decreasing, include_upper) {
        // Decreasing range, stop is inclusive
        (true, true) => Box::new((stop..=start).rev()),
        // Decreasing range, stop is exclusive
        (true, false) => {
            if stop == i64::MAX {
                // start is never greater than stop, and stop is exclusive,
                // so the decreasing range must be empty.
                Box::new(std::iter::empty())
            } else {
                // Increase the stop value by one to exclude it.
                // Since stop is not i64::MAX, `stop + 1` will not overflow.
                Box::new((stop + 1..=start).rev())
            }
        }
        // Increasing range, stop is inclusive
        (false, true) => Box::new(start..=stop),
        // Increasing range, stop is exclusive
        (false, false) => Box::new(start..stop),
    }
}

fn gen_range_date(args: &[ArrayRef], include_upper_bound: bool) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("arguments length does not match");
    }
    let (start_array, stop_array, step_array) = (
        Some(as_date32_array(&args[0])?),
        as_date32_array(&args[1])?,
        Some(as_interval_mdn_array(&args[2])?),
    );

    // values are date32s
    let values_builder = Date32Builder::new();
    let mut list_builder = ListBuilder::new(values_builder);

    for (idx, stop) in stop_array.iter().enumerate() {
        let mut stop = stop.unwrap_or(0);

        let start = if let Some(start_array_values) = start_array {
            start_array_values.value(idx)
        } else {
            list_builder.append_null();
            continue;
        };

        let step = if let Some(step) = step_array {
            step.value(idx)
        } else {
            list_builder.append_null();
            continue;
        };

        let (months, days, _) = IntervalMonthDayNanoType::to_parts(step);

        if months == 0 && days == 0 {
            return exec_err!("Cannot generate date range less than 1 day.");
        }

        let neg = months < 0 || days < 0;
        if !include_upper_bound {
            stop = Date32Type::subtract_month_day_nano(stop, step);
        }
        let mut new_date = start;

        let values = from_fn(|| {
            if (neg && new_date < stop) || (!neg && new_date > stop) {
                None
            } else {
                let current_date = new_date;
                new_date = Date32Type::add_month_day_nano(new_date, step);
                Some(Some(current_date))
            }
        });

        list_builder.append_value(values);
    }

    let arr = Arc::new(list_builder.finish());

    Ok(arr)
}

fn gen_range_timestamp(args: &[ArrayRef], include_upper_bound: bool) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "Arguments length must be 3 for {}",
            if include_upper_bound {
                "GENERATE_SERIES"
            } else {
                "RANGE"
            }
        );
    }

    // coerce_types fn should coerce all types to Timestamp(Nanosecond, tz)
    let (start_arr, start_tz_opt) = cast_timestamp_arg(&args[0], include_upper_bound)?;
    let (stop_arr, stop_tz_opt) = cast_timestamp_arg(&args[1], include_upper_bound)?;
    let step_arr = as_interval_mdn_array(&args[2])?;
    let start_tz = parse_tz(start_tz_opt)?;
    let stop_tz = parse_tz(stop_tz_opt)?;

    // values are timestamps
    let values_builder = start_tz_opt
        .clone()
        .map_or_else(TimestampNanosecondBuilder::new, |start_tz_str| {
            TimestampNanosecondBuilder::new().with_timezone(start_tz_str)
        });
    let mut list_builder = ListBuilder::new(values_builder);

    for idx in 0..start_arr.len() {
        if start_arr.is_null(idx) || stop_arr.is_null(idx) || step_arr.is_null(idx) {
            list_builder.append_null();
            continue;
        }

        let start = start_arr.value(idx);
        let stop = stop_arr.value(idx);
        let step = step_arr.value(idx);

        let (months, days, ns) = IntervalMonthDayNanoType::to_parts(step);
        if months == 0 && days == 0 && ns == 0 {
            return exec_err!(
                "Interval argument to {} must not be 0",
                if include_upper_bound {
                    "GENERATE_SERIES"
                } else {
                    "RANGE"
                }
            );
        }

        let neg = TSNT::add_month_day_nano(start, step, start_tz)
            .ok_or(exec_datafusion_err!(
                "Cannot generate timestamp range where start + step overflows"
            ))?
            .cmp(&start)
            == Ordering::Less;

        let stop_dt = as_datetime_with_timezone::<TSNT>(stop, stop_tz).ok_or(
            exec_datafusion_err!(
                "Cannot generate timestamp for stop: {}: {:?}",
                stop,
                stop_tz
            ),
        )?;

        let mut current = start;
        let mut current_dt = as_datetime_with_timezone::<TSNT>(current, start_tz).ok_or(
            exec_datafusion_err!(
                "Cannot generate timestamp for start: {}: {:?}",
                current,
                start_tz
            ),
        )?;

        let values = from_fn(|| {
            if (include_upper_bound
                && ((neg && current_dt < stop_dt) || (!neg && current_dt > stop_dt)))
                || (!include_upper_bound
                    && ((neg && current_dt <= stop_dt)
                        || (!neg && current_dt >= stop_dt)))
            {
                return None;
            }

            let prev_current = current;

            if let Some(ts) = TSNT::add_month_day_nano(current, step, start_tz) {
                current = ts;
                current_dt = as_datetime_with_timezone::<TSNT>(current, start_tz)?;

                Some(Some(prev_current))
            } else {
                // we failed to parse the timestamp here so terminate the series
                None
            }
        });

        list_builder.append_value(values);
    }

    let arr = Arc::new(list_builder.finish());

    Ok(arr)
}

fn cast_timestamp_arg(
    arg: &ArrayRef,
    include_upper: bool,
) -> Result<(&TimestampNanosecondArray, &Option<Arc<str>>)> {
    match arg.data_type() {
        Timestamp(Nanosecond, tz_opt) => {
            Ok((as_timestamp_nanosecond_array(arg)?, tz_opt))
        }
        _ => {
            internal_err!(
                "Unexpected argument type for {} : {}",
                if include_upper {
                    "GENERATE_SERIES"
                } else {
                    "RANGE"
                },
                arg.data_type()
            )
        }
    }
}

fn parse_tz(tz: &Option<Arc<str>>) -> Result<Tz> {
    let tz = tz.as_ref().map_or_else(|| "+00", |s| s);

    Tz::from_str(tz)
        .map_err(|op| exec_datafusion_err!("failed to parse timezone {tz}: {:?}", op))
}
