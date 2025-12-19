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
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::TimeUnit;
use arrow::datatypes::{DataType, Field, IntervalUnit::MonthDayNano};
use arrow::{
    array::{
        Array, ArrayRef, Int64Array, ListArray, ListBuilder, NullBufferBuilder,
        builder::{Date32Builder, TimestampNanosecondBuilder},
        temporal_conversions::as_datetime_with_timezone,
        timezone::Tz,
        types::{Date32Type, IntervalMonthDayNanoType, TimestampNanosecondType},
    },
    compute::cast,
};
use datafusion_common::internal_err;
use datafusion_common::{
    Result, exec_datafusion_err, exec_err, not_impl_datafusion_err,
    utils::take_function_args,
};
use datafusion_common::{
    ScalarValue,
    cast::{
        as_date32_array, as_int64_array, as_interval_mdn_array,
        as_timestamp_nanosecond_array,
    },
    types::{
        NativeType, logical_date, logical_int64, logical_interval_mdn, logical_string,
    },
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::cmp::Ordering;
use std::iter::from_fn;
use std::str::FromStr;
use std::sync::Arc;

make_udf_expr_and_func!(
    Range,
    range,
    start stop step,
    "create a list of values in the range between start and stop",
    range_udf,
    Range::new
);

make_udf_expr_and_func!(
    GenSeries,
    gen_series,
    start stop step,
    "create a list of values in the range between start and stop, include upper bound",
    gen_series_udf,
    Range::generate_series
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an Arrow array between start and stop with step. The range start..end contains all values with start <= x < end. It is empty if start >= end. Step cannot be 0.",
    syntax_example = "range(stop)
range(start, stop[, step])",
    sql_example = r#"```sql
> select range(2, 10, 3);
+-----------------------------------+
| range(Int64(2),Int64(10),Int64(3))|
+-----------------------------------+
| [2, 5, 8]                         |
+-----------------------------------+

> select range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);
+--------------------------------------------------------------------------+
| range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH)          |
+--------------------------------------------------------------------------+
| [1992-09-01, 1992-10-01, 1992-11-01, 1992-12-01, 1993-01-01, 1993-02-01] |
+--------------------------------------------------------------------------+
```"#,
    argument(
        name = "start",
        description = "Start of the range. Ints, timestamps, dates or string types that can be coerced to Date32 are supported."
    ),
    argument(
        name = "end",
        description = "End of the range (not included). Type must be the same as start."
    ),
    argument(
        name = "step",
        description = "Increase by step (cannot be 0). Steps less than a day are supported only for timestamp ranges."
    )
)]
struct RangeDoc {}

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Similar to the range function, but it includes the upper bound.",
    syntax_example = "generate_series(stop)
generate_series(start, stop[, step])",
    sql_example = r#"```sql
> select generate_series(1,3);
+------------------------------------+
| generate_series(Int64(1),Int64(3)) |
+------------------------------------+
| [1, 2, 3]                          |
+------------------------------------+
```"#,
    argument(
        name = "start",
        description = "Start of the series. Ints, timestamps, dates or string types that can be coerced to Date32 are supported."
    ),
    argument(
        name = "end",
        description = "End of the series (included). Type must be the same as start."
    ),
    argument(
        name = "step",
        description = "Increase by step (can not be 0). Steps less than a day are supported only for timestamp ranges."
    )
)]
struct GenerateSeriesDoc {}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Range {
    signature: Signature,
    /// `false` for range, `true` for generate_series
    include_upper_bound: bool,
}

impl Default for Range {
    fn default() -> Self {
        Self::new()
    }
}

impl Range {
    fn defined_signature() -> Signature {
        // We natively only support i64 in our implementation; so ensure we cast other integer
        // types to it.
        let integer = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int64,
        );
        // We natively only support mdn in our implementation; so ensure we cast other interval
        // types to it.
        let interval = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_interval_mdn()),
            vec![TypeSignatureClass::Interval],
            NativeType::Interval(MonthDayNano),
        );
        // Ideally we'd limit to only Date32 & Timestamp(Nanoseconds) as those are the implementations
        // we have but that is difficult to do with this current API; we'll cast later on to
        // handle such types.
        let date = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_date()),
            vec![TypeSignatureClass::Native(logical_string())],
            NativeType::Date,
        );
        let timestamp = Coercion::new_exact(TypeSignatureClass::Timestamp);
        Signature::one_of(
            vec![
                // Integer ranges
                // Stop
                TypeSignature::Coercible(vec![integer.clone()]),
                // Start & stop
                TypeSignature::Coercible(vec![integer.clone(), integer.clone()]),
                // Start, stop & step
                TypeSignature::Coercible(vec![integer.clone(), integer.clone(), integer]),
                // Date range
                TypeSignature::Coercible(vec![date.clone(), date, interval.clone()]),
                // Timestamp range
                TypeSignature::Coercible(vec![timestamp.clone(), timestamp, interval]),
            ],
            Volatility::Immutable,
        )
    }

    /// Generate `range()` function which excludes upper bound.
    pub fn new() -> Self {
        Self {
            signature: Self::defined_signature(),
            include_upper_bound: false,
        }
    }

    /// Generate `generate_series()` function which includes upper bound.
    fn generate_series() -> Self {
        Self {
            signature: Self::defined_signature(),
            include_upper_bound: true,
        }
    }
}

impl ScalarUDFImpl for Range {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.include_upper_bound {
            "generate_series"
        } else {
            "range"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.iter().any(|t| t.is_null()) {
            return Ok(DataType::Null);
        }

        match (&arg_types[0], arg_types.get(1)) {
            // In implementation we downcast to Date32 so ensure reflect that here
            (_, Some(DataType::Date64)) | (DataType::Date64, _) => Ok(DataType::List(
                Arc::new(Field::new_list_field(DataType::Date32, true)),
            )),
            // Ensure we preserve timezone
            (DataType::Timestamp(_, tz), _) => {
                Ok(DataType::List(Arc::new(Field::new_list_field(
                    DataType::Timestamp(TimeUnit::Nanosecond, tz.to_owned()),
                    true,
                ))))
            }
            _ => Ok(DataType::List(Arc::new(Field::new_list_field(
                arg_types[0].clone(),
                true,
            )))),
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;

        if args.iter().any(|arg| arg.data_type().is_null()) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        match args[0].data_type() {
            DataType::Int64 => {
                make_scalar_function(|args| self.gen_range_inner(args))(args)
            }
            DataType::Date32 | DataType::Date64 => {
                make_scalar_function(|args| self.gen_range_date(args))(args)
            }
            DataType::Timestamp(_, _) => {
                make_scalar_function(|args| self.gen_range_timestamp(args))(args)
            }
            dt => {
                internal_err!(
                    "Signature failed to guard unknown input type for {}: {dt}",
                    self.name()
                )
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        if self.include_upper_bound {
            GenerateSeriesDoc {}.doc()
        } else {
            RangeDoc {}.doc()
        }
    }
}

impl Range {
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
    fn gen_range_inner(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let (start_array, stop_array, step_array) = match args {
            [stop_array] => (None, as_int64_array(stop_array)?, None),
            [start_array, stop_array] => (
                Some(as_int64_array(start_array)?),
                as_int64_array(stop_array)?,
                None,
            ),
            [start_array, stop_array, step_array] => (
                Some(as_int64_array(start_array)?),
                as_int64_array(stop_array)?,
                Some(as_int64_array(step_array)?),
            ),
            _ => return internal_err!("{} expects 1 to 3 arguments", self.name()),
        };

        let mut values = vec![];
        let mut offsets = vec![0];
        let mut valid = NullBufferBuilder::new(stop_array.len());
        for (idx, stop) in stop_array.iter().enumerate() {
            match retrieve_range_args(start_array, stop, step_array, idx) {
                Some((_, _, 0)) => {
                    return exec_err!(
                        "step can't be 0 for function {}(start [, stop, step])",
                        self.name()
                    );
                }
                Some((start, stop, step)) => {
                    // Below, we utilize `usize` to represent steps.
                    // On 32-bit targets, the absolute value of `i64` may fail to fit into `usize`.
                    let step_abs =
                        usize::try_from(step.unsigned_abs()).map_err(|_| {
                            not_impl_datafusion_err!("step {} can't fit into usize", step)
                        })?;
                    values.extend(
                        gen_range_iter(start, stop, step < 0, self.include_upper_bound)
                            .step_by(step_abs),
                    );
                    offsets.push(values.len() as i32);
                    valid.append_non_null();
                }
                // If any of the arguments is NULL, append a NULL value to the result.
                None => {
                    offsets.push(values.len() as i32);
                    valid.append_null();
                }
            };
        }
        let arr = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(Int64Array::from(values)),
            valid.finish(),
        )?);
        Ok(arr)
    }

    fn gen_range_date(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [start, stop, step] = take_function_args(self.name(), args)?;
        let step = as_interval_mdn_array(step)?;

        // Signature can only guarantee we get a date type, not specifically
        // date32 so handle potential cast from date64 here.
        let start = cast(start, &DataType::Date32)?;
        let start = as_date32_array(&start)?;
        let stop = cast(stop, &DataType::Date32)?;
        let stop = as_date32_array(&stop)?;

        // values are date32s
        let values_builder = Date32Builder::new();
        let mut list_builder = ListBuilder::new(values_builder);

        for idx in 0..stop.len() {
            if start.is_null(idx) || stop.is_null(idx) || step.is_null(idx) {
                list_builder.append_null();
                continue;
            }

            let start = start.value(idx);
            let stop = stop.value(idx);
            let step = step.value(idx);

            let (months, days, _) = IntervalMonthDayNanoType::to_parts(step);
            if months == 0 && days == 0 {
                return exec_err!("Cannot generate date range less than 1 day.");
            }

            let stop = if !self.include_upper_bound {
                Date32Type::subtract_month_day_nano(stop, step)
            } else {
                stop
            };

            let neg = months < 0 || days < 0;
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

    fn gen_range_timestamp(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let [start, stop, step] = take_function_args(self.name(), args)?;
        let step = as_interval_mdn_array(step)?;

        // Signature can only guarantee we get a timestamp type, not specifically
        // timestamp(ns) so handle potential cast from other timestamps here.
        fn cast_to_ns(arr: &ArrayRef) -> Result<ArrayRef> {
            match arr.data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Arc::clone(arr)),
                DataType::Timestamp(_, tz) => Ok(cast(
                    arr,
                    &DataType::Timestamp(TimeUnit::Nanosecond, tz.to_owned()),
                )?),
                _ => unreachable!(),
            }
        }
        let start = cast_to_ns(start)?;
        let start = as_timestamp_nanosecond_array(&start)?;
        let stop = cast_to_ns(stop)?;
        let stop = as_timestamp_nanosecond_array(&stop)?;

        let start_tz = parse_tz(&start.timezone())?;
        let stop_tz = parse_tz(&stop.timezone())?;

        // values are timestamps
        let values_builder = start
            .timezone()
            .map_or_else(TimestampNanosecondBuilder::new, |start_tz_str| {
                TimestampNanosecondBuilder::new().with_timezone(start_tz_str)
            });
        let mut list_builder = ListBuilder::new(values_builder);

        for idx in 0..start.len() {
            if start.is_null(idx) || stop.is_null(idx) || step.is_null(idx) {
                list_builder.append_null();
                continue;
            }

            let start = start.value(idx);
            let stop = stop.value(idx);
            let step = step.value(idx);

            let (months, days, ns) = IntervalMonthDayNanoType::to_parts(step);
            if months == 0 && days == 0 && ns == 0 {
                return exec_err!("Interval argument to {} must not be 0", self.name());
            }

            let neg = TimestampNanosecondType::add_month_day_nano(start, step, start_tz)
                .ok_or(exec_datafusion_err!(
                    "Cannot generate timestamp range where start + step overflows"
                ))?
                .cmp(&start)
                == Ordering::Less;

            let stop_dt =
                as_datetime_with_timezone::<TimestampNanosecondType>(stop, stop_tz)
                    .ok_or(exec_datafusion_err!(
                        "Cannot generate timestamp for stop: {}: {:?}",
                        stop,
                        stop_tz
                    ))?;

            let mut current = start;
            let mut current_dt =
                as_datetime_with_timezone::<TimestampNanosecondType>(current, start_tz)
                    .ok_or(exec_datafusion_err!(
                    "Cannot generate timestamp for start: {}: {:?}",
                    current,
                    start_tz
                ))?;

            let values = from_fn(|| {
                let generate_series_should_end = self.include_upper_bound
                    && ((neg && current_dt < stop_dt) || (!neg && current_dt > stop_dt));
                let range_should_end = !self.include_upper_bound
                    && ((neg && current_dt <= stop_dt)
                        || (!neg && current_dt >= stop_dt));
                if generate_series_should_end || range_should_end {
                    return None;
                }

                let prev_current = current;

                if let Some(ts) =
                    TimestampNanosecondType::add_month_day_nano(current, step, start_tz)
                {
                    current = ts;
                    current_dt = as_datetime_with_timezone::<TimestampNanosecondType>(
                        current, start_tz,
                    )?;

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

fn parse_tz(tz: &Option<&str>) -> Result<Tz> {
    let tz = tz.unwrap_or_else(|| "+00");

    Tz::from_str(tz)
        .map_err(|op| exec_datafusion_err!("failed to parse timezone {tz}: {:?}", op))
}
