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

//! implementation kernels for array functions

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array,
    GenericListArray, Int16Array, Int32Array, Int64Array, Int8Array, LargeListArray,
    LargeStringArray, ListArray, ListBuilder, OffsetSizeTrait, StringArray,
    StringBuilder, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::{
    DataType, Date32Type, Field, IntervalMonthDayNanoType, UInt64Type,
};
use arrow::row::{RowConverter, SortField};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer};
use arrow_schema::FieldRef;
use arrow_schema::SortOptions;

use datafusion_common::cast::{
    as_date32_array, as_generic_list_array, as_generic_string_array, as_int64_array,
    as_interval_mdn_array, as_large_list_array, as_list_array, as_null_array,
    as_string_array,
};
use datafusion_common::{
    exec_err, internal_err, not_impl_datafusion_err, DataFusionError, Result,
};
use itertools::Itertools;
use std::any::type_name;
use std::sync::Arc;

macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast to {}",
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

macro_rules! to_string {
    ($ARG:expr, $ARRAY:expr, $DELIMITER:expr, $NULL_STRING:expr, $WITH_NULL_STRING:expr, $ARRAY_TYPE:ident) => {{
        let arr = downcast_arg!($ARRAY, $ARRAY_TYPE);
        for x in arr {
            match x {
                Some(x) => {
                    $ARG.push_str(&x.to_string());
                    $ARG.push_str($DELIMITER);
                }
                None => {
                    if $WITH_NULL_STRING {
                        $ARG.push_str($NULL_STRING);
                        $ARG.push_str($DELIMITER);
                    }
                }
            }
        }
        Ok($ARG)
    }};
}

macro_rules! call_array_function {
    ($DATATYPE:expr, false) => {
        match $DATATYPE {
            DataType::Utf8 => array_function!(StringArray),
            DataType::LargeUtf8 => array_function!(LargeStringArray),
            DataType::Boolean => array_function!(BooleanArray),
            DataType::Float32 => array_function!(Float32Array),
            DataType::Float64 => array_function!(Float64Array),
            DataType::Int8 => array_function!(Int8Array),
            DataType::Int16 => array_function!(Int16Array),
            DataType::Int32 => array_function!(Int32Array),
            DataType::Int64 => array_function!(Int64Array),
            DataType::UInt8 => array_function!(UInt8Array),
            DataType::UInt16 => array_function!(UInt16Array),
            DataType::UInt32 => array_function!(UInt32Array),
            DataType::UInt64 => array_function!(UInt64Array),
            _ => unreachable!(),
        }
    };
    ($DATATYPE:expr, $INCLUDE_LIST:expr) => {{
        match $DATATYPE {
            DataType::List(_) => array_function!(ListArray),
            DataType::Utf8 => array_function!(StringArray),
            DataType::LargeUtf8 => array_function!(LargeStringArray),
            DataType::Boolean => array_function!(BooleanArray),
            DataType::Float32 => array_function!(Float32Array),
            DataType::Float64 => array_function!(Float64Array),
            DataType::Int8 => array_function!(Int8Array),
            DataType::Int16 => array_function!(Int16Array),
            DataType::Int32 => array_function!(Int32Array),
            DataType::Int64 => array_function!(Int64Array),
            DataType::UInt8 => array_function!(UInt8Array),
            DataType::UInt16 => array_function!(UInt16Array),
            DataType::UInt32 => array_function!(UInt32Array),
            DataType::UInt64 => array_function!(UInt64Array),
            _ => unreachable!(),
        }
    }};
}

/// Array_to_string SQL function
pub(super) fn array_to_string(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_to_string expects two or three arguments");
    }

    let arr = &args[0];

    let delimiters = as_string_array(&args[1])?;
    let delimiters: Vec<Option<&str>> = delimiters.iter().collect();

    let mut null_string = String::from("");
    let mut with_null_string = false;
    if args.len() == 3 {
        null_string = as_string_array(&args[2])?.value(0).to_string();
        with_null_string = true;
    }

    fn compute_array_to_string(
        arg: &mut String,
        arr: ArrayRef,
        delimiter: String,
        null_string: String,
        with_null_string: bool,
    ) -> datafusion_common::Result<&mut String> {
        match arr.data_type() {
            DataType::List(..) => {
                let list_array = as_list_array(&arr)?;
                for i in 0..list_array.len() {
                    compute_array_to_string(
                        arg,
                        list_array.value(i),
                        delimiter.clone(),
                        null_string.clone(),
                        with_null_string,
                    )?;
                }

                Ok(arg)
            }
            DataType::LargeList(..) => {
                let list_array = as_large_list_array(&arr)?;
                for i in 0..list_array.len() {
                    compute_array_to_string(
                        arg,
                        list_array.value(i),
                        delimiter.clone(),
                        null_string.clone(),
                        with_null_string,
                    )?;
                }

                Ok(arg)
            }
            DataType::Null => Ok(arg),
            data_type => {
                macro_rules! array_function {
                    ($ARRAY_TYPE:ident) => {
                        to_string!(
                            arg,
                            arr,
                            &delimiter,
                            &null_string,
                            with_null_string,
                            $ARRAY_TYPE
                        )
                    };
                }
                call_array_function!(data_type, false)
            }
        }
    }

    fn generate_string_array<O: OffsetSizeTrait>(
        list_arr: &GenericListArray<O>,
        delimiters: Vec<Option<&str>>,
        null_string: String,
        with_null_string: bool,
    ) -> datafusion_common::Result<StringArray> {
        let mut res: Vec<Option<String>> = Vec::new();
        for (arr, &delimiter) in list_arr.iter().zip(delimiters.iter()) {
            if let (Some(arr), Some(delimiter)) = (arr, delimiter) {
                let mut arg = String::from("");
                let s = compute_array_to_string(
                    &mut arg,
                    arr,
                    delimiter.to_string(),
                    null_string.clone(),
                    with_null_string,
                )?
                .clone();

                if let Some(s) = s.strip_suffix(delimiter) {
                    res.push(Some(s.to_string()));
                } else {
                    res.push(Some(s));
                }
            } else {
                res.push(None);
            }
        }

        Ok(StringArray::from(res))
    }

    let arr_type = arr.data_type();
    let string_arr = match arr_type {
        DataType::List(_) | DataType::FixedSizeList(_, _) => {
            let list_array = as_list_array(&arr)?;
            generate_string_array::<i32>(
                list_array,
                delimiters,
                null_string,
                with_null_string,
            )?
        }
        DataType::LargeList(_) => {
            let list_array = as_large_list_array(&arr)?;
            generate_string_array::<i64>(
                list_array,
                delimiters,
                null_string,
                with_null_string,
            )?
        }
        _ => {
            let mut arg = String::from("");
            let mut res: Vec<Option<String>> = Vec::new();
            // delimiter length is 1
            assert_eq!(delimiters.len(), 1);
            let delimiter = delimiters[0].unwrap();
            let s = compute_array_to_string(
                &mut arg,
                arr.clone(),
                delimiter.to_string(),
                null_string,
                with_null_string,
            )?
            .clone();

            if !s.is_empty() {
                let s = s.strip_suffix(delimiter).unwrap().to_string();
                res.push(Some(s));
            } else {
                res.push(Some(s));
            }
            StringArray::from(res)
        }
    };

    Ok(Arc::new(string_arr))
}

/// Splits string at occurrences of delimiter and returns an array of parts
/// string_to_array('abc~@~def~@~ghi', '~@~') = '["abc", "def", "ghi"]'
pub fn string_to_array<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("string_to_array expects two or three arguments");
    }
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let delimiter_array = as_generic_string_array::<T>(&args[1])?;

    let mut list_builder = ListBuilder::new(StringBuilder::with_capacity(
        string_array.len(),
        string_array.get_buffer_memory_size(),
    ));

    match args.len() {
        2 => {
            string_array.iter().zip(delimiter_array.iter()).for_each(
                |(string, delimiter)| {
                    match (string, delimiter) {
                        (Some(string), Some("")) => {
                            list_builder.values().append_value(string);
                            list_builder.append(true);
                        }
                        (Some(string), Some(delimiter)) => {
                            string.split(delimiter).for_each(|s| {
                                list_builder.values().append_value(s);
                            });
                            list_builder.append(true);
                        }
                        (Some(string), None) => {
                            string.chars().map(|c| c.to_string()).for_each(|c| {
                                list_builder.values().append_value(c);
                            });
                            list_builder.append(true);
                        }
                        _ => list_builder.append(false), // null value
                    }
                },
            );
        }

        3 => {
            let null_value_array = as_generic_string_array::<T>(&args[2])?;
            string_array
                .iter()
                .zip(delimiter_array.iter())
                .zip(null_value_array.iter())
                .for_each(|((string, delimiter), null_value)| {
                    match (string, delimiter) {
                        (Some(string), Some("")) => {
                            if Some(string) == null_value {
                                list_builder.values().append_null();
                            } else {
                                list_builder.values().append_value(string);
                            }
                            list_builder.append(true);
                        }
                        (Some(string), Some(delimiter)) => {
                            string.split(delimiter).for_each(|s| {
                                if Some(s) == null_value {
                                    list_builder.values().append_null();
                                } else {
                                    list_builder.values().append_value(s);
                                }
                            });
                            list_builder.append(true);
                        }
                        (Some(string), None) => {
                            string.chars().map(|c| c.to_string()).for_each(|c| {
                                if Some(c.as_str()) == null_value {
                                    list_builder.values().append_null();
                                } else {
                                    list_builder.values().append_value(c);
                                }
                            });
                            list_builder.append(true);
                        }
                        _ => list_builder.append(false), // null value
                    }
                });
        }
        _ => {
            return exec_err!(
                "Expect string_to_array function to take two or three parameters"
            )
        }
    }

    let list_array = list_builder.finish();
    Ok(Arc::new(list_array) as ArrayRef)
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
pub(super) fn gen_range(args: &[ArrayRef], include_upper: bool) -> Result<ArrayRef> {
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
        Arc::new(Field::new("item", DataType::Int64, true)),
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

/// Returns the length of each array dimension
fn compute_array_dims(arr: Option<ArrayRef>) -> Result<Option<Vec<Option<u64>>>> {
    let mut value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };
    if value.is_empty() {
        return Ok(None);
    }
    let mut res = vec![Some(value.len() as u64)];

    loop {
        match value.data_type() {
            DataType::List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                res.push(Some(value.len() as u64));
            }
            _ => return Ok(Some(res)),
        }
    }
}

fn generic_list_cardinality<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let result = array
        .iter()
        .map(|arr| match compute_array_dims(arr)? {
            Some(vector) => Ok(Some(vector.iter().map(|x| x.unwrap()).product::<u64>())),
            None => Ok(None),
        })
        .collect::<Result<UInt64Array>>()?;
    Ok(Arc::new(result) as ArrayRef)
}

/// Cardinality SQL function
pub fn cardinality(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("cardinality expects one argument");
    }

    match &args[0].data_type() {
        DataType::List(_) => {
            let list_array = as_list_array(&args[0])?;
            generic_list_cardinality::<i32>(list_array)
        }
        DataType::LargeList(_) => {
            let list_array = as_large_list_array(&args[0])?;
            generic_list_cardinality::<i64>(list_array)
        }
        other => {
            exec_err!("cardinality does not support type '{:?}'", other)
        }
    }
}

/// Array_dims SQL function
pub fn array_dims(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_dims needs one argument");
    }

    let data = match args[0].data_type() {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            array
                .iter()
                .map(compute_array_dims)
                .collect::<Result<Vec<_>>>()?
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            array
                .iter()
                .map(compute_array_dims)
                .collect::<Result<Vec<_>>>()?
        }
        array_type => {
            return exec_err!("array_dims does not support type '{array_type:?}'");
        }
    };

    let result = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

    Ok(Arc::new(result) as ArrayRef)
}

/// Array_ndims SQL function
pub fn array_ndims(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_ndims needs one argument");
    }

    fn general_list_ndims<O: OffsetSizeTrait>(
        array: &GenericListArray<O>,
    ) -> Result<ArrayRef> {
        let mut data = Vec::new();
        let ndims = datafusion_common::utils::list_ndims(array.data_type());

        for arr in array.iter() {
            if arr.is_some() {
                data.push(Some(ndims))
            } else {
                data.push(None)
            }
        }

        Ok(Arc::new(UInt64Array::from(data)) as ArrayRef)
    }
    match args[0].data_type() {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            general_list_ndims::<i32>(array)
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            general_list_ndims::<i64>(array)
        }
        array_type => exec_err!("array_ndims does not support type {array_type:?}"),
    }
}
pub fn gen_range_date(
    args: &[ArrayRef],
    include_upper: bool,
) -> datafusion_common::Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!("arguments length does not match");
    }
    let (start_array, stop_array, step_array) = (
        Some(as_date32_array(&args[0])?),
        as_date32_array(&args[1])?,
        Some(as_interval_mdn_array(&args[2])?),
    );

    let mut values = vec![];
    let mut offsets = vec![0];
    for (idx, stop) in stop_array.iter().enumerate() {
        let mut stop = stop.unwrap_or(0);
        let start = start_array.as_ref().map(|x| x.value(idx)).unwrap_or(0);
        let step = step_array.as_ref().map(|arr| arr.value(idx)).unwrap_or(1);
        let (months, days, _) = IntervalMonthDayNanoType::to_parts(step);
        let neg = months < 0 || days < 0;
        if !include_upper {
            stop = Date32Type::subtract_month_day_nano(stop, step);
        }
        let mut new_date = start;
        loop {
            if neg && new_date < stop || !neg && new_date > stop {
                break;
            }
            values.push(new_date);
            new_date = Date32Type::add_month_day_nano(new_date, step);
        }
        offsets.push(values.len() as i32);
    }

    let arr = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", DataType::Date32, true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(Date32Array::from(values)),
        None,
    )?);
    Ok(arr)
}

/// Array_empty SQL function
pub fn array_empty(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_empty expects one argument");
    }

    if as_null_array(&args[0]).is_ok() {
        // Make sure to return Boolean type.
        return Ok(Arc::new(BooleanArray::new_null(args[0].len())));
    }
    let array_type = args[0].data_type();

    match array_type {
        DataType::List(_) => general_array_empty::<i32>(&args[0]),
        DataType::LargeList(_) => general_array_empty::<i64>(&args[0]),
        _ => exec_err!("array_empty does not support type '{array_type:?}'."),
    }
}

fn general_array_empty<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let array = as_generic_list_array::<O>(array)?;
    let builder = array
        .iter()
        .map(|arr| arr.map(|arr| arr.len() == arr.null_count()))
        .collect::<BooleanArray>();
    Ok(Arc::new(builder))
}

/// Returns the length of a concrete array dimension
fn compute_array_length(
    arr: Option<ArrayRef>,
    dimension: Option<i64>,
) -> Result<Option<u64>> {
    let mut current_dimension: i64 = 1;
    let mut value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let dimension = match dimension {
        Some(value) => {
            if value < 1 {
                return Ok(None);
            }

            value
        }
        None => return Ok(None),
    };

    loop {
        if current_dimension == dimension {
            return Ok(Some(value.len() as u64));
        }

        match value.data_type() {
            DataType::List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                current_dimension += 1;
            }
            DataType::LargeList(..) => {
                value = downcast_arg!(value, LargeListArray).value(0);
                current_dimension += 1;
            }
            _ => return Ok(None),
        }
    }
}

/// Dispatch array length computation based on the offset type.
fn general_array_length<O: OffsetSizeTrait>(array: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&array[0])?;
    let dimension = if array.len() == 2 {
        as_int64_array(&array[1])?.clone()
    } else {
        Int64Array::from_value(1, list_array.len())
    };

    let result = list_array
        .iter()
        .zip(dimension.iter())
        .map(|(arr, dim)| compute_array_length(arr, dim))
        .collect::<Result<UInt64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Array_length SQL function
pub fn array_length(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!("array_length expects one or two arguments");
    }

    match &args[0].data_type() {
        DataType::List(_) => general_array_length::<i32>(args),
        DataType::LargeList(_) => general_array_length::<i64>(args),
        array_type => exec_err!("array_length does not support type '{array_type:?}'"),
    }
}

/// Array_sort SQL function
pub fn array_sort(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 3 {
        return exec_err!("array_sort expects one to three arguments");
    }

    let sort_option = match args.len() {
        1 => None,
        2 => {
            let sort = as_string_array(&args[1])?.value(0);
            Some(SortOptions {
                descending: order_desc(sort)?,
                nulls_first: true,
            })
        }
        3 => {
            let sort = as_string_array(&args[1])?.value(0);
            let nulls_first = as_string_array(&args[2])?.value(0);
            Some(SortOptions {
                descending: order_desc(sort)?,
                nulls_first: order_nulls_first(nulls_first)?,
            })
        }
        _ => return exec_err!("array_sort expects 1 to 3 arguments"),
    };

    let list_array = as_list_array(&args[0])?;
    let row_count = list_array.len();

    let mut array_lengths = vec![];
    let mut arrays = vec![];
    let mut valid = BooleanBufferBuilder::new(row_count);
    for i in 0..row_count {
        if list_array.is_null(i) {
            array_lengths.push(0);
            valid.append(false);
        } else {
            let arr_ref = list_array.value(i);
            let arr_ref = arr_ref.as_ref();

            let sorted_array = compute::sort(arr_ref, sort_option)?;
            array_lengths.push(sorted_array.len());
            arrays.push(sorted_array);
            valid.append(true);
        }
    }

    // Assume all arrays have the same data type
    let data_type = list_array.value_type();
    let buffer = valid.finish();

    let elements = arrays
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<&dyn Array>>();

    let list_arr = ListArray::new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::from_lengths(array_lengths),
        Arc::new(compute::concat(elements.as_slice())?),
        Some(NullBuffer::new(buffer)),
    );
    Ok(Arc::new(list_arr))
}

fn order_desc(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "DESC" => Ok(true),
        "ASC" => Ok(false),
        _ => exec_err!("the second parameter of array_sort expects DESC or ASC"),
    }
}

fn order_nulls_first(modifier: &str) -> Result<bool> {
    match modifier.to_uppercase().as_str() {
        "NULLS FIRST" => Ok(true),
        "NULLS LAST" => Ok(false),
        _ => exec_err!(
            "the third parameter of array_sort expects NULLS FIRST or NULLS LAST"
        ),
    }
}

// Create new offsets that are euqiavlent to `flatten` the array.
fn get_offsets_for_flatten<O: OffsetSizeTrait>(
    offsets: OffsetBuffer<O>,
    indexes: OffsetBuffer<O>,
) -> OffsetBuffer<O> {
    let buffer = offsets.into_inner();
    let offsets: Vec<O> = indexes
        .iter()
        .map(|i| buffer[i.to_usize().unwrap()])
        .collect();
    OffsetBuffer::new(offsets.into())
}

fn flatten_internal<O: OffsetSizeTrait>(
    list_arr: GenericListArray<O>,
    indexes: Option<OffsetBuffer<O>>,
) -> Result<GenericListArray<O>> {
    let (field, offsets, values, _) = list_arr.clone().into_parts();
    let data_type = field.data_type();

    match data_type {
        // Recursively get the base offsets for flattened array
        DataType::List(_) | DataType::LargeList(_) => {
            let sub_list = as_generic_list_array::<O>(&values)?;
            if let Some(indexes) = indexes {
                let offsets = get_offsets_for_flatten(offsets, indexes);
                flatten_internal::<O>(sub_list.clone(), Some(offsets))
            } else {
                flatten_internal::<O>(sub_list.clone(), Some(offsets))
            }
        }
        // Reach the base level, create a new list array
        _ => {
            if let Some(indexes) = indexes {
                let offsets = get_offsets_for_flatten(offsets, indexes);
                let list_arr = GenericListArray::<O>::new(field, offsets, values, None);
                Ok(list_arr)
            } else {
                Ok(list_arr.clone())
            }
        }
    }
}

/// Flatten SQL function
pub fn flatten(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("flatten expects one argument");
    }

    let array_type = args[0].data_type();
    match array_type {
        DataType::List(_) => {
            let list_arr = as_list_array(&args[0])?;
            let flattened_array = flatten_internal::<i32>(list_arr.clone(), None)?;
            Ok(Arc::new(flattened_array) as ArrayRef)
        }
        DataType::LargeList(_) => {
            let list_arr = as_large_list_array(&args[0])?;
            let flattened_array = flatten_internal::<i64>(list_arr.clone(), None)?;
            Ok(Arc::new(flattened_array) as ArrayRef)
        }
        DataType::Null => Ok(args[0].clone()),
        _ => {
            exec_err!("flatten does not support type '{array_type:?}'")
        }
    }
}

/// array_distinct SQL function
/// example: from list [1, 3, 2, 3, 1, 2, 4] to [1, 2, 3, 4]
pub fn array_distinct(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_distinct needs one argument");
    }

    // handle null
    if args[0].data_type() == &DataType::Null {
        return Ok(args[0].clone());
    }

    // handle for list & largelist
    match args[0].data_type() {
        DataType::List(field) => {
            let array = as_list_array(&args[0])?;
            general_array_distinct(array, field)
        }
        DataType::LargeList(field) => {
            let array = as_large_list_array(&args[0])?;
            general_array_distinct(array, field)
        }
        array_type => exec_err!("array_distinct does not support type '{array_type:?}'"),
    }
}

pub fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let dt = array.value_type();
    let mut offsets = Vec::with_capacity(array.len());
    offsets.push(OffsetSize::usize_as(0));
    let mut new_arrays = Vec::with_capacity(array.len());
    let converter = RowConverter::new(vec![SortField::new(dt)])?;
    // distinct for each list in ListArray
    for arr in array.iter().flatten() {
        let values = converter.convert_columns(&[arr])?;
        // sort elements in list and remove duplicates
        let rows = values.iter().sorted().dedup().collect::<Vec<_>>();
        let last_offset: OffsetSize = offsets.last().copied().unwrap();
        offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
        let arrays = converter.convert_rows(rows)?;
        let array = match arrays.first() {
            Some(array) => array.clone(),
            None => {
                return internal_err!("array_distinct: failed to get array from rows")
            }
        };
        new_arrays.push(array);
    }
    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field.clone(),
        offsets,
        values,
        None,
    )?))
}
