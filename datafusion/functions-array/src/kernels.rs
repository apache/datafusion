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

use arrow::array::ListArray;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, GenericListArray,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, OffsetSizeTrait,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Field;
use arrow::datatypes::{DataType, UInt64Type};
use datafusion_common::cast::{
    as_int64_array, as_large_list_array, as_list_array, as_string_array,
};
use datafusion_common::{exec_err, DataFusionError, Result};
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
pub fn gen_range(args: &[ArrayRef], include_upper: i64) -> Result<ArrayRef> {
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
    for (idx, stop) in stop_array.iter().enumerate() {
        let mut stop = stop.unwrap_or(0);
        let start = start_array.as_ref().map(|arr| arr.value(idx)).unwrap_or(0);
        let step = step_array.as_ref().map(|arr| arr.value(idx)).unwrap_or(1);
        if step == 0 {
            return exec_err!("step can't be 0 for function range(start [, stop, step]");
        }
        if step < 0 {
            // Decreasing range
            stop -= include_upper;
            values.extend((stop + 1..start + 1).rev().step_by((-step) as usize));
        } else {
            // Increasing range
            stop += include_upper;
            values.extend((start..stop).step_by(step as usize));
        }

        offsets.push(values.len() as i32);
    }
    let arr = Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(Int64Array::from(values)),
        None,
    )?);
    Ok(arr)
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
