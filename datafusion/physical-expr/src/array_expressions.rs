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

//! Array expressions

use arrow::array::*;
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::compute;
use arrow::datatypes::{DataType, Field, UInt64Type};
use arrow_buffer::NullBuffer;
use core::any::type_name;
use datafusion_common::cast::{as_generic_string_array, as_int64_array, as_list_array};
use datafusion_common::{internal_err, plan_err, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use itertools::Itertools;
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

/// Downcasts multiple arguments into a single concrete type
/// $ARGS:  &[ArrayRef]
/// $ARRAY_TYPE: type to downcast to
///
/// $returns a Vec<$ARRAY_TYPE>
macro_rules! downcast_vec {
    ($ARGS:expr, $ARRAY_TYPE:ident) => {{
        $ARGS
            .iter()
            .map(|e| match e.as_any().downcast_ref::<$ARRAY_TYPE>() {
                Some(array) => Ok(array),
                _ => Err(DataFusionError::Internal("failed to downcast".to_string())),
            })
    }};
}

macro_rules! new_builder {
    (BooleanBuilder, $len:expr) => {
        BooleanBuilder::with_capacity($len)
    };
    (StringBuilder, $len:expr) => {
        StringBuilder::new()
    };
    (LargeStringBuilder, $len:expr) => {
        LargeStringBuilder::new()
    };
    ($el:ident, $len:expr) => {{
        <$el>::with_capacity($len)
    }};
}

/// Combines multiple arrays into a single ListArray
///
/// $ARGS: slice of arrays, each with $ARRAY_TYPE
/// $ARRAY_TYPE: the type of the list elements
/// $BUILDER_TYPE: the type of ArrayBuilder for the list elements
///
/// Returns: a ListArray where the elements each have the same type as
/// $ARRAY_TYPE and each element have a length of $ARGS.len()
macro_rules! array {
    ($ARGS:expr, $ARRAY_TYPE:ident, $BUILDER_TYPE:ident) => {{
        let builder = new_builder!($BUILDER_TYPE, $ARGS[0].len());
        let mut builder =
            ListBuilder::<$BUILDER_TYPE>::with_capacity(builder, $ARGS.len());

        let num_rows = $ARGS[0].len();
        assert!(
            $ARGS.iter().all(|a| a.len() == num_rows),
            "all arguments must have the same number of rows"
        );

        // for each entry in the array
        for index in 0..num_rows {
            // for each column
            for arg in $ARGS {
                match arg.as_any().downcast_ref::<$ARRAY_TYPE>() {
                    // Copy the source array value into the target ListArray
                    Some(arr) => {
                        if arr.is_valid(index) {
                            builder.values().append_value(arr.value(index));
                        } else {
                            builder.values().append_null();
                        }
                    }
                    None => match arg.as_any().downcast_ref::<NullArray>() {
                        Some(arr) => {
                            for _ in 0..arr.len() {
                                builder.values().append_null();
                            }
                        }
                        None => {
                            return Err(DataFusionError::Internal(
                                "failed to downcast".to_string(),
                            ))
                        }
                    },
                }
            }
            builder.append(true);
        }
        Arc::new(builder.finish())
    }};
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
            _ => return Ok(None),
        }
    }
}

/// Returns the dimension of the array
fn compute_array_ndims(arr: Option<ArrayRef>) -> Result<Option<u64>> {
    Ok(compute_array_ndims_with_datatype(arr)?.0)
}

/// Returns the dimension and the datatype of elements of the array
fn compute_array_ndims_with_datatype(
    arr: Option<ArrayRef>,
) -> Result<(Option<u64>, DataType)> {
    let mut res: u64 = 1;
    let mut value = match arr {
        Some(arr) => arr,
        None => return Ok((None, DataType::Null)),
    };
    if value.is_empty() {
        return Ok((None, DataType::Null));
    }

    loop {
        match value.data_type() {
            DataType::List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                res += 1;
            }
            data_type => return Ok((Some(res), data_type.clone())),
        }
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

#[derive(Debug)]
enum ListOrNull<'a> {
    List(&'a dyn Array),
    Null,
}

/// Convert one or more [`ArrayRef`] of the same type into a
/// `ListArray`
///
/// # Example (non nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are non nested
/// would return a single new `ListArray`, where each row was a list
/// of 2 elements:
///
/// ```text
/// ┌─────────┐   ┌─────────┐           ┌──────────────┐
/// │ ┌─────┐ │   │ ┌─────┐ │           │ ┌──────────┐ │
/// │ │  A  │ │   │ │  X  │ │           │ │  [A, X]  │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │NULL │ │   │ │  Y  │ │──────────▶│ │[NULL, Y] │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │  C  │ │   │ │  Z  │ │           │ │  [C, Z]  │ │
/// │ └─────┘ │   │ └─────┘ │           │ └──────────┘ │
/// └─────────┘   └─────────┘           └──────────────┘
///   col1           col2                    output
/// ```
///
/// # Example (nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are lists
/// would return a single new `ListArray`, where each row was a list
/// of the corresponding elements of col1 and col2.
///
/// ``` text
/// ┌──────────────┐   ┌──────────────┐        ┌─────────────────────────────┐
/// │ ┌──────────┐ │   │ ┌──────────┐ │        │ ┌────────────────────────┐  │
/// │ │  [A, X]  │ │   │ │    []    │ │        │ │    [[A, X], []]        │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────┤  │
/// │ │[NULL, Y] │ │   │ │[Q, R, S] │ │───────▶│ │ [[NULL, Y], [Q, R, S]] │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────│  │
/// │ │  [C, Z]  │ │   │ │   NULL   │ │        │ │    [[C, Z], NULL]      │  │
/// │ └──────────┘ │   │ └──────────┘ │        │ └────────────────────────┘  │
/// └──────────────┘   └──────────────┘        └─────────────────────────────┘
///      col1               col2                         output
/// ```
fn array_array(args: &[ArrayRef], data_type: DataType) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return plan_err!("Array requires at least one argument");
    }

    let res = match data_type {
        DataType::List(..) => {
            let mut arrays = vec![];
            let mut row_count = 0;

            for arg in args {
                let list_arr = arg.as_list_opt::<i32>();
                if let Some(list_arr) = list_arr {
                    // Assume number of rows is the same for all arrays
                    row_count = list_arr.len();
                    arrays.push(ListOrNull::List(list_arr));
                } else if arg.as_any().downcast_ref::<NullArray>().is_some() {
                    arrays.push(ListOrNull::Null);
                } else {
                    return Err(DataFusionError::Internal(
                        "Unsupported argument type for array".to_string(),
                    ));
                }
            }

            let mut total_capacity = 0;
            let mut array_data = vec![];
            for arr in arrays.iter() {
                if let ListOrNull::List(arr) = arr {
                    total_capacity += arr.len();
                    array_data.push(arr.to_data());
                }
            }
            let capacity = Capacities::Array(total_capacity);
            let array_data = array_data.iter().collect();

            let mut mutable =
                MutableArrayData::with_capacities(array_data, true, capacity);

            for i in 0..row_count {
                let mut nulls = 0;
                for (j, arr) in arrays.iter().enumerate() {
                    match arr {
                        ListOrNull::List(_) => {
                            mutable.extend(j - nulls, i, i + 1);
                        }
                        ListOrNull::Null => {
                            mutable.extend_nulls(1);
                            nulls += 1;
                        }
                    }
                }
            }

            let list_data_type =
                DataType::List(Arc::new(Field::new("item", data_type, true)));

            let offsets: Vec<i32> = (0..row_count as i32 + 1)
                .map(|i| i * arrays.len() as i32)
                .collect();

            let list_data = ArrayData::builder(list_data_type)
                .len(row_count)
                .buffers(vec![Buffer::from_vec(offsets)])
                .add_child_data(mutable.freeze())
                .build()?;
            Arc::new(ListArray::from(list_data))
        }
        DataType::Utf8 => array!(args, StringArray, StringBuilder),
        DataType::LargeUtf8 => array!(args, LargeStringArray, LargeStringBuilder),
        DataType::Boolean => array!(args, BooleanArray, BooleanBuilder),
        DataType::Float32 => array!(args, Float32Array, Float32Builder),
        DataType::Float64 => array!(args, Float64Array, Float64Builder),
        DataType::Int8 => array!(args, Int8Array, Int8Builder),
        DataType::Int16 => array!(args, Int16Array, Int16Builder),
        DataType::Int32 => array!(args, Int32Array, Int32Builder),
        DataType::Int64 => array!(args, Int64Array, Int64Builder),
        DataType::UInt8 => array!(args, UInt8Array, UInt8Builder),
        DataType::UInt16 => array!(args, UInt16Array, UInt16Builder),
        DataType::UInt32 => array!(args, UInt32Array, UInt32Builder),
        DataType::UInt64 => array!(args, UInt64Array, UInt64Builder),
        data_type => {
            return Err(DataFusionError::NotImplemented(format!(
                "Array is not implemented for type '{data_type:?}'."
            )))
        }
    };

    Ok(res)
}

/// Convert one or more [`ColumnarValue`] of the same type into a
/// `ListArray`
///
/// See [`array_array`] for more details.
fn array(values: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays: Vec<ArrayRef> = values
        .iter()
        .map(|x| match x {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        })
        .collect();

    let mut data_type = None;
    for arg in &arrays {
        let arg_data_type = arg.data_type();
        if !arg_data_type.equals_datatype(&DataType::Null) {
            data_type = Some(arg_data_type.clone());
            break;
        } else {
            data_type = Some(DataType::Null);
        }
    }

    match data_type {
        // empty array
        None => Ok(ColumnarValue::Scalar(ScalarValue::new_list(
            Some(vec![]),
            DataType::Null,
        ))),
        // all nulls, set default data type as int32
        Some(DataType::Null) => {
            let nulls = arrays.len();
            let null_arr = Int32Array::from(vec![None; nulls]);
            let field = Arc::new(Field::new("item", DataType::Int32, true));
            let offsets = OffsetBuffer::from_lengths([nulls]);
            let values = Arc::new(null_arr) as ArrayRef;
            let nulls = None;
            Ok(ColumnarValue::Array(Arc::new(ListArray::new(
                field, offsets, values, nulls,
            ))))
        }
        Some(data_type) => Ok(ColumnarValue::Array(array_array(
            arrays.as_slice(),
            data_type,
        )?)),
    }
}

/// `make_array` SQL function
pub fn make_array(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let values: Vec<ColumnarValue> = arrays
        .iter()
        .map(|x| ColumnarValue::Array(x.clone()))
        .collect();

    match array(values.as_slice())? {
        ColumnarValue::Array(array) => Ok(array),
        ColumnarValue::Scalar(scalar) => Ok(scalar.to_array().clone()),
    }
}

fn return_empty(return_null: bool, data_type: DataType) -> Arc<dyn Array> {
    if return_null {
        new_null_array(&data_type, 1)
    } else {
        new_empty_array(&data_type)
    }
}

macro_rules! list_slice {
    ($ARRAY:expr, $I:expr, $J:expr, $RETURN_ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let array = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        if $I == 0 && $J == 0 || $ARRAY.is_empty() {
            return return_empty($RETURN_ELEMENT, $ARRAY.data_type().clone());
        }

        let i = if $I < 0 {
            if $I.abs() as usize > array.len() {
                return return_empty(true, $ARRAY.data_type().clone());
            }

            (array.len() as i64 + $I + 1) as usize
        } else {
            if $I == 0 {
                1
            } else {
                $I as usize
            }
        };
        let j = if $J < 0 {
            if $J.abs() as usize > array.len() {
                return return_empty(true, $ARRAY.data_type().clone());
            }

            if $RETURN_ELEMENT {
                (array.len() as i64 + $J + 1) as usize
            } else {
                (array.len() as i64 + $J) as usize
            }
        } else {
            if $J == 0 {
                1
            } else {
                if $J as usize > array.len() {
                    array.len()
                } else {
                    $J as usize
                }
            }
        };

        if i > j || i as usize > $ARRAY.len() {
            return_empty($RETURN_ELEMENT, $ARRAY.data_type().clone())
        } else {
            Arc::new(array.slice((i - 1), (j + 1 - i)))
        }
    }};
}

macro_rules! slice {
    ($ARRAY:expr, $KEY:expr, $EXTRA_KEY:expr, $RETURN_ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let sliced_array: Vec<Arc<dyn Array>> = $ARRAY
            .iter()
            .zip($KEY.iter())
            .zip($EXTRA_KEY.iter())
            .map(|((arr, i), j)| match (arr, i, j) {
                (Some(arr), Some(i), Some(j)) => {
                    list_slice!(arr, i, j, $RETURN_ELEMENT, $ARRAY_TYPE)
                }
                (Some(arr), None, Some(j)) => {
                    list_slice!(arr, 1i64, j, $RETURN_ELEMENT, $ARRAY_TYPE)
                }
                (Some(arr), Some(i), None) => {
                    list_slice!(arr, i, arr.len() as i64, $RETURN_ELEMENT, $ARRAY_TYPE)
                }
                (Some(arr), None, None) if !$RETURN_ELEMENT => arr,
                _ => return_empty($RETURN_ELEMENT, $ARRAY.value_type().clone()),
            })
            .collect();

        // concat requires input of at least one array
        if sliced_array.is_empty() {
            Ok(return_empty($RETURN_ELEMENT, $ARRAY.value_type()))
        } else {
            let vec = sliced_array
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<&dyn Array>>();
            let mut i: i32 = 0;
            let mut offsets = vec![i];
            offsets.extend(
                vec.iter()
                    .map(|a| {
                        i += a.len() as i32;
                        i
                    })
                    .collect::<Vec<_>>(),
            );
            let values = compute::concat(vec.as_slice()).unwrap();

            if $RETURN_ELEMENT {
                Ok(values)
            } else {
                let field =
                    Arc::new(Field::new("item", $ARRAY.value_type().clone(), true));
                Ok(Arc::new(ListArray::try_new(
                    field,
                    OffsetBuffer::new(offsets.into()),
                    values,
                    None,
                )?))
            }
        }
    }};
}

fn define_array_slice(
    list_array: &ListArray,
    key: &Int64Array,
    extra_key: &Int64Array,
    return_element: bool,
) -> Result<ArrayRef> {
    match list_array.value_type() {
        DataType::List(_) => {
            slice!(list_array, key, extra_key, return_element, ListArray)
        }
        DataType::Utf8 => slice!(list_array, key, extra_key, return_element, StringArray),
        DataType::LargeUtf8 => {
            slice!(list_array, key, extra_key, return_element, LargeStringArray)
        }
        DataType::Boolean => {
            slice!(list_array, key, extra_key, return_element, BooleanArray)
        }
        DataType::Float32 => {
            slice!(list_array, key, extra_key, return_element, Float32Array)
        }
        DataType::Float64 => {
            slice!(list_array, key, extra_key, return_element, Float64Array)
        }
        DataType::Int8 => slice!(list_array, key, extra_key, return_element, Int8Array),
        DataType::Int16 => slice!(list_array, key, extra_key, return_element, Int16Array),
        DataType::Int32 => slice!(list_array, key, extra_key, return_element, Int32Array),
        DataType::Int64 => slice!(list_array, key, extra_key, return_element, Int64Array),
        DataType::UInt8 => slice!(list_array, key, extra_key, return_element, UInt8Array),
        DataType::UInt16 => {
            slice!(list_array, key, extra_key, return_element, UInt16Array)
        }
        DataType::UInt32 => {
            slice!(list_array, key, extra_key, return_element, UInt32Array)
        }
        DataType::UInt64 => {
            slice!(list_array, key, extra_key, return_element, UInt64Array)
        }
        data_type => Err(DataFusionError::NotImplemented(format!(
            "array is not implemented for types '{data_type:?}'"
        ))),
    }
}

pub fn array_element(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_list_array(&args[0])?;
    let key = as_int64_array(&args[1])?;
    define_array_slice(list_array, key, key, true)
}

pub fn array_slice(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_list_array(&args[0])?;
    let key = as_int64_array(&args[1])?;
    let extra_key = as_int64_array(&args[2])?;
    define_array_slice(list_array, key, extra_key, false)
}

macro_rules! append {
    ($ARRAY:expr, $ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array($ELEMENT.data_type()), $ARRAY_TYPE).clone();

        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        for (arr, el) in $ARRAY.iter().zip(element.iter()) {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty"))
            })?;
            match arr {
                Some(arr) => {
                    let child_array = downcast_arg!(arr, $ARRAY_TYPE);
                    values = downcast_arg!(
                        compute::concat(&[
                            &values,
                            child_array,
                            &$ARRAY_TYPE::from(vec![el])
                        ])?
                        .clone(),
                        $ARRAY_TYPE
                    )
                    .clone();
                    offsets.push(last_offset + child_array.len() as i32 + 1i32);
                }
                None => {
                    values = downcast_arg!(
                        compute::concat(&[
                            &values,
                            &$ARRAY_TYPE::from(vec![el.clone()])
                        ])?
                        .clone(),
                        $ARRAY_TYPE
                    )
                    .clone();
                    offsets.push(last_offset + 1i32);
                }
            }
        }

        let field = Arc::new(Field::new("item", $ELEMENT.data_type().clone(), true));

        Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )?)
    }};
}

/// Array_append SQL function
pub fn array_append(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return internal_err!(
            "Array_append function requires two arguments, got {}",
            args.len()
        );
    }

    let arr = as_list_array(&args[0])?;
    let element = &args[1];

    let res = match (arr.value_type(), element.data_type()) {
                (DataType::List(_), DataType::List(_)) => concat_internal(args)?,
                (DataType::Utf8, DataType::Utf8) => append!(arr, element, StringArray),
                (DataType::LargeUtf8, DataType::LargeUtf8) => append!(arr, element, LargeStringArray),
                (DataType::Boolean, DataType::Boolean) => append!(arr, element, BooleanArray),
                (DataType::Float32, DataType::Float32) => append!(arr, element, Float32Array),
                (DataType::Float64, DataType::Float64) => append!(arr, element, Float64Array),
                (DataType::Int8, DataType::Int8) => append!(arr, element, Int8Array),
                (DataType::Int16, DataType::Int16) => append!(arr, element, Int16Array),
                (DataType::Int32, DataType::Int32) => append!(arr, element, Int32Array),
                (DataType::Int64, DataType::Int64) => append!(arr, element, Int64Array),
                (DataType::UInt8, DataType::UInt8) => append!(arr, element, UInt8Array),
                (DataType::UInt16, DataType::UInt16) => append!(arr, element, UInt16Array),
                (DataType::UInt32, DataType::UInt32) => append!(arr, element, UInt32Array),
                (DataType::UInt64, DataType::UInt64) => append!(arr, element, UInt64Array),
                (DataType::Null, _) => return Ok(array(&[ColumnarValue::Array(args[1].clone())])?.into_array(1)),
                (array_data_type, element_data_type) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Array_append is not implemented for types '{array_data_type:?}' and '{element_data_type:?}'."
                    )))
                }
    };

    Ok(res)
}

macro_rules! prepend {
    ($ARRAY:expr, $ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array($ELEMENT.data_type()), $ARRAY_TYPE).clone();

        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        for (arr, el) in $ARRAY.iter().zip(element.iter()) {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty"))
            })?;
            match arr {
                Some(arr) => {
                    let child_array = downcast_arg!(arr, $ARRAY_TYPE);
                    values = downcast_arg!(
                        compute::concat(&[
                            &values,
                            &$ARRAY_TYPE::from(vec![el]),
                            child_array
                        ])?
                        .clone(),
                        $ARRAY_TYPE
                    )
                    .clone();
                    offsets.push(last_offset + child_array.len() as i32 + 1i32);
                }
                None => {
                    values = downcast_arg!(
                        compute::concat(&[
                            &values,
                            &$ARRAY_TYPE::from(vec![el.clone()])
                        ])?
                        .clone(),
                        $ARRAY_TYPE
                    )
                    .clone();
                    offsets.push(last_offset + 1i32);
                }
            }
        }

        let field = Arc::new(Field::new("item", $ELEMENT.data_type().clone(), true));

        Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )?)
    }};
}

/// Array_prepend SQL function
pub fn array_prepend(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return internal_err!(
            "Array_prepend function requires two arguments, got {}",
            args.len()
        );
    }

    let element = &args[0];
    let arr = as_list_array(&args[1])?;

    let res = match (arr.value_type(), element.data_type()) {
                (DataType::List(_), DataType::List(_)) => concat_internal(args)?,
                (DataType::Utf8, DataType::Utf8) => prepend!(arr, element, StringArray),
                (DataType::LargeUtf8, DataType::LargeUtf8) => prepend!(arr, element, LargeStringArray),
                (DataType::Boolean, DataType::Boolean) => prepend!(arr, element, BooleanArray),
                (DataType::Float32, DataType::Float32) => prepend!(arr, element, Float32Array),
                (DataType::Float64, DataType::Float64) => prepend!(arr, element, Float64Array),
                (DataType::Int8, DataType::Int8) => prepend!(arr, element, Int8Array),
                (DataType::Int16, DataType::Int16) => prepend!(arr, element, Int16Array),
                (DataType::Int32, DataType::Int32) => prepend!(arr, element, Int32Array),
                (DataType::Int64, DataType::Int64) => prepend!(arr, element, Int64Array),
                (DataType::UInt8, DataType::UInt8) => prepend!(arr, element, UInt8Array),
                (DataType::UInt16, DataType::UInt16) => prepend!(arr, element, UInt16Array),
                (DataType::UInt32, DataType::UInt32) => prepend!(arr, element, UInt32Array),
                (DataType::UInt64, DataType::UInt64) => prepend!(arr, element, UInt64Array),
                (DataType::Null, _) => return Ok(array(&[ColumnarValue::Array(args[0].clone())])?.into_array(1)),
                (array_data_type, element_data_type) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Array_prepend is not implemented for types '{array_data_type:?}' and '{element_data_type:?}'."
                    )))
                }
    };

    Ok(res)
}

fn align_array_dimensions(args: Vec<ArrayRef>) -> Result<Vec<ArrayRef>> {
    // Find the maximum number of dimensions
    let max_ndim: u64 = (*args
        .iter()
        .map(|arr| compute_array_ndims(Some(arr.clone())))
        .collect::<Result<Vec<Option<u64>>>>()?
        .iter()
        .max()
        .unwrap())
    .unwrap();

    // Align the dimensions of the arrays
    let aligned_args: Result<Vec<ArrayRef>> = args
        .into_iter()
        .map(|array| {
            let ndim = compute_array_ndims(Some(array.clone()))?.unwrap();
            if ndim < max_ndim {
                let mut aligned_array = array.clone();
                for _ in 0..(max_ndim - ndim) {
                    let data_type = aligned_array.as_ref().data_type().clone();
                    let offsets: Vec<i32> =
                        (0..downcast_arg!(aligned_array, ListArray).offsets().len())
                            .map(|i| i as i32)
                            .collect();
                    let field = Arc::new(Field::new("item", data_type, true));

                    aligned_array = Arc::new(ListArray::try_new(
                        field,
                        OffsetBuffer::new(offsets.into()),
                        Arc::new(aligned_array.clone()),
                        None,
                    )?)
                }
                Ok(aligned_array)
            } else {
                Ok(array.clone())
            }
        })
        .collect();

    aligned_args
}

fn concat_internal(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args = align_array_dimensions(args.to_vec())?;

    let list_arrays =
        downcast_vec!(args, ListArray).collect::<Result<Vec<&ListArray>>>()?;

    // Assume number of rows is the same for all arrays
    let row_count = list_arrays[0].len();
    let capacity = Capacities::Array(list_arrays.iter().map(|a| a.len()).sum());
    let array_data: Vec<_> = list_arrays.iter().map(|a| a.to_data()).collect::<Vec<_>>();
    let array_data: Vec<&ArrayData> = array_data.iter().collect();

    let mut mutable = MutableArrayData::with_capacities(array_data, true, capacity);

    let mut array_lens = vec![0; row_count];
    let mut null_bit_map: Vec<bool> = vec![true; row_count];

    for (i, array_len) in array_lens.iter_mut().enumerate().take(row_count) {
        let null_count = mutable.null_count();
        for (j, a) in list_arrays.iter().enumerate() {
            mutable.extend(j, i, i + 1);
            *array_len += a.value_length(i);
        }

        // This means all arrays are null
        if mutable.null_count() == null_count + list_arrays.len() {
            null_bit_map[i] = false;
        }
    }

    let mut buffer = BooleanBufferBuilder::new(row_count);
    buffer.append_slice(null_bit_map.as_slice());
    let nulls = Some(NullBuffer::from(buffer.finish()));

    let offsets: Vec<i32> = std::iter::once(0)
        .chain(array_lens.iter().scan(0, |state, &x| {
            *state += x;
            Some(*state)
        }))
        .collect();

    let builder = mutable.into_builder();

    let list = builder
        .len(row_count)
        .buffers(vec![Buffer::from_vec(offsets)])
        .nulls(nulls)
        .build()?;

    let list = arrow::array::make_array(list);
    Ok(Arc::new(list))
}

/// Array_concat/Array_cat SQL function
pub fn array_concat(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut new_args = vec![];
    for arg in args {
        let (ndim, lower_data_type) =
            compute_array_ndims_with_datatype(Some(arg.clone()))?;
        if ndim.is_none() || ndim == Some(1) {
            return Err(DataFusionError::NotImplemented(format!(
                "Array is not type '{lower_data_type:?}'."
            )));
        } else if !lower_data_type.equals_datatype(&DataType::Null) {
            new_args.push(arg.clone());
        }
    }

    concat_internal(new_args.as_slice())
}

macro_rules! general_repeat {
    ($ELEMENT:expr, $COUNT:expr, $ARRAY_TYPE:ident) => {{
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array($ELEMENT.data_type()), $ARRAY_TYPE).clone();

        let element_array = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        for (el, c) in element_array.iter().zip($COUNT.iter()) {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty"))
            })?;
            match el {
                Some(el) => {
                    let c = if c < Some(0) { 0 } else { c.unwrap() } as usize;
                    let repeated_array =
                        [Some(el.clone())].repeat(c).iter().collect::<$ARRAY_TYPE>();

                    values = downcast_arg!(
                        compute::concat(&[&values, &repeated_array])?.clone(),
                        $ARRAY_TYPE
                    )
                    .clone();
                    offsets.push(last_offset + repeated_array.len() as i32);
                }
                None => {
                    offsets.push(last_offset);
                }
            }
        }

        let field = Arc::new(Field::new("item", $ELEMENT.data_type().clone(), true));

        Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )?)
    }};
}

macro_rules! general_repeat_list {
    ($ELEMENT:expr, $COUNT:expr, $ARRAY_TYPE:ident) => {{
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array($ELEMENT.data_type()), ListArray).clone();

        let element_array = downcast_arg!($ELEMENT, ListArray);
        for (el, c) in element_array.iter().zip($COUNT.iter()) {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty"))
            })?;
            match el {
                Some(el) => {
                    let c = if c < Some(0) { 0 } else { c.unwrap() } as usize;
                    let repeated_vec = vec![el; c];

                    let mut i: i32 = 0;
                    let mut repeated_offsets = vec![i];
                    repeated_offsets.extend(
                        repeated_vec
                            .clone()
                            .into_iter()
                            .map(|a| {
                                i += a.len() as i32;
                                i
                            })
                            .collect::<Vec<_>>(),
                    );

                    let mut repeated_values = downcast_arg!(
                        new_empty_array(&element_array.value_type()),
                        $ARRAY_TYPE
                    )
                    .clone();
                    for repeated_list in repeated_vec {
                        repeated_values = downcast_arg!(
                            compute::concat(&[&repeated_values, &repeated_list])?,
                            $ARRAY_TYPE
                        )
                        .clone();
                    }

                    let field = Arc::new(Field::new(
                        "item",
                        element_array.value_type().clone(),
                        true,
                    ));
                    let repeated_array = ListArray::try_new(
                        field,
                        OffsetBuffer::new(repeated_offsets.clone().into()),
                        Arc::new(repeated_values),
                        None,
                    )?;

                    values = downcast_arg!(
                        compute::concat(&[&values, &repeated_array,])?.clone(),
                        ListArray
                    )
                    .clone();
                    offsets.push(last_offset + repeated_array.len() as i32);
                }
                None => {
                    offsets.push(last_offset);
                }
            }
        }

        let field = Arc::new(Field::new("item", $ELEMENT.data_type().clone(), true));

        Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )?)
    }};
}

/// Array_repeat SQL function
pub fn array_repeat(args: &[ArrayRef]) -> Result<ArrayRef> {
    let element = &args[0];
    let count = as_int64_array(&args[1])?;

    let res = match element.data_type() {
        DataType::List(field) => match field.data_type() {
            DataType::List(_) => general_repeat_list!(element, count, ListArray),
            DataType::Utf8 => general_repeat_list!(element, count, StringArray),
            DataType::LargeUtf8 => general_repeat_list!(element, count, LargeStringArray),
            DataType::Boolean => general_repeat_list!(element, count, BooleanArray),
            DataType::Float32 => general_repeat_list!(element, count, Float32Array),
            DataType::Float64 => general_repeat_list!(element, count, Float64Array),
            DataType::Int8 => general_repeat_list!(element, count, Int8Array),
            DataType::Int16 => general_repeat_list!(element, count, Int16Array),
            DataType::Int32 => general_repeat_list!(element, count, Int32Array),
            DataType::Int64 => general_repeat_list!(element, count, Int64Array),
            DataType::UInt8 => general_repeat_list!(element, count, UInt8Array),
            DataType::UInt16 => general_repeat_list!(element, count, UInt16Array),
            DataType::UInt32 => general_repeat_list!(element, count, UInt32Array),
            DataType::UInt64 => general_repeat_list!(element, count, UInt64Array),
            data_type => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Array_repeat is not implemented for types 'List({data_type:?})'."
                )))
            }
        },
        DataType::Utf8 => general_repeat!(element, count, StringArray),
        DataType::LargeUtf8 => general_repeat!(element, count, LargeStringArray),
        DataType::Boolean => general_repeat!(element, count, BooleanArray),
        DataType::Float32 => general_repeat!(element, count, Float32Array),
        DataType::Float64 => general_repeat!(element, count, Float64Array),
        DataType::Int8 => general_repeat!(element, count, Int8Array),
        DataType::Int16 => general_repeat!(element, count, Int16Array),
        DataType::Int32 => general_repeat!(element, count, Int32Array),
        DataType::Int64 => general_repeat!(element, count, Int64Array),
        DataType::UInt8 => general_repeat!(element, count, UInt8Array),
        DataType::UInt16 => general_repeat!(element, count, UInt16Array),
        DataType::UInt32 => general_repeat!(element, count, UInt32Array),
        DataType::UInt64 => general_repeat!(element, count, UInt64Array),
        data_type => {
            return Err(DataFusionError::NotImplemented(format!(
                "Array_repeat is not implemented for types '{data_type:?}'."
            )))
        }
    };

    Ok(res)
}

macro_rules! position {
    ($ARRAY:expr, $ELEMENT:expr, $INDEX:expr, $ARRAY_TYPE:ident) => {{
        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        $ARRAY
            .iter()
            .zip(element.iter())
            .zip($INDEX.iter())
            .map(|((arr, el), i)| {
                let index = match i {
                    Some(i) => {
                        if i <= 0 {
                            0
                        } else {
                            i - 1
                        }
                    }
                    None => {
                        return Err(DataFusionError::Execution(
                            "initial position must not be null".to_string(),
                        ))
                    }
                };

                match arr {
                    Some(arr) => {
                        let child_array = downcast_arg!(arr, $ARRAY_TYPE);

                        match child_array
                            .iter()
                            .skip(index as usize)
                            .position(|x| x == el)
                        {
                            Some(value) => Ok(Some(value as u64 + index as u64 + 1u64)),
                            None => Ok(None),
                        }
                    }
                    None => Ok(None),
                }
            })
            .collect::<Result<UInt64Array>>()?
    }};
}

/// Array_position SQL function
pub fn array_position(args: &[ArrayRef]) -> Result<ArrayRef> {
    let arr = as_list_array(&args[0])?;
    let element = &args[1];

    let index = if args.len() == 3 {
        as_int64_array(&args[2])?.clone()
    } else {
        Int64Array::from_value(0, arr.len())
    };

    let res = match arr.value_type() {
        DataType::List(_) => position!(arr, element, index, ListArray),
        DataType::Utf8 => position!(arr, element, index, StringArray),
        DataType::LargeUtf8 => position!(arr, element, index, LargeStringArray),
        DataType::Boolean => position!(arr, element, index, BooleanArray),
        DataType::Float32 => position!(arr, element, index, Float32Array),
        DataType::Float64 => position!(arr, element, index, Float64Array),
        DataType::Int8 => position!(arr, element, index, Int8Array),
        DataType::Int16 => position!(arr, element, index, Int16Array),
        DataType::Int32 => position!(arr, element, index, Int32Array),
        DataType::Int64 => position!(arr, element, index, Int64Array),
        DataType::UInt8 => position!(arr, element, index, UInt8Array),
        DataType::UInt16 => position!(arr, element, index, UInt16Array),
        DataType::UInt32 => position!(arr, element, index, UInt32Array),
        DataType::UInt64 => position!(arr, element, index, UInt64Array),
        data_type => {
            return Err(DataFusionError::NotImplemented(format!(
                "Array_position is not implemented for types '{data_type:?}'."
            )))
        }
    };

    Ok(Arc::new(res))
}

macro_rules! positions {
    ($ARRAY:expr, $ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array(&DataType::UInt64), UInt64Array).clone();
        for comp in $ARRAY
            .iter()
            .zip(element.iter())
            .map(|(arr, el)| match arr {
                Some(arr) => {
                    let child_array = downcast_arg!(arr, $ARRAY_TYPE);
                    let res = child_array
                        .iter()
                        .enumerate()
                        .filter(|(_, x)| *x == el)
                        .flat_map(|(i, _)| Some((i + 1) as u64))
                        .collect::<UInt64Array>();

                    Ok(res)
                }
                None => Ok(downcast_arg!(
                    new_empty_array(&DataType::UInt64),
                    UInt64Array
                )
                .clone()),
            })
            .collect::<Result<Vec<UInt64Array>>>()?
        {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty",))
            })?;
            values =
                downcast_arg!(compute::concat(&[&values, &comp,])?.clone(), UInt64Array)
                    .clone();
            offsets.push(last_offset + comp.len() as i32);
        }

        let field = Arc::new(Field::new("item", DataType::UInt64, true));

        Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )?)
    }};
}

/// Array_positions SQL function
pub fn array_positions(args: &[ArrayRef]) -> Result<ArrayRef> {
    let arr = as_list_array(&args[0])?;
    let element = &args[1];

    let res = match arr.value_type() {
        DataType::List(_) => positions!(arr, element, ListArray),
        DataType::Utf8 => positions!(arr, element, StringArray),
        DataType::LargeUtf8 => positions!(arr, element, LargeStringArray),
        DataType::Boolean => positions!(arr, element, BooleanArray),
        DataType::Float32 => positions!(arr, element, Float32Array),
        DataType::Float64 => positions!(arr, element, Float64Array),
        DataType::Int8 => positions!(arr, element, Int8Array),
        DataType::Int16 => positions!(arr, element, Int16Array),
        DataType::Int32 => positions!(arr, element, Int32Array),
        DataType::Int64 => positions!(arr, element, Int64Array),
        DataType::UInt8 => positions!(arr, element, UInt8Array),
        DataType::UInt16 => positions!(arr, element, UInt16Array),
        DataType::UInt32 => positions!(arr, element, UInt32Array),
        DataType::UInt64 => positions!(arr, element, UInt64Array),
        data_type => {
            return Err(DataFusionError::NotImplemented(format!(
                "Array_positions is not implemented for types '{data_type:?}'."
            )))
        }
    };

    Ok(res)
}

macro_rules! general_remove {
    ($ARRAY:expr, $ELEMENT:expr, $MAX:expr, $ARRAY_TYPE:ident) => {{
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array($ELEMENT.data_type()), $ARRAY_TYPE).clone();

        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        for ((arr, el), max) in $ARRAY.iter().zip(element.iter()).zip($MAX.iter()) {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty"))
            })?;
            match arr {
                Some(arr) => {
                    let child_array = downcast_arg!(arr, $ARRAY_TYPE);
                    let mut counter = 0;
                    let max = if max < Some(1) { 1 } else { max.unwrap() };

                    let filter_array = child_array
                        .iter()
                        .map(|element| {
                            if counter != max && element == el {
                                counter += 1;
                                Some(false)
                            } else {
                                Some(true)
                            }
                        })
                        .collect::<BooleanArray>();

                    let filtered_array = compute::filter(&child_array, &filter_array)?;
                    values = downcast_arg!(
                        compute::concat(&[&values, &filtered_array,])?.clone(),
                        $ARRAY_TYPE
                    )
                    .clone();
                    offsets.push(last_offset + filtered_array.len() as i32);
                }
                None => offsets.push(last_offset),
            }
        }

        let field = Arc::new(Field::new("item", $ELEMENT.data_type().clone(), true));

        Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )?)
    }};
}

macro_rules! array_removement_function {
    ($FUNC:ident, $MAX_FUNC:expr, $DOC:expr) => {
        #[doc = $DOC]
        pub fn $FUNC(args: &[ArrayRef]) -> Result<ArrayRef> {
            let arr = as_list_array(&args[0])?;
            let element = &args[1];
            let max = $MAX_FUNC(args)?;

            let res = match (arr.value_type(), element.data_type()) {
                        (DataType::List(_), DataType::List(_)) => general_remove!(arr, element, max, ListArray),
                        (DataType::Utf8, DataType::Utf8) => general_remove!(arr, element, max, StringArray),
                        (DataType::LargeUtf8, DataType::LargeUtf8) => general_remove!(arr, element, max, LargeStringArray),
                        (DataType::Boolean, DataType::Boolean) => general_remove!(arr, element, max, BooleanArray),
                        (DataType::Float32, DataType::Float32) => general_remove!(arr, element, max, Float32Array),
                        (DataType::Float64, DataType::Float64) => general_remove!(arr, element, max, Float64Array),
                        (DataType::Int8, DataType::Int8) => general_remove!(arr, element, max, Int8Array),
                        (DataType::Int16, DataType::Int16) => general_remove!(arr, element, max, Int16Array),
                        (DataType::Int32, DataType::Int32) => general_remove!(arr, element, max, Int32Array),
                        (DataType::Int64, DataType::Int64) => general_remove!(arr, element, max, Int64Array),
                        (DataType::UInt8, DataType::UInt8) => general_remove!(arr, element, max, UInt8Array),
                        (DataType::UInt16, DataType::UInt16) => general_remove!(arr, element, max, UInt16Array),
                        (DataType::UInt32, DataType::UInt32) => general_remove!(arr, element, max, UInt32Array),
                        (DataType::UInt64, DataType::UInt64) => general_remove!(arr, element, max, UInt64Array),
                        (array_data_type, element_data_type) => {
                            return Err(DataFusionError::NotImplemented(format!(
                                "{} is not implemented for types '{array_data_type:?}' and '{element_data_type:?}'.",
                                stringify!($FUNC),
                            )))
                        }
            };

            Ok(res)
        }
    }
}

fn remove_one(args: &[ArrayRef]) -> Result<Int64Array> {
    Ok(Int64Array::from_value(1, args[0].len()))
}

fn remove_n(args: &[ArrayRef]) -> Result<Int64Array> {
    as_int64_array(&args[2]).cloned()
}

fn remove_all(args: &[ArrayRef]) -> Result<Int64Array> {
    Ok(Int64Array::from_value(i64::MAX, args[0].len()))
}

// array removement functions
array_removement_function!(array_remove, remove_one, "Array_remove SQL function");
array_removement_function!(array_remove_n, remove_n, "Array_remove_n SQL function");
array_removement_function!(
    array_remove_all,
    remove_all,
    "Array_remove_all SQL function"
);

macro_rules! general_replace {
    ($ARRAY:expr, $FROM:expr, $TO:expr, $MAX:expr, $ARRAY_TYPE:ident) => {{
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array($FROM.data_type()), $ARRAY_TYPE).clone();

        let from_array = downcast_arg!($FROM, $ARRAY_TYPE);
        let to_array = downcast_arg!($TO, $ARRAY_TYPE);
        for (((arr, from), to), max) in $ARRAY
            .iter()
            .zip(from_array.iter())
            .zip(to_array.iter())
            .zip($MAX.iter())
        {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty"))
            })?;
            match arr {
                Some(arr) => {
                    let child_array = downcast_arg!(arr, $ARRAY_TYPE);
                    let mut counter = 0;
                    let max = if max < Some(1) { 1 } else { max.unwrap() };

                    let replaced_array = child_array
                        .iter()
                        .map(|el| {
                            if counter != max && el == from {
                                counter += 1;
                                to
                            } else {
                                el
                            }
                        })
                        .collect::<$ARRAY_TYPE>();

                    values = downcast_arg!(
                        compute::concat(&[&values, &replaced_array])?.clone(),
                        $ARRAY_TYPE
                    )
                    .clone();
                    offsets.push(last_offset + replaced_array.len() as i32);
                }
                None => {
                    offsets.push(last_offset);
                }
            }
        }

        let field = Arc::new(Field::new("item", $FROM.data_type().clone(), true));

        Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )?)
    }};
}

macro_rules! general_replace_list {
    ($ARRAY:expr, $FROM:expr, $TO:expr, $MAX:expr, $ARRAY_TYPE:ident) => {{
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array($FROM.data_type()), ListArray).clone();

        let from_array = downcast_arg!($FROM, ListArray);
        let to_array = downcast_arg!($TO, ListArray);
        for (((arr, from), to), max) in $ARRAY
            .iter()
            .zip(from_array.iter())
            .zip(to_array.iter())
            .zip($MAX.iter())
        {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty"))
            })?;
            match arr {
                Some(arr) => {
                    let child_array = downcast_arg!(arr, ListArray);
                    let mut counter = 0;
                    let max = if max < Some(1) { 1 } else { max.unwrap() };

                    let replaced_vec = child_array
                        .iter()
                        .map(|el| {
                            if counter != max && el == from {
                                counter += 1;
                                to.clone().unwrap()
                            } else {
                                el.clone().unwrap()
                            }
                        })
                        .collect::<Vec<_>>();

                    let mut i: i32 = 0;
                    let mut replaced_offsets = vec![i];
                    replaced_offsets.extend(
                        replaced_vec
                            .clone()
                            .into_iter()
                            .map(|a| {
                                i += a.len() as i32;
                                i
                            })
                            .collect::<Vec<_>>(),
                    );

                    let mut replaced_values = downcast_arg!(
                        new_empty_array(&from_array.value_type()),
                        $ARRAY_TYPE
                    )
                    .clone();
                    for replaced_list in replaced_vec {
                        replaced_values = downcast_arg!(
                            compute::concat(&[&replaced_values, &replaced_list])?,
                            $ARRAY_TYPE
                        )
                        .clone();
                    }

                    let field = Arc::new(Field::new(
                        "item",
                        from_array.value_type().clone(),
                        true,
                    ));
                    let replaced_array = ListArray::try_new(
                        field,
                        OffsetBuffer::new(replaced_offsets.clone().into()),
                        Arc::new(replaced_values),
                        None,
                    )?;

                    values = downcast_arg!(
                        compute::concat(&[&values, &replaced_array,])?.clone(),
                        ListArray
                    )
                    .clone();
                    offsets.push(last_offset + replaced_array.len() as i32);
                }
                None => {
                    offsets.push(last_offset);
                }
            }
        }

        let field = Arc::new(Field::new("item", $FROM.data_type().clone(), true));

        Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )?)
    }};
}

macro_rules! array_replacement_function {
    ($FUNC:ident, $MAX_FUNC:expr, $DOC:expr) => {
        #[doc = $DOC]
        pub fn $FUNC(args: &[ArrayRef]) -> Result<ArrayRef> {
            let arr = as_list_array(&args[0])?;
            let from = &args[1];
            let to = &args[2];
            let max = $MAX_FUNC(args)?;

            let res = match (arr.value_type(), from.data_type(), to.data_type()) {
                        (DataType::List(afield), DataType::List(ffield), DataType::List(tfield)) => {
                            match (afield.data_type(), ffield.data_type(), tfield.data_type()) {
                                (DataType::List(_), DataType::List(_), DataType::List(_)) => general_replace_list!(arr, from, to, max, ListArray),
                                (DataType::Utf8, DataType::Utf8, DataType::Utf8) => general_replace_list!(arr, from, to, max, StringArray),
                                (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => general_replace_list!(arr, from, to, max, LargeStringArray),
                                (DataType::Boolean, DataType::Boolean, DataType::Boolean) => general_replace_list!(arr, from, to, max, BooleanArray),
                                (DataType::Float32, DataType::Float32, DataType::Float32) => general_replace_list!(arr, from, to, max, Float32Array),
                                (DataType::Float64, DataType::Float64, DataType::Float64) => general_replace_list!(arr, from, to, max, Float64Array),
                                (DataType::Int8, DataType::Int8, DataType::Int8) => general_replace_list!(arr, from, to, max, Int8Array),
                                (DataType::Int16, DataType::Int16, DataType::Int16) => general_replace_list!(arr, from, to, max, Int16Array),
                                (DataType::Int32, DataType::Int32, DataType::Int32) => general_replace_list!(arr, from, to, max, Int32Array),
                                (DataType::Int64, DataType::Int64, DataType::Int64) => general_replace_list!(arr, from, to, max, Int64Array),
                                (DataType::UInt8, DataType::UInt8, DataType::UInt8) => general_replace_list!(arr, from, to, max, UInt8Array),
                                (DataType::UInt16, DataType::UInt16, DataType::UInt16) => general_replace_list!(arr, from, to, max, UInt16Array),
                                (DataType::UInt32, DataType::UInt32, DataType::UInt32) => general_replace_list!(arr, from, to, max, UInt32Array),
                                (DataType::UInt64, DataType::UInt64, DataType::UInt64) => general_replace_list!(arr, from, to, max, UInt64Array),
                                (array_data_type, from_data_type, to_data_type) => {
                                    return Err(DataFusionError::NotImplemented(format!(
                                        "{} is not implemented for types 'List({array_data_type:?})', 'List({from_data_type:?})' and 'List({to_data_type:?})'.",
                                        stringify!($FUNC),
                                    )))
                                }
                            }
                        }
                        (DataType::Utf8, DataType::Utf8, DataType::Utf8) => general_replace!(arr, from, to, max, StringArray),
                        (DataType::LargeUtf8, DataType::LargeUtf8, DataType::LargeUtf8) => general_replace!(arr, from, to, max, LargeStringArray),
                        (DataType::Boolean, DataType::Boolean, DataType::Boolean) => general_replace!(arr, from, to, max, BooleanArray),
                        (DataType::Float32, DataType::Float32, DataType::Float32) => general_replace!(arr, from, to, max, Float32Array),
                        (DataType::Float64, DataType::Float64, DataType::Float64) => general_replace!(arr, from, to, max, Float64Array),
                        (DataType::Int8, DataType::Int8, DataType::Int8) => general_replace!(arr, from, to, max, Int8Array),
                        (DataType::Int16, DataType::Int16, DataType::Int16) => general_replace!(arr, from, to, max, Int16Array),
                        (DataType::Int32, DataType::Int32, DataType::Int32) => general_replace!(arr, from, to, max, Int32Array),
                        (DataType::Int64, DataType::Int64, DataType::Int64) => general_replace!(arr, from, to, max, Int64Array),
                        (DataType::UInt8, DataType::UInt8, DataType::UInt8) => general_replace!(arr, from, to, max, UInt8Array),
                        (DataType::UInt16, DataType::UInt16, DataType::UInt16) => general_replace!(arr, from, to, max, UInt16Array),
                        (DataType::UInt32, DataType::UInt32, DataType::UInt32) => general_replace!(arr, from, to, max, UInt32Array),
                        (DataType::UInt64, DataType::UInt64, DataType::UInt64) => general_replace!(arr, from, to, max, UInt64Array),
                        (array_data_type, from_data_type, to_data_type) => {
                            return Err(DataFusionError::NotImplemented(format!(
                                "{} is not implemented for types '{array_data_type:?}', '{from_data_type:?}' and '{to_data_type:?}'.",
                                stringify!($FUNC),
                            )))
                        }
            };

            Ok(res)
        }
    };
}

fn replace_one(args: &[ArrayRef]) -> Result<Int64Array> {
    Ok(Int64Array::from_value(1, args[0].len()))
}

fn replace_n(args: &[ArrayRef]) -> Result<Int64Array> {
    as_int64_array(&args[3]).cloned()
}

fn replace_all(args: &[ArrayRef]) -> Result<Int64Array> {
    Ok(Int64Array::from_value(i64::MAX, args[0].len()))
}

// array replacement functions
array_replacement_function!(array_replace, replace_one, "Array_replace SQL function");
array_replacement_function!(array_replace_n, replace_n, "Array_replace_n SQL function");
array_replacement_function!(
    array_replace_all,
    replace_all,
    "Array_replace_all SQL function"
);

macro_rules! to_string {
    ($ARG:expr, $ARRAY:expr, $DELIMETER:expr, $NULL_STRING:expr, $WITH_NULL_STRING:expr, $ARRAY_TYPE:ident) => {{
        let arr = downcast_arg!($ARRAY, $ARRAY_TYPE);
        for x in arr {
            match x {
                Some(x) => {
                    $ARG.push_str(&x.to_string());
                    $ARG.push_str($DELIMETER);
                }
                None => {
                    if $WITH_NULL_STRING {
                        $ARG.push_str($NULL_STRING);
                        $ARG.push_str($DELIMETER);
                    }
                }
            }
        }
        Ok($ARG)
    }};
}

/// Array_to_string SQL function
pub fn array_to_string(args: &[ArrayRef]) -> Result<ArrayRef> {
    let arr = &args[0];

    let delimeters = as_generic_string_array::<i32>(&args[1])?;
    let delimeters: Vec<Option<&str>> = delimeters.iter().collect();

    let mut null_string = String::from("");
    let mut with_null_string = false;
    if args.len() == 3 {
        null_string = as_generic_string_array::<i32>(&args[2])?
            .value(0)
            .to_string();
        with_null_string = true;
    }

    fn compute_array_to_string(
        arg: &mut String,
        arr: ArrayRef,
        delimeter: String,
        null_string: String,
        with_null_string: bool,
    ) -> Result<&mut String> {
        match arr.data_type() {
            DataType::List(..) => {
                let list_array = downcast_arg!(arr, ListArray);

                for i in 0..list_array.len() {
                    compute_array_to_string(
                        arg,
                        list_array.value(i),
                        delimeter.clone(),
                        null_string.clone(),
                        with_null_string,
                    )?;
                }

                Ok(arg)
            }
            DataType::Utf8 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                StringArray
            ),
            DataType::LargeUtf8 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                LargeStringArray
            ),
            DataType::Boolean => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                BooleanArray
            ),
            DataType::Float32 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                Float32Array
            ),
            DataType::Float64 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                Float64Array
            ),
            DataType::Int8 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                Int8Array
            ),
            DataType::Int16 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                Int16Array
            ),
            DataType::Int32 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                Int32Array
            ),
            DataType::Int64 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                Int64Array
            ),
            DataType::UInt8 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                UInt8Array
            ),
            DataType::UInt16 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                UInt16Array
            ),
            DataType::UInt32 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                UInt32Array
            ),
            DataType::UInt64 => to_string!(
                arg,
                arr,
                &delimeter,
                &null_string,
                with_null_string,
                UInt64Array
            ),
            DataType::Null => Ok(arg),
            data_type => Err(DataFusionError::NotImplemented(format!(
                "Array is not implemented for type '{data_type:?}'."
            ))),
        }
    }

    let mut arg = String::from("");
    let mut res: Vec<Option<String>> = Vec::new();

    match arr.data_type() {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let list_array = arr.as_list::<i32>();
            for (arr, &delimeter) in list_array.iter().zip(delimeters.iter()) {
                if let (Some(arr), Some(delimeter)) = (arr, delimeter) {
                    arg = String::from("");
                    let s = compute_array_to_string(
                        &mut arg,
                        arr,
                        delimeter.to_string(),
                        null_string.clone(),
                        with_null_string,
                    )?
                    .clone();

                    if let Some(s) = s.strip_suffix(delimeter) {
                        res.push(Some(s.to_string()));
                    } else {
                        res.push(Some(s));
                    }
                } else {
                    res.push(None);
                }
            }
        }
        _ => {
            // delimeter length is 1
            assert_eq!(delimeters.len(), 1);
            let delimeter = delimeters[0].unwrap();
            let s = compute_array_to_string(
                &mut arg,
                arr.clone(),
                delimeter.to_string(),
                null_string,
                with_null_string,
            )?
            .clone();

            if !s.is_empty() {
                let s = s.strip_suffix(delimeter).unwrap().to_string();
                res.push(Some(s));
            } else {
                res.push(Some(s));
            }
        }
    }

    Ok(Arc::new(StringArray::from(res)))
}

/// Cardinality SQL function
pub fn cardinality(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_list_array(&args[0])?.clone();

    let result = list_array
        .iter()
        .map(|arr| match compute_array_dims(arr)? {
            Some(vector) => Ok(Some(vector.iter().map(|x| x.unwrap()).product::<u64>())),
            None => Ok(None),
        })
        .collect::<Result<UInt64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

// Create new offsets that are euqiavlent to `flatten` the array.
fn get_offsets_for_flatten(
    offsets: OffsetBuffer<i32>,
    indexes: OffsetBuffer<i32>,
) -> OffsetBuffer<i32> {
    let buffer = offsets.into_inner();
    let offsets: Vec<i32> = indexes.iter().map(|i| buffer[*i as usize]).collect();
    OffsetBuffer::new(offsets.into())
}

fn flatten_internal(
    array: &dyn Array,
    indexes: Option<OffsetBuffer<i32>>,
) -> Result<ListArray> {
    let list_arr = as_list_array(array)?;
    let (field, offsets, values, nulls) = list_arr.clone().into_parts();
    let data_type = field.data_type();

    match data_type {
        // Recursively get the base offsets for flattened array
        DataType::List(_) => {
            if let Some(indexes) = indexes {
                let offsets = get_offsets_for_flatten(offsets, indexes);
                flatten_internal(&values, Some(offsets))
            } else {
                flatten_internal(&values, Some(offsets))
            }
        }
        // Reach the base level, create a new list array
        _ => {
            if let Some(indexes) = indexes {
                let offsets = get_offsets_for_flatten(offsets, indexes);
                let list_arr = ListArray::new(field, offsets, values, nulls);
                Ok(list_arr)
            } else {
                Ok(list_arr.clone())
            }
        }
    }
}

/// Flatten SQL function
pub fn flatten(args: &[ArrayRef]) -> Result<ArrayRef> {
    let flattened_array = flatten_internal(&args[0], None)?;
    Ok(Arc::new(flattened_array) as ArrayRef)
}

/// Array_length SQL function
pub fn array_length(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_list_array(&args[0])?;
    let dimension = if args.len() == 2 {
        as_int64_array(&args[1])?.clone()
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

/// Array_dims SQL function
pub fn array_dims(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_list_array(&args[0])?;

    let data = list_array
        .iter()
        .map(compute_array_dims)
        .collect::<Result<Vec<_>>>()?;
    let result = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

    Ok(Arc::new(result) as ArrayRef)
}

/// Array_ndims SQL function
pub fn array_ndims(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_list_array(&args[0])?;

    let result = list_array
        .iter()
        .map(compute_array_ndims)
        .collect::<Result<UInt64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

macro_rules! non_list_contains {
    ($ARRAY:expr, $SUB_ARRAY:expr, $ARRAY_TYPE:ident) => {{
        let sub_array = downcast_arg!($SUB_ARRAY, $ARRAY_TYPE);
        let mut boolean_builder = BooleanArray::builder($ARRAY.len());

        for (arr, elem) in $ARRAY.iter().zip(sub_array.iter()) {
            if let (Some(arr), Some(elem)) = (arr, elem) {
                let arr = downcast_arg!(arr, $ARRAY_TYPE);
                let res = arr.iter().dedup().flatten().any(|x| x == elem);
                boolean_builder.append_value(res);
            }
        }
        Ok(Arc::new(boolean_builder.finish()))
    }};
}

/// Array_has SQL function
pub fn array_has(args: &[ArrayRef]) -> Result<ArrayRef> {
    assert_eq!(args.len(), 2);

    let array = args[0].as_list::<i32>();

    match args[1].data_type() {
        DataType::List(_) => {
            let sub_array = args[1].as_list::<i32>();
            let mut boolean_builder = BooleanArray::builder(array.len());

            for (arr, elem) in array.iter().zip(sub_array.iter()) {
                if let (Some(arr), Some(elem)) = (arr, elem) {
                    let list_arr = arr.as_list::<i32>();
                    let res = list_arr.iter().dedup().flatten().any(|x| *x == *elem);
                    boolean_builder.append_value(res);
                }
            }
            Ok(Arc::new(boolean_builder.finish()))
        }

        // Int64, Int32, Int16, Int8
        // UInt64, UInt32, UInt16, UInt8
        DataType::Int64 => {
            non_list_contains!(array, args[1], Int64Array)
        }
        DataType::Int32 => {
            non_list_contains!(array, args[1], Int32Array)
        }
        DataType::Int16 => {
            non_list_contains!(array, args[1], Int16Array)
        }
        DataType::Int8 => {
            non_list_contains!(array, args[1], Int8Array)
        }
        DataType::UInt64 => {
            non_list_contains!(array, args[1], UInt64Array)
        }
        DataType::UInt32 => {
            non_list_contains!(array, args[1], UInt32Array)
        }
        DataType::UInt16 => {
            non_list_contains!(array, args[1], UInt16Array)
        }
        DataType::UInt8 => {
            non_list_contains!(array, args[1], UInt8Array)
        }

        DataType::Float64 => {
            non_list_contains!(array, args[1], Float64Array)
        }
        DataType::Float32 => {
            non_list_contains!(array, args[1], Float32Array)
        }
        DataType::Utf8 => {
            non_list_contains!(array, args[1], StringArray)
        }
        DataType::LargeUtf8 => {
            non_list_contains!(array, args[1], LargeStringArray)
        }
        DataType::Boolean => {
            non_list_contains!(array, args[1], BooleanArray)
        }
        data_type => Err(DataFusionError::NotImplemented(format!(
            "Array_has is not implemented for '{data_type:?}'"
        ))),
    }
}

macro_rules! array_has_any_non_list_check {
    ($ARRAY:expr, $SUB_ARRAY:expr, $ARRAY_TYPE:ident) => {{
        let arr = downcast_arg!($ARRAY, $ARRAY_TYPE);
        let sub_arr = downcast_arg!($SUB_ARRAY, $ARRAY_TYPE);

        let mut res = false;
        for elem in sub_arr.iter().dedup() {
            if let Some(elem) = elem {
                res |= arr.iter().dedup().flatten().any(|x| x == elem);
            } else {
                return internal_err!(
                    "array_has_any does not support Null type for element in sub_array"
                );
            }
        }
        res
    }};
}

/// Array_has_any SQL function
pub fn array_has_any(args: &[ArrayRef]) -> Result<ArrayRef> {
    assert_eq!(args.len(), 2);

    let array = args[0].as_list::<i32>();
    let sub_array = args[1].as_list::<i32>();

    let mut boolean_builder = BooleanArray::builder(array.len());
    for (arr, sub_arr) in array.iter().zip(sub_array.iter()) {
        if let (Some(arr), Some(sub_arr)) = (arr, sub_arr) {
            let res = match (arr.data_type(), sub_arr.data_type()) {
                (DataType::List(_), DataType::List(_)) => {
                    let arr = downcast_arg!(arr, ListArray);
                    let sub_arr = downcast_arg!(sub_arr, ListArray);

                    let mut res = false;
                    for elem in sub_arr.iter().dedup().flatten() {
                        res |= arr.iter().dedup().flatten().any(|x| *x == *elem);
                    }
                    res
                }
                // Int64, Int32, Int16, Int8
                // UInt64, UInt32, UInt16, UInt8
                (DataType::Int64, DataType::Int64) => {
                    array_has_any_non_list_check!(arr, sub_arr, Int64Array)
                }
                (DataType::Int32, DataType::Int32) => {
                    array_has_any_non_list_check!(arr, sub_arr, Int32Array)
                }
                (DataType::Int16, DataType::Int16) => {
                    array_has_any_non_list_check!(arr, sub_arr, Int16Array)
                }
                (DataType::Int8, DataType::Int8) => {
                    array_has_any_non_list_check!(arr, sub_arr, Int8Array)
                }
                (DataType::UInt64, DataType::UInt64) => {
                    array_has_any_non_list_check!(arr, sub_arr, UInt64Array)
                }
                (DataType::UInt32, DataType::UInt32) => {
                    array_has_any_non_list_check!(arr, sub_arr, UInt32Array)
                }
                (DataType::UInt16, DataType::UInt16) => {
                    array_has_any_non_list_check!(arr, sub_arr, UInt16Array)
                }
                (DataType::UInt8, DataType::UInt8) => {
                    array_has_any_non_list_check!(arr, sub_arr, UInt8Array)
                }

                (DataType::Float64, DataType::Float64) => {
                    array_has_any_non_list_check!(arr, sub_arr, Float64Array)
                }
                (DataType::Float32, DataType::Float32) => {
                    array_has_any_non_list_check!(arr, sub_arr, Float32Array)
                }
                (DataType::Boolean, DataType::Boolean) => {
                    array_has_any_non_list_check!(arr, sub_arr, BooleanArray)
                }
                // Utf8, LargeUtf8
                (DataType::Utf8, DataType::Utf8) => {
                    array_has_any_non_list_check!(arr, sub_arr, StringArray)
                }
                (DataType::LargeUtf8, DataType::LargeUtf8) => {
                    array_has_any_non_list_check!(arr, sub_arr, LargeStringArray)
                }

                (arr_type, sub_arr_type) => Err(DataFusionError::NotImplemented(format!(
                    "Array_has_any is not implemented for '{arr_type:?}' and '{sub_arr_type:?}'",
                )))?,
            };
            boolean_builder.append_value(res);
        }
    }
    Ok(Arc::new(boolean_builder.finish()))
}

macro_rules! array_has_all_non_list_check {
    ($ARRAY:expr, $SUB_ARRAY:expr, $ARRAY_TYPE:ident) => {{
        let arr = downcast_arg!($ARRAY, $ARRAY_TYPE);
        let sub_arr = downcast_arg!($SUB_ARRAY, $ARRAY_TYPE);

        let mut res = true;
        for elem in sub_arr.iter().dedup() {
            if let Some(elem) = elem {
                res &= arr.iter().dedup().flatten().any(|x| x == elem);
            } else {
                return internal_err!(
                    "array_has_all does not support Null type for element in sub_array"
                );
            }
        }
        res
    }};
}

/// Array_has_all SQL function
pub fn array_has_all(args: &[ArrayRef]) -> Result<ArrayRef> {
    assert_eq!(args.len(), 2);

    let array = args[0].as_list::<i32>();
    let sub_array = args[1].as_list::<i32>();

    let mut boolean_builder = BooleanArray::builder(array.len());
    for (arr, sub_arr) in array.iter().zip(sub_array.iter()) {
        if let (Some(arr), Some(sub_arr)) = (arr, sub_arr) {
            let res = match (arr.data_type(), sub_arr.data_type()) {
                (DataType::List(_), DataType::List(_)) => {
                    let arr = downcast_arg!(arr, ListArray);
                    let sub_arr = downcast_arg!(sub_arr, ListArray);

                    let mut res = true;
                    for elem in sub_arr.iter().dedup().flatten() {
                        res &= arr.iter().dedup().flatten().any(|x| *x == *elem);
                    }
                    res
                }
                // Int64, Int32, Int16, Int8
                // UInt64, UInt32, UInt16, UInt8
                (DataType::Int64, DataType::Int64) => {
                    array_has_all_non_list_check!(arr, sub_arr, Int64Array)
                }
                (DataType::Int32, DataType::Int32) => {
                    array_has_all_non_list_check!(arr, sub_arr, Int32Array)
                }
                (DataType::Int16, DataType::Int16) => {
                    array_has_all_non_list_check!(arr, sub_arr, Int16Array)
                }
                (DataType::Int8, DataType::Int8) => {
                    array_has_all_non_list_check!(arr, sub_arr, Int8Array)
                }
                (DataType::UInt64, DataType::UInt64) => {
                    array_has_all_non_list_check!(arr, sub_arr, UInt64Array)
                }
                (DataType::UInt32, DataType::UInt32) => {
                    array_has_all_non_list_check!(arr, sub_arr, UInt32Array)
                }
                (DataType::UInt16, DataType::UInt16) => {
                    array_has_all_non_list_check!(arr, sub_arr, UInt16Array)
                }
                (DataType::UInt8, DataType::UInt8) => {
                    array_has_all_non_list_check!(arr, sub_arr, UInt8Array)
                }

                (DataType::Float64, DataType::Float64) => {
                    array_has_all_non_list_check!(arr, sub_arr, Float64Array)
                }
                (DataType::Float32, DataType::Float32) => {
                    array_has_all_non_list_check!(arr, sub_arr, Float32Array)
                }
                (DataType::Boolean, DataType::Boolean) => {
                    array_has_all_non_list_check!(arr, sub_arr, BooleanArray)
                }
                (DataType::Utf8, DataType::Utf8) => {
                    array_has_all_non_list_check!(arr, sub_arr, StringArray)
                }
                (DataType::LargeUtf8, DataType::LargeUtf8) => {
                    array_has_all_non_list_check!(arr, sub_arr, LargeStringArray)
                }
                (arr_type, sub_arr_type) => Err(DataFusionError::NotImplemented(format!(
                    "Array_has_all is not implemented for '{arr_type:?}' and '{sub_arr_type:?}'",
                )))?,
            };
            boolean_builder.append_value(res);
        }
    }
    Ok(Arc::new(boolean_builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Int64Type;
    use datafusion_common::cast::{
        as_generic_string_array, as_list_array, as_uint64_array,
    };
    use datafusion_common::scalar::ScalarValue;

    #[test]
    fn test_array() {
        // make_array(1, 2, 3) = [1, 2, 3]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let array = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        let result = as_list_array(&array).expect("failed to initialize function array");
        assert_eq!(result.len(), 1);
        assert_eq!(
            &[1, 2, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        )
    }

    #[test]
    fn test_nested_array() {
        // make_array([1, 3, 5], [2, 4, 6]) = [[1, 3, 5], [2, 4, 6]]
        let args = [
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![3, 4]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![5, 6]))),
        ];
        let array = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        let result = as_list_array(&array).expect("failed to initialize function array");
        assert_eq!(result.len(), 2);
        assert_eq!(
            &[1, 3, 5],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
        assert_eq!(
            &[2, 4, 6],
            result
                .value(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_element() {
        // array_element([1, 2, 3, 4], 1) = 1
        let list_array = return_array().into_array(1);
        let arr = array_element(&[list_array, Arc::new(Int64Array::from_value(1, 1))])
            .expect("failed to initialize function array_element");
        let result =
            as_int64_array(&arr).expect("failed to initialize function array_element");

        assert_eq!(result, &Int64Array::from_value(1, 1));

        // array_element([1, 2, 3, 4], 3) = 3
        let list_array = return_array().into_array(1);
        let arr = array_element(&[list_array, Arc::new(Int64Array::from_value(3, 1))])
            .expect("failed to initialize function array_element");
        let result =
            as_int64_array(&arr).expect("failed to initialize function array_element");

        assert_eq!(result, &Int64Array::from_value(3, 1));

        // array_element([1, 2, 3, 4], 0) = NULL
        let list_array = return_array().into_array(1);
        let arr = array_element(&[list_array, Arc::new(Int64Array::from_value(0, 1))])
            .expect("failed to initialize function array_element");
        let result =
            as_int64_array(&arr).expect("failed to initialize function array_element");

        assert_eq!(result, &Int64Array::from(vec![None]));

        // array_element([1, 2, 3, 4], NULL) = NULL
        let list_array = return_array().into_array(1);
        let arr = array_element(&[list_array, Arc::new(Int64Array::from(vec![None]))])
            .expect("failed to initialize function array_element");
        let result =
            as_int64_array(&arr).expect("failed to initialize function array_element");

        assert_eq!(result, &Int64Array::from(vec![None]));

        // array_element([1, 2, 3, 4], -1) = 4
        let list_array = return_array().into_array(1);
        let arr = array_element(&[list_array, Arc::new(Int64Array::from_value(-1, 1))])
            .expect("failed to initialize function array_element");
        let result =
            as_int64_array(&arr).expect("failed to initialize function array_element");

        assert_eq!(result, &Int64Array::from_value(4, 1));

        // array_element([1, 2, 3, 4], -3) = 2
        let list_array = return_array().into_array(1);
        let arr = array_element(&[list_array, Arc::new(Int64Array::from_value(-3, 1))])
            .expect("failed to initialize function array_element");
        let result =
            as_int64_array(&arr).expect("failed to initialize function array_element");

        assert_eq!(result, &Int64Array::from_value(2, 1));

        // array_element([1, 2, 3, 4], 10) = NULL
        let list_array = return_array().into_array(1);
        let arr = array_element(&[list_array, Arc::new(Int64Array::from_value(10, 1))])
            .expect("failed to initialize function array_element");
        let result =
            as_int64_array(&arr).expect("failed to initialize function array_element");

        assert_eq!(result, &Int64Array::from(vec![None]));
    }

    #[test]
    fn test_nested_array_element() {
        // array_element([[1, 2, 3, 4], [5, 6, 7, 8]], 2) = [5, 6, 7, 8]
        let list_array = return_nested_array().into_array(1);
        let arr = array_element(&[list_array, Arc::new(Int64Array::from_value(2, 1))])
            .expect("failed to initialize function array_element");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_element");

        assert_eq!(
            &[5, 6, 7, 8],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_slice() {
        // array_slice([1, 2, 3, 4], 1, 3) = [1, 2, 3]
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(1, 1)),
            Arc::new(Int64Array::from_value(3, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert_eq!(
            &[1, 2, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // array_slice([1, 2, 3, 4], 2, 2) = [2]
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(2, 1)),
            Arc::new(Int64Array::from_value(2, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert_eq!(
            &[2],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // array_slice([1, 2, 3, 4], 0, 0) = []
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(0, 1)),
            Arc::new(Int64Array::from_value(0, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert!(result
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_empty());

        // array_slice([1, 2, 3, 4], 0, 6) = [1, 2, 3, 4]
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(0, 1)),
            Arc::new(Int64Array::from_value(6, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert_eq!(
            &[1, 2, 3, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // array_slice([1, 2, 3, 4], -2, -2) = []
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(-2, 1)),
            Arc::new(Int64Array::from_value(-2, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert!(result
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_empty());

        // array_slice([1, 2, 3, 4], -3, -1) = [2, 3]
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(-3, 1)),
            Arc::new(Int64Array::from_value(-1, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert_eq!(
            &[2, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // array_slice([1, 2, 3, 4], -3, 2) = [2]
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(-3, 1)),
            Arc::new(Int64Array::from_value(2, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert_eq!(
            &[2],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // array_slice([1, 2, 3, 4], 2, 11) = [2, 3, 4]
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(2, 1)),
            Arc::new(Int64Array::from_value(11, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert_eq!(
            &[2, 3, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // array_slice([1, 2, 3, 4], 3, 1) = []
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(3, 1)),
            Arc::new(Int64Array::from_value(1, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert!(result
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_empty());

        // array_slice([1, 2, 3, 4], -7, -2) = NULL
        let list_array = return_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(-7, 1)),
            Arc::new(Int64Array::from_value(-2, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert!(result
            .value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn test_nested_array_slice() {
        // array_slice([[1, 2, 3, 4], [5, 6, 7, 8]], 1, 1) = [[1, 2, 3, 4]]
        let list_array = return_nested_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(1, 1)),
            Arc::new(Int64Array::from_value(1, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert_eq!(
            &[1, 2, 3, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // array_slice([[1, 2, 3, 4], [5, 6, 7, 8]], -1, -1) = []
        let list_array = return_nested_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(-1, 1)),
            Arc::new(Int64Array::from_value(-1, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert!(result
            .value(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .is_empty());

        // array_slice([[1, 2, 3, 4], [5, 6, 7, 8]], -1, 2) = [[5, 6, 7, 8]]
        let list_array = return_nested_array().into_array(1);
        let arr = array_slice(&[
            list_array,
            Arc::new(Int64Array::from_value(-1, 1)),
            Arc::new(Int64Array::from_value(2, 1)),
        ])
        .expect("failed to initialize function array_slice");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_slice");

        assert_eq!(
            &[5, 6, 7, 8],
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_append() {
        // array_append([1, 2, 3], 4) = [1, 2, 3, 4]
        let data = vec![Some(vec![Some(1), Some(2), Some(3)])];
        let list_array =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(data)) as ArrayRef;
        let int64_array = Arc::new(Int64Array::from(vec![Some(4)])) as ArrayRef;

        let args = [list_array, int64_array];

        let array =
            array_append(&args).expect("failed to initialize function array_append");
        let result =
            as_list_array(&array).expect("failed to initialize function array_append");

        assert_eq!(
            &[1, 2, 3, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_prepend() {
        // array_prepend(1, [2, 3, 4]) = [1, 2, 3, 4]
        let data = vec![Some(vec![Some(2), Some(3), Some(4)])];
        let list_array =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(data)) as ArrayRef;
        let int64_array = Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef;

        let args = [int64_array, list_array];

        let array =
            array_prepend(&args).expect("failed to initialize function array_append");
        let result =
            as_list_array(&array).expect("failed to initialize function array_append");

        assert_eq!(
            &[1, 2, 3, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_concat() {
        // array_concat([1, 2, 3], [4, 5, 6], [7, 8, 9]) = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        let data = vec![Some(vec![Some(1), Some(2), Some(3)])];
        let list_array1 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(data)) as ArrayRef;
        let data = vec![Some(vec![Some(4), Some(5), Some(6)])];
        let list_array2 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(data)) as ArrayRef;
        let data = vec![Some(vec![Some(7), Some(8), Some(9)])];
        let list_array3 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(data)) as ArrayRef;

        let args = [list_array1, list_array2, list_array3];

        let array =
            array_concat(&args).expect("failed to initialize function array_concat");
        let result =
            as_list_array(&array).expect("failed to initialize function array_concat");

        assert_eq!(
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_concat() {
        // array_concat([1, 2, 3, 4], [1, 2, 3, 4]) = [1, 2, 3, 4, 1, 2, 3, 4]
        let list_array = return_array().into_array(1);
        let arr = array_concat(&[list_array.clone(), list_array.clone()])
            .expect("failed to initialize function array_concat");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_concat");

        assert_eq!(
            &[1, 2, 3, 4, 1, 2, 3, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // array_concat([[1, 2, 3, 4], [5, 6, 7, 8]], [1, 2, 3, 4]) = [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4]]
        let list_nested_array = return_nested_array().into_array(1);
        let list_array = return_array().into_array(1);
        let arr = array_concat(&[list_nested_array, list_array])
            .expect("failed to initialize function array_concat");
        let result =
            as_list_array(&arr).expect("failed to initialize function array_concat");

        assert_eq!(
            &[1, 2, 3, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_position() {
        // array_position([1, 2, 3, 4], 3) = 3
        let list_array = return_array().into_array(1);
        let array = array_position(&[list_array, Arc::new(Int64Array::from_value(3, 1))])
            .expect("failed to initialize function array_position");
        let result = as_uint64_array(&array)
            .expect("failed to initialize function array_position");

        assert_eq!(result, &UInt64Array::from(vec![3]));
    }

    #[test]
    fn test_array_positions() {
        // array_positions([1, 2, 3, 4], 3) = [3]
        let list_array = return_array().into_array(1);
        let array =
            array_positions(&[list_array, Arc::new(Int64Array::from_value(3, 1))])
                .expect("failed to initialize function array_position");
        let result =
            as_list_array(&array).expect("failed to initialize function array_position");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_remove() {
        // array_remove([3, 1, 2, 3, 2, 3], 3) = [1, 2, 3, 2, 3]
        let list_array = return_array_with_repeating_elements().into_array(1);
        let array = array_remove(&[list_array, Arc::new(Int64Array::from_value(3, 1))])
            .expect("failed to initialize function array_remove");
        let result =
            as_list_array(&array).expect("failed to initialize function array_remove");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[1, 2, 3, 2, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_remove() {
        // array_remove(
        //     [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]],
        //     [1, 2, 3, 4],
        // ) = [[5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]]
        let list_array = return_nested_array_with_repeating_elements().into_array(1);
        let element_array = return_array().into_array(1);
        let array = array_remove(&[list_array, element_array])
            .expect("failed to initialize function array_remove");
        let result =
            as_list_array(&array).expect("failed to initialize function array_remove");

        assert_eq!(result.len(), 1);
        let data = vec![
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
            Some(vec![Some(1), Some(2), Some(3), Some(4)]),
            Some(vec![Some(9), Some(10), Some(11), Some(12)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
        ];
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(data);
        assert_eq!(
            expected,
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .clone()
        );
    }

    #[test]
    fn test_array_remove_n() {
        // array_remove_n([3, 1, 2, 3, 2, 3], 3, 2) = [1, 2, 2, 3]
        let list_array = return_array_with_repeating_elements().into_array(1);
        let array = array_remove_n(&[
            list_array,
            Arc::new(Int64Array::from_value(3, 1)),
            Arc::new(Int64Array::from_value(2, 1)),
        ])
        .expect("failed to initialize function array_remove_n");
        let result =
            as_list_array(&array).expect("failed to initialize function array_remove_n");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[1, 2, 2, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_remove_n() {
        // array_remove_n(
        //     [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]],
        //     [1, 2, 3, 4],
        //     3,
        // ) = [[5, 6, 7, 8], [9, 10, 11, 12], [5, 6, 7, 8]]
        let list_array = return_nested_array_with_repeating_elements().into_array(1);
        let element_array = return_array().into_array(1);
        let array = array_remove_n(&[
            list_array,
            element_array,
            Arc::new(Int64Array::from_value(3, 1)),
        ])
        .expect("failed to initialize function array_remove_n");
        let result =
            as_list_array(&array).expect("failed to initialize function array_remove_n");

        assert_eq!(result.len(), 1);
        let data = vec![
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
            Some(vec![Some(9), Some(10), Some(11), Some(12)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
        ];
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(data);
        assert_eq!(
            expected,
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .clone()
        );
    }

    #[test]
    fn test_array_remove_all() {
        // array_remove_all([3, 1, 2, 3, 2, 3], 3) = [1, 2, 2]
        let list_array = return_array_with_repeating_elements().into_array(1);
        let array =
            array_remove_all(&[list_array, Arc::new(Int64Array::from_value(3, 1))])
                .expect("failed to initialize function array_remove_all");
        let result = as_list_array(&array)
            .expect("failed to initialize function array_remove_all");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[1, 2, 2],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_remove_all() {
        // array_remove_all(
        //     [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]],
        //     [1, 2, 3, 4],
        // ) = [[5, 6, 7, 8], [9, 10, 11, 12], [5, 6, 7, 8]]
        let list_array = return_nested_array_with_repeating_elements().into_array(1);
        let element_array = return_array().into_array(1);
        let array = array_remove_all(&[list_array, element_array])
            .expect("failed to initialize function array_remove_all");
        let result = as_list_array(&array)
            .expect("failed to initialize function array_remove_all");

        assert_eq!(result.len(), 1);
        let data = vec![
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
            Some(vec![Some(9), Some(10), Some(11), Some(12)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
        ];
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(data);
        assert_eq!(
            expected,
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .clone()
        );
    }

    #[test]
    fn test_array_replace() {
        // array_replace([3, 1, 2, 3, 2, 3], 3, 4) = [4, 1, 2, 3, 2, 3]
        let list_array = return_array_with_repeating_elements().into_array(1);
        let array = array_replace(&[
            list_array,
            Arc::new(Int64Array::from_value(3, 1)),
            Arc::new(Int64Array::from_value(4, 1)),
        ])
        .expect("failed to initialize function array_replace");
        let result =
            as_list_array(&array).expect("failed to initialize function array_replace");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[4, 1, 2, 3, 2, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_replace() {
        // array_replace(
        //     [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]],
        //     [1, 2, 3, 4],
        //     [11, 12, 13, 14],
        // ) = [[11, 12, 13, 14], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]]
        let list_array = return_nested_array_with_repeating_elements().into_array(1);
        let from_array = return_array().into_array(1);
        let to_array = return_extra_array().into_array(1);
        let array = array_replace(&[list_array, from_array, to_array])
            .expect("failed to initialize function array_replace");
        let result =
            as_list_array(&array).expect("failed to initialize function array_replace");

        assert_eq!(result.len(), 1);
        let data = vec![
            Some(vec![Some(11), Some(12), Some(13), Some(14)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
            Some(vec![Some(1), Some(2), Some(3), Some(4)]),
            Some(vec![Some(9), Some(10), Some(11), Some(12)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
        ];
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(data);
        assert_eq!(
            expected,
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .clone()
        );
    }

    #[test]
    fn test_array_replace_n() {
        // array_replace_n([3, 1, 2, 3, 2, 3], 3, 4, 2) = [4, 1, 2, 4, 2, 3]
        let list_array = return_array_with_repeating_elements().into_array(1);
        let array = array_replace_n(&[
            list_array,
            Arc::new(Int64Array::from_value(3, 1)),
            Arc::new(Int64Array::from_value(4, 1)),
            Arc::new(Int64Array::from_value(2, 1)),
        ])
        .expect("failed to initialize function array_replace_n");
        let result =
            as_list_array(&array).expect("failed to initialize function array_replace_n");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[4, 1, 2, 4, 2, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_replace_n() {
        // array_replace_n(
        //     [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]],
        //     [1, 2, 3, 4],
        //     [11, 12, 13, 14],
        //     2,
        // ) = [[11, 12, 13, 14], [5, 6, 7, 8], [11, 12, 13, 14], [9, 10, 11, 12], [5, 6, 7, 8]]
        let list_array = return_nested_array_with_repeating_elements().into_array(1);
        let from_array = return_array().into_array(1);
        let to_array = return_extra_array().into_array(1);
        let array = array_replace_n(&[
            list_array,
            from_array,
            to_array,
            Arc::new(Int64Array::from_value(2, 1)),
        ])
        .expect("failed to initialize function array_replace_n");
        let result =
            as_list_array(&array).expect("failed to initialize function array_replace_n");

        assert_eq!(result.len(), 1);
        let data = vec![
            Some(vec![Some(11), Some(12), Some(13), Some(14)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
            Some(vec![Some(11), Some(12), Some(13), Some(14)]),
            Some(vec![Some(9), Some(10), Some(11), Some(12)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
        ];
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(data);
        assert_eq!(
            expected,
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .clone()
        );
    }

    #[test]
    fn test_array_replace_all() {
        // array_replace_all([3, 1, 2, 3, 2, 3], 3, 4) = [4, 1, 2, 4, 2, 4]
        let list_array = return_array_with_repeating_elements().into_array(1);
        let array = array_replace_all(&[
            list_array,
            Arc::new(Int64Array::from_value(3, 1)),
            Arc::new(Int64Array::from_value(4, 1)),
        ])
        .expect("failed to initialize function array_replace_all");
        let result = as_list_array(&array)
            .expect("failed to initialize function array_replace_all");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[4, 1, 2, 4, 2, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_replace_all() {
        // array_replace_all(
        //     [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]],
        //     [1, 2, 3, 4],
        //     [11, 12, 13, 14],
        // ) = [[11, 12, 13, 14], [5, 6, 7, 8], [11, 12, 13, 14], [9, 10, 11, 12], [5, 6, 7, 8]]
        let list_array = return_nested_array_with_repeating_elements().into_array(1);
        let from_array = return_array().into_array(1);
        let to_array = return_extra_array().into_array(1);
        let array = array_replace_all(&[list_array, from_array, to_array])
            .expect("failed to initialize function array_replace_all");
        let result = as_list_array(&array)
            .expect("failed to initialize function array_replace_all");

        assert_eq!(result.len(), 1);
        let data = vec![
            Some(vec![Some(11), Some(12), Some(13), Some(14)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
            Some(vec![Some(11), Some(12), Some(13), Some(14)]),
            Some(vec![Some(9), Some(10), Some(11), Some(12)]),
            Some(vec![Some(5), Some(6), Some(7), Some(8)]),
        ];
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(data);
        assert_eq!(
            expected,
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .clone()
        );
    }

    #[test]
    fn test_array_repeat() {
        // array_repeat(3, 5) = [3, 3, 3, 3, 3]
        let array = array_repeat(&[
            Arc::new(Int64Array::from_value(3, 1)),
            Arc::new(Int64Array::from_value(5, 1)),
        ])
        .expect("failed to initialize function array_repeat");
        let result =
            as_list_array(&array).expect("failed to initialize function array_repeat");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[3, 3, 3, 3, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_repeat() {
        // array_repeat([1, 2, 3, 4], 3) = [[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]]
        let element = return_array().into_array(1);
        let array = array_repeat(&[element, Arc::new(Int64Array::from_value(3, 1))])
            .expect("failed to initialize function array_repeat");
        let result =
            as_list_array(&array).expect("failed to initialize function array_repeat");

        assert_eq!(result.len(), 1);
        let data = vec![
            Some(vec![Some(1), Some(2), Some(3), Some(4)]),
            Some(vec![Some(1), Some(2), Some(3), Some(4)]),
            Some(vec![Some(1), Some(2), Some(3), Some(4)]),
        ];
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(data);
        assert_eq!(
            expected,
            result
                .value(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .clone()
        );
    }
    #[test]
    fn test_array_to_string() {
        // array_to_string([1, 2, 3, 4], ',') = 1,2,3,4
        let list_array = return_array().into_array(1);
        let array =
            array_to_string(&[list_array, Arc::new(StringArray::from(vec![Some(",")]))])
                .expect("failed to initialize function array_to_string");
        let result = as_generic_string_array::<i32>(&array)
            .expect("failed to initialize function array_to_string");

        assert_eq!(result.len(), 1);
        assert_eq!("1,2,3,4", result.value(0));

        // array_to_string([1, NULL, 3, NULL], ',', '*') = 1,*,3,*
        let list_array = return_array_with_nulls().into_array(1);
        let array = array_to_string(&[
            list_array,
            Arc::new(StringArray::from(vec![Some(",")])),
            Arc::new(StringArray::from(vec![Some("*")])),
        ])
        .expect("failed to initialize function array_to_string");
        let result = as_generic_string_array::<i32>(&array)
            .expect("failed to initialize function array_to_string");

        assert_eq!(result.len(), 1);
        assert_eq!("1,*,3,*", result.value(0));
    }

    #[test]
    fn test_nested_array_to_string() {
        // array_to_string([[1, 2, 3, 4], [5, 6, 7, 8]], '-') = 1-2-3-4-5-6-7-8
        let list_array = return_nested_array().into_array(1);
        let array =
            array_to_string(&[list_array, Arc::new(StringArray::from(vec![Some("-")]))])
                .expect("failed to initialize function array_to_string");
        let result = as_generic_string_array::<i32>(&array)
            .expect("failed to initialize function array_to_string");

        assert_eq!(result.len(), 1);
        assert_eq!("1-2-3-4-5-6-7-8", result.value(0));

        // array_to_string([[1, NULL, 3, NULL], [NULL, 6, 7, NULL]], '-', '*') = 1-*-3-*-*-6-7-*
        let list_array = return_nested_array_with_nulls().into_array(1);
        let array = array_to_string(&[
            list_array,
            Arc::new(StringArray::from(vec![Some("-")])),
            Arc::new(StringArray::from(vec![Some("*")])),
        ])
        .expect("failed to initialize function array_to_string");
        let result = as_generic_string_array::<i32>(&array)
            .expect("failed to initialize function array_to_string");

        assert_eq!(result.len(), 1);
        assert_eq!("1-*-3-*-*-6-7-*", result.value(0));
    }

    #[test]
    fn test_cardinality() {
        // cardinality([1, 2, 3, 4]) = 4
        let list_array = return_array().into_array(1);
        let arr = cardinality(&[list_array])
            .expect("failed to initialize function cardinality");
        let result =
            as_uint64_array(&arr).expect("failed to initialize function cardinality");

        assert_eq!(result, &UInt64Array::from(vec![4]));
    }

    #[test]
    fn test_nested_cardinality() {
        // cardinality([[1, 2, 3, 4], [5, 6, 7, 8]]) = 8
        let list_array = return_nested_array().into_array(1);
        let arr = cardinality(&[list_array])
            .expect("failed to initialize function cardinality");
        let result =
            as_uint64_array(&arr).expect("failed to initialize function cardinality");

        assert_eq!(result, &UInt64Array::from(vec![8]));
    }

    #[test]
    fn test_array_length() {
        // array_length([1, 2, 3, 4]) = 4
        let list_array = return_array().into_array(1);
        let arr = array_length(&[list_array.clone()])
            .expect("failed to initialize function array_ndims");
        let result =
            as_uint64_array(&arr).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt64Array::from_value(4, 1));

        // array_length([1, 2, 3, 4], 1) = 4
        let array = array_length(&[list_array, Arc::new(Int64Array::from_value(1, 1))])
            .expect("failed to initialize function array_ndims");
        let result =
            as_uint64_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt64Array::from_value(4, 1));
    }

    #[test]
    fn test_nested_array_length() {
        let list_array = return_nested_array().into_array(1);

        // array_length([[1, 2, 3, 4], [5, 6, 7, 8]]) = 2
        let arr = array_length(&[list_array.clone()])
            .expect("failed to initialize function array_length");
        let result =
            as_uint64_array(&arr).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt64Array::from_value(2, 1));

        // array_length([[1, 2, 3, 4], [5, 6, 7, 8]], 1) = 2
        let arr =
            array_length(&[list_array.clone(), Arc::new(Int64Array::from_value(1, 1))])
                .expect("failed to initialize function array_length");
        let result =
            as_uint64_array(&arr).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt64Array::from_value(2, 1));

        // array_length([[1, 2, 3, 4], [5, 6, 7, 8]], 2) = 4
        let arr =
            array_length(&[list_array.clone(), Arc::new(Int64Array::from_value(2, 1))])
                .expect("failed to initialize function array_length");
        let result =
            as_uint64_array(&arr).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt64Array::from_value(4, 1));

        // array_length([[1, 2, 3, 4], [5, 6, 7, 8]], 3) = NULL
        let arr = array_length(&[list_array, Arc::new(Int64Array::from_value(3, 1))])
            .expect("failed to initialize function array_length");
        let result =
            as_uint64_array(&arr).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt64Array::from(vec![None]));
    }

    #[test]
    fn test_array_dims() {
        // array_dims([1, 2, 3, 4]) = [4]
        let list_array = return_array().into_array(1);

        let array =
            array_dims(&[list_array]).expect("failed to initialize function array_dims");
        let result =
            as_list_array(&array).expect("failed to initialize function array_dims");

        assert_eq!(
            &[4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_array_dims() {
        // array_dims([[1, 2, 3, 4], [5, 6, 7, 8]]) = [2, 4]
        let list_array = return_nested_array().into_array(1);

        let array =
            array_dims(&[list_array]).expect("failed to initialize function array_dims");
        let result =
            as_list_array(&array).expect("failed to initialize function array_dims");

        assert_eq!(
            &[2, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_ndims() {
        // array_ndims([1, 2, 3, 4]) = 1
        let list_array = return_array().into_array(1);

        let array = array_ndims(&[list_array])
            .expect("failed to initialize function array_ndims");
        let result =
            as_uint64_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt64Array::from_value(1, 1));
    }

    #[test]
    fn test_nested_array_ndims() {
        // array_ndims([[1, 2, 3, 4], [5, 6, 7, 8]]) = 2
        let list_array = return_nested_array().into_array(1);

        let array = array_ndims(&[list_array])
            .expect("failed to initialize function array_ndims");
        let result =
            as_uint64_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt64Array::from_value(2, 1));
    }

    fn return_array() -> ColumnarValue {
        // Returns: [1, 2, 3, 4]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ];
        let result = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        ColumnarValue::Array(result.clone())
    }

    fn return_extra_array() -> ColumnarValue {
        // Returns: [11, 12, 13, 14]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(11))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(12))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(13))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(14))),
        ];
        let result = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        ColumnarValue::Array(result.clone())
    }

    fn return_nested_array() -> ColumnarValue {
        // Returns: [[1, 2, 3, 4], [5, 6, 7, 8]]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ];
        let arr1 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(6))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(8))),
        ];
        let arr2 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [ColumnarValue::Array(arr1), ColumnarValue::Array(arr2)];
        let result = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        ColumnarValue::Array(result.clone())
    }

    fn return_array_with_nulls() -> ColumnarValue {
        // Returns: [1, NULL, 3, NULL]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Null),
        ];
        let result = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        ColumnarValue::Array(result.clone())
    }

    fn return_nested_array_with_nulls() -> ColumnarValue {
        // Returns: [[1, NULL, 3, NULL], [NULL, 6, 7, NULL]]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Null),
        ];
        let arr1 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(6))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
            ColumnarValue::Scalar(ScalarValue::Null),
        ];
        let arr2 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [ColumnarValue::Array(arr1), ColumnarValue::Array(arr2)];
        let result = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        ColumnarValue::Array(result.clone())
    }

    fn return_array_with_repeating_elements() -> ColumnarValue {
        // Returns: [3, 1, 2, 3, 2, 3]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let result = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        ColumnarValue::Array(result.clone())
    }

    fn return_nested_array_with_repeating_elements() -> ColumnarValue {
        // Returns: [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12], [5, 6, 7, 8]]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ];
        let arr1 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(6))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(8))),
        ];
        let arr2 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ];
        let arr3 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(9))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(11))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(12))),
        ];
        let arr4 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(6))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(8))),
        ];
        let arr5 = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);

        let args = [
            ColumnarValue::Array(arr1),
            ColumnarValue::Array(arr2),
            ColumnarValue::Array(arr3),
            ColumnarValue::Array(arr4),
            ColumnarValue::Array(arr5),
        ];
        let result = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        ColumnarValue::Array(result.clone())
    }
}
