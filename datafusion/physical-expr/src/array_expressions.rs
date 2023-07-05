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
use arrow::datatypes::{DataType, Field};
use core::any::type_name;
use datafusion_common::cast::{as_generic_string_array, as_int64_array, as_list_array};
use datafusion_common::ScalarValue;
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

macro_rules! array {
    ($ARGS:expr, $ARRAY_TYPE:ident, $BUILDER_TYPE:ident) => {{
        let builder = new_builder!($BUILDER_TYPE, $ARGS[0].len());
        let mut builder =
            ListBuilder::<$BUILDER_TYPE>::with_capacity(builder, $ARGS.len());

        // for each entry in the array
        for index in 0..$ARGS[0].len() {
            for arg in $ARGS {
                match arg.as_any().downcast_ref::<$ARRAY_TYPE>() {
                    Some(arr) => {
                        builder.values().append_value(arr.value(index));
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

fn array_array(args: &[ArrayRef], data_type: DataType) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Plan(
            "Array requires at least one argument".to_string(),
        ));
    }

    let res = match data_type {
        DataType::List(..) => {
            let arrays =
                downcast_vec!(args, ListArray).collect::<Result<Vec<&ListArray>>>()?;
            let len = arrays.iter().map(|arr| arr.len() as i32).sum();
            let capacity =
                Capacities::Array(arrays.iter().map(|a| a.get_array_memory_size()).sum());
            let array_data: Vec<_> =
                arrays.iter().map(|a| a.to_data()).collect::<Vec<_>>();
            let array_data = array_data.iter().collect();
            let mut mutable =
                MutableArrayData::with_capacities(array_data, false, capacity);

            for (i, a) in arrays.iter().enumerate() {
                mutable.extend(i, 0, a.len())
            }

            let list_data_type =
                DataType::List(Arc::new(Field::new("item", data_type, true)));

            let list_data = ArrayData::builder(list_data_type)
                .len(1)
                .add_buffer(Buffer::from_slice_ref([0, len]))
                .add_child_data(mutable.freeze())
                .build()
                .unwrap();

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

/// put values in an array.
pub fn array(values: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays: Vec<ArrayRef> = values
        .iter()
        .map(|x| match x {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        })
        .collect();

    let mut data_type = DataType::Null;
    for arg in &arrays {
        let arg_data_type = arg.data_type();
        if !arg_data_type.equals_datatype(&DataType::Null) {
            data_type = arg_data_type.clone();
            break;
        }
    }

    match data_type {
        DataType::Null => Ok(ColumnarValue::Scalar(ScalarValue::new_list(
            Some(vec![]),
            DataType::Null,
        ))),
        _ => Ok(ColumnarValue::Array(array_array(
            arrays.as_slice(),
            data_type,
        )?)),
    }
}

/// `make_array` SQL function
pub fn make_array(values: &[ColumnarValue]) -> Result<ColumnarValue> {
    array(values)
}

macro_rules! append {
    ($ARRAY:expr, $ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let mut offsets: Vec<i32> = vec![0];
        let mut values =
            downcast_arg!(new_empty_array($ELEMENT.data_type()), $ARRAY_TYPE).clone();

        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        for (arr, el) in $ARRAY.iter().zip(element.iter()) {
            let last_offset: i32 = offsets.last().copied().ok_or_else(|| {
                DataFusionError::Internal(format!("offsets should not be empty",))
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
        return Err(DataFusionError::Internal(format!(
            "Array_append function requires two arguments, got {}",
            args.len()
        )));
    }

    let arr = as_list_array(&args[0])?;
    let element = &args[1];

    let res = match (arr.value_type(), element.data_type()) {
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
                DataFusionError::Internal(format!("offsets should not be empty",))
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
        return Err(DataFusionError::Internal(format!(
            "Array_prepend function requires two arguments, got {}",
            args.len()
        )));
    }

    let element = &args[0];
    let arr = as_list_array(&args[1])?;

    let res = match (arr.value_type(), element.data_type()) {
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

/// Array_concat/Array_cat SQL function
pub fn array_concat(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::List(field) => match field.data_type() {
            DataType::Null => array_concat(&args[1..]),
            _ => {
                let list_arrays = downcast_vec!(args, ListArray)
                    .collect::<Result<Vec<&ListArray>>>()?;
                let len: usize = list_arrays.iter().map(|a| a.values().len()).sum();
                let capacity =
                    Capacities::Array(list_arrays.iter().map(|a| a.len()).sum());
                let array_data: Vec<_> =
                    list_arrays.iter().map(|a| a.to_data()).collect::<Vec<_>>();
                let array_data = array_data.iter().collect();
                let mut mutable =
                    MutableArrayData::with_capacities(array_data, false, capacity);

                for (i, a) in list_arrays.iter().enumerate() {
                    mutable.extend(i, 0, a.len())
                }

                let builder = mutable.into_builder();
                let list = builder
                    .len(1)
                    .buffers(vec![Buffer::from_slice_ref([0, len as i32])])
                    .build()
                    .unwrap();

                return Ok(Arc::new(arrow::array::make_array(list)));
            }
        },
        data_type => Err(DataFusionError::NotImplemented(format!(
            "Array is not type '{data_type:?}'."
        ))),
    }
}

macro_rules! fill {
    ($ARRAY:expr, $ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let arr = downcast_arg!($ARRAY, $ARRAY_TYPE);

        let mut acc = ColumnarValue::Scalar($ELEMENT);
        for value in arr.iter().rev() {
            match value {
                Some(value) => {
                    let mut repeated = vec![];
                    for _ in 0..value {
                        repeated.push(acc.clone());
                    }
                    acc = array(repeated.as_slice()).unwrap();
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Array_fill function requires non nullable array"
                    )));
                }
            }
        }

        acc
    }};
}

/// Array_fill SQL function
pub fn array_fill(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "Array_fill function requires two arguments, got {}",
            args.len()
        )));
    }

    let element = match &args[0] {
        ColumnarValue::Scalar(scalar) => scalar.clone(),
        _ => {
            return Err(DataFusionError::Internal(
                "Array_fill function requires scalar element".to_string(),
            ))
        }
    };

    let arr = match &args[1] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        ColumnarValue::Array(arr) => arr.clone(),
    };

    let res = match arr.data_type() {
        DataType::List(..) => {
            let arr = downcast_arg!(arr, ListArray);
            let array_values = arr.values();
            match arr.value_type() {
                DataType::Int8 => fill!(array_values, element, Int8Array),
                DataType::Int16 => fill!(array_values, element, Int16Array),
                DataType::Int32 => fill!(array_values, element, Int32Array),
                DataType::Int64 => fill!(array_values, element, Int64Array),
                DataType::UInt8 => fill!(array_values, element, UInt8Array),
                DataType::UInt16 => fill!(array_values, element, UInt16Array),
                DataType::UInt32 => fill!(array_values, element, UInt32Array),
                DataType::UInt64 => fill!(array_values, element, UInt64Array),
                DataType::Null => {
                    return Ok(datafusion_expr::ColumnarValue::Scalar(
                        ScalarValue::new_list(Some(vec![]), DataType::Null),
                    ))
                }
                data_type => {
                    return Err(DataFusionError::Internal(format!(
                        "Array_fill is not implemented for type '{data_type:?}'."
                    )));
                }
            }
        }
        data_type => {
            return Err(DataFusionError::Internal(format!(
                "Array is not type '{data_type:?}'."
            )));
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

    let mut index = Int64Array::from_value(0, arr.len());
    if args.len() == 3 {
        index = as_int64_array(&args[2])?.clone();
    }

    let res = match arr.data_type() {
        DataType::List(field) => match field.data_type() {
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
        },
        data_type => {
            return Err(DataFusionError::NotImplemented(format!(
                "Array is not type '{data_type:?}'."
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

    let res = match arr.data_type() {
        DataType::List(field) => match field.data_type() {
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
        },
        data_type => {
            return Err(DataFusionError::NotImplemented(format!(
                "Array is not type '{data_type:?}'."
            )))
        }
    };

    Ok(res)
}

macro_rules! remove {
    ($ARRAY:expr, $ELEMENT:expr, $ARRAY_TYPE:ident, $BUILDER_TYPE:ident) => {{
        let child_array =
            downcast_arg!(downcast_arg!($ARRAY, ListArray).values(), $ARRAY_TYPE);
        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE).value(0);
        let mut builder = new_builder!($BUILDER_TYPE, child_array.len());

        for x in child_array {
            match x {
                Some(x) => {
                    if x != element {
                        builder.append_value(x);
                    }
                }
                None => builder.append_null(),
            }
        }
        let arr = builder.finish();

        let mut scalars = vec![];
        for i in 0..arr.len() {
            scalars.push(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, i)?));
        }
        scalars
    }};
}

/// Array_remove SQL function
pub fn array_remove(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr = match &args[0] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        ColumnarValue::Array(arr) => arr.clone(),
    };

    let element = match &args[1] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        _ => {
            return Err(DataFusionError::Internal(
                "Array_remove function requires scalar element".to_string(),
            ))
        }
    };

    let data_type = arr.data_type();
    let res = match data_type {
        DataType::List(field) => {
            match (field.data_type(), element.data_type()) {
                (DataType::Utf8, DataType::Utf8) => remove!(arr, element, StringArray, StringBuilder),
                (DataType::LargeUtf8, DataType::LargeUtf8) => remove!(arr, element, LargeStringArray, LargeStringBuilder),
                (DataType::Boolean, DataType::Boolean) => remove!(arr, element, BooleanArray, BooleanBuilder),
                (DataType::Float32, DataType::Float32) => remove!(arr, element, Float32Array, Float32Builder),
                (DataType::Float64, DataType::Float64) => remove!(arr, element, Float64Array, Float64Builder),
                (DataType::Int8, DataType::Int8) => remove!(arr, element, Int8Array, Int8Builder),
                (DataType::Int16, DataType::Int16) => remove!(arr, element, Int16Array, Int16Builder),
                (DataType::Int32, DataType::Int32) => remove!(arr, element, Int32Array, Int32Builder),
                (DataType::Int64, DataType::Int64) => remove!(arr, element, Int64Array, Int64Builder),
                (DataType::UInt8, DataType::UInt8) => remove!(arr, element, UInt8Array, UInt8Builder),
                (DataType::UInt16, DataType::UInt16) => remove!(arr, element, UInt16Array, UInt16Builder),
                (DataType::UInt32, DataType::UInt32) => remove!(arr, element, UInt32Array, UInt32Builder),
                (DataType::UInt64, DataType::UInt64) => remove!(arr, element, UInt64Array, UInt64Builder),
                (array_data_type, element_data_type) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Array_remove is not implemented for types '{array_data_type:?}' and '{element_data_type:?}'."
                    )))
                }
            }
        }
        data_type => {
            return Err(DataFusionError::Internal(format!(
                "Array is not type '{data_type:?}'."
            )))
        }
    };

    array(res.as_slice())
}

macro_rules! replace {
    ($ARRAY:expr, $FROM:expr, $TO:expr, $ARRAY_TYPE:ident, $BUILDER_TYPE:ident) => {{
        let child_array =
            downcast_arg!(downcast_arg!($ARRAY, ListArray).values(), $ARRAY_TYPE);
        let from = downcast_arg!($FROM, $ARRAY_TYPE).value(0);
        let to = downcast_arg!($TO, $ARRAY_TYPE).value(0);
        let mut builder = new_builder!($BUILDER_TYPE, child_array.len());

        for x in child_array {
            match x {
                Some(x) => {
                    if x == from {
                        builder.append_value(to);
                    } else {
                        builder.append_value(x);
                    }
                }
                None => builder.append_null(),
            }
        }
        let arr = builder.finish();

        let mut scalars = vec![];
        for i in 0..arr.len() {
            scalars.push(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, i)?));
        }
        scalars
    }};
}

/// Array_replace SQL function
pub fn array_replace(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr = match &args[0] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        ColumnarValue::Array(arr) => arr.clone(),
    };

    let from = match &args[1] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        _ => {
            return Err(DataFusionError::Internal(
                "array_replace function requires scalar element".to_string(),
            ))
        }
    };

    let to = match &args[2] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        _ => {
            return Err(DataFusionError::Internal(
                "array_replace function requires scalar element".to_string(),
            ))
        }
    };

    if from.data_type() != to.data_type() {
        return Err(DataFusionError::Internal(
            "array_replace function requires scalar element".to_string(),
        ));
    }

    let data_type = arr.data_type();
    let res = match data_type {
        DataType::List(field) => {
            match (field.data_type(), from.data_type()) {
                (DataType::Utf8, DataType::Utf8) => replace!(arr, from, to, StringArray, StringBuilder),
                (DataType::LargeUtf8, DataType::LargeUtf8) => replace!(arr, from, to, LargeStringArray, LargeStringBuilder),
                (DataType::Boolean, DataType::Boolean) => replace!(arr, from, to, BooleanArray, BooleanBuilder),
                (DataType::Float32, DataType::Float32) => replace!(arr, from, to, Float32Array, Float32Builder),
                (DataType::Float64, DataType::Float64) => replace!(arr, from, to, Float64Array, Float64Builder),
                (DataType::Int8, DataType::Int8) => replace!(arr, from, to, Int8Array, Int8Builder),
                (DataType::Int16, DataType::Int16) => replace!(arr, from, to, Int16Array, Int16Builder),
                (DataType::Int32, DataType::Int32) => replace!(arr, from, to, Int32Array, Int32Builder),
                (DataType::Int64, DataType::Int64) => replace!(arr, from, to, Int64Array, Int64Builder),
                (DataType::UInt8, DataType::UInt8) => replace!(arr, from, to, UInt8Array, UInt8Builder),
                (DataType::UInt16, DataType::UInt16) => replace!(arr, from, to, UInt16Array, UInt16Builder),
                (DataType::UInt32, DataType::UInt32) => replace!(arr, from, to, UInt32Array, UInt32Builder),
                (DataType::UInt64, DataType::UInt64) => replace!(arr, from, to, UInt64Array, UInt64Builder),
                (array_data_type, element_data_type) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Array_replace is not implemented for types '{array_data_type:?}' and '{element_data_type:?}'."
                    )))
                }
            }
        }
        data_type => {
            return Err(DataFusionError::Internal(format!(
                "Array is not type '{data_type:?}'."
            )))
        }
    };

    array(res.as_slice())
}

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
    let delimeter = as_generic_string_array::<i32>(&args[1])?
        .value(0)
        .to_string();
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
    let mut res = compute_array_to_string(
        &mut arg,
        arr.clone(),
        delimeter.clone(),
        null_string,
        with_null_string,
    )?
    .clone();
    match res.as_str() {
        "" => Ok(Arc::new(StringArray::from(vec![Some(res)]))),
        _ => {
            res.truncate(res.len() - delimeter.len());
            Ok(Arc::new(StringArray::from(vec![Some(res)])))
        }
    }
}

/// Trim_array SQL function
pub fn trim_array(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_list_array(&args[0])?;
    let n = as_int64_array(&args[1])?.value(0) as usize;

    let values = list_array.value(0);
    if values.len() <= n {
        return Ok(array(&[ColumnarValue::Scalar(ScalarValue::Null)])?.into_array(1));
    }

    let res = values.slice(0, values.len() - n);
    let mut scalars = vec![];
    for i in 0..res.len() {
        scalars.push(ColumnarValue::Scalar(ScalarValue::try_from_array(&res, i)?));
    }

    Ok(array(scalars.as_slice())?.into_array(1))
}

/// Cardinality SQL function
pub fn cardinality(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr = match &args[0] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        ColumnarValue::Array(arr) => arr.clone(),
    };

    fn compute_cardinality(arg: &mut u64, arr: ArrayRef) -> Result<&mut u64> {
        match arr.data_type() {
            DataType::List(..) => {
                let list_array = downcast_arg!(arr, ListArray);
                for i in 0..list_array.len() {
                    compute_cardinality(arg, list_array.value(i))?;
                }

                Ok(arg)
            }
            DataType::Null
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                *arg += arr.len() as u64;
                Ok(arg)
            }
            data_type => Err(DataFusionError::NotImplemented(format!(
                "Array is not implemented for type '{data_type:?}'."
            ))),
        }
    }
    let mut arg: u64 = 0;
    Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(vec![
        *compute_cardinality(&mut arg, arr)?,
    ]))))
}

/// Array_length SQL function
pub fn array_length(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr = match &args[0] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        ColumnarValue::Array(arr) => arr.clone(),
    };
    let mut element: u8 = 1;
    if args.len() == 2 {
        let scalar = match &args[1] {
            ColumnarValue::Scalar(scalar) => scalar.clone(),
            _ => {
                return Err(DataFusionError::Internal(
                    "Array_length function requires positive integer scalar element"
                        .to_string(),
                ))
            }
        };

        element = match scalar {
            ScalarValue::Int8(Some(value)) => value as u8,
            ScalarValue::Int16(Some(value)) => value as u8,
            ScalarValue::Int32(Some(value)) => value as u8,
            ScalarValue::Int64(Some(value)) => value as u8,
            ScalarValue::UInt8(Some(value)) => value,
            ScalarValue::UInt16(Some(value)) => value as u8,
            ScalarValue::UInt32(Some(value)) => value as u8,
            ScalarValue::UInt64(Some(value)) => value as u8,
            _ => {
                return Err(DataFusionError::Internal(
                    "Array_length function requires positive integer scalar element"
                        .to_string(),
                ))
            }
        };

        if element == 0 {
            return Err(DataFusionError::Internal(
                "Array_length function requires positive integer scalar element"
                    .to_string(),
            ));
        }
    }

    fn compute_array_length(arg: u8, array: ArrayRef, element: u8) -> Result<Option<u8>> {
        match array.data_type() {
            DataType::List(..) => {
                let list_array = downcast_arg!(array, ListArray);
                if arg == element + 1 {
                    Ok(Some(list_array.len() as u8))
                } else {
                    compute_array_length(arg + 1, list_array.value(0), element)
                }
            }
            DataType::Null
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => {
                if arg == element + 1 {
                    Ok(Some(array.len() as u8))
                } else {
                    Ok(None)
                }
            }
            data_type => Err(DataFusionError::NotImplemented(format!(
                "Array is not implemented for type '{data_type:?}'."
            ))),
        }
    }
    let arg: u8 = 1;
    Ok(ColumnarValue::Array(Arc::new(UInt8Array::from(vec![
        compute_array_length(arg, arr, element)?,
    ]))))
}

/// Array_dims SQL function
pub fn array_dims(args: &[ArrayRef]) -> Result<ArrayRef> {
    fn compute_array_dims(
        arg: &mut Vec<ColumnarValue>,
        arr: ArrayRef,
    ) -> Result<&mut Vec<ColumnarValue>> {
        match arr.data_type() {
            DataType::List(..) => {
                let list_array = downcast_arg!(arr, ListArray).value(0);
                arg.push(ColumnarValue::Scalar(ScalarValue::UInt8(Some(
                    list_array.len() as u8,
                ))));
                return compute_array_dims(arg, list_array);
            }
            DataType::Null
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(arg),
            data_type => Err(DataFusionError::NotImplemented(format!(
                "Array is not implemented for type '{data_type:?}'."
            ))),
        }
    }

    let mut arg: Vec<ColumnarValue> = vec![];
    Ok(array(
        compute_array_dims(&mut arg, args[0].clone())?
            .clone()
            .as_slice(),
    )?
    .into_array(1))
}

/// Array_ndims SQL function
pub fn array_ndims(args: &[ArrayRef]) -> Result<ArrayRef> {
    fn compute_array_ndims(arg: u8, arr: ArrayRef) -> Result<u8> {
        match arr.data_type() {
            DataType::List(..) => {
                let list_array = downcast_arg!(arr, ListArray);
                compute_array_ndims(arg + 1, list_array.value(0))
            }
            DataType::Null
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(arg),
            data_type => Err(DataFusionError::NotImplemented(format!(
                "Array is not implemented for type '{data_type:?}'."
            ))),
        }
    }
    let arg: u8 = 0;
    Ok(Arc::new(UInt8Array::from(vec![compute_array_ndims(
        arg,
        args[0].clone(),
    )?])))
}

macro_rules! contains {
    ($FIRST_ARRAY:expr, $SECOND_ARRAY:expr, $ARRAY_TYPE:ident) => {{
        let first_array = downcast_arg!($FIRST_ARRAY, $ARRAY_TYPE);
        let second_array = downcast_arg!($SECOND_ARRAY, $ARRAY_TYPE);
        let mut res = true;
        for x in second_array.values().iter().dedup() {
            if !first_array.values().contains(x) {
                res = false;
            }
        }

        res
    }};
}

/// Array_contains SQL function
pub fn array_contains(args: &[ArrayRef]) -> Result<ArrayRef> {
    fn concat_inner_lists(arg: ArrayRef) -> Result<ArrayRef> {
        match arg.data_type() {
            DataType::List(field) => match field.data_type() {
                DataType::List(..) => {
                    concat_inner_lists(array_concat(&[as_list_array(&arg)?
                        .values()
                        .clone()])?)
                }
                _ => Ok(as_list_array(&arg)?.values().clone()),
            },
            data_type => Err(DataFusionError::NotImplemented(format!(
                "Array is not type '{data_type:?}'."
            ))),
        }
    }

    let concat_first_array = concat_inner_lists(args[0].clone())?.clone();
    let concat_second_array = concat_inner_lists(args[1].clone())?.clone();

    let res = match (concat_first_array.data_type(), concat_second_array.data_type()) {
        (DataType::Utf8, DataType::Utf8) => contains!(concat_first_array, concat_second_array, StringArray),
        (DataType::LargeUtf8, DataType::LargeUtf8) => contains!(concat_first_array, concat_second_array, LargeStringArray),
        (DataType::Boolean, DataType::Boolean) => {
            let first_array = downcast_arg!(concat_first_array, BooleanArray);
            let second_array = downcast_arg!(concat_second_array, BooleanArray);
            compute::bool_or(first_array) == compute::bool_or(second_array)
        }
        (DataType::Float32, DataType::Float32) => contains!(concat_first_array, concat_second_array, Float32Array),
        (DataType::Float64, DataType::Float64) => contains!(concat_first_array, concat_second_array, Float64Array),
        (DataType::Int8, DataType::Int8) => contains!(concat_first_array, concat_second_array, Int8Array),
        (DataType::Int16, DataType::Int16) => contains!(concat_first_array, concat_second_array, Int16Array),
        (DataType::Int32, DataType::Int32) => contains!(concat_first_array, concat_second_array, Int32Array),
        (DataType::Int64, DataType::Int64) => contains!(concat_first_array, concat_second_array, Int64Array),
        (DataType::UInt8, DataType::UInt8) => contains!(concat_first_array, concat_second_array, UInt8Array),
        (DataType::UInt16, DataType::UInt16) => contains!(concat_first_array, concat_second_array, UInt16Array),
        (DataType::UInt32, DataType::UInt32) => contains!(concat_first_array, concat_second_array, UInt32Array),
        (DataType::UInt64, DataType::UInt64) => contains!(concat_first_array, concat_second_array, UInt64Array),
        (first_array_data_type, second_array_data_type) => {
            return Err(DataFusionError::NotImplemented(format!(
                "Array_contains is not implemented for types '{first_array_data_type:?}' and '{second_array_data_type:?}'."
            )))
        }
    };

    Ok(Arc::new(BooleanArray::from(vec![res])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt8Array;
    use arrow::datatypes::Int64Type;
    use datafusion_common::cast::{
        as_generic_string_array, as_list_array, as_uint64_array, as_uint8_array,
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
    fn test_array_with_nulls() {
        // make_array(NULL, 1, NULL, 2, NULL, 3, NULL, NULL, 4, 5) = [NULL, 1, NULL, 2, NULL, 3, NULL, NULL, 4, 5]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
        ];
        let array = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        let result = as_list_array(&array).expect("failed to initialize function array");
        assert_eq!(result.len(), 1);
        assert_eq!(
            &[0, 1, 0, 2, 0, 3, 0, 0, 4, 5],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        )
    }

    #[test]
    fn test_array_all_nulls() {
        // make_array(NULL, NULL, NULL) = []
        let args = [
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Null),
        ];
        let array = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        let result = as_list_array(&array).expect("failed to initialize function array");
        assert_eq!(result.len(), 1);
        assert_eq!(
            0,
            result
                .value(0)
                .as_any()
                .downcast_ref::<NullArray>()
                .unwrap()
                .null_count()
        )
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
    fn test_array_fill() {
        // array_fill(4, [5]) = [4, 4, 4, 4, 4]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ColumnarValue::Scalar(ScalarValue::List(
                Some(vec![ScalarValue::Int64(Some(5))]),
                Arc::new(Field::new("item", DataType::Int64, false)),
            )),
        ];

        let array = array_fill(&args)
            .expect("failed to initialize function array_fill")
            .into_array(1);
        let result =
            as_list_array(&array).expect("failed to initialize function array_fill");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[4, 4, 4, 4, 4],
            result
                .value(0)
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
        // array_remove([1, 2, 3, 4], 3) = [1, 2, 4]
        let list_array = return_array();
        let arr = array_remove(&[
            list_array,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ])
        .expect("failed to initialize function array_remove")
        .into_array(1);
        let result =
            as_list_array(&arr).expect("failed to initialize function array_remove");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[1, 2, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_array_replace() {
        // array_replace([1, 2, 3, 4], 3, 4) = [1, 2, 4, 4]
        let list_array = return_array();
        let array = array_replace(&[
            list_array,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ])
        .expect("failed to initialize function array_replace")
        .into_array(1);
        let result =
            as_list_array(&array).expect("failed to initialize function array_replace");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[1, 2, 4, 4],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
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
    fn test_trim_array() {
        // trim_array([1, 2, 3, 4], 1) = [1, 2, 3]
        let list_array = return_array().into_array(1);
        let arr = trim_array(&[list_array, Arc::new(Int64Array::from(vec![Some(1)]))])
            .expect("failed to initialize function trim_array");
        let result =
            as_list_array(&arr).expect("failed to initialize function trim_array");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[1, 2, 3],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );

        // trim_array([1, 2, 3, 4], 3) = [1]
        let list_array = return_array().into_array(1);
        let arr = trim_array(&[list_array, Arc::new(Int64Array::from(vec![Some(3)]))])
            .expect("failed to initialize function trim_array");
        let result =
            as_list_array(&arr).expect("failed to initialize function trim_array");

        assert_eq!(result.len(), 1);
        assert_eq!(
            &[1],
            result
                .value(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
        );
    }

    #[test]
    fn test_nested_trim_array() {
        // trim_array([[1, 2, 3, 4], [5, 6, 7, 8]], 1) = [[1, 2, 3, 4]]
        let list_array = return_nested_array().into_array(1);
        let arr = trim_array(&[list_array, Arc::new(Int64Array::from(vec![Some(1)]))])
            .expect("failed to initialize function trim_array");
        let binding = as_list_array(&arr)
            .expect("failed to initialize function trim_array")
            .value(0);
        let result =
            as_list_array(&binding).expect("failed to initialize function trim_array");

        assert_eq!(result.len(), 1);
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
    fn test_cardinality() {
        // cardinality([1, 2, 3, 4]) = 4
        let list_array = return_array();
        let arr = cardinality(&[list_array])
            .expect("failed to initialize function cardinality")
            .into_array(1);
        let result =
            as_uint64_array(&arr).expect("failed to initialize function cardinality");

        assert_eq!(result, &UInt64Array::from(vec![4]));
    }

    #[test]
    fn test_nested_cardinality() {
        // cardinality([[1, 2, 3, 4], [5, 6, 7, 8]]) = 8
        let list_array = return_nested_array();
        let arr = cardinality(&[list_array])
            .expect("failed to initialize function cardinality")
            .into_array(1);
        let result =
            as_uint64_array(&arr).expect("failed to initialize function cardinality");

        assert_eq!(result, &UInt64Array::from(vec![8]));
    }

    #[test]
    fn test_array_length() {
        // array_length([1, 2, 3, 4]) = 4
        let list_array = return_array();
        let array = array_length(&[list_array.clone()])
            .expect("failed to initialize function array_ndims")
            .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt8Array::from(vec![4]));

        // array_length([1, 2, 3, 4], 1) = 4
        let array = array_length(&[
            list_array,
            ColumnarValue::Scalar(ScalarValue::UInt8(Some(1_u8))),
        ])
        .expect("failed to initialize function array_ndims")
        .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt8Array::from(vec![4]));
    }

    #[test]
    fn test_nested_array_length() {
        let list_array = return_nested_array();

        // array_length([[1, 2, 3, 4], [5, 6, 7, 8]], 1) = 2
        let array = array_length(&[
            list_array.clone(),
            ColumnarValue::Scalar(ScalarValue::UInt8(Some(1_u8))),
        ])
        .expect("failed to initialize function array_length")
        .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt8Array::from(vec![2]));

        // array_length([[1, 2, 3, 4], [5, 6, 7, 8]], 2) = 4
        let array = array_length(&[
            list_array.clone(),
            ColumnarValue::Scalar(ScalarValue::UInt8(Some(2_u8))),
        ])
        .expect("failed to initialize function array_length")
        .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt8Array::from(vec![4]));

        // array_length([[1, 2, 3, 4], [5, 6, 7, 8]], 3) = NULL
        let array = array_length(&[
            list_array,
            ColumnarValue::Scalar(ScalarValue::UInt8(Some(3_u8))),
        ])
        .expect("failed to initialize function array_length")
        .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt8Array::from(vec![None]));
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
                .downcast_ref::<UInt8Array>()
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
                .downcast_ref::<UInt8Array>()
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
            as_uint8_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt8Array::from(vec![1]));
    }

    #[test]
    fn test_nested_array_ndims() {
        // array_ndims([[1, 2, 3, 4], [5, 6, 7, 8]]) = 2
        let list_array = return_nested_array().into_array(1);

        let array = array_ndims(&[list_array])
            .expect("failed to initialize function array_ndims");
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt8Array::from(vec![2]));
    }

    #[test]
    fn test_array_contains() {
        // array_contains([1, 2, 3, 4], array_append([1, 2, 3, 4], 3)) = t
        let first_array = return_array().into_array(1);
        let second_array = array_append(&[
            first_array.clone(),
            Arc::new(Int64Array::from(vec![Some(3)])),
        ])
        .expect("failed to initialize function array_contains");

        let arr = array_contains(&[first_array.clone(), second_array])
            .expect("failed to initialize function array_contains");
        let result = as_boolean_array(&arr);

        assert_eq!(result, &BooleanArray::from(vec![true]));

        // array_contains([1, 2, 3, 4], array_append([1, 2, 3, 4], 5)) = f
        let second_array = array_append(&[
            first_array.clone(),
            Arc::new(Int64Array::from(vec![Some(5)])),
        ])
        .expect("failed to initialize function array_contains");

        let arr = array_contains(&[first_array.clone(), second_array])
            .expect("failed to initialize function array_contains");
        let result = as_boolean_array(&arr);

        assert_eq!(result, &BooleanArray::from(vec![false]));
    }

    #[test]
    fn test_nested_array_contains() {
        // array_contains([[1, 2, 3, 4], [5, 6, 7, 8]], array_append([1, 2, 3, 4], 3)) = t
        let first_array = return_nested_array().into_array(1);
        let array = return_array().into_array(1);
        let second_array =
            array_append(&[array.clone(), Arc::new(Int64Array::from(vec![Some(3)]))])
                .expect("failed to initialize function array_contains");

        let arr = array_contains(&[first_array.clone(), second_array])
            .expect("failed to initialize function array_contains");
        let result = as_boolean_array(&arr);

        assert_eq!(result, &BooleanArray::from(vec![true]));

        // array_contains([[1, 2, 3, 4], [5, 6, 7, 8]], array_append([1, 2, 3, 4], 9)) = f
        let second_array =
            array_append(&[array.clone(), Arc::new(Int64Array::from(vec![Some(9)]))])
                .expect("failed to initialize function array_contains");

        let arr = array_contains(&[first_array.clone(), second_array])
            .expect("failed to initialize function array_contains");
        let result = as_boolean_array(&arr);

        assert_eq!(result, &BooleanArray::from(vec![false]));
    }

    fn return_array() -> ColumnarValue {
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

    fn return_nested_array() -> ColumnarValue {
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
}
