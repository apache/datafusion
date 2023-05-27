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
use arrow::buffer::Buffer;
use arrow::compute;
use arrow::datatypes::{DataType, Field};
use core::any::type_name;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

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
        // downcast all arguments to their common format
        let args =
            downcast_vec!($ARGS, $ARRAY_TYPE).collect::<Result<Vec<&$ARRAY_TYPE>>>()?;

        let builder = new_builder!($BUILDER_TYPE, args[0].len());
        let mut builder =
            ListBuilder::<$BUILDER_TYPE>::with_capacity(builder, args.len());
        // for each entry in the array
        for index in 0..args[0].len() {
            for arg in &args {
                if arg.is_null(index) {
                    builder.values().append_null();
                } else {
                    builder.values().append_value(arg.value(index));
                }
            }
            builder.append(true);
        }
        Arc::new(builder.finish())
    }};
}

fn array_array(args: &[ArrayRef]) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(
            "Array requires at least one argument".to_string(),
        ));
    }

    let data_type = args[0].data_type();
    let res = match data_type {
        DataType::List(..) => {
            let arrays =
                downcast_vec!(args, ListArray).collect::<Result<Vec<&ListArray>>>()?;
            let len: i32 = arrays.len() as i32;
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
                DataType::List(Arc::new(Field::new("item", data_type.clone(), true)));

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
    Ok(ColumnarValue::Array(array_array(arrays.as_slice())?))
}

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

macro_rules! append {
    ($ARRAY:expr, $ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let child_array =
            downcast_arg!(downcast_arg!($ARRAY, ListArray).values(), $ARRAY_TYPE);
        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        let concat = compute::concat(&[child_array, element])?;
        let mut scalars = vec![];
        for i in 0..concat.len() {
            scalars.push(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &concat, i,
            )?));
        }
        scalars
    }};
}

/// Array_append SQL function
pub fn array_append(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "Array_append function requires two arguments, got {}",
            args.len()
        )));
    }

    let arr = match &args[0] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        ColumnarValue::Array(arr) => arr.clone(),
    };

    let element = match &args[1] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        _ => {
            return Err(DataFusionError::Internal(
                "Array_append function requires scalar element".to_string(),
            ))
        }
    };

    let data_type = arr.data_type();
    let arrays = match data_type {
        DataType::List(field) => {
            match (field.data_type(), element.data_type()) {
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
                (array_data_type, element_data_type) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Array_append is not implemented for types '{array_data_type:?}' and '{element_data_type:?}'."
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

    array(arrays.as_slice())
}

macro_rules! prepend {
    ($ARRAY:expr, $ELEMENT:expr, $ARRAY_TYPE:ident) => {{
        let child_array =
            downcast_arg!(downcast_arg!($ARRAY, ListArray).values(), $ARRAY_TYPE);
        let element = downcast_arg!($ELEMENT, $ARRAY_TYPE);
        let concat = compute::concat(&[element, child_array])?;
        let mut scalars = vec![];
        for i in 0..concat.len() {
            scalars.push(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &concat, i,
            )?));
        }
        scalars
    }};
}

/// Array_prepend SQL function
pub fn array_prepend(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "Array_prepend function requires two arguments, got {}",
            args.len()
        )));
    }

    let element = match &args[0] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        _ => {
            return Err(DataFusionError::Internal(
                "Array_prepend function requires scalar element".to_string(),
            ))
        }
    };

    let arr = match &args[1] {
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        ColumnarValue::Array(arr) => arr.clone(),
    };

    let data_type = arr.data_type();
    let arrays = match data_type {
        DataType::List(field) => {
            match (field.data_type(), element.data_type()) {
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
                (array_data_type, element_data_type) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Array_prepend is not implemented for types '{array_data_type:?}' and '{element_data_type:?}'."
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

    array(arrays.as_slice())
}

/// Array_concat/Array_cat SQL function
pub fn array_concat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays: Vec<ArrayRef> = args
        .iter()
        .map(|x| match x {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        })
        .collect();
    let data_type = arrays[0].data_type();
    match data_type {
        DataType::List(..) => {
            let list_arrays =
                downcast_vec!(arrays, ListArray).collect::<Result<Vec<&ListArray>>>()?;
            let len: usize = list_arrays.iter().map(|a| a.values().len()).sum();
            let capacity = Capacities::Array(
                list_arrays.iter().map(|a| a.get_buffer_memory_size()).sum(),
            );
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

            return Ok(ColumnarValue::Array(Arc::new(make_array(list))));
        }
        data_type => Err(DataFusionError::NotImplemented(format!(
            "Array is not implemented for type '{data_type:?}'."
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
                    "Array_fill function requires positive integer scalar element"
                        .to_string(),
                ))
            }
        };

        element = match scalar {
            ScalarValue::Int8(Some(value)) => value as u8,
            ScalarValue::Int16(Some(value)) => value as u8,
            ScalarValue::Int32(Some(value)) => value as u8,
            ScalarValue::Int64(Some(value)) => value as u8,
            ScalarValue::UInt8(Some(value)) => value as u8,
            ScalarValue::UInt16(Some(value)) => value as u8,
            ScalarValue::UInt32(Some(value)) => value as u8,
            ScalarValue::UInt64(Some(value)) => value as u8,
            _ => {
                return Err(DataFusionError::Internal(
                    "Array_fill function requires positive integer scalar element"
                        .to_string(),
                ))
            }
        };

        if element == 0 {
            return Err(DataFusionError::Internal(
                "Array_fill function requires positive integer scalar element"
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
        compute_array_length(arg, arr, element.clone())?,
    ]))))
}

/// Array_dims SQL function
pub fn array_dims(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr = match &args[0] {
        ColumnarValue::Array(arr) => arr.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
    };

    fn compute_array_dims(
        arg: &mut Vec<ScalarValue>,
        arr: ArrayRef,
    ) -> Result<&mut Vec<ScalarValue>> {
        match arr.data_type() {
            DataType::List(..) => {
                let list_array = downcast_arg!(arr, ListArray).value(0);
                arg.push(ScalarValue::UInt8(Some(list_array.len() as u8)));
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

    let list_field = Arc::new(Field::new("item", DataType::UInt8, true));
    let mut arg: Vec<ScalarValue> = vec![];
    Ok(ColumnarValue::Scalar(ScalarValue::List(
        Some(compute_array_dims(&mut arg, arr)?.clone()),
        list_field,
    )))
}

/// Array_ndims SQL function
pub fn array_ndims(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr = match &args[0] {
        ColumnarValue::Array(arr) => arr.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
    };

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
    Ok(ColumnarValue::Array(Arc::new(UInt8Array::from(vec![
        compute_array_ndims(arg, arr)?,
    ]))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt8Array;
    use datafusion_common::cast::{as_list_array, as_uint8_array};
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
    fn test_array_append() {
        // array_append([1, 2, 3], 4) = [1, 2, 3, 4]
        let args = [
            ColumnarValue::Scalar(ScalarValue::List(
                Some(vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int64(Some(3)),
                ]),
                Arc::new(Field::new("item", DataType::Int64, false)),
            )),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ];

        let array = array_append(&args)
            .expect("failed to initialize function array_append")
            .into_array(1);
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
        )
    }

    #[test]
    fn test_array_prepend() {
        // array_prepend(1, [2, 3, 4]) = [1, 2, 3, 4]
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::List(
                Some(vec![
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int64(Some(3)),
                    ScalarValue::Int64(Some(4)),
                ]),
                Arc::new(Field::new("item", DataType::Int64, false)),
            )),
        ];

        let array = array_prepend(&args)
            .expect("failed to initialize function array_append")
            .into_array(1);
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
        )
    }

    #[test]
    fn test_array_concat() {
        // array_concat([1, 2, 3], [4, 5, 6], [7, 8, 9]) = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        let args = [
            ColumnarValue::Scalar(ScalarValue::List(
                Some(vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int64(Some(3)),
                ]),
                Arc::new(Field::new("item", DataType::Int64, false)),
            )),
            ColumnarValue::Scalar(ScalarValue::List(
                Some(vec![
                    ScalarValue::Int64(Some(4)),
                    ScalarValue::Int64(Some(5)),
                    ScalarValue::Int64(Some(6)),
                ]),
                Arc::new(Field::new("item", DataType::Int64, false)),
            )),
            ColumnarValue::Scalar(ScalarValue::List(
                Some(vec![
                    ScalarValue::Int64(Some(7)),
                    ScalarValue::Int64(Some(8)),
                    ScalarValue::Int64(Some(9)),
                ]),
                Arc::new(Field::new("item", DataType::Int64, false)),
            )),
        ];

        let array = array_concat(&args)
            .expect("failed to initialize function array_concat")
            .into_array(1);
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
        )
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
        )
    }

    #[test]
    fn test_array_length() {
        let values_builder = Int64Builder::new();
        let mut builder = ListBuilder::new(values_builder);
        builder.values().append_value(1);
        builder.append(true);
        builder.values().append_value(2);
        builder.append(true);
        let list_array = ColumnarValue::Array(Arc::new(builder.finish().value(0)));

        // array_length([1, 2]) = 2
        let array = array_length(&[list_array.clone()])
            .expect("failed to initialize function array_ndims")
            .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt8Array::from(vec![1]));

        // array_length([1, 2], 1) = 2
        let array = array_length(&[
            list_array.clone(),
            ColumnarValue::Scalar(ScalarValue::UInt8(Some(1_u8))),
        ])
        .expect("failed to initialize function array_ndims")
        .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt8Array::from(vec![1]));
    }

    #[test]
    fn test_nested_array_length() {
        let int64_values_builder = Int64Builder::new();
        let list_values_builder = ListBuilder::new(int64_values_builder);

        let mut builder = ListBuilder::new(list_values_builder);
        builder.values().values().append_value(1);
        builder.values().append(true);
        builder.values().values().append_value(2);
        builder.values().append(true);
        builder.values().values().append_value(3);
        builder.values().append(true);
        builder.append(true);
        builder.values().values().append_value(4);
        builder.values().append(true);
        builder.values().values().append_value(5);
        builder.values().append(true);
        builder.values().values().append_value(6);
        builder.values().append(true);
        builder.append(true);
        let list_array = ColumnarValue::Array(Arc::new(builder.finish()));

        // array_length([[1, 2, 3], [4, 5, 6]], 1) = 2
        let array = array_length(&[
            list_array.clone(),
            ColumnarValue::Scalar(ScalarValue::UInt8(Some(1_u8))),
        ])
        .expect("failed to initialize function array_length")
        .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt8Array::from(vec![1]));

        // array_length([[1, 2, 3], [4, 5, 6]], 2) = 3
        let array = array_length(&[
            list_array.clone(),
            ColumnarValue::Scalar(ScalarValue::UInt8(Some(2_u8))),
        ])
        .expect("failed to initialize function array_length")
        .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_length");

        assert_eq!(result, &UInt8Array::from(vec![3]));

        // array_length([[1, 2, 3], [4, 5, 6]], 3) = NULL
        let array = array_length(&[
            list_array.clone(),
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
        // array_dims([1, 2]) = [2]
        let values_builder = Int64Builder::new();
        let mut builder = ListBuilder::new(values_builder);
        builder.values().append_value(1);
        builder.append(true);
        builder.values().append_value(2);
        builder.append(true);
        let list_array = builder.finish();

        let array = array_dims(&[ColumnarValue::Array(Arc::new(list_array))])
            .expect("failed to initialize function array_dims")
            .into_array(1);
        let result =
            as_list_array(&array).expect("failed to initialize function array_dims");

        assert_eq!(
            &[2],
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
        // array_dims([[1, 2], [3, 4]]) = [2, 2]
        let int64_values_builder = Int64Builder::new();
        let list_values_builder = ListBuilder::new(int64_values_builder);

        let mut builder = ListBuilder::new(list_values_builder);
        builder.values().values().append_value(1);
        builder.values().append(true);
        builder.values().values().append_value(2);
        builder.values().append(true);
        builder.append(true);
        builder.values().values().append_value(3);
        builder.values().append(true);
        builder.values().values().append_value(4);
        builder.values().append(true);
        builder.append(true);
        let list_array = builder.finish();

        let array = array_dims(&[ColumnarValue::Array(Arc::new(list_array))])
            .expect("failed to initialize function array_dims")
            .into_array(1);
        let result =
            as_list_array(&array).expect("failed to initialize function array_dims");

        assert_eq!(
            &[2, 1],
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
        // array_ndims([1, 2]) = 1
        let values_builder = Int64Builder::new();
        let mut builder = ListBuilder::new(values_builder);
        builder.values().append_value(1);
        builder.append(true);
        builder.values().append_value(2);
        builder.append(true);
        let list_array = builder.finish();

        let array = array_ndims(&[ColumnarValue::Array(Arc::new(list_array))])
            .expect("failed to initialize function array_ndims")
            .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt8Array::from(vec![1]));
    }

    #[test]
    fn test_nested_array_ndims() {
        // array_ndims([[1, 2], [3, 4]]) = 2
        let int64_values_builder = Int64Builder::new();
        let list_values_builder = ListBuilder::new(int64_values_builder);

        let mut builder = ListBuilder::new(list_values_builder);
        builder.values().values().append_value(1);
        builder.values().append(true);
        builder.values().values().append_value(2);
        builder.values().append(true);
        builder.append(true);
        builder.values().values().append_value(3);
        builder.values().append(true);
        builder.values().values().append_value(4);
        builder.values().append(true);
        builder.append(true);
        let list_array = builder.finish();

        let array = array_ndims(&[ColumnarValue::Array(Arc::new(list_array))])
            .expect("failed to initialize function array_ndims")
            .into_array(1);
        let result =
            as_uint8_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &UInt8Array::from(vec![2]));
    }
}
