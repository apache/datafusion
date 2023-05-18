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
use arrow::datatypes::{DataType, Field};
use core::any::type_name;
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
            FixedSizeListBuilder::<$BUILDER_TYPE>::new(builder, args.len() as i32);
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
        return Ok(Arc::new(builder.finish()));
    }};
}

fn array_array(args: &[ArrayRef]) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(
            "array requires at least one argument".to_string(),
        ));
    }

    let data_type = args[0].data_type();
    match data_type {
        DataType::List(_) => {
            let arrays =
                downcast_vec!(args, ListArray).collect::<Result<Vec<&ListArray>>>()?;
            let len = arrays.len();
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

            let list_data_type = DataType::FixedSizeList(
                Arc::new(Field::new("item", data_type.clone(), false)),
                len as i32,
            );

            let list_data = ArrayData::builder(list_data_type)
                .len(1)
                .add_child_data(mutable.freeze())
                .build()
                .unwrap();

            return Ok(Arc::new(FixedSizeListArray::from(list_data)));
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

pub fn array_ndims(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = match &args[0] {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
    };

    fn count_ndims(result: i64, array: ArrayRef) -> Result<i64> {
        match array.data_type() {
            DataType::List(..) => {
                let list_array = downcast_arg!(array, ListArray);
                match list_array.is_empty() {
                    true => Ok(result),
                    false => count_ndims(result + 1, list_array.value(0)),
                }
            }
            DataType::FixedSizeList(..) => {
                let list_array = downcast_arg!(array, FixedSizeListArray);
                match list_array.is_empty() {
                    true => Ok(result),
                    false => count_ndims(result + 1, list_array.value(0)),
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
            | DataType::UInt64 => Ok(result),
            data_type => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Array is not implemented for type '{data_type:?}'."
                )))
            }
        }
    }
    let result: i64 = 0;
    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(vec![
        count_ndims(result, array)?,
    ]))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion_common::cast::{as_fixed_size_list_array, as_int64_array};
    use datafusion_common::scalar::ScalarValue;

    #[test]
    fn test_array() {
        let args = [
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let array = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        let result = as_fixed_size_list_array(&array)
            .expect("failed to initialize function array");
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
        let args = [
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![3, 4]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![5, 6]))),
        ];
        let array = array(&args)
            .expect("failed to initialize function array")
            .into_array(1);
        let result = as_fixed_size_list_array(&array)
            .expect("failed to initialize function array");
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
    fn test_array_ndims() {
        // array_ndims([1, 2]) = 1
        let values_builder = Int64Builder::new();
        let mut builder = FixedSizeListBuilder::new(values_builder, 1);
        builder.values().append_value(1);
        builder.append(true);
        builder.values().append_value(2);
        builder.append(true);
        let list_array = builder.finish();

        let array = array_ndims(&[ColumnarValue::Array(Arc::new(list_array))])
            .expect("failed to initialize function array_ndims")
            .into_array(1);
        let result =
            as_int64_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &Int64Array::from(vec![1]),);
    }

    #[test]
    fn test_nested_array_ndims() {
        // array_ndims([[1, 2], [3, 4]]) = 2
        let int64_values_builder = Int64Builder::new();
        let list_values_builder = ListBuilder::new(int64_values_builder);

        let mut builder = FixedSizeListBuilder::new(list_values_builder, 2);
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
            as_int64_array(&array).expect("failed to initialize function array_ndims");

        assert_eq!(result, &Int64Array::from(vec![2]),);
    }
}
