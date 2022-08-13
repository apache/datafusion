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
use arrow::datatypes::DataType;
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

/// Create an array of FixedSizeList from a set of individual Arrays
/// where each element in the output FixedSizeList is the result of
/// concatenating the corresponding values in the input Arrays
macro_rules! make_fixed_size_list {
    ($ARGS:expr, $ARRAY_TYPE:ident, $BUILDER_TYPE:ident) => {{
        // downcast all arguments to their common format
        let args =
            downcast_vec!($ARGS, $ARRAY_TYPE).collect::<Result<Vec<&$ARRAY_TYPE>>>()?;

        let mut builder = FixedSizeListBuilder::<$BUILDER_TYPE>::new(
            <$BUILDER_TYPE>::new(args[0].len()),
            args.len() as i32,
        );
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

fn arrays_to_fixed_size_list_array(args: &[ArrayRef]) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(
            "array requires at least one argument".to_string(),
        ));
    }

    let res = match args[0].data_type() {
        DataType::Utf8 => make_fixed_size_list!(args, StringArray, StringBuilder),
        DataType::LargeUtf8 => {
            make_fixed_size_list!(args, LargeStringArray, LargeStringBuilder)
        }
        DataType::Boolean => make_fixed_size_list!(args, BooleanArray, BooleanBuilder),
        DataType::Float32 => make_fixed_size_list!(args, Float32Array, Float32Builder),
        DataType::Float64 => make_fixed_size_list!(args, Float64Array, Float64Builder),
        DataType::Int8 => make_fixed_size_list!(args, Int8Array, Int8Builder),
        DataType::Int16 => make_fixed_size_list!(args, Int16Array, Int16Builder),
        DataType::Int32 => make_fixed_size_list!(args, Int32Array, Int32Builder),
        DataType::Int64 => make_fixed_size_list!(args, Int64Array, Int64Builder),
        DataType::UInt8 => make_fixed_size_list!(args, UInt8Array, UInt8Builder),
        DataType::UInt16 => make_fixed_size_list!(args, UInt16Array, UInt16Builder),
        DataType::UInt32 => make_fixed_size_list!(args, UInt32Array, UInt32Builder),
        DataType::UInt64 => make_fixed_size_list!(args, UInt64Array, UInt64Builder),
        data_type => {
            return Err(DataFusionError::NotImplemented(format!(
                "Array is not implemented for type '{:?}'.",
                data_type
            )))
        }
    };
    Ok(res)
}

/// put values in an array.
pub fn make_array(values: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays: Vec<ArrayRef> = values
        .iter()
        .map(|x| match x {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        })
        .collect();
    Ok(ColumnarValue::Array(arrays_to_fixed_size_list_array(
        arrays.as_slice(),
    )?))
}
