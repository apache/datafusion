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

//! Struct expressions

use arrow::array::*;
use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

fn array_struct(args: &[ArrayRef]) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(
            "struct requires at least one argument".to_string(),
        ));
    }

    let vec: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(i, arg)| -> (Field, ArrayRef) {
            let field_name = format!("c_{}", i);
            match arg.data_type() {
                DataType::Utf8 => (
                    Field::new(field_name.as_str(), DataType::Utf8, true),
                    arg.clone(),
                ),
                DataType::LargeUtf8 => (
                    Field::new(field_name.as_str(), DataType::LargeUtf8, true),
                    arg.clone(),
                ),
                DataType::Boolean => (
                    Field::new(field_name.as_str(), DataType::Boolean, true),
                    arg.clone(),
                ),
                DataType::Float32 => (
                    Field::new(field_name.as_str(), DataType::Float64, true),
                    arg.clone(),
                ),
                DataType::Float64 => (
                    Field::new(field_name.as_str(), DataType::Float64, true),
                    arg.clone(),
                ),
                DataType::Int8 => (
                    Field::new(field_name.as_str(), DataType::Int8, true),
                    arg.clone(),
                ),
                DataType::Int16 => (
                    Field::new(field_name.as_str(), DataType::Int16, true),
                    arg.clone(),
                ),
                DataType::Int32 => (
                    Field::new(field_name.as_str(), DataType::Int32, true),
                    arg.clone(),
                ),
                DataType::Int64 => (
                    Field::new(field_name.as_str(), DataType::Int64, true),
                    arg.clone(),
                ),
                DataType::UInt8 => (
                    Field::new(field_name.as_str(), DataType::UInt8, true),
                    arg.clone(),
                ),
                DataType::UInt16 => (
                    Field::new(field_name.as_str(), DataType::UInt16, true),
                    arg.clone(),
                ),
                DataType::UInt32 => (
                    Field::new(field_name.as_str(), DataType::UInt32, true),
                    arg.clone(),
                ),
                DataType::UInt64 => (
                    Field::new(field_name.as_str(), DataType::UInt64, true),
                    arg.clone(),
                ),
                data_type => unimplemented!("struct not support {} type", data_type),
            }
        })
        .collect();

    Ok(Arc::new(StructArray::from(vec)))
}

/// put values in a struct array.
pub fn struct_expr(values: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays: Vec<ArrayRef> = values
        .iter()
        .map(|x| match x {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array().clone(),
        })
        .collect();
    Ok(ColumnarValue::Array(array_struct(arrays.as_slice())?))
}
