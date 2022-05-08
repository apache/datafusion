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
        .map(|(i, arg)| -> Result<(Field, ArrayRef)> {
            let field_name = format!("c{}", i);
            match arg.data_type() {
                DataType::Utf8
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
                | DataType::UInt64 => Ok((
                    Field::new(field_name.as_str(), arg.data_type().clone(), true),
                    arg.clone(),
                )),
                data_type => Err(DataFusionError::NotImplemented(format!(
                    "Struct is not implemented for type '{:?}'.",
                    data_type
                ))),
            }
        })
        .collect::<Result<Vec<_>>>()?;

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
