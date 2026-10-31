// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{Array, ArrayRef, AsArray, Int32Array};
use arrow::compute::kernels::length::length as arrow_length;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_err};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use std::sync::Arc;

/// Spark-compatible `size` function.
///
/// Returns the number of elements in an array or the number of key-value pairs in a map.
/// Returns -1 for null input (Spark behavior).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSize {
    signature: Signature,
}

impl Default for SparkSize {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSize {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // Array Type
                    TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                        arguments: vec![ArrayFunctionArgument::Array],
                        array_coercion: None,
                    }),
                    // Map Type
                    TypeSignature::ArraySignature(ArrayFunctionSignature::MapArray),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSize {
    fn name(&self) -> &str {
        "size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        // nullable=false for legacy behavior (NULL -> -1); set to input nullability for null-on-null
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_size_inner, vec![])(&args.args)
    }
}

fn spark_size_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let array = &args[0];

    match array.data_type() {
        DataType::List(_) => {
            if array.null_count() == 0 {
                Ok(arrow_length(array)?)
            } else {
                let list_array = array.as_list::<i32>();
                let lengths: Vec<i32> = list_array
                    .offsets()
                    .lengths()
                    .enumerate()
                    .map(|(i, len)| if array.is_null(i) { -1 } else { len as i32 })
                    .collect();
                Ok(Arc::new(Int32Array::from(lengths)))
            }
        }
        DataType::FixedSizeList(_, size) => {
            if array.null_count() == 0 {
                Ok(arrow_length(array)?)
            } else {
                let length: Vec<i32> = (0..array.len())
                    .map(|i| if array.is_null(i) { -1 } else { *size })
                    .collect();
                Ok(Arc::new(Int32Array::from(length)))
            }
        }
        DataType::LargeList(_) => {
            // Arrow length kernel returns Int64 for LargeList
            let list_array = array.as_list::<i64>();
            if array.null_count() == 0 {
                let lengths: Vec<i32> = list_array
                    .offsets()
                    .lengths()
                    .map(|len| len as i32)
                    .collect();
                Ok(Arc::new(Int32Array::from(lengths)))
            } else {
                let lengths: Vec<i32> = list_array
                    .offsets()
                    .lengths()
                    .enumerate()
                    .map(|(i, len)| if array.is_null(i) { -1 } else { len as i32 })
                    .collect();
                Ok(Arc::new(Int32Array::from(lengths)))
            }
        }
        DataType::Map(_, _) => {
            let map_array = array.as_map();
            let length: Vec<i32> = if array.null_count() == 0 {
                map_array
                    .offsets()
                    .lengths()
                    .map(|len| len as i32)
                    .collect()
            } else {
                map_array
                    .offsets()
                    .lengths()
                    .enumerate()
                    .map(|(i, len)| if array.is_null(i) { -1 } else { len as i32 })
                    .collect()
            };
            Ok(Arc::new(Int32Array::from(length)))
        }
        DataType::Null => Ok(Arc::new(Int32Array::from(vec![-1; array.len()]))),
        dt => {
            plan_err!("size function does not support type: {}", dt)
        }
    }
}
