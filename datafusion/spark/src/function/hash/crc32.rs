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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use crc32fast::Hasher;
use datafusion_common::cast::{
    as_binary_array, as_binary_view_array, as_large_binary_array,
};
use datafusion_common::{assert_eq_or_internal_err, exec_err, DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#crc32>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCrc32 {
    signature: Signature,
}

impl Default for SparkCrc32 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCrc32 {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCrc32 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "crc32"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_crc32, vec![])(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!(
                "`crc32` function requires 1 argument, got {}",
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                Ok(vec![arg_types[0].clone()])
            }
            DataType::Utf8 | DataType::Utf8View => Ok(vec![DataType::Binary]),
            DataType::LargeUtf8 => Ok(vec![DataType::LargeBinary]),
            DataType::Null => Ok(vec![DataType::Binary]),
            _ => exec_err!("`crc32` function does not support type {}", arg_types[0]),
        }
    }
}

fn spark_crc32_digest(value: &[u8]) -> i64 {
    let mut hasher = Hasher::new();
    hasher.update(value);
    hasher.finalize() as i64
}

fn spark_crc32_impl<'a>(input: impl Iterator<Item = Option<&'a [u8]>>) -> ArrayRef {
    let result = input
        .map(|value| value.map(spark_crc32_digest))
        .collect::<Int64Array>();
    Arc::new(result)
}

fn spark_crc32(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        assert_eq_or_internal_err!(
            args.len(),
            1,
            "Spark `crc32` function requires 1 argument"
        );
        unreachable!()
    };

    match input.data_type() {
        DataType::Binary => {
            let input = as_binary_array(input)?;
            Ok(spark_crc32_impl(input.iter()))
        }
        DataType::LargeBinary => {
            let input = as_large_binary_array(input)?;
            Ok(spark_crc32_impl(input.iter()))
        }
        DataType::BinaryView => {
            let input = as_binary_view_array(input)?;
            Ok(spark_crc32_impl(input.iter()))
        }
        _ => {
            exec_err!(
                "Spark `crc32` function: argument must be binary or large binary, got {:?}",
                input.data_type()
            )
        }
    }
}
