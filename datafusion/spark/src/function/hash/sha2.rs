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

extern crate datafusion_functions;

use crate::function::error_utils::{
    invalid_arg_count_exec_err, unsupported_data_type_exec_err,
};
use crate::function::math::hex::spark_hex;
use arrow::array::{ArrayRef, AsArray, StringArray};
use arrow::datatypes::{DataType, UInt32Type};
use datafusion_common::{exec_err, internal_datafusion_err, Result, ScalarValue};
use datafusion_expr::Signature;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Volatility};
pub use datafusion_functions::crypto::basic::{sha224, sha256, sha384, sha512};
use std::any::Any;
use std::sync::Arc;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#sha2>
#[derive(Debug)]
pub struct SparkSha2 {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkSha2 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSha2 {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkSha2 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[1].is_null() {
            return Ok(DataType::Null);
        }
        Ok(match arg_types[0] {
            DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Utf8
            | DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary => DataType::Utf8,
            DataType::Null => DataType::Null,
            _ => {
                return exec_err!(
                    "{} function can only accept strings or binary arrays.",
                    self.name()
                )
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 2] = args.args.try_into().map_err(|_| {
            internal_datafusion_err!("Expected 2 arguments for function sha2")
        })?;

        sha2(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (2, 2),
                arg_types.len(),
            ));
        }
        let expr_type = match &arg_types[0] {
            DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Utf8
            | DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary
            | DataType::Null => Ok(arg_types[0].clone()),
            _ => Err(unsupported_data_type_exec_err(
                self.name(),
                "String, Binary",
                &arg_types[0],
            )),
        }?;
        let bit_length_type = if arg_types[1].is_numeric() {
            Ok(DataType::UInt32)
        } else if arg_types[1].is_null() {
            Ok(DataType::Null)
        } else {
            Err(unsupported_data_type_exec_err(
                self.name(),
                "Numeric Type",
                &arg_types[1],
            ))
        }?;

        Ok(vec![expr_type, bit_length_type])
    }
}

pub fn sha2(args: [ColumnarValue; 2]) -> Result<ColumnarValue> {
    match args {
        [ColumnarValue::Scalar(ScalarValue::Utf8(expr_arg)), ColumnarValue::Scalar(ScalarValue::UInt32(Some(bit_length_arg)))] => {
            match bit_length_arg {
                0 | 256 => sha256(&[ColumnarValue::from(ScalarValue::Utf8(expr_arg))]),
                224 => sha224(&[ColumnarValue::from(ScalarValue::Utf8(expr_arg))]),
                384 => sha384(&[ColumnarValue::from(ScalarValue::Utf8(expr_arg))]),
                512 => sha512(&[ColumnarValue::from(ScalarValue::Utf8(expr_arg))]),
                _ => exec_err!(
                    "sha2 function only supports 224, 256, 384, and 512 bit lengths."
                ),
            }
            .map(|hashed| spark_hex(&[hashed]).unwrap())
        }
        [ColumnarValue::Array(expr_arg), ColumnarValue::Scalar(ScalarValue::UInt32(Some(bit_length_arg)))] => {
            match bit_length_arg {
                0 | 256 => sha256(&[ColumnarValue::from(expr_arg)]),
                224 => sha224(&[ColumnarValue::from(expr_arg)]),
                384 => sha384(&[ColumnarValue::from(expr_arg)]),
                512 => sha512(&[ColumnarValue::from(expr_arg)]),
                _ => exec_err!(
                    "sha2 function only supports 224, 256, 384, and 512 bit lengths."
                ),
            }
            .map(|hashed| spark_hex(&[hashed]).unwrap())
        }
        [ColumnarValue::Scalar(ScalarValue::Utf8(expr_arg)), ColumnarValue::Array(bit_length_arg)] =>
        {
            let arr: StringArray = bit_length_arg
                .as_primitive::<UInt32Type>()
                .iter()
                .map(|bit_length| {
                    match sha2([
                        ColumnarValue::Scalar(ScalarValue::Utf8(expr_arg.clone())),
                        ColumnarValue::Scalar(ScalarValue::UInt32(bit_length)),
                    ])
                    .unwrap()
                    {
                        ColumnarValue::Scalar(ScalarValue::Utf8(str)) => str,
                        ColumnarValue::Array(arr) => arr
                            .as_string::<i32>()
                            .iter()
                            .map(|str| str.unwrap().to_string())
                            .next(), // first element
                        _ => unreachable!(),
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(arr) as ArrayRef))
        }
        [ColumnarValue::Array(expr_arg), ColumnarValue::Array(bit_length_arg)] => {
            let expr_iter = expr_arg.as_string::<i32>().iter();
            let bit_length_iter = bit_length_arg.as_primitive::<UInt32Type>().iter();
            let arr: StringArray = expr_iter
                .zip(bit_length_iter)
                .map(|(expr, bit_length)| {
                    match sha2([
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                            expr.unwrap().to_string(),
                        ))),
                        ColumnarValue::Scalar(ScalarValue::UInt32(bit_length)),
                    ])
                    .unwrap()
                    {
                        ColumnarValue::Scalar(ScalarValue::Utf8(str)) => str,
                        ColumnarValue::Array(arr) => arr
                            .as_string::<i32>()
                            .iter()
                            .map(|str| str.unwrap().to_string())
                            .next(), // first element
                        _ => unreachable!(),
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(arr) as ArrayRef))
        }
        _ => exec_err!("Unsupported argument types for sha2 function"),
    }
}
