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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{
        DataType,
        DataType::{Int64, Utf8},
    },
};

use datafusion_common::{cast::as_int64_array, exec_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Spark-compatible `char` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#char>
#[derive(Debug)]
pub struct SparkChar {
    signature: Signature,
}

impl Default for SparkChar {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkChar {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkChar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_chr(&args.args)
    }
}

/// Returns the ASCII character having the binary equivalent to the input expression.
/// E.g., chr(65) = 'A'.
/// Compatible with Apache Spark's Chr function
fn spark_chr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = args[0].clone();
    match array {
        ColumnarValue::Array(array) => {
            let array = chr(&[array])?;
            Ok(ColumnarValue::Array(array))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => {
            if value < 0 {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "".to_string(),
                ))))
            } else {
                match core::char::from_u32((value % 256) as u32) {
                    Some(ch) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                        ch.to_string(),
                    )))),
                    None => {
                        exec_err!("requested character was incompatible for encoding.")
                    }
                }
            }
        }
        _ => exec_err!("The argument must be an Int64 array or scalar."),
    }
}

fn chr(args: &[ArrayRef]) -> Result<ArrayRef> {
    let integer_array = as_int64_array(&args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    let result = integer_array
        .iter()
        .map(|integer: Option<i64>| {
            integer
                .map(|integer| {
                    if integer < 0 {
                        return Ok("".to_string()); // Return empty string for negative integers
                    }
                    match core::char::from_u32((integer % 256) as u32) {
                        Some(ch) => Ok(ch.to_string()),
                        None => {
                            exec_err!("requested character not compatible for encoding.")
                        }
                    }
                })
                .transpose()
        })
        .collect::<Result<StringArray>>()?;

    Ok(Arc::new(result) as ArrayRef)
}
