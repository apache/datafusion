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

use crate::math_funcs::hex::hex_strings;
use arrow_array::{Array, StringArray};
use datafusion::functions::crypto::{sha224, sha256, sha384, sha512};
use datafusion_common::cast::as_binary_array;
use datafusion_common::{exec_err, DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDF};
use std::sync::Arc;

/// `sha224` function that simulates Spark's `sha2` expression with bit width 224
pub fn spark_sha224(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    wrap_digest_result_as_hex_string(args, sha224())
}

/// `sha256` function that simulates Spark's `sha2` expression with bit width 0 or 256
pub fn spark_sha256(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    wrap_digest_result_as_hex_string(args, sha256())
}

/// `sha384` function that simulates Spark's `sha2` expression with bit width 384
pub fn spark_sha384(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    wrap_digest_result_as_hex_string(args, sha384())
}

/// `sha512` function that simulates Spark's `sha2` expression with bit width 512
pub fn spark_sha512(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    wrap_digest_result_as_hex_string(args, sha512())
}

// Spark requires hex string as the result of sha2 functions, we have to wrap the
// result of digest functions as hex string
fn wrap_digest_result_as_hex_string(
    args: &[ColumnarValue],
    digest: Arc<ScalarUDF>,
) -> Result<ColumnarValue, DataFusionError> {
    let row_count = match &args[0] {
        ColumnarValue::Array(array) => array.len(),
        ColumnarValue::Scalar(_) => 1,
    };
    let value = digest.invoke_batch(args, row_count)?;
    match value {
        ColumnarValue::Array(array) => {
            let binary_array = as_binary_array(&array)?;
            let string_array: StringArray = binary_array
                .iter()
                .map(|opt| opt.map(hex_strings::<_>))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(string_array)))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(opt.map(hex_strings::<_>)),
        )),
        _ => {
            exec_err!(
                "digest function should return binary value, but got: {:?}",
                value.data_type()
            )
        }
    }
}
