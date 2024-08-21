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

use crate::scalar_funcs::hex::hex_strings;
use crate::spark_hash::{create_murmur3_hashes, create_xxhash64_hashes};

use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray};
use datafusion::functions::crypto::{sha224, sha256, sha384, sha512};
use datafusion_common::cast::as_binary_array;
use datafusion_common::{exec_err, internal_err, DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};
use std::sync::Arc;

/// Spark compatible murmur3 hash (just `hash` in Spark) in vectorized execution fashion
pub fn spark_murmur3_hash(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let length = args.len();
    let seed = &args[length - 1];
    match seed {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(seed))) => {
            // iterate over the arguments to find out the length of the array
            let num_rows = args[0..args.len() - 1]
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let mut hashes: Vec<u32> = vec![0_u32; num_rows];
            hashes.fill(*seed as u32);
            let arrays = args[0..args.len() - 1]
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => Arc::clone(array),
                    ColumnarValue::Scalar(scalar) => {
                        scalar.clone().to_array_of_size(num_rows).unwrap()
                    }
                })
                .collect::<Vec<ArrayRef>>();
            create_murmur3_hashes(&arrays, &mut hashes)?;
            if num_rows == 1 {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                    hashes[0] as i32,
                ))))
            } else {
                let hashes: Vec<i32> = hashes.into_iter().map(|x| x as i32).collect();
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(hashes))))
            }
        }
        _ => {
            internal_err!(
                "The seed of function murmur3_hash must be an Int32 scalar value, but got: {:?}.",
                seed
            )
        }
    }
}

/// Spark compatible xxhash64 in vectorized execution fashion
pub fn spark_xxhash64(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let length = args.len();
    let seed = &args[length - 1];
    match seed {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(seed))) => {
            // iterate over the arguments to find out the length of the array
            let num_rows = args[0..args.len() - 1]
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let mut hashes: Vec<u64> = vec![0_u64; num_rows];
            hashes.fill(*seed as u64);
            let arrays = args[0..args.len() - 1]
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => Arc::clone(array),
                    ColumnarValue::Scalar(scalar) => {
                        scalar.clone().to_array_of_size(num_rows).unwrap()
                    }
                })
                .collect::<Vec<ArrayRef>>();
            create_xxhash64_hashes(&arrays, &mut hashes)?;
            if num_rows == 1 {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                    hashes[0] as i64,
                ))))
            } else {
                let hashes: Vec<i64> = hashes.into_iter().map(|x| x as i64).collect();
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(hashes))))
            }
        }
        _ => {
            internal_err!(
                "The seed of function xxhash64 must be an Int64 scalar value, but got: {:?}.",
                seed
            )
        }
    }
}

/// `sha224` function that simulates Spark's `sha2` expression with bit width 224
pub fn spark_sha224(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    wrap_digest_result_as_hex_string(args, sha224().fun())
}

/// `sha256` function that simulates Spark's `sha2` expression with bit width 0 or 256
pub fn spark_sha256(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    wrap_digest_result_as_hex_string(args, sha256().fun())
}

/// `sha384` function that simulates Spark's `sha2` expression with bit width 384
pub fn spark_sha384(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    wrap_digest_result_as_hex_string(args, sha384().fun())
}

/// `sha512` function that simulates Spark's `sha2` expression with bit width 512
pub fn spark_sha512(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    wrap_digest_result_as_hex_string(args, sha512().fun())
}

// Spark requires hex string as the result of sha2 functions, we have to wrap the
// result of digest functions as hex string
fn wrap_digest_result_as_hex_string(
    args: &[ColumnarValue],
    digest: ScalarFunctionImplementation,
) -> Result<ColumnarValue, DataFusionError> {
    let value = digest(args)?;
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
