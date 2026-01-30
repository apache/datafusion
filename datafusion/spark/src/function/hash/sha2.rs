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

use arrow::array::{ArrayRef, AsArray, BinaryArrayType, Int32Array, StringArray};
use arrow::datatypes::{DataType, Int32Type};
use datafusion_common::types::{
    NativeType, logical_binary, logical_int32, logical_string,
};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use sha2::{self, Digest};
use std::any::Any;
use std::fmt::Write;
use std::sync::Arc;

/// Differs from DataFusion version in allowing array input for bit lengths, and
/// also hex encoding the output.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#sha2>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSha2 {
    signature: Signature,
}

impl Default for SparkSha2 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSha2 {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_binary()),
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::Binary,
                    ),
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int32()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int32,
                    ),
                ],
                Volatility::Immutable,
            ),
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(sha2_impl, vec![])(&args.args)
    }
}

fn sha2_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [values, bit_lengths] = take_function_args("sha2", args)?;

    let bit_lengths = bit_lengths.as_primitive::<Int32Type>();
    let output = match values.data_type() {
        DataType::Binary => sha2_binary_impl(&values.as_binary::<i32>(), bit_lengths),
        DataType::LargeBinary => {
            sha2_binary_impl(&values.as_binary::<i64>(), bit_lengths)
        }
        DataType::BinaryView => sha2_binary_impl(&values.as_binary_view(), bit_lengths),
        dt => return internal_err!("Unsupported datatype for sha2: {dt}"),
    };
    Ok(output)
}

fn sha2_binary_impl<'a, BinaryArrType>(
    values: &BinaryArrType,
    bit_lengths: &Int32Array,
) -> ArrayRef
where
    BinaryArrType: BinaryArrayType<'a>,
{
    let array = values
        .iter()
        .zip(bit_lengths.iter())
        .map(|(value, bit_length)| match (value, bit_length) {
            (Some(value), Some(224)) => {
                let mut digest = sha2::Sha224::default();
                digest.update(value);
                Some(hex_encode(digest.finalize()))
            }
            (Some(value), Some(0 | 256)) => {
                let mut digest = sha2::Sha256::default();
                digest.update(value);
                Some(hex_encode(digest.finalize()))
            }
            (Some(value), Some(384)) => {
                let mut digest = sha2::Sha384::default();
                digest.update(value);
                Some(hex_encode(digest.finalize()))
            }
            (Some(value), Some(512)) => {
                let mut digest = sha2::Sha512::default();
                digest.update(value);
                Some(hex_encode(digest.finalize()))
            }
            // Unknown bit-lengths go to null, same as in Spark
            _ => None,
        })
        .collect::<StringArray>();
    Arc::new(array)
}

fn hex_encode<T: AsRef<[u8]>>(data: T) -> String {
    let mut s = String::with_capacity(data.as_ref().len() * 2);
    for b in data.as_ref() {
        // Writing to a string never errors, so we can unwrap here.
        write!(&mut s, "{b:02x}").unwrap();
    }
    s
}
