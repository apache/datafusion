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
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use sha2::{self, Digest};
use std::any::Any;
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
        let [values, bit_lengths] = take_function_args(self.name(), args.args.iter())?;

        match (values, bit_lengths) {
            (
                ColumnarValue::Scalar(value_scalar),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(bit_length))),
            ) => {
                if value_scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }

                // Accept both Binary and Utf8 scalars (depending on coercion)
                let bytes = match value_scalar {
                    ScalarValue::Binary(Some(b)) => b.as_slice(),
                    ScalarValue::LargeBinary(Some(b)) => b.as_slice(),
                    ScalarValue::BinaryView(Some(b)) => b.as_slice(),
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)) => s.as_bytes(),
                    other => {
                        return internal_err!(
                            "Unsupported scalar datatype for sha2: {}",
                            other.data_type()
                        );
                    }
                };

                let out = match bit_length {
                    224 => {
                        let mut digest = sha2::Sha224::default();
                        digest.update(bytes);
                        Some(hex_encode(digest.finalize()))
                    }
                    0 | 256 => {
                        let mut digest = sha2::Sha256::default();
                        digest.update(bytes);
                        Some(hex_encode(digest.finalize()))
                    }
                    384 => {
                        let mut digest = sha2::Sha384::default();
                        digest.update(bytes);
                        Some(hex_encode(digest.finalize()))
                    }
                    512 => {
                        let mut digest = sha2::Sha512::default();
                        digest.update(bytes);
                        Some(hex_encode(digest.finalize()))
                    }
                    _ => None,
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)))
            }
            // Array values + scalar bit length (common case: sha2(col, 256))
            (
                ColumnarValue::Array(values_array),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(bit_length))),
            ) => {
                let output: ArrayRef = match values_array.data_type() {
                    DataType::Binary => sha2_binary_scalar_bitlen(
                        &values_array.as_binary::<i32>(),
                        *bit_length,
                    ),
                    DataType::LargeBinary => sha2_binary_scalar_bitlen(
                        &values_array.as_binary::<i64>(),
                        *bit_length,
                    ),
                    DataType::BinaryView => sha2_binary_scalar_bitlen(
                        &values_array.as_binary_view(),
                        *bit_length,
                    ),
                    dt => return internal_err!("Unsupported datatype for sha2: {dt}"),
                };
                Ok(ColumnarValue::Array(output))
            }
            (
                ColumnarValue::Scalar(_),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            (
                ColumnarValue::Array(_),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            _ => {
                // Fallback to existing behavior for any array/mixed cases
                make_scalar_function(sha2_impl, vec![])(&args.args)
            }
        }
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
    sha2_binary_bitlen_iter(values, bit_lengths.iter())
}

fn sha2_binary_scalar_bitlen<'a, BinaryArrType>(
    values: &BinaryArrType,
    bit_length: i32,
) -> ArrayRef
where
    BinaryArrType: BinaryArrayType<'a>,
{
    sha2_binary_bitlen_iter(values, std::iter::repeat(Some(bit_length)))
}

fn sha2_binary_bitlen_iter<'a, BinaryArrType, I>(
    values: &BinaryArrType,
    bit_lengths: I,
) -> ArrayRef
where
    BinaryArrType: BinaryArrayType<'a>,
    I: Iterator<Item = Option<i32>>,
{
    let array = values
        .iter()
        .zip(bit_lengths)
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

const HEX_CHARS: [u8; 16] = *b"0123456789abcdef";

#[inline]
fn hex_encode<T: AsRef<[u8]>>(data: T) -> String {
    let bytes = data.as_ref();
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for &b in bytes {
        let hi = b >> 4;
        let lo = b & 0x0F;
        out.push(HEX_CHARS[hi as usize]);
        out.push(HEX_CHARS[lo as usize]);
    }
    // SAFETY: out contains only ASCII
    unsafe { String::from_utf8_unchecked(out) }
}
