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

use arrow::array::{Array, ArrayRef, BinaryBuilder};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#unhex>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnhex {
    signature: Signature,
}

impl Default for SparkUnhex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUnhex {
    pub fn new() -> Self {
        let string = Coercion::new_exact(TypeSignatureClass::Native(logical_string()));

        Self {
            signature: Signature::coercible(vec![string], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkUnhex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "unhex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_unhex(&args.args)
    }
}

#[inline]
fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Decodes a hex-encoded byte slice into binary data.
/// Returns `true` if decoding succeeded, `false` if the input contains invalid hex characters.
fn unhex_common(bytes: &[u8], out: &mut Vec<u8>) -> bool {
    if bytes.is_empty() {
        return true;
    }

    let mut i = 0usize;

    // If the hex string length is odd, implicitly left-pad with '0'.
    if (bytes.len() & 1) == 1 {
        match hex_nibble(bytes[0]) {
            // Equivalent to (0 << 4) | lo
            Some(lo) => out.push(lo),
            None => return false,
        }
        i = 1;
    }

    while i + 1 < bytes.len() {
        match (hex_nibble(bytes[i]), hex_nibble(bytes[i + 1])) {
            (Some(hi), Some(lo)) => out.push((hi << 4) | lo),
            _ => return false,
        }
        i += 2;
    }

    true
}

/// Converts an iterator of hex strings to a binary array.
fn unhex_array<I, T>(
    iter: I,
    len: usize,
    capacity: usize,
) -> Result<ArrayRef, DataFusionError>
where
    I: Iterator<Item = Option<T>>,
    T: AsRef<str>,
{
    let mut builder = BinaryBuilder::with_capacity(len, capacity);
    let mut buffer = Vec::new();

    for v in iter {
        if let Some(s) = v {
            buffer.clear();
            buffer.reserve(s.as_ref().len().div_ceil(2));
            if unhex_common(s.as_ref().as_bytes(), &mut buffer) {
                builder.append_value(&buffer);
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Convert a single hex string to binary
fn unhex_scalar(s: &str) -> Option<Vec<u8>> {
    let mut buffer = Vec::with_capacity(s.len().div_ceil(2));
    if unhex_common(s.as_bytes(), &mut buffer) {
        Some(buffer)
    } else {
        None
    }
}

fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let [args] = take_function_args("unhex", args)?;

    match args {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => {
                let array = as_string_array(array)?;
                let capacity = array.values().len().div_ceil(2);
                Ok(ColumnarValue::Array(unhex_array(
                    array.iter(),
                    array.len(),
                    capacity,
                )?))
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array)?;
                // Estimate capacity since StringViewArray data can be scattered or inlined.
                let capacity = array.len() * 32;
                Ok(ColumnarValue::Array(unhex_array(
                    array.iter(),
                    array.len(),
                    capacity,
                )?))
            }
            DataType::LargeUtf8 => {
                let array = as_large_string_array(array)?;
                let capacity = array.values().len().div_ceil(2);
                Ok(ColumnarValue::Array(unhex_array(
                    array.iter(),
                    array.len(),
                    capacity,
                )?))
            }
            _ => exec_err!(
                "unhex only supports string argument, but got: {}",
                array.data_type()
            ),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Utf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::LargeUtf8(None) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            ScalarValue::Utf8(Some(s))
            | ScalarValue::Utf8View(Some(s))
            | ScalarValue::LargeUtf8(Some(s)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(unhex_scalar(s))))
            }
            _ => {
                exec_err!(
                    "unhex only supports string argument, but got: {}",
                    sv.data_type()
                )
            }
        },
    }
}
