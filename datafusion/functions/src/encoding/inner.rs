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

//! Encoding expressions

use arrow::{
    array::{Array, ArrayRef, BinaryArray, OffsetSizeTrait, StringArray},
    datatypes::DataType,
};
use base64::{engine::general_purpose, Engine as _};
use datafusion_common::ScalarValue;
use datafusion_common::{
    cast::{as_generic_binary_array, as_generic_string_array},
    internal_err, not_impl_err, plan_err,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;
use std::{fmt, str::FromStr};

#[derive(Debug, Copy, Clone)]
enum Encoding {
    Base64,
    Hex,
}
fn encode_process(value: &ColumnarValue, encoding: Encoding) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => encoding.encode_utf8_array::<i32>(a.as_ref()),
            DataType::LargeUtf8 => encoding.encode_utf8_array::<i64>(a.as_ref()),
            DataType::Binary => encoding.encode_binary_array::<i32>(a.as_ref()),
            DataType::LargeBinary => encoding.encode_binary_array::<i64>(a.as_ref()),
            other => internal_err!(
                "Unsupported data type {other:?} for function encode({encoding})"
            ),
        },
        ColumnarValue::Scalar(scalar) => {
            match scalar {
                ScalarValue::Utf8(a) => {
                    Ok(encoding.encode_scalar(a.as_ref().map(|s: &String| s.as_bytes())))
                }
                ScalarValue::LargeUtf8(a) => Ok(encoding
                    .encode_large_scalar(a.as_ref().map(|s: &String| s.as_bytes()))),
                ScalarValue::Binary(a) => Ok(
                    encoding.encode_scalar(a.as_ref().map(|v: &Vec<u8>| v.as_slice()))
                ),
                ScalarValue::LargeBinary(a) => Ok(encoding
                    .encode_large_scalar(a.as_ref().map(|v: &Vec<u8>| v.as_slice()))),
                other => internal_err!(
                    "Unsupported data type {other:?} for function encode({encoding})"
                ),
            }
        }
    }
}

fn decode_process(value: &ColumnarValue, encoding: Encoding) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => encoding.decode_utf8_array::<i32>(a.as_ref()),
            DataType::LargeUtf8 => encoding.decode_utf8_array::<i64>(a.as_ref()),
            DataType::Binary => encoding.decode_binary_array::<i32>(a.as_ref()),
            DataType::LargeBinary => encoding.decode_binary_array::<i64>(a.as_ref()),
            other => internal_err!(
                "Unsupported data type {other:?} for function decode({encoding})"
            ),
        },
        ColumnarValue::Scalar(scalar) => {
            match scalar {
                ScalarValue::Utf8(a) => {
                    encoding.decode_scalar(a.as_ref().map(|s: &String| s.as_bytes()))
                }
                ScalarValue::LargeUtf8(a) => encoding
                    .decode_large_scalar(a.as_ref().map(|s: &String| s.as_bytes())),
                ScalarValue::Binary(a) => {
                    encoding.decode_scalar(a.as_ref().map(|v: &Vec<u8>| v.as_slice()))
                }
                ScalarValue::LargeBinary(a) => encoding
                    .decode_large_scalar(a.as_ref().map(|v: &Vec<u8>| v.as_slice())),
                other => internal_err!(
                    "Unsupported data type {other:?} for function decode({encoding})"
                ),
            }
        }
    }
}

fn hex_encode(input: &[u8]) -> String {
    hex::encode(input)
}

fn base64_encode(input: &[u8]) -> String {
    general_purpose::STANDARD_NO_PAD.encode(input)
}

fn hex_decode(input: &[u8]) -> Result<Vec<u8>> {
    hex::decode(input).map_err(|e| {
        DataFusionError::Internal(format!("Failed to decode from hex: {}", e))
    })
}

fn base64_decode(input: &[u8]) -> Result<Vec<u8>> {
    general_purpose::STANDARD_NO_PAD.decode(input).map_err(|e| {
        DataFusionError::Internal(format!("Failed to decode from base64: {}", e))
    })
}

macro_rules! encode_to_array {
    ($METHOD: ident, $INPUT:expr) => {{
        let utf8_array: StringArray = $INPUT
            .iter()
            .map(|x| x.map(|x| $METHOD(x.as_ref())))
            .collect();
        Arc::new(utf8_array)
    }};
}

macro_rules! decode_to_array {
    ($METHOD: ident, $INPUT:expr) => {{
        let binary_array: BinaryArray = $INPUT
            .iter()
            .map(|x| x.map(|x| $METHOD(x.as_ref())).transpose())
            .collect::<Result<_>>()?;
        Arc::new(binary_array)
    }};
}

impl Encoding {
    fn encode_scalar(self, value: Option<&[u8]>) -> ColumnarValue {
        ColumnarValue::Scalar(match self {
            Self::Base64 => ScalarValue::Utf8(
                value.map(|v| general_purpose::STANDARD_NO_PAD.encode(v)),
            ),
            Self::Hex => ScalarValue::Utf8(value.map(hex::encode)),
        })
    }

    fn encode_large_scalar(self, value: Option<&[u8]>) -> ColumnarValue {
        ColumnarValue::Scalar(match self {
            Self::Base64 => ScalarValue::LargeUtf8(
                value.map(|v| general_purpose::STANDARD_NO_PAD.encode(v)),
            ),
            Self::Hex => ScalarValue::LargeUtf8(value.map(hex::encode)),
        })
    }

    fn encode_binary_array<T>(self, value: &dyn Array) -> Result<ColumnarValue>
    where
        T: OffsetSizeTrait,
    {
        let input_value = as_generic_binary_array::<T>(value)?;
        let array: ArrayRef = match self {
            Self::Base64 => encode_to_array!(base64_encode, input_value),
            Self::Hex => encode_to_array!(hex_encode, input_value),
        };
        Ok(ColumnarValue::Array(array))
    }

    fn encode_utf8_array<T>(self, value: &dyn Array) -> Result<ColumnarValue>
    where
        T: OffsetSizeTrait,
    {
        let input_value = as_generic_string_array::<T>(value)?;
        let array: ArrayRef = match self {
            Self::Base64 => encode_to_array!(base64_encode, input_value),
            Self::Hex => encode_to_array!(hex_encode, input_value),
        };
        Ok(ColumnarValue::Array(array))
    }

    fn decode_scalar(self, value: Option<&[u8]>) -> Result<ColumnarValue> {
        let value = match value {
            Some(value) => value,
            None => return Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
        };

        let out = match self {
            Self::Base64 => {
                general_purpose::STANDARD_NO_PAD
                    .decode(value)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to decode value using base64: {}",
                            e
                        ))
                    })?
            }
            Self::Hex => hex::decode(value).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Failed to decode value using hex: {}",
                    e
                ))
            })?,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(out))))
    }

    fn decode_large_scalar(self, value: Option<&[u8]>) -> Result<ColumnarValue> {
        let value = match value {
            Some(value) => value,
            None => return Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(None))),
        };

        let out = match self {
            Self::Base64 => {
                general_purpose::STANDARD_NO_PAD
                    .decode(value)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to decode value using base64: {}",
                            e
                        ))
                    })?
            }
            Self::Hex => hex::decode(value).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Failed to decode value using hex: {}",
                    e
                ))
            })?,
        };

        Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(out))))
    }

    fn decode_binary_array<T>(self, value: &dyn Array) -> Result<ColumnarValue>
    where
        T: OffsetSizeTrait,
    {
        let input_value = as_generic_binary_array::<T>(value)?;
        let array: ArrayRef = match self {
            Self::Base64 => decode_to_array!(base64_decode, input_value),
            Self::Hex => decode_to_array!(hex_decode, input_value),
        };
        Ok(ColumnarValue::Array(array))
    }

    fn decode_utf8_array<T>(self, value: &dyn Array) -> Result<ColumnarValue>
    where
        T: OffsetSizeTrait,
    {
        let input_value = as_generic_string_array::<T>(value)?;
        let array: ArrayRef = match self {
            Self::Base64 => decode_to_array!(base64_decode, input_value),
            Self::Hex => decode_to_array!(hex_decode, input_value),
        };
        Ok(ColumnarValue::Array(array))
    }
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

impl FromStr for Encoding {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<Encoding> {
        Ok(match name {
            "base64" => Self::Base64,
            "hex" => Self::Hex,
            _ => {
                let options = [Self::Base64, Self::Hex]
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return plan_err!(
                    "There is no built-in encoding named '{name}', currently supported encodings are: {options}"
                );
            }
        })
    }
}

/// Encodes the given data, accepts Binary, LargeBinary, Utf8 or LargeUtf8 and returns a [`ColumnarValue`].
/// Second argument is the encoding to use.
/// Standard encodings are base64 and hex.
pub fn encode(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return internal_err!(
            "{:?} args were supplied but encode takes exactly two arguments",
            args.len()
        );
    }
    let encoding = match &args[1] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(Some(method)) | ScalarValue::LargeUtf8(Some(method)) => {
                method.parse::<Encoding>()
            }
            _ => not_impl_err!(
                "Second argument to encode must be a constant: Encode using dynamically decided method is not yet supported"
            ),
        },
        ColumnarValue::Array(_) => not_impl_err!(
            "Second argument to encode must be a constant: Encode using dynamically decided method is not yet supported"
        ),
    }?;
    encode_process(&args[0], encoding)
}

/// Decodes the given data, accepts Binary, LargeBinary, Utf8 or LargeUtf8 and returns a [`ColumnarValue`].
/// Second argument is the encoding to use.
/// Standard encodings are base64 and hex.
pub fn decode(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return internal_err!(
            "{:?} args were supplied but decode takes exactly two arguments",
            args.len()
        );
    }
    let encoding = match &args[1] {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(Some(method)) | ScalarValue::LargeUtf8(Some(method)) => {
                method.parse::<Encoding>()
            }
            _ => not_impl_err!(
                "Second argument to decode must be a utf8 constant: Decode using dynamically decided method is not yet supported"
            ),
        },
        ColumnarValue::Array(_) => not_impl_err!(
            "Second argument to decode must be a utf8 constant: Decode using dynamically decided method is not yet supported"
        ),
    }?;
    decode_process(&args[0], encoding)
}
