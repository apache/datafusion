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
    array::{
        Array, ArrayRef, AsArray, BinaryArrayType, GenericBinaryArray,
        GenericStringArray, OffsetSizeTrait,
    },
    datatypes::DataType,
};
use arrow_buffer::{Buffer, OffsetBufferBuilder};
use base64::{
    Engine as _,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, internal_err,
    not_impl_err, plan_err,
    types::{NativeType, logical_string},
    utils::take_function_args,
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

// Allow padding characters, but don't require them, and don't generate them.
const BASE64_ENGINE: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

#[user_doc(
    doc_section(label = "Binary String Functions"),
    description = "Encode binary data into a textual representation.",
    syntax_example = "encode(expression, format)",
    argument(
        name = "expression",
        description = "Expression containing string or binary data"
    ),
    argument(
        name = "format",
        description = "Supported formats are: `base64`, `hex`"
    ),
    related_udf(name = "decode")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EncodeFunc {
    signature: Signature,
}

impl Default for EncodeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl EncodeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Binary,
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::Binary,
                    ),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncodeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::LargeBinary => Ok(DataType::LargeUtf8),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [expression, encoding] = take_function_args("encode", &args.args)?;
        let encoding = Encoding::try_from(encoding)?;
        match expression {
            _ if expression.data_type().is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(array) => encode_array(array, encoding),
            ColumnarValue::Scalar(scalar) => encode_scalar(scalar, encoding),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[user_doc(
    doc_section(label = "Binary String Functions"),
    description = "Decode binary data from textual representation in string.",
    syntax_example = "decode(expression, format)",
    argument(
        name = "expression",
        description = "Expression containing encoded string data"
    ),
    argument(name = "format", description = "Same arguments as [encode](#encode)"),
    related_udf(name = "encode")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecodeFunc {
    signature: Signature,
}

impl Default for DecodeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Binary,
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::Binary,
                    ),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DecodeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "decode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::LargeBinary => Ok(DataType::LargeBinary),
            _ => Ok(DataType::Binary),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [expression, encoding] = take_function_args("decode", &args.args)?;
        let encoding = Encoding::try_from(encoding)?;
        match expression {
            _ if expression.data_type().is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
            ColumnarValue::Array(array) => decode_array(array, encoding),
            ColumnarValue::Scalar(scalar) => decode_scalar(scalar, encoding),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn encode_scalar(value: &ScalarValue, encoding: Encoding) -> Result<ColumnarValue> {
    match value {
        ScalarValue::Binary(maybe_bytes)
        | ScalarValue::BinaryView(maybe_bytes)
        | ScalarValue::FixedSizeBinary(_, maybe_bytes) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                maybe_bytes
                    .as_ref()
                    .map(|bytes| encoding.encode_bytes(bytes)),
            )))
        }
        ScalarValue::LargeBinary(maybe_bytes) => {
            Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(
                maybe_bytes
                    .as_ref()
                    .map(|bytes| encoding.encode_bytes(bytes)),
            )))
        }
        v => internal_err!("Unexpected value for encode: {v}"),
    }
}

fn encode_array(array: &ArrayRef, encoding: Encoding) -> Result<ColumnarValue> {
    let array = match array.data_type() {
        DataType::Binary => encoding.encode_array::<_, i32>(&array.as_binary::<i32>()),
        DataType::BinaryView => encoding.encode_array::<_, i32>(&array.as_binary_view()),
        DataType::LargeBinary => {
            encoding.encode_array::<_, i64>(&array.as_binary::<i64>())
        }
        DataType::FixedSizeBinary(_) => {
            encoding.encode_array::<_, i32>(&array.as_fixed_size_binary())
        }
        dt => {
            internal_err!("Unexpected data type for encode: {dt}")
        }
    };
    array.map(ColumnarValue::Array)
}

fn decode_scalar(value: &ScalarValue, encoding: Encoding) -> Result<ColumnarValue> {
    match value {
        ScalarValue::Binary(maybe_bytes)
        | ScalarValue::BinaryView(maybe_bytes)
        | ScalarValue::FixedSizeBinary(_, maybe_bytes) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(
                maybe_bytes
                    .as_ref()
                    .map(|x| encoding.decode_bytes(x))
                    .transpose()?,
            )))
        }
        ScalarValue::LargeBinary(maybe_bytes) => {
            Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(
                maybe_bytes
                    .as_ref()
                    .map(|x| encoding.decode_bytes(x))
                    .transpose()?,
            )))
        }
        v => internal_err!("Unexpected value for decode: {v}"),
    }
}

/// Estimate how many bytes are actually represented by the array; in case the
/// the array slices it's internal buffer, this returns the byte size of that slice
/// but not the byte size of the entire buffer.
///
/// This is an estimation only as it can estimate higher if null slots are non-zero
/// sized.
fn estimate_byte_data_size<O: OffsetSizeTrait>(array: &GenericBinaryArray<O>) -> usize {
    let offsets = array.value_offsets();
    // Unwraps are safe as should always have 1 element in offset buffer
    let start = *offsets.first().unwrap();
    let end = *offsets.last().unwrap();
    let data_size = end - start;
    data_size.as_usize()
}

fn decode_array(array: &ArrayRef, encoding: Encoding) -> Result<ColumnarValue> {
    let array = match array.data_type() {
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            encoding.decode_array::<_, i32>(&array, estimate_byte_data_size(array))
        }
        DataType::BinaryView => {
            let array = array.as_binary_view();
            // Don't know if there is a more strict upper bound we can infer
            // for view arrays byte data size.
            encoding.decode_array::<_, i32>(&array, array.get_buffer_memory_size())
        }
        DataType::LargeBinary => {
            let array = array.as_binary::<i64>();
            encoding.decode_array::<_, i64>(&array, estimate_byte_data_size(array))
        }
        DataType::FixedSizeBinary(size) => {
            let array = array.as_fixed_size_binary();
            // TODO: could we be more conservative by accounting for nulls?
            let estimate = array.len().saturating_mul(*size as usize);
            encoding.decode_array::<_, i32>(&array, estimate)
        }
        dt => {
            internal_err!("Unexpected data type for decode: {dt}")
        }
    };
    array.map(ColumnarValue::Array)
}

#[derive(Debug, Copy, Clone)]
enum Encoding {
    Base64,
    Hex,
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

impl TryFrom<&ColumnarValue> for Encoding {
    type Error = DataFusionError;

    fn try_from(encoding: &ColumnarValue) -> Result<Self> {
        let encoding = match encoding {
            ColumnarValue::Scalar(encoding) => match encoding.try_as_str().flatten() {
                Some(encoding) => encoding,
                _ => return exec_err!("Encoding must be a non-null string"),
            },
            ColumnarValue::Array(_) => {
                return not_impl_err!(
                    "Encoding must be a scalar; array specified encoding is not yet supported"
                );
            }
        };
        match encoding {
            "base64" => Ok(Self::Base64),
            "hex" => Ok(Self::Hex),
            _ => {
                let options = [Self::Base64, Self::Hex]
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                plan_err!(
                    "There is no built-in encoding named '{encoding}', currently supported encodings are: {options}"
                )
            }
        }
    }
}

impl Encoding {
    fn encode_bytes(self, value: &[u8]) -> String {
        match self {
            Self::Base64 => BASE64_ENGINE.encode(value),
            Self::Hex => hex::encode(value),
        }
    }

    fn decode_bytes(self, value: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::Base64 => BASE64_ENGINE.decode(value).map_err(|e| {
                exec_datafusion_err!("Failed to decode value using base64: {e}")
            }),
            Self::Hex => hex::decode(value).map_err(|e| {
                exec_datafusion_err!("Failed to decode value using hex: {e}")
            }),
        }
    }

    // OutputOffset important to ensure Large types output Large arrays
    fn encode_array<'a, InputBinaryArray, OutputOffset>(
        self,
        array: &InputBinaryArray,
    ) -> Result<ArrayRef>
    where
        InputBinaryArray: BinaryArrayType<'a>,
        OutputOffset: OffsetSizeTrait,
    {
        match self {
            Self::Base64 => {
                let array: GenericStringArray<OutputOffset> = array
                    .iter()
                    .map(|x| x.map(|x| BASE64_ENGINE.encode(x)))
                    .collect();
                Ok(Arc::new(array))
            }
            Self::Hex => {
                let array: GenericStringArray<OutputOffset> =
                    array.iter().map(|x| x.map(hex::encode)).collect();
                Ok(Arc::new(array))
            }
        }
    }

    // OutputOffset important to ensure Large types output Large arrays
    fn decode_array<'a, InputBinaryArray, OutputOffset>(
        self,
        value: &InputBinaryArray,
        approx_data_size: usize,
    ) -> Result<ArrayRef>
    where
        InputBinaryArray: BinaryArrayType<'a>,
        OutputOffset: OffsetSizeTrait,
    {
        fn hex_decode(input: &[u8], buf: &mut [u8]) -> Result<usize> {
            // only write input / 2 bytes to buf
            let out_len = input.len() / 2;
            let buf = &mut buf[..out_len];
            hex::decode_to_slice(input, buf)
                .map_err(|e| exec_datafusion_err!("Failed to decode from hex: {e}"))?;
            Ok(out_len)
        }

        fn base64_decode(input: &[u8], buf: &mut [u8]) -> Result<usize> {
            BASE64_ENGINE
                .decode_slice(input, buf)
                .map_err(|e| exec_datafusion_err!("Failed to decode from base64: {e}"))
        }

        match self {
            Self::Base64 => {
                let upper_bound = base64::decoded_len_estimate(approx_data_size);
                delegated_decode::<_, _, OutputOffset>(base64_decode, value, upper_bound)
            }
            Self::Hex => {
                // Calculate the upper bound for decoded byte size
                // For hex encoding, each pair of hex characters (2 bytes) represents 1 byte when decoded
                // So the upper bound is half the length of the input values.
                let upper_bound = approx_data_size / 2;
                delegated_decode::<_, _, OutputOffset>(hex_decode, value, upper_bound)
            }
        }
    }
}

fn delegated_decode<'a, DecodeFunction, InputBinaryArray, OutputOffset>(
    decode: DecodeFunction,
    input: &InputBinaryArray,
    conservative_upper_bound_size: usize,
) -> Result<ArrayRef>
where
    DecodeFunction: Fn(&[u8], &mut [u8]) -> Result<usize>,
    InputBinaryArray: BinaryArrayType<'a>,
    OutputOffset: OffsetSizeTrait,
{
    let mut values = vec![0; conservative_upper_bound_size];
    let mut offsets = OffsetBufferBuilder::new(input.len());
    let mut total_bytes_decoded = 0;
    for v in input.iter() {
        if let Some(v) = v {
            let cursor = &mut values[total_bytes_decoded..];
            let decoded = decode(v, cursor)?;
            total_bytes_decoded += decoded;
            offsets.push_length(decoded);
        } else {
            offsets.push_length(0);
        }
    }
    // We reserved an upper bound size for the values buffer, but we only use the actual size
    values.truncate(total_bytes_decoded);
    let binary_array = GenericBinaryArray::<OutputOffset>::try_new(
        offsets.finish(),
        Buffer::from_vec(values),
        input.nulls().cloned(),
    )?;
    Ok(Arc::new(binary_array))
}

#[cfg(test)]
mod tests {
    use arrow::array::BinaryArray;
    use arrow_buffer::OffsetBuffer;

    use super::*;

    #[test]
    fn test_estimate_byte_data_size() {
        // Offsets starting at 0, but don't count entire data buffer size
        let array = BinaryArray::new(
            OffsetBuffer::new(vec![0, 5, 10, 15].into()),
            vec![0; 100].into(),
            None,
        );
        let size = estimate_byte_data_size(&array);
        assert_eq!(size, 15);

        // Offsets starting at 0, but don't count entire data buffer size
        let array = BinaryArray::new(
            OffsetBuffer::new(vec![50, 51, 51, 60, 80, 81].into()),
            vec![0; 100].into(),
            Some(vec![true, false, false, true, true].into()),
        );
        let size = estimate_byte_data_size(&array);
        assert_eq!(size, 31);
    }
}
