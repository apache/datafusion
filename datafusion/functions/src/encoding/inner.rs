// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Encoding expressions

use crate::utils::transform_leaf_type_preserving_encoding;
use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArrayType, GenericBinaryArray,
        GenericStringArray, OffsetSizeTrait,
    },
    datatypes::DataType,
};
use arrow_buffer::{Buffer, OffsetBuffer};
use base64::{
    Engine as _,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, internal_err,
    not_impl_err, plan_err,
    types::{NativeType, logical_string},
    utils::{
        hex::{HexCase, encode_bytes, encode_bytes_to_slice},
        map_dictionary_values, offset_span_len, take_function_args,
    },
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, EncodingPreservation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use std::fmt;
use std::sync::Arc;

// Allow padding characters, but don't require them, and don't generate them.
const BASE64_ENGINE: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

// Generate padding characters when encoding
const BASE64_ENGINE_PADDED: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new().with_encode_padding(true),
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
        description = "Supported formats are: `base64`, `base64pad`, `hex`"
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
                    )
                    .with_encoding_preservation(EncodingPreservation::dictionary()),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncodeFunc {
    fn name(&self) -> &str {
        "encode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        transform_leaf_type_preserving_encoding(&arg_types[0], &|data_type| {
            Ok(match data_type {
                DataType::LargeBinary => DataType::LargeUtf8,
                _ => DataType::Utf8,
            })
        })
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
        ScalarValue::Dictionary(key_type, value) => {
            let ColumnarValue::Scalar(encoded) = encode_scalar(value, encoding)? else {
                unreachable!("encoding a scalar returns a scalar")
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Dictionary(
                key_type.clone(),
                Box::new(encoded),
            )))
        }
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
    if matches!(array.data_type(), DataType::Dictionary(_, _)) {
        let encoded = map_dictionary_values(array, |values| {
            encode_array(values, encoding)?.into_array(values.len())
        })?;
        return Ok(ColumnarValue::Array(encoded));
    }
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

/// Estimates the number of input bytes covered by the array's visible rows.
///
/// Sliced arrays exclude bytes outside their visible offset span. This can
/// overestimate the bytes processed because null rows may cover nonempty byte
/// ranges.
fn estimate_byte_data_size<O: OffsetSizeTrait>(array: &GenericBinaryArray<O>) -> usize {
    offset_span_len(array.offsets())
}

fn decode_array(array: &ArrayRef, encoding: Encoding) -> Result<ColumnarValue> {
    let array = match array.data_type() {
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            encoding.decode_array::<_, i32>(&array, estimate_byte_data_size(array))
        }
        DataType::BinaryView => {
            let array = array.as_binary_view();
            encoding.decode_array::<_, i32>(
                &array,
                array.lengths().map(|l| l as usize).sum::<usize>(),
            )
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
    Base64Padded,
    Hex,
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            Self::Base64 => "base64",
            Self::Base64Padded => "base64pad",
            Self::Hex => "hex",
        };
        write!(f, "{name}")
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
            "base64pad" => Ok(Self::Base64Padded),
            "hex" => Ok(Self::Hex),
            _ => {
                let options = [Self::Base64, Self::Base64Padded, Self::Hex]
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
            Self::Base64Padded => BASE64_ENGINE_PADDED.encode(value),
            Self::Hex => encode_bytes(value, HexCase::Lower),
        }
    }

    fn decode_bytes(self, value: &[u8]) -> Result<Vec<u8>> {
        match self {
            Self::Base64 | Self::Base64Padded => {
                BASE64_ENGINE.decode(value).map_err(|e| {
                    exec_datafusion_err!("Failed to decode value using {self}: {e}")
                })
            }
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
            Self::Base64Padded => {
                let array: GenericStringArray<OutputOffset> = array
                    .iter()
                    .map(|x| x.map(|x| BASE64_ENGINE_PADDED.encode(x)))
                    .collect();
                Ok(Arc::new(array))
            }
            Self::Hex => hex_encode_array::<_, OutputOffset>(array),
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
            Self::Base64 | Self::Base64Padded => {
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

/// Hex-encode a binary array into a string array, writing the lowercase hex
/// digits directly into a single pre-sized value buffer. Each input byte maps
/// to exactly two hex characters, so the output size is known up front and no
/// per-element `String` is allocated.
fn hex_encode_array<'a, InputBinaryArray, OutputOffset>(
    array: &InputBinaryArray,
) -> Result<ArrayRef>
where
    InputBinaryArray: BinaryArrayType<'a>,
    OutputOffset: OffsetSizeTrait,
{
    let total_input_bytes: usize = array.iter().flatten().map(|v| v.len()).sum();

    let mut values = vec![0u8; total_input_bytes * 2];
    let mut offsets = Vec::<OutputOffset>::with_capacity(array.len() + 1);
    offsets.push(OutputOffset::zero());

    let mut pos = 0usize;
    for v in array.iter() {
        if let Some(v) = v {
            let out_len = v.len() * 2;
            encode_bytes_to_slice(v, HexCase::Lower, &mut values[pos..pos + out_len])?;
            pos += out_len;
        }
        offsets.push(OutputOffset::usize_as(pos));
    }

    let array = GenericStringArray::<OutputOffset>::try_new(
        OffsetBuffer::new(offsets.into()),
        Buffer::from_vec(values),
        array.nulls().cloned(),
    )?;
    Ok(Arc::new(array))
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
    let mut offsets = Vec::<OutputOffset>::with_capacity(input.len() + 1);
    offsets.push(OutputOffset::zero());
    let mut total_bytes_decoded = 0;
    for v in input.iter() {
        if let Some(v) = v {
            let cursor = &mut values[total_bytes_decoded..];
            let decoded = decode(v, cursor)?;
            total_bytes_decoded += decoded;
        }
        offsets.push(OutputOffset::usize_as(total_bytes_decoded));
    }
    // We reserved an upper bound size for the values buffer, but we only use the actual size
    values.truncate(total_bytes_decoded);
    let binary_array = GenericBinaryArray::<OutputOffset>::try_new(
        OffsetBuffer::new(offsets.into()),
        Buffer::from_vec(values),
        input.nulls().cloned(),
    )?;
    Ok(Arc::new(binary_array))
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        ArrayBuilder, BinaryArray, BinaryViewBuilder, DictionaryArray, Int8Array,
        StringArray,
    };
    use arrow::datatypes::Int8Type;
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

        // Nonzero starting offset, including bytes covered by null slots
        let array = BinaryArray::new(
            OffsetBuffer::new(vec![50, 51, 51, 60, 80, 81].into()),
            vec![0; 100].into(),
            Some(vec![true, false, false, true, true].into()),
        );
        let size = estimate_byte_data_size(&array);
        assert_eq!(size, 31);
    }

    #[test]
    fn test_estimate_view_size() {
        let mut builder = BinaryViewBuilder::new().with_deduplicate_strings();
        for _ in 0..1000 {
            builder.append_value([65u8; 64]);
        }
        let arr = ArrayBuilder::finish(&mut builder);
        decode_array(&arr, Encoding::Base64).unwrap();
    }

    #[test]
    fn encode_dictionary_values_once() -> Result<()> {
        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(0)]);
        let values: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"abc".as_slice()),
            Some(b"xy".as_slice()),
        ]));
        let input: ArrayRef =
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys.clone(), values)?);

        let ColumnarValue::Array(output) = encode_array(&input, Encoding::Hex)? else {
            unreachable!("encoding an array returns an array")
        };
        let output = output
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        assert_eq!(output.keys(), &keys);
        assert_eq!(
            output.values().as_string::<i32>(),
            &StringArray::from(vec![Some("616263"), Some("7879")])
        );
        Ok(())
    }

    #[test]
    fn encode_dictionary_scalar_preserves_key_type() -> Result<()> {
        let input = ScalarValue::Dictionary(
            Box::new(DataType::Int8),
            Box::new(ScalarValue::Binary(Some(b"abc".to_vec()))),
        );
        let ColumnarValue::Scalar(output) = encode_scalar(&input, Encoding::Hex)? else {
            unreachable!("encoding a scalar returns a scalar")
        };
        assert_eq!(
            output,
            ScalarValue::Dictionary(
                Box::new(DataType::Int8),
                Box::new(ScalarValue::Utf8(Some("616263".to_string())))
            )
        );
        Ok(())
    }

    #[test]
    fn encode_compacts_oversized_dictionary() -> Result<()> {
        let values: ArrayRef = Arc::new(BinaryArray::from_iter_values(
            (0..100).map(|i| format!("{i:08x}")),
        ));
        let input: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![42]),
            values,
        )?);

        let ColumnarValue::Array(output) = encode_array(&input, Encoding::Hex)? else {
            unreachable!("encoding an array returns an array")
        };
        let output = output
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        assert_eq!(output.values().len(), 1);
        assert_eq!(output.keys().value(0), 0);
        Ok(())
    }

    #[test]
    fn encode_dictionary_return_type_preserves_keys() -> Result<()> {
        let input = DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::LargeBinary),
        );
        assert_eq!(
            EncodeFunc::new().return_type(&[input])?,
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8))
        );
        Ok(())
    }
}
