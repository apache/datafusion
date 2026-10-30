// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, GenericBinaryBuilder, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::{Field, FieldRef};
use datafusion_common::cast::{
    as_binary_view_array, as_fixed_size_binary_array, as_generic_binary_array,
};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{Coercion, Expr, ReturnFieldArgs, TypeSignatureClass, lit};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::expr_fn::encode;
use datafusion_functions::utils::make_scalar_function;

/// Apache Spark base64 uses padded base64 encoding.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#base64>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBase64 {
    signature: Signature,
}

impl Default for SparkBase64 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBase64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Binary,
                    vec![TypeSignatureClass::Native(logical_string())],
                    NativeType::Binary,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkBase64 {
    fn name(&self) -> &str {
        "base64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type should not be called for {}", self.name())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [bin] = take_function_args(self.name(), args.arg_fields)?;
        let return_type = match bin.data_type() {
            DataType::LargeBinary => DataType::LargeUtf8,
            _ => DataType::Utf8,
        };
        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            bin.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!(
            "invoke should not be called on a simplified {} function",
            self.name()
        )
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [bin] = take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(encode(
            bin,
            lit("base64pad"),
        )))
    }
}

/// <https://spark.apache.org/docs/latest/api/sql/index.html#unbase64>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnBase64 {
    signature: Signature,
}

impl Default for SparkUnBase64 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUnBase64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Binary,
                    vec![TypeSignatureClass::Native(logical_string())],
                    NativeType::Binary,
                )],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkUnBase64 {
    fn name(&self) -> &str {
        "unbase64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_type should not be called for {}", self.name())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [str] = take_function_args(self.name(), args.arg_fields)?;
        let return_type = match str.data_type() {
            DataType::LargeBinary => DataType::LargeBinary,
            _ => DataType::Binary,
        };
        Ok(Arc::new(Field::new(
            self.name(),
            return_type,
            str.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_unbase64, vec![])(&args.args)
    }
}

fn spark_unbase64(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("unbase64", args)?;
    match array.data_type() {
        DataType::Binary => {
            let array = as_generic_binary_array::<i32>(array)?;
            unbase64_array::<i32>(array.iter(), array.len())
        }
        DataType::LargeBinary => {
            let array = as_generic_binary_array::<i64>(array)?;
            unbase64_array::<i64>(array.iter(), array.len())
        }
        DataType::BinaryView => {
            let array = as_binary_view_array(array)?;
            unbase64_array::<i32>(array.iter(), array.len())
        }
        DataType::FixedSizeBinary(_) => {
            let array = as_fixed_size_binary_array(array)?;
            unbase64_array::<i32>(array.iter(), array.len())
        }
        other => exec_err!("unbase64 does not support type {other}"),
    }
}

fn unbase64_array<'a, O: OffsetSizeTrait>(
    iter: impl Iterator<Item = Option<&'a [u8]>>,
    len: usize,
) -> Result<ArrayRef> {
    let mut builder = GenericBinaryBuilder::<O>::with_capacity(len, 0);
    let mut buffer = Vec::new();
    for value in iter {
        match value {
            Some(bytes) => {
                buffer.clear();
                if let Err(msg) = decode_mime(bytes, &mut buffer) {
                    return exec_err!("Failed to decode value using base64: {msg}");
                }
                builder.append_value(&buffer);
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

const INVALID: u8 = 0xFF;
const PAD: u8 = 0xFE;

const DECODE_TABLE: [u8; 256] = {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut table = [INVALID; 256];
    let mut i = 0;
    while i < alphabet.len() {
        table[alphabet[i] as usize] = i as u8;
        i += 1;
    }
    table[b'=' as usize] = PAD;
    table
};

/// Decodes `input` the same way as `java.util.Base64.getMimeDecoder()`,
/// which Spark uses for `unbase64`.
///
/// Padding is optional and bytes outside the base64 alphabet are skipped.
/// A dangling single character, malformed padding, or base64 characters
/// after the padding are rejected.
fn decode_mime(input: &[u8], out: &mut Vec<u8>) -> Result<(), &'static str> {
    let mut bits: u32 = 0;
    let mut shift: i32 = 18;
    let mut pos = 0;
    while pos < input.len() {
        let value = DECODE_TABLE[input[pos] as usize];
        pos += 1;
        match value {
            INVALID => {}
            PAD => {
                // "=" after a complete unit is invalid and "xx=" needs a second "="
                let bad_ending = match shift {
                    18 => true,
                    6 => {
                        let next_is_pad = input.get(pos) == Some(&b'=');
                        pos += 1;
                        !next_is_pad
                    }
                    _ => false,
                };
                if bad_ending {
                    return Err("Input byte array has wrong 4-byte ending unit");
                }
                break;
            }
            _ => {
                bits |= (value as u32) << shift;
                shift -= 6;
                if shift < 0 {
                    out.extend_from_slice(&[
                        (bits >> 16) as u8,
                        (bits >> 8) as u8,
                        bits as u8,
                    ]);
                    bits = 0;
                    shift = 18;
                }
            }
        }
    }
    match shift {
        6 => out.push((bits >> 16) as u8),
        0 => out.extend_from_slice(&[(bits >> 16) as u8, (bits >> 8) as u8]),
        12 => return Err("Last unit does not have enough valid bits"),
        _ => {}
    }
    // after the padding only non-alphabet bytes (including "=") are allowed
    let rest = input.get(pos..).unwrap_or_default();
    if rest.iter().any(|b| DECODE_TABLE[*b as usize] < 64) {
        return Err("Input byte array has incorrect ending byte");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(input: &str) -> Result<Vec<u8>, &'static str> {
        let mut out = Vec::new();
        decode_mime(input.as_bytes(), &mut out).map(|_| out)
    }

    #[test]
    fn test_decode_mime_matches_java_mime_decoder() {
        assert_eq!(decode("U3BhcmsgU1FM").unwrap(), b"Spark SQL");
        assert_eq!(decode("U3BhcmsgU1E=").unwrap(), b"Spark SQ");
        assert_eq!(decode("U3BhcmsgUw==").unwrap(), b"Spark S");
        assert_eq!(decode("U3BhcmsgUw").unwrap(), b"Spark S");
        assert_eq!(decode("123").unwrap(), [0xD7, 0x6D]);
        assert_eq!(decode("12").unwrap(), [0xD7]);
        assert_eq!(decode("YW Jj").unwrap(), b"abc");
        assert_eq!(decode("YWJj\r\nZGVm").unwrap(), b"abcdef");
        assert_eq!(decode("YWJj!!").unwrap(), b"abc");
        assert_eq!(decode("YQ==\r\n").unwrap(), b"a");
        assert_eq!(decode("YQ===").unwrap(), b"a");
        assert_eq!(decode("Y\u{e9}Q").unwrap(), b"a");
        assert_eq!(decode("!!!!").unwrap(), b"");
        assert_eq!(decode("").unwrap(), b"");

        assert_eq!(
            decode("Y").unwrap_err(),
            "Last unit does not have enough valid bits"
        );
        assert_eq!(
            decode("YWJjZ").unwrap_err(),
            "Last unit does not have enough valid bits"
        );
        assert_eq!(
            decode("Y=").unwrap_err(),
            "Last unit does not have enough valid bits"
        );
        for input in ["=", "YWJj=", "YQ=", "YQ=!"] {
            assert_eq!(
                decode(input).unwrap_err(),
                "Input byte array has wrong 4-byte ending unit",
                "{input}"
            );
        }
        assert_eq!(
            decode("YQ==YQ==").unwrap_err(),
            "Input byte array has incorrect ending byte"
        );
    }
}
