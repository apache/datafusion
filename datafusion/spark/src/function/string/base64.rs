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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, LargeBinaryArray};
use arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::{Field, FieldRef};
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
    let [bin] = take_function_args("unbase64", args)?;
    fn decode_all<'a, T>(
        values: impl Iterator<Item = Option<&'a [u8]>>,
    ) -> Result<ArrayRef>
    where
        T: Array + FromIterator<Option<Vec<u8>>> + 'static,
    {
        let decoded = values
            .map(|value| value.map(mime_decode).transpose())
            .collect::<Result<T>>()?;
        Ok(Arc::new(decoded))
    }
    match bin.data_type() {
        DataType::Binary => decode_all::<BinaryArray>(bin.as_binary::<i32>().iter()),
        DataType::LargeBinary => {
            decode_all::<LargeBinaryArray>(bin.as_binary::<i64>().iter())
        }
        DataType::BinaryView => decode_all::<BinaryArray>(bin.as_binary_view().iter()),
        other => exec_err!("unbase64 expects a binary argument, got {other}"),
    }
}

/// Decodes `input` the way Spark's `unbase64` does, with Java's MIME Base64
/// decoder: characters outside the Base64 alphabet are skipped and padding is
/// optional, but a final unit of one character, padding that does not
/// complete a unit, and alphabet characters after the padding are errors.
fn mime_decode(input: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() / 4 * 3 + 2);
    let mut unit = 0u32;
    let mut unit_len = 0;
    let mut rest = input.iter();
    while let Some(&byte) = rest.next() {
        if byte == b'=' {
            let completes_unit = match unit_len {
                2 => rest.next() == Some(&b'='),
                3 => true,
                _ => false,
            };
            if !completes_unit || rest.any(|&byte| base64_value(byte).is_some()) {
                return exec_err!("Failed to decode value using base64: invalid padding");
            }
            break;
        }
        let Some(value) = base64_value(byte) else {
            continue;
        };
        unit = (unit << 6) | u32::from(value);
        unit_len += 1;
        if unit_len == 4 {
            out.extend_from_slice(&unit.to_be_bytes()[1..]);
            unit = 0;
            unit_len = 0;
        }
    }
    match unit_len {
        1 => {
            return exec_err!(
                "Failed to decode value using base64: last unit does not have enough valid bits"
            );
        }
        2 => out.push((unit >> 4) as u8),
        3 => out.extend_from_slice(&((unit >> 2) as u16).to_be_bytes()),
        _ => {}
    }
    Ok(out)
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(input: &str) -> Result<Vec<u8>> {
        mime_decode(input.as_bytes())
    }

    #[test]
    fn test_mime_decode_matches_java_mime_decoder() -> Result<()> {
        // Expected values from java.util.Base64.getMimeDecoder()
        assert_eq!(decode("U3BhcmsgU1FM")?, b"Spark SQL");
        assert_eq!(decode("U3BhcmsgUw==")?, b"Spark S");
        assert_eq!(decode("123")?, [0xd7, 0x6d]);
        assert_eq!(decode("12")?, [0xd7]);
        assert_eq!(decode("YQ")?, b"a");
        assert_eq!(decode("YW Jj")?, b"abc");
        assert_eq!(decode("YW\nJj")?, b"abc");
        assert_eq!(decode("YWJj!!")?, b"abc");
        assert_eq!(decode("YQ!==")?, b"a");
        assert_eq!(decode("YQ===")?, b"a");
        assert_eq!(decode("YQ== ")?, b"a");
        assert_eq!(decode("YWJ=")?, b"ab");
        assert_eq!(decode("YWJ= =")?, b"ab");
        assert_eq!(decode("+/8=")?, [0xfb, 0xff]);
        assert_eq!(decode("!!!!")?, b"");
        assert_eq!(decode("")?, b"");
        Ok(())
    }

    #[test]
    fn test_mime_decode_errors_match_java_mime_decoder() {
        for input in [
            "Y", "YWJjZ", "Y===", "Y=Q", "YQ=", "YQ=x", "YQ= =", "YQ==x", "YWJj=", "=YQ",
            "YQ==YQ==", "-_8",
        ] {
            assert!(decode(input).is_err(), "{input:?} should not decode");
        }
    }
}
