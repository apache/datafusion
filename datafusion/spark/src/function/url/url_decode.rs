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

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_large_string_array, as_string_array, as_string_view_array,
};
use datafusion_common::{exec_datafusion_err, exec_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use percent_encoding::percent_decode;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UrlDecode {
    signature: Signature,
}

impl Default for UrlDecode {
    fn default() -> Self {
        Self::new()
    }
}

impl UrlDecode {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }

    /// Decodes a URL-encoded string from application/x-www-form-urlencoded format.
    ///
    /// # Arguments
    ///
    /// * `value` - The URL-encoded string to decode
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - The decoded string
    /// * `Err(DataFusionError)` - If the input is malformed or contains invalid UTF-8
    ///
    fn decode(value: &str) -> Result<String> {
        // Check if the string has valid percent encoding
        // TODO: Support `try_url_decode`
        Self::validate_percent_encoding(value)?;

        let replaced = Self::replace_plus(value.as_bytes());
        percent_decode(&replaced)
            .decode_utf8()
            .map_err(|e| exec_datafusion_err!("Invalid UTF-8 sequence: {e}"))
            .map(|parsed| parsed.into_owned())
    }

    /// Replace b'+' with b' '
    fn replace_plus(input: &[u8]) -> Cow<'_, [u8]> {
        match input.iter().position(|&b| b == b'+') {
            None => Cow::Borrowed(input),
            Some(first_position) => {
                let mut replaced = input.to_owned();
                replaced[first_position] = b' ';
                for byte in &mut replaced[first_position + 1..] {
                    if *byte == b'+' {
                        *byte = b' ';
                    }
                }
                Cow::Owned(replaced)
            }
        }
    }

    /// Validate percent-encoding of the string
    fn validate_percent_encoding(value: &str) -> Result<()> {
        let bytes = value.as_bytes();
        let mut i = 0;

        while i < bytes.len() {
            if bytes[i] == b'%' {
                // Check if we have at least 2 more characters
                if i + 2 >= bytes.len() {
                    return exec_err!(
                        "Invalid percent-encoding: incomplete sequence at position {}",
                        i
                    );
                }

                let hex1 = bytes[i + 1];
                let hex2 = bytes[i + 2];

                if !hex1.is_ascii_hexdigit() || !hex2.is_ascii_hexdigit() {
                    return exec_err!(
                        "Invalid percent-encoding: invalid hex sequence '%{}{}' at position {}",
                        hex1 as char,
                        hex2 as char,
                        i
                    );
                }
                i += 3;
            } else {
                i += 1;
            }
        }
        Ok(())
    }
}

impl ScalarUDFImpl for UrlDecode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "url_decode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!(
                "{} expects 1 argument, but got {}",
                self.name(),
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Utf8 | DataType::Utf8View => Ok(DataType::Utf8),
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => plan_err!("1st argument should be STRING, got {}", arg_types[0]),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_url_decode, vec![])(&args)
    }
}

/// Core implementation of URL decoding function.
///
/// # Arguments
///
/// * `args` - A slice containing exactly one ArrayRef with the URL-encoded strings to decode
///
/// # Returns
///
/// * `Ok(ArrayRef)` - A new array of the same type containing decoded strings
/// * `Err(DataFusionError)` - If validation fails or invalid arguments are provided
///
fn spark_url_decode(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("`url_decode` expects 1 argument");
    }

    match &args[0].data_type() {
        DataType::Utf8 => as_string_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlDecode::decode).transpose())
            .collect::<Result<StringArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        DataType::LargeUtf8 => as_large_string_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlDecode::decode).transpose())
            .collect::<Result<LargeStringArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        DataType::Utf8View => as_string_view_array(&args[0])?
            .iter()
            .map(|x| x.map(UrlDecode::decode).transpose())
            .collect::<Result<StringViewArray>>()
            .map(|array| Arc::new(array) as ArrayRef),
        other => exec_err!("`url_decode`: Expr must be STRING, got {other:?}"),
    }
}
