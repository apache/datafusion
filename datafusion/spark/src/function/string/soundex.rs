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

use arrow::array::StringBuilder;
use arrow::array::{Array, ArrayRef, GenericStringBuilder, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::utils::make_scalar_function;
use std::sync::Arc;

/// Spark-compatible `soundex` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#soundex>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSoundex {
    signature: Signature,
}

impl Default for SparkSoundex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSoundex {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSoundex {
    fn name(&self) -> &str {
        "soundex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_soundex_inner, vec![])(&args.args)
    }
}

fn spark_soundex_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("soundex", arg)?;
    match &array.data_type() {
        DataType::Utf8 => soundex_array::<i32>(array),
        DataType::LargeUtf8 => soundex_array::<i64>(array),
        DataType::Utf8View => soundex_view(array),
        other => {
            exec_err!("unsupported data type {other:?} for function `soundex`")
        }
    }
}

fn soundex_array<O: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_generic_string_array::<O>(array)?;

    // Pre-allocate exact memory: row count and total string bytes (each soundex is 4 bytes)
    let mut builder =
        GenericStringBuilder::<O>::with_capacity(str_array.len(), str_array.len() * 4);

    for opt_s in str_array.iter() {
        if let Some(s) = opt_s {
            let code = compute_soundex(s);
            builder.append_value(&code);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn soundex_view(str_view: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_string_view_array(str_view)?;

    // Pre-allocate for Utf8View as well
    let mut builder = StringBuilder::with_capacity(str_array.len(), str_array.len() * 4);

    for opt_str in str_array.iter() {
        if let Some(s) = opt_str {
            let code = compute_soundex(s);
            builder.append_value(&code);
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[inline]
fn classify_byte(c: u8) -> Option<u8> {
    match c {
        b'B' | b'F' | b'P' | b'V' => Some(b'1'),
        b'C' | b'G' | b'J' | b'K' | b'Q' | b'S' | b'X' | b'Z' => Some(b'2'),
        b'D' | b'T' => Some(b'3'),
        b'L' => Some(b'4'),
        b'M' | b'N' => Some(b'5'),
        b'R' => Some(b'6'),
        _ => None,
    }
}

fn classify_char(c: char) -> Option<char> {
    match c.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => Some('1'),
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
        'D' | 'T' => Some('3'),
        'L' => Some('4'),
        'M' | 'N' => Some('5'),
        'R' => Some('6'),
        _ => None,
    }
}

fn is_ignored(c: char) -> bool {
    matches!(c.to_ascii_uppercase(), 'H' | 'W')
}

fn compute_soundex(s: &str) -> String {
    // Fast path for ASCII strings: operate directly on raw bytes with zero heap allocations
    if s.is_ascii() {
        let bytes = s.as_bytes();
        let mut iter = bytes.iter();

        let first_byte = match iter.next() {
            Some(&c) if c.is_ascii_alphabetic() => c.to_ascii_uppercase(),
            _ => return s.to_string(),
        };

        // Soundex codes are always exactly 4 characters, padded with '0'
        let mut buf = [b'0'; 4];
        buf[0] = first_byte;
        let mut len = 1;
        let mut last_code = classify_byte(first_byte);

        for &c in iter {
            if len >= 4 {
                break;
            }
            let upper = c.to_ascii_uppercase();
            if upper == b'H' || upper == b'W' {
                continue;
            }

            match classify_byte(upper) {
                Some(code) => {
                    if last_code != Some(code) {
                        buf[len] = code;
                        len += 1;
                    }
                    last_code = Some(code);
                }
                None => {
                    last_code = None;
                }
            }
        }

        // SAFETY: buf contains exclusively valid ASCII uppercase letters and '0' digits
        return unsafe { std::str::from_utf8_unchecked(&buf).to_string() };
    }

    let mut chars = s.chars();
    let first_char = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => c.to_ascii_uppercase(),
        _ => return s.to_string(),
    };

    let mut buf = [b'0'; 4];
    buf[0] = first_char as u8;
    let mut len = 1;
    let mut last_code = classify_char(first_char);

    for c in chars {
        if len >= 4 {
            break;
        }
        if is_ignored(c) {
            continue;
        }

        match classify_char(c) {
            Some(code) => {
                let code_u8 = code as u8;
                if last_code != Some(code) {
                    buf[len] = code_u8;
                    len += 1;
                }
                last_code = Some(code);
            }
            None => {
                last_code = None;
            }
        }
    }

    unsafe { std::str::from_utf8_unchecked(&buf).to_string() }
}
