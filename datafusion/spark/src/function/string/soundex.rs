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

use arrow::array::{ArrayRef, OffsetSizeTrait, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "soundex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_soundex_inner, vec![])(&args.args)
    }
}

fn spark_soundex_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("soundex", arg)?;
    match &array.data_type() {
        DataType::Utf8 => soundex::<i32>(array),
        DataType::LargeUtf8 => soundex::<i64>(array),
        other => {
            exec_err!("unsupported data type {other:?} for function `soundex`")
        }
    }
}

fn soundex<T: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_generic_string_array::<T>(array)?;

    let result = str_array
        .iter()
        .map(|s| s.map(compute_soundex))
        .collect::<StringArray>();

    Ok(Arc::new(result))
}

const US_ENGLISH_MAPPING: [u8; 26] = [
    b'0', b'1', b'2', b'3', b'0', b'1', b'2', b'7', b'0', b'2', b'2', b'4', b'5', b'5',
    b'0', b'1', b'2', b'6', b'2', b'3', b'0', b'1', b'7', b'2', b'0', b'2',
];

fn compute_soundex(s: &str) -> String {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return String::new();
    }

    let mut b = bytes[0];

    if b.is_ascii_lowercase() {
        b -= 32;
    } else if !b.is_ascii_uppercase() {
        return s.to_string();
    }

    let mut soundex_code = [b, b'0', b'0', b'0'];
    let mut sxi = 1;
    let idx = (b - b'A') as usize;
    let mut last_code = US_ENGLISH_MAPPING[idx];

    for i in bytes.iter().skip(1) {
        let mut b = *i;

        if b.is_ascii_lowercase() {
            b -= 32;
        } else if !b.is_ascii_uppercase() {
            last_code = b'0';
            continue;
        }

        let idx = (b - b'A') as usize;
        let code = US_ENGLISH_MAPPING[idx];

        if code == b'7' {
            continue;
        } else {
            if code != b'0' && code != last_code {
                soundex_code[sxi] = code;
                sxi += 1;
                if sxi > 3 {
                    break;
                }
            }
            last_code = code;
        }
    }

    String::from_utf8_lossy(&soundex_code).to_string()
}
