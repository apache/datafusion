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

pub fn compute_soundex(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }

    let mut chars = s.chars();
    let first_char = chars.next().unwrap();

    let first_code = match classify_char(first_char) {
        Some(code) => code,
        None => return s.to_string(),
    };

    let mut result = [first_code, b'0', b'0', b'0'];
    let mut result_index = 1;
    let mut last_code = first_code;

    for c in chars {
        let current_code = match classify_char(c) {
            Some(code) => code,
            None => {
                last_code = b'0';
                continue;
            }
        };

        if current_code != b'0' && current_code != last_code {
            result[result_index] = current_code;
            result_index += 1;
            if result_index >= result.len() {
                break;
            }
        }

        last_code = current_code;
    }

    String::from_utf8_lossy(&result).to_string()
}

fn classify_char(c: char) -> Option<u8> {
    match c.to_ascii_uppercase() {
        'A' | 'E' | 'I' | 'O' | 'U' | 'Y' => Some(0),
        'B' | 'F' | 'P' | 'V' => Some(1),
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some(2),
        'D' | 'T' => Some(3),
        'L' => Some(4),
        'M' | 'N' => Some(5),
        'R' => Some(6),
        'H' | 'W' => Some(7),
        _ => None,
    }
}
