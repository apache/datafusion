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

fn compute_soundex(s: &str) -> String {
    let mut chars = s.chars().filter(|c| c.is_ascii_alphabetic());

    let first_ch = match chars.next() {
        Some(c) => c.to_ascii_uppercase(),
        None => return "".to_string(),
    };

    if first_ch.is_ascii_digit() {
        return s.to_string()
    }

    let mut result = String::with_capacity(4);
    result.push(first_ch);
    let mut last_code = classify_char(first_ch);

    for c in chars {
        if result.len() >= 4 {
            break;
        }
        let current = classify_char(c);
        if let Some(digit) = current
            && current != last_code
        {
            result.push(digit);
        }
        last_code = current;
    }
    while result.len() < 4 {
        result.push('0');
    }
    result
}

fn classify_char(c: char) -> Option<char> {
    match c.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => Some('1'),
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
        'D' | 'T' => Some('3'),
        'L' => Some('4'),
        'M' | 'N' => Some('5'),
        'R' => Some('6'),
        _ => None, // A, E, I, O, U, H, W, Y
    }
}
