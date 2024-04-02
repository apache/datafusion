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
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct SubstrIndexFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl SubstrIndexFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("substring_index")],
        }
    }
}

impl ScalarUDFImpl for SubstrIndexFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "substr_index"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "substr_index")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(substr_index::<i32>, vec![])(args),
            DataType::LargeUtf8 => {
                make_scalar_function(substr_index::<i64>, vec![])(args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function substr_index")
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Returns the substring from str before count occurrences of the delimiter delim. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
/// SUBSTRING_INDEX('www.apache.org', '.', 1) = www
/// SUBSTRING_INDEX('www.apache.org', '.', 2) = www.apache
/// SUBSTRING_INDEX('www.apache.org', '.', -2) = apache.org
/// SUBSTRING_INDEX('www.apache.org', '.', -1) = org
pub fn substr_index<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "substr_index was called with {} arguments. It requires 3.",
            args.len()
        );
    }

    let string_array = as_generic_string_array::<T>(&args[0])?;
    let delimiter_array = as_generic_string_array::<T>(&args[1])?;
    let count_array = as_int64_array(&args[2])?;

    let result = string_array
        .iter()
        .zip(delimiter_array.iter())
        .zip(count_array.iter())
        .map(|((string, delimiter), n)| match (string, delimiter, n) {
            (Some(string), Some(delimiter), Some(n)) => {
                // In MySQL, these cases will return an empty string.
                if n == 0 || string.is_empty() || delimiter.is_empty() {
                    return Some(String::new());
                }

                let splitted: Box<dyn Iterator<Item = _>> = if n > 0 {
                    Box::new(string.split(delimiter))
                } else {
                    Box::new(string.rsplit(delimiter))
                };
                let occurrences = usize::try_from(n.unsigned_abs()).unwrap_or(usize::MAX);
                // The length of the substring covered by substr_index.
                let length = splitted
                    .take(occurrences) // at least 1 element, since n != 0
                    .map(|s| s.len() + delimiter.len())
                    .sum::<usize>()
                    - delimiter.len();
                if n > 0 {
                    Some(string[..length].to_owned())
                } else {
                    Some(string[string.len() - length..].to_owned())
                }
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}
