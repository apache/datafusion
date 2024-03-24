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

use arrow::array::{ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::utils::datafusion_strsim;
use datafusion_common::{exec_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

use crate::string::common::{make_scalar_function, utf8_to_int_type};

#[derive(Debug)]
pub(super) struct LevenshteinFunc {
    signature: Signature,
}

impl LevenshteinFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Utf8, Utf8]), Exact(vec![LargeUtf8, LargeUtf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LevenshteinFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "levenshtein"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "levenshtein")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(levenshtein::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(levenshtein::<i64>, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function levenshtein")
            }
        }
    }
}

///Returns the Levenshtein distance between the two given strings.
/// LEVENSHTEIN('kitten', 'sitting') = 3
pub fn levenshtein<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!(
            "levenshtein function requires two arguments, got {}",
            args.len()
        );
    }
    let str1_array = as_generic_string_array::<T>(&args[0])?;
    let str2_array = as_generic_string_array::<T>(&args[1])?;
    match args[0].data_type() {
        DataType::Utf8 => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .map(|(string1, string2)| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        Some(datafusion_strsim::levenshtein(string1, string2) as i32)
                    }
                    _ => None,
                })
                .collect::<Int32Array>();
            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .map(|(string1, string2)| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        Some(datafusion_strsim::levenshtein(string1, string2) as i64)
                    }
                    _ => None,
                })
                .collect::<Int64Array>();
            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "levenshtein was called with {other} datatype arguments. It requires Utf8 or LargeUtf8."
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};

    use datafusion_common::cast::as_int32_array;

    use super::*;

    #[test]
    fn to_levenshtein() -> Result<()> {
        let string1_array =
            Arc::new(StringArray::from(vec!["123", "abc", "xyz", "kitten"]));
        let string2_array =
            Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));
        let res = levenshtein::<i32>(&[string1_array, string2_array]).unwrap();
        let result =
            as_int32_array(&res).expect("failed to initialized function levenshtein");
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        Ok(())
    }
}
