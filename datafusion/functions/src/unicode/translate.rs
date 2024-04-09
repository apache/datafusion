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
use hashbrown::HashMap;
use unicode_segmentation::UnicodeSegmentation;

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub struct TranslateFunc {
    signature: Signature,
}

impl TranslateFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Utf8, Utf8, Utf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TranslateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "translate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "translate")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(translate::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(translate::<i64>, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function translate")
            }
        }
    }
}

/// Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.
/// translate('12345', '143', 'ax') = 'a2x5'
fn translate<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let from_array = as_generic_string_array::<T>(&args[1])?;
    let to_array = as_generic_string_array::<T>(&args[2])?;

    let result = string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => {
                // create a hashmap of [char, index] to change from O(n) to O(1) for from list
                let from_map: HashMap<&str, usize> = from
                    .graphemes(true)
                    .collect::<Vec<&str>>()
                    .iter()
                    .enumerate()
                    .map(|(index, c)| (c.to_owned(), index))
                    .collect();

                let to = to.graphemes(true).collect::<Vec<&str>>();

                Some(
                    string
                        .graphemes(true)
                        .collect::<Vec<&str>>()
                        .iter()
                        .flat_map(|c| match from_map.get(*c) {
                            Some(n) => to.get(*n).copied(),
                            None => Some(*c),
                        })
                        .collect::<Vec<&str>>()
                        .concat(),
                )
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::translate::TranslateFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            TranslateFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("12345")),
                ColumnarValue::Scalar(ScalarValue::from("143")),
                ColumnarValue::Scalar(ScalarValue::from("ax"))
            ],
            Ok(Some("a2x5")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            TranslateFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("143")),
                ColumnarValue::Scalar(ScalarValue::from("ax"))
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            TranslateFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("12345")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("ax"))
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            TranslateFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("12345")),
                ColumnarValue::Scalar(ScalarValue::from("143")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None))
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            TranslateFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("é2íñ5")),
                ColumnarValue::Scalar(ScalarValue::from("éñí")),
                ColumnarValue::Scalar(ScalarValue::from("óü")),
            ],
            Ok(Some("ó2ü5")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            TranslateFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("12345")),
                ColumnarValue::Scalar(ScalarValue::from("143")),
                ColumnarValue::Scalar(ScalarValue::from("ax")),
            ],
            internal_err!(
                "function translate requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
