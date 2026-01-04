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

use arrow::array::{
    ArrayAccessor, ArrayIter, ArrayRef, AsArray, GenericStringArray, OffsetSizeTrait,
};
use arrow::datatypes::DataType;
use datafusion_common::HashMap;
use unicode_segmentation::UnicodeSegmentation;

use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::{Result, exec_err};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Translates characters in a string to specified translation characters.",
    syntax_example = "translate(str, chars, translation)",
    sql_example = r#"```sql
> select translate('twice', 'wic', 'her');
+--------------------------------------------------+
| translate(Utf8("twice"),Utf8("wic"),Utf8("her")) |
+--------------------------------------------------+
| there                                            |
+--------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "chars", description = "Characters to translate."),
    argument(
        name = "translation",
        description = "Translation characters. Translation characters replace only characters at the same position in the **chars** string."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TranslateFunc {
    signature: Signature,
}

impl Default for TranslateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TranslateFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Utf8, Utf8]),
                    Exact(vec![Utf8, Utf8, Utf8]),
                ],
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

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(invoke_translate, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn invoke_translate(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
            translate::<i32, _, _>(string_array, from_array, to_array)
        }
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
            translate::<i32, _, _>(string_array, from_array, to_array)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            let from_array = args[1].as_string::<i64>();
            let to_array = args[2].as_string::<i64>();
            translate::<i64, _, _>(string_array, from_array, to_array)
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function translate")
        }
    }
}

/// Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.
/// translate('12345', '143', 'ax') = 'a2x5'
fn translate<'a, T: OffsetSizeTrait, V, B>(
    string_array: V,
    from_array: B,
    to_array: B,
) -> Result<ArrayRef>
where
    V: ArrayAccessor<Item = &'a str>,
    B: ArrayAccessor<Item = &'a str>,
{
    let string_array_iter = ArrayIter::new(string_array);
    let from_array_iter = ArrayIter::new(from_array);
    let to_array_iter = ArrayIter::new(to_array);

    // Reusable buffers to avoid allocating for each row
    let mut from_map: HashMap<&str, usize> = HashMap::new();
    let mut from_graphemes: Vec<&str> = Vec::new();
    let mut to_graphemes: Vec<&str> = Vec::new();
    let mut string_graphemes: Vec<&str> = Vec::new();
    let mut result_graphemes: Vec<&str> = Vec::new();

    let result = string_array_iter
        .zip(from_array_iter)
        .zip(to_array_iter)
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => {
                // Clear and reuse buffers
                from_map.clear();
                from_graphemes.clear();
                to_graphemes.clear();
                string_graphemes.clear();
                result_graphemes.clear();

                // Build from_map using reusable buffer
                from_graphemes.extend(from.graphemes(true));
                for (index, c) in from_graphemes.iter().enumerate() {
                    // Ignore characters that already exist in from_map, else insert
                    from_map.entry(*c).or_insert(index);
                }

                // Build to_graphemes
                to_graphemes.extend(to.graphemes(true));

                // Process string and build result
                string_graphemes.extend(string.graphemes(true));
                for c in &string_graphemes {
                    match from_map.get(*c) {
                        Some(n) => {
                            if let Some(replacement) = to_graphemes.get(*n) {
                                result_graphemes.push(*replacement);
                            }
                        }
                        None => result_graphemes.push(*c),
                    }
                }

                Some(result_graphemes.concat())
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcabc")),
                ColumnarValue::Scalar(ScalarValue::from("aa")),
                ColumnarValue::Scalar(ScalarValue::from("de"))
            ],
            Ok(Some("dbcdbc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            TranslateFunc::new(),
            vec![
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
            vec![
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
