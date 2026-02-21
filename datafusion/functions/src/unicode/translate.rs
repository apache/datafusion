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
    description = "Performs character-wise substitution based on a mapping.",
    syntax_example = "translate(str, from, to)",
    sql_example = r#"```sql
> select translate('twice', 'wic', 'her');
+--------------------------------------------------+
| translate(Utf8("twice"),Utf8("wic"),Utf8("her")) |
+--------------------------------------------------+
| there                                            |
+--------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "from", description = "The characters to be replaced."),
    argument(
        name = "to",
        description = "The characters to replace them with. Each character in **from** that is found in **str** is replaced by the character at the same index in **to**. Any characters in **from** that don't have a corresponding character in **to** are removed. If a character appears more than once in **from**, the first occurrence determines the mapping."
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
                    Exact(vec![LargeUtf8, Utf8, Utf8]),
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
        // When from and to are scalars, pre-build the translation map once
        if let (Some(from_str), Some(to_str)) = (
            try_as_scalar_str(&args.args[1]),
            try_as_scalar_str(&args.args[2]),
        ) {
            let to_graphemes: Vec<&str> = to_str.graphemes(true).collect();

            let mut from_map: HashMap<&str, usize> = HashMap::new();
            for (index, c) in from_str.graphemes(true).enumerate() {
                // Ignore characters that already exist in from_map
                from_map.entry(c).or_insert(index);
            }

            let ascii_table = build_ascii_translate_table(from_str, to_str);

            let string_array = args.args[0].to_array_of_size(args.number_rows)?;

            let result = match string_array.data_type() {
                DataType::Utf8View => {
                    let arr = string_array.as_string_view();
                    translate_with_map::<i32, _>(
                        arr,
                        &from_map,
                        &to_graphemes,
                        ascii_table.as_ref(),
                    )
                }
                DataType::Utf8 => {
                    let arr = string_array.as_string::<i32>();
                    translate_with_map::<i32, _>(
                        arr,
                        &from_map,
                        &to_graphemes,
                        ascii_table.as_ref(),
                    )
                }
                DataType::LargeUtf8 => {
                    let arr = string_array.as_string::<i64>();
                    translate_with_map::<i64, _>(
                        arr,
                        &from_map,
                        &to_graphemes,
                        ascii_table.as_ref(),
                    )
                }
                other => {
                    return exec_err!(
                        "Unsupported data type {other:?} for function translate"
                    );
                }
            }?;

            return Ok(ColumnarValue::Array(result));
        }

        make_scalar_function(invoke_translate, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// If `cv` is a non-null scalar string, return its value.
fn try_as_scalar_str(cv: &ColumnarValue) -> Option<&str> {
    match cv {
        ColumnarValue::Scalar(s) => s.try_as_str().flatten(),
        _ => None,
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
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
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
                    // Ignore characters that already exist in from_map
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

/// Sentinel value in the ASCII translate table indicating the character should
/// be deleted (the `from` character has no corresponding `to` character).  Any
/// value > 127 works since valid ASCII is 0–127.
const ASCII_DELETE: u8 = 0xFF;

/// If `from` and `to` are both ASCII, build a fixed-size lookup table for
/// translation. Each entry maps an input byte to its replacement byte, or to
/// [`ASCII_DELETE`] if the character should be removed.  Returns `None` if
/// either string contains non-ASCII characters.
fn build_ascii_translate_table(from: &str, to: &str) -> Option<[u8; 128]> {
    if !from.is_ascii() || !to.is_ascii() {
        return None;
    }
    let mut table = [0u8; 128];
    for i in 0..128u8 {
        table[i as usize] = i;
    }
    let to_bytes = to.as_bytes();
    let mut seen = [false; 128];
    for (i, from_byte) in from.bytes().enumerate() {
        let idx = from_byte as usize;
        if !seen[idx] {
            seen[idx] = true;
            if i < to_bytes.len() {
                table[idx] = to_bytes[i];
            } else {
                table[idx] = ASCII_DELETE;
            }
        }
    }
    Some(table)
}

/// Optimized translate for constant `from` and `to` arguments: uses a pre-built
/// translation map instead of rebuilding it for every row.  When an ASCII byte
/// lookup table is provided, ASCII input rows use the lookup table; non-ASCII
/// inputs fallback to using the map.
fn translate_with_map<'a, T: OffsetSizeTrait, V>(
    string_array: V,
    from_map: &HashMap<&str, usize>,
    to_graphemes: &[&str],
    ascii_table: Option<&[u8; 128]>,
) -> Result<ArrayRef>
where
    V: ArrayAccessor<Item = &'a str>,
{
    let mut result_graphemes: Vec<&str> = Vec::new();
    let mut ascii_buf: Vec<u8> = Vec::new();

    let result = ArrayIter::new(string_array)
        .map(|string| {
            string.map(|s| {
                // Fast path: byte-level table lookup for ASCII strings
                if let Some(table) = ascii_table
                    && s.is_ascii()
                {
                    ascii_buf.clear();
                    for &b in s.as_bytes() {
                        let mapped = table[b as usize];
                        if mapped != ASCII_DELETE {
                            ascii_buf.push(mapped);
                        }
                    }
                    // SAFETY: all bytes are ASCII, hence valid UTF-8.
                    return unsafe {
                        std::str::from_utf8_unchecked(&ascii_buf).to_owned()
                    };
                }

                // Slow path: grapheme-based translation
                result_graphemes.clear();

                for c in s.graphemes(true) {
                    match from_map.get(c) {
                        Some(n) => {
                            if let Some(replacement) = to_graphemes.get(*n) {
                                result_graphemes.push(*replacement);
                            }
                        }
                        None => result_graphemes.push(c),
                    }
                }

                result_graphemes.concat()
            })
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
        // Non-ASCII input with ASCII scalar from/to: exercises the
        // grapheme fallback within translate_with_map.
        test_function!(
            TranslateFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("café")),
                ColumnarValue::Scalar(ScalarValue::from("ae")),
                ColumnarValue::Scalar(ScalarValue::from("AE"))
            ],
            Ok(Some("cAfé")),
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
