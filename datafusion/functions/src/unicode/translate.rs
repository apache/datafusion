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

use arrow::array::{
    ArrayAccessor, ArrayIter, ArrayRef, AsArray, LargeStringBuilder, StringBuilder,
    StringLikeArrayBuilder, StringViewBuilder,
};
use arrow::datatypes::DataType;
use datafusion_common::HashMap;

use crate::utils::make_scalar_function;
use datafusion_common::{Result, exec_err};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
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
    fn name(&self) -> &str {
        "translate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // When from and to are scalars, pre-build the translation map once
        if let (Some(from_str), Some(to_str)) = (
            try_as_scalar_str(&args.args[1]),
            try_as_scalar_str(&args.args[2]),
        ) {
            let to_chars: Vec<char> = to_str.chars().collect();

            let mut from_map: HashMap<char, usize> = HashMap::new();
            for (index, c) in from_str.chars().enumerate() {
                from_map.entry(c).or_insert(index);
            }

            let ascii_table = build_ascii_translate_table(from_str, to_str);

            let string_array = args.args[0].to_array_of_size(args.number_rows)?;
            let len = string_array.len();

            let result = match string_array.data_type() {
                DataType::Utf8View => {
                    let arr = string_array.as_string_view();
                    let builder = StringViewBuilder::with_capacity(len);
                    translate_with_map(
                        arr,
                        &from_map,
                        &to_chars,
                        ascii_table.as_ref(),
                        builder,
                    )
                }
                DataType::Utf8 => {
                    let arr = string_array.as_string::<i32>();
                    let builder =
                        StringBuilder::with_capacity(len, arr.value_data().len());
                    translate_with_map(
                        arr,
                        &from_map,
                        &to_chars,
                        ascii_table.as_ref(),
                        builder,
                    )
                }
                DataType::LargeUtf8 => {
                    let arr = string_array.as_string::<i64>();
                    let builder =
                        LargeStringBuilder::with_capacity(len, arr.value_data().len());
                    translate_with_map(
                        arr,
                        &from_map,
                        &to_chars,
                        ascii_table.as_ref(),
                        builder,
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

use super::common::try_as_scalar_str;

fn invoke_translate(args: &[ArrayRef]) -> Result<ArrayRef> {
    let len = args[0].len();
    match args[0].data_type() {
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
            let builder = StringViewBuilder::with_capacity(len);
            translate(string_array, from_array, to_array, builder)
        }
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
            let builder =
                StringBuilder::with_capacity(len, string_array.value_data().len());
            translate(string_array, from_array, to_array, builder)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
            let builder =
                LargeStringBuilder::with_capacity(len, string_array.value_data().len());
            translate(string_array, from_array, to_array, builder)
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function translate")
        }
    }
}

/// Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.
/// translate('12345', '143', 'ax') = 'a2x5'
fn translate<'a, V, B, O>(
    string_array: V,
    from_array: B,
    to_array: B,
    mut builder: O,
) -> Result<ArrayRef>
where
    V: ArrayAccessor<Item = &'a str>,
    B: ArrayAccessor<Item = &'a str>,
    O: StringLikeArrayBuilder,
{
    let string_array_iter = ArrayIter::new(string_array);
    let from_array_iter = ArrayIter::new(from_array);
    let to_array_iter = ArrayIter::new(to_array);

    let mut from_map: HashMap<char, usize> = HashMap::new();
    let mut to_chars: Vec<char> = Vec::new();
    let mut result_buf = String::new();

    for ((string, from), to) in string_array_iter.zip(from_array_iter).zip(to_array_iter)
    {
        match (string, from, to) {
            (Some(string), Some(from), Some(to)) => {
                from_map.clear();
                to_chars.clear();
                result_buf.clear();

                for (index, c) in from.chars().enumerate() {
                    from_map.entry(c).or_insert(index);
                }

                to_chars.extend(to.chars());

                translate_char_by_char(string, &from_map, &to_chars, &mut result_buf);

                builder.append_value(&result_buf);
            }
            _ => builder.append_null(),
        }
    }

    Ok(builder.finish())
}

/// Translate `input` character-by-character using `from_map` and `to_chars`,
/// appending the result to `buf`.
#[inline]
fn translate_char_by_char(
    input: &str,
    from_map: &HashMap<char, usize>,
    to_chars: &[char],
    buf: &mut String,
) {
    for c in input.chars() {
        match from_map.get(&c) {
            Some(n) => {
                if let Some(&replacement) = to_chars.get(*n) {
                    buf.push(replacement);
                }
            }
            None => buf.push(c),
        }
    }
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
/// inputs fall back to the char-based map.
fn translate_with_map<'a, V, O>(
    string_array: V,
    from_map: &HashMap<char, usize>,
    to_chars: &[char],
    ascii_table: Option<&[u8; 128]>,
    mut builder: O,
) -> Result<ArrayRef>
where
    V: ArrayAccessor<Item = &'a str>,
    O: StringLikeArrayBuilder,
{
    let mut result_buf = String::new();
    let mut ascii_buf: Vec<u8> = Vec::new();

    for string in ArrayIter::new(string_array) {
        match string {
            Some(s) => {
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
                    builder.append_value(unsafe {
                        std::str::from_utf8_unchecked(&ascii_buf)
                    });
                } else {
                    result_buf.clear();
                    translate_char_by_char(s, from_map, to_chars, &mut result_buf);
                    builder.append_value(&result_buf);
                }
            }
            None => builder.append_null(),
        }
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};

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
        // char-based fallback within translate_with_map.
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
        // Utf8View input should produce Utf8View output
        test_function!(
            TranslateFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("12345".into()))),
                ColumnarValue::Scalar(ScalarValue::from("143")),
                ColumnarValue::Scalar(ScalarValue::from("ax"))
            ],
            Ok(Some("a2x5")),
            &str,
            Utf8View,
            StringViewArray
        );
        // Null Utf8View input
        test_function!(
            TranslateFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::from("143")),
                ColumnarValue::Scalar(ScalarValue::from("ax"))
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        // Non-ASCII Utf8View input
        test_function!(
            TranslateFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("é2íñ5".into()))),
                ColumnarValue::Scalar(ScalarValue::from("éñí")),
                ColumnarValue::Scalar(ScalarValue::from("óü"))
            ],
            Ok(Some("ó2ü5")),
            &str,
            Utf8View,
            StringViewArray
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
