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

use arrow::array::{Array, ArrayRef, AsArray, GenericStringArray, StringArrayType};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use datafusion_common::HashMap;

use super::common::try_as_scalar_str;
use crate::strings::{
    BulkNullStringArrayBuilder, GenericStringArrayBuilder, StringViewArrayBuilder,
    StringWriter,
};
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
            let table = build_translate_table(from_str, to_str);

            let string_array = args.args[0].to_array_of_size(args.number_rows)?;
            let len = string_array.len();

            let result = match string_array.data_type() {
                DataType::Utf8View => {
                    let arr = string_array.as_string_view();
                    let builder = StringViewArrayBuilder::with_capacity(len);
                    translate_with_table(&arr, &table, builder)
                }
                DataType::Utf8 => {
                    let arr = string_array.as_string::<i32>();
                    let builder = GenericStringArrayBuilder::<i32>::with_capacity(
                        len,
                        arr.value_data().len(),
                    );
                    translate_with_table(&arr, &table, builder)
                }
                DataType::LargeUtf8 => {
                    let arr = string_array.as_string::<i64>();
                    let builder = GenericStringArrayBuilder::<i64>::with_capacity(
                        len,
                        arr.value_data().len(),
                    );
                    translate_with_table(&arr, &table, builder)
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

fn invoke_translate(args: &[ArrayRef]) -> Result<ArrayRef> {
    let len = args[0].len();
    match args[0].data_type() {
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
            let builder = StringViewArrayBuilder::with_capacity(len);
            translate(&string_array, from_array, to_array, builder)
        }
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
            let builder = GenericStringArrayBuilder::<i32>::with_capacity(
                len,
                string_array.value_data().len(),
            );
            translate(&string_array, from_array, to_array, builder)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            let from_array = args[1].as_string::<i32>();
            let to_array = args[2].as_string::<i32>();
            let builder = GenericStringArrayBuilder::<i64>::with_capacity(
                len,
                string_array.value_data().len(),
            );
            translate(&string_array, from_array, to_array, builder)
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function translate")
        }
    }
}

/// Replaces each character in string that matches a character in the from set
/// with the corresponding character in the to set. If from is longer than to,
/// occurrences of the extra characters in from are deleted.
///
/// translate('12345', '143', 'ax') = 'a2x5'
fn translate<'a, S, O>(
    string_array: &S,
    from_array: &GenericStringArray<i32>,
    to_array: &GenericStringArray<i32>,
    mut builder: O,
) -> Result<ArrayRef>
where
    S: StringArrayType<'a>,
    O: BulkNullStringArrayBuilder,
{
    let mut from_map: HashMap<char, Option<char>> = HashMap::new();
    let len = string_array.len();
    let nulls = NullBuffer::union_many([
        string_array.nulls(),
        from_array.nulls(),
        to_array.nulls(),
    ]);

    if let Some(nulls_ref) = nulls.as_ref() {
        for i in 0..len {
            if nulls_ref.is_null(i) {
                builder.append_placeholder();
                continue;
            }

            // SAFETY: union of input nulls is non-null at i, so each input is too.
            let string = unsafe { string_array.value_unchecked(i) };
            let from = unsafe { from_array.value_unchecked(i) };
            let to = unsafe { to_array.value_unchecked(i) };
            append_translated_row(&mut builder, string, from, to, &mut from_map);
        }
    } else {
        for i in 0..len {
            // SAFETY: i < len, and no input has a null buffer.
            let string = unsafe { string_array.value_unchecked(i) };
            let from = unsafe { from_array.value_unchecked(i) };
            let to = unsafe { to_array.value_unchecked(i) };
            append_translated_row(&mut builder, string, from, to, &mut from_map);
        }
    }

    builder.finish(nulls)
}

#[inline]
fn append_translated_row<B: BulkNullStringArrayBuilder>(
    builder: &mut B,
    string: &str,
    from: &str,
    to: &str,
    from_map: &mut HashMap<char, Option<char>>,
) {
    if let Some(ascii_table) = build_ascii_translate_table(from, to) {
        append_translated_ascii(builder, string, &ascii_table);
        return;
    }

    from_map.clear();
    let mut to_iter = to.chars();
    for c in from.chars() {
        let replacement = to_iter.next();
        from_map.entry(c).or_insert(replacement);
    }

    builder.append_with(|w| write_translated_chars(w, string, from_map));
}

#[inline]
fn write_translated_chars<W: StringWriter>(
    w: &mut W,
    input: &str,
    from_map: &HashMap<char, Option<char>>,
) {
    for c in input.chars() {
        match from_map.get(&c) {
            Some(Some(r)) => w.write_char(*r),
            Some(None) => {} // delete: `from` had no corresponding `to` char
            None => w.write_char(c),
        }
    }
}

/// Sentinel value in the ASCII translate table indicating the character should
/// be deleted (the `from` character has no corresponding `to` character).  Any
/// value > 127 works since valid ASCII is 0–127.
const ASCII_DELETE: u8 = 0xFF;

/// Lookup table for ASCII-only translation. Entries 0..128 map input bytes to
/// replacement bytes, or `ASCII_DELETE` if the character should be deleted.
/// Entries 128..256 map to themselves so non-ASCII bytes pass through
/// unchanged.
#[derive(Debug)]
struct AsciiTranslateTable {
    map: [u8; 256],
    has_delete: bool,
}

/// We use a byte-indexed table when both `from` and `to` strings are ASCII,
/// otherwise a char-indexed map where `None` means delete.
#[expect(
    clippy::large_enum_variant,
    reason = "one instance per call, passed by reference"
)]
enum TranslateTable {
    Byte(AsciiTranslateTable),
    Char(HashMap<char, Option<char>>),
}

#[inline]
fn build_translate_table(from: &str, to: &str) -> TranslateTable {
    if let Some(ascii) = build_ascii_translate_table(from, to) {
        return TranslateTable::Byte(ascii);
    }
    let mut from_map: HashMap<char, Option<char>> = HashMap::with_capacity(from.len());
    let mut to_iter = to.chars();
    for c in from.chars() {
        let replacement = to_iter.next();
        from_map.entry(c).or_insert(replacement);
    }
    TranslateTable::Char(from_map)
}

/// Returns `None` if either string contains non-ASCII characters.
fn build_ascii_translate_table(from: &str, to: &str) -> Option<AsciiTranslateTable> {
    if !from.is_ascii() || !to.is_ascii() {
        return None;
    }

    let to_bytes = to.as_bytes();
    let mut map = std::array::from_fn::<u8, 256, _>(|i| i as u8);
    let mut seen = [false; 128];
    let mut has_delete = false;

    for (i, from_byte) in from.bytes().enumerate() {
        let idx = from_byte as usize;
        if !seen[idx] {
            seen[idx] = true;
            if i < to_bytes.len() {
                map[idx] = to_bytes[i];
            } else {
                map[idx] = ASCII_DELETE;
                has_delete = true;
            }
        }
    }

    Some(AsciiTranslateTable { map, has_delete })
}

#[inline]
fn append_translated_ascii<B: BulkNullStringArrayBuilder>(
    builder: &mut B,
    input: &str,
    table: &AsciiTranslateTable,
) {
    // Fast path: equal-length byte-to-byte map when no deletions.
    if !table.has_delete {
        // SAFETY: ASCII source bytes map to ASCII replacements; non-ASCII
        // bytes 128..256 map to themselves, so multi-byte UTF-8 sequences
        // pass through unchanged. Output length equals input length and
        // remains valid UTF-8.
        unsafe {
            builder.append_byte_map(input.as_bytes(), |b| table.map[b as usize]);
        }
    } else {
        builder.append_with(|w| write_translated_ascii(w, input, table));
    }
}

#[inline]
fn write_translated_ascii<W: StringWriter>(
    w: &mut W,
    input: &str,
    table: &AsciiTranslateTable,
) {
    let bytes = input.as_bytes();
    let mut copy_start = 0;

    for (i, &b) in bytes.iter().enumerate() {
        let mapped = table.map[b as usize];
        if mapped == b {
            continue;
        }

        if copy_start < i {
            w.write_str(&input[copy_start..i]);
        }
        if mapped != ASCII_DELETE {
            w.write_char(mapped as char);
        }
        copy_start = i + 1;
    }

    if copy_start < input.len() {
        w.write_str(&input[copy_start..]);
    }
}

fn translate_with_table<'a, S, O>(
    string_array: &S,
    table: &TranslateTable,
    mut builder: O,
) -> Result<ArrayRef>
where
    S: StringArrayType<'a>,
    O: BulkNullStringArrayBuilder,
{
    let len = string_array.len();
    let nulls = string_array.nulls().cloned();

    if let Some(nulls_ref) = nulls.as_ref() {
        for i in 0..len {
            if nulls_ref.is_null(i) {
                builder.append_placeholder();
                continue;
            }

            // SAFETY: input null buffer is non-null at i.
            let s = unsafe { string_array.value_unchecked(i) };
            apply_translate_table(&mut builder, s, table);
        }
    } else {
        for i in 0..len {
            // SAFETY: no null buffer means every index is valid.
            let s = unsafe { string_array.value_unchecked(i) };
            apply_translate_table(&mut builder, s, table);
        }
    }

    builder.finish(nulls)
}

#[inline]
fn apply_translate_table<B: BulkNullStringArrayBuilder>(
    builder: &mut B,
    input: &str,
    table: &TranslateTable,
) {
    match table {
        TranslateTable::Byte(t) => append_translated_ascii(builder, input, t),
        TranslateTable::Char(m) => {
            builder.append_with(|w| write_translated_chars(w, input, m))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, StringArray, StringViewArray};
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
        // Non-ASCII input with ASCII scalar from/to.
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

    #[test]
    fn test_array_args_with_nulls() -> Result<()> {
        let string_array = Arc::new(StringArray::from(vec![
            Some("café!"),
            Some("abc"),
            Some("abc"),
        ])) as ArrayRef;
        let from_array =
            Arc::new(StringArray::from(vec![Some("!"), Some("a"), None])) as ArrayRef;
        let to_array =
            Arc::new(StringArray::from(vec![Some(""), Some("x"), Some("y")])) as ArrayRef;

        let result = super::invoke_translate(&[string_array, from_array, to_array])?;
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "café");
        assert_eq!(result.value(1), "xbc");
        assert!(result.is_null(2));

        Ok(())
    }
}
