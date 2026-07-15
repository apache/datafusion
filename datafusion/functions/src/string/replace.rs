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

use std::sync::Arc;

use arrow::array::{ArrayRef, OffsetSizeTrait, StringArrayType};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use memchr::memmem;

use crate::strings::{GenericStringArrayBuilder, StringWriter};
use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::{Result, exec_err};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
#[user_doc(
    doc_section(label = "String Functions"),
    description = "Replaces all occurrences of a specified substring in a string with a new substring.",
    syntax_example = "replace(str, substr, replacement)",
    sql_example = r#"```sql
> select replace('ABabbaBA', 'ab', 'cd');
+-------------------------------------------------+
| replace(Utf8("ABabbaBA"),Utf8("ab"),Utf8("cd")) |
+-------------------------------------------------+
| ABcdbaBA                                        |
+-------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(
        name = "substr",
        prefix = "Substring expression to replace in the input string. Substring"
    ),
    standard_argument(name = "replacement", prefix = "Replacement substring")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReplaceFunc {
    signature: Signature,
}

impl Default for ReplaceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplaceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ReplaceFunc {
    fn name(&self) -> &str {
        "replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1])
            .and_then(|dt| string_coercion(&dt, &arg_types[2]))
            .or_else(|| {
                binary_to_string_coercion(&arg_types[0], &arg_types[1])
                    .and_then(|dt| binary_to_string_coercion(&dt, &arg_types[2]))
            })
        {
            utf8_to_str_type(&coercion_data_type, "replace")
        } else {
            exec_err!(
                "Unsupported data types for replace. Expected Utf8, LargeUtf8 or Utf8View"
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let data_types = args
            .args
            .iter()
            .map(|arg| arg.data_type())
            .collect::<Vec<_>>();

        if let Some(coercion_type) = string_coercion(&data_types[0], &data_types[1])
            .and_then(|dt| string_coercion(&dt, &data_types[2]))
            .or_else(|| {
                binary_to_string_coercion(&data_types[0], &data_types[1])
                    .and_then(|dt| binary_to_string_coercion(&dt, &data_types[2]))
            })
        {
            let mut converted_args = Vec::with_capacity(args.args.len());
            for arg in &args.args {
                if arg.data_type() == coercion_type {
                    converted_args.push(arg.clone());
                } else {
                    let converted = arg.cast_to(&coercion_type, None)?;
                    converted_args.push(converted);
                }
            }

            // Fast path: when `from` and `to` are non-null scalars we can
            // pre-build a substring finder once and reuse it for every haystack
            // row, mirroring the scalar-argument fast paths in
            // `strpos`/`translate`/`split_part`.
            if let (
                ColumnarValue::Array(haystack),
                ColumnarValue::Scalar(from),
                ColumnarValue::Scalar(to),
            ) = (&converted_args[0], &converted_args[1], &converted_args[2])
                && let (Some(Some(from)), Some(Some(to))) =
                    (from.try_as_str(), to.try_as_str())
            {
                let result = match coercion_type {
                    DataType::Utf8 => replace_scalar::<_, i32>(
                        as_generic_string_array::<i32>(haystack)?,
                        from,
                        to,
                    ),
                    DataType::LargeUtf8 => replace_scalar::<_, i64>(
                        as_generic_string_array::<i64>(haystack)?,
                        from,
                        to,
                    ),
                    DataType::Utf8View => replace_scalar::<_, i32>(
                        as_string_view_array(haystack)?,
                        from,
                        to,
                    ),
                    other => {
                        return exec_err!(
                            "Unsupported coercion data type {other:?} for function replace"
                        );
                    }
                };
                return result.map(ColumnarValue::Array);
            }

            match coercion_type {
                DataType::Utf8 => {
                    make_scalar_function(replace::<i32>, vec![])(&converted_args)
                }
                DataType::LargeUtf8 => {
                    make_scalar_function(replace::<i64>, vec![])(&converted_args)
                }
                DataType::Utf8View => {
                    make_scalar_function(replace_view, vec![])(&converted_args)
                }
                other => exec_err!(
                    "Unsupported coercion data type {other:?} for function replace"
                ),
            }
        } else {
            exec_err!(
                "Unsupported data type {}, {:?}, {:?} for function replace.",
                data_types[0],
                data_types[1],
                data_types[2]
            )
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn replace_view(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_string_view_array(&args[0])?;
    let from_array = as_string_view_array(&args[1])?;
    let to_array = as_string_view_array(&args[2])?;

    replace_arrays::<_, i32>(string_array, from_array, to_array)
}

/// Replaces all occurrences in string of substring from with substring to.
/// replace('abcdefabcdef', 'cd', 'XX') = 'abXXefabXXef'
fn replace<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let from_array = as_generic_string_array::<T>(&args[1])?;
    let to_array = as_generic_string_array::<T>(&args[2])?;

    replace_arrays::<_, T>(string_array, from_array, to_array)
}

fn replace_arrays<'a, S, O>(
    string_array: S,
    from_array: S,
    to_array: S,
) -> Result<ArrayRef>
where
    S: StringArrayType<'a> + Copy,
    O: OffsetSizeTrait,
{
    let len = string_array.len();
    let nulls = NullBuffer::union_many([
        string_array.nulls(),
        from_array.nulls(),
        to_array.nulls(),
    ]);
    build_replaced::<O>(len, nulls, |builder, i| {
        // SAFETY: build_replaced only calls this for rows that are non-null in
        // the union buffer, so every input array is non-null at i.
        let string = unsafe { string_array.value_unchecked(i) };
        let from = unsafe { from_array.value_unchecked(i) };
        let to = unsafe { to_array.value_unchecked(i) };
        apply_replace(builder, string, from, to, None)
    })
}

/// Appends `len` rows to a fresh string builder: a null placeholder for each
/// null row and `append_row` for each non-null row. The `nulls.is_some()` check
/// is hoisted out of the loop so the all-non-null case does not depend on LLVM
/// loop-unswitching heuristics.
fn build_replaced<O: OffsetSizeTrait>(
    len: usize,
    nulls: Option<NullBuffer>,
    mut append_row: impl FnMut(&mut GenericStringArrayBuilder<O>, usize) -> Result<()>,
) -> Result<ArrayRef> {
    let mut builder = GenericStringArrayBuilder::<O>::with_capacity(len, 0);
    if let Some(nulls_ref) = nulls.as_ref() {
        for i in 0..len {
            if nulls_ref.is_null(i) {
                builder.try_append_placeholder()?;
            } else {
                append_row(&mut builder, i)?;
            }
        }
    } else {
        for i in 0..len {
            append_row(&mut builder, i)?;
        }
    }
    Ok(Arc::new(builder.finish(nulls)?) as ArrayRef)
}

#[inline]
fn apply_replace<O: OffsetSizeTrait>(
    builder: &mut GenericStringArrayBuilder<O>,
    string: &str,
    from: &str,
    to: &str,
    finder: Option<&memmem::Finder>,
) -> Result<()> {
    // Hot path: single ASCII byte → single ASCII byte. An ASCII byte (< 0x80)
    // cannot appear inside a multi-byte UTF-8 sequence, so any multi-byte
    // sequences in `string` pass through unchanged and output stays valid
    // UTF-8.
    if let (&[from_byte], &[to_byte]) = (from.as_bytes(), to.as_bytes())
        && from_byte.is_ascii()
        && to_byte.is_ascii()
    {
        // SAFETY: see the contract above.
        return unsafe {
            builder.try_append_byte_map(string.as_bytes(), |b| {
                if b == from_byte { to_byte } else { b }
            })
        };
    }

    if from.is_empty() {
        // PostgreSQL returns the input unchanged when `from` is empty (#22253).
        return builder.try_append_value(string);
    }

    builder.try_append_with(|w| replace_into_writer(w, string, from, to, finder))
}

/// Writes `string` into `w` with every non-overlapping occurrence of `from`
/// replaced by `to`. When `finder` is `Some`, matches are located with the
/// pre-built finder (the scalar fast path, where `from` is constant across all
/// rows); otherwise `str::match_indices` builds a searcher per call.
///
/// Both `string` and `from` are valid UTF-8, and UTF-8 is self-synchronizing,
/// so a byte match of `from` can only start on a char boundary of `string`; the
/// slices below are therefore always valid.
#[inline]
fn replace_into_writer<W: StringWriter>(
    w: &mut W,
    string: &str,
    from: &str,
    to: &str,
    finder: Option<&memmem::Finder>,
) {
    match finder {
        Some(finder) => write_replaced(
            w,
            string,
            to,
            from.len(),
            finder.find_iter(string.as_bytes()),
        ),
        None => write_replaced(
            w,
            string,
            to,
            from.len(),
            string.match_indices(from).map(|(start, _)| start),
        ),
    }
}

/// Copies `string` into `w`, replacing the `from_len`-byte substring at each
/// byte offset yielded by `starts` with `to`. `starts` must be ascending and
/// non-overlapping, as produced by both `memmem::Finder::find_iter` and
/// `str::match_indices`.
#[inline]
fn write_replaced<W: StringWriter>(
    w: &mut W,
    string: &str,
    to: &str,
    from_len: usize,
    starts: impl Iterator<Item = usize>,
) {
    let mut last_end = 0;
    for start in starts {
        w.write_str(&string[last_end..start]);
        w.write_str(to);
        last_end = start + from_len;
    }
    w.write_str(&string[last_end..]);
}

/// Fast path for a `from`/`to` pair that is constant across all rows. The
/// substring finder is built once and reused for every haystack value, which
/// avoids the per-row searcher construction incurred by `str::match_indices`.
fn replace_scalar<'a, S, O>(haystack: S, from: &str, to: &str) -> Result<ArrayRef>
where
    S: StringArrayType<'a> + Copy,
    O: OffsetSizeTrait,
{
    // `from` and `to` are non-null scalars, so the output nulls are exactly the
    // haystack's nulls (matching the null union computed by the general path).
    let nulls = haystack.nulls().cloned();
    // Built once and reused for every row.
    let finder = memmem::Finder::new(from.as_bytes());
    build_replaced::<O>(haystack.len(), nulls, |builder, i| {
        // SAFETY: build_replaced only calls this for non-null rows.
        let string = unsafe { haystack.value_unchecked(i) };
        apply_replace(builder, string, from, to, Some(&finder))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::test_function;
    use arrow::array::Array;
    use arrow::array::LargeStringArray;
    use arrow::array::StringArray;
    use arrow::datatypes::DataType::{LargeUtf8, Utf8};
    use datafusion_common::ScalarValue;
    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ReplaceFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("aabbdqcbb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("bb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("ccc")))),
            ],
            Ok(Some("aacccdqcccc")),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            ReplaceFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from(
                    "aabbb"
                )))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("bbb")))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("cc")))),
            ],
            Ok(Some("aacc")),
            &str,
            LargeUtf8,
            LargeStringArray
        );

        test_function!(
            ReplaceFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "aabbbcw"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("bb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("cc")))),
            ],
            Ok(Some("aaccbcw")),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            ReplaceFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("abc")))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("x")))),
            ],
            Ok(Some("abc")),
            &str,
            LargeUtf8,
            LargeStringArray
        );

        Ok(())
    }

    /// The scalar-argument fast path must produce output that is bit-identical
    /// to the general (array-argument) path for every kind of pattern.
    #[test]
    fn scalar_fast_path_matches_general() {
        use arrow::array::{ArrayRef, StringViewArray};
        use arrow::datatypes::Field;
        use datafusion_common::config::ConfigOptions;
        use std::sync::Arc;

        let rows = vec![
            Some("hello world"),
            None,
            Some("aaaa"),
            Some(""),
            Some("a.b.c.d"),
            Some("úñîçödé abcúñ"),
            Some("mississippi"),
            Some("  double  spaces  "),
        ];
        // Covers byte-map (single ASCII → single ASCII), deletion (empty `to`),
        // empty `from`, multi-byte `to`, and multi-byte non-ASCII `from`.
        let cases = [
            (" ", "_"),
            ("a", "X"),
            ("ss", "Z"),
            ("", "Q"),
            ("a", "yy"),
            ("úñ", "A"),
            (".", ""),
            ("i", "II"),
        ];

        let invoke = |haystack: &ArrayRef,
                      from: ColumnarValue,
                      to: ColumnarValue|
         -> ArrayRef {
            let args = vec![ColumnarValue::Array(Arc::clone(haystack)), from, to];
            let arg_fields = args
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("a{i}"), a.data_type(), true).into())
                .collect();
            match ReplaceFunc::new()
                .invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields,
                    number_rows: haystack.len(),
                    return_field: Field::new("f", Utf8, true).into(),
                    config_options: Arc::new(ConfigOptions::default()),
                })
                .unwrap()
            {
                ColumnarValue::Array(a) => a,
                ColumnarValue::Scalar(s) => s.to_array_of_size(haystack.len()).unwrap(),
            }
        };

        for (from, to) in cases {
            let n = rows.len();
            for haystack in [
                Arc::new(StringArray::from(rows.clone())) as ArrayRef,
                Arc::new(StringViewArray::from(rows.clone())) as ArrayRef,
            ] {
                // scalar `from`/`to` -> new fast path
                let fast = invoke(
                    &haystack,
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(from.to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(to.to_string()))),
                );
                // array `from`/`to` -> general path
                let general = invoke(
                    &haystack,
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![from; n]))),
                    ColumnarValue::Array(Arc::new(StringArray::from(vec![to; n]))),
                );
                assert_eq!(
                    &fast,
                    &general,
                    "mismatch for from={from:?} to={to:?} on {:?}",
                    haystack.data_type()
                );
            }
        }
    }
}
