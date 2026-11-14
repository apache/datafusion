// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, OffsetSizeTrait, StringArrayType};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow_select::dictionary::garbage_collect_any_dictionary;
use memchr::memmem;

use crate::strings::{GenericStringArrayBuilder, StringWriter};
use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::utils::map_dictionary_values;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, EncodingPreservation, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
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
        let string = Coercion::new_exact(TypeSignatureClass::Native(logical_string()));
        Self {
            signature: Signature::coercible(
                vec![
                    string
                        .clone()
                        .with_encoding_preservation(EncodingPreservation::dictionary()),
                    string.clone(),
                    string,
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
        let value_type = replace_coercion_type(arg_types)?;
        match &arg_types[0] {
            DataType::Dictionary(key_type, _) => {
                Ok(DataType::Dictionary(key_type.clone(), Box::new(value_type)))
            }
            _ => Ok(value_type),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let mut output_type = self.return_type(&arg_types)?;

        // A row-varying pattern or replacement can produce a different value for
        // every row, so it cannot reuse the input dictionary's keys.
        if !args.scalar_arguments[1..].iter().all(|arg| arg.is_some())
            && let DataType::Dictionary(_, value_type) = output_type
        {
            output_type = *value_type;
        }

        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), output_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let data_types = args
            .args
            .iter()
            .map(|arg| arg.data_type())
            .collect::<Vec<_>>();
        let coercion_type = replace_coercion_type(&data_types)?;
        let preserve_dictionary =
            matches!(args.return_field.data_type(), DataType::Dictionary(_, _));

        let mut converted_args = Vec::with_capacity(args.args.len());
        for (index, arg) in args.args.iter().enumerate() {
            let target_type = if index == 0 && preserve_dictionary {
                match arg.data_type() {
                    DataType::Dictionary(key_type, _) => {
                        DataType::Dictionary(key_type, Box::new(coercion_type.clone()))
                    }
                    _ => coercion_type.clone(),
                }
            } else {
                coercion_type.clone()
            };
            if arg.data_type() == target_type {
                converted_args.push(arg.clone());
            } else {
                converted_args.push(arg.cast_to(&target_type, None)?);
            }
        }

        if preserve_dictionary
            && let ColumnarValue::Scalar(ScalarValue::Dictionary(key_type, value)) =
                &converted_args[0]
        {
            let mut inner = converted_args.clone();
            inner[0] = ColumnarValue::Scalar(value.as_ref().clone());
            return match invoke_replace(&inner, &coercion_type)? {
                ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Dictionary(key_type.clone(), Box::new(value)),
                )),
                ColumnarValue::Array(_) => {
                    exec_err!(
                        "replace unexpectedly returned an array for scalar arguments"
                    )
                }
            };
        }

        if preserve_dictionary
            && let ColumnarValue::Array(array) = &converted_args[0]
            && matches!(array.data_type(), DataType::Dictionary(_, _))
        {
            let compacted;
            let mut dictionary = array;
            if array.as_any_dictionary().values().len() > array.len() {
                // Slices and shared Parquet dictionaries can retain many values
                // that no visible row references. Remove those before applying
                // replacement to the dictionary values.
                compacted = garbage_collect_any_dictionary(array.as_any_dictionary())?;
                dictionary = &compacted;
            }

            let result = map_dictionary_values(dictionary, |values| {
                let inner = vec![
                    ColumnarValue::Array(Arc::clone(values)),
                    converted_args[1].clone(),
                    converted_args[2].clone(),
                ];
                invoke_replace(&inner, &coercion_type)?.into_array(values.len())
            })?;
            return Ok(ColumnarValue::Array(result));
        }

        invoke_replace(&converted_args, &coercion_type)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn replace_value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => value_type,
        other => other,
    }
}

fn replace_coercion_type(arg_types: &[DataType]) -> Result<DataType> {
    let [string, from, to] = arg_types else {
        return exec_err!("replace expects three arguments");
    };
    let string = replace_value_type(string);
    let from = replace_value_type(from);
    let to = replace_value_type(to);

    if let Some(coercion_data_type) = string_coercion(string, from)
        .and_then(|dt| string_coercion(&dt, to))
        .or_else(|| {
            binary_to_string_coercion(string, from)
                .and_then(|dt| binary_to_string_coercion(&dt, to))
        })
    {
        utf8_to_str_type(&coercion_data_type, "replace")
    } else {
        exec_err!(
            "Unsupported data types for replace. Expected Utf8, LargeUtf8 or Utf8View"
        )
    }
}

fn invoke_replace(
    converted_args: &[ColumnarValue],
    coercion_type: &DataType,
) -> Result<ColumnarValue> {
    // Fast path: when `from` and `to` are non-null scalars we can pre-build a
    // substring finder once and reuse it for every haystack row.
    if let (
        ColumnarValue::Array(haystack),
        ColumnarValue::Scalar(from),
        ColumnarValue::Scalar(to),
    ) = (&converted_args[0], &converted_args[1], &converted_args[2])
        && let (Some(Some(from)), Some(Some(to))) = (from.try_as_str(), to.try_as_str())
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
            DataType::Utf8View => {
                replace_scalar::<_, i32>(as_string_view_array(haystack)?, from, to)
            }
            other => {
                return exec_err!(
                    "Unsupported coercion data type {other:?} for function replace"
                );
            }
        };
        return result.map(ColumnarValue::Array);
    }

    match coercion_type {
        DataType::Utf8 => make_scalar_function(replace::<i32>, vec![])(converted_args),
        DataType::LargeUtf8 => {
            make_scalar_function(replace::<i64>, vec![])(converted_args)
        }
        DataType::Utf8View => make_scalar_function(replace_view, vec![])(converted_args),
        other => {
            exec_err!("Unsupported coercion data type {other:?} for function replace")
        }
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
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, LargeStringArray, StringArray,
    };
    use arrow::compute::cast;
    use arrow::datatypes::DataType::{LargeUtf8, Utf8};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarUDF;
    use datafusion_expr::type_coercion::functions::fields_with_udf;
    use std::sync::Arc;

    fn invoke(args: Vec<ColumnarValue>) -> Result<ColumnarValue> {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, arg)| Field::new(format!("a{i}"), arg.data_type(), true).into())
            .collect::<Vec<_>>();
        let scalar_arguments = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(value) => Some(value),
                ColumnarValue::Array(_) => None,
            })
            .collect::<Vec<_>>();
        let function = ReplaceFunc::new();
        let return_field = function.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        let number_rows = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        function.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }
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
                Arc::new(LargeStringArray::from(rows.clone())) as ArrayRef,
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

    #[test]
    fn scalar_arguments_preserve_dictionary() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("alpha ab"),
            None,
            Some("ab beta"),
        ]));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(2), None, Some(1)]),
            values,
        )?);
        let result = invoke(vec![
            ColumnarValue::Array(dictionary),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("ab".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("X".to_string()))),
        ])?
        .into_array(4)?;

        assert_eq!(
            result.data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(Utf8))
        );
        let decoded = cast(&result, &Utf8)?;
        assert_eq!(
            decoded.as_string::<i32>(),
            &StringArray::from(vec![Some("alpha X"), Some("X beta"), None, None,])
        );
        Ok(())
    }

    #[test]
    fn coercion_preserves_only_the_haystack_dictionary() -> Result<()> {
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(Utf8));
        let fields = vec![
            Field::new("haystack", dictionary_type.clone(), true).into(),
            Field::new("from", Utf8, false).into(),
            Field::new("to", Utf8, false).into(),
        ];
        let function = ScalarUDF::new_from_impl(ReplaceFunc::new());
        let coerced = fields_with_udf(&fields, &function)?;
        assert_eq!(coerced[0].data_type(), &dictionary_type);
        assert_eq!(coerced[1].data_type(), &Utf8);
        assert_eq!(coerced[2].data_type(), &Utf8);
        Ok(())
    }

    #[test]
    fn scalar_dictionary_is_preserved() -> Result<()> {
        let result = invoke(vec![
            ColumnarValue::Scalar(ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("alpha ab".to_string()))),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("ab".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("X".to_string()))),
        ])?;
        let ColumnarValue::Scalar(result) = result else {
            panic!("expected scalar result");
        };
        assert_eq!(
            result,
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("alpha X".to_string()))),
            )
        );
        Ok(())
    }

    #[test]
    fn array_arguments_decode_dictionary() -> Result<()> {
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter([
            Some("alpha ab"),
            Some("ab beta"),
            None,
            Some("alpha ab"),
        ]));
        let result = invoke(vec![
            ColumnarValue::Array(dictionary),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("ab"),
                Some("beta"),
                Some("ab"),
                Some("alpha"),
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("X"),
                Some("Y"),
                Some("Z"),
                Some("Q"),
            ]))),
        ])?
        .into_array(4)?;

        assert_eq!(result.data_type(), &Utf8);
        assert_eq!(
            result.as_string::<i32>(),
            &StringArray::from(vec![Some("alpha X"), Some("ab Y"), None, Some("Q ab"),])
        );
        Ok(())
    }

    #[test]
    fn oversized_dictionary_is_compacted_before_replace() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..1000).map(|i| format!("value-{i}-ab")),
        ));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            [Some(999), Some(42), None, Some(999)].into_iter().collect(),
            values,
        )?);
        let result = invoke(vec![
            ColumnarValue::Array(dictionary),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("ab".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("X".to_string()))),
        ])?
        .into_array(4)?;

        assert_eq!(result.as_any_dictionary().values().len(), 2);
        let decoded = cast(&result, &Utf8)?;
        assert_eq!(
            decoded.as_string::<i32>(),
            &StringArray::from(vec![
                Some("value-999-X"),
                Some("value-42-X"),
                None,
                Some("value-999-X"),
            ])
        );
        Ok(())
    }
}
