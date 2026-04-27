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

use arrow::array::{
    Array, ArrayRef, AsArray, ByteView, GenericStringArray, GenericStringBuilder,
    OffsetSizeTrait, PrimitiveArray, StringArrayType, StringLikeArrayBuilder,
    StringViewArray, make_view, new_null_array,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, Int64Type};
use arrow_buffer::NullBuffer;

use crate::utils::make_scalar_function;
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, exec_err, utils::take_function_args,
};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use memchr::{memchr_iter, memmem, memrchr_iter};

#[user_doc(
    doc_section(label = "String Functions"),
    description = r#"Returns the substring from str before count occurrences of the delimiter delim.
If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
If count is negative, everything to the right of the final delimiter (counting from the right) is returned."#,
    syntax_example = "substr_index(str, delim, count)",
    sql_example = r#"```sql
> select substr_index('www.apache.org', '.', 1);
+---------------------------------------------------------+
| substr_index(Utf8("www.apache.org"),Utf8("."),Int64(1)) |
+---------------------------------------------------------+
| www                                                     |
+---------------------------------------------------------+
> select substr_index('www.apache.org', '.', -1);
+----------------------------------------------------------+
| substr_index(Utf8("www.apache.org"),Utf8("."),Int64(-1)) |
+----------------------------------------------------------+
| org                                                      |
+----------------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "delim",
        description = "The string to find in str to split str."
    ),
    argument(
        name = "count",
        description = "The number of times to search for the delimiter. Can be either a positive or negative number."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SubstrIndexFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SubstrIndexFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SubstrIndexFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Utf8View, Int64]),
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
    fn name(&self) -> &str {
        "substr_index"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        if let (
            ColumnarValue::Array(string_array),
            ColumnarValue::Scalar(delim_scalar),
            ColumnarValue::Scalar(count_scalar),
        ) = (&args[0], &args[1], &args[2])
        {
            return substr_index_scalar(string_array, delim_scalar, count_scalar);
        }

        make_scalar_function(substr_index, vec![])(&args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns the substring from str before count occurrences of the delimiter delim. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
/// SUBSTRING_INDEX('www.apache.org', '.', 1) = www
/// SUBSTRING_INDEX('www.apache.org', '.', 2) = www.apache
/// SUBSTRING_INDEX('www.apache.org', '.', -2) = apache.org
/// SUBSTRING_INDEX('www.apache.org', '.', -1) = org
fn substr_index(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [str, delim, count] = take_function_args("substr_index", args)?;

    match str.data_type() {
        DataType::Utf8 => {
            let string_array = str.as_string::<i32>();
            let delimiter_array = delim.as_string::<i32>();
            let count_array: &PrimitiveArray<Int64Type> = count.as_primitive();
            substr_index_general(
                string_array,
                delimiter_array,
                count_array,
                GenericStringBuilder::<i32>::with_capacity(
                    string_array.len(),
                    visible_string_bytes(string_array),
                ),
            )
        }
        DataType::LargeUtf8 => {
            let string_array = str.as_string::<i64>();
            let delimiter_array = delim.as_string::<i64>();
            let count_array: &PrimitiveArray<Int64Type> = count.as_primitive();
            substr_index_general(
                string_array,
                delimiter_array,
                count_array,
                GenericStringBuilder::<i64>::with_capacity(
                    string_array.len(),
                    visible_string_bytes(string_array),
                ),
            )
        }
        DataType::Utf8View => {
            let string_array = str.as_string_view();
            let delimiter_array = delim.as_string_view();
            let count_array: &PrimitiveArray<Int64Type> = count.as_primitive();
            substr_index_view(string_array, delimiter_array, count_array)
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function substr_index")
        }
    }
}

fn substr_index_scalar(
    string_array: &ArrayRef,
    delim_scalar: &ScalarValue,
    count_scalar: &ScalarValue,
) -> Result<ColumnarValue> {
    if string_array.is_empty() {
        return Ok(ColumnarValue::Array(new_null_array(
            string_array.data_type(),
            0,
        )));
    }

    let delimiter = delim_scalar.try_as_str().ok_or_else(|| {
        exec_datafusion_err!(
            "Unsupported delimiter type {:?} for substr_index",
            delim_scalar.data_type()
        )
    })?;

    let count = match count_scalar {
        ScalarValue::Int64(v) => *v,
        other => {
            return exec_err!(
                "Unsupported count type {:?} for substr_index",
                other.data_type()
            );
        }
    };

    let (Some(delimiter), Some(count)) = (delimiter, count) else {
        return Ok(ColumnarValue::Array(new_null_array(
            string_array.data_type(),
            string_array.len(),
        )));
    };

    let result = match string_array.data_type() {
        DataType::Utf8View => {
            substr_index_scalar_view(string_array.as_string_view(), delimiter, count)
        }
        DataType::Utf8 => {
            let arr = string_array.as_string::<i32>();
            substr_index_scalar_impl(
                arr,
                delimiter,
                count,
                GenericStringBuilder::<i32>::with_capacity(
                    arr.len(),
                    visible_string_bytes(arr),
                ),
            )
        }
        DataType::LargeUtf8 => {
            let arr = string_array.as_string::<i64>();
            substr_index_scalar_impl(
                arr,
                delimiter,
                count,
                GenericStringBuilder::<i64>::with_capacity(
                    arr.len(),
                    visible_string_bytes(arr),
                ),
            )
        }
        other => exec_err!("Unsupported string type {other:?} for substr_index"),
    }?;

    Ok(ColumnarValue::Array(result))
}

#[inline]
fn visible_string_bytes<T: OffsetSizeTrait>(
    string_array: &GenericStringArray<T>,
) -> usize {
    let offsets = string_array.value_offsets();
    offsets[offsets.len() - 1].as_usize() - offsets[0].as_usize()
}

fn substr_index_general<'a, S, B>(
    string_array: S,
    delimiter_array: S,
    count_array: &PrimitiveArray<Int64Type>,
    mut builder: B,
) -> Result<ArrayRef>
where
    S: StringArrayType<'a> + Copy,
    B: StringLikeArrayBuilder,
{
    for ((string, delimiter), n) in string_array
        .iter()
        .zip(delimiter_array.iter())
        .zip(count_array.iter())
    {
        match (string, delimiter, n) {
            (Some(string), Some(delimiter), Some(n)) => {
                builder.append_value(substr_index_slice(string, delimiter, n));
            }
            _ => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn substr_index_view(
    string_array: &StringViewArray,
    delimiter_array: &StringViewArray,
    count_array: &PrimitiveArray<Int64Type>,
) -> Result<ArrayRef> {
    let nulls = NullBuffer::union(
        NullBuffer::union(string_array.nulls(), delimiter_array.nulls()).as_ref(),
        count_array.nulls(),
    );
    let views = string_array.views();
    let mut views_buf = Vec::with_capacity(string_array.len());
    let mut has_out_of_line = false;

    for (i, raw_view) in views.iter().enumerate() {
        if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
            views_buf.push(0);
            continue;
        }

        let string = string_array.value(i);
        let delimiter = delimiter_array.value(i);
        let count = count_array.value(i);
        let substr = substr_index_slice(string, delimiter, count);
        has_out_of_line |= append_substr_view(&mut views_buf, raw_view, string, substr);
    }

    let data_buffers = if has_out_of_line {
        string_array.data_buffers().to_vec()
    } else {
        vec![]
    };

    // Safety: each appended view is either:
    // (1) a copied null sentinel,
    // (2) the original valid input view, or
    // (3) built by `append_view` for a contiguous substring of the input row.
    unsafe {
        Ok(Arc::new(StringViewArray::new_unchecked(
            ScalarBuffer::from(views_buf),
            data_buffers,
            nulls,
        )) as ArrayRef)
    }
}

fn substr_index_scalar_impl<'a, S, B>(
    string_array: S,
    delimiter: &str,
    count: i64,
    builder: B,
) -> Result<ArrayRef>
where
    S: StringArrayType<'a> + Copy,
    B: StringLikeArrayBuilder,
{
    if count == 0 || delimiter.is_empty() {
        return map_strings(string_array, builder, |string| &string[..0]);
    }

    if delimiter.len() == 1 {
        let delimiter_byte = delimiter.as_bytes()[0];
        return map_strings(string_array, builder, |string| {
            substr_index_single_byte(string, delimiter_byte, count)
        });
    }

    let occurrence_idx = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX) - 1;
    if count > 0 {
        let finder = memmem::Finder::new(delimiter.as_bytes());
        map_strings(string_array, builder, |string| {
            substr_index_slice_finder(string, &finder, delimiter.len(), occurrence_idx)
        })
    } else {
        let finder_rev = memmem::FinderRev::new(delimiter.as_bytes());
        map_strings(string_array, builder, |string| {
            substr_index_rslice_finder(
                string,
                &finder_rev,
                delimiter.len(),
                occurrence_idx,
            )
        })
    }
}

fn substr_index_scalar_view(
    string_array: &StringViewArray,
    delimiter: &str,
    count: i64,
) -> Result<ArrayRef> {
    let views = string_array.views();
    let mut views_buf = Vec::with_capacity(string_array.len());
    let mut has_out_of_line = false;

    if count == 0 || delimiter.is_empty() {
        let empty_view = make_view(b"", 0, 0);
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                views_buf.push(0);
            } else {
                views_buf.push(empty_view);
            }
        }
    } else if delimiter.len() == 1 {
        let delimiter_byte = delimiter.as_bytes()[0];
        for (i, raw_view) in views.iter().enumerate() {
            if string_array.is_null(i) {
                views_buf.push(0);
                continue;
            }

            let string = string_array.value(i);
            let substr = substr_index_single_byte(string, delimiter_byte, count);
            has_out_of_line |=
                append_substr_view(&mut views_buf, raw_view, string, substr);
        }
    } else {
        let occurrence_idx =
            usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX) - 1;
        if count > 0 {
            let finder = memmem::Finder::new(delimiter.as_bytes());
            for (i, raw_view) in views.iter().enumerate() {
                if string_array.is_null(i) {
                    views_buf.push(0);
                    continue;
                }

                let string = string_array.value(i);
                let substr = substr_index_slice_finder(
                    string,
                    &finder,
                    delimiter.len(),
                    occurrence_idx,
                );
                has_out_of_line |=
                    append_substr_view(&mut views_buf, raw_view, string, substr);
            }
        } else {
            let finder_rev = memmem::FinderRev::new(delimiter.as_bytes());
            for (i, raw_view) in views.iter().enumerate() {
                if string_array.is_null(i) {
                    views_buf.push(0);
                    continue;
                }

                let string = string_array.value(i);
                let substr = substr_index_rslice_finder(
                    string,
                    &finder_rev,
                    delimiter.len(),
                    occurrence_idx,
                );
                has_out_of_line |=
                    append_substr_view(&mut views_buf, raw_view, string, substr);
            }
        }
    }

    let data_buffers = if has_out_of_line {
        string_array.data_buffers().to_vec()
    } else {
        vec![]
    };

    // Safety: each appended view is either:
    // (1) a copied null sentinel,
    // (2) the original valid input view,
    // (3) an inline empty string view, or
    // (4) built by `append_view` for a contiguous substring of the input row.
    unsafe {
        Ok(Arc::new(StringViewArray::new_unchecked(
            ScalarBuffer::from(views_buf),
            data_buffers,
            string_array.nulls().cloned(),
        )) as ArrayRef)
    }
}

fn map_strings<'a, S, B, F>(string_array: S, mut builder: B, f: F) -> Result<ArrayRef>
where
    S: StringArrayType<'a> + Copy,
    B: StringLikeArrayBuilder,
    F: Fn(&'a str) -> &'a str,
{
    for string in string_array.iter() {
        match string {
            Some(s) => builder.append_value(f(s)),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[inline]
fn substr_index_slice<'a>(string: &'a str, delimiter: &str, count: i64) -> &'a str {
    if count == 0 || string.is_empty() || delimiter.is_empty() {
        return &string[..0];
    }

    if delimiter.len() == 1 {
        return substr_index_single_byte(string, delimiter.as_bytes()[0], count);
    }

    let occurrences = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
    if count > 0 {
        string
            .match_indices(delimiter)
            .nth(occurrences - 1)
            .map(|(idx, _)| &string[..idx])
            .unwrap_or(string)
    } else {
        string
            .rmatch_indices(delimiter)
            .nth(occurrences - 1)
            .map(|(idx, _)| &string[idx + delimiter.len()..])
            .unwrap_or(string)
    }
}

#[inline]
fn substr_index_single_byte(string: &str, delimiter: u8, count: i64) -> &str {
    let occurrences = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
    let idx = if count > 0 {
        memchr_iter(delimiter, string.as_bytes()).nth(occurrences - 1)
    } else {
        memrchr_iter(delimiter, string.as_bytes())
            .nth(occurrences - 1)
            .map(|idx| idx + 1)
    };

    match idx {
        Some(idx) if count > 0 => &string[..idx],
        Some(idx) => &string[idx..],
        None => string,
    }
}

#[inline]
fn substr_index_slice_finder<'a>(
    string: &'a str,
    finder: &memmem::Finder,
    delimiter_len: usize,
    occurrence_idx: usize,
) -> &'a str {
    let bytes = string.as_bytes();
    let mut start = 0;
    for _ in 0..occurrence_idx {
        match finder.find(&bytes[start..]) {
            Some(pos) => start += pos + delimiter_len,
            None => return string,
        }
    }

    match finder.find(&bytes[start..]) {
        Some(pos) => &string[..start + pos],
        None => string,
    }
}

#[inline]
fn substr_index_rslice_finder<'a>(
    string: &'a str,
    finder: &memmem::FinderRev,
    delimiter_len: usize,
    occurrence_idx: usize,
) -> &'a str {
    let bytes = string.as_bytes();
    let mut end = bytes.len();
    for _ in 0..occurrence_idx {
        match finder.rfind(&bytes[..end]) {
            Some(pos) => end = pos,
            None => return string,
        }
    }

    match finder.rfind(&bytes[..end]) {
        Some(pos) => &string[pos + delimiter_len..],
        None => string,
    }
}

#[inline]
fn substr_view(original_view: &u128, substr: &str, start_offset: u32) -> u128 {
    if substr.len() > 12 {
        let view = ByteView::from(*original_view);
        make_view(
            substr.as_bytes(),
            view.buffer_index,
            view.offset + start_offset,
        )
    } else {
        make_view(substr.as_bytes(), 0, 0)
    }
}

#[inline]
fn append_substr_view(
    views_buf: &mut Vec<u128>,
    raw_view: &u128,
    string: &str,
    substr: &str,
) -> bool {
    if substr.len() == string.len() {
        views_buf.push(*raw_view);
        return substr.len() > 12;
    }

    if substr.is_empty() {
        views_buf.push(make_view(b"", 0, 0));
        return false;
    }

    let start_offset = substr.as_ptr() as usize - string.as_ptr() as usize;
    let start_offset =
        u32::try_from(start_offset).expect("string view offsets fit in u32");
    views_buf.push(substr_view(raw_view, substr, start_offset));
    substr.len() > 12
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, ArrayRef, AsArray, Int64Array, StringArray, StringViewArray,
    };
    use arrow::datatypes::DataType::{Utf8, Utf8View};
    use arrow::datatypes::{DataType, Field};

    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    use crate::unicode::substrindex::SubstrIndexFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("www")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("www.apache")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(-2i64)),
            ],
            Ok(Some("apache.org")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(-1i64)),
            ],
            Ok(Some("org")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("")),
                ColumnarValue::Scalar(ScalarValue::from(".")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("www.apache.org")),
                ColumnarValue::Scalar(ScalarValue::from("")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                    "verylongprefix.segment.tail".into(),
                ))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(".".into()))),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("verylongprefix")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrIndexFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                    "www.apache.org".into(),
                ))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(".".into()))),
                ColumnarValue::Scalar(ScalarValue::from(-1i64)),
            ],
            Ok(Some("org")),
            &str,
            Utf8View,
            StringViewArray
        );
        Ok(())
    }

    #[test]
    fn test_substr_index_utf8view_scalar_fast_path() -> Result<()> {
        let input = Arc::new(StringViewArray::from(vec![
            Some("alpha.beta.gamma"),
            Some("short.val"),
            None,
        ])) as ArrayRef;

        let arg_fields = vec![
            Field::new("a", Utf8View, true).into(),
            Field::new("b", Utf8View, true).into(),
            Field::new("c", DataType::Int64, true).into(),
        ];

        let args = ScalarFunctionArgs {
            number_rows: input.len(),
            args: vec![
                ColumnarValue::Array(Arc::clone(&input)),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(".".into()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            arg_fields,
            return_field: Field::new("f", Utf8View, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = match SubstrIndexFunc::new().invoke_with_args(args)? {
            ColumnarValue::Array(result) => result,
            other => panic!("expected array result, got {other:?}"),
        };
        let result = result.as_string_view();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "alpha");
        assert_eq!(result.value(1), "short");
        assert!(result.is_null(2));

        Ok(())
    }

    #[test]
    fn test_substr_index_utf8view_array_sliced() -> Result<()> {
        use super::substr_index_view;

        let strings: StringViewArray = vec![
            Some("skip_this.value"),
            Some("this_is_a_long_prefix.suffix"),
            Some("short.val"),
            Some("another_long_result.rest"),
            None,
        ]
        .into_iter()
        .collect();
        let delimiters: StringViewArray =
            vec![Some("."), Some("."), Some("."), Some("."), Some(".")]
                .into_iter()
                .collect();
        let counts = Int64Array::from(vec![1, 1, -1, 1, 1]);

        let sliced_strings = strings.slice(1, 4);
        let sliced_delimiters = delimiters.slice(1, 4);
        let sliced_counts = counts.slice(1, 4);

        let result =
            substr_index_view(&sliced_strings, &sliced_delimiters, &sliced_counts)?;
        let result = result.as_string_view();

        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), "this_is_a_long_prefix");
        assert_eq!(result.value(1), "val");
        assert_eq!(result.value(2), "another_long_result");
        assert!(result.is_null(3));

        Ok(())
    }

    #[test]
    fn test_substr_index_utf8view_scalar_reuses_original_view_when_unchanged()
    -> Result<()> {
        use super::substr_index_scalar_view;

        let strings: StringViewArray = vec![
            Some("very_long_value_without_separator"),
            Some("short"),
            None,
        ]
        .into_iter()
        .collect();

        let result = substr_index_scalar_view(&strings, ".", 1)?;
        let result = result.as_string_view();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), "very_long_value_without_separator");
        assert_eq!(result.value(1), "short");
        assert_eq!(result.views()[0], strings.views()[0]);
        assert_eq!(result.views()[1], strings.views()[1]);
        assert!(result.is_null(2));

        Ok(())
    }
}
