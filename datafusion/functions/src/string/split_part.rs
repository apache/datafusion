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

use crate::utils::utf8_to_str_type;
use arrow::array::{
    Array, ArrayRef, AsArray, ByteView, GenericStringBuilder, Int64Array,
    StringArrayType, StringLikeArrayBuilder, StringViewArray, StringViewBuilder,
    make_view, new_null_array,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_int64_array;
use datafusion_common::types::{NativeType, logical_int64, logical_string};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, TypeSignatureClass, Volatility,
};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_macros::user_doc;
use memchr::memmem;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Splits a string based on a specified delimiter and returns the substring in the specified position.",
    syntax_example = "split_part(str, delimiter, pos)",
    sql_example = r#"```sql
> select split_part('1.2.3.4.5', '.', 3);
+--------------------------------------------------+
| split_part(Utf8("1.2.3.4.5"),Utf8("."),Int64(3)) |
+--------------------------------------------------+
| 3                                                |
+--------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "delimiter", description = "String or character to split on."),
    argument(
        name = "pos",
        description = "Position of the part to return (counting from 1). Negative values count backward from the end of the string."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SplitPartFunc {
    signature: Signature,
}

impl Default for SplitPartFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SplitPartFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_int64()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Int64,
                    ),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SplitPartFunc {
    fn name(&self) -> &str {
        "split_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[0] == DataType::Utf8View {
            Ok(DataType::Utf8View)
        } else {
            utf8_to_str_type(&arg_types[0], "split_part")
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;

        // Fast path: array string, scalar delimiter and position.
        if let (
            ColumnarValue::Array(string_array),
            ColumnarValue::Scalar(delim_scalar),
            ColumnarValue::Scalar(pos_scalar),
        ) = (&args[0], &args[1], &args[2])
        {
            return split_part_scalar(string_array, delim_scalar, pos_scalar);
        }

        // First, determine if any of the arguments is an Array
        let len = args.iter().find_map(|arg| match arg {
            ColumnarValue::Array(a) => Some(a.len()),
            _ => None,
        });

        let inferred_length = len.unwrap_or(1);
        let is_scalar = len.is_none();

        // Convert all ColumnarValues to ArrayRefs
        let args = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(inferred_length),
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
            })
            .collect::<Result<Vec<_>>>()?;

        // Unpack the ArrayRefs from the arguments
        let n_array = as_int64_array(&args[2])?;

        // Dispatch on delimiter type for a given string array and builder.
        macro_rules! split_part_for_delimiter_type {
            ($str_arr:expr, $builder:expr) => {
                match args[1].data_type() {
                    DataType::Utf8View => split_part_impl(
                        $str_arr,
                        &args[1].as_string_view(),
                        n_array,
                        $builder,
                    ),
                    DataType::Utf8 => split_part_impl(
                        $str_arr,
                        &args[1].as_string::<i32>(),
                        n_array,
                        $builder,
                    ),
                    DataType::LargeUtf8 => split_part_impl(
                        $str_arr,
                        &args[1].as_string::<i64>(),
                        n_array,
                        $builder,
                    ),
                    other => {
                        exec_err!("Unsupported delimiter type {other:?} for split_part")
                    }
                }
            };
        }

        let result = match args[0].data_type() {
            DataType::Utf8View => split_part_for_delimiter_type!(
                &args[0].as_string_view(),
                StringViewBuilder::with_capacity(inferred_length)
            ),
            DataType::Utf8 => {
                let str_arr = &args[0].as_string::<i32>();
                // Conservative under-estimate for data capacity: split_part
                // output is typically much smaller than the input, so avoid
                // pre-allocating the full input data size.
                split_part_for_delimiter_type!(
                    str_arr,
                    GenericStringBuilder::<i32>::with_capacity(
                        inferred_length,
                        inferred_length,
                    )
                )
            }
            DataType::LargeUtf8 => {
                let str_arr = &args[0].as_string::<i64>();
                // Conservative under-estimate; see Utf8 comment above.
                split_part_for_delimiter_type!(
                    str_arr,
                    GenericStringBuilder::<i64>::with_capacity(
                        inferred_length,
                        inferred_length,
                    )
                )
            }
            other => exec_err!("Unsupported string type {other:?} for split_part"),
        };
        if is_scalar {
            // If all inputs are scalar, keep the output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Finds the `n`th (0-based) split part of `string` by `delimiter`.
#[inline]
fn split_nth<'a>(string: &'a str, delimiter: &str, n: usize) -> Option<&'a str> {
    if delimiter.len() == 1 {
        // A single-byte UTF-8 string is always ASCII, so we can safely cast
        // just the first byte to a character. `str::split(char)` internally
        // uses memchr::memchr and is notably faster than `str::split(&str)`,
        // even for a single character string.
        string.split(delimiter.as_bytes()[0] as char).nth(n)
    } else {
        string.split(delimiter).nth(n)
    }
}

/// Like `split_nth` but splits from the right (`n` is 0-based from the end).
#[inline]
fn rsplit_nth<'a>(string: &'a str, delimiter: &str, n: usize) -> Option<&'a str> {
    if delimiter.len() == 1 {
        // A single-byte UTF-8 string is always ASCII, so we can safely cast
        // just the first byte to a character. `str::rsplit(char)` internally
        // uses memchr::memrchr and is notably faster than `str::rsplit(&str)`,
        // even for a single character string.
        string.rsplit(delimiter.as_bytes()[0] as char).nth(n)
    } else {
        string.rsplit(delimiter).nth(n)
    }
}

/// Fast path for `split_part(array, scalar_delimiter, scalar_position)`.
fn split_part_scalar(
    string_array: &ArrayRef,
    delim_scalar: &ScalarValue,
    pos_scalar: &ScalarValue,
) -> Result<ColumnarValue> {
    // Empty input array → empty result.
    if string_array.is_empty() {
        return Ok(ColumnarValue::Array(new_null_array(
            string_array.data_type(),
            0,
        )));
    }

    let delimiter = delim_scalar.try_as_str().ok_or_else(|| {
        exec_datafusion_err!(
            "Unsupported delimiter type {:?} for split_part",
            delim_scalar.data_type()
        )
    })?;

    let position = match pos_scalar {
        ScalarValue::Int64(v) => *v,
        other => {
            return exec_err!(
                "Unsupported position type {:?} for split_part",
                other.data_type()
            );
        }
    };

    // Null delimiter or position → every row is null.
    let (Some(delimiter), Some(position)) = (delimiter, position) else {
        return Ok(ColumnarValue::Array(new_null_array(
            string_array.data_type(),
            string_array.len(),
        )));
    };

    if position == 0 {
        return exec_err!("field position must not be zero");
    }

    let result = match string_array.data_type() {
        DataType::Utf8View => {
            split_part_scalar_view(string_array.as_string_view(), delimiter, position)
        }
        DataType::Utf8 => {
            let arr = string_array.as_string::<i32>();
            // Conservative under-estimate for data capacity: split_part output
            // is typically much smaller than the input, so avoid pre-allocating
            // the full input data size.
            split_part_scalar_impl(
                arr,
                delimiter,
                position,
                GenericStringBuilder::<i32>::with_capacity(arr.len(), arr.len()),
            )
        }
        DataType::LargeUtf8 => {
            let arr = string_array.as_string::<i64>();
            // Conservative under-estimate; see Utf8 comment above.
            split_part_scalar_impl(
                arr,
                delimiter,
                position,
                GenericStringBuilder::<i64>::with_capacity(arr.len(), arr.len()),
            )
        }
        other => exec_err!("Unsupported string type {other:?} for split_part"),
    }?;

    Ok(ColumnarValue::Array(result))
}

/// Inner implementation for the scalar-delimiter, scalar-position fast path.
/// Constructing a `memmem::Finder` is somewhat expensive but it's a win when
/// done once and amortized over the entire batch.
fn split_part_scalar_impl<'a, S, B>(
    string_array: S,
    delimiter: &str,
    position: i64,
    builder: B,
) -> Result<ArrayRef>
where
    S: StringArrayType<'a> + Copy,
    B: StringLikeArrayBuilder,
{
    if delimiter.is_empty() {
        // PostgreSQL: empty delimiter treats input as a single field,
        // so only position 1 or -1 returns the input string.
        return if position == 1 || position == -1 {
            map_strings(string_array, builder, Some)
        } else {
            map_strings(string_array, builder, |_| None)
        };
    }

    let delim_bytes = delimiter.as_bytes();
    let delim_len = delimiter.len();

    if position > 0 {
        let idx: usize = (position - 1).try_into().map_err(|_| {
            exec_datafusion_err!(
                "split_part index {position} exceeds maximum supported value"
            )
        })?;
        let finder = memmem::Finder::new(delim_bytes);
        map_strings(string_array, builder, |s| {
            split_nth_finder(s, &finder, delim_len, idx)
        })
    } else {
        let idx: usize = (position.unsigned_abs() - 1).try_into().map_err(|_| {
            exec_datafusion_err!(
                "split_part index {position} exceeds minimum supported value"
            )
        })?;
        let finder_rev = memmem::FinderRev::new(delim_bytes);
        map_strings(string_array, builder, |s| {
            rsplit_nth_finder(s, &finder_rev, delim_len, idx)
        })
    }
}

/// Applies `f` to each non-null string in `string_array`, appending the
/// result (or `""` when `f` returns `None`) to `builder`.
#[inline]
fn map_strings<'a, S, B, F>(string_array: S, mut builder: B, f: F) -> Result<ArrayRef>
where
    S: StringArrayType<'a> + Copy,
    B: StringLikeArrayBuilder,
    F: Fn(&'a str) -> Option<&'a str>,
{
    for string in string_array.iter() {
        match string {
            Some(s) => builder.append_value(f(s).unwrap_or("")),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Finds the `n`th (0-based) split part using a pre-built `memmem::Finder`.
#[inline]
fn split_nth_finder<'a>(
    string: &'a str,
    finder: &memmem::Finder,
    delim_len: usize,
    n: usize,
) -> Option<&'a str> {
    let bytes = string.as_bytes();
    let mut start = 0;
    for _ in 0..n {
        match finder.find(&bytes[start..]) {
            Some(pos) => start += pos + delim_len,
            None => return None,
        }
    }
    match finder.find(&bytes[start..]) {
        Some(pos) => Some(&string[start..start + pos]),
        None => Some(&string[start..]),
    }
}

/// Like `split_nth_finder` but splits from the right (`n` is 0-based from
/// the end).
#[inline]
fn rsplit_nth_finder<'a>(
    string: &'a str,
    finder: &memmem::FinderRev,
    delim_len: usize,
    n: usize,
) -> Option<&'a str> {
    let bytes = string.as_bytes();
    let mut end = bytes.len();
    for _ in 0..n {
        match finder.rfind(&bytes[..end]) {
            Some(pos) => end = pos,
            None => return None,
        }
    }
    match finder.rfind(&bytes[..end]) {
        Some(pos) => Some(&string[pos + delim_len..end]),
        None => Some(&string[..end]),
    }
}

/// Zero-copy scalar fast path for `StringViewArray` inputs.
///
/// Instead of copying substring bytes into a new buffer, constructs
/// `StringView` entries that point back into the original array's data
/// buffers.
fn split_part_scalar_view(
    string_view_array: &StringViewArray,
    delimiter: &str,
    position: i64,
) -> Result<ArrayRef> {
    let len = string_view_array.len();
    let mut views_buf = Vec::with_capacity(len);
    let views = string_view_array.views();

    if delimiter.is_empty() {
        // PostgreSQL: empty delimiter treats input as a single field.
        let empty_view = make_view(b"", 0, 0);
        let return_input = position == 1 || position == -1;
        for i in 0..len {
            if string_view_array.is_null(i) {
                views_buf.push(0);
            } else if return_input {
                views_buf.push(views[i]);
            } else {
                views_buf.push(empty_view);
            }
        }
    } else if position > 0 {
        let idx: usize = (position - 1).try_into().map_err(|_| {
            exec_datafusion_err!(
                "split_part index {position} exceeds maximum supported value"
            )
        })?;
        let finder = memmem::Finder::new(delimiter.as_bytes());
        split_view_loop(string_view_array, views, &mut views_buf, |s| {
            split_nth_finder(s, &finder, delimiter.len(), idx)
        });
    } else {
        let idx: usize = (position.unsigned_abs() - 1).try_into().map_err(|_| {
            exec_datafusion_err!(
                "split_part index {position} exceeds minimum supported value"
            )
        })?;
        let finder_rev = memmem::FinderRev::new(delimiter.as_bytes());
        split_view_loop(string_view_array, views, &mut views_buf, |s| {
            rsplit_nth_finder(s, &finder_rev, delimiter.len(), idx)
        });
    }

    let views_buf = ScalarBuffer::from(views_buf);

    // Nulls pass through unchanged, so we can use the input's null array.
    let nulls = string_view_array.nulls().cloned();

    // Safety: each view is either copied unchanged from the input, or built
    // by `substr_view` from a substring that is a contiguous sub-range of the
    // original string value stored in the input's data buffers.
    unsafe {
        Ok(Arc::new(StringViewArray::new_unchecked(
            views_buf,
            string_view_array.data_buffers().to_vec(),
            nulls,
        )) as ArrayRef)
    }
}

/// Creates a `StringView` referencing a substring of an existing view's buffer.
/// For substrings ≤ 12 bytes, creates an inline view instead.
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

/// Applies `split_fn` to each non-null string and appends the resulting view to
/// `views_buf`.
#[inline(always)]
fn split_view_loop<F>(
    string_view_array: &StringViewArray,
    views: &[u128],
    views_buf: &mut Vec<u128>,
    split_fn: F,
) where
    F: Fn(&str) -> Option<&str>,
{
    let empty_view = make_view(b"", 0, 0);
    for (i, raw_view) in views.iter().enumerate() {
        if string_view_array.is_null(i) {
            views_buf.push(0);
            continue;
        }
        let string = string_view_array.value(i);
        match split_fn(string) {
            Some(substr) => {
                let start_offset = substr.as_ptr() as usize - string.as_ptr() as usize;
                views_buf.push(substr_view(raw_view, substr, start_offset as u32));
            }
            None => views_buf.push(empty_view),
        }
    }
}

fn split_part_impl<'a, StringArrType, DelimiterArrType, B>(
    string_array: &StringArrType,
    delimiter_array: &DelimiterArrType,
    n_array: &Int64Array,
    mut builder: B,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    DelimiterArrType: StringArrayType<'a>,
    B: StringLikeArrayBuilder,
{
    for ((string, delimiter), n) in string_array
        .iter()
        .zip(delimiter_array.iter())
        .zip(n_array.iter())
    {
        match (string, delimiter, n) {
            (Some(string), Some(delimiter), Some(n)) => {
                let result = match n.cmp(&0) {
                    std::cmp::Ordering::Greater => {
                        let idx: usize = (n - 1).try_into().map_err(|_| {
                            exec_datafusion_err!(
                                "split_part index {n} exceeds maximum supported value"
                            )
                        })?;
                        if delimiter.is_empty() {
                            // Match PostgreSQL's behavior: empty delimiter
                            // treats input as a single field, so only position
                            // 1 returns data.
                            (n == 1).then_some(string)
                        } else {
                            split_nth(string, delimiter, idx)
                        }
                    }
                    std::cmp::Ordering::Less => {
                        let idx: usize =
                            (n.unsigned_abs() - 1).try_into().map_err(|_| {
                                exec_datafusion_err!(
                                    "split_part index {n} exceeds minimum supported value"
                                )
                            })?;
                        if delimiter.is_empty() {
                            // Match PostgreSQL's behavior: empty delimiter
                            // treats input as a single field, so only position
                            // -1 returns data.
                            (n == -1).then_some(string)
                        } else {
                            rsplit_nth(string, delimiter, idx)
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        return exec_err!("field position must not be zero");
                    }
                };
                builder.append_value(result.unwrap_or(""));
            }
            _ => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, StringArray, StringViewArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::ScalarValue;
    use datafusion_common::{Result, exec_err};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::split_part::SplitPartFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("def")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            Ok(Some("ghi")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            exec_err!("field position must not be zero"),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MIN))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        // Edge cases with delimiters
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(",")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            Ok(Some("a")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(",")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            Ok(Some("a,b")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(" ")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            Ok(Some("a,b")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(" ")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );

        // Edge cases with delimiters with negative n
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            Ok(Some("a,b")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(" ")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            Ok(Some("a,b")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("a,b")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-2))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }

    #[test]
    fn test_split_part_stringview_sliced() -> Result<()> {
        use super::split_part_scalar_view;

        let strings: StringViewArray = vec![
            Some("skip_this.value"),
            Some("this_is_a_long_prefix.suffix"),
            Some("short.val"),
            Some("another_long_result.rest"),
            None,
        ]
        .into_iter()
        .collect();

        // Slice off the first element to get a non-zero offset array.
        let sliced = strings.slice(1, 4);
        let result = split_part_scalar_view(&sliced, ".", 1)?;
        let result = result.as_string_view();
        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), "this_is_a_long_prefix");
        assert_eq!(result.value(1), "short");
        assert_eq!(result.value(2), "another_long_result");
        assert!(result.is_null(3));

        Ok(())
    }
}
