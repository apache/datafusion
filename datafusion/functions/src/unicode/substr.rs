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
use std::sync::{Arc, OnceLock};

use crate::strings::{make_and_append_view, StringArrayType};
use crate::utils::{make_scalar_function, utf8_to_str_type};
use arrow::array::{
    Array, ArrayIter, ArrayRef, AsArray, GenericStringArray, Int64Array, OffsetSizeTrait,
    StringViewArray,
};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, ScalarBuffer};
use datafusion_common::cast::as_int64_array;
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct SubstrFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SubstrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SubstrFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("substring")],
        }
    }
}

impl ScalarUDFImpl for SubstrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[0] == DataType::Utf8View {
            Ok(DataType::Utf8View)
        } else {
            utf8_to_str_type(&arg_types[0], "substr")
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(substr, vec![])(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return plan_err!(
                "The {} function requires 2 or 3 arguments, but got {}.",
                self.name(),
                arg_types.len()
            );
        }
        let first_data_type = match &arg_types[0] {
            DataType::Null => Ok(DataType::Utf8),
            DataType::LargeUtf8 | DataType::Utf8View | DataType::Utf8 => Ok(arg_types[0].clone()),
            DataType::Dictionary(key_type, value_type) => {
                if key_type.is_integer() {
                    match value_type.as_ref() {
                        DataType::Null => Ok(DataType::Utf8),
                        DataType::LargeUtf8 | DataType::Utf8View | DataType::Utf8 => Ok(*value_type.clone()),
                        _ => plan_err!(
                                "The first argument of the {} function can only be a string, but got {:?}.",
                                self.name(),
                                arg_types[0]
                        ),
                    }
                } else {
                    plan_err!(
                        "The first argument of the {} function can only be a string, but got {:?}.",
                        self.name(),
                        arg_types[0]
                    )
                }
            }
            _ => plan_err!(
                "The first argument of the {} function can only be a string, but got {:?}.",
                self.name(),
                arg_types[0]
            )
        }?;

        if ![DataType::Int64, DataType::Int32, DataType::Null].contains(&arg_types[1]) {
            return plan_err!(
                "The second argument of the {} function can only be an integer, but got {:?}.",
                self.name(),
                arg_types[1]
            );
        }

        if arg_types.len() == 3
            && ![DataType::Int64, DataType::Int32, DataType::Null].contains(&arg_types[2])
        {
            return plan_err!(
                "The third argument of the {} function can only be an integer, but got {:?}.",
                self.name(),
                arg_types[2]
            );
        }

        if arg_types.len() == 2 {
            Ok(vec![first_data_type.to_owned(), DataType::Int64])
        } else {
            Ok(vec![
                first_data_type.to_owned(),
                DataType::Int64,
                DataType::Int64,
            ])
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_substr_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_substr_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description("Extracts a substring of a specified number of characters from a specific starting position in a string.")
            .with_syntax_example("substr(str, start_pos[, length])")
            .with_sql_example(r#"```sql
> select substr('datafusion', 5, 3);
+----------------------------------------------+
| substr(Utf8("datafusion"),Int64(5),Int64(3)) |
+----------------------------------------------+
| fus                                          |
+----------------------------------------------+ 
```"#)
            .with_standard_argument("str", Some("String"))
            .with_argument("start_pos", "Character position to start the substring at. The first character in the string has a position of 1.")
            .with_argument("length", "Number of characters to extract. If not specified, returns the rest of the string after the start position.")
            .with_alternative_syntax("substring(str from start_pos for length)")
            .build()
            .unwrap()
    })
}

/// Extracts the substring of string starting at the start'th character, and extending for count characters if that is specified. (Same as substring(string from start for count).)
/// substr('alphabet', 3) = 'phabet'
/// substr('alphabet', 3, 2) = 'ph'
/// The implementation uses UTF-8 code points as characters
pub fn substr(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            string_substr::<_, i32>(string_array, &args[1..])
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            string_substr::<_, i64>(string_array, &args[1..])
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            string_view_substr(string_array, &args[1..])
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function substr,\
            expected Utf8View, Utf8 or LargeUtf8."
        ),
    }
}

// Convert the given `start` and `count` to valid byte indices within `input` string
//
// Input `start` and `count` are equivalent to PostgreSQL's `substr(s, start, count)`
// `start` is 1-based, if `count` is not provided count to the end of the string
// Input indices are character-based, and return values are byte indices
// The input bounds can be outside string bounds, this function will return
// the intersection between input bounds and valid string bounds
// `input_ascii_only` is used to optimize this function if `input` is ASCII-only
//
// * Example
// 'Hi🌏' in-mem (`[]` for one char, `x` for one byte): [x][x][xxxx]
// `get_true_start_end('Hi🌏', 1, None) -> (0, 6)`
// `get_true_start_end('Hi🌏', 1, 1) -> (0, 1)`
// `get_true_start_end('Hi🌏', -10, 2) -> (0, 0)`
fn get_true_start_end(
    input: &str,
    start: i64,
    count: Option<u64>,
    is_input_ascii_only: bool,
) -> (usize, usize) {
    let start = start.checked_sub(1).unwrap_or(start);

    let end = match count {
        Some(count) => start + count as i64,
        None => input.len() as i64,
    };
    let count_to_end = count.is_some();

    let start = start.clamp(0, input.len() as i64) as usize;
    let end = end.clamp(0, input.len() as i64) as usize;
    let count = end - start;

    // If input is ASCII-only, byte-based indices equals to char-based indices
    if is_input_ascii_only {
        return (start, end);
    }

    // Otherwise, calculate byte indices from char indices
    // Note this decoding is relatively expensive for this simple `substr` function,,
    // so the implementation attempts to decode in one pass (and caused the complexity)
    let (mut st, mut ed) = (input.len(), input.len());
    let mut start_counting = false;
    let mut cnt = 0;
    for (char_cnt, (byte_cnt, _)) in input.char_indices().enumerate() {
        if char_cnt == start {
            st = byte_cnt;
            if count_to_end {
                start_counting = true;
            } else {
                break;
            }
        }
        if start_counting {
            if cnt == count {
                ed = byte_cnt;
                break;
            }
            cnt += 1;
        }
    }
    (st, ed)
}

// String characters are variable length encoded in UTF-8, `substr()` function's
// arguments are character-based, converting them into byte-based indices
// requires expensive decoding.
// However, checking if a string is ASCII-only is relatively cheap.
// If strings are ASCII only, use byte-based indices instead.
//
// A common pattern to call `substr()` is taking a small prefix of a long
// string, such as `substr(long_str_with_1k_chars, 1, 32)`.
// In such case the overhead of ASCII-validation may not be worth it, so
// skip the validation for short prefix for now.
fn enable_ascii_fast_path<'a, V: StringArrayType<'a>>(
    string_array: &V,
    start: &Int64Array,
    count: Option<&Int64Array>,
) -> bool {
    let is_short_prefix = match count {
        Some(count) => {
            let short_prefix_threshold = 32.0;
            let n_sample = 10;

            // HACK: can be simplified if function has specialized
            // implementation for `ScalarValue` (implement without `make_scalar_function()`)
            let avg_prefix_len = start
                .iter()
                .zip(count.iter())
                .take(n_sample)
                .map(|(start, count)| {
                    let start = start.unwrap_or(0);
                    let count = count.unwrap_or(0);
                    // To get substring, need to decode from 0 to start+count instead of start to start+count
                    start + count
                })
                .sum::<i64>();

            avg_prefix_len as f64 / n_sample as f64 <= short_prefix_threshold
        }
        None => false,
    };

    if is_short_prefix {
        // Skip ASCII validation for short prefix
        false
    } else {
        string_array.is_ascii()
    }
}

// The decoding process refs the trait at: arrow/arrow-data/src/byte_view.rs:44
// From<u128> for ByteView
fn string_view_substr(
    string_view_array: &StringViewArray,
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let mut views_buf = Vec::with_capacity(string_view_array.len());
    let mut null_builder = NullBufferBuilder::new(string_view_array.len());

    let start_array = as_int64_array(&args[0])?;
    let count_array_opt = if args.len() == 2 {
        Some(as_int64_array(&args[1])?)
    } else {
        None
    };

    let enable_ascii_fast_path =
        enable_ascii_fast_path(&string_view_array, start_array, count_array_opt);

    // In either case of `substr(s, i)` or `substr(s, i, cnt)`
    // If any of input argument is `NULL`, the result is `NULL`
    match args.len() {
        1 => {
            for ((str_opt, raw_view), start_opt) in string_view_array
                .iter()
                .zip(string_view_array.views().iter())
                .zip(start_array.iter())
            {
                if let (Some(str), Some(start)) = (str_opt, start_opt) {
                    let (start, end) =
                        get_true_start_end(str, start, None, enable_ascii_fast_path);
                    let substr = &str[start..end];

                    make_and_append_view(
                        &mut views_buf,
                        &mut null_builder,
                        raw_view,
                        substr,
                        start as u32,
                    );
                } else {
                    null_builder.append_null();
                    views_buf.push(0);
                }
            }
        }
        2 => {
            let count_array = count_array_opt.unwrap();
            for (((str_opt, raw_view), start_opt), count_opt) in string_view_array
                .iter()
                .zip(string_view_array.views().iter())
                .zip(start_array.iter())
                .zip(count_array.iter())
            {
                if let (Some(str), Some(start), Some(count)) =
                    (str_opt, start_opt, count_opt)
                {
                    if count < 0 {
                        return exec_err!(
                            "negative substring length not allowed: substr(<str>, {start}, {count})"
                        );
                    } else {
                        if start == i64::MIN {
                            return exec_err!(
                                "negative overflow when calculating skip value"
                            );
                        }
                        let (start, end) = get_true_start_end(
                            str,
                            start,
                            Some(count as u64),
                            enable_ascii_fast_path,
                        );
                        let substr = &str[start..end];

                        make_and_append_view(
                            &mut views_buf,
                            &mut null_builder,
                            raw_view,
                            substr,
                            start as u32,
                        );
                    }
                } else {
                    null_builder.append_null();
                    views_buf.push(0);
                }
            }
        }
        other => {
            return exec_err!(
                "substr was called with {other} arguments. It requires 2 or 3."
            )
        }
    }

    let views_buf = ScalarBuffer::from(views_buf);
    let nulls_buf = null_builder.finish();

    // Safety:
    // (1) The blocks of the given views are all provided
    // (2) Each of the range `view.offset+start..end` of view in views_buf is within
    // the bounds of each of the blocks
    unsafe {
        let array = StringViewArray::new_unchecked(
            views_buf,
            string_view_array.data_buffers().to_vec(),
            nulls_buf,
        );
        Ok(Arc::new(array) as ArrayRef)
    }
}

fn string_substr<'a, V, T>(string_array: V, args: &[ArrayRef]) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
    T: OffsetSizeTrait,
{
    let start_array = as_int64_array(&args[0])?;
    let count_array_opt = if args.len() == 2 {
        Some(as_int64_array(&args[1])?)
    } else {
        None
    };

    let enable_ascii_fast_path =
        enable_ascii_fast_path(&string_array, start_array, count_array_opt);

    match args.len() {
        1 => {
            let iter = ArrayIter::new(string_array);

            let result = iter
                .zip(start_array.iter())
                .map(|(string, start)| match (string, start) {
                    (Some(string), Some(start)) => {
                        let (start, end) = get_true_start_end(
                            string,
                            start,
                            None,
                            enable_ascii_fast_path,
                        ); // start, end is byte-based
                        let substr = &string[start..end];
                        Some(substr.to_string())
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();
            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let iter = ArrayIter::new(string_array);
            let count_array = count_array_opt.unwrap();

            let result = iter
                .zip(start_array.iter())
                .zip(count_array.iter())
                .map(|((string, start), count)| {
                    match (string, start, count) {
                        (Some(string), Some(start), Some(count)) => {
                            if count < 0 {
                                exec_err!(
                                "negative substring length not allowed: substr(<str>, {start}, {count})"
                            )
                            } else {
                                if start == i64::MIN {
                                    return exec_err!("negative overflow when calculating skip value");
                                }
                                let (start, end) = get_true_start_end(
                                    string,
                                    start,
                                    Some(count as u64),
                                    enable_ascii_fast_path,
                                ); // start, end is byte-based
                                let substr = &string[start..end];
                                Ok(Some(substr.to_string()))
                            }
                        }
                        _ => Ok(None),
                    }
                })
                .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!("substr was called with {other} arguments. It requires 2 or 3.")
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};

    use datafusion_common::{exec_err, Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::substr::SubstrFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "this és longer than 12B"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some(" é")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "this is longer than 12B"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some(" is longer than 12B")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "joséésoj"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("ésoj")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("ph")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(20i64)),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("ésoj")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
            ],
            Ok(Some("joséésoj")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("lphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-3i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(30i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("ph")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(20i64)),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("alph")),
            &str,
            Utf8,
            StringArray
        );
        // starting from 5 (10 + -5)
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(10i64)),
            ],
            Ok(Some("alph")),
            &str,
            Utf8,
            StringArray
        );
        // starting from -1 (4 + -5)
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(4i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        // starting from 0 (5 + -5)
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
                ColumnarValue::Scalar(ScalarValue::from(20i64)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
                ColumnarValue::Scalar(ScalarValue::from(-1i64)),
            ],
            exec_err!("negative substring length not allowed: substr(<str>, 1, -1)"),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("és")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            internal_err!(
                "function substr requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abc")),
                ColumnarValue::Scalar(ScalarValue::from(-9223372036854775808i64)),
            ],
            Ok(Some("abc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("overflow")),
                ColumnarValue::Scalar(ScalarValue::from(-9223372036854775808i64)),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            exec_err!("negative overflow when calculating skip value"),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
