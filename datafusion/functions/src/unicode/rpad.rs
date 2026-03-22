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
use std::fmt::Write;
use std::sync::Arc;

use DataType::{LargeUtf8, Utf8, Utf8View};
use arrow::array::{
    ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringArrayType, StringViewArray,
};
use arrow::datatypes::DataType;
use unicode_segmentation::UnicodeSegmentation;

use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::as_int64_array;
use datafusion_common::{Result, exec_err};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Pads the right side of a string with another string to a specified string length.",
    syntax_example = "rpad(str, n[, padding_str])",
    sql_example = r#"```sql
>  select rpad('datafusion', 20, '_-');
+-----------------------------------------------+
| rpad(Utf8("datafusion"),Int64(20),Utf8("_-")) |
+-----------------------------------------------+
| datafusion_-_-_-_-_-                          |
+-----------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "n",
        description = "String length to pad to. If the input string is longer than this length, it is truncated."
    ),
    argument(
        name = "padding_str",
        description = "String expression to pad with. Can be a constant, column, or function, and any combination of string operators. _Default is a space._"
    ),
    related_udf(name = "lpad")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RPadFunc {
    signature: Signature,
}

impl Default for RPadFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RPadFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Int64]),
                    Exact(vec![Utf8View, Int64, Utf8View]),
                    Exact(vec![Utf8View, Int64, Utf8]),
                    Exact(vec![Utf8View, Int64, LargeUtf8]),
                    Exact(vec![Utf8, Int64]),
                    Exact(vec![Utf8, Int64, Utf8View]),
                    Exact(vec![Utf8, Int64, Utf8]),
                    Exact(vec![Utf8, Int64, LargeUtf8]),
                    Exact(vec![LargeUtf8, Int64]),
                    Exact(vec![LargeUtf8, Int64, Utf8View]),
                    Exact(vec![LargeUtf8, Int64, Utf8]),
                    Exact(vec![LargeUtf8, Int64, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RPadFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "rpad")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;

        const MAX_SCALAR_TARGET_LEN: usize = 16384;

        // If target_len and fill (if specified) are constants, use the
        // scalar fast path.
        if let Some(target_len) = try_as_scalar_i64(&args[1]) {
            let target_len: usize = match usize::try_from(target_len) {
                Ok(n) if n <= i32::MAX as usize => n,
                Ok(n) => {
                    return exec_err!(
                        "rpad requested length {n} too large, maximum allowed length is {}",
                        i32::MAX
                    );
                }
                Err(_) => 0, // negative → 0
            };

            let fill_str = if args.len() == 3 {
                try_as_scalar_str(&args[2])
            } else {
                Some(" ")
            };

            // Skip the fast path for very large `target_len` values to avoid
            // consuming too much memory. Such large padding values are uncommon
            // in practice.
            if target_len <= MAX_SCALAR_TARGET_LEN
                && let Some(fill) = fill_str
            {
                let string_array = args[0].to_array_of_size(number_rows)?;
                let result = match string_array.data_type() {
                    Utf8View => rpad_scalar_args::<_, i32>(
                        string_array.as_string_view(),
                        target_len,
                        fill,
                    ),
                    Utf8 => rpad_scalar_args::<_, i32>(
                        string_array.as_string::<i32>(),
                        target_len,
                        fill,
                    ),
                    LargeUtf8 => rpad_scalar_args::<_, i64>(
                        string_array.as_string::<i64>(),
                        target_len,
                        fill,
                    ),
                    other => {
                        exec_err!("Unsupported data type {other:?} for function rpad")
                    }
                }?;
                return Ok(ColumnarValue::Array(result));
            }
        }

        match args[0].data_type() {
            Utf8 | Utf8View => make_scalar_function(rpad::<i32>, vec![])(&args),
            LargeUtf8 => make_scalar_function(rpad::<i64>, vec![])(&args),
            other => exec_err!("Unsupported data type {other:?} for function rpad"),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

use super::common::{try_as_scalar_i64, try_as_scalar_str};

/// Optimized rpad for constant target_len and fill arguments.
fn rpad_scalar_args<'a, V: StringArrayType<'a> + Copy, T: OffsetSizeTrait>(
    string_array: V,
    target_len: usize,
    fill: &str,
) -> Result<ArrayRef> {
    if string_array.is_ascii() && fill.is_ascii() {
        rpad_scalar_ascii::<V, T>(string_array, target_len, fill)
    } else {
        rpad_scalar_unicode::<V, T>(string_array, target_len, fill)
    }
}

fn rpad_scalar_ascii<'a, V: StringArrayType<'a> + Copy, T: OffsetSizeTrait>(
    string_array: V,
    target_len: usize,
    fill: &str,
) -> Result<ArrayRef> {
    // With a scalar `target_len` and `fill`, we can precompute a padding
    // buffer of `target_len` fill characters repeated cyclically.
    let padding_buf = if !fill.is_empty() {
        let mut buf = String::with_capacity(target_len);
        while buf.len() < target_len {
            let remaining = target_len - buf.len();
            if remaining >= fill.len() {
                buf.push_str(fill);
            } else {
                buf.push_str(&fill[..remaining]);
            }
        }
        buf
    } else {
        String::new()
    };

    // Each output row is exactly `target_len` ASCII bytes (string + padding).
    let data_capacity = string_array.len().saturating_mul(target_len);
    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), data_capacity);

    for maybe_string in string_array.iter() {
        match maybe_string {
            Some(string) => {
                let str_len = string.len();
                if target_len <= str_len {
                    builder.append_value(&string[..target_len]);
                } else if fill.is_empty() {
                    builder.append_value(string);
                } else {
                    let pad_needed = target_len - str_len;
                    builder.write_str(string)?;
                    builder.write_str(&padding_buf[..pad_needed])?;
                    builder.append_value("");
                }
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn rpad_scalar_unicode<'a, V: StringArrayType<'a> + Copy, T: OffsetSizeTrait>(
    string_array: V,
    target_len: usize,
    fill: &str,
) -> Result<ArrayRef> {
    let fill_chars: Vec<char> = fill.chars().collect();

    // With a scalar `target_len` and `fill`, we can precompute a padding buffer
    // of `target_len` fill characters repeated cyclically. Because Unicode
    // characters are variable-width, we build a byte-offset table to map from
    // character count to the corresponding byte position in the padding buffer.
    let (padding_buf, char_byte_offsets) = if !fill_chars.is_empty() {
        let mut buf = String::new();
        let mut offsets = Vec::with_capacity(target_len + 1);
        offsets.push(0usize);
        for i in 0..target_len {
            buf.push(fill_chars[i % fill_chars.len()]);
            offsets.push(buf.len());
        }
        (buf, offsets)
    } else {
        (String::new(), vec![0])
    };

    // Each output row is `target_len` chars; multiply by 4 (max UTF-8 bytes
    // per char) for an upper bound in bytes.
    let data_capacity = string_array.len().saturating_mul(target_len * 4);
    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), data_capacity);
    let mut graphemes_buf = Vec::new();

    for maybe_string in string_array.iter() {
        match maybe_string {
            Some(string) => {
                graphemes_buf.clear();
                graphemes_buf.extend(string.graphemes(true));

                if target_len < graphemes_buf.len() {
                    let end: usize =
                        graphemes_buf[..target_len].iter().map(|g| g.len()).sum();
                    builder.append_value(&string[..end]);
                } else if fill_chars.is_empty() {
                    builder.append_value(string);
                } else {
                    let pad_chars = target_len - graphemes_buf.len();
                    let pad_bytes = char_byte_offsets[pad_chars];
                    builder.write_str(string)?;
                    builder.write_str(&padding_buf[..pad_bytes])?;
                    builder.append_value("");
                }
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn rpad<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() <= 1 || args.len() > 3 {
        return exec_err!(
            "rpad was called with {} arguments. It requires at least 2 and at most 3.",
            args.len()
        );
    }

    let length_array = as_int64_array(&args[1])?;

    match (args.len(), args[0].data_type()) {
        (2, Utf8View) => rpad_impl::<&StringViewArray, &GenericStringArray<i32>, T>(
            &args[0].as_string_view(),
            length_array,
            None,
        ),
        (2, Utf8 | LargeUtf8) => rpad_impl::<
            &GenericStringArray<T>,
            &GenericStringArray<T>,
            T,
        >(&args[0].as_string::<T>(), length_array, None),
        (3, Utf8View) => rpad_with_replace::<&StringViewArray, T>(
            &args[0].as_string_view(),
            length_array,
            &args[2],
        ),
        (3, Utf8 | LargeUtf8) => rpad_with_replace::<&GenericStringArray<T>, T>(
            &args[0].as_string::<T>(),
            length_array,
            &args[2],
        ),
        (len, dt) => unreachable!("rpad: unexpected arg count ({len}) or type ({dt})"),
    }
}

fn rpad_with_replace<'a, V, T: OffsetSizeTrait>(
    string_array: &V,
    length_array: &Int64Array,
    fill_array: &'a ArrayRef,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
{
    match fill_array.data_type() {
        Utf8View => rpad_impl::<V, &StringViewArray, T>(
            string_array,
            length_array,
            Some(fill_array.as_string_view()),
        ),
        LargeUtf8 => rpad_impl::<V, &GenericStringArray<i64>, T>(
            string_array,
            length_array,
            Some(fill_array.as_string::<i64>()),
        ),
        Utf8 => rpad_impl::<V, &GenericStringArray<i32>, T>(
            string_array,
            length_array,
            Some(fill_array.as_string::<i32>()),
        ),
        other => {
            exec_err!("Unsupported data type {other:?} for function rpad")
        }
    }
}

fn rpad_impl<'a, V, V2, T>(
    string_array: &V,
    length_array: &Int64Array,
    fill_array: Option<V2>,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
    V2: StringArrayType<'a>,
    T: OffsetSizeTrait,
{
    let array = if let Some(fill_array) = fill_array {
        let mut builder: GenericStringBuilder<T> = GenericStringBuilder::new();
        let mut graphemes_buf = Vec::new();
        let mut fill_chars_buf = Vec::new();

        for ((string, target_len), fill) in string_array
            .iter()
            .zip(length_array.iter())
            .zip(fill_array.iter())
        {
            if let (Some(string), Some(target_len), Some(fill)) =
                (string, target_len, fill)
            {
                if target_len > i32::MAX as i64 {
                    return exec_err!(
                        "rpad requested length {target_len} too large, maximum allowed length is {}",
                        i32::MAX
                    );
                }

                let target_len = if target_len < 0 {
                    0
                } else {
                    target_len as usize
                };
                if target_len == 0 {
                    builder.append_value("");
                    continue;
                }

                if string.is_ascii() && fill.is_ascii() {
                    // ASCII fast path: byte length == character length,
                    // so we skip expensive grapheme segmentation.
                    let str_len = string.len();
                    if target_len < str_len {
                        builder.append_value(&string[..target_len]);
                    } else if fill.is_empty() {
                        builder.append_value(string);
                    } else {
                        let pad_len = target_len - str_len;
                        let fill_len = fill.len();
                        let full_reps = pad_len / fill_len;
                        let remainder = pad_len % fill_len;
                        builder.write_str(string)?;
                        for _ in 0..full_reps {
                            builder.write_str(fill)?;
                        }
                        if remainder > 0 {
                            builder.write_str(&fill[..remainder])?;
                        }
                        builder.append_value("");
                    }
                } else {
                    graphemes_buf.clear();
                    graphemes_buf.extend(string.graphemes(true));

                    fill_chars_buf.clear();
                    fill_chars_buf.extend(fill.chars());

                    if target_len < graphemes_buf.len() {
                        let end: usize =
                            graphemes_buf[..target_len].iter().map(|g| g.len()).sum();
                        builder.append_value(&string[..end]);
                    } else if fill_chars_buf.is_empty() {
                        builder.append_value(string);
                    } else {
                        builder.write_str(string)?;
                        for l in 0..target_len - graphemes_buf.len() {
                            let c =
                                *fill_chars_buf.get(l % fill_chars_buf.len()).unwrap();
                            builder.write_char(c)?;
                        }
                        builder.append_value("");
                    }
                }
            } else {
                builder.append_null();
            }
        }

        builder.finish()
    } else {
        let mut builder: GenericStringBuilder<T> = GenericStringBuilder::new();
        let mut graphemes_buf = Vec::new();

        for (string, target_len) in string_array.iter().zip(length_array.iter()) {
            if let (Some(string), Some(target_len)) = (string, target_len) {
                if target_len > i32::MAX as i64 {
                    return exec_err!(
                        "rpad requested length {target_len} too large, maximum allowed length is {}",
                        i32::MAX
                    );
                }

                let target_len = if target_len < 0 {
                    0
                } else {
                    target_len as usize
                };
                if target_len == 0 {
                    builder.append_value("");
                    continue;
                }

                if string.is_ascii() {
                    // ASCII fast path: byte length == character length
                    let str_len = string.len();
                    if target_len < str_len {
                        builder.append_value(&string[..target_len]);
                    } else {
                        builder.write_str(string)?;
                        for _ in 0..(target_len - str_len) {
                            builder.write_str(" ")?;
                        }
                        builder.append_value("");
                    }
                } else {
                    graphemes_buf.clear();
                    graphemes_buf.extend(string.graphemes(true));

                    if target_len < graphemes_buf.len() {
                        let end: usize =
                            graphemes_buf[..target_len].iter().map(|g| g.len()).sum();
                        builder.append_value(&string[..end]);
                    } else {
                        builder.write_str(string)?;
                        for _ in 0..(target_len - graphemes_buf.len()) {
                            builder.write_str(" ")?;
                        }
                        builder.append_value("");
                    }
                }
            } else {
                builder.append_null();
            }
        }

        builder.finish()
    };

    Ok(Arc::new(array) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::rpad::RPadFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("josé")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("josé ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from("xy")),
            ],
            Ok(Some("hixyx")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(21i64)),
                ColumnarValue::Scalar(ScalarValue::from("abcdef")),
            ],
            Ok(Some("hiabcdefabcdefabcdefa")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from(" ")),
            ],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from("")),
            ],
            Ok(Some("hi")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from("xy")),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
                ColumnarValue::Scalar(ScalarValue::from("xy")),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hello")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("he")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(6i64)),
                ColumnarValue::Scalar(ScalarValue::from("xy")),
            ],
            Ok(Some("hixyxy")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("josé")),
                ColumnarValue::Scalar(ScalarValue::from(10i64)),
                ColumnarValue::Scalar(ScalarValue::from("xy")),
            ],
            Ok(Some("joséxyxyxy")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("josé")),
                ColumnarValue::Scalar(ScalarValue::from(10i64)),
                ColumnarValue::Scalar(ScalarValue::from("éñ")),
            ],
            Ok(Some("josééñéñéñ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("josé")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            internal_err!(
                "function rpad requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
