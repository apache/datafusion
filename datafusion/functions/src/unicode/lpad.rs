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

use DataType::{LargeUtf8, Utf8, Utf8View};
use arrow::array::{
    ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringArrayType, StringViewArray,
};
use arrow::datatypes::DataType;
use unicode_segmentation::UnicodeSegmentation;

use crate::unicode::common::{StringArrayWriter, StringViewWriter};
use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::as_int64_array;
use datafusion_common::{Result, exec_err};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Pads the left side of a string with another string to a specified string length.",
    syntax_example = "lpad(str, n[, padding_str])",
    sql_example = r#"```sql
> select lpad('Dolly', 10, 'hello');
+---------------------------------------------+
| lpad(Utf8("Dolly"),Int64(10),Utf8("hello")) |
+---------------------------------------------+
| helloDolly                                  |
+---------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "n",
        description = "String length to pad to. If the input string is longer than this length, it is truncated (on the right)."
    ),
    argument(
        name = "padding_str",
        description = "Optional string expression to pad with. Can be a constant, column, or function, and any combination of string operators. _Default is a space._"
    ),
    related_udf(name = "rpad")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LPadFunc {
    signature: Signature,
}

impl Default for LPadFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LPadFunc {
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

impl ScalarUDFImpl for LPadFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "lpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types[0] == Utf8View {
            Ok(Utf8View)
        } else {
            utf8_to_str_type(&arg_types[0], "lpad")
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        match args[0].data_type() {
            Utf8 | Utf8View => make_scalar_function(lpad::<i32>, vec![])(args),
            LargeUtf8 => make_scalar_function(lpad::<i64>, vec![])(args),
            other => exec_err!("Unsupported data type {other:?} for function lpad"),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Extends the string to length 'length' by prepending the characters fill (a space by default).
/// If the string is already longer than length then it is truncated (on the right).
/// lpad('hi', 5, 'xy') = 'xyxhi'
fn lpad<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() <= 1 || args.len() > 3 {
        return exec_err!(
            "lpad was called with {} arguments. It requires at least 2 and at most 3.",
            args.len()
        );
    }

    let length_array = as_int64_array(&args[1])?;
    let len = args[0].len();
    // 32 bytes per value is a rough estimate for the output data capacity.
    let data_capacity = 32 * len;

    match (args.len(), args[0].data_type()) {
        (2, Utf8View) => lpad_impl(
            &args[0].as_string_view(),
            length_array,
            None::<&StringViewArray>,
            StringViewWriter::new(len),
        ),
        (2, Utf8 | LargeUtf8) => lpad_impl(
            &args[0].as_string::<T>(),
            length_array,
            None::<&GenericStringArray<T>>,
            GenericStringBuilder::<T>::with_capacity(len, data_capacity),
        ),
        (3, Utf8View) => lpad_with_replace(
            &args[0].as_string_view(),
            length_array,
            &args[2],
            StringViewWriter::new(len),
        ),
        (3, Utf8 | LargeUtf8) => lpad_with_replace(
            &args[0].as_string::<T>(),
            length_array,
            &args[2],
            GenericStringBuilder::<T>::with_capacity(len, data_capacity),
        ),
        (_, _) => unreachable!("lpad"),
    }
}

fn lpad_with_replace<'a, V, W>(
    string_array: &V,
    length_array: &Int64Array,
    fill_array: &'a ArrayRef,
    writer: W,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
    W: StringArrayWriter,
{
    match fill_array.data_type() {
        Utf8View => lpad_impl(
            string_array,
            length_array,
            Some(fill_array.as_string_view()),
            writer,
        ),
        LargeUtf8 => lpad_impl(
            string_array,
            length_array,
            Some(fill_array.as_string::<i64>()),
            writer,
        ),
        Utf8 => lpad_impl(
            string_array,
            length_array,
            Some(fill_array.as_string::<i32>()),
            writer,
        ),
        other => exec_err!("Unsupported data type {other:?} for function lpad"),
    }
}

fn lpad_impl<'a, V, V2, W>(
    string_array: &V,
    length_array: &Int64Array,
    fill_array: Option<V2>,
    mut writer: W,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
    V2: StringArrayType<'a>,
    W: StringArrayWriter,
{
    if let Some(fill_array) = fill_array {
        let mut graphemes_buf = Vec::new();
        let mut fill_chars_buf = Vec::new();

        for ((string, length), fill) in string_array
            .iter()
            .zip(length_array.iter())
            .zip(fill_array.iter())
        {
            if let (Some(string), Some(length), Some(fill)) = (string, length, fill) {
                if length > i32::MAX as i64 {
                    return exec_err!("lpad requested length {length} too large");
                }

                let length = if length < 0 { 0 } else { length as usize };
                if length == 0 {
                    // append empty string
                    writer.finalize();
                    continue;
                }

                if string.is_ascii() && fill.is_ascii() {
                    // ASCII fast path: byte length == character length,
                    // so we skip expensive grapheme segmentation.
                    let str_len = string.len();
                    if length < str_len {
                        writer.write_str(&string[..length])?;
                    } else if fill.is_empty() {
                        writer.write_str(string)?;
                    } else {
                        let pad_len = length - str_len;
                        let fill_len = fill.len();
                        let full_reps = pad_len / fill_len;
                        let remainder = pad_len % fill_len;
                        for _ in 0..full_reps {
                            writer.write_str(fill)?;
                        }
                        if remainder > 0 {
                            writer.write_str(&fill[..remainder])?;
                        }
                        writer.write_str(string)?;
                    }
                    writer.finalize();
                } else {
                    // Reuse buffers by clearing and refilling
                    graphemes_buf.clear();
                    graphemes_buf.extend(string.graphemes(true));

                    fill_chars_buf.clear();
                    fill_chars_buf.extend(fill.chars());

                    if length < graphemes_buf.len() {
                        writer.write_str(&graphemes_buf[..length].concat())?;
                    } else if fill_chars_buf.is_empty() {
                        writer.write_str(string)?;
                    } else {
                        for l in 0..length - graphemes_buf.len() {
                            writer
                                .write_char(fill_chars_buf[l % fill_chars_buf.len()])?;
                        }
                        writer.write_str(string)?;
                    }
                    writer.finalize();
                }
            } else {
                writer.append_null();
            }
        }
    } else {
        let mut graphemes_buf = Vec::new();

        for (string, length) in string_array.iter().zip(length_array.iter()) {
            if let (Some(string), Some(length)) = (string, length) {
                if length > i32::MAX as i64 {
                    return exec_err!("lpad requested length {length} too large");
                }

                let length = if length < 0 { 0 } else { length as usize };
                if length == 0 {
                    // append empty string
                    writer.finalize();
                    continue;
                }

                if string.is_ascii() {
                    // ASCII fast path: byte length == character length
                    let str_len = string.len();
                    if length < str_len {
                        writer.write_str(&string[..length])?;
                    } else {
                        for _ in 0..(length - str_len) {
                            writer.write_str(" ")?;
                        }
                        writer.write_str(string)?;
                    }
                    writer.finalize();
                } else {
                    // Reuse buffer by clearing and refilling
                    graphemes_buf.clear();
                    graphemes_buf.extend(string.graphemes(true));

                    if length < graphemes_buf.len() {
                        writer.write_str(&graphemes_buf[..length].concat())?;
                    } else {
                        for _ in 0..(length - graphemes_buf.len()) {
                            writer.write_str(" ")?;
                        }
                        writer.write_str(string)?;
                    }
                    writer.finalize();
                }
            } else {
                writer.append_null();
            }
        }
    }

    Ok(writer.finish())
}

#[cfg(test)]
mod tests {
    use crate::unicode::lpad::LPadFunc;
    use crate::utils::test::test_function;

    use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{LargeUtf8, Utf8, Utf8View};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    macro_rules! test_lpad {
        ($INPUT:expr, $LENGTH:expr, $EXPECTED:expr) => {
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8($INPUT)),
                    ColumnarValue::Scalar($LENGTH)
                ],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );

            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT)),
                    ColumnarValue::Scalar($LENGTH)
                ],
                $EXPECTED,
                &str,
                LargeUtf8,
                LargeStringArray
            );

            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT)),
                    ColumnarValue::Scalar($LENGTH)
                ],
                $EXPECTED,
                &str,
                Utf8View,
                StringViewArray
            );
        };

        ($INPUT:expr, $LENGTH:expr, $REPLACE:expr, $EXPECTED:expr) => {
            // utf8, utf8
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::Utf8($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );
            // utf8, largeutf8
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );
            // utf8, utf8view
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::Utf8View($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );

            // largeutf8, utf8
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::Utf8($REPLACE))
                ],
                $EXPECTED,
                &str,
                LargeUtf8,
                LargeStringArray
            );
            // largeutf8, largeutf8
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($REPLACE))
                ],
                $EXPECTED,
                &str,
                LargeUtf8,
                LargeStringArray
            );
            // largeutf8, utf8view
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::Utf8View($REPLACE))
                ],
                $EXPECTED,
                &str,
                LargeUtf8,
                LargeStringArray
            );

            // utf8view, utf8
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::Utf8($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8View,
                StringViewArray
            );
            // utf8view, largeutf8
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8View,
                StringViewArray
            );
            // utf8view, utf8view
            test_function!(
                LPadFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::Utf8View($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8View,
                StringViewArray
            );
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_lpad!(
            Some("josé".into()),
            ScalarValue::Int64(Some(5i64)),
            Ok(Some(" josé"))
        );
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(Some(5i64)),
            Ok(Some("   hi"))
        );
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(Some(0i64)),
            Ok(Some(""))
        );
        test_lpad!(Some("hi".into()), ScalarValue::Int64(None), Ok(None));
        test_lpad!(None, ScalarValue::Int64(Some(5i64)), Ok(None));
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(Some(5i64)),
            Some("xy".into()),
            Ok(Some("xyxhi"))
        );
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(Some(21i64)),
            Some("abcdef".into()),
            Ok(Some("abcdefabcdefabcdefahi"))
        );
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(Some(5i64)),
            Some(" ".into()),
            Ok(Some("   hi"))
        );
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(Some(5i64)),
            Some("".into()),
            Ok(Some("hi"))
        );
        test_lpad!(
            None,
            ScalarValue::Int64(Some(5i64)),
            Some("xy".into()),
            Ok(None)
        );
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(None),
            Some("xy".into()),
            Ok(None)
        );
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(Some(5i64)),
            None,
            Ok(None)
        );
        test_lpad!(
            Some("hello".into()),
            ScalarValue::Int64(Some(2i64)),
            Ok(Some("he"))
        );
        test_lpad!(
            Some("hi".into()),
            ScalarValue::Int64(Some(6i64)),
            Some("xy".into()),
            Ok(Some("xyxyhi"))
        );
        test_lpad!(
            Some("josé".into()),
            ScalarValue::Int64(Some(10i64)),
            Some("xy".into()),
            Ok(Some("xyxyxyjosé"))
        );
        test_lpad!(
            Some("josé".into()),
            ScalarValue::Int64(Some(10i64)),
            Some("éñ".into()),
            Ok(Some("éñéñéñjosé"))
        );

        #[cfg(not(feature = "unicode_expressions"))]
        test_lpad!(
            Some("josé".into()),
            ScalarValue::Int64(Some(5i64)),
            internal_err!(
                "function lpad requires compilation with feature flag: unicode_expressions."
            )
        );

        Ok(())
    }
}
