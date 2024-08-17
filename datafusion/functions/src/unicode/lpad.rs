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

use arrow::array::{
    Array, ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringViewArray,
};
use arrow::datatypes::DataType;
use unicode_segmentation::UnicodeSegmentation;
use DataType::{LargeUtf8, Utf8, Utf8View};

use datafusion_common::cast::as_int64_array;
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::string::common::StringArrayType;
use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
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
        utf8_to_str_type(&arg_types[0], "lpad")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            Utf8 | Utf8View => make_scalar_function(lpad::<i32>, vec![])(args),
            LargeUtf8 => make_scalar_function(lpad::<i64>, vec![])(args),
            other => exec_err!("Unsupported data type {other:?} for function lpad"),
        }
    }
}

/// Extends the string to length 'length' by prepending the characters fill (a space by default).
/// If the string is already longer than length then it is truncated (on the right).
/// lpad('hi', 5, 'xy') = 'xyxhi'
pub fn lpad<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() <= 1 || args.len() > 3 {
        return exec_err!(
            "lpad was called with {} arguments. It requires at least 2 and at most 3.",
            args.len()
        );
    }

    let length_array = as_int64_array(&args[1])?;

    match (args.len(), args[0].data_type()) {
        (2, Utf8View) => lpad_impl::<&StringViewArray, &GenericStringArray<i32>, T>(
            args[0].as_string_view(),
            length_array,
            None,
        ),
        (2, Utf8 | LargeUtf8) => lpad_impl::<
            &GenericStringArray<T>,
            &GenericStringArray<T>,
            T,
        >(args[0].as_string::<T>(), length_array, None),
        (3, Utf8View) => lpad_with_replace::<&StringViewArray, T>(
            args[0].as_string_view(),
            length_array,
            &args[2],
        ),
        (3, Utf8 | LargeUtf8) => lpad_with_replace::<&GenericStringArray<T>, T>(
            args[0].as_string::<T>(),
            length_array,
            &args[2],
        ),
        (_, _) => unreachable!(),
    }
}

fn lpad_with_replace<'a, V, T: OffsetSizeTrait>(
    string_array: V,
    length_array: &Int64Array,
    fill_array: &'a ArrayRef,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
{
    match fill_array.data_type() {
        Utf8View => lpad_impl::<V, &StringViewArray, T>(
            string_array,
            length_array,
            Some(fill_array.as_string_view()),
        ),
        LargeUtf8 => lpad_impl::<V, &GenericStringArray<i64>, T>(
            string_array,
            length_array,
            Some(fill_array.as_string::<i64>()),
        ),
        Utf8 => lpad_impl::<V, &GenericStringArray<i32>, T>(
            string_array,
            length_array,
            Some(fill_array.as_string::<i32>()),
        ),
        other => {
            exec_err!("Unsupported data type {other:?} for function lpad")
        }
    }
}

fn lpad_impl<'a, V, V2, T>(
    string_array: V,
    length_array: &Int64Array,
    fill_array: Option<V2>,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
    V2: StringArrayType<'a>,
    T: OffsetSizeTrait,
{
    let array = if fill_array.is_none() {
        let mut builder: GenericStringBuilder<T> = GenericStringBuilder::new();

        for (string, length) in string_array.iter().zip(length_array.iter()) {
            if let (Some(string), Some(length)) = (string, length) {
                if length > i32::MAX as i64 {
                    return exec_err!("lpad requested length {length} too large");
                }

                let length = if length < 0 { 0 } else { length as usize };
                if length == 0 {
                    builder.append_value("");
                    continue;
                }

                let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                if length < graphemes.len() {
                    builder.append_value(graphemes[..length].concat());
                } else {
                    builder.write_str(" ".repeat(length - graphemes.len()).as_str())?;
                    builder.write_str(string)?;
                    builder.append_value("");
                }
            } else {
                builder.append_null();
            }
        }

        builder.finish()
    } else {
        let mut builder: GenericStringBuilder<T> = GenericStringBuilder::new();

        for ((string, length), fill) in string_array
            .iter()
            .zip(length_array.iter())
            .zip(fill_array.unwrap().iter())
        {
            if let (Some(string), Some(length), Some(fill)) = (string, length, fill) {
                if length > i32::MAX as i64 {
                    return exec_err!("lpad requested length {length} too large");
                }

                let length = if length < 0 { 0 } else { length as usize };
                if length == 0 {
                    builder.append_value("");
                    continue;
                }

                let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                let fill_chars = fill.chars().collect::<Vec<char>>();

                if length < graphemes.len() {
                    builder.append_value(graphemes[..length].concat());
                } else if fill_chars.is_empty() {
                    builder.append_value(string);
                } else {
                    for l in 0..length - graphemes.len() {
                        let c = *fill_chars.get(l % fill_chars.len()).unwrap();
                        builder.write_char(c)?;
                    }
                    builder.write_str(string)?;
                    builder.append_value("");
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
    use crate::unicode::lpad::LPadFunc;
    use crate::utils::test::test_function;

    use arrow::array::{Array, LargeStringArray, StringArray};
    use arrow::datatypes::DataType::{LargeUtf8, Utf8};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    macro_rules! test_lpad {
        ($INPUT:expr, $LENGTH:expr, $EXPECTED:expr) => {
            test_function!(
                LPadFunc::new(),
                &[
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
                &[
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
                &[
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT)),
                    ColumnarValue::Scalar($LENGTH)
                ],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );
        };

        ($INPUT:expr, $LENGTH:expr, $REPLACE:expr, $EXPECTED:expr) => {
            // utf8, utf8
            test_function!(
                LPadFunc::new(),
                &[
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
                &[
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
                &[
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
                &[
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
                &[
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
                &[
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
                &[
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::Utf8($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );
            // utf8view, largeutf8
            test_function!(
                LPadFunc::new(),
                &[
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );
            // utf8view, utf8view
            test_function!(
                LPadFunc::new(),
                &[
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT)),
                    ColumnarValue::Scalar($LENGTH),
                    ColumnarValue::Scalar(ScalarValue::Utf8View($REPLACE))
                ],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
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
        test_lpad!(Some("josé".into()), ScalarValue::Int64(Some(5i64)), internal_err!(
                "function lpad requires compilation with feature flag: unicode_expressions."
        ));

        Ok(())
    }
}
