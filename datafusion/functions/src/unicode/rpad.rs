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

use crate::string::common::StringArrayType;
use crate::utils::{make_scalar_function, utf8_to_str_type};
use arrow::array::{
    ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, Int64Array,
    OffsetSizeTrait, StringViewArray,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_int64_array;
use datafusion_common::DataFusionError;
use datafusion_common::{exec_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::fmt::Write;
use std::sync::{Arc, OnceLock};
use unicode_segmentation::UnicodeSegmentation;
use DataType::{LargeUtf8, Utf8, Utf8View};

#[derive(Debug)]
pub struct RPadFunc {
    signature: Signature,
}

impl Default for RPadFunc {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_rpad_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description("Pads the right side of a string with another string to a specified string length.")
            .with_syntax_example("rpad(str, n[, padding_str])")
            .with_standard_argument(
                "str",
                "String",
            )
            .with_argument("n", "String length to pad to.")
            .with_argument("padding_str",
                           "String expression to pad with. Can be a constant, column, or function, and any combination of string operators. _Default is a space._")
            .with_related_udf("lpad")
            .build()
            .unwrap()
    })
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match (
            args.len(),
            args[0].data_type(),
            args.get(2).map(|arg| arg.data_type()),
        ) {
            (2, Utf8 | Utf8View, _) => {
                make_scalar_function(rpad::<i32, i32>, vec![])(args)
            }
            (2, LargeUtf8, _) => make_scalar_function(rpad::<i64, i64>, vec![])(args),
            (3, Utf8 | Utf8View, Some(Utf8 | Utf8View)) => {
                make_scalar_function(rpad::<i32, i32>, vec![])(args)
            }
            (3, LargeUtf8, Some(LargeUtf8)) => {
                make_scalar_function(rpad::<i64, i64>, vec![])(args)
            }
            (3, Utf8 | Utf8View, Some(LargeUtf8)) => {
                make_scalar_function(rpad::<i32, i64>, vec![])(args)
            }
            (3, LargeUtf8, Some(Utf8 | Utf8View)) => {
                make_scalar_function(rpad::<i64, i32>, vec![])(args)
            }
            (_, _, _) => {
                exec_err!("Unsupported combination of data types for function rpad")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_rpad_doc())
    }
}

pub fn rpad<StringArrayLen: OffsetSizeTrait, FillArrayLen: OffsetSizeTrait>(
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!(
            "rpad was called with {} arguments. It requires 2 or 3 arguments.",
            args.len()
        );
    }

    let length_array = as_int64_array(&args[1])?;
    match (
        args.len(),
        args[0].data_type(),
        args.get(2).map(|arg| arg.data_type()),
    ) {
        (2, Utf8View, _) => {
            rpad_impl::<&StringViewArray, &StringViewArray, StringArrayLen>(
                args[0].as_string_view(),
                length_array,
                None,
            )
        }
        (3, Utf8View, Some(Utf8View)) => {
            rpad_impl::<&StringViewArray, &StringViewArray, StringArrayLen>(
                args[0].as_string_view(),
                length_array,
                Some(args[2].as_string_view()),
            )
        }
        (3, Utf8View, Some(Utf8 | LargeUtf8)) => {
            rpad_impl::<&StringViewArray, &GenericStringArray<FillArrayLen>, StringArrayLen>(
                args[0].as_string_view(),
                length_array,
                Some(args[2].as_string::<FillArrayLen>()),
            )
        }
        (3, Utf8 | LargeUtf8, Some(Utf8View)) => rpad_impl::<
            &GenericStringArray<StringArrayLen>,
            &StringViewArray,
            StringArrayLen,
        >(
            args[0].as_string::<StringArrayLen>(),
            length_array,
            Some(args[2].as_string_view()),
        ),
        (_, _, _) => rpad_impl::<
            &GenericStringArray<StringArrayLen>,
            &GenericStringArray<FillArrayLen>,
            StringArrayLen,
        >(
            args[0].as_string::<StringArrayLen>(),
            length_array,
            args.get(2).map(|arg| arg.as_string::<FillArrayLen>()),
        ),
    }
}

/// Extends the string to length 'length' by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.
/// rpad('hi', 5, 'xy') = 'hixyx'
pub fn rpad_impl<'a, StringArrType, FillArrType, StringArrayLen>(
    string_array: StringArrType,
    length_array: &Int64Array,
    fill_array: Option<FillArrType>,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    FillArrType: StringArrayType<'a>,
    StringArrayLen: OffsetSizeTrait,
{
    let mut builder: GenericStringBuilder<StringArrayLen> = GenericStringBuilder::new();

    match fill_array {
        None => {
            string_array.iter().zip(length_array.iter()).try_for_each(
                |(string, length)| -> Result<(), DataFusionError> {
                    match (string, length) {
                        (Some(string), Some(length)) => {
                            if length > i32::MAX as i64 {
                                return exec_err!(
                                    "rpad requested length {} too large",
                                    length
                                );
                            }
                            let length = if length < 0 { 0 } else { length as usize };
                            if length == 0 {
                                builder.append_value("");
                            } else {
                                let graphemes =
                                    string.graphemes(true).collect::<Vec<&str>>();
                                if length < graphemes.len() {
                                    builder.append_value(graphemes[..length].concat());
                                } else {
                                    builder.write_str(string)?;
                                    builder.write_str(
                                        &" ".repeat(length - graphemes.len()),
                                    )?;
                                    builder.append_value("");
                                }
                            }
                        }
                        _ => builder.append_null(),
                    }
                    Ok(())
                },
            )?;
        }
        Some(fill_array) => {
            string_array
                .iter()
                .zip(length_array.iter())
                .zip(fill_array.iter())
                .try_for_each(
                    |((string, length), fill)| -> Result<(), DataFusionError> {
                        match (string, length, fill) {
                            (Some(string), Some(length), Some(fill)) => {
                                if length > i32::MAX as i64 {
                                    return exec_err!(
                                        "rpad requested length {} too large",
                                        length
                                    );
                                }
                                let length = if length < 0 { 0 } else { length as usize };
                                let graphemes =
                                    string.graphemes(true).collect::<Vec<&str>>();

                                if length < graphemes.len() {
                                    builder.append_value(graphemes[..length].concat());
                                } else if fill.is_empty() {
                                    builder.append_value(string);
                                } else {
                                    builder.write_str(string)?;
                                    fill.chars()
                                        .cycle()
                                        .take(length - graphemes.len())
                                        .for_each(|ch| builder.write_char(ch).unwrap());
                                    builder.append_value("");
                                }
                            }
                            _ => builder.append_null(),
                        }
                        Ok(())
                    },
                )?;
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
            &[
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
