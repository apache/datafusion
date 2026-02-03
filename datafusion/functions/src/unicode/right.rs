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
use std::cmp::Ordering;
use std::sync::Arc;

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, ByteView, GenericStringArray, Int64Array,
    OffsetSizeTrait, StringViewArray, make_view,
};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBuffer, ScalarBuffer};
use datafusion_common::Result;
use datafusion_common::cast::{
    as_generic_string_array, as_int64_array, as_string_view_array,
};
use datafusion_common::exec_err;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns a specified number of characters from the right side of a string.",
    syntax_example = "right(str, n)",
    sql_example = r#"```sql
> select right('datafusion', 6);
+------------------------------------+
| right(Utf8("datafusion"),Int64(6)) |
+------------------------------------+
| fusion                             |
+------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "n", description = "Number of characters to return."),
    related_udf(name = "left")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RightFunc {
    signature: Signature,
}

impl Default for RightFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RightFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8View, Int64]),
                    Exact(vec![Utf8, Int64]),
                    Exact(vec![LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RightFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "right"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        match args[0].data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                make_scalar_function(right, vec![])(args)
            }
            other => exec_err!(
                "Unsupported data type {other:?} for function {},\
                expected Utf8View, Utf8 or LargeUtf8.",
                self.name()
            ),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns right n characters in the string, or when n is negative, returns all but first |n| characters.
/// right('abcde', 2) = 'de'
/// right('abcde', -2) = 'cde'
/// The implementation uses UTF-8 code points as characters
fn right(args: &[ArrayRef]) -> Result<ArrayRef> {
    let n_array = as_int64_array(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = as_generic_string_array::<i32>(&args[0])?;
            right_impl::<i32, _>(string_array, n_array)
        }
        DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<i64>(&args[0])?;
            right_impl::<i64, _>(string_array, n_array)
        }
        DataType::Utf8View => {
            let string_view_array = as_string_view_array(&args[0])?;
            right_impl_view(string_view_array, n_array)
        }
        _ => exec_err!("Not supported"),
    }
}

/// `right` implementation for strings
fn right_impl<'a, T: OffsetSizeTrait, V: ArrayAccessor<Item = &'a str>>(
    string_array: V,
    n_array: &Int64Array,
) -> Result<ArrayRef> {
    let iter = ArrayIter::new(string_array);
    let result = iter
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => {
                let byte_length = right_byte_length(string, n);
                // Extract starting from `byte_length` bytes from a byte-indexed slice
                Some(&string[byte_length..])
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// `right` implementation for StringViewArray
fn right_impl_view(
    string_view_array: &StringViewArray,
    n_array: &Int64Array,
) -> Result<ArrayRef> {
    let len = n_array.len();

    let views = string_view_array.views();
    // Every string in StringViewArray has one corresponding view in `views`
    debug_assert!(views.len() == string_view_array.len());

    // Compose null buffer at once
    let string_nulls = string_view_array.nulls();
    let n_nulls = n_array.nulls();
    let new_nulls = NullBuffer::union(string_nulls, n_nulls);

    let new_views = (0..len)
        .map(|idx| {
            let view = views[idx];

            let is_valid = match &new_nulls {
                Some(nulls_buf) => nulls_buf.is_valid(idx),
                None => true,
            };

            if is_valid {
                let string: &str = string_view_array.value(idx);
                let n = n_array.value(idx);

                let new_offset = right_byte_length(string, n);
                let result_bytes = &string.as_bytes()[new_offset..];

                if result_bytes.len() > 12 {
                    let byte_view = ByteView::from(view);
                    // Reuse buffer, but adjust offset and length
                    make_view(
                        result_bytes,
                        byte_view.buffer_index,
                        byte_view.offset + new_offset as u32,
                    )
                } else {
                    // inline value does not need block id or offset
                    make_view(result_bytes, 0, 0)
                }
            } else {
                // For nulls, keep the original view
                view
            }
        })
        .collect::<Vec<u128>>();

    // Buffers are unchanged
    let result = StringViewArray::try_new(
        ScalarBuffer::from(new_views),
        Vec::from(string_view_array.data_buffers()),
        new_nulls,
    )?;
    Ok(Arc::new(result) as ArrayRef)
}

/// Calculate the byte length of the substring of last `n` chars from string `string`
/// (or all but first `|n|` chars if n is negative)
fn right_byte_length(string: &str, n: i64) -> usize {
    match n.cmp(&0) {
        Ordering::Less => string
            .char_indices()
            .nth(n.unsigned_abs().min(usize::MAX as u64) as usize)
            .map(|(index, _)| index)
            .unwrap_or(string.len()),
        Ordering::Equal => string.len(),
        Ordering::Greater => string
            .char_indices()
            .nth_back((n.unsigned_abs().min(usize::MAX as u64) - 1) as usize)
            .map(|(index, _)| index)
            .unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::right::RightFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("de")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(200i64)),
            ],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(-2i64)),
            ],
            Ok(Some("cde")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(i64::MIN)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(-200i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("joséérend")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("érend")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("joséérend")),
                ColumnarValue::Scalar(ScalarValue::from(-3i64)),
            ],
            Ok(Some("éérend")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            internal_err!(
                "function right requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        // StringView cases
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("abcde".to_string()))),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("de")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("abcde".to_string()))),
                ColumnarValue::Scalar(ScalarValue::from(200i64)),
            ],
            Ok(Some("abcde")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("".to_string()))),
                ColumnarValue::Scalar(ScalarValue::from(200i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            RightFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                    "joséérend".to_string()
                ))),
                ColumnarValue::Scalar(ScalarValue::from(-3i64)),
            ],
            Ok(Some("éérend")),
            &str,
            Utf8View,
            StringViewArray
        );

        // Unicode indexing case
        let input = "joé楽s𐀀so↓j";
        for n in 1..=input.chars().count() {
            let expected = input.chars().skip(n).collect::<String>();
            test_function!(
                RightFunc::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::from(input)),
                    ColumnarValue::Scalar(ScalarValue::from(-(n as i64))),
                ],
                Ok(Some(expected.as_str())),
                &str,
                Utf8,
                StringArray
            );
        }

        Ok(())
    }
}
