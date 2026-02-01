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
    OffsetSizeTrait, StringViewArray,
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
    description = "Returns a specified number of characters from the left side of a string.",
    syntax_example = "left(str, n)",
    sql_example = r#"```sql
> select left('datafusion', 4);
+-----------------------------------+
| left(Utf8("datafusion"),Int64(4)) |
+-----------------------------------+
| data                              |
+-----------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(name = "n", description = "Number of characters to return."),
    related_udf(name = "right")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LeftFunc {
    signature: Signature,
}

impl Default for LeftFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LeftFunc {
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

impl ScalarUDFImpl for LeftFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "left"
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
                make_scalar_function(left, vec![])(args)
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

/// Returns first n characters in the string, or when n is negative, returns all but last |n| characters.
/// left('abcde', 2) = 'ab'
/// left('abcde', -2) = 'ab'
/// The implementation uses UTF-8 code points as characters
fn left(args: &[ArrayRef]) -> Result<ArrayRef> {
    let n_array = as_int64_array(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = as_generic_string_array::<i32>(&args[0])?;
            left_impl::<i32, _>(string_array, n_array)
        }
        DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<i64>(&args[0])?;
            left_impl::<i64, _>(string_array, n_array)
        }
        DataType::Utf8View => {
            let string_view_array = as_string_view_array(&args[0])?;
            left_impl_view(string_view_array, n_array)
        }
        _ => exec_err!("Not supported"),
    }
}

/// `left` implementation for strings
fn left_impl<'a, T: OffsetSizeTrait, V: ArrayAccessor<Item = &'a str>>(
    string_array: V,
    n_array: &Int64Array,
) -> Result<ArrayRef> {
    let iter = ArrayIter::new(string_array);
    let result = iter
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => {
                let byte_length = left_byte_length(string, n);
                // Extract first `byte_length` bytes from a byte-indexed slice
                Some(&string[0..byte_length])
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// `left` implementation for StringViewArray
fn left_impl_view(
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

                // Input string comes from StringViewArray, so it should fit in 32-bit length
                let new_length: u32 = left_byte_length(string, n) as u32;
                let byte_view = ByteView::from(view);
                // Construct a new view
                shrink_string_view_array_view(string, new_length, byte_view)
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

/// Calculate the byte length of the substring of `n` chars from string `string`
fn left_byte_length(string: &str, n: i64) -> usize {
    match n.cmp(&0) {
        Ordering::Less => string
            .char_indices()
            .nth_back(n.unsigned_abs() as usize - 1)
            .map(|(index, _)| index)
            .unwrap_or(0),
        Ordering::Equal => 0,
        Ordering::Greater => string
            .char_indices()
            .nth(n as usize)
            .map(|(index, _)| index)
            .unwrap_or(string.len()),
    }
}

/// Construct a new StringViewArray view from existing view `byte_view` and new length `len`.
/// Prefix is taken from the original string `string`.
/// Handles both inline and non-inline views, referencing the same buffers.
fn shrink_string_view_array_view(string: &str, len: u32, byte_view: ByteView) -> u128 {
    debug_assert!(len <= byte_view.length);
    // Acquire bytes view to string (no allocations)
    let bytes = string.as_bytes();

    if len <= 12 {
        // Inline view
        // Construct manually since ByteView cannot work with inline views
        let mut view_buffer = [0u8; 16];
        // 4 bytes: length
        view_buffer[0..4].copy_from_slice(&len.to_le_bytes());
        // 12 bytes: the whole zero-padded string
        view_buffer[4..4 + len as usize].copy_from_slice(&bytes[..len as usize]);
        u128::from_le_bytes(view_buffer)
    } else {
        // Non-inline view.
        // Use ByteView constructor to reference existing buffers
        let new_byte_view = ByteView::new(len, &bytes[..4])
            .with_buffer_index(byte_view.buffer_index)
            .with_offset(byte_view.offset);
        new_byte_view.as_u128()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{Utf8, Utf8View};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::left::LeftFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            LeftFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("ab")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
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
            LeftFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(-2i64)),
            ],
            Ok(Some("abc")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
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
            LeftFunc::new(),
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
            LeftFunc::new(),
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
            LeftFunc::new(),
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
            LeftFunc::new(),
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
            LeftFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LeftFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(-3i64)),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            LeftFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("abcde")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            internal_err!(
                "function left requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        // StringView cases
        test_function!(
            LeftFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some("abcde".to_string()))),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("ab")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LeftFunc::new(),
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
            LeftFunc::new(),
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
            LeftFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                    "joséésoj".to_string()
                ))),
                ColumnarValue::Scalar(ScalarValue::from(-3i64)),
            ],
            Ok(Some("joséé")),
            &str,
            Utf8View,
            StringViewArray
        );

        // Unicode indexing case
        let input = "joé楽s𐀀so↓j";
        for n in 1..=input.chars().count() {
            let expected = input
                .chars()
                .take(input.chars().count() - n)
                .collect::<String>();
            test_function!(
                LeftFunc::new(),
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
