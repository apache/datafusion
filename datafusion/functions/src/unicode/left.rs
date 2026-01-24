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

use crate::utils::{make_scalar_function, utf8_to_str_type};
use arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, ByteView, GenericStringArray, Int64Array,
    OffsetSizeTrait, StringViewArray,
};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, ScalarBuffer};
use datafusion_common::Result;
use datafusion_common::cast::{
    as_generic_string_array, as_int64_array, as_string_view_array,
};
use datafusion_common::{exec_datafusion_err, exec_err};
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
        if arg_types[0] == DataType::Utf8View {
            Ok(DataType::Utf8View)
        } else {
            utf8_to_str_type(&arg_types[0], "left")
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        match args[0].data_type() {
            DataType::Utf8 | DataType::Utf8View => {
                make_scalar_function(left::<i32>, vec![])(args)
            }
            DataType::LargeUtf8 => make_scalar_function(left::<i64>, vec![])(args),
            other => exec_err!(
                "Unsupported data type {other:?} for function left,\
                expected Utf8View, Utf8 or LargeUtf8."
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
fn left<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let n_array = as_int64_array(&args[1])?;

    if args[0].data_type() == &DataType::Utf8View {
        let string_view_array = as_string_view_array(&args[0])?;
        left_impl_view(string_view_array, n_array)
    } else {
        let string_array = as_generic_string_array::<T>(&args[0])?;
        left_impl::<T, _>(string_array, n_array)
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

    let (views, buffers, _nulls) = string_view_array.clone().into_parts();

    if string_view_array.len() != n_array.len() {
        return exec_err!(
            "Expected same shape of arrays, given {} and {}",
            string_view_array.len(),
            n_array.len()
        );
    }

    // Every string in StringViewArray has one corresponding view in `views`
    if views.len() != string_view_array.len() {
        return exec_err!(
            "StringViewArray views length {} does not match array length {}",
            views.len(),
            string_view_array.len()
        );
    }
    let mut null_buffer_builder = NullBufferBuilder::new(len);
    let mut new_views = Vec::with_capacity(len);

    for idx in 0..len {
        let view = views[idx];

        if string_view_array.is_valid(idx) && n_array.is_valid(idx) {
            let string: &str = string_view_array.value(idx);
            let n = n_array.value(idx);

            let byte_length = left_byte_length(string, n);
            let new_length: u32 = byte_length.try_into().map_err(|_| {
                exec_datafusion_err!("String is larger than 32-bit limit at index {idx}")
            })?;
            let byte_view = ByteView::from(view);
            // Construct a new view
            let new_view = shrink_string_view_array_view(string, new_length, byte_view)?;
            new_views.push(new_view);
            null_buffer_builder.append_non_null();
        } else {
            new_views.push(view);
            // Emit null
            null_buffer_builder.append_null();
        }
    }
    // Buffers are unchanged
    // Nulls are rebuilt from scratch
    let nulls = null_buffer_builder.finish();
    let result = StringViewArray::try_new(ScalarBuffer::from(new_views), buffers, nulls)?;
    Ok(Arc::new(result) as ArrayRef)
}

/// Calculate the byte length of the substring of `n` chars from string `string`
fn left_byte_length(string: &str, n: i64) -> usize {
    match n.cmp(&0) {
        Ordering::Less => {
            let mut indices = string.char_indices();
            // Find the first byte of a character past last `-n` characters
            let mut end_idx: usize = usize::MAX;
            for _ in n..0 {
                if let Some((i, _)) = indices.next_back() {
                    end_idx = i;
                } else {
                    end_idx = 0;
                    break;
                }
            }
            end_idx
        }
        Ordering::Equal => 0,
        Ordering::Greater => {
            if let Some((end_idx, end_char)) =
                string.char_indices().take(n as usize).last()
            {
                // Include length of the last character
                end_idx + end_char.len_utf8()
            } else {
                // String is empty
                0
            }
        }
    }
}

/// Construct a new StringViewArray view from existing view `byte_view` and new length `len`.
/// Prefix is taken from the original string `string`.
/// Handles both inline and non-inline views, referencing the same buffers.
fn shrink_string_view_array_view(
    string: &str,
    len: u32,
    byte_view: ByteView,
) -> Result<u128> {
    if len > byte_view.length {
        return exec_err!(
            "Original length {} must be greater than new length {}",
            byte_view.length,
            len
        );
    }
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
        Ok(u128::from_le_bytes(view_buffer))
    } else {
        // Non-inline view.
        // Use ByteView constructor to reference existing buffers
        // 4 bytes: string prefix
        let mut prefix = [0u8; 4];
        prefix.copy_from_slice(&bytes[..4]);

        let new_byte_view = ByteView::new(len, &prefix)
            .with_buffer_index(byte_view.buffer_index)
            .with_offset(byte_view.offset);
        Ok(new_byte_view.as_u128())
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
