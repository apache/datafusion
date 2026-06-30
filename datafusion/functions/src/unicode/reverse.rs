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

use crate::strings::{
    BulkNullStringArrayBuilder, GenericStringArrayBuilder, StringViewArrayBuilder,
};
use crate::utils::make_scalar_function;
use DataType::{LargeUtf8, Utf8, Utf8View};
use arrow::array::{Array, ArrayRef, AsArray, StringArrayType};
use arrow::datatypes::DataType;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Reverses the character order of a string.",
    syntax_example = "reverse(str)",
    sql_example = r#"```sql
> select reverse('datafusion');
+-----------------------------+
| reverse(Utf8("datafusion")) |
+-----------------------------+
| noisufatad                  |
+-----------------------------+
```"#,
    standard_argument(name = "str", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReverseFunc {
    signature: Signature,
}

impl Default for ReverseFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ReverseFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8View, Utf8, LargeUtf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ReverseFunc {
    fn name(&self) -> &str {
        "reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = &args.args;
        match args[0].data_type() {
            Utf8 | Utf8View | LargeUtf8 => make_scalar_function(reverse, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function reverse")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Reverses the order of the characters in the string `reverse('abcde') = 'edcba'`.
/// The implementation uses UTF-8 code points as characters
fn reverse(args: &[ArrayRef]) -> Result<ArrayRef> {
    let len = args[0].len();

    match args[0].data_type() {
        LargeUtf8 => reverse_impl(
            &args[0].as_string::<i64>(),
            GenericStringArrayBuilder::<i64>::with_capacity(len, 1024),
        ),
        Utf8 => reverse_impl(
            &args[0].as_string::<i32>(),
            GenericStringArrayBuilder::<i32>::with_capacity(len, 1024),
        ),
        Utf8View => reverse_impl(
            &args[0].as_string_view(),
            StringViewArrayBuilder::with_capacity(len),
        ),
        _ => unreachable!(
            "Reverse can only be applied to Utf8View, Utf8 and LargeUtf8 types"
        ),
    }
}

fn reverse_impl<'a, StringArrType, B>(
    string_array: &StringArrType,
    mut array_builder: B,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    B: BulkNullStringArrayBuilder,
{
    let item_len = string_array.len();
    // Null-preserving: reuse the input null buffer as the output null buffer.
    let nulls = string_array.nulls().cloned();
    let mut string_buf = String::new();
    let mut byte_buf = Vec::<u8>::new();

    if let Some(ref n) = nulls {
        for i in 0..item_len {
            if n.is_null(i) {
                array_builder.append_placeholder();
            } else {
                // SAFETY: `n.is_null(i)` was false in the branch above.
                let s = unsafe { string_array.value_unchecked(i) };
                append_reversed(s, &mut array_builder, &mut byte_buf, &mut string_buf);
            }
        }
    } else {
        for i in 0..item_len {
            // SAFETY: no null buffer means every index is valid.
            let s = unsafe { string_array.value_unchecked(i) };
            append_reversed(s, &mut array_builder, &mut byte_buf, &mut string_buf);
        }
    }

    array_builder.finish(nulls)
}

#[inline]
fn append_reversed<B: BulkNullStringArrayBuilder>(
    s: &str,
    builder: &mut B,
    byte_buf: &mut Vec<u8>,
    string_buf: &mut String,
) {
    if s.is_ascii() {
        // reverse bytes directly since ASCII characters are single bytes
        byte_buf.extend(s.as_bytes());
        byte_buf.reverse();
        // SAFETY: input was ASCII, so reversed bytes are still valid UTF-8.
        let reversed = unsafe { std::str::from_utf8_unchecked(byte_buf) };
        builder.append_value(reversed);
        byte_buf.clear();
    } else {
        string_buf.extend(s.chars().rev());
        builder.append_value(string_buf);
        string_buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray};
    use arrow::datatypes::DataType::{LargeUtf8, Utf8, Utf8View};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::reverse::ReverseFunc;
    use crate::utils::test::test_function;

    macro_rules! test_reverse {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_function!(
                ReverseFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8($INPUT))],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );

            test_function!(
                ReverseFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT))],
                $EXPECTED,
                &str,
                LargeUtf8,
                LargeStringArray
            );

            test_function!(
                ReverseFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT))],
                $EXPECTED,
                &str,
                Utf8View,
                StringViewArray
            );
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_reverse!(Some("abcde".into()), Ok(Some("edcba")));
        test_reverse!(Some("loẅks".into()), Ok(Some("sk̈wol")));
        test_reverse!(Some("loẅks".into()), Ok(Some("sk̈wol")));
        test_reverse!(None, Ok(None));
        #[cfg(not(feature = "unicode_expressions"))]
        test_reverse!(
            Some("abcde".into()),
            internal_err!(
                "function reverse requires compilation with feature flag: unicode_expressions."
            ),
        );

        Ok(())
    }

    #[test]
    fn test_array_with_nulls() {
        use crate::unicode::reverse::reverse;
        use arrow::array::ArrayRef;
        use std::sync::Arc;

        let input_values = vec![Some("abcd"), None, Some("XYZ"), Some("héllo"), None];
        let expected: Vec<Option<&str>> =
            vec![Some("dcba"), None, Some("ZYX"), Some("olléh"), None];

        let cases: Vec<(&str, ArrayRef)> = vec![
            (
                "StringArray",
                Arc::new(StringArray::from(input_values.clone())),
            ),
            (
                "LargeStringArray",
                Arc::new(LargeStringArray::from(input_values.clone())),
            ),
            (
                "StringViewArray",
                Arc::new(StringViewArray::from(input_values.clone())),
            ),
        ];

        for (label, input) in cases {
            let out = reverse(&[input]).unwrap();
            assert_eq!(out.len(), expected.len(), "{label}: length mismatch");

            let actual: Vec<Option<&str>> = match out.data_type() {
                Utf8 => out
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect(),
                LargeUtf8 => out
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .iter()
                    .collect(),
                Utf8View => out
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap()
                    .iter()
                    .collect(),
                other => panic!("{label}: unexpected output type {other:?}"),
            };
            assert_eq!(actual, expected, "{label}: value mismatch");
        }
    }
}
