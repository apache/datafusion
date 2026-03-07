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
use std::sync::Arc;

use crate::utils::make_scalar_function;
use DataType::{LargeUtf8, Utf8, Utf8View};
use arrow::array::{
    Array, ArrayRef, AsArray, LargeStringBuilder, StringArrayType, StringBuilder,
    StringLikeArrayBuilder, StringViewBuilder,
};
use arrow::datatypes::DataType;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "reverse"
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
        Utf8 => reverse_impl(
            &args[0].as_string::<i32>(),
            StringBuilder::with_capacity(len, 1024),
        ),
        Utf8View => reverse_impl(
            &args[0].as_string_view(),
            StringViewBuilder::with_capacity(len),
        ),
        LargeUtf8 => reverse_impl(
            &args[0].as_string::<i64>(),
            LargeStringBuilder::with_capacity(len, 1024),
        ),
        _ => unreachable!(
            "Reverse can only be applied to Utf8View, Utf8 and LargeUtf8 types"
        ),
    }
}

fn reverse_impl<'a, StringArrType, StringBuilderType>(
    string_array: &StringArrType,
    mut array_builder: StringBuilderType,
) -> Result<ArrayRef>
where
    StringArrType: StringArrayType<'a>,
    StringBuilderType: StringLikeArrayBuilder,
{
    let mut string_buf = String::new();
    let mut byte_buf = Vec::<u8>::new();

    for string in string_array.iter() {
        if let Some(s) = string {
            if s.is_ascii() {
                // reverse bytes directly since ASCII characters are single bytes
                byte_buf.extend(s.as_bytes());
                byte_buf.reverse();
                // SAFETY: Since the original string was ASCII, reversing the bytes still results in valid UTF-8.
                let reversed = unsafe { std::str::from_utf8_unchecked(&byte_buf) };
                array_builder.append_value(reversed);
                byte_buf.clear();
            } else {
                string_buf.extend(s.chars().rev());
                array_builder.append_value(&string_buf);
                string_buf.clear();
            }
        } else {
            array_builder.append_null();
        }
    }

    Ok(Arc::new(array_builder.finish()) as ArrayRef)
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
}
