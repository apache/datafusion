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

use crate::unicode::common::{LeftSlicer, general_left_right};
use crate::utils::make_scalar_function;
use arrow::datatypes::DataType;
use datafusion_common::Result;
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

    /// Returns first n characters in the string, or when n is negative, returns all but last |n| characters.
    /// left('abcde', 2) = 'ab'
    /// left('abcde', -2) = 'abc'
    /// The implementation uses UTF-8 code points as characters
    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        match args[0].data_type() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                make_scalar_function(general_left_right::<LeftSlicer>, vec![])(args)
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
