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

use arrow::datatypes::DataType;

use super::pad::{LeftPad, invoke_pad, pad_signature};
use crate::utils::utf8_to_str_type;
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
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
        Self {
            signature: pad_signature(),
        }
    }
}

impl ScalarUDFImpl for LPadFunc {
    fn name(&self) -> &str {
        "lpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "lpad")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_pad::<LeftPad>(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
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
                Utf8,
                StringArray
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
                Utf8,
                StringArray
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
                Utf8,
                StringArray
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
