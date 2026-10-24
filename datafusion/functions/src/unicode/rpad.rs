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

use super::pad::{RightPad, invoke_pad, pad_signature};
use crate::utils::utf8_to_str_type;
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Pads the right side of a string with another string to a specified string length.",
    syntax_example = "rpad(str, n[, padding_str])",
    sql_example = r#"```sql
>  select rpad('datafusion', 20, '_-');
+-----------------------------------------------+
| rpad(Utf8("datafusion"),Int64(20),Utf8("_-")) |
+-----------------------------------------------+
| datafusion_-_-_-_-_-                          |
+-----------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "n",
        description = "String length to pad to. If the input string is longer than this length, it is truncated."
    ),
    argument(
        name = "padding_str",
        description = "String expression to pad with. Can be a constant, column, or function, and any combination of string operators. _Default is a space._"
    ),
    related_udf(name = "lpad")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RPadFunc {
    signature: Signature,
}

impl Default for RPadFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RPadFunc {
    pub fn new() -> Self {
        Self {
            signature: pad_signature(),
        }
    }
}

impl ScalarUDFImpl for RPadFunc {
    fn name(&self) -> &str {
        "rpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "rpad")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_pad::<RightPad>(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
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
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hello")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("he")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("hi")),
                ColumnarValue::Scalar(ScalarValue::from(6i64)),
                ColumnarValue::Scalar(ScalarValue::from("xy")),
            ],
            Ok(Some("hixyxy")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            RPadFunc::new(),
            vec![
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
            vec![
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
