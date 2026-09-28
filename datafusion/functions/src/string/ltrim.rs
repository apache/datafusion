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

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::string::common::*;
use crate::utils::make_scalar_function;
use datafusion_common::types::logical_string;
use datafusion_common::{Result, exec_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, EncodingPreservation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

/// Returns the longest string with leading characters removed. If the characters are not specified, spaces are removed.
/// ltrim('zzzytest', 'xyz') = 'test'
fn ltrim(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args = if args.len() > 1 {
        let arg1 = arrow::compute::kernels::cast::cast(&args[1], args[0].data_type())?;
        vec![Arc::clone(&args[0]), arg1]
    } else {
        args.to_owned()
    };
    match args[0].data_type() {
        DataType::Utf8 => general_trim::<i32, TrimLeft>(&args, false),
        DataType::LargeUtf8 => general_trim::<i64, TrimLeft>(&args, false),
        DataType::Utf8View => general_trim::<i32, TrimLeft>(&args, true),
        DataType::Dictionary(_, _) => {
            let dictionary = args[0].as_any_dictionary();
            let trimmed = ltrim(&[Arc::clone(dictionary.values())])?;
            Ok(dictionary.with_values(trimmed))
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function ltrim, expected Utf8, LargeUtf8 or Utf8View."
        ),
    }
}

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Trims the specified trim string from the beginning of a string. If no trim string is provided, spaces are removed from the start of the input string.",
    syntax_example = "ltrim(str[, trim_str])",
    sql_example = r#"```sql
> select ltrim('  datafusion  ');
+-------------------------------+
| ltrim(Utf8("  datafusion  ")) |
+-------------------------------+
| datafusion                    |
+-------------------------------+
> select ltrim('___datafusion___', '_');
+-------------------------------------------+
| ltrim(Utf8("___datafusion___"),Utf8("_")) |
+-------------------------------------------+
| datafusion___                             |
+-------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "trim_str",
        description = r"String expression to trim from the beginning of the input string. Can be a constant, column, or function, and any combination of arithmetic operators. _Default is a space._"
    ),
    alternative_syntax = "trim(LEADING trim_str FROM str)",
    related_udf(name = "btrim"),
    related_udf(name = "rtrim")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LtrimFunc {
    signature: Signature,
}

impl Default for LtrimFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LtrimFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string()))
                            .with_encoding_preservation(
                                EncodingPreservation::dictionary(),
                            ),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LtrimFunc {
    fn name(&self) -> &str {
        "ltrim"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(ltrim, vec![Hint::Pad, Hint::AcceptsSingular])(&args.args)
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

    use crate::string::ltrim::LtrimFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() {
        // String view cases for checking normal logic
        test_function!(
            LtrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet  ")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LtrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("  alphabet  ")
            ))),],
            Ok(Some("alphabet  ")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("t")))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabe"
                )))),
            ],
            Ok(Some("t")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            LtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        // Special string view case for checking unlined output(len > 12)
        test_function!(
            LtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "xxxalphabetalphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("x")))),
            ],
            Ok(Some("alphabetalphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        // String cases
        test_function!(
            LtrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet  ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LtrimFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("alphabet  ")
            ))),],
            Ok(Some("alphabet  ")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("t")))),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabe")))),
            ],
            Ok(Some("t")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            LtrimFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("alphabet")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
    }
}
