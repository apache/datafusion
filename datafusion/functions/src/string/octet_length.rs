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

use arrow::compute::kernels::length::length;
use arrow::datatypes::DataType;
use std::any::Any;

use crate::utils::utf8_to_int_type;
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the length of a string in bytes.",
    syntax_example = "octet_length(str)",
    sql_example = r#"```sql
> select octet_length('Ångström');
+--------------------------------+
| octet_length(Utf8("Ångström")) |
+--------------------------------+
| 10                             |
+--------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "bit_length"),
    related_udf(name = "length")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct OctetLengthFunc {
    signature: Signature,
}

impl Default for OctetLengthFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl OctetLengthFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for OctetLengthFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "octet_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "octet_length")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array] = take_function_args(self.name(), &args.args)?;

        match array {
            ColumnarValue::Array(v) => Ok(ColumnarValue::Array(length(v.as_ref())?)),
            ColumnarValue::Scalar(v) => match v {
                ScalarValue::Utf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| x.len() as i32),
                ))),
                ScalarValue::LargeUtf8(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int64(v.as_ref().map(|x| x.len() as i64)),
                )),
                ScalarValue::Utf8View(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int32(v.as_ref().map(|x| x.len() as i32)),
                )),
                _ => unreachable!("OctetLengthFunc"),
            },
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::DataType::Int32;

    use datafusion_common::ScalarValue;
    use datafusion_common::{Result, exec_err};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::octet_length::OctetLengthFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(12)))],
            exec_err!(
                "The OCTET_LENGTH function can only accept strings, but got Int32."
            ),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                String::from("chars"),
                String::from("chars2"),
            ])))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("chars")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("chars"))))
            ],
            exec_err!("octet_length function requires 1 argument, got 2"),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("chars")
            )))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("josé")
            )))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                String::from("")
            )))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("joséjoséjoséjosé")
            )))],
            Ok(Some(20)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("josé")
            )))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                String::from("")
            )))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );

        Ok(())
    }
}
