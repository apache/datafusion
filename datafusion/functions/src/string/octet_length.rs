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

use crate::utils::{transform_leaf_type_preserving_encoding, utf8_to_int_type};
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, EncodingPreservation, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the length of a string or binary in bytes.",
    syntax_example = "octet_length(str)",
    sql_example = r#"```sql
> select octet_length('Ångström');
+--------------------------------+
| octet_length(Utf8("Ångström")) |
+--------------------------------+
| 10                             |
+--------------------------------+
```"#,
    argument(
        name = "str",
        description = "String or binary expression to operate on. Can be a constant, column, or function, and any combination of operators."
    ),
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
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string()))
                            .with_encoding_preservation(
                                EncodingPreservation::dictionary(),
                            ),
                    ]),
                    // `TypeSignatureClass::Binary` also admits FixedSizeBinary,
                    // which `Native(logical_binary())` would reject.
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Binary)
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

impl ScalarUDFImpl for OctetLengthFunc {
    fn name(&self) -> &str {
        "octet_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        transform_leaf_type_preserving_encoding(&arg_types[0], &|data_type| {
            utf8_to_int_type(data_type, "octet_length")
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array] = take_function_args(self.name(), &args.args)?;

        match array {
            ColumnarValue::Array(v) => Ok(ColumnarValue::Array(length(v.as_ref())?)),
            ColumnarValue::Scalar(v) => Ok(ColumnarValue::Scalar(octet_length_scalar(v))),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn octet_length_scalar(value: &ScalarValue) -> ScalarValue {
    match value {
        ScalarValue::Utf8(v) => ScalarValue::Int32(v.as_ref().map(|x| x.len() as i32)),
        ScalarValue::LargeUtf8(v) => {
            ScalarValue::Int64(v.as_ref().map(|x| x.len() as i64))
        }
        ScalarValue::Utf8View(v) => {
            ScalarValue::Int32(v.as_ref().map(|x| x.len() as i32))
        }
        ScalarValue::Binary(v) => ScalarValue::Int32(v.as_ref().map(|x| x.len() as i32)),
        ScalarValue::LargeBinary(v) => {
            ScalarValue::Int64(v.as_ref().map(|x| x.len() as i64))
        }
        ScalarValue::BinaryView(v) => {
            ScalarValue::Int32(v.as_ref().map(|x| x.len() as i32))
        }
        ScalarValue::FixedSizeBinary(_, v) => {
            ScalarValue::Int32(v.as_ref().map(|x| x.len() as i32))
        }
        ScalarValue::Dictionary(key_type, value) => ScalarValue::Dictionary(
            key_type.clone(),
            Box::new(octet_length_scalar(value)),
        ),
        _ => unreachable!("OctetLengthFunc"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Int32Array,
        Int64Array, LargeBinaryArray, StringArray,
    };
    use arrow::datatypes::DataType::{Int32, Int64};

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

        // Binary inputs: byte length, no string coercion.
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Array(Arc::new(BinaryArray::from(vec![
                &b"chars"[..],
                &b"chars2"[..],
            ])))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Binary(Some(
                b"chars".to_vec()
            )))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        // Arbitrary non-UTF-8 bytes: the case CAST(col AS VARCHAR) cannot serve.
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![
                0xff, 0xfe, 0x00, 0x80
            ])))],
            Ok(Some(4)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![])))],
            Ok(Some(0)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Binary(None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Array(Arc::new(BinaryViewArray::from(vec![
                &b"chars"[..],
                &b"chars2"[..],
            ])))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::BinaryView(Some(
                b"chars".to_vec()
            )))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        // FixedSizeBinary reports its fixed width per non-null row.
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Array(Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    vec![&b"abc"[..], &b"def"[..]].into_iter()
                )?
            ))],
            Ok(Some(3)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(
                5,
                Some(b"chars".to_vec())
            ))],
            Ok(Some(5)),
            i32,
            Int32,
            Int32Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(5, None))],
            Ok(None),
            i32,
            Int32,
            Int32Array
        );
        // LargeBinary widens the return type to Int64, mirroring LargeUtf8.
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Array(Arc::new(LargeBinaryArray::from(
                vec![&b"chars"[..], &b"chars2"[..]]
            )))],
            Ok(Some(5)),
            i64,
            Int64,
            Int64Array
        );
        test_function!(
            OctetLengthFunc::new(),
            vec![ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
                b"chars".to_vec()
            )))],
            Ok(Some(5)),
            i64,
            Int64,
            Int64Array
        );

        Ok(())
    }
}
