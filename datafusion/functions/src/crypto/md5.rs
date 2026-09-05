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

use arrow::{
    array::{Array, BinaryViewBuilder},
    datatypes::DataType,
};
use datafusion_common::{
    Result, ScalarValue,
    cast::as_binary_array,
    internal_err,
    types::{logical_binary, logical_string},
    utils::hex::{HexCase, encode_bytes_into},
    utils::take_function_args,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;
use std::sync::Arc;

use crate::crypto::basic::{DigestAlgorithm, digest_process};

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes an MD5 128-bit checksum for a string expression.",
    syntax_example = "md5(expression)",
    sql_example = r#"```sql
> select md5('foo');
+----------------------------------+
| md5(Utf8("foo"))                 |
+----------------------------------+
| acbd18db4cc2f85cedef654fccc4a4d8 |
+----------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Md5Func {
    signature: Signature,
}

impl Default for Md5Func {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_string()),
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Native(logical_binary()),
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for Md5Func {
    fn name(&self) -> &str {
        "md5"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        md5(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn md5(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [data] = take_function_args("md5", args)?;
    let value = digest_process(data, DigestAlgorithm::Md5)?;

    // md5 requires special handling because of its unique utf8view return type
    Ok(match value {
        ColumnarValue::Array(array) => {
            let binary_array = as_binary_array(&array)?;
            let mut byte_builder = BinaryViewBuilder::with_capacity(binary_array.len());
            let mut hex_bytes = Vec::with_capacity(32);

            for i in 0..binary_array.len() {
                if binary_array.is_null(i) {
                    byte_builder.append_null();
                    continue;
                }

                hex_bytes.clear();
                let digest = binary_array.value(i);
                encode_bytes_into(digest, HexCase::Lower, &mut hex_bytes);
                byte_builder.append_value(&hex_bytes);
            }

            let str_array = unsafe {
                // Safe: `encode_bytes_into` only writes ASCII hex digits, so the bytes are valid UTF-8.
                byte_builder.finish().to_string_view_unchecked()
            };
            ColumnarValue::Array(Arc::new(str_array))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => {
            ColumnarValue::Scalar(ScalarValue::Utf8View(opt.map(|b| {
                let mut hex_bytes = Vec::with_capacity(b.len() * 2);
                encode_bytes_into(&b, HexCase::Lower, &mut hex_bytes);
                // Safe: `encode_bytes_into` only writes ASCII hex digits, so the bytes are valid UTF-8.
                unsafe { String::from_utf8_unchecked(hex_bytes) }
            })))
        }
        _ => return internal_err!("Impossibly got invalid results from digest"),
    })
}
