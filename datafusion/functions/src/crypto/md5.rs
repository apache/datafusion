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

use arrow::{array::StringViewArray, datatypes::DataType};
use datafusion_common::{
    Result, ScalarValue,
    cast::as_binary_array,
    internal_err,
    types::{logical_binary, logical_string},
    utils::take_function_args,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;
use std::{any::Any, sync::Arc};

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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

/// Hex encoding lookup table for fast byte-to-hex conversion
const HEX_CHARS_LOWER: &[u8; 16] = b"0123456789abcdef";

/// Fast hex encoding using a lookup table instead of format strings.
/// This is significantly faster than using `write!("{:02x}")` for each byte.
#[inline]
fn hex_encode(data: impl AsRef<[u8]>) -> String {
    let bytes = data.as_ref();
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX_CHARS_LOWER[(b >> 4) as usize] as char);
        s.push(HEX_CHARS_LOWER[(b & 0x0f) as usize] as char);
    }
    s
}

fn md5(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [data] = take_function_args("md5", args)?;
    let value = digest_process(data, DigestAlgorithm::Md5)?;

    // md5 requires special handling because of its unique utf8view return type
    Ok(match value {
        ColumnarValue::Array(array) => {
            let binary_array = as_binary_array(&array)?;
            let string_array: StringViewArray =
                binary_array.iter().map(|opt| opt.map(hex_encode)).collect();
            ColumnarValue::Array(Arc::new(string_array))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => {
            ColumnarValue::Scalar(ScalarValue::Utf8View(opt.map(hex_encode)))
        }
        _ => return internal_err!("Impossibly got invalid results from digest"),
    })
}
