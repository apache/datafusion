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

//! "crypto" DataFusion functions
use super::basic::{sha512, utf8_or_binary_to_binary_type};
use arrow::datatypes::DataType;
use datafusion_common::{
    types::{logical_binary, logical_string, NativeType},
    Result,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the SHA-512 hash of a binary string.",
    syntax_example = "sha512(expression)",
    sql_example = r#"```sql
> select sha512('foo');
+-------------------------------------------+
| sha512(Utf8("foo"))                       |
+-------------------------------------------+
| <sha512_hash_result>                      |
+-------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
#[derive(Debug)]
pub struct SHA512Func {
    signature: Signature,
}
impl Default for SHA512Func {
    fn default() -> Self {
        Self::new()
    }
}

impl SHA512Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_binary()),
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::String,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_binary()),
                        vec![TypeSignatureClass::Native(logical_binary())],
                        NativeType::Binary,
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for SHA512Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha512"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_or_binary_to_binary_type(&arg_types[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        sha512(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
