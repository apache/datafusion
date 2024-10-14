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
use super::basic::{digest, utf8_or_binary_to_binary_type};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_HASHING;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature::*, Volatility,
};
use std::any::Any;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct DigestFunc {
    signature: Signature,
}
impl Default for DigestFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DigestFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![Binary, Utf8]),
                    Exact(vec![LargeBinary, Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for DigestFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "digest"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_or_binary_to_binary_type(&arg_types[0], self.name())
    }
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        digest(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_digest_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_digest_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_HASHING)
            .with_description(
                "Computes the binary hash of an expression using the specified algorithm.",
            )
            .with_syntax_example("digest(expression, algorithm)")
            .with_sql_example(
                r#"```sql
> select digest('foo', 'sha256');
+------------------------------------------+
| digest(Utf8("foo"), Utf8("sha256"))      |
+------------------------------------------+
| <binary_hash_result>                     |
+------------------------------------------+
```"#,
            )
            .with_standard_argument(
                "expression", Some("String"))
            .with_argument(
                "algorithm",
                "String expression specifying algorithm to use. Must be one of:
                
- md5
- sha224
- sha256
- sha384
- sha512
- blake2s
- blake2b
- blake3",
            )
            .build()
            .unwrap()
    })
}
