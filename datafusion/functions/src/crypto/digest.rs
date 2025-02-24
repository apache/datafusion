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
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature::*, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the binary hash of an expression using the specified algorithm.",
    syntax_example = "digest(expression, algorithm)",
    sql_example = r#"```sql
> select digest('foo', 'sha256');
+------------------------------------------+
| digest(Utf8("foo"), Utf8("sha256"))      |
+------------------------------------------+
| <binary_hash_result>                     |
+------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String"),
    argument(
        name = "algorithm",
        description = "String expression specifying algorithm to use. Must be one of:       
    - md5
    - sha224
    - sha256
    - sha384
    - sha512
    - blake2s
    - blake2b
    - blake3"
    )
)]
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
                    Exact(vec![Utf8View, Utf8View]),
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
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        digest(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
