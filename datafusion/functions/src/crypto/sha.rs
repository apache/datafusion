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

use crate::crypto::basic::{DigestAlgorithm, digest_process};

use arrow::datatypes::DataType;
use datafusion_common::{
    Result,
    types::{logical_binary, logical_string},
    utils::take_function_args,
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
    description = "Computes the SHA-224 hash of a binary string.",
    syntax_example = "sha224(expression)",
    sql_example = r#"```sql
> select sha224('foo');
+----------------------------------------------------------+
| sha224(Utf8("foo"))                                      |
+----------------------------------------------------------+
| 0808f64e60d58979fcb676c96ec938270dea42445aeefcd3a4e6f8db |
+----------------------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
struct SHA224Doc;

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the SHA-256 hash of a binary string.",
    syntax_example = "sha256(expression)",
    sql_example = r#"```sql
> select sha256('foo');
+------------------------------------------------------------------+
| sha256(Utf8("foo"))                                              |
+------------------------------------------------------------------+
| 2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae |
+------------------------------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
struct SHA256Doc;

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the SHA-384 hash of a binary string.",
    syntax_example = "sha384(expression)",
    sql_example = r#"```sql
> select sha384('foo');
+--------------------------------------------------------------------------------------------------+
| sha384(Utf8("foo"))                                                                              |
+--------------------------------------------------------------------------------------------------+
| 98c11ffdfdd540676b1a137cb1a22b2a70350c9a44171d6b1180c6be5cbb2ee3f79d532c8a1dd9ef2e8e08e752a3babb |
+--------------------------------------------------------------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
struct SHA384Doc;

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the SHA-512 hash of a binary string.",
    syntax_example = "sha512(expression)",
    sql_example = r#"```sql
> select sha512('foo');
+----------------------------------------------------------------------------------------------------------------------------------+
| sha512(Utf8("foo"))                                                                                                              |
+----------------------------------------------------------------------------------------------------------------------------------+
| f7fbba6e0636f890e56fbbf3283e524c6fa3204ae298382d624741d0dc6638326e282c41be5e4254d8820772c5518a2c5a8c0c7f7eda19594a7eb539453e1ed7 |
+----------------------------------------------------------------------------------------------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
struct SHA512Doc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SHAFunc {
    signature: Signature,
    name: &'static str,
    algorithm: DigestAlgorithm,
}

impl SHAFunc {
    pub fn sha224() -> Self {
        Self::new("sha224", DigestAlgorithm::Sha224)
    }

    pub fn sha256() -> Self {
        Self::new("sha256", DigestAlgorithm::Sha256)
    }

    pub fn sha384() -> Self {
        Self::new("sha384", DigestAlgorithm::Sha384)
    }

    pub fn sha512() -> Self {
        Self::new("sha512", DigestAlgorithm::Sha512)
    }

    fn new(name: &'static str, algorithm: DigestAlgorithm) -> Self {
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
            name,
            algorithm,
        }
    }
}

impl ScalarUDFImpl for SHAFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [data] = take_function_args(self.name(), args.args)?;
        digest_process(&data, self.algorithm)
    }

    fn documentation(&self) -> Option<&Documentation> {
        match self.algorithm {
            DigestAlgorithm::Sha224 => SHA224Doc {}.doc(),
            DigestAlgorithm::Sha256 => SHA256Doc {}.doc(),
            DigestAlgorithm::Sha384 => SHA384Doc {}.doc(),
            DigestAlgorithm::Sha512 => SHA512Doc {}.doc(),
            DigestAlgorithm::Md5
            | DigestAlgorithm::Blake2s
            | DigestAlgorithm::Blake2b
            | DigestAlgorithm::Blake3 => unreachable!(),
        }
    }
}
