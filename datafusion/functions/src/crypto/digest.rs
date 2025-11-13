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

use crate::crypto::basic::{digest_process, DigestAlgorithm};

use arrow::datatypes::DataType;
use datafusion_common::{
    exec_err, not_impl_err,
    types::{logical_binary, logical_string},
    utils::take_function_args,
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
    description = "Computes the binary hash of an expression using the specified algorithm.",
    syntax_example = "digest(expression, algorithm)",
    sql_example = r#"```sql
> select digest('foo', 'sha256');
+------------------------------------------------------------------+
| digest(Utf8("foo"),Utf8("sha256"))                               |
+------------------------------------------------------------------+
| 2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae |
+------------------------------------------------------------------+
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
#[derive(Debug, PartialEq, Eq, Hash)]
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
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_binary())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        digest(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Digest computes a binary hash of the given data, accepts Utf8 or LargeUtf8 and returns a [`ColumnarValue`].
/// Second argument is the algorithm to use.
/// Standard algorithms are md5, sha1, sha224, sha256, sha384 and sha512.
fn digest(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [data, digest_algorithm] = take_function_args("digest", args)?;
    let digest_algorithm = match digest_algorithm {
        ColumnarValue::Scalar(scalar) => match scalar.try_as_str() {
            Some(Some(method)) => method.parse::<DigestAlgorithm>(),
            _ => exec_err!("Unsupported data type {scalar:?} for function digest"),
        },
        ColumnarValue::Array(_) => {
            not_impl_err!("Digest using dynamically decided method is not yet supported")
        }
    }?;
    digest_process(data, digest_algorithm)
}
