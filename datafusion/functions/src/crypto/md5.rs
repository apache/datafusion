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

use crate::crypto::basic::md5;
use arrow::datatypes::DataType;
use datafusion_common::{
    Result,
    types::{logical_binary, logical_string},
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
