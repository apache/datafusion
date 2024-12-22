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
use super::basic::{sha224, utf8_or_binary_to_binary_type};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_doc::DocSection;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::OnceLock;

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Computes the SHA-224 hash of a binary string.",
    syntax_example = "sha224(expression",
    sql_example = r#"```sql
> select sha224('foo');
+------------------------------------------+
| sha224(Utf8("foo"))                      |
+------------------------------------------+
| <sha224_hash_result>                     |
+------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String")
)]
#[derive(Debug)]
pub struct SHA224Func {
    signature: Signature,
}

impl Default for SHA224Func {
    fn default() -> Self {
        Self::new()
    }
}

impl SHA224Func {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8View, Utf8, LargeUtf8, Binary, LargeBinary],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SHA224Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha224"
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
        sha224(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
