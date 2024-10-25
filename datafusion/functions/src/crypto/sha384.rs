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
use super::basic::{sha384, utf8_or_binary_to_binary_type};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_HASHING;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct SHA384Func {
    signature: Signature,
}
impl Default for SHA384Func {
    fn default() -> Self {
        Self::new()
    }
}

impl SHA384Func {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8, LargeUtf8, Binary, LargeBinary],
                Volatility::Immutable,
            ),
        }
    }
}
impl ScalarUDFImpl for SHA384Func {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha384"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_or_binary_to_binary_type(&arg_types[0], self.name())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        sha384(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_sha384_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_sha384_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_HASHING)
            .with_description("Computes the SHA-384 hash of a binary string.")
            .with_syntax_example("sha384(expression)")
            .with_sql_example(
                r#"```sql
> select sha384('foo');
+-----------------------------------------+
| sha384(Utf8("foo"))                     |
+-----------------------------------------+
| <sha384_hash_result>                    |
+-----------------------------------------+
```"#,
            )
            .with_standard_argument("expression", Some("String"))
            .build()
            .unwrap()
    })
}
