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
use crate::crypto::basic::md5;
use arrow::datatypes::DataType;
use datafusion_common::{plan_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_HASHING;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::OnceLock;

#[derive(Debug)]
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match &arg_types[0] {
            LargeUtf8 | LargeBinary => LargeUtf8,
            Utf8 | Binary => Utf8,
            Null => Null,
            Dictionary(_, t) => match **t {
                LargeUtf8 | LargeBinary => LargeUtf8,
                Utf8 | Binary => Utf8,
                Null => Null,
                _ => {
                    return plan_err!(
                        "the md5 can only accept strings but got {:?}",
                        **t
                    );
                }
            },
            other => {
                return plan_err!(
                    "The md5 function can only accept strings. Got {other}"
                );
            }
        })
    }
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        md5(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_md5_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_md5_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_HASHING)
            .with_description("Computes an MD5 128-bit checksum for a string expression.")
            .with_syntax_example("md5(expression)")
            .with_sql_example(
                r#"```sql
> select md5('foo');
+-------------------------------------+
| md5(Utf8("foo"))                    |
+-------------------------------------+
| <md5_checksum_result>               |
+-------------------------------------+
```"#,
            )
            .with_standard_argument("expression", Some("String"))
            .build()
            .unwrap()
    })
}
