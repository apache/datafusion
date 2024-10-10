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

use arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion_expr::{ColumnarValue, Documentation};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct ArrowTypeOfFunc {
    signature: Signature,
}

impl Default for ArrowTypeOfFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrowTypeOfFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrowTypeOfFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "arrow_typeof"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "arrow_typeof function requires 1 arguments, got {}",
                args.len()
            );
        }

        let input_data_type = args[0].data_type();
        Ok(ColumnarValue::Scalar(ScalarValue::from(format!(
            "{input_data_type}"
        ))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_arrowtypeof_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_arrowtypeof_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_OTHER)
            .with_description(
                "Returns the name of the underlying [Arrow data type](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) of the expression.",
            )
            .with_syntax_example("arrow_typeof(expression)")
            .with_sql_example(
                r#"```sql
> select arrow_typeof('foo'), arrow_typeof(1);
+---------------------------+------------------------+
| arrow_typeof(Utf8("foo")) | arrow_typeof(Int64(1)) |
+---------------------------+------------------------+
| Utf8                      | Int64                  |
+---------------------------+------------------------+
```
"#,
            )
            .with_argument("expression", "Expression to evaluate. The expression can be a constant, column, or function, and any combination of operators.")
            .build()
            .unwrap()
    })
}
