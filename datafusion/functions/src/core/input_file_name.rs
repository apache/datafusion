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

//! [`InputFileNameFunc`]: Implementation of the `input_file_name` function.

use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, utils::take_function_args};
use datafusion_doc::Documentation;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;

#[user_doc(
    doc_section(label = "Other Functions"),
    description = r#"Returns the path of the input file that produced the current row.

Note: file paths/URIs may be sensitive metadata depending on your environment.

This function is intended to be rewritten at file-scan time (when the file is
known). If the input file is not known (for example, if this function is
evaluated outside a file scan, or was not pushed down into one), this function
returns NULL.
"#,
    syntax_example = "input_file_name()",
    sql_example = r#"```sql
SELECT input_file_name() FROM t;
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct InputFileNameFunc {
    signature: Signature,
}

impl Default for InputFileNameFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl InputFileNameFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for InputFileNameFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "input_file_name"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [] = take_function_args(self.name(), args)?;
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [] = take_function_args(self.name(), args.args)?;
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
