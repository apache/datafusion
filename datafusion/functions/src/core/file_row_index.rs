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

//! Implementation of the `file_row_index` scalar function.

use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_doc::Documentation;
use datafusion_expr::{
    ColumnarValue, ExpressionPlacement, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

/// Scalar UDF implementation for `file_row_index()`.
///
/// File sources that can expose per-file row indexes rewrite this placeholder
/// function into a source-provided physical expression. Direct evaluation
/// returns an error because there is no file context outside a scan.
#[user_doc(
    doc_section(label = "Other Functions"),
    description = r#"Returns the zero-based row offset within the source file
that produced the current row.

The value is scoped to one file, so rows from different files in the same scan
can have the same row index. This function is intended to be rewritten at
file-scan time. If the input file is not known (for example, if this function
is evaluated outside a file scan, or was not pushed down into one), direct
evaluation returns an error.
"#,
    syntax_example = "file_row_index()",
    sql_example = r#"```sql
SELECT file_row_index() FROM t;
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FileRowIndexFunc {
    signature: Signature,
}

impl Default for FileRowIndexFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FileRowIndexFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for FileRowIndexFunc {
    fn name(&self) -> &str {
        "file_row_index"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [] = take_function_args(self.name(), args)?;
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [] = take_function_args(self.name(), args.args)?;
        exec_err!("file_row_index() is source dependent and cannot be evaluated directly")
    }

    fn placement(&self, _args: &[ExpressionPlacement]) -> ExpressionPlacement {
        ExpressionPlacement::MoveTowardsLeafNodes
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
