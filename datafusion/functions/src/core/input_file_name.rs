// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`InputFileNameFunc`]: Implementation of the `input_file_name` function.

use arrow::datatypes::DataType;
use datafusion_common::{exec_err, utils::take_function_args};
use datafusion_doc::Documentation;
use datafusion_expr::{
    ColumnarValue, ExpressionPlacement, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Other Functions"),
    description = r#"Returns the path of the input file that produced the current row.

Note: file paths/URIs may be sensitive metadata depending on your environment.

This function is intended to be rewritten at file-scan time (when the file is
known). If the input file is not known (for example, if this function is
evaluated outside a file scan, or was not pushed down into one), direct evaluation returns an error.
"#,
    syntax_example = "input_file_name()",
    sql_example = r#"```sql
> COPY (SELECT * from values (100), (200), (300)) to '/tmp/foo.parquet';

> select *, input_file_name(), file_row_index() from '/tmp/foo.parquet';
+---------+-------------------+------------------+
| column1 | input_file_name() | file_row_index() |
+---------+-------------------+------------------+
| 100     | tmp/foo.parquet   | 0                |
| 200     | tmp/foo.parquet   | 1                |
| 300     | tmp/foo.parquet   | 2                |
+---------+-------------------+------------------+
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
    fn name(&self) -> &str {
        "input_file_name"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        let [] = take_function_args(self.name(), arg_types)?;
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [] = take_function_args(self.name(), args.args)?;

        exec_err!(
            "input_file_name() is source dependent and cannot be evaluated directly"
        )
    }

    fn placement(&self, _args: &[ExpressionPlacement]) -> ExpressionPlacement {
        ExpressionPlacement::MoveTowardsLeafNodes
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
