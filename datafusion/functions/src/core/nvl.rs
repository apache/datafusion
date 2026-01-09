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

use crate::core::coalesce::CoalesceFunc;
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::Result;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Conditional Functions"),
    description = "Returns _expression2_ if _expression1_ is NULL otherwise it returns _expression1_ and _expression2_ is not evaluated. This function can be used to substitute a default value for NULL values.",
    syntax_example = "nvl(expression1, expression2)",
    sql_example = r#"```sql
> select nvl(null, 'a');
+---------------------+
| nvl(NULL,Utf8("a")) |
+---------------------+
| a                   |
+---------------------+\
> select nvl('b', 'a');
+--------------------------+
| nvl(Utf8("b"),Utf8("a")) |
+--------------------------+
| b                        |
+--------------------------+
```
"#,
    argument(
        name = "expression1",
        description = "Expression to return if not null. Can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "expression2",
        description = "Expression to return if expr1 is null. Can be a constant, column, or function, and any combination of operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NVLFunc {
    coalesce: CoalesceFunc,
    aliases: Vec<String>,
}

/// Currently supported types by the nvl/ifnull function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
static SUPPORTED_NVL_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Utf8View,
    DataType::Utf8,
    DataType::LargeUtf8,
];

impl Default for NVLFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NVLFunc {
    pub fn new() -> Self {
        Self {
            coalesce: CoalesceFunc {
                signature: Signature::uniform(
                    2,
                    SUPPORTED_NVL_TYPES.to_vec(),
                    Volatility::Immutable,
                ),
            },
            aliases: vec![String::from("ifnull")],
        }
    }
}

impl ScalarUDFImpl for NVLFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "nvl"
    }

    fn signature(&self) -> &Signature {
        &self.coalesce.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.coalesce.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.coalesce.return_field_from_args(args)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        self.coalesce.simplify(args, info)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.coalesce.invoke_with_args(args)
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.coalesce.conditional_arguments(args)
    }

    fn short_circuits(&self) -> bool {
        self.coalesce.short_circuits()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
