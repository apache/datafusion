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

use std::sync::Arc;

use crate::core::coalesce::CoalesceFunc;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
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
+---------------------+
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

/// Argument types that keep nvl's original coercion, ordered from least to
/// most informative. Other argument types are coerced the way coalesce does,
/// falling back to the original coercion when coalesce cannot unify them.
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

/// nvl's original coercion: both arguments become the first type in
/// [`SUPPORTED_NVL_TYPES`] that both can be coerced to.
fn uniform_coercion(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let uniform = ScalarUDF::new_from_impl(CoalesceFunc {
        signature: Signature::uniform(
            2,
            SUPPORTED_NVL_TYPES.to_vec(),
            Volatility::Immutable,
        ),
    });
    let fields: Vec<FieldRef> = arg_types
        .iter()
        .map(|t| Arc::new(Field::new("", t.clone(), true)))
        .collect();
    Ok(fields_with_udf(&fields, &uniform)?
        .iter()
        .map(|f| f.data_type().clone())
        .collect())
}

impl Default for NVLFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl NVLFunc {
    pub fn new() -> Self {
        Self {
            coalesce: CoalesceFunc {
                signature: Signature::user_defined(Volatility::Immutable),
            },
            aliases: vec![String::from("ifnull")],
        }
    }
}

impl ScalarUDFImpl for NVLFunc {
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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!(
                "nvl expects exactly two arguments, but received {}",
                arg_types.len()
            );
        }
        let listed = arg_types
            .iter()
            .all(|t| t.is_null() || SUPPORTED_NVL_TYPES.contains(t));
        if !listed && let Ok(coerced) = self.coalesce.coerce_types(arg_types) {
            return Ok(coerced);
        }

        uniform_coercion(arg_types)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
