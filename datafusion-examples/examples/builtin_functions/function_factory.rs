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
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{exec_datafusion_err, exec_err, internal_err, DataFusionError};
use datafusion::error::Result;
use datafusion::execution::context::{
    FunctionFactory, RegisterFunction, SessionContext, SessionState,
};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, CreateFunction, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use std::hash::Hash;
use std::result::Result as RResult;
use std::sync::Arc;

/// This example shows how to utilize [FunctionFactory] to implement simple
/// SQL-macro like functions using a `CREATE FUNCTION` statement. The same
/// functionality can support functions defined in any language or library.
///
/// Apart from [FunctionFactory], this example covers
/// [ScalarUDFImpl::simplify()] which is often used at the same time, to replace
/// a function call with another expression at runtime.
///
/// This example is rather simple and does not cover all cases required for a
/// real implementation.
pub async fn function_factory() -> Result<()> {
    // First we must configure the SessionContext with our function factory
    let ctx = SessionContext::new()
        // register custom function factory
        .with_function_factory(Arc::new(CustomFunctionFactory::default()));

    // With the function factory, we can now call `CREATE FUNCTION` SQL functions

    // Let us register a function called f which takes a single argument and
    // returns that value plus one
    let sql = r#"
    CREATE FUNCTION f1(BIGINT)
        RETURNS BIGINT
        RETURN $1 + 1
    "#;

    ctx.sql(sql).await?.show().await?;

    // Now, let us register a function called f2  which takes two arguments and
    // returns the first argument added to the result of calling f1 on that
    // argument
    let sql = r#"
    CREATE FUNCTION f2(BIGINT, BIGINT)
        RETURNS BIGINT
        RETURN $1 + f1($2)
    "#;

    ctx.sql(sql).await?.show().await?;

    // Invoke f2, and we expect to see 1 + (1 + 2) = 4
    // Note this function works on columns as well as constants.
    let sql = r#"
    SELECT f2(1, 2)
    "#;
    ctx.sql(sql).await?.show().await?;

    // Now we clean up the session by dropping the functions
    ctx.sql("DROP FUNCTION f1").await?.show().await?;
    ctx.sql("DROP FUNCTION f2").await?.show().await?;

    Ok(())
}

/// This is our FunctionFactory that is responsible for converting `CREATE
/// FUNCTION` statements into function instances
#[derive(Debug, Default)]
struct CustomFunctionFactory {}

#[async_trait::async_trait]
impl FunctionFactory for CustomFunctionFactory {
    /// This function takes the parsed `CREATE FUNCTION` statement and returns
    /// the function instance.
    async fn create(
        &self,
        _state: &SessionState,
        statement: CreateFunction,
    ) -> Result<RegisterFunction> {
        let f: ScalarFunctionWrapper = statement.try_into()?;

        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(f))))
    }
}

/// this function represents the newly created execution engine.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ScalarFunctionWrapper {
    /// The text of the function body, `$1 + f1($2)` in our example
    name: String,
    expr: Expr,
    signature: Signature,
    return_type: DataType,
}

impl ScalarUDFImpl for ScalarFunctionWrapper {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Since this function is always simplified to another expression, it
        // should never actually be invoked
        internal_err!("This function should not get invoked!")
    }

    /// The simplify function is called to simply a call such as `f2(2)`. This
    /// function parses the string and returns the resulting expression
    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let replacement = Self::replacement(&self.expr, &args)?;

        Ok(ExprSimplifyResult::Simplified(replacement))
    }

    fn output_ordering(&self, _input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(SortProperties::Unordered)
    }
}

impl ScalarFunctionWrapper {
    // replaces placeholders such as $1 with actual arguments (args[0]
    fn replacement(expr: &Expr, args: &[Expr]) -> Result<Expr> {
        let result = expr.clone().transform(|e| {
            let r = match e {
                Expr::Placeholder(placeholder) => {
                    let placeholder_position =
                        Self::parse_placeholder_identifier(&placeholder.id)?;
                    if placeholder_position < args.len() {
                        Transformed::yes(args[placeholder_position].clone())
                    } else {
                        exec_err!(
                            "Function argument {} not provided, argument missing!",
                            placeholder.id
                        )?
                    }
                }
                _ => Transformed::no(e),
            };

            Ok(r)
        })?;

        Ok(result.data)
    }
    // Finds placeholder identifier such as `$X` format where X >= 1
    fn parse_placeholder_identifier(placeholder: &str) -> Result<usize> {
        if let Some(value) = placeholder.strip_prefix('$') {
            Ok(value.parse().map(|v: usize| v - 1).map_err(|e| {
                exec_datafusion_err!("Placeholder `{placeholder}` parsing error: {e}!")
            })?)
        } else {
            exec_err!("Placeholder should start with `$`!")
        }
    }
}

/// This impl block creates a scalar function from
/// a parsed `CREATE FUNCTION` statement (`CreateFunction`)
impl TryFrom<CreateFunction> for ScalarFunctionWrapper {
    type Error = DataFusionError;

    fn try_from(definition: CreateFunction) -> RResult<Self, Self::Error> {
        Ok(Self {
            name: definition.name,
            expr: definition
                .params
                .function_body
                .expect("Expression has to be defined!"),
            return_type: definition
                .return_type
                .expect("Return type has to be defined!"),
            signature: Signature::exact(
                definition
                    .args
                    .unwrap_or_default()
                    .into_iter()
                    .map(|a| a.data_type)
                    .collect(),
                definition.params.behavior.unwrap_or(Volatility::Volatile),
            ),
        })
    }
}
