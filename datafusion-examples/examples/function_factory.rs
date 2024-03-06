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

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::error::Result;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::{
    FunctionFactory, RegisterFunction, SessionContext, SessionState,
};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{exec_err, internal_err, DataFusionError};
use datafusion_expr::simplify::ExprSimplifyResult;
use datafusion_expr::simplify::SimplifyInfo;
use datafusion_expr::{CreateFunction, Expr, ScalarUDF, ScalarUDFImpl, Signature};
use std::result::Result as RResult;
use std::sync::Arc;

/// This example shows how to utilize [FunctionFactory] to register
/// `CREATE FUNCTION` handler. Apart from [FunctionFactory] this
/// example covers [ScalarUDFImpl::simplify()] usage and synergy
/// between those two functionality.
///
/// This example is rather simple, there are many edge cases to be covered
///

#[tokio::main]
async fn main() -> Result<()> {
    let runtime_config = RuntimeConfig::new();
    let runtime_environment = RuntimeEnv::new(runtime_config)?;

    let session_config = SessionConfig::new();
    let state =
        SessionState::new_with_config_rt(session_config, Arc::new(runtime_environment))
            // register custom function factory
            .with_function_factory(Arc::new(CustomFunctionFactory::default()));

    let ctx = SessionContext::new_with_state(state);

    let sql = r#"
    CREATE FUNCTION f1(BIGINT)
        RETURNS BIGINT
        RETURN $1 + 1
    "#;

    ctx.sql(sql).await?.show().await?;

    let sql = r#"
    CREATE FUNCTION f2(BIGINT, BIGINT)
        RETURNS BIGINT
        RETURN $1 + f1($2)
    "#;

    ctx.sql(sql).await?.show().await?;

    let sql = r#"
    SELECT f1(1)
    "#;

    ctx.sql(sql).await?.show().await?;

    let sql = r#"
    SELECT f2(1, 2)
    "#;

    ctx.sql(sql).await?.show().await?;

    let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

    ctx.register_batch("t", batch)?;

    let sql = r#"
    SELECT f2(a, b) from t
    "#;

    ctx.sql(sql).await?.show().await?;

    ctx.sql("DROP FUNCTION f1").await?.show().await?;

    ctx.sql("DROP FUNCTION f2").await?.show().await?;

    Ok(())
}

#[derive(Debug, Default)]
struct CustomFunctionFactory {}

#[async_trait::async_trait]
impl FunctionFactory for CustomFunctionFactory {
    async fn create(
        &self,
        _state: &SessionConfig,
        statement: CreateFunction,
    ) -> Result<RegisterFunction> {
        let f: ScalarFunctionWrapper = statement.try_into()?;

        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(f))))
    }
}
// a wrapper type to be used to register
// custom function to datafusion context
//
// it also defines custom [ScalarUDFImpl::simplify()]
// to replace ScalarUDF expression with one instance contains.
#[derive(Debug)]
struct ScalarFunctionWrapper {
    name: String,
    expr: Expr,
    signature: Signature,
    return_type: arrow_schema::DataType,
}

impl ScalarUDFImpl for ScalarFunctionWrapper {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[arrow_schema::DataType],
    ) -> Result<arrow_schema::DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(
        &self,
        _args: &[datafusion_expr::ColumnarValue],
    ) -> Result<datafusion_expr::ColumnarValue> {
        internal_err!("This function should not get invoked!")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let replacement = Self::replacement(&self.expr, &args)?;

        Ok(ExprSimplifyResult::Simplified(replacement))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn monotonicity(&self) -> Result<Option<datafusion_expr::FuncMonotonicity>> {
        Ok(None)
    }
}

impl ScalarFunctionWrapper {
    // replaces placeholders with actual arguments
    fn replacement(expr: &Expr, args: &[Expr]) -> Result<Expr> {
        let result = expr.clone().transform(&|e| {
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
    // Finds placeholder identifier.
    // placeholders are in `$X` format where X >= 1
    fn parse_placeholder_identifier(placeholder: &str) -> Result<usize> {
        if let Some(value) = placeholder.strip_prefix('$') {
            Ok(value.parse().map(|v: usize| v - 1).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Placeholder `{}` parsing error: {}!",
                    placeholder, e
                ))
            })?)
        } else {
            exec_err!("Placeholder should start with `$`!")
        }
    }
}

impl TryFrom<CreateFunction> for ScalarFunctionWrapper {
    type Error = DataFusionError;

    fn try_from(definition: CreateFunction) -> RResult<Self, Self::Error> {
        Ok(Self {
            name: definition.name,
            expr: definition
                .params
                .return_
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
                definition
                    .params
                    .behavior
                    .unwrap_or(datafusion_expr::Volatility::Volatile),
            ),
        })
    }
}
