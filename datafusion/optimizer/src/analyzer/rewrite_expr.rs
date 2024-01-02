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

//! Optimizer rule for expression rewrite

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::BuiltinScalarFunction;
use datafusion_expr::Operator;
use datafusion_expr::Projection;
use datafusion_expr::ScalarFunctionDefinition;
use datafusion_expr::{BinaryExpr, Expr, LogicalPlan};

use super::AnalyzerRule;

#[derive(Default)]
pub struct OperatorToFunction {}

impl OperatorToFunction {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for OperatorToFunction {
    fn name(&self) -> &str {
        "operator_to_function"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        analyze_internal(plan)
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<LogicalPlan> {
    // OperatorToFunction is only applied to Projection
    match plan {
        LogicalPlan::Projection(_) => {}
        _ => {
            return Ok(plan);
        }
    }

    let mut expr_rewriter = OperatorToFunctionRewriter {};

    let new_expr = plan
        .expressions()
        .into_iter()
        .map(|expr| expr.rewrite(&mut expr_rewriter))
        .collect::<Result<Vec<_>>>()?;

    // Not found cases that inputs more than one
    assert_eq!(plan.inputs().len(), 1);
    let input = plan.inputs()[0];

    Ok(LogicalPlan::Projection(Projection::try_new(
        new_expr,
        input.to_owned().into(),
    )?))
}

pub(crate) struct OperatorToFunctionRewriter {}

impl TreeNodeRewriter for OperatorToFunctionRewriter {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                op,
                ref right,
            }) => {
                if let Some(fun) = rewrite_array_concat_operator_to_func(
                    left.as_ref(),
                    op,
                    right.as_ref(),
                ) {
                    // Convert &Box<Expr> -> Expr
                    let left = (**left).clone();
                    let right = (**right).clone();
                    return Ok(Expr::ScalarFunction(ScalarFunction {
                        func_def: ScalarFunctionDefinition::BuiltIn(fun),
                        args: vec![left, right],
                    }));
                }
                Ok(expr)
            }
            _ => Ok(expr),
        }
    }
}

/// Summary of the logic below:
///
/// array || array -> array concat
///
/// array || scalar -> array append
///
/// scalar || array -> array prepend
///
/// (arry concat, array append, array prepend) || array -> array concat
///
/// (arry concat, array append, array prepend) || scalar -> array append
fn rewrite_array_concat_operator_to_func(
    left: &Expr,
    op: Operator,
    right: &Expr,
) -> Option<BuiltinScalarFunction> {
    // Convert `Array StringConcat Array` to ScalarFunction::ArrayConcat

    if op != Operator::StringConcat {
        return None;
    }

    match (left, right) {
        // Chain concat operator (a || b) || array,
        // (arry concat, array append, array prepend) || array -> array concat
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayConcat),
                args: _left_args,
            }),
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::MakeArray),
                args: _right_args,
            }),
        )
        | (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayAppend),
                args: _left_args,
            }),
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::MakeArray),
                args: _right_args,
            }),
        )
        | (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayPrepend),
                args: _left_args,
            }),
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::MakeArray),
                args: _right_args,
            }),
        ) => Some(BuiltinScalarFunction::ArrayConcat),
        // Chain concat operator (a || b) || scalar,
        // (arry concat, array append, array prepend) || scalar -> array append
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayConcat),
                args: _left_args,
            }),
            _scalar,
        )
        | (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayAppend),
                args: _left_args,
            }),
            _scalar,
        )
        | (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayPrepend),
                args: _left_args,
            }),
            _scalar,
        ) => Some(BuiltinScalarFunction::ArrayAppend),
        // array || array -> array concat
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::MakeArray),
                args: _left_args,
            }),
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::MakeArray),
                args: _right_args,
            }),
        ) => Some(BuiltinScalarFunction::ArrayConcat),
        // array || scalar -> array append
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::MakeArray),
                args: _left_args,
            }),
            _right_scalar,
        ) => Some(BuiltinScalarFunction::ArrayAppend),
        // scalar || array -> array prepend
        (
            _left_scalar,
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::MakeArray),
                args: _right_args,
            }),
        ) => Some(BuiltinScalarFunction::ArrayPrepend),

        _ => None,
    }
}
