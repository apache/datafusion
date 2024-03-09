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

//! Analyzer rule for to replace operators with function calls (e.g `||` to array_concat`)

use std::sync::Arc;

use super::AnalyzerRule;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::utils::list_ndims;
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::rewrite_preserving_name;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    BinaryExpr, BuiltinScalarFunction, Expr, LogicalPlan, Operator,
    ScalarFunctionDefinition,
};

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
        analyze_internal(&plan)
    }
}

fn analyze_internal(plan: &LogicalPlan) -> Result<LogicalPlan> {
    // optimize child plans first
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|p| analyze_internal(p))
        .collect::<Result<Vec<_>>>()?;

    // get schema representing all available input fields. This is used for data type
    // resolution only, so order does not matter here
    let mut schema = merge_schema(new_inputs.iter().collect());

    if let LogicalPlan::TableScan(ts) = plan {
        let source_schema =
            DFSchema::try_from_qualified_schema(&ts.table_name, &ts.source.schema())?;
        schema.merge(&source_schema);
    }

    let mut expr_rewrite = OperatorToFunctionRewriter {
        schema: Arc::new(schema),
    };

    let new_expr = plan
        .expressions()
        .into_iter()
        .map(|expr| {
            // ensure names don't change:
            // https://github.com/apache/arrow-datafusion/issues/3555
            rewrite_preserving_name(expr, &mut expr_rewrite)
        })
        .collect::<Result<Vec<_>>>()?;

    plan.with_new_exprs(new_expr, new_inputs)
}

pub(crate) struct OperatorToFunctionRewriter {
    pub(crate) schema: DFSchemaRef,
}

impl TreeNodeRewriter for OperatorToFunctionRewriter {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::BinaryExpr(BinaryExpr {
            ref left,
            op,
            ref right,
        }) = expr
        {
            if let Some(fun) = rewrite_array_concat_operator_to_func_for_column(
                left.as_ref(),
                op,
                right.as_ref(),
                self.schema.as_ref(),
            )?
            .or_else(|| {
                rewrite_array_concat_operator_to_func(left.as_ref(), op, right.as_ref())
            }) {
                // Convert &Box<Expr> -> Expr
                let left = (**left).clone();
                let right = (**right).clone();
                return Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                    func_def: ScalarFunctionDefinition::BuiltIn(fun),
                    args: vec![left, right],
                })));
            }

            // TODO: change OperatorToFunction to OperatoToArrayFunction and configure it with array_expressions feature
            // after other array functions are udf-based
            #[cfg(feature = "array_expressions")]
            if let Some(expr) = rewrite_array_has_all_operator_to_func(left, op, right) {
                return Ok(Transformed::yes(expr));
            }
        }
        Ok(Transformed::no(expr))
    }
}

// Note This rewrite is only done if the built in DataFusion `array_expressions` feature is enabled.
// Even if users  implement their own array functions, those functions are not equal to the DataFusion
// udf based array functions, so this rewrite is not corrrect
#[cfg(feature = "array_expressions")]
fn rewrite_array_has_all_operator_to_func(
    left: &Expr,
    op: Operator,
    right: &Expr,
) -> Option<Expr> {
    use super::array_has_all;

    if op != Operator::AtArrow && op != Operator::ArrowAt {
        return None;
    }

    match (left, right) {
        // array1 @> array2 -> array_has_all(array1, array2)
        // array1 <@ array2 -> array_has_all(array2, array1)
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
        ) => {
            let left = left.clone();
            let right = right.clone();

            let expr = if let Operator::ArrowAt = op {
                array_has_all(right, left)
            } else {
                array_has_all(left, right)
            };
            Some(expr)
        }
        _ => None,
    }
}

/// Summary of the logic below:
///
/// 1) array || array -> array concat
///
/// 2) array || scalar -> array append
///
/// 3) scalar || array -> array prepend
///
/// 4) (arry concat, array append, array prepend) || array -> array concat
///
/// 5) (arry concat, array append, array prepend) || scalar -> array append
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

/// Summary of the logic below:
///
/// 1) (arry concat, array append, array prepend) || column -> (array append, array concat)
///
/// 2) column1 || column2 -> (array prepend, array append, array concat)
fn rewrite_array_concat_operator_to_func_for_column(
    left: &Expr,
    op: Operator,
    right: &Expr,
    schema: &DFSchema,
) -> Result<Option<BuiltinScalarFunction>> {
    if op != Operator::StringConcat {
        return Ok(None);
    }

    match (left, right) {
        // Column cases:
        // 1) array_prepend/append/concat || column
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayPrepend),
                args: _left_args,
            }),
            Expr::Column(c),
        )
        | (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayAppend),
                args: _left_args,
            }),
            Expr::Column(c),
        )
        | (
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::ArrayConcat),
                args: _left_args,
            }),
            Expr::Column(c),
        ) => {
            let d = schema.field_from_column(c)?.data_type();
            let ndim = list_ndims(d);
            match ndim {
                0 => Ok(Some(BuiltinScalarFunction::ArrayAppend)),
                _ => Ok(Some(BuiltinScalarFunction::ArrayConcat)),
            }
        }
        // 2) select column1 || column2
        (Expr::Column(c1), Expr::Column(c2)) => {
            let d1 = schema.field_from_column(c1)?.data_type();
            let d2 = schema.field_from_column(c2)?.data_type();
            let ndim1 = list_ndims(d1);
            let ndim2 = list_ndims(d2);
            match (ndim1, ndim2) {
                (0, _) => Ok(Some(BuiltinScalarFunction::ArrayPrepend)),
                (_, 0) => Ok(Some(BuiltinScalarFunction::ArrayAppend)),
                _ => Ok(Some(BuiltinScalarFunction::ArrayConcat)),
            }
        }
        _ => Ok(None),
    }
}
