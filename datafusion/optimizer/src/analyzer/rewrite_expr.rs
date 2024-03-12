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

#[cfg(feature = "array_expressions")]
use std::sync::Arc;

use super::AnalyzerRule;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
#[cfg(feature = "array_expressions")]
use datafusion_common::{utils::list_ndims, DFSchemaRef};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::rewrite_preserving_name;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::BuiltinScalarFunction;
use datafusion_expr::GetFieldAccess;
use datafusion_expr::GetIndexedField;
#[cfg(feature = "array_expressions")]
use datafusion_expr::{BinaryExpr, Operator, ScalarFunctionDefinition};
use datafusion_expr::{Expr, LogicalPlan};
#[cfg(feature = "array_expressions")]
use datafusion_functions_array::expr_fn::{array_append, array_concat, array_prepend};

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
        #[cfg(feature = "array_expressions")]
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
    #[cfg(feature = "array_expressions")]
    pub(crate) schema: DFSchemaRef,
}

impl TreeNodeRewriter for OperatorToFunctionRewriter {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        #[cfg(feature = "array_expressions")]
        if let Expr::BinaryExpr(BinaryExpr {
            ref left,
            op,
            ref right,
        }) = expr
        {
            if let Some(expr) = rewrite_array_concat_operator_to_func_for_column(
                left.as_ref(),
                op,
                right.as_ref(),
                self.schema.as_ref(),
            )?
            .or_else(|| {
                rewrite_array_concat_operator_to_func(left.as_ref(), op, right.as_ref())
            }) {
                // Convert &Box<Expr> -> Expr
                return Ok(Transformed::yes(expr));
            }

            // TODO: change OperatorToFunction to OperatoToArrayFunction and configure it with array_expressions feature
            // after other array functions are udf-based
            #[cfg(feature = "array_expressions")]
            if let Some(expr) = rewrite_array_has_all_operator_to_func(left, op, right) {
                return Ok(Transformed::yes(expr));
            }
        }

        if let Expr::GetIndexedField(GetIndexedField {
            ref expr,
            ref field,
        }) = expr
        {
            match field {
                GetFieldAccess::ListIndex { ref key } => {
                    let expr = *expr.clone();
                    let key = *key.clone();
                    let args = vec![expr, key];
                    return Ok(Transformed::yes(Expr::ScalarFunction(
                        ScalarFunction::new(BuiltinScalarFunction::ArrayElement, args),
                    )));
                }
                GetFieldAccess::ListRange {
                    start,
                    stop,
                    stride,
                } => {
                    let expr = *expr.clone();
                    let start = *start.clone();
                    let stop = *stop.clone();
                    let stride = *stride.clone();
                    let args = vec![expr, start, stop, stride];
                    return Ok(Transformed::yes(Expr::ScalarFunction(
                        ScalarFunction::new(BuiltinScalarFunction::ArraySlice, args),
                    )));
                }
                _ => {}
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
                func_def: ScalarFunctionDefinition::UDF(left_fun),
                args: _left_args,
            }),
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(right_fun),
                args: _right_args,
            }),
        ) if left_fun.name() == "make_array" && right_fun.name() == "make_array" => {
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
#[cfg(feature = "array_expressions")]
fn rewrite_array_concat_operator_to_func(
    left: &Expr,
    op: Operator,
    right: &Expr,
) -> Option<Expr> {
    // Convert `Array StringConcat Array` to ScalarFunction::ArrayConcat

    if op != Operator::StringConcat {
        return None;
    }

    match (left, right) {
        // Chain concat operator (a || b) || array,
        // (arry concat, array append, array prepend) || array -> array concat
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(left_fun),
                args: _left_args,
            }),
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(right_fun),
                args: _right_args,
            }),
        ) if ["array_append", "array_prepend", "array_concat"]
            .contains(&left_fun.name())
            && right_fun.name() == "make_array" =>
        {
            Some(array_concat(vec![left.clone(), right.clone()]))
        }
        // Chain concat operator (a || b) || scalar,
        // (arry concat, array append, array prepend) || scalar -> array append
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(left_fun),
                args: _left_args,
            }),
            _scalar,
        ) if ["array_append", "array_prepend", "array_concat"]
            .contains(&left_fun.name()) =>
        {
            Some(array_append(left.clone(), right.clone()))
        }
        // array || array -> array concat
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(left_fun),
                args: _left_args,
            }),
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(right_fun),
                args: _right_args,
            }),
        ) if left_fun.name() == "make_array" && right_fun.name() == "make_array" => {
            Some(array_concat(vec![left.clone(), right.clone()]))
        }
        // array || scalar -> array append
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(left_fun),
                args: _left_args,
            }),
            _right_scalar,
        ) if left_fun.name() == "make_array" => {
            Some(array_append(left.clone(), right.clone()))
        }
        // scalar || array -> array prepend
        (
            _left_scalar,
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(right_fun),
                args: _right_args,
            }),
        ) if right_fun.name() == "make_array" => {
            Some(array_prepend(left.clone(), right.clone()))
        }

        _ => None,
    }
}

/// Summary of the logic below:
///
/// 1) (arry concat, array append, array prepend) || column -> (array append, array concat)
///
/// 2) column1 || column2 -> (array prepend, array append, array concat)
#[cfg(feature = "array_expressions")]
fn rewrite_array_concat_operator_to_func_for_column(
    left: &Expr,
    op: Operator,
    right: &Expr,
    schema: &DFSchema,
) -> Result<Option<Expr>> {
    if op != Operator::StringConcat {
        return Ok(None);
    }

    match (left, right) {
        // Column cases:
        // 1) array_prepend/append/concat || column
        (
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(left_fun),
                args: _left_args,
            }),
            Expr::Column(c),
        ) if ["array_append", "array_prepend", "array_concat"]
            .contains(&left_fun.name()) =>
        {
            let d = schema.field_from_column(c)?.data_type();
            let ndim = list_ndims(d);
            match ndim {
                0 => Ok(Some(array_append(left.clone(), right.clone()))),
                _ => Ok(Some(array_concat(vec![left.clone(), right.clone()]))),
            }
        }
        // 2) select column1 || column2
        (Expr::Column(c1), Expr::Column(c2)) => {
            let d1 = schema.field_from_column(c1)?.data_type();
            let d2 = schema.field_from_column(c2)?.data_type();
            let ndim1 = list_ndims(d1);
            let ndim2 = list_ndims(d2);
            match (ndim1, ndim2) {
                (0, _) => Ok(Some(array_prepend(left.clone(), right.clone()))),
                (_, 0) => Ok(Some(array_append(left.clone(), right.clone()))),
                _ => Ok(Some(array_concat(vec![left.clone(), right.clone()]))),
            }
        }
        _ => Ok(None),
    }
}
