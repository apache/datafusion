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

//! Collection of utility functions that are leveraged by the query optimizer rules

use super::optimizer::OptimizerRule;
use crate::execution::context::ExecutionProps;
use datafusion_expr::logical_plan::Filter;

use crate::error::{DataFusionError, Result};
use crate::logical_plan::{and, Expr, LogicalPlan, Operator};
use crate::prelude::lit;
use crate::scalar::ScalarValue;
use datafusion_expr::expr::GroupingSet;
use datafusion_expr::utils::from_plan;
use std::sync::Arc;

const CASE_EXPR_MARKER: &str = "__DATAFUSION_CASE_EXPR__";
const CASE_ELSE_MARKER: &str = "__DATAFUSION_CASE_ELSE__";
const WINDOW_PARTITION_MARKER: &str = "__DATAFUSION_WINDOW_PARTITION__";
const WINDOW_SORT_MARKER: &str = "__DATAFUSION_WINDOW_SORT__";

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
/// This also handles the case when the `plan` is a [`LogicalPlan::Explain`].
pub fn optimize_children(
    optimizer: &impl OptimizerRule,
    plan: &LogicalPlan,
    execution_props: &ExecutionProps,
) -> Result<LogicalPlan> {
    let new_exprs = plan.expressions();
    let new_inputs = plan
        .inputs()
        .into_iter()
        .map(|plan| optimizer.optimize(plan, execution_props))
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, &new_exprs, &new_inputs)
}

/// Returns all direct children `Expression`s of `expr`.
/// E.g. if the expression is "(a + 1) + 1", it returns ["a + 1", "1"] (as Expr objects)
pub fn expr_sub_expressions(expr: &Expr) -> Result<Vec<Expr>> {
    match expr {
        Expr::BinaryExpr { left, right, .. } => {
            Ok(vec![left.as_ref().to_owned(), right.as_ref().to_owned()])
        }
        Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. }
        | Expr::TryCast { expr, .. }
        | Expr::Alias(expr, ..)
        | Expr::Not(expr)
        | Expr::Negative(expr)
        | Expr::Sort { expr, .. }
        | Expr::GetIndexedField { expr, .. } => Ok(vec![expr.as_ref().to_owned()]),
        Expr::ScalarFunction { args, .. }
        | Expr::ScalarUDF { args, .. }
        | Expr::AggregateFunction { args, .. }
        | Expr::AggregateUDF { args, .. } => Ok(args.clone()),
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::Rollup(exprs) => Ok(exprs.clone()),
            GroupingSet::Cube(exprs) => Ok(exprs.clone()),
            GroupingSet::GroupingSets(_) => Err(DataFusionError::Plan(
                "GroupingSets are not supported yet".to_string(),
            )),
        },
        Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            let mut expr_list: Vec<Expr> = vec![];
            expr_list.extend(args.clone());
            expr_list.push(lit(WINDOW_PARTITION_MARKER));
            expr_list.extend(partition_by.clone());
            expr_list.push(lit(WINDOW_SORT_MARKER));
            expr_list.extend(order_by.clone());
            Ok(expr_list)
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
            ..
        } => {
            let mut expr_list: Vec<Expr> = vec![];
            if let Some(e) = expr {
                expr_list.push(lit(CASE_EXPR_MARKER));
                expr_list.push(e.as_ref().to_owned());
            }
            for (w, t) in when_then_expr {
                expr_list.push(w.as_ref().to_owned());
                expr_list.push(t.as_ref().to_owned());
            }
            if let Some(e) = else_expr {
                expr_list.push(lit(CASE_ELSE_MARKER));
                expr_list.push(e.as_ref().to_owned());
            }
            Ok(expr_list)
        }
        Expr::Column(_) | Expr::Literal(_) | Expr::ScalarVariable(_, _) => Ok(vec![]),
        Expr::Between {
            expr, low, high, ..
        } => Ok(vec![
            expr.as_ref().to_owned(),
            low.as_ref().to_owned(),
            high.as_ref().to_owned(),
        ]),
        Expr::InList { expr, list, .. } => {
            let mut expr_list: Vec<Expr> = vec![expr.as_ref().to_owned()];
            for list_expr in list {
                expr_list.push(list_expr.to_owned());
            }
            Ok(expr_list)
        }
        Expr::Exists { .. } => Ok(vec![]),
        Expr::InSubquery { expr, .. } => Ok(vec![expr.as_ref().to_owned()]),
        Expr::ScalarSubquery(_) => Ok(vec![]),
        Expr::Wildcard { .. } => Err(DataFusionError::Internal(
            "Wildcard expressions are not valid in a logical query plan".to_owned(),
        )),
        Expr::QualifiedWildcard { .. } => Err(DataFusionError::Internal(
            "QualifiedWildcard expressions are not valid in a logical query plan"
                .to_owned(),
        )),
    }
}

/// returns a new expression where the expressions in `expr` are replaced by the ones in
/// `expressions`.
/// This is used in conjunction with ``expr_expressions`` to re-write expressions.
pub fn rewrite_expression(expr: &Expr, expressions: &[Expr]) -> Result<Expr> {
    match expr {
        Expr::BinaryExpr { op, .. } => Ok(Expr::BinaryExpr {
            left: Box::new(expressions[0].clone()),
            op: *op,
            right: Box::new(expressions[1].clone()),
        }),
        Expr::IsNull(_) => Ok(Expr::IsNull(Box::new(expressions[0].clone()))),
        Expr::IsNotNull(_) => Ok(Expr::IsNotNull(Box::new(expressions[0].clone()))),
        Expr::ScalarFunction { fun, .. } => Ok(Expr::ScalarFunction {
            fun: fun.clone(),
            args: expressions.to_vec(),
        }),
        Expr::ScalarUDF { fun, .. } => Ok(Expr::ScalarUDF {
            fun: fun.clone(),
            args: expressions.to_vec(),
        }),
        Expr::WindowFunction {
            fun, window_frame, ..
        } => {
            let partition_index = expressions
                .iter()
                .position(|expr| {
                    matches!(expr, Expr::Literal(ScalarValue::Utf8(Some(str)))
            if str == WINDOW_PARTITION_MARKER)
                })
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Ill-formed window function expressions: unexpected marker"
                            .to_owned(),
                    )
                })?;

            let sort_index = expressions
                .iter()
                .position(|expr| {
                    matches!(expr, Expr::Literal(ScalarValue::Utf8(Some(str)))
            if str == WINDOW_SORT_MARKER)
                })
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Ill-formed window function expressions".to_owned(),
                    )
                })?;

            if partition_index >= sort_index {
                Err(DataFusionError::Internal(
                    "Ill-formed window function expressions: partition index too large"
                        .to_owned(),
                ))
            } else {
                Ok(Expr::WindowFunction {
                    fun: fun.clone(),
                    args: expressions[..partition_index].to_vec(),
                    partition_by: expressions[partition_index + 1..sort_index].to_vec(),
                    order_by: expressions[sort_index + 1..].to_vec(),
                    window_frame: *window_frame,
                })
            }
        }
        Expr::AggregateFunction { fun, distinct, .. } => Ok(Expr::AggregateFunction {
            fun: fun.clone(),
            args: expressions.to_vec(),
            distinct: *distinct,
        }),
        Expr::AggregateUDF { fun, .. } => Ok(Expr::AggregateUDF {
            fun: fun.clone(),
            args: expressions.to_vec(),
        }),
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::Rollup(_exprs) => {
                Ok(Expr::GroupingSet(GroupingSet::Rollup(expressions.to_vec())))
            }
            GroupingSet::Cube(_exprs) => {
                Ok(Expr::GroupingSet(GroupingSet::Rollup(expressions.to_vec())))
            }
            GroupingSet::GroupingSets(_) => Err(DataFusionError::Plan(
                "GroupingSets are not supported yet".to_string(),
            )),
        },
        Expr::Case { .. } => {
            let mut base_expr: Option<Box<Expr>> = None;
            let mut when_then: Vec<(Box<Expr>, Box<Expr>)> = vec![];
            let mut else_expr: Option<Box<Expr>> = None;
            let mut i = 0;

            while i < expressions.len() {
                match &expressions[i] {
                    Expr::Literal(ScalarValue::Utf8(Some(str)))
                        if str == CASE_EXPR_MARKER =>
                    {
                        base_expr = Some(Box::new(expressions[i + 1].clone()));
                        i += 2;
                    }
                    Expr::Literal(ScalarValue::Utf8(Some(str)))
                        if str == CASE_ELSE_MARKER =>
                    {
                        else_expr = Some(Box::new(expressions[i + 1].clone()));
                        i += 2;
                    }
                    _ => {
                        when_then.push((
                            Box::new(expressions[i].clone()),
                            Box::new(expressions[i + 1].clone()),
                        ));
                        i += 2;
                    }
                }
            }

            Ok(Expr::Case {
                expr: base_expr,
                when_then_expr: when_then,
                else_expr,
            })
        }
        Expr::Cast { data_type, .. } => Ok(Expr::Cast {
            expr: Box::new(expressions[0].clone()),
            data_type: data_type.clone(),
        }),
        Expr::TryCast { data_type, .. } => Ok(Expr::TryCast {
            expr: Box::new(expressions[0].clone()),
            data_type: data_type.clone(),
        }),
        Expr::Alias(_, alias) => {
            Ok(Expr::Alias(Box::new(expressions[0].clone()), alias.clone()))
        }
        Expr::Not(_) => Ok(Expr::Not(Box::new(expressions[0].clone()))),
        Expr::Negative(_) => Ok(Expr::Negative(Box::new(expressions[0].clone()))),
        Expr::Column(_)
        | Expr::Literal(_)
        | Expr::InList { .. }
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::ScalarSubquery(_)
        | Expr::ScalarVariable(_, _) => Ok(expr.clone()),
        Expr::Sort {
            asc, nulls_first, ..
        } => Ok(Expr::Sort {
            expr: Box::new(expressions[0].clone()),
            asc: *asc,
            nulls_first: *nulls_first,
        }),
        Expr::Between { negated, .. } => {
            let expr = Expr::BinaryExpr {
                left: Box::new(Expr::BinaryExpr {
                    left: Box::new(expressions[0].clone()),
                    op: Operator::GtEq,
                    right: Box::new(expressions[1].clone()),
                }),
                op: Operator::And,
                right: Box::new(Expr::BinaryExpr {
                    left: Box::new(expressions[0].clone()),
                    op: Operator::LtEq,
                    right: Box::new(expressions[2].clone()),
                }),
            };

            if *negated {
                Ok(Expr::Not(Box::new(expr)))
            } else {
                Ok(expr)
            }
        }
        Expr::Wildcard => Err(DataFusionError::Internal(
            "Wildcard expressions are not valid in a logical query plan".to_owned(),
        )),
        Expr::QualifiedWildcard { .. } => Err(DataFusionError::Internal(
            "QualifiedWildcard expressions are not valid in a logical query plan"
                .to_owned(),
        )),
        Expr::GetIndexedField { expr: _, key } => Ok(Expr::GetIndexedField {
            expr: Box::new(expressions[0].clone()),
            key: key.clone(),
        }),
    }
}

/// converts "A AND B AND C" => [A, B, C]
pub fn split_conjunction<'a>(predicate: &'a Expr, predicates: &mut Vec<&'a Expr>) {
    match predicate {
        Expr::BinaryExpr {
            right,
            op: Operator::And,
            left,
        } => {
            split_conjunction(left, predicates);
            split_conjunction(right, predicates);
        }
        Expr::Alias(expr, _) => {
            split_conjunction(expr, predicates);
        }
        other => predicates.push(other),
    }
}

/// returns a new [LogicalPlan] that wraps `plan` in a [LogicalPlan::Filter] with
/// its predicate be all `predicates` ANDed.
pub fn add_filter(plan: LogicalPlan, predicates: &[&Expr]) -> LogicalPlan {
    // reduce filters to a single filter with an AND
    let predicate = predicates
        .iter()
        .skip(1)
        .fold(predicates[0].clone(), |acc, predicate| {
            and(acc, (*predicate).to_owned())
        });

    LogicalPlan::Filter(Filter {
        predicate,
        input: Arc::new(plan),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::col;
    use arrow::datatypes::DataType;
    use datafusion_common::Column;
    use datafusion_expr::utils::expr_to_columns;
    use std::collections::HashSet;

    #[test]
    fn test_collect_expr() -> Result<()> {
        let mut accum: HashSet<Column> = HashSet::new();
        expr_to_columns(
            &Expr::Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Float64,
            },
            &mut accum,
        )?;
        expr_to_columns(
            &Expr::Cast {
                expr: Box::new(col("a")),
                data_type: DataType::Float64,
            },
            &mut accum,
        )?;
        assert_eq!(1, accum.len());
        assert!(accum.contains(&Column::from_name("a")));
        Ok(())
    }
}
