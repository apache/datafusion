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

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DFSchemaRef};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::logical_plan::Projection;
use datafusion_expr::{
    and, combine_filters,
    logical_plan::{Filter, LogicalPlan},
    utils::from_plan,
    Expr, Operator,
};
use std::collections::HashSet;
use std::sync::Arc;

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
/// This also handles the case when the `plan` is a [`LogicalPlan::Explain`].
pub fn optimize_children(
    optimizer: &impl OptimizerRule,
    plan: &LogicalPlan,
    optimizer_config: &mut OptimizerConfig,
) -> Result<LogicalPlan> {
    let new_exprs = plan.expressions();
    let new_inputs = plan
        .inputs()
        .into_iter()
        .map(|plan| optimizer.optimize(plan, optimizer_config))
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, &new_exprs, &new_inputs)
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

/// Looks for correlating expressions: equality expressions with one field from the subquery, and
/// one not in the subquery (closed upon from outer scope)
///
/// # Arguments
///
/// * `exprs` - List of expressions that may or may not be joins
/// * `fields` - HashSet of fully qualified (table.col) fields in subquery schema
///
/// # Return value
///
/// Tuple of (expressions containing joins, remaining non-join expressions)
pub fn find_join_exprs(
    exprs: Vec<&Expr>,
    schema: &DFSchemaRef,
) -> (Vec<Expr>, Vec<Expr>) {
    let fields: HashSet<_> = schema
        .fields()
        .iter()
        .map(|it| it.qualified_name())
        .collect();

    let mut joins = vec![];
    let mut others = vec![];
    for filter in exprs.iter() {
        let (left, op, right) = match filter {
            Expr::BinaryExpr { left, op, right } => (*left.clone(), *op, *right.clone()),
            _ => {
                others.push((*filter).clone());
                continue;
            }
        };
        match op {
            Operator::Eq => {}
            Operator::NotEq => {}
            _ => {
                others.push((*filter).clone());
                continue; // not a column=column expression
            }
        }
        let left = match left {
            Expr::Column(c) => c,
            _ => {
                others.push((*filter).clone());
                continue;
            }
        };
        let right = match right {
            Expr::Column(c) => c,
            _ => {
                others.push((*filter).clone());
                continue;
            }
        };
        if fields.contains(&left.flat_name()) && fields.contains(&right.flat_name()) {
            others.push((*filter).clone());
            continue; // both columns present (none closed-upon)
        }
        if !fields.contains(&left.flat_name()) && !fields.contains(&right.flat_name()) {
            others.push((*filter).clone());
            continue; // neither column present (syntax error?)
        }

        joins.push((*filter).clone())
    }

    (joins, others)
}

/// Extracts correlating columns from expressions
///
/// # Arguments
///
/// * `exprs` - List of expressions that correlate a subquery to an outer scope
/// * `fields` - HashSet of fully qualified (table.col) fields in subquery schema
///
/// # Return value
///
/// Tuple of tuples ((outer-scope cols, subquery cols), non-equal expressions)
pub fn exprs_to_join_cols(
    exprs: &[Expr],
    schema: &DFSchemaRef,
    include_negated: bool,
) -> Result<(Vec<Column>, Vec<Column>, Option<Expr>)> {
    let fields: HashSet<_> = schema
        .fields()
        .iter()
        .map(|it| it.qualified_name())
        .collect();

    let mut joins: Vec<(String, String)> = vec![];
    let mut others: Vec<Expr> = vec![];
    for filter in exprs.iter() {
        let (left, op, right) = match filter {
            Expr::BinaryExpr { left, op, right } => (*left.clone(), *op, *right.clone()),
            _ => Err(DataFusionError::Plan("Invalid expression!".to_string()))?,
        };
        match op {
            Operator::Eq => {}
            Operator::NotEq => {
                if !include_negated {
                    others.push((*filter).clone());
                    continue;
                }
            }
            _ => Err(DataFusionError::Plan("Invalid expression!".to_string()))?,
        }
        let left = match left {
            Expr::Column(c) => c,
            _ => Err(DataFusionError::Plan("Invalid expression!".to_string()))?,
        };
        let right = match right {
            Expr::Column(c) => c,
            _ => Err(DataFusionError::Plan("Invalid expression!".to_string()))?,
        };
        let sorted = if fields.contains(&left.flat_name()) {
            (right.flat_name(), left.flat_name())
        } else {
            (left.flat_name(), right.flat_name())
        };
        joins.push(sorted);
    }

    let (left_cols, right_cols): (Vec<_>, Vec<_>) = joins
        .into_iter()
        .map(|(l, r)| (Column::from(l.as_str()), Column::from(r.as_str())))
        .unzip();
    let pred = combine_filters(&others);

    Ok((left_cols, right_cols, pred))
}

pub fn proj_or_err(plan: &LogicalPlan) -> Result<&Projection> {
    match plan {
        LogicalPlan::Projection(it) => Ok(it),
        _ => Err(DataFusionError::Plan(
            "Could not coerce into projection!".to_string(),
        )),
    }
}

pub fn only_or_err<T>(slice: &[T]) -> Result<&T> {
    match slice {
        [it] => Ok(it),
        [] => Err(DataFusionError::Plan("Empty slice!".to_string())),
        _ => Err(DataFusionError::Plan(
            "More than one item in slice!".to_string(),
        )),
    }
}

pub fn col_or_err(expr: &Expr) -> Result<Column> {
    match expr {
        Expr::Column(it) => Ok(it.clone()),
        _ => Err(DataFusionError::Plan(
            "Could not coerce into column!".to_string(),
        )),
    }
}

pub fn merge_cols(a: &[Column], b: &[Column]) -> Vec<Column> {
    let a: Vec<_> = a.iter().map(|it| it.flat_name()).collect();
    let b: Vec<_> = b.iter().map(|it| it.flat_name()).collect();
    let c: HashSet<_> = a.iter().cloned().chain(b).collect();
    let mut res: Vec<_> = c.iter().map(|it| Column::from(it.as_str())).collect();
    res.sort();
    res
}

pub fn alias_cols(alias: &str, cols: &[Column]) -> Vec<Column> {
    cols.iter()
        .map(|it| Column {
            relation: Some(alias.to_string()),
            name: it.name.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion_common::Column;
    use datafusion_expr::{col, utils::expr_to_columns};
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
