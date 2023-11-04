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
use datafusion_common::DataFusionError;
use datafusion_common::{plan_err, Column, DFSchemaRef};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr::{Alias, BinaryExpr};
use datafusion_expr::expr_rewriter::{replace_col, strip_outer_reference};
use datafusion_expr::{
    and,
    logical_plan::{Filter, LogicalPlan},
    Expr, Operator,
};
use log::{debug, trace};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
/// This also handles the case when the `plan` is a [`LogicalPlan::Explain`].
///
/// Returning `Ok(None)` indicates that the plan can't be optimized by the `optimizer`.
pub fn optimize_children(
    optimizer: &impl OptimizerRule,
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    let mut plan_is_changed = false;
    for input in plan.inputs() {
        let new_input = optimizer.try_optimize(input, config)?;
        plan_is_changed = plan_is_changed || new_input.is_some();
        new_inputs.push(new_input.unwrap_or_else(|| input.clone()))
    }
    if plan_is_changed {
        Ok(Some(plan.with_new_inputs(&new_inputs)?))
    } else {
        Ok(None)
    }
}

/// Splits a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// See [`split_conjunction_owned`] for more details and an example.
pub fn split_conjunction(expr: &Expr) -> Vec<&Expr> {
    split_conjunction_impl(expr, vec![])
}

fn split_conjunction_impl<'a>(expr: &'a Expr, mut exprs: Vec<&'a Expr>) -> Vec<&'a Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            right,
            op: Operator::And,
            left,
        }) => {
            let exprs = split_conjunction_impl(left, exprs);
            split_conjunction_impl(right, exprs)
        }
        Expr::Alias(Alias { expr, .. }) => split_conjunction_impl(expr, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Splits an owned conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// This is often used to "split" filter expressions such as `col1 = 5
/// AND col2 = 10` into [`col1 = 5`, `col2 = 10`];
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_optimizer::utils::split_conjunction_owned;
/// // a=1 AND b=2
/// let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use split_conjunction_owned to split them
/// assert_eq!(split_conjunction_owned(expr), split);
/// ```
pub fn split_conjunction_owned(expr: Expr) -> Vec<Expr> {
    split_binary_owned(expr, Operator::And)
}

/// Splits an owned binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
///
/// This is often used to "split" expressions such as `col1 = 5
/// AND col2 = 10` into [`col1 = 5`, `col2 = 10`];
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit, Operator};
/// # use datafusion_optimizer::utils::split_binary_owned;
/// # use std::ops::Add;
/// // a=1 + b=2
/// let expr = col("a").eq(lit(1)).add(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use split_binary_owned to split them
/// assert_eq!(split_binary_owned(expr, Operator::Plus), split);
/// ```
pub fn split_binary_owned(expr: Expr, op: Operator) -> Vec<Expr> {
    split_binary_owned_impl(expr, op, vec![])
}

fn split_binary_owned_impl(
    expr: Expr,
    operator: Operator,
    mut exprs: Vec<Expr>,
) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { right, op, left }) if op == operator => {
            let exprs = split_binary_owned_impl(*left, operator, exprs);
            split_binary_owned_impl(*right, operator, exprs)
        }
        Expr::Alias(Alias { expr, .. }) => {
            split_binary_owned_impl(*expr, operator, exprs)
        }
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Splits an binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
///
/// See [`split_binary_owned`] for more details and an example.
pub fn split_binary(expr: &Expr, op: Operator) -> Vec<&Expr> {
    split_binary_impl(expr, op, vec![])
}

fn split_binary_impl<'a>(
    expr: &'a Expr,
    operator: Operator,
    mut exprs: Vec<&'a Expr>,
) -> Vec<&'a Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { right, op, left }) if *op == operator => {
            let exprs = split_binary_impl(left, operator, exprs);
            split_binary_impl(right, operator, exprs)
        }
        Expr::Alias(Alias { expr, .. }) => split_binary_impl(expr, operator, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical AND.
///
/// Returns None if the filters array is empty.
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_optimizer::utils::conjunction;
/// // a=1 AND b=2
/// let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use conjunction to join them together with `AND`
/// assert_eq!(conjunction(split), Some(expr));
/// ```
pub fn conjunction(filters: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    filters.into_iter().reduce(|accum, expr| accum.and(expr))
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical OR.
///
/// Returns None if the filters array is empty.
pub fn disjunction(filters: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    filters.into_iter().reduce(|accum, expr| accum.or(expr))
}

/// returns a new [LogicalPlan] that wraps `plan` in a [LogicalPlan::Filter] with
/// its predicate be all `predicates` ANDed.
pub fn add_filter(plan: LogicalPlan, predicates: &[&Expr]) -> Result<LogicalPlan> {
    // reduce filters to a single filter with an AND
    let predicate = predicates
        .iter()
        .skip(1)
        .fold(predicates[0].clone(), |acc, predicate| {
            and(acc, (*predicate).to_owned())
        });

    Ok(LogicalPlan::Filter(Filter::try_new(
        predicate,
        Arc::new(plan),
        None,
    )?))
}

/// Looks for correlating expressions: for example, a binary expression with one field from the subquery, and
/// one not in the subquery (closed upon from outer scope)
///
/// # Arguments
///
/// * `exprs` - List of expressions that may or may not be joins
///
/// # Return value
///
/// Tuple of (expressions containing joins, remaining non-join expressions)
pub fn find_join_exprs(exprs: Vec<&Expr>) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let mut joins = vec![];
    let mut others = vec![];
    for filter in exprs.into_iter() {
        // If the expression contains correlated predicates, add it to join filters
        if filter.contains_outer() {
            if !matches!(filter, Expr::BinaryExpr(BinaryExpr{ left, op: Operator::Eq, right }) if left.eq(right))
            {
                joins.push(strip_outer_reference((*filter).clone()));
            }
        } else {
            others.push((*filter).clone());
        }
    }

    Ok((joins, others))
}

/// Returns the first (and only) element in a slice, or an error
///
/// # Arguments
///
/// * `slice` - The slice to extract from
///
/// # Return value
///
/// The first element, or an error
pub fn only_or_err<T>(slice: &[T]) -> Result<&T> {
    match slice {
        [it] => Ok(it),
        [] => plan_err!("No items found!"),
        _ => plan_err!("More than one item found!"),
    }
}

/// merge inputs schema into a single schema.
pub fn merge_schema(inputs: Vec<&LogicalPlan>) -> DFSchema {
    if inputs.len() == 1 {
        inputs[0].schema().clone().as_ref().clone()
    } else {
        inputs.iter().map(|input| input.schema()).fold(
            DFSchema::empty(),
            |mut lhs, rhs| {
                lhs.merge(rhs);
                lhs
            },
        )
    }
}

pub(crate) fn collect_subquery_cols(
    exprs: &[Expr],
    subquery_schema: DFSchemaRef,
) -> Result<BTreeSet<Column>> {
    exprs.iter().try_fold(BTreeSet::new(), |mut cols, expr| {
        let mut using_cols: Vec<Column> = vec![];
        for col in expr.to_columns()?.into_iter() {
            if subquery_schema.has_column(&col) {
                using_cols.push(col);
            }
        }

        cols.extend(using_cols);
        Result::<_>::Ok(cols)
    })
}

pub(crate) fn replace_qualified_name(
    expr: Expr,
    cols: &BTreeSet<Column>,
    subquery_alias: &str,
) -> Result<Expr> {
    let alias_cols: Vec<Column> = cols
        .iter()
        .map(|col| {
            Column::from_qualified_name(format!("{}.{}", subquery_alias, col.name))
        })
        .collect();
    let replace_map: HashMap<&Column, &Column> =
        cols.iter().zip(alias_cols.iter()).collect();

    replace_col(expr, &replace_map)
}

/// Log the plan in debug/tracing mode after some part of the optimizer runs
pub fn log_plan(description: &str, plan: &LogicalPlan) {
    debug!("{description}:\n{}\n", plan.display_indent());
    trace!("{description}::\n{}\n", plan.display_indent_schema());
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion_common::Column;
    use datafusion_expr::expr::Cast;
    use datafusion_expr::{col, lit, utils::expr_to_columns};
    use std::collections::HashSet;

    #[test]
    fn test_split_conjunction() {
        let expr = col("a");
        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr]);
    }

    #[test]
    fn test_split_conjunction_two() {
        let expr = col("a").eq(lit(5)).and(col("b"));
        let expr1 = col("a").eq(lit(5));
        let expr2 = col("b");

        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr1, &expr2]);
    }

    #[test]
    fn test_split_conjunction_alias() {
        let expr = col("a").eq(lit(5)).and(col("b").alias("the_alias"));
        let expr1 = col("a").eq(lit(5));
        let expr2 = col("b"); // has no alias

        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr1, &expr2]);
    }

    #[test]
    fn test_split_conjunction_or() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr]);
    }

    #[test]
    fn test_split_binary_owned() {
        let expr = col("a");
        assert_eq!(split_binary_owned(expr.clone(), Operator::And), vec![expr]);
    }

    #[test]
    fn test_split_binary_owned_two() {
        assert_eq!(
            split_binary_owned(col("a").eq(lit(5)).and(col("b")), Operator::And),
            vec![col("a").eq(lit(5)), col("b")]
        );
    }

    #[test]
    fn test_split_binary_owned_different_op() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        assert_eq!(
            // expr is connected by OR, but pass in AND
            split_binary_owned(expr.clone(), Operator::And),
            vec![expr]
        );
    }

    #[test]
    fn test_split_conjunction_owned() {
        let expr = col("a");
        assert_eq!(split_conjunction_owned(expr.clone()), vec![expr]);
    }

    #[test]
    fn test_split_conjunction_owned_two() {
        assert_eq!(
            split_conjunction_owned(col("a").eq(lit(5)).and(col("b"))),
            vec![col("a").eq(lit(5)), col("b")]
        );
    }

    #[test]
    fn test_split_conjunction_owned_alias() {
        assert_eq!(
            split_conjunction_owned(col("a").eq(lit(5)).and(col("b").alias("the_alias"))),
            vec![
                col("a").eq(lit(5)),
                // no alias on b
                col("b"),
            ]
        );
    }

    #[test]
    fn test_conjunction_empty() {
        assert_eq!(conjunction(vec![]), None);
    }

    #[test]
    fn test_conjunction() {
        // `[A, B, C]`
        let expr = conjunction(vec![col("a"), col("b"), col("c")]);

        // --> `(A AND B) AND C`
        assert_eq!(expr, Some(col("a").and(col("b")).and(col("c"))));

        // which is different than `A AND (B AND C)`
        assert_ne!(expr, Some(col("a").and(col("b").and(col("c")))));
    }

    #[test]
    fn test_disjunction_empty() {
        assert_eq!(disjunction(vec![]), None);
    }

    #[test]
    fn test_disjunction() {
        // `[A, B, C]`
        let expr = disjunction(vec![col("a"), col("b"), col("c")]);

        // --> `(A OR B) OR C`
        assert_eq!(expr, Some(col("a").or(col("b")).or(col("c"))));

        // which is different than `A OR (B OR C)`
        assert_ne!(expr, Some(col("a").or(col("b").or(col("c")))));
    }

    #[test]
    fn test_split_conjunction_owned_or() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        assert_eq!(split_conjunction_owned(expr.clone()), vec![expr]);
    }

    #[test]
    fn test_collect_expr() -> Result<()> {
        let mut accum: HashSet<Column> = HashSet::new();
        expr_to_columns(
            &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64)),
            &mut accum,
        )?;
        expr_to_columns(
            &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64)),
            &mut accum,
        )?;
        assert_eq!(1, accum.len());
        assert!(accum.contains(&Column::from_name("a")));
        Ok(())
    }
}
