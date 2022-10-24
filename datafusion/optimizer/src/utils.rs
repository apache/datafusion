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
use datafusion_common::Result;
use datafusion_common::{plan_err, Column, DFSchemaRef};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::expr_rewriter::{ExprRewritable, ExprRewriter};
use datafusion_expr::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use datafusion_expr::{
    and, col,
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
        Expr::Alias(expr, _) => split_conjunction_impl(expr, exprs),
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
    split_conjunction_owned_impl(expr, vec![])
}

fn split_conjunction_owned_impl(expr: Expr, mut exprs: Vec<Expr>) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            right,
            op: Operator::And,
            left,
        }) => {
            let exprs = split_conjunction_owned_impl(*left, exprs);
            split_conjunction_owned_impl(*right, exprs)
        }
        Expr::Alias(expr, _) => split_conjunction_owned_impl(*expr, exprs),
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

/// Recursively un-alias an expressions
#[inline]
pub fn unalias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(sub_expr, _) => unalias(*sub_expr),
        _ => expr,
    }
}

/// Recursively scans a slice of expressions for any `Or` operators
///
/// # Arguments
///
/// * `predicates` - the expressions to scan
///
/// # Return value
///
/// A PlanError if a disjunction is found
pub fn verify_not_disjunction(predicates: &[&Expr]) -> Result<()> {
    struct DisjunctionVisitor {}

    impl ExpressionVisitor for DisjunctionVisitor {
        fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
            match expr {
                Expr::BinaryExpr(BinaryExpr {
                    left: _,
                    op: Operator::Or,
                    right: _,
                }) => {
                    plan_err!("Optimizing disjunctions not supported!")
                }
                _ => Ok(Recursion::Continue(self)),
            }
        }
    }

    for predicate in predicates.iter() {
        predicate.accept(DisjunctionVisitor {})?;
    }

    Ok(())
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
    )?))
}

/// Looks for correlating expressions: equality expressions with one field from the subquery, and
/// one not in the subquery (closed upon from outer scope)
///
/// # Arguments
///
/// * `exprs` - List of expressions that may or may not be joins
/// * `schema` - HashSet of fully qualified (table.col) fields in subquery schema
///
/// # Return value
///
/// Tuple of (expressions containing joins, remaining non-join expressions)
pub fn find_join_exprs(
    exprs: Vec<&Expr>,
    schema: &DFSchemaRef,
) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let fields: HashSet<_> = schema
        .fields()
        .iter()
        .map(|it| it.qualified_name())
        .collect();

    let mut joins = vec![];
    let mut others = vec![];
    for filter in exprs.iter() {
        let (left, op, right) = match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                (*left.clone(), *op, *right.clone())
            }
            _ => {
                others.push((*filter).clone());
                continue;
            }
        };
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
        match op {
            Operator::Eq => {}
            Operator::NotEq => {}
            _ => {
                plan_err!(format!("can't optimize {} column comparison", op))?;
            }
        }

        joins.push((*filter).clone())
    }

    Ok((joins, others))
}

/// Extracts correlating columns from expressions
///
/// # Arguments
///
/// * `exprs` - List of expressions that correlate a subquery to an outer scope
/// * `schema` - subquery schema
/// * `include_negated` - true if `NotEq` counts as a join operator
///
/// # Return value
///
/// Tuple of (outer-scope cols, subquery cols, non-correlation expressions)
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
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                (*left.clone(), *op, *right.clone())
            }
            _ => plan_err!("Invalid correlation expression!")?,
        };
        match op {
            Operator::Eq => {}
            Operator::NotEq => {
                if !include_negated {
                    others.push((*filter).clone());
                    continue;
                }
            }
            _ => plan_err!(format!("Correlation operator unsupported: {}", op))?,
        }
        let left = left.try_into_col()?;
        let right = right.try_into_col()?;
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
    let pred = conjunction(others);

    Ok((left_cols, right_cols, pred))
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

/// Merge and deduplicate two sets Column slices
///
/// # Arguments
///
/// * `a` - A tuple of slices of Columns
/// * `b` - A tuple of slices of Columns
///
/// # Return value
///
/// The deduplicated union of the two slices
pub fn merge_cols(
    a: (&[Column], &[Column]),
    b: (&[Column], &[Column]),
) -> (Vec<Column>, Vec<Column>) {
    let e =
        a.0.iter()
            .map(|it| it.flat_name())
            .chain(a.1.iter().map(|it| it.flat_name()))
            .map(|it| Column::from(it.as_str()));
    let f =
        b.0.iter()
            .map(|it| it.flat_name())
            .chain(b.1.iter().map(|it| it.flat_name()))
            .map(|it| Column::from(it.as_str()));
    let mut g = e.zip(f).collect::<Vec<_>>();
    g.dedup();
    g.into_iter().unzip()
}

/// Change the relation on a slice of Columns
///
/// # Arguments
///
/// * `new_table` - The table/relation for the new columns
/// * `cols` - A slice of Columns
///
/// # Return value
///
/// A new slice of columns, now belonging to the new table
pub fn swap_table(new_table: &str, cols: &[Column]) -> Vec<Column> {
    cols.iter()
        .map(|it| Column {
            relation: Some(new_table.to_string()),
            name: it.name.clone(),
        })
        .collect()
}

pub fn alias_cols(cols: &[Column]) -> Vec<Expr> {
    cols.iter()
        .map(|it| col(it.flat_name().as_str()).alias(it.name.as_str()))
        .collect()
}

/// Rewrites `expr` using `rewriter`, ensuring that the output has the
/// same name as `expr` prior to rewrite, adding an alias if necessary.
///
/// This is important when optimzing plans to ensure the the output
/// schema of plan nodes don't change after optimization
pub fn rewrite_preserving_name<R>(expr: Expr, rewriter: &mut R) -> Result<Expr>
where
    R: ExprRewriter<Expr>,
{
    let original_name = name_for_alias(&expr)?;
    let expr = expr.rewrite(rewriter)?;
    add_alias_if_changed(original_name, expr)
}

/// Return the name to use for the specific Expr, recursing into
/// `Expr::Sort` as appropriate
fn name_for_alias(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Sort { expr, .. } => name_for_alias(expr),
        expr => expr.display_name(),
    }
}

/// Ensure `expr` has the name name as `original_name` by adding an
/// alias if necessary.
fn add_alias_if_changed(original_name: String, expr: Expr) -> Result<Expr> {
    let new_name = name_for_alias(&expr)?;

    if new_name == original_name {
        return Ok(expr);
    }

    Ok(match expr {
        Expr::Sort {
            expr,
            asc,
            nulls_first,
        } => {
            let expr = add_alias_if_changed(original_name, *expr)?;
            Expr::Sort {
                expr: Box::new(expr),
                asc,
                nulls_first,
            }
        }
        expr => expr.alias(original_name),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion_common::Column;
    use datafusion_expr::expr::Cast;
    use datafusion_expr::{col, lit, utils::expr_to_columns};
    use std::collections::HashSet;
    use std::ops::Add;

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

    #[test]
    fn test_rewrite_preserving_name() {
        test_rewrite(col("a"), col("a"));

        test_rewrite(col("a"), col("b"));

        // cast data types
        test_rewrite(
            col("a"),
            Expr::Cast(Cast::new(Box::new(col("a")), DataType::Int32)),
        );

        // change literal type from i32 to i64
        test_rewrite(col("a").add(lit(1i32)), col("a").add(lit(1i64)));

        // SortExpr a+1 ==> b + 2
        test_rewrite(
            Expr::Sort {
                expr: Box::new(col("a").add(lit(1i32))),
                asc: true,
                nulls_first: false,
            },
            Expr::Sort {
                expr: Box::new(col("b").add(lit(2i64))),
                asc: true,
                nulls_first: false,
            },
        );
    }

    /// rewrites `expr_from` to `rewrite_to` using
    /// `rewrite_preserving_name` verifying the result is `expected_expr`
    fn test_rewrite(expr_from: Expr, rewrite_to: Expr) {
        struct TestRewriter {
            rewrite_to: Expr,
        }

        impl ExprRewriter for TestRewriter {
            fn mutate(&mut self, _: Expr) -> Result<Expr> {
                Ok(self.rewrite_to.clone())
            }
        }

        let mut rewriter = TestRewriter {
            rewrite_to: rewrite_to.clone(),
        };
        let expr = rewrite_preserving_name(expr_from.clone(), &mut rewriter).unwrap();

        let original_name = match &expr_from {
            Expr::Sort { expr, .. } => expr.display_name(),
            expr => expr.display_name(),
        }
        .unwrap();

        let new_name = match &expr {
            Expr::Sort { expr, .. } => expr.display_name(),
            expr => expr.display_name(),
        }
        .unwrap();

        assert_eq!(
            original_name, new_name,
            "mismatch rewriting expr_from: {expr_from} to {rewrite_to}"
        )
    }
}
