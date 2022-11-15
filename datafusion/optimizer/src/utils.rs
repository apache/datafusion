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
use std::collections::{HashSet, VecDeque};
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
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    for input in plan.inputs() {
        let new_input = optimizer.try_optimize(input, optimizer_config)?;
        new_inputs.push(new_input.unwrap_or_else(|| input.clone()))
    }
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
        Expr::Alias(expr, _) => split_binary_owned_impl(*expr, operator, exprs),
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
        Expr::Alias(expr, _) => split_binary_impl(expr, operator, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Given a list of lists of [`Expr`]s, returns a list of lists of
/// [`Expr`]s of expressions where there is one expression from each
/// from each of the input expressions
///
/// For example, given the input `[[a, b], [c], [d, e]]` returns
/// `[a, c, d], [a, c, e], [b, c, d], [b, c, e]]`.
fn permutations(mut exprs: VecDeque<Vec<&Expr>>) -> Vec<Vec<&Expr>> {
    let first = if let Some(first) = exprs.pop_front() {
        first
    } else {
        return vec![];
    };

    // base case:
    if exprs.is_empty() {
        first.into_iter().map(|e| vec![e]).collect()
    } else {
        first
            .into_iter()
            .flat_map(|expr| {
                permutations(exprs.clone())
                    .into_iter()
                    .map(|expr_list| {
                        // Create [expr, ...] for each permutation
                        std::iter::once(expr)
                            .chain(expr_list.into_iter())
                            .collect::<Vec<&Expr>>()
                    })
                    .collect::<Vec<Vec<&Expr>>>()
            })
            .collect()
    }
}

const MAX_CNF_REWRITE_CONJUNCTS: usize = 10;

/// Tries to convert an expression to conjunctive normal form (CNF).
///
/// Does not convert the expression if the total number of conjuncts
/// (exprs ANDed together) would exceed [`MAX_CNF_REWRITE_CONJUNCTS`].
///
/// The following expression is in CNF:
///  `(a OR b) AND (c OR d)`
///
/// The following is not in CNF:
///  `(a AND b) OR c`.
///
/// But could be rewrite to a CNF expression:
///  `(a OR c) AND (b OR c)`.
///
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_optimizer::utils::cnf_rewrite;
/// // （a=1 AND b=2）OR c = 3
/// let expr1 = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
/// let expr2 = col("c").eq(lit(3));
/// let expr = expr1.or(expr2);
///
///  //（a=1 or c=3）AND（b=2 or c=3）
/// let expr1 = col("a").eq(lit(1)).or(col("c").eq(lit(3)));
/// let expr2 = col("b").eq(lit(2)).or(col("c").eq(lit(3)));
/// let expect = expr1.and(expr2);
/// assert_eq!(expect, cnf_rewrite(expr));
/// ```
pub fn cnf_rewrite(expr: Expr) -> Expr {
    // Find all exprs joined by OR
    let disjuncts = split_binary(&expr, Operator::Or);

    // For each expr, split now on AND
    // A OR B OR C --> split each A, B and C
    let disjunct_conjuncts: VecDeque<Vec<&Expr>> = disjuncts
        .into_iter()
        .map(|e| split_binary(e, Operator::And))
        .collect::<VecDeque<_>>();

    // Decide if we want to distribute the clauses. Heuristic is
    // chosen to avoid creating huge predicates
    let num_conjuncts = disjunct_conjuncts
        .iter()
        .fold(1usize, |sz, exprs| sz.saturating_mul(exprs.len()));

    if disjunct_conjuncts.iter().any(|exprs| exprs.len() > 1)
        && num_conjuncts < MAX_CNF_REWRITE_CONJUNCTS
    {
        let or_clauses = permutations(disjunct_conjuncts)
            .into_iter()
            // form the OR clauses( A OR B OR C ..)
            .map(|exprs| disjunction(exprs.into_iter().cloned()).unwrap());
        conjunction(or_clauses).unwrap()
    }
    // otherwise return the original expression
    else {
        expr
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
    use datafusion_expr::{col, lit, or, utils::expr_to_columns};
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

    #[test]
    fn test_permutations() {
        assert_eq!(make_permutations(vec![]), vec![] as Vec<Vec<Expr>>)
    }

    #[test]
    fn test_permutations_one() {
        // [[a]] --> [[a]]
        assert_eq!(
            make_permutations(vec![vec![col("a")]]),
            vec![vec![col("a")]]
        )
    }

    #[test]
    fn test_permutations_two() {
        // [[a, b]] --> [[a], [b]]
        assert_eq!(
            make_permutations(vec![vec![col("a"), col("b")]]),
            vec![vec![col("a")], vec![col("b")]]
        )
    }

    #[test]
    fn test_permutations_two_and_one() {
        // [[a, b], [c]] --> [[a, c], [b, c]]
        assert_eq!(
            make_permutations(vec![vec![col("a"), col("b")], vec![col("c")]]),
            vec![vec![col("a"), col("c")], vec![col("b"), col("c")]]
        )
    }

    #[test]
    fn test_permutations_two_and_one_and_two() {
        // [[a, b], [c], [d, e]] --> [[a, c, d], [a, c, e], [b, c, d], [b, c, e]]
        assert_eq!(
            make_permutations(vec![
                vec![col("a"), col("b")],
                vec![col("c")],
                vec![col("d"), col("e")]
            ]),
            vec![
                vec![col("a"), col("c"), col("d")],
                vec![col("a"), col("c"), col("e")],
                vec![col("b"), col("c"), col("d")],
                vec![col("b"), col("c"), col("e")],
            ]
        )
    }

    /// call permutations with owned `Expr`s for easier testing
    fn make_permutations(exprs: impl IntoIterator<Item = Vec<Expr>>) -> Vec<Vec<Expr>> {
        let exprs = exprs.into_iter().collect::<Vec<_>>();

        let exprs: VecDeque<Vec<&Expr>> = exprs
            .iter()
            .map(|exprs| exprs.iter().collect::<Vec<&Expr>>())
            .collect();

        permutations(exprs)
            .into_iter()
            // copy &Expr --> Expr
            .map(|exprs| exprs.into_iter().cloned().collect())
            .collect()
    }

    #[test]
    fn test_rewrite_cnf() {
        let a_1 = col("a").eq(lit(1i64));
        let a_2 = col("a").eq(lit(2i64));

        let b_1 = col("b").eq(lit(1i64));
        let b_2 = col("b").eq(lit(2i64));

        // Test rewrite on a1_and_b2 and a2_and_b1 -> not change
        let expr1 = and(a_1.clone(), b_2.clone());
        let expect = expr1.clone();
        assert_eq!(expect, cnf_rewrite(expr1));

        // Test rewrite on a1_and_b2 and a2_and_b1 -> (((a1 and b2) and a2) and b1)
        let expr1 = and(and(a_1.clone(), b_2.clone()), and(a_2.clone(), b_1.clone()));
        let expect = and(a_1.clone(), b_2.clone())
            .and(a_2.clone())
            .and(b_1.clone());
        assert_eq!(expect, cnf_rewrite(expr1));

        // Test rewrite on a1_or_b2  -> not change
        let expr1 = or(a_1.clone(), b_2.clone());
        let expect = expr1.clone();
        assert_eq!(expect, cnf_rewrite(expr1));

        // Test rewrite on a1_and_b2 or a2_and_b1 ->  a1_or_a2 and a1_or_b1 and b2_or_a2 and b2_or_b1
        let expr1 = or(and(a_1.clone(), b_2.clone()), and(a_2.clone(), b_1.clone()));
        let a1_or_a2 = or(a_1.clone(), a_2.clone());
        let a1_or_b1 = or(a_1.clone(), b_1.clone());
        let b2_or_a2 = or(b_2.clone(), a_2.clone());
        let b2_or_b1 = or(b_2.clone(), b_1.clone());
        let expect = and(a1_or_a2, a1_or_b1).and(b2_or_a2).and(b2_or_b1);
        assert_eq!(expect, cnf_rewrite(expr1));

        // Test rewrite on a1_or_b2 or a2_and_b1 ->  ( a1_or_a2 or a2 ) and (a1_or_a2 or b1)
        let a1_or_b2 = or(a_1.clone(), b_2.clone());
        let expr1 = or(or(a_1.clone(), b_2.clone()), and(a_2.clone(), b_1.clone()));
        let expect = or(a1_or_b2.clone(), a_2.clone()).and(or(a1_or_b2, b_1.clone()));
        assert_eq!(expect, cnf_rewrite(expr1));

        // Test rewrite on a1_or_b2 or a2_or_b1 ->  not change
        let expr1 = or(or(a_1, b_2), or(a_2, b_1));
        let expect = expr1.clone();
        assert_eq!(expect, cnf_rewrite(expr1));
    }

    #[test]
    fn test_rewrite_cnf_overflow() {
        // in this situation:
        // AND = (a=1 and b=2)
        // rewrite (AND * 10) or (AND * 10), it will produce 10 * 10 = 100 (a=1 or b=2)
        // which cause size expansion.

        let mut expr1 = col("test1").eq(lit(1i64));
        let expr2 = col("test2").eq(lit(2i64));

        for _i in 0..9 {
            expr1 = expr1.clone().and(expr2.clone());
        }
        let expr3 = expr1.clone();
        let expr = or(expr1, expr3);

        assert_eq!(expr, cnf_rewrite(expr.clone()));
    }
}
