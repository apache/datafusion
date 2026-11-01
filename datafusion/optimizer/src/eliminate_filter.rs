// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`EliminateFilter`] removes filters that accept all or no input rows.

use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::InList;
use datafusion_expr::{EmptyRelation, Expr, Filter, LogicalPlan, Operator};
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Optimization rule that eliminates filters whose predicates always accept or
/// reject their input rows.
///
/// This saves time in planning and executing the query.
/// Note that this rule should be applied after simplify expressions optimizer rule.
#[derive(Default, Debug)]
pub struct EliminateFilter;

impl EliminateFilter {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateFilter {
    fn name(&self) -> &str {
        "eliminate_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) = plan
        else {
            return Ok(Transformed::no(plan));
        };

        let simplified = simplify_filter_predicate(predicate);
        match simplified.predicate {
            FilterPredicate::AcceptsAll => {
                Ok(Transformed::yes(Arc::unwrap_or_clone(input)))
            }
            FilterPredicate::RejectsAll(_) => Ok(Transformed::yes(
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::clone(input.schema()),
                }),
            )),
            FilterPredicate::Expression(predicate) => Ok(Transformed::new_transformed(
                LogicalPlan::Filter(Filter::new(predicate, input)),
                simplified.transformed,
            )),
        }
    }
}

/// The outcome of a predicate when only rows for which it evaluates to `TRUE`
/// are retained.
enum FilterPredicate {
    AcceptsAll,
    RejectsAll(Expr),
    Expression(Expr),
}

struct SimplifiedFilterPredicate {
    predicate: FilterPredicate,
    transformed: bool,
}

/// Simplifies positive `AND` / `OR` trees according to filter semantics.
///
/// A `NULL` predicate rejects a row just like `FALSE`. This equivalence is safe
/// throughout an `AND` / `OR` tree because both operators are monotonic with
/// respect to whether their result can be `TRUE`. It is not safe through `NOT`
/// or arbitrary expressions, so those are deliberately treated as leaves.
///
/// The same equivalence lets `IN` lists drop their `NULL` members, and makes
/// `NOT IN` lists that contain one reject every row. See
/// [`simplify_filter_in_list`].
fn simplify_filter_predicate(expr: Expr) -> SimplifiedFilterPredicate {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(true)), _) => {
            simplified(FilterPredicate::AcceptsAll, false)
        }
        expr @ Expr::Literal(ScalarValue::Boolean(Some(false) | None), _) => {
            simplified(FilterPredicate::RejectsAll(expr), false)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            simplify_filter_and(*binary.left, *binary.right)
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            simplify_filter_or(*binary.left, *binary.right)
        }
        Expr::InList(in_list) => simplify_filter_in_list(in_list),
        expr => simplified(FilterPredicate::Expression(expr), false),
    }
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(scalar, _) if scalar.is_null())
}

/// Simplifies an `IN` list under filter semantics by dropping `NULL` members.
///
/// `x NOT IN (a, NULL)` is `FALSE` when `x` matches `a` and `NULL` otherwise, so
/// it is never `TRUE` and rejects every row.
///
/// This is only sound under filter semantics; the same rewrite in a general
/// expression context would change `SELECT 3 IN (1, NULL)` from `NULL` to `FALSE`.
fn simplify_filter_in_list(in_list: InList) -> SimplifiedFilterPredicate {
    let InList {
        expr,
        mut list,
        negated,
    } = in_list;

    // `retain` drops the `NULL`s in place. Most lists have none, and that case
    // then costs one pass and no allocation.
    let len_before = list.len();
    list.retain(|e| !is_null_literal(e));

    if list.len() == len_before {
        // No `NULL` members, so there is nothing to simplify.
        return simplified(
            FilterPredicate::Expression(Expr::InList(InList::new(expr, list, negated))),
            false,
        );
    }

    if negated || list.is_empty() {
        // `NOT IN` with a `NULL` member, and `IN` over nothing but `NULL`s, are
        // both `NULL` for every row that they do not reject outright.
        return simplified(
            FilterPredicate::RejectsAll(Expr::Literal(ScalarValue::Boolean(None), None)),
            true,
        );
    }

    simplified(
        FilterPredicate::Expression(Expr::InList(InList::new(expr, list, negated))),
        true,
    )
}

fn simplify_filter_and(left: Expr, right: Expr) -> SimplifiedFilterPredicate {
    let left = simplify_filter_predicate(left);
    let right = simplify_filter_predicate(right);
    let children_transformed = left.transformed || right.transformed;

    match (left.predicate, right.predicate) {
        (FilterPredicate::RejectsAll(left), FilterPredicate::RejectsAll(_)) => {
            simplified(FilterPredicate::RejectsAll(left), true)
        }
        (FilterPredicate::RejectsAll(rejects), FilterPredicate::AcceptsAll)
        | (FilterPredicate::AcceptsAll, FilterPredicate::RejectsAll(rejects)) => {
            simplified(FilterPredicate::RejectsAll(rejects), true)
        }
        (FilterPredicate::RejectsAll(rejects), FilterPredicate::Expression(expr)) => {
            if is_safe_to_discard(&expr) {
                simplified(FilterPredicate::RejectsAll(rejects), true)
            } else {
                simplified(
                    FilterPredicate::Expression(rejects.and(expr)),
                    children_transformed,
                )
            }
        }
        (FilterPredicate::Expression(expr), FilterPredicate::RejectsAll(rejects)) => {
            if is_safe_to_discard(&expr) {
                simplified(FilterPredicate::RejectsAll(rejects), true)
            } else {
                simplified(
                    FilterPredicate::Expression(expr.and(rejects)),
                    children_transformed,
                )
            }
        }
        (FilterPredicate::AcceptsAll, predicate)
        | (predicate, FilterPredicate::AcceptsAll) => simplified(predicate, true),
        (FilterPredicate::Expression(left), FilterPredicate::Expression(right)) => {
            simplified(
                FilterPredicate::Expression(left.and(right)),
                children_transformed,
            )
        }
    }
}

fn simplify_filter_or(left: Expr, right: Expr) -> SimplifiedFilterPredicate {
    let left = simplify_filter_predicate(left);
    let right = simplify_filter_predicate(right);
    let children_transformed = left.transformed || right.transformed;

    match (left.predicate, right.predicate) {
        (FilterPredicate::AcceptsAll, _) | (_, FilterPredicate::AcceptsAll) => {
            simplified(FilterPredicate::AcceptsAll, true)
        }
        (FilterPredicate::RejectsAll(_), predicate)
        | (predicate, FilterPredicate::RejectsAll(_)) => simplified(predicate, true),
        (FilterPredicate::Expression(left), FilterPredicate::Expression(right)) => {
            simplified(
                FilterPredicate::Expression(left.or(right)),
                children_transformed,
            )
        }
    }
}

/// Returns true when skipping evaluation of `expr` cannot hide a runtime error
/// or an observable side effect.
///
/// Logical expressions do not currently expose general infallibility metadata,
/// so this is deliberately a small allowlist. In particular, arithmetic,
/// casts, and function calls are retained even when another branch proves that
/// a filter cannot accept a row.
fn is_safe_to_discard(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_, _) => true,
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::And
                    | Operator::Or
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
            ) =>
        {
            is_safe_to_discard(&binary.left) && is_safe_to_discard(&binary.right)
        }
        Expr::Not(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsNotUnknown(expr) => is_safe_to_discard(expr),
        _ => false,
    }
}

fn simplified(
    predicate: FilterPredicate,
    transformed: bool,
) -> SimplifiedFilterPredicate {
    SimplifiedFilterPredicate {
        predicate,
        transformed,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{Expr, col, lit, logical_plan::builder::LogicalPlanBuilder};

    use crate::eliminate_filter::{
        EliminateFilter, FilterPredicate, simplify_filter_predicate,
    };
    use crate::test::*;
    use datafusion_expr::test::function_stub::sum;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateFilter::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn filter_false() -> Result<()> {
        let filter_expr = lit(false);

        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .build()?;

        // No aggregate / scan / limit
        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")
    }

    #[test]
    fn filter_null() -> Result<()> {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(None), None);

        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .build()?;

        // No aggregate / scan / limit
        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")
    }

    #[test]
    fn filter_and_null() -> Result<()> {
        let null = Expr::Literal(ScalarValue::Boolean(None), None);

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(col("b").and(null.clone()))?
            .build()?;
        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")?;

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(null.and(col("b")))?
            .build()?;
        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")?;

        Ok(())
    }

    #[test]
    fn filter_and_null_preserves_fallible_expression() -> Result<()> {
        let null = Expr::Literal(ScalarValue::Boolean(None), None);
        let fallible = (col("a") / lit(0u32)).gt(lit(0u32));

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(fallible.clone().and(null.clone()))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
            Filter: test.a / UInt32(0) > UInt32(0) AND Boolean(NULL)
              TableScan: test
            "
        )?;

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(null.and(fallible))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
            Filter: Boolean(NULL) AND test.a / UInt32(0) > UInt32(0)
              TableScan: test
            "
        )?;

        Ok(())
    }

    #[test]
    fn filter_or_null() -> Result<()> {
        let null = Expr::Literal(ScalarValue::Boolean(None), None);

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(col("b").or(null.clone()))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
            Filter: test.b
              TableScan: test
            "
        )?;

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(null.or(col("b")))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
            Filter: test.b
              TableScan: test
            "
        )?;

        Ok(())
    }

    #[test]
    fn filter_nested_null_predicates() -> Result<()> {
        let null = Expr::Literal(ScalarValue::Boolean(None), None);

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(col("b").and(col("b").or(null.clone())))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
            Filter: test.b AND test.b
              TableScan: test
            "
        )?;

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(col("b").or(col("c").and(null)))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
            Filter: test.b
              TableScan: test
            "
        )?;

        Ok(())
    }

    #[test]
    fn filter_null_simplification_does_not_cross_not() {
        let null = Expr::Literal(ScalarValue::Boolean(None), None);
        let predicate = col("b").or(null).not();
        let result = simplify_filter_predicate(predicate.clone());

        assert!(!result.transformed);
        let FilterPredicate::Expression(actual) = result.predicate else {
            panic!("expected the predicate to remain an expression")
        };
        assert_eq!(actual, predicate);
    }

    #[test]
    fn filter_false_nested() -> Result<()> {
        let filter_expr = lit(false);

        let table_scan = test_table_scan()?;
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .union(plan1)?
            .build()?;

        // Left side is removed
        assert_optimized_plan_equal!(plan, @r"
        Union
          EmptyRelation: rows=0
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
            TableScan: test
        ")
    }

    #[test]
    fn filter_true() -> Result<()> {
        let filter_expr = lit(true);

        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
          TableScan: test
        ")
    }

    #[test]
    fn filter_true_nested() -> Result<()> {
        let filter_expr = lit(true);

        let table_scan = test_table_scan()?;
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .union(plan1)?
            .build()?;

        // Filter is removed
        assert_optimized_plan_equal!(plan, @r"
        Union
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
            TableScan: test
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
            TableScan: test
        ")
    }

    #[test]
    fn filter_from_subquery() -> Result<()> {
        // SELECT a FROM (SELECT a FROM test WHERE FALSE) WHERE TRUE

        let false_filter = lit(false);
        let table_scan = test_table_scan()?;
        let plan1 = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(false_filter)?
            .build()?;

        let true_filter = lit(true);
        let plan = LogicalPlanBuilder::from(plan1)
            .project(vec![col("a")])?
            .filter(true_filter)?
            .build()?;

        // Filter is removed
        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a
          EmptyRelation: rows=0
        ")
    }
}
