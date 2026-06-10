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

//! [`EliminateOuterJoin`] rewrites outer joins to simpler join types when
//! filters make the outer rows unnecessary (e.g. `LEFT`/`RIGHT` to `INNER`,
//! and `FULL` to `LEFT`/`RIGHT`/`INNER`).
use crate::push_down_filter::replace_cols_by_name;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DFSchema, Result, qualified_name};
use datafusion_expr::logical_plan::{Join, JoinType, LogicalPlan, Projection};
use datafusion_expr::{Expr, Filter, Operator};

use crate::optimizer::ApplyOrder;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::expr::{BinaryExpr, Cast, InList, Like, TryCast};
use std::collections::HashMap;
use std::sync::Arc;

/// Attempt to simplify outer joins when filters make their null-padded
/// rows impossible to observe.
///
/// Outer joins are generally more expensive than inner joins and can block
/// predicate pushdown and other optimizations. When a filter above an outer
/// join removes every row the join would add for unmatched input rows, the
/// join can be changed to a cheaper join type.
///
/// For example:
///
/// ```sql
/// SELECT ...
/// FROM a LEFT JOIN b ON ...
/// WHERE b.xx = 100
/// ```
///
/// For unmatched rows from `a`, the LEFT JOIN would produce a row with
/// `b.xx` set to NULL. The predicate `b.xx = 100` does not pass for those
/// rows, so the query does not need the LEFT JOIN's null-padded output and
/// the join can be rewritten as an inner join.
///
/// The same reasoning can also simplify FULL joins to LEFT, RIGHT, or INNER
/// joins when filters remove the rows padded on one or both sides.
///
/// This rule looks for a filter above an outer join:
///
/// ```text
/// Filter(predicate)
///   Join(LEFT/RIGHT/FULL)
/// ```
///
/// It also handles plan shapes where projection pruning has inserted one or
/// more Projection nodes between the filter and join:
///
/// ```text
/// Filter(predicate over projection output)
///   Projection(...)
///     ...
///       Join(LEFT/RIGHT/FULL)
/// ```
///
/// In the projection case, the rule rewrites a copy of the predicate through
/// each Projection so it can analyze the predicate against the Join inputs.
/// The original filter predicate and Projection nodes are preserved when the
/// plan is rebuilt.
#[derive(Default, Debug)]
pub struct EliminateOuterJoin;

impl EliminateOuterJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Attempt to eliminate outer joins.
impl OptimizerRule for EliminateOuterJoin {
    fn name(&self) -> &str {
        "eliminate_outer_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        // Descend through one or more Projection nodes until we find a Join.
        // For each Projection we encounter, rewrite a working copy of the
        // predicate by replacing references to projection output columns with
        // the expressions that define them. Keep the filter's original
        // predicate intact for eventual use in the rebuilt plan; the rewritten
        // predicate is used only for the null-rejection analysis.
        let mut rewritten_predicate = filter.predicate.clone();
        let mut projections: Vec<Projection> = Vec::new();
        let mut cur = Arc::clone(&filter.input);

        let new_join = loop {
            match cur.as_ref() {
                LogicalPlan::Projection(p) => {
                    rewritten_predicate =
                        inline_through_projection(rewritten_predicate, p)?;
                    let next = Arc::clone(&p.input);
                    projections.push(p.clone());
                    cur = next;
                }
                LogicalPlan::Join(join) => {
                    let Some(new_join) = try_simplify_join(join, &rewritten_predicate)
                    else {
                        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                    };
                    break new_join;
                }
                _ => {
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }
            }
        };

        let rebuilt_inner = rewrap_projections(new_join, projections);
        Filter::try_new(filter.predicate, Arc::new(rebuilt_inner))
            .map(|f| Transformed::yes(LogicalPlan::Filter(f)))
    }
}

/// Run the null-rejection analysis on `predicate` against `join`'s left/right
/// schemas. Return `Some(new_join_plan)` if the join type can be tightened
/// (e.g. LEFT → INNER), `None` otherwise.
fn try_simplify_join(join: &Join, predicate: &Expr) -> Option<LogicalPlan> {
    if !join.join_type.is_outer() {
        return None;
    }

    let mut null_rejecting_cols: Vec<Column> = vec![];
    extract_null_rejecting_columns(
        predicate,
        &mut null_rejecting_cols,
        join.left.schema(),
        join.right.schema(),
        true,
    );

    let mut left_non_nullable = false;
    let mut right_non_nullable = false;
    for col in null_rejecting_cols.iter() {
        if join.left.schema().has_column(col) {
            left_non_nullable = true;
        }
        if join.right.schema().has_column(col) {
            right_non_nullable = true;
        }
    }

    let new_join_type =
        eliminate_outer(join.join_type, left_non_nullable, right_non_nullable);
    if new_join_type == join.join_type {
        return None;
    }

    Some(LogicalPlan::Join(Join {
        left: Arc::clone(&join.left),
        right: Arc::clone(&join.right),
        join_type: new_join_type,
        join_constraint: join.join_constraint,
        on: join.on.clone(),
        filter: join.filter.clone(),
        schema: Arc::clone(&join.schema),
        null_equality: join.null_equality,
        null_aware: join.null_aware,
    }))
}

/// Substitute the projection's output column references in `predicate` with
/// the projection's defining expressions (stripped of any `Alias` wrapper).
/// The result expresses `predicate` over the projection's *input* schema.
///
/// Unlike `PushDownFilter`, this rule does not change expression evaluation
/// behavior (in fact, the rewritten expressions are only used for analysis
/// purposes). Therefore, function volatility and `MoveTowardsLeafNodes`
/// placement can be ignored here.
fn inline_through_projection(predicate: Expr, p: &Projection) -> Result<Expr> {
    let mut map: HashMap<String, Expr> = HashMap::new();
    for ((qualifier, field), expr) in p.schema.iter().zip(p.expr.iter()) {
        map.insert(
            qualified_name(qualifier, field.name()),
            unalias(expr).clone(),
        );
    }
    replace_cols_by_name(predicate, &map)
}

/// Re-attach a stack of projections above `new_inner`, restoring the original
/// plan shape with the new (possibly retyped) join at the bottom. Projection
/// schemas are reused as-is; only nullability of columns sourced from the
/// formerly-outer side may have changed, and the existing rule already takes
/// this looser-schema approach at the join itself.
fn rewrap_projections(
    new_inner: LogicalPlan,
    projections: Vec<Projection>,
) -> LogicalPlan {
    let mut current = new_inner;
    for mut p in projections.into_iter().rev() {
        p.input = Arc::new(current);
        current = LogicalPlan::Projection(p);
    }
    current
}

fn unalias(expr: &Expr) -> &Expr {
    if let Expr::Alias(a) = expr {
        unalias(&a.expr)
    } else {
        expr
    }
}

pub fn eliminate_outer(
    join_type: JoinType,
    left_non_nullable: bool,
    right_non_nullable: bool,
) -> JoinType {
    match (join_type, left_non_nullable, right_non_nullable) {
        (JoinType::Left, _, true) => JoinType::Inner,
        (JoinType::Right, true, _) => JoinType::Inner,
        (JoinType::Full, true, true) => JoinType::Inner,
        (JoinType::Full, true, false) => JoinType::Left,
        (JoinType::Full, false, true) => JoinType::Right,
        _ => join_type,
    }
}

/// Find the columns that `expr` rejects NULL on. If any of these columns are
/// NULL, `expr` is guaranteed to evaluate to NULL or false, and the row
/// therefore cannot survive a WHERE clause. Matching columns are appended to
/// `null_rejecting_cols`.
///
/// The caller uses the result to decide whether an outer join's null-padded
/// rows could survive the predicate above the join: if a column from the
/// nullable side appears in `null_rejecting_cols`, it cannot, and the outer
/// join can be converted to an inner join.
///
/// `left_schema` and `right_schema` are the join's two child schemas.
/// `top_level` is true at the root of the WHERE predicate and false on each
/// recursion.
fn extract_null_rejecting_columns(
    expr: &Expr,
    null_rejecting_cols: &mut Vec<Column>,
    left_schema: &Arc<DFSchema>,
    right_schema: &Arc<DFSchema>,
    top_level: bool,
) {
    match expr {
        Expr::Column(col) => {
            null_rejecting_cols.push(col.clone());
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And | Operator::Or => {
                // AND distributes only down a top-level AND chain in the WHERE
                // clause: each conjunct is independently null- rejecting, so
                // any column either side discovers is a column the WHERE
                // rejects NULL on. Once an AND appears below any other context,
                // we fall back to the per-side analysis used for OR, because
                // the context might influence whether the row is filtered.
                if top_level && *op == Operator::And {
                    extract_null_rejecting_columns(
                        left,
                        null_rejecting_cols,
                        left_schema,
                        right_schema,
                        top_level,
                    );
                    extract_null_rejecting_columns(
                        right,
                        null_rejecting_cols,
                        left_schema,
                        right_schema,
                        top_level,
                    );
                    return;
                }

                // OR (and nested AND): a row survives if EITHER operand returns
                // true. We can credit a join side as null-rejecting only when
                // BOTH operands independently reject NULL on a column from that
                // side — otherwise the other branch could let the NULL row
                // through.
                let mut left_cols: Vec<Column> = vec![];
                let mut right_cols: Vec<Column> = vec![];
                extract_null_rejecting_columns(
                    left,
                    &mut left_cols,
                    left_schema,
                    right_schema,
                    top_level,
                );
                extract_null_rejecting_columns(
                    right,
                    &mut right_cols,
                    left_schema,
                    right_schema,
                    top_level,
                );

                let find_on = |cols: &[Column], schema: &DFSchema| {
                    cols.iter().find(|c| schema.has_column(c)).cloned()
                };
                for schema in [left_schema, right_schema] {
                    if let (Some(c), Some(_)) =
                        (find_on(&left_cols, schema), find_on(&right_cols, schema))
                    {
                        null_rejecting_cols.push(c);
                    }
                }
            }
            // Any other operator that DataFusion declares as NULL-on-NULL:
            // recurse into both operands so we collect their columns.
            op if op.returns_null_on_null() => {
                extract_null_rejecting_columns(
                    left,
                    null_rejecting_cols,
                    left_schema,
                    right_schema,
                    false,
                );
                extract_null_rejecting_columns(
                    right,
                    null_rejecting_cols,
                    left_schema,
                    right_schema,
                    false,
                )
            }
            // All other operators (notably including IS [ NOT ] DISTINCT FROM)
            // are declared as not null-propagating, so they don't contribute
            // any null-rejecting columns.
            _ => {}
        },
        Expr::Not(arg) | Expr::Negative(arg) => extract_null_rejecting_columns(
            arg,
            null_rejecting_cols,
            left_schema,
            right_schema,
            false,
        ),
        // IS NOT NULL / IS TRUE / IS FALSE / IS NOT UNKNOWN all return FALSE on
        // NULL input. At the top of a WHERE clause, that FALSE filters the row
        // and so we can recurse; below the top level the surrounding context
        // may transform that FALSE into something that accepts NULL rows,
        // making the recursion unsound.
        Expr::IsNotNull(arg)
        | Expr::IsTrue(arg)
        | Expr::IsFalse(arg)
        | Expr::IsNotUnknown(arg) => {
            if !top_level {
                return;
            }
            extract_null_rejecting_columns(
                arg,
                null_rejecting_cols,
                left_schema,
                right_schema,
                false,
            )
        }
        Expr::Cast(Cast { expr, field: _ })
        | Expr::TryCast(TryCast { expr, field: _ }) => extract_null_rejecting_columns(
            expr,
            null_rejecting_cols,
            left_schema,
            right_schema,
            false,
        ),
        // IN list and BETWEEN are null-rejecting on the input expression:
        // NULL input yields a NULL result, regardless of whether the list
        // or range bounds themselves contain NULLs.
        Expr::InList(InList { expr, .. }) => extract_null_rejecting_columns(
            expr,
            null_rejecting_cols,
            left_schema,
            right_schema,
            false,
        ),
        Expr::Between(between) => extract_null_rejecting_columns(
            &between.expr,
            null_rejecting_cols,
            left_schema,
            right_schema,
            false,
        ),
        Expr::Like(Like { expr, pattern, .. }) => {
            extract_null_rejecting_columns(
                expr,
                null_rejecting_cols,
                left_schema,
                right_schema,
                false,
            );
            extract_null_rejecting_columns(
                pattern,
                null_rejecting_cols,
                left_schema,
                right_schema,
                false,
            );
        }
        // Anything not handled above contributes no null-rejecting
        // columns. Two categories worth calling out:
        //   - IS NULL, IS NOT TRUE, IS NOT FALSE, IS UNKNOWN — return
        //     TRUE on NULL input, so they actively *accept* NULL rows
        //     and are intentionally excluded.
        //   - Function calls (scalar / aggregate / window / UDF),
        //     scalar subqueries, struct/list accessors, aliases,
        //     literals, etc. — we don't have a uniform NULL-propagation
        //     guarantee for these cases, so we conservatively skip them.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::*;
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{
        Operator::{And, Or},
        binary_expr, cast, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        not, try_cast,
    };

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateOuterJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn eliminate_left_with_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_null())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IS NULL
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_not_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_not_null())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IS NOT NULL
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_right_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                Or,
                col("t1.c").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b > UInt32(10) OR t1.c < UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b > UInt32(10) AND t2.c < UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_in_list() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // t2.b IN (1, 2, 3) rejects nulls — if t2.b is NULL the IN returns
        // NULL which is filtered out. So Left Join should become Inner Join.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").in_list(vec![lit(1u32), lit(2u32), lit(3u32)], false))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(2), UInt32(3)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_in_list_containing_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IN list with NULL still rejects null input columns:
        // if t2.b is NULL, NULL IN (1, NULL) evaluates to NULL, which is filtered out
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(
                col("t2.b")
                    .in_list(vec![lit(1u32), lit(ScalarValue::UInt32(None))], false),
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(NULL)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_not_in_list() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // NOT IN also rejects nulls: if t2.b is NULL, NOT (NULL IN (...))
        // evaluates to NULL, which is filtered out
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").in_list(vec![lit(1u32), lit(2u32)], true))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b NOT IN ([UInt32(1), UInt32(2)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_between() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // BETWEEN rejects nulls: if t2.b is NULL, NULL BETWEEN 1 AND 10
        // evaluates to NULL, which is filtered out
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").between(lit(1u32), lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b BETWEEN UInt32(1) AND UInt32(10)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_right_with_between() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Right join: filter on left (nullable) side with BETWEEN should convert to Inner
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.b").between(lit(1u32), lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b BETWEEN UInt32(1) AND UInt32(10)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_with_between() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Full join with BETWEEN on both sides should become Inner
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").between(lit(1u32), lit(10u32)),
                And,
                col("t2.b").between(lit(5u32), lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b BETWEEN UInt32(1) AND UInt32(10) AND t2.b BETWEEN UInt32(5) AND UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_with_in_list() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Full join with IN filters on both sides should become Inner
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").in_list(vec![lit(1u32), lit(2u32)], false),
                And,
                col("t2.b").in_list(vec![lit(3u32), lit(4u32)], false),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b IN ([UInt32(1), UInt32(2)]) AND t2.b IN ([UInt32(3), UInt32(4)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_in_list_or_is_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // WHERE (t2.b IN (1, 2)) OR (t2.b IS NULL)
        // The OR with IS NULL makes the predicate null-tolerant:
        // when t2.b is NULL, IS NULL returns true, so the whole OR is true.
        // The outer join must be preserved.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b").in_list(vec![lit(1u32), lit(2u32)], false),
                Or,
                col("t2.b").is_null(),
            ))?
            .build()?;

        // Should NOT be converted to Inner — OR with IS NULL preserves null rows
        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(2)]) OR t2.b IS NULL
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_like() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // LIKE rejects nulls: if t2.b is NULL, the result is NULL (filtered out)
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").like(lit("%pattern%")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Filter: t2.b LIKE Utf8("%pattern%")
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "#)
    }

    #[test]
    fn eliminate_left_with_like_pattern_column() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // LIKE with nullable column on the pattern side:
        // 'x' LIKE t2.b → if t2.b is NULL, result is NULL (filtered out)
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(lit("x").like(col("t2.b")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Filter: Utf8("x") LIKE t2.b
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "#)
    }

    #[test]
    fn eliminate_full_with_like_cross_side() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // LIKE with columns from both sides: t1.c LIKE t2.b
        // If t1 is NULL → NULL LIKE t2.b → NULL → filtered out (left non-nullable)
        // If t2 is NULL → t1.c LIKE NULL → NULL → filtered out (right non-nullable)
        // Both sides are non-nullable → FULL → INNER
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.c").like(col("t2.b")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.c LIKE t2.b
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_is_true() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS TRUE rejects nulls: if the expression is NULL, IS TRUE returns false
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_true())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS TRUE
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_is_false() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS FALSE rejects nulls: if the expression is NULL, IS FALSE returns false
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_false())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS FALSE
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_is_not_unknown() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS NOT UNKNOWN rejects nulls: if the expression is NULL, IS NOT UNKNOWN returns false
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_not_unknown())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS NOT UNKNOWN
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_is_not_true() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS NOT TRUE is NOT null-rejecting: if the expression is NULL,
        // IS NOT TRUE returns true, so null rows pass through
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_not_true())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS NOT TRUE
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_is_unknown() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS UNKNOWN is NOT null-rejecting: if the expression is NULL,
        // IS UNKNOWN returns true, so null rows pass through
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_unknown())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS UNKNOWN
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_not_is_true() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // NOT(<x> IS TRUE) is equivalent to (<x> IS NOT TRUE): TRUE when
        // <x> is FALSE OR NULL. So `WHERE NOT((t2.b > 5) IS TRUE)` accepts
        // rows where t2.b is NULL (because t2.b > 5 is NULL → IS TRUE is
        // FALSE → NOT FALSE = TRUE). The LEFT JOIN must NOT be converted.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(not(col("t2.b").gt(lit(5u32)).is_true()))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: NOT t2.b > UInt32(5) IS TRUE
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_not_is_false() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Same shape, IS FALSE: NOT(<x> IS FALSE) accepts NULL on the
        // inner column.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(not(col("t2.b").gt(lit(5u32)).is_false()))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: NOT t2.b > UInt32(5) IS FALSE
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_not_is_not_unknown() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Same shape, IS NOT UNKNOWN: NOT(<x> IS NOT UNKNOWN) is
        // equivalent to (<x> IS UNKNOWN), which is TRUE when <x> is NULL.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(not(col("t2.b").gt(lit(5u32)).is_not_unknown()))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: NOT t2.b > UInt32(5) IS NOT UNKNOWN
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_with_type_cast() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                cast(col("t1.b"), DataType::Int64).gt(lit(10u32)),
                And,
                try_cast(col("t2.c"), DataType::Int64).lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: CAST(t1.b AS Int64) > UInt32(10) AND TRY_CAST(t2.c AS Int64) < UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    // ----- FULL JOIN → LEFT / RIGHT tests -----
    #[test]
    fn eliminate_full_to_left_with_left_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with null-rejecting filter only on left side → LEFT JOIN
        // (left side becomes non-nullable, right side stays nullable)
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.b").gt(lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b > UInt32(10)
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_to_right_with_right_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with null-rejecting filter only on right side → RIGHT JOIN
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").in_list(vec![lit(1u32), lit(2u32)], false))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(2)])
          Right Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_full_to_left_with_like() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with LIKE on left side only → LEFT JOIN
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t1.b").like(lit("%val%")))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Filter: t1.b LIKE Utf8("%val%")
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "#)
    }

    #[test]
    fn eliminate_full_to_right_with_is_true() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with IS TRUE on right side only → RIGHT JOIN
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").gt(lit(10u32)).is_true())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) IS TRUE
          Right Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    // ----- Nested AND / OR tests -----

    #[test]
    fn eliminate_left_with_and_multiple_null_rejecting() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Multiple null-rejecting predicates combined with AND on nullable side
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b").in_list(vec![lit(1u32), lit(2u32)], false),
                And,
                col("t2.c").between(lit(5u32), lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IN ([UInt32(1), UInt32(2)]) AND t2.c BETWEEN UInt32(5) AND UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_or_same_side() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // OR of two null-rejecting predicates on different columns of the same
        // nullable side. If t2 rows are NULL (from LEFT JOIN), both t2.b and
        // t2.c are NULL, so the entire OR evaluates to NULL → filtered out.
        // This IS null-rejecting, so join should be eliminated.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b").gt(lit(10u32)),
                Or,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10) OR t2.c < UInt32(20)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_or_cross_side() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // OR with columns from different sides — t1.b (preserved) OR t2.b
        // (nullable). When t2 is NULL, t1.b > 10 can still be true, so the
        // OR is NOT null-rejecting. Join must be preserved.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                Or,
                col("t2.b").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t1.b > UInt32(10) OR t2.b < UInt32(20)
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    // ----- Mixed predicate tests -----

    #[test]
    fn eliminate_full_with_mixed_predicates() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // FULL JOIN with different null-rejecting expr types on each side:
        // LIKE on left, BETWEEN on right → INNER JOIN
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").like(lit("%pattern%")),
                And,
                col("t2.b").between(lit(1u32), lit(10u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r#"
        Filter: t1.b LIKE Utf8("%pattern%") AND t2.b BETWEEN UInt32(1) AND UInt32(10)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "#)
    }

    #[test]
    fn eliminate_left_with_is_true_and_in_list() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // AND of IS TRUE and IN on nullable side — both null-rejecting
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b").gt(lit(5u32)).is_true(),
                And,
                col("t2.c").in_list(vec![lit(1u32), lit(2u32)], false),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(5) IS TRUE AND t2.c IN ([UInt32(1), UInt32(2)])
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    // ----- Filter pierces a Projection to reach the Join -----

    #[test]
    fn eliminate_left_through_projection() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Filter → Projection → LeftJoin is the shape produced by projection
        // pruning in queries such as TPC-DS q49, where the post-join
        // Projection sits between the filter and the join.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .project(vec![col("t1.a"), col("t2.b").alias("bb")])?
            .filter(col("bb").gt(lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: bb > UInt32(10)
          Projection: t1.a, t2.b AS bb
            Inner Join: t1.a = t2.a
              TableScan: t1
              TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_through_projection_with_or_cross_side() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // After inlining the filter is still t1.b > 10 OR t2.b < 20, which
        // is null-tolerant when t2 is NULL (the t1.b clause can still hold).
        // The LEFT JOIN must be preserved.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .project(vec![col("t1.b").alias("x"), col("t2.b").alias("y")])?
            .filter(binary_expr(
                col("x").gt(lit(10u32)),
                Or,
                col("y").lt(lit(20u32)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: x > UInt32(10) OR y < UInt32(20)
          Projection: t1.b AS x, t2.b AS y
            Left Join: t1.a = t2.a
              TableScan: t1
              TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_through_projection_with_only_left_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // A filter that constrains only the preserved (left) side of a
        // LEFT JOIN does not justify converting it to INNER — the LEFT
        // would still pass nullable right-side rows that the filter
        // accepts.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .project(vec![col("t1.b").alias("x"), col("t2.b")])?
            .filter(col("x").gt(lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: x > UInt32(10)
          Projection: t1.b AS x, t2.b
            Left Join: t1.a = t2.a
              TableScan: t1
              TableScan: t2
        ")
    }

    #[test]
    fn eliminate_left_with_arithmetic_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // t2.b * 2 + 1 > 10 is null-rejecting on t2.b: arithmetic
        // operators propagate NULL, so the whole expression is NULL when
        // t2.b is NULL, and NULL > 10 is filtered out by WHERE.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(
                binary_expr(
                    binary_expr(col("t2.b"), Operator::Multiply, lit(2u32)),
                    Operator::Plus,
                    lit(1u32),
                )
                .gt(lit(10u32)),
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b * UInt32(2) + UInt32(1) > UInt32(10)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }
    #[test]
    fn eliminate_left_with_negative_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Unary minus propagates NULL: -NULL is NULL, so `WHERE -t2.b > 0`
        // is null-rejecting on t2.b.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(Expr::Negative(Box::new(col("t2.b"))).gt(lit(0u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: (- t2.b) > UInt32(0)
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_is_distinct_from() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS DISTINCT FROM is NOT null-rejecting: t2.b IS DISTINCT FROM 5 is
        // true when t2.b is NULL (NULL is distinct from 5). Padding rows from
        // a LEFT JOIN would survive the filter, so the LEFT JOIN must stay.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b"),
                Operator::IsDistinctFrom,
                lit(5u32),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IS DISTINCT FROM UInt32(5)
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_left_with_is_not_distinct_from() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // IS NOT DISTINCT FROM is also not null-rejecting: t2.b IS NOT
        // DISTINCT FROM NULL is true when t2.b is NULL. The LEFT JOIN must
        // stay.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t2.b"),
                Operator::IsNotDistinctFrom,
                lit(ScalarValue::UInt32(None)),
            ))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b IS NOT DISTINCT FROM UInt32(NULL)
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        ")
    }

    #[test]
    fn no_eliminate_through_non_transparent() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Limit is intentionally not treated as transparent: a Limit below
        // the Filter changes which rows survive, so swapping LEFT→INNER
        // beneath it could yield a different surviving-row set even when
        // the filter is null-rejecting on the right side.
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .limit(0, Some(5))?
            .filter(col("t2.b").gt(lit(10u32)))?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: t2.b > UInt32(10)
          Limit: skip=0, fetch=5
            Left Join: t1.a = t2.a
              TableScan: t1
              TableScan: t2
        ")
    }
}
