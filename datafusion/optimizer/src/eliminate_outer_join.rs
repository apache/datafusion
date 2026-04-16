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

//! [`EliminateOuterJoin`] converts `LEFT/RIGHT/FULL` joins to `INNER` joins
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::logical_plan::{Join, JoinType, LogicalPlan};
use datafusion_expr::{Expr, Filter, Operator};

use crate::optimizer::ApplyOrder;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::expr::{BinaryExpr, Cast, InList, Like, TryCast};
use std::sync::Arc;

///
/// Attempt to replace outer joins with inner joins.
///
/// Outer joins are typically more expensive to compute at runtime
/// than inner joins and prevent various forms of predicate pushdown
/// and other optimizations, so removing them if possible is beneficial.
///
/// Inner joins filter out rows that do match. Outer joins pass rows
/// that do not match padded with nulls. If there is a filter in the
/// query that would filter any such null rows after the join the rows
/// introduced by the outer join are filtered.
///
/// For example, in the `select ... from a left join b on ... where b.xx = 100;`
///
/// For rows when `b.xx` is null (as it would be after an outer join),
/// the `b.xx = 100` predicate filters them out and there is no
/// need to produce null rows for output.
///
/// Generally, an outer join can be rewritten to inner join if the
/// filters from the WHERE clause return false while any inputs are
/// null and columns of those quals are come from nullable side of
/// outer join.
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
        match plan {
            LogicalPlan::Filter(mut filter) => match Arc::unwrap_or_clone(filter.input) {
                LogicalPlan::Join(join) => {
                    let mut non_nullable_cols: Vec<Column> = vec![];

                    extract_non_nullable_columns(
                        &filter.predicate,
                        &mut non_nullable_cols,
                        join.left.schema(),
                        join.right.schema(),
                        true,
                    );

                    let new_join_type = if join.join_type.is_outer() {
                        let mut left_non_nullable = false;
                        let mut right_non_nullable = false;
                        for col in non_nullable_cols.iter() {
                            if join.left.schema().has_column(col) {
                                left_non_nullable = true;
                            }
                            if join.right.schema().has_column(col) {
                                right_non_nullable = true;
                            }
                        }
                        eliminate_outer(
                            join.join_type,
                            left_non_nullable,
                            right_non_nullable,
                        )
                    } else {
                        join.join_type
                    };

                    let new_join = Arc::new(LogicalPlan::Join(Join {
                        left: join.left,
                        right: join.right,
                        join_type: new_join_type,
                        join_constraint: join.join_constraint,
                        on: join.on.clone(),
                        filter: join.filter.clone(),
                        schema: Arc::clone(&join.schema),
                        null_equality: join.null_equality,
                        null_aware: join.null_aware,
                    }));
                    Filter::try_new(filter.predicate, new_join)
                        .map(|f| Transformed::yes(LogicalPlan::Filter(f)))
                }
                filter_input => {
                    filter.input = Arc::new(filter_input);
                    Ok(Transformed::no(LogicalPlan::Filter(filter)))
                }
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}

pub fn eliminate_outer(
    join_type: JoinType,
    left_non_nullable: bool,
    right_non_nullable: bool,
) -> JoinType {
    let mut new_join_type = join_type;
    match join_type {
        JoinType::Left => {
            if right_non_nullable {
                new_join_type = JoinType::Inner;
            }
        }
        JoinType::Right => {
            if left_non_nullable {
                new_join_type = JoinType::Inner;
            }
        }
        JoinType::Full => {
            if left_non_nullable && right_non_nullable {
                new_join_type = JoinType::Inner;
            } else if left_non_nullable {
                new_join_type = JoinType::Left;
            } else if right_non_nullable {
                new_join_type = JoinType::Right;
            }
        }
        _ => {}
    }
    new_join_type
}

/// Recursively traverses expr, if expr returns false when
/// any inputs are null, treats columns of both sides as non_nullable columns.
///
/// For and/or expr, extracts from all sub exprs and merges the columns.
/// For or expr, if one of sub exprs returns true, discards all columns from or expr.
/// For IS NOT NULL/NOT expr, always returns false for NULL input.
///     extracts columns from these exprs.
/// For all other exprs, fall through
fn extract_non_nullable_columns(
    expr: &Expr,
    non_nullable_cols: &mut Vec<Column>,
    left_schema: &Arc<DFSchema>,
    right_schema: &Arc<DFSchema>,
    top_level: bool,
) {
    match expr {
        Expr::Column(col) => {
            non_nullable_cols.push(col.clone());
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            // If one of the inputs are null for these operators, the results should be false.
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                extract_non_nullable_columns(
                    left,
                    non_nullable_cols,
                    left_schema,
                    right_schema,
                    false,
                );
                extract_non_nullable_columns(
                    right,
                    non_nullable_cols,
                    left_schema,
                    right_schema,
                    false,
                )
            }
            Operator::And | Operator::Or => {
                // treat And as Or if does not from top level, such as
                // not (c1 < 10 and c2 > 100)
                if top_level && *op == Operator::And {
                    extract_non_nullable_columns(
                        left,
                        non_nullable_cols,
                        left_schema,
                        right_schema,
                        top_level,
                    );
                    extract_non_nullable_columns(
                        right,
                        non_nullable_cols,
                        left_schema,
                        right_schema,
                        top_level,
                    );
                    return;
                }

                let mut left_non_nullable_cols: Vec<Column> = vec![];
                let mut right_non_nullable_cols: Vec<Column> = vec![];

                extract_non_nullable_columns(
                    left,
                    &mut left_non_nullable_cols,
                    left_schema,
                    right_schema,
                    top_level,
                );
                extract_non_nullable_columns(
                    right,
                    &mut right_non_nullable_cols,
                    left_schema,
                    right_schema,
                    top_level,
                );

                // for query: select *** from a left join b where b.c1 ... or b.c2 ...
                // this can be eliminated to inner join.
                // for query: select *** from a left join b where a.c1 ... or b.c2 ...
                // this can not be eliminated.
                // If columns of relation exist in both sub exprs, any columns of this relation
                // can be added to non nullable columns.
                if !left_non_nullable_cols.is_empty()
                    && !right_non_nullable_cols.is_empty()
                {
                    for left_col in &left_non_nullable_cols {
                        for right_col in &right_non_nullable_cols {
                            if (left_schema.has_column(left_col)
                                && left_schema.has_column(right_col))
                                || (right_schema.has_column(left_col)
                                    && right_schema.has_column(right_col))
                            {
                                non_nullable_cols.push(left_col.clone());
                                break;
                            }
                        }
                    }
                }
            }
            _ => {}
        },
        Expr::Not(arg) => extract_non_nullable_columns(
            arg,
            non_nullable_cols,
            left_schema,
            right_schema,
            false,
        ),
        Expr::IsNotNull(arg) => {
            if !top_level {
                return;
            }
            extract_non_nullable_columns(
                arg,
                non_nullable_cols,
                left_schema,
                right_schema,
                false,
            )
        }
        Expr::Cast(Cast { expr, field: _ })
        | Expr::TryCast(TryCast { expr, field: _ }) => extract_non_nullable_columns(
            expr,
            non_nullable_cols,
            left_schema,
            right_schema,
            false,
        ),
        // IN list and BETWEEN are null-rejecting on the input expression:
        // if the input column is NULL, the result is NULL (filtered out),
        // regardless of whether the list/range contains NULLs.
        Expr::InList(InList { expr, .. }) => extract_non_nullable_columns(
            expr,
            non_nullable_cols,
            left_schema,
            right_schema,
            false,
        ),
        Expr::Between(between) => extract_non_nullable_columns(
            &between.expr,
            non_nullable_cols,
            left_schema,
            right_schema,
            false,
        ),
        // LIKE is null-rejecting: if either the input column or the pattern
        // is NULL, the result is NULL (filtered out by WHERE).
        Expr::Like(Like { expr, pattern, .. }) => {
            extract_non_nullable_columns(
                expr,
                non_nullable_cols,
                left_schema,
                right_schema,
                false,
            );
            extract_non_nullable_columns(
                pattern,
                non_nullable_cols,
                left_schema,
                right_schema,
                false,
            );
        }
        // IS TRUE, IS FALSE, and IS NOT UNKNOWN are null-rejecting:
        // if the input is NULL, they return false (filtered out by WHERE).
        // Note: IS NOT TRUE, IS NOT FALSE, and IS UNKNOWN are NOT null-rejecting
        // because they return true for NULL input.
        Expr::IsTrue(arg) | Expr::IsFalse(arg) | Expr::IsNotUnknown(arg) => {
            extract_non_nullable_columns(
                arg,
                non_nullable_cols,
                left_schema,
                right_schema,
                false,
            )
        }
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
        try_cast,
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
}
