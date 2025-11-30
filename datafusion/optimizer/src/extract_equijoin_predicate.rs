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

//! [`ExtractEquijoinPredicate`] identifies equality join (equijoin) predicates
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{assert_or_internal_err, DFSchema};
use datafusion_common::{NullEquality, Result};
use datafusion_expr::utils::split_conjunction_owned;
use datafusion_expr::utils::{can_hash, find_valid_equijoin_key_pair};
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, Join, LogicalPlan, Operator};
// equijoin predicate
type EquijoinPredicate = (Expr, Expr);

/// Optimizer that splits conjunctive join predicates into equijoin
/// predicates and (other) filter predicates.
///
/// Join algorithms are often highly optimized for equality predicates such as `x = y`,
/// often called `equijoin` predicates, so it is important to locate such predicates
/// and treat them specially.
///
/// For example, `SELECT ... FROM A JOIN B ON (A.x = B.y AND B.z > 50)`
/// has one equijoin predicate (`A.x = B.y`) and one filter predicate (`B.z > 50`).
/// See [find_valid_equijoin_key_pair] for more information on what predicates
/// are considered equijoins.
#[derive(Default, Debug)]
pub struct ExtractEquijoinPredicate;

impl ExtractEquijoinPredicate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ExtractEquijoinPredicate {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "extract_equijoin_predicate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(Join {
                left,
                right,
                mut on,
                filter: Some(expr),
                join_type,
                join_constraint,
                schema,
                null_equality,
            }) => {
                let left_schema = left.schema();
                let right_schema = right.schema();
                let (equijoin_predicates, non_equijoin_expr) =
                    split_eq_and_noneq_join_predicate(expr, left_schema, right_schema)?;

                // Equi-join operators like HashJoin support a special behavior
                // that evaluates `NULL = NULL` as true instead of NULL. Therefore,
                // we transform `t1.c1 IS NOT DISTINCT FROM t2.c1` into an equi-join
                // and set the `NullEquality` configuration in the join operator.
                // This allows certain queries to use Hash Join instead of
                // Nested Loop Join, resulting in better performance.
                //
                // Only convert when there are NO equijoin predicates, to be conservative.
                if on.is_empty()
                    && equijoin_predicates.is_empty()
                    && non_equijoin_expr.is_some()
                {
                    // SAFETY: checked in the outer `if`
                    let expr = non_equijoin_expr.clone().unwrap();
                    let (equijoin_predicates, non_equijoin_expr) =
                        split_is_not_distinct_from_and_other_join_predicate(
                            expr,
                            left_schema,
                            right_schema,
                        )?;

                    if !equijoin_predicates.is_empty() {
                        on.extend(equijoin_predicates);

                        return Ok(Transformed::yes(LogicalPlan::Join(Join {
                            left,
                            right,
                            on,
                            filter: non_equijoin_expr,
                            join_type,
                            join_constraint,
                            schema,
                            // According to `is not distinct from`'s semantics, it's
                            // safe to override it
                            null_equality: NullEquality::NullEqualsNull,
                        })));
                    }
                }

                if !equijoin_predicates.is_empty() {
                    on.extend(equijoin_predicates);
                    Ok(Transformed::yes(LogicalPlan::Join(Join {
                        left,
                        right,
                        on,
                        filter: non_equijoin_expr,
                        join_type,
                        join_constraint,
                        schema,
                        null_equality,
                    })))
                } else {
                    Ok(Transformed::no(LogicalPlan::Join(Join {
                        left,
                        right,
                        on,
                        filter: non_equijoin_expr,
                        join_type,
                        join_constraint,
                        schema,
                        null_equality,
                    })))
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

/// Splits an ANDed filter expression into equijoin predicates and remaining filters.
/// Returns all equijoin predicates and the remaining filters combined with AND.
///
/// # Example
///
/// For the expression `a.id = b.id AND a.x > 10 AND b.x > b.id`, this function will extract `a.id = b.id` as an equijoin predicate.
///
/// It first splits the ANDed sub-expressions:
/// - expr1: a.id = b.id
/// - expr2: a.x > 10
/// - expr3: b.x > b.id
///
/// Then, it filters out the equijoin predicates and collects the non-equality expressions.
/// The equijoin condition is:
/// - It is an equality expression like `lhs == rhs`
/// - All column references in `lhs` are from the left schema, and all in `rhs` are from the right schema
///
/// According to the above rule, `expr1` is the equijoin predicate, while `expr2` and `expr3` are not.
/// The function returns Ok(\[expr1\], Some(expr2 AND expr3))
fn split_eq_and_noneq_join_predicate(
    filter: Expr,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
) -> Result<(Vec<EquijoinPredicate>, Option<Expr>)> {
    split_op_and_other_join_predicates(filter, left_schema, right_schema, Operator::Eq)
}

/// See `split_eq_and_noneq_join_predicate`'s comment for the idea. This function
/// is splitting out `is not distinct from` expressions instead of equal exprs.
/// The `is not distinct from` exprs will be return as `EquijoinPredicate`.
///
/// # Example
/// - Input: `a.id IS NOT DISTINCT FROM b.id AND a.x > 10 AND b.x > b.id`
/// - Output from this splitter: `Ok([a.id, b.id], Some((a.x > 10) AND (b.x > b.id)))`
///
/// # Note
/// Caller should be cautious -- `is not distinct from` is not equivalent to an
/// equal expression; the caller is responsible for correctly setting the
/// `nulls equals nulls` property in the join operator (if it supports it) to
/// make the transformation valid.
///
/// For the above example: in downstream, a valid plan that uses the extracted
/// equijoin keys should look like:
///
/// HashJoin
/// - on: `a.id = b.id` (equality)
/// - join_filter: `(a.x > 10) AND (b.x > b.id)`
/// - nulls_equals_null: `true`
///
/// This reflects that `IS NOT DISTINCT FROM` treats `NULL = NULL` as true and
/// thus requires setting `NullEquality::NullEqualsNull` in the join operator to
/// preserve semantics while enabling an equi-join implementation (e.g., HashJoin).
fn split_is_not_distinct_from_and_other_join_predicate(
    filter: Expr,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
) -> Result<(Vec<EquijoinPredicate>, Option<Expr>)> {
    split_op_and_other_join_predicates(
        filter,
        left_schema,
        right_schema,
        Operator::IsNotDistinctFrom,
    )
}

/// See comments in `split_eq_and_noneq_join_predicate` for details.
fn split_op_and_other_join_predicates(
    filter: Expr,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
    operator: Operator,
) -> Result<(Vec<EquijoinPredicate>, Option<Expr>)> {
    assert_or_internal_err!(
        matches!(operator, Operator::Eq | Operator::IsNotDistinctFrom),
        "split_op_and_other_join_predicates only supports 'Eq' or 'IsNotDistinctFrom' operators, \
        but received: {:?}",
        operator
    );

    let exprs = split_conjunction_owned(filter);

    // Treat 'is not distinct from' comparison as join key in equal joins
    let mut accum_join_keys: Vec<(Expr, Expr)> = vec![];
    let mut accum_filters: Vec<Expr> = vec![];
    for expr in exprs {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref op,
                ref right,
            }) if *op == operator => {
                let join_key_pair =
                    find_valid_equijoin_key_pair(left, right, left_schema, right_schema)?;

                if let Some((left_expr, right_expr)) = join_key_pair {
                    let left_expr_type = left_expr.get_type(left_schema)?;
                    let right_expr_type = right_expr.get_type(right_schema)?;

                    if can_hash(&left_expr_type) && can_hash(&right_expr_type) {
                        accum_join_keys.push((left_expr, right_expr));
                    } else {
                        accum_filters.push(expr);
                    }
                } else {
                    accum_filters.push(expr);
                }
            }
            _ => accum_filters.push(expr),
        }
    }

    let result_filter = accum_filters.into_iter().reduce(Expr::and);
    Ok((accum_join_keys, result_filter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use crate::test::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::{
        col, lit, logical_plan::builder::LogicalPlanBuilder, JoinType,
    };
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(ExtractEquijoinPredicate {});
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn join_with_only_column_equi_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join_on(t2, JoinType::Left, Some(col("t1.a").eq(col("t2.a"))))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn join_with_only_equi_expr_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join_on(
                t2,
                JoinType::Left,
                Some((col("t1.a") + lit(10i64)).eq(col("t2.a") * lit(2u32))),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: t1.a + Int64(10) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn join_with_only_none_equi_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join_on(
                t2,
                JoinType::Left,
                Some(
                    (col("t1.a") + lit(10i64))
                        .gt_eq(col("t2.a") * lit(2u32))
                        .and(col("t1.b").lt(lit(100i32))),
                ),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join:  Filter: t1.a + Int64(10) >= t2.a * UInt32(2) AND t1.b < Int32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn join_with_expr_both_from_filter_and_keys() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join_with_expr_keys(
                t2,
                JoinType::Left,
                (
                    vec![col("t1.a") + lit(11u32)],
                    vec![col("t2.a") * lit(2u32)],
                ),
                Some(
                    (col("t1.a") + lit(10i64))
                        .eq(col("t2.a") * lit(2u32))
                        .and(col("t1.b").lt(lit(100i32))),
                ),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: t1.a + UInt32(11) = t2.a * UInt32(2), t1.a + Int64(10) = t2.a * UInt32(2) Filter: t1.b < Int32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn join_with_and_or_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join_on(
                t2,
                JoinType::Left,
                Some(
                    col("t1.c")
                        .eq(col("t2.c"))
                        .or((col("t1.a") + col("t1.b")).gt(col("t2.b") + col("t2.c")))
                        .and(
                            col("t1.a").eq(col("t2.a")).and(col("t1.b").eq(col("t2.b"))),
                        ),
                ),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: t1.a = t2.a, t1.b = t2.b Filter: t1.c = t2.c OR t1.a + t1.b > t2.b + t2.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn join_with_multiple_table() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        let input = LogicalPlanBuilder::from(t2)
            .join_on(
                t3,
                JoinType::Left,
                Some(
                    col("t2.a")
                        .eq(col("t3.a"))
                        .and((col("t2.a") + col("t3.b")).gt(lit(100u32))),
                ),
            )?
            .build()?;
        let plan = LogicalPlanBuilder::from(t1)
            .join_on(
                input,
                JoinType::Left,
                Some(
                    col("t1.a")
                        .eq(col("t2.a"))
                        .and((col("t1.c") + col("t2.c") + col("t3.c")).lt(lit(100u32))),
                ),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: t1.a = t2.a Filter: t1.c + t2.c + t3.c < UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
          Left Join: t2.a = t3.a Filter: t2.a + t3.b > UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
            TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
            TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn join_with_multiple_table_and_eq_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        let input = LogicalPlanBuilder::from(t2)
            .join_on(
                t3,
                JoinType::Left,
                Some(
                    col("t2.a")
                        .eq(col("t3.a"))
                        .and((col("t2.a") + col("t3.b")).gt(lit(100u32))),
                ),
            )?
            .build()?;
        let plan = LogicalPlanBuilder::from(t1)
            .join_on(
                input,
                JoinType::Left,
                Some(col("t1.a").eq(col("t2.a")).and(col("t2.c").eq(col("t3.c")))),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: t1.a = t2.a Filter: t2.c = t3.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
          Left Join: t2.a = t3.a Filter: t2.a + t3.b > UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
            TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
            TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn join_with_alias_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let t1_schema = Arc::clone(t1.schema());
        let t2_schema = Arc::clone(t2.schema());

        // filter: t1.a + CAST(Int64(1), UInt32) = t2.a + CAST(Int64(2), UInt32) as t1.a + 1 = t2.a + 2
        let filter = Expr::eq(
            col("t1.a") + lit(1i64).cast_to(&DataType::UInt32, &t1_schema)?,
            col("t2.a") + lit(2i32).cast_to(&DataType::UInt32, &t2_schema)?,
        )
        .alias("t1.a + 1 = t2.a + 2");
        let plan = LogicalPlanBuilder::from(t1)
            .join_on(t2, JoinType::Left, Some(filter))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: t1.a + CAST(Int64(1) AS UInt32) = t2.a + CAST(Int32(2) AS UInt32) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]
          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
          TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }
}
