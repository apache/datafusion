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

//! Optimizer rule to extract equijoin expr from filter
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::utils::{can_hash, check_all_column_from_schema};
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, Join, LogicalPlan, Operator};
use std::sync::Arc;

/// Optimization rule that extract equijoin expr from the filter
#[derive(Default)]
pub struct ExtractEquijoinPredicate;

impl ExtractEquijoinPredicate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ExtractEquijoinPredicate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equals_null,
            }) => {
                let left_schema = left.schema();
                let right_schema = right.schema();

                filter.as_ref().map_or(Result::Ok(None), |expr| {
                    let mut accum: Vec<(Expr, Expr)> = vec![];
                    let mut accum_filter: Vec<Expr> = vec![];
                    // TODO: avoding clone with split_conjunction
                    extract_join_keys(
                        expr.clone(),
                        &mut accum,
                        &mut accum_filter,
                        left_schema,
                        right_schema,
                    )?;

                    let optimized_plan = (!accum.is_empty()).then(|| {
                        let mut new_on = on.clone();
                        new_on.extend(accum);

                        let new_filter = accum_filter.into_iter().reduce(Expr::and);
                        LogicalPlan::Join(Join {
                            left: left.clone(),
                            right: right.clone(),
                            on: new_on,
                            filter: new_filter,
                            join_type: *join_type,
                            join_constraint: *join_constraint,
                            schema: schema.clone(),
                            null_equals_null: *null_equals_null,
                        })
                    });

                    Ok(optimized_plan)
                })
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "extract_equijoin_predicate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

/// Extracts equijoin ON condition be a single Eq or multiple conjunctive Eqs
/// Filters matching this pattern are added to `accum`
/// Filters that don't match this pattern are added to `accum_filter`
/// Examples:
/// ```text
/// foo = bar => accum=[(foo, bar)] accum_filter=[]
/// foo = bar AND bar = baz => accum=[(foo, bar), (bar, baz)] accum_filter=[]
/// foo = bar AND baz > 1 => accum=[(foo, bar)] accum_filter=[baz > 1]
///
/// For equijoin join key, assume we have tables -- a(c0, c1 c2) and b(c0, c1, c2):
/// (a.c0 = 10) => accum=[], accum_filter=[a.c0 = 10]
/// (a.c0 + 1 = b.c0 * 2) => accum=[(a.c0 + 1, b.c0 * 2)],  accum_filter=[]
/// (a.c0 + b.c0 = 10) =>  accum=[], accum_filter=[a.c0 + b.c0 = 10]
/// ```
fn extract_join_keys(
    expr: Expr,
    accum: &mut Vec<(Expr, Expr)>,
    accum_filter: &mut Vec<Expr>,
    left_schema: &Arc<DFSchema>,
    right_schema: &Arc<DFSchema>,
) -> Result<()> {
    match &expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => {
                let left = *left.clone();
                let right = *right.clone();
                let left_using_columns = left.to_columns()?;
                let right_using_columns = right.to_columns()?;

                // When one side key does not contain columns, we need move this expression to filter.
                // For example: a = 1, a = now() + 10.
                if left_using_columns.is_empty() || right_using_columns.is_empty() {
                    accum_filter.push(expr);
                    return Ok(());
                }

                // Checking left join key is from left schema, right join key is from right schema, or the opposite.
                let l_is_left = check_all_column_from_schema(
                    &left_using_columns,
                    left_schema.clone(),
                )?;
                let r_is_right = check_all_column_from_schema(
                    &right_using_columns,
                    right_schema.clone(),
                )?;

                let r_is_left_and_l_is_right = || {
                    let result = check_all_column_from_schema(
                        &right_using_columns,
                        left_schema.clone(),
                    )? && check_all_column_from_schema(
                        &left_using_columns,
                        right_schema.clone(),
                    )?;

                    Result::Ok(result)
                };

                let join_key_pair = match (l_is_left, r_is_right) {
                    (true, true) => Some((left, right)),
                    (_, _) if r_is_left_and_l_is_right()? => Some((right, left)),
                    _ => None,
                };

                if let Some((left_expr, right_expr)) = join_key_pair {
                    let left_expr_type = left_expr.get_type(left_schema)?;
                    let right_expr_type = right_expr.get_type(right_schema)?;

                    if can_hash(&left_expr_type) && can_hash(&right_expr_type) {
                        accum.push((left_expr, right_expr));
                    } else {
                        accum_filter.push(expr);
                    }
                } else {
                    accum_filter.push(expr);
                }
            }
            Operator::And => {
                if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = expr {
                    extract_join_keys(
                        *left,
                        accum,
                        accum_filter,
                        left_schema,
                        right_schema,
                    )?;
                    extract_join_keys(
                        *right,
                        accum,
                        accum_filter,
                        left_schema,
                        right_schema,
                    )?;
                }
            }
            _other => {
                accum_filter.push(expr);
            }
        },
        _other => {
            accum_filter.push(expr);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::Column;
    use datafusion_expr::{
        col, lit, logical_plan::builder::LogicalPlanBuilder, JoinType,
    };

    fn assert_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq_display_indent(
            Arc::new(ExtractEquijoinPredicate {}),
            plan,
            expected,
        );

        Ok(())
    }

    #[test]
    fn join_with_only_column_equi_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(col("t1.a").eq(col("t2.a"))),
            )?
            .build()?;
        let expected = "Left Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn join_with_only_equi_expr_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some((col("t1.a") + lit(10i64)).eq(col("t2.a") * lit(2u32))),
            )?
            .build()?;
        let expected = "Left Join: t1.a + Int64(10) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn join_with_only_none_equi_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(
                    (col("t1.a") + lit(10i64))
                        .gt_eq(col("t2.a") * lit(2u32))
                        .and(col("t1.b").lt(lit(100i32))),
                ),
            )?
            .build()?;
        let expected = "Left Join:  Filter: t1.a + Int64(10) >= t2.a * UInt32(2) AND t1.b < Int32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
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
        let expected = "Left Join: t1.a + UInt32(11) = t2.a * UInt32(2), t1.a + Int64(10) = t2.a * UInt32(2) Filter: t1.b < Int32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn join_with_and_or_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (Vec::<Column>::new(), Vec::<Column>::new()),
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
        let expected = "Left Join: t1.a = t2.a, t1.b = t2.b Filter: t1.c = t2.c OR t1.a + t1.b > t2.b + t2.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn join_with_multiple_table() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        let input = LogicalPlanBuilder::from(t2)
            .join(
                t3,
                JoinType::Left,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(
                    col("t2.a")
                        .eq(col("t3.a"))
                        .and((col("t2.a") + col("t3.b")).gt(lit(100u32))),
                ),
            )?
            .build()?;
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                input,
                JoinType::Left,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(
                    col("t1.a")
                        .eq(col("t2.a"))
                        .and((col("t1.c") + col("t2.c") + col("t3.c")).lt(lit(100u32))),
                ),
            )?
            .build()?;
        let expected = "Left Join: t1.a = t2.a Filter: t1.c + t2.c + t3.c < UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]\
            \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
            \n  Left Join: t2.a = t3.a Filter: t2.a + t3.b > UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }
}
