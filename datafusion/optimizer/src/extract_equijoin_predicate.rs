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
use crate::utils::split_conjunction;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::utils::{can_hash, find_valid_equijoin_key_pair};
use datafusion_expr::{BinaryExpr, Expr, ExprSchemable, Join, LogicalPlan, Operator};
use std::sync::Arc;

// equijoin predicate
type EquijoinPredicate = (Expr, Expr);

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
                    let (equijoin_predicates, non_equijoin_expr) =
                        split_eq_and_noneq_join_predicate(
                            expr,
                            left_schema,
                            right_schema,
                        )?;

                    let optimized_plan = (!equijoin_predicates.is_empty()).then(|| {
                        let mut new_on = on.clone();
                        new_on.extend(equijoin_predicates);

                        LogicalPlan::Join(Join {
                            left: left.clone(),
                            right: right.clone(),
                            on: new_on,
                            filter: non_equijoin_expr,
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

fn split_eq_and_noneq_join_predicate(
    filter: &Expr,
    left_schema: &Arc<DFSchema>,
    right_schema: &Arc<DFSchema>,
) -> Result<(Vec<EquijoinPredicate>, Option<Expr>)> {
    let exprs = split_conjunction(filter);

    let mut accum_join_keys: Vec<(Expr, Expr)> = vec![];
    let mut accum_filters: Vec<Expr> = vec![];
    for expr in exprs {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) => {
                let left = left.as_ref();
                let right = right.as_ref();

                let join_key_pair = find_valid_equijoin_key_pair(
                    left,
                    right,
                    left_schema.clone(),
                    right_schema.clone(),
                )?;

                if let Some((left_expr, right_expr)) = join_key_pair {
                    let left_expr_type = left_expr.get_type(left_schema)?;
                    let right_expr_type = right_expr.get_type(right_schema)?;

                    if can_hash(&left_expr_type) && can_hash(&right_expr_type) {
                        accum_join_keys.push((left_expr, right_expr));
                    } else {
                        accum_filters.push(expr.clone());
                    }
                } else {
                    accum_filters.push(expr.clone());
                }
            }
            _ => accum_filters.push(expr.clone()),
        }
    }

    let result_filter = accum_filters.into_iter().reduce(Expr::and);
    Ok((accum_join_keys, result_filter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use arrow::datatypes::DataType;
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
        let expected = "Left Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
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
        let expected = "Left Join: t1.a + Int64(10) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
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
        let expected = "Left Join:  Filter: t1.a + Int64(10) >= t2.a * UInt32(2) AND t1.b < Int32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
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
        let expected = "Left Join: t1.a + UInt32(11) = t2.a * UInt32(2), t1.a + Int64(10) = t2.a * UInt32(2) Filter: t1.b < Int32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
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
        let expected = "Left Join: t1.a = t2.a, t1.b = t2.b Filter: t1.c = t2.c OR t1.a + t1.b > t2.b + t2.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
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
        let expected = "Left Join: t1.a = t2.a Filter: t1.c + t2.c + t3.c < UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
            \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
            \n  Left Join: t2.a = t3.a Filter: t2.a + t3.b > UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
            \n    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
            \n    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn join_with_multiple_table_and_eq_filter() -> Result<()> {
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
                Some(col("t1.a").eq(col("t2.a")).and(col("t2.c").eq(col("t3.c")))),
            )?
            .build()?;
        let expected = "Left Join: t1.a = t2.a Filter: t2.c = t3.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
        \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
        \n  Left Join: t2.a = t3.a Filter: t2.a + t3.b > UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
        \n    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }

    #[test]
    fn join_with_alias_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let t1_schema = t1.schema().clone();
        let t2_schema = t2.schema().clone();

        // filter: t1.a + CAST(Int64(1), UInt32) = t2.a + CAST(Int64(2), UInt32) as t1.a + 1 = t2.a + 2
        let filter = Expr::eq(
            col("t1.a") + lit(1i64).cast_to(&DataType::UInt32, &t1_schema)?,
            col("t2.a") + lit(2i32).cast_to(&DataType::UInt32, &t2_schema)?,
        )
        .alias("t1.a + 1 = t2.a + 2");
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(filter),
            )?
            .build()?;
        let expected = "Left Join: t1.a + CAST(Int64(1) AS UInt32) = t2.a + CAST(Int32(2) AS UInt32) [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, b:UInt32;N, c:UInt32;N]\
        \n  TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]\
        \n  TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_plan_eq(&plan, expected)
    }
}
