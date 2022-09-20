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

//! Optimizer rule to reduce cross join to inner join if join predicates are available in filters.
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, Result};
use datafusion_expr::{
    and,
    logical_plan::{CrossJoin, Filter, Join, JoinType, LogicalPlan},
    or,
    utils::can_hash,
    utils::from_plan,
};
use datafusion_expr::{Expr, Operator};

use std::collections::HashSet;

//use std::collections::HashMap;
use datafusion_expr::logical_plan::JoinConstraint;
use std::sync::Arc;

#[derive(Default)]
pub struct ReduceCrossJoin;

impl ReduceCrossJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ReduceCrossJoin {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut possible_join_keys: Vec<(Column, Column)> = vec![];
        let mut all_join_keys = HashSet::new();

        reduce_cross_join(self, plan, &mut possible_join_keys, &mut all_join_keys)
    }

    fn name(&self) -> &str {
        "reduce_cross_join"
    }
}

/// Attempt to reduce cross joins to inner joins.
/// for queries:
/// 'select ... from a, b where a.x = b.y and b.xx = 100;'
/// 'select ... from a, b where (a.x = b.y and b.xx = 100) or (a.x = b.y and b.xx = 200);'
/// 'select ... from a, b, c where (a.x = b.y and b.xx = 100 and a.z = c.z)
/// or (a.x = b.y and b.xx = 200 and a.z=c.z);'
/// For above queries, the join predicate is available in filters and they are moved to
/// join nodes appropriately
/// This fix helps to improve the performance of TPCH Q19. issue#78
///
fn reduce_cross_join(
    _optimizer: &ReduceCrossJoin,
    plan: &LogicalPlan,
    possible_join_keys: &mut Vec<(Column, Column)>,
    all_join_keys: &mut HashSet<(Column, Column)>,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter(Filter { input, predicate }) => {
            // join keys are handled locally
            let mut new_possible_join_keys: Vec<(Column, Column)> = vec![];
            let mut new_all_join_keys = HashSet::new();

            extract_possible_join_keys(predicate, &mut new_possible_join_keys);

            let new_plan = reduce_cross_join(
                _optimizer,
                input,
                &mut new_possible_join_keys,
                &mut new_all_join_keys,
            )?;

            // if there are no join keys then do nothing.
            if new_all_join_keys.is_empty() {
                Ok(LogicalPlan::Filter(Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(new_plan),
                }))
            } else {
                // remove join expressions from filter
                match remove_join_expressions(predicate, &new_all_join_keys)? {
                    Some(filter_expr) => Ok(LogicalPlan::Filter(Filter {
                        predicate: filter_expr,
                        input: Arc::new(new_plan),
                    })),
                    _ => Ok(new_plan),
                }
            }
        }
        LogicalPlan::CrossJoin(cross_join) => {
            let left_plan = reduce_cross_join(
                _optimizer,
                &cross_join.left,
                possible_join_keys,
                all_join_keys,
            )?;
            let right_plan = reduce_cross_join(
                _optimizer,
                &cross_join.right,
                possible_join_keys,
                all_join_keys,
            )?;
            // can we find a match?
            let left_schema = left_plan.schema();
            let right_schema = right_plan.schema();
            let mut join_keys = vec![];

            for (l, r) in possible_join_keys {
                if left_schema.field_from_column(l).is_ok()
                    && right_schema.field_from_column(r).is_ok()
                    && can_hash(left_schema.field_from_column(l).unwrap().data_type())
                {
                    join_keys.push((l.clone(), r.clone()));
                } else if left_schema.field_from_column(r).is_ok()
                    && right_schema.field_from_column(l).is_ok()
                    && can_hash(left_schema.field_from_column(r).unwrap().data_type())
                {
                    join_keys.push((r.clone(), l.clone()));
                }
            }

            // if there are no join keys then do nothing.
            if join_keys.is_empty() {
                Ok(LogicalPlan::CrossJoin(CrossJoin {
                    left: Arc::new(left_plan),
                    right: Arc::new(right_plan),
                    schema: cross_join.schema.clone(),
                }))
            } else {
                // Keep track of join keys being pushed to Join nodes
                all_join_keys.extend(join_keys.clone());

                Ok(LogicalPlan::Join(Join {
                    left: Arc::new(left_plan),
                    right: Arc::new(right_plan),
                    join_type: JoinType::Inner,
                    join_constraint: JoinConstraint::On,
                    on: join_keys,
                    filter: None,
                    schema: cross_join.schema.clone(),
                    null_equals_null: false,
                }))
            }
        }
        _ => {
            let expr = plan.expressions();

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| {
                    reduce_cross_join(_optimizer, plan, possible_join_keys, all_join_keys)
                })
                .collect::<Result<Vec<_>>>()?;

            from_plan(plan, &expr, &new_inputs)
        }
    }
}

fn intersect(
    accum: &mut Vec<(Column, Column)>,
    vec1: &[(Column, Column)],
    vec2: &[(Column, Column)],
) {
    for x1 in vec1.iter() {
        for x2 in vec2.iter() {
            if x1.0 == x2.0 && x1.1 == x2.1 || x1.1 == x2.0 && x1.0 == x2.1 {
                accum.push((x1.0.clone(), x1.1.clone()));
            }
        }
    }
}

/// Extract join keys from a WHERE clause
fn extract_possible_join_keys(expr: &Expr, accum: &mut Vec<(Column, Column)>) {
    if let Expr::BinaryExpr { left, op, right } = expr {
        match op {
            Operator::Eq => {
                if let (Expr::Column(l), Expr::Column(r)) =
                    (left.as_ref(), right.as_ref())
                {
                    // Ensure that we don't add the same Join keys multiple times
                    if !(accum.contains(&(l.clone(), r.clone()))
                        || accum.contains(&(r.clone(), l.clone())))
                    {
                        accum.push((l.clone(), r.clone()));
                    }
                }
            }
            Operator::And => {
                extract_possible_join_keys(left, accum);
                extract_possible_join_keys(right, accum)
            }
            // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
            Operator::Or => {
                let mut left_join_keys = vec![];
                let mut right_join_keys = vec![];

                extract_possible_join_keys(left, &mut left_join_keys);
                extract_possible_join_keys(right, &mut right_join_keys);

                intersect(accum, &left_join_keys, &right_join_keys)
            }
            _ => (),
        }
    }
}

/// Remove join expressions from a filter expression
/// Returns Some() when there are few remaining predicates in filter_expr
/// Returns None otherwise
fn remove_join_expressions(
    expr: &Expr,
    join_columns: &HashSet<(Column, Column)>,
) -> Result<Option<Expr>> {
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => {
                    if join_columns.contains(&(l.clone(), r.clone()))
                        || join_columns.contains(&(r.clone(), l.clone()))
                    {
                        Ok(None)
                    } else {
                        Ok(Some(expr.clone()))
                    }
                }
                _ => Ok(Some(expr.clone())),
            },
            Operator::And => {
                let l = remove_join_expressions(left, join_columns)?;
                let r = remove_join_expressions(right, join_columns)?;
                match (l, r) {
                    (Some(ll), Some(rr)) => Ok(Some(and(ll, rr))),
                    (Some(ll), _) => Ok(Some(ll)),
                    (_, Some(rr)) => Ok(Some(rr)),
                    _ => Ok(None),
                }
            }
            // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
            Operator::Or => {
                let l = remove_join_expressions(left, join_columns)?;
                let r = remove_join_expressions(right, join_columns)?;
                match (l, r) {
                    (Some(ll), Some(rr)) => Ok(Some(or(ll, rr))),
                    (Some(ll), _) => Ok(Some(ll)),
                    (_, Some(rr)) => Ok(Some(rr)),
                    _ => Ok(None),
                }
            }
            _ => Ok(Some(expr.clone())),
        },
        _ => Ok(Some(expr.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{
        binary_expr, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        Operator::{And, Or},
    };

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: Vec<&str>) {
        let rule = ReduceCrossJoin::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted = optimized_plan.display_indent_schema().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();

        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected, actual
        );

        assert_eq!(plan.schema(), optimized_plan.schema())
    }

    #[test]
    fn reduce_cross_with_simple_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could reduce to inner join since filter has Join predicates
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                col("t1.a").eq(col("t2.a")),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t2.c < UInt32(20) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: #t1.a = #t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_with_simple_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not reduce to inner join since filter OR expression and there is no common
        // Join predicates in left and right of OR expr.
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                col("t1.a").eq(col("t2.a")),
                Or,
                col("t2.b").eq(col("t1.a")),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t1.a = #t2.a OR #t2.b = #t1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(20u32))),
                And,
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").eq(lit(10u32))),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t2.c < UInt32(20) AND #t2.c = UInt32(10) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: #t1.a = #t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could reduce to inner join since Or predicates have common Join predicates
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t2.c < UInt32(15) OR #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: #t1.a = #t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_not_possible_simple() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.b").eq(col("t2.b")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t1.a = #t2.a AND #t2.c < UInt32(15) OR #t1.b = #t2.b AND #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_not_possible() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(col("t1.a").eq(col("t2.a")), Or, col("t2.c").eq(lit(688u32))),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t1.a = #t2.a AND #t2.c < UInt32(15) OR #t1.a = #t2.a OR #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_join_multi_tables() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could reduce to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let plan2 = LogicalPlanBuilder::from(t3)
            .cross_join(&t4)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.c").eq(lit(688u32)),
                    ),
                ),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t4.a")),
                    And,
                    col("t3.b").eq(col("t4.b")),
                ),
            ))?
            .build()?;

        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(&plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t1.a")),
                    And,
                    col("t4.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t4.c < UInt32(15) OR #t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: #t1.a = #t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t2.c < UInt32(15) OR #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: #t1.a = #t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t4.c < UInt32(15) OR #t3.c = UInt32(688) OR #t3.b = #t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: #t3.a = #t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_join_multi_tables_1() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could reduce to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // could reduce to inner join
        let plan2 = LogicalPlanBuilder::from(t3)
            .cross_join(&t4)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.c").eq(lit(688u32)),
                    ),
                ),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t4.a")),
                    And,
                    col("t3.b").eq(col("t4.b")),
                ),
            ))?
            .build()?;

        // could not reduce to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(&plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(col("t3.a").eq(col("t1.a")), Or, col("t4.c").eq(lit(688u32))),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t3.a = #t1.a AND #t4.c < UInt32(15) OR #t3.a = #t1.a OR #t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t2.c < UInt32(15) OR #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: #t1.a = #t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t4.c < UInt32(15) OR #t3.c = UInt32(688) OR #t3.b = #t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: #t3.a = #t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_join_multi_tables_2() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could reduce to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // could not reduce to inner join
        let plan2 = LogicalPlanBuilder::from(t3)
            .cross_join(&t4)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.c").eq(lit(688u32)),
                    ),
                ),
                Or,
                binary_expr(col("t3.a").eq(col("t4.a")), Or, col("t3.b").eq(col("t4.b"))),
            ))?
            .build()?;

        // could reduce to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(&plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t1.a")),
                    And,
                    col("t4.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t4.c < UInt32(15) OR #t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: #t1.a = #t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t2.c < UInt32(15) OR #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: #t1.a = #t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t3.a = #t4.a AND #t4.c < UInt32(15) OR #t3.a = #t4.a AND #t3.c = UInt32(688) OR #t3.a = #t4.a OR #t3.b = #t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_join_multi_tables_3() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could not reduce to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), Or, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // could reduce to inner join
        let plan2 = LogicalPlanBuilder::from(t3)
            .cross_join(&t4)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.c").eq(lit(688u32)),
                    ),
                ),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t4.a")),
                    And,
                    col("t3.b").eq(col("t4.b")),
                ),
            ))?
            .build()?;

        // could reduce to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(&plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(
                    col("t3.a").eq(col("t1.a")),
                    And,
                    col("t4.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t4.c < UInt32(15) OR #t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: #t1.a = #t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t1.a = #t2.a OR #t2.c < UInt32(15) OR #t1.a = #t2.a AND #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t4.c < UInt32(15) OR #t3.c = UInt32(688) OR #t3.b = #t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: #t3.a = #t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_join_multi_tables_4() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could reduce to inner join
        let plan1 = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), Or, col("t2.c").lt(lit(15u32))),
                And,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                ),
            ))?
            .build()?;

        // could reduce to inner join
        let plan2 = LogicalPlanBuilder::from(t3).cross_join(&t4)?.build()?;

        // could reduce to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(&plan2)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        col("t3.a").eq(col("t1.a")),
                        And,
                        col("t4.c").lt(lit(15u32)),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t1.a")),
                        And,
                        col("t4.c").eq(lit(688u32)),
                    ),
                ),
                And,
                binary_expr(
                    binary_expr(
                        binary_expr(
                            col("t3.a").eq(col("t4.a")),
                            And,
                            col("t4.c").lt(lit(15u32)),
                        ),
                        Or,
                        binary_expr(
                            col("t3.a").eq(col("t4.a")),
                            And,
                            col("t3.c").eq(lit(688u32)),
                        ),
                    ),
                    Or,
                    binary_expr(
                        col("t3.a").eq(col("t4.a")),
                        And,
                        col("t3.b").eq(col("t4.b")),
                    ),
                ),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t4.c < UInt32(15) OR #t4.c = UInt32(688) AND #t4.c < UInt32(15) OR #t3.c = UInt32(688) OR #t3.b = #t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: #t1.a = #t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: #t2.c < UInt32(15) AND #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: #t1.a = #t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: #t3.a = #t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_join_multi_tables_5() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could reduce to inner join
        let plan1 = LogicalPlanBuilder::from(t1).cross_join(&t2)?.build()?;

        // could reduce to inner join
        let plan2 = LogicalPlanBuilder::from(t3).cross_join(&t4)?.build()?;

        // could reduce to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(&plan2)?
            .filter(binary_expr(
                binary_expr(
                    binary_expr(
                        binary_expr(
                            col("t3.a").eq(col("t1.a")),
                            And,
                            col("t4.c").lt(lit(15u32)),
                        ),
                        Or,
                        binary_expr(
                            col("t3.a").eq(col("t1.a")),
                            And,
                            col("t4.c").eq(lit(688u32)),
                        ),
                    ),
                    And,
                    binary_expr(
                        binary_expr(
                            binary_expr(
                                col("t3.a").eq(col("t4.a")),
                                And,
                                col("t4.c").lt(lit(15u32)),
                            ),
                            Or,
                            binary_expr(
                                col("t3.a").eq(col("t4.a")),
                                And,
                                col("t3.c").eq(lit(688u32)),
                            ),
                        ),
                        Or,
                        binary_expr(
                            col("t3.a").eq(col("t4.a")),
                            And,
                            col("t3.b").eq(col("t4.b")),
                        ),
                    ),
                ),
                And,
                binary_expr(
                    binary_expr(
                        col("t1.a").eq(col("t2.a")),
                        Or,
                        col("t2.c").lt(lit(15u32)),
                    ),
                    And,
                    binary_expr(
                        col("t1.a").eq(col("t2.a")),
                        And,
                        col("t2.c").eq(lit(688u32)),
                    ),
                ),
            ))?
            .build()?;

        let expected =vec![
            "Filter: #t4.c < UInt32(15) OR #t4.c = UInt32(688) AND #t4.c < UInt32(15) OR #t3.c = UInt32(688) OR #t3.b = #t4.b AND #t2.c < UInt32(15) AND #t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: #t1.a = #t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: #t1.a = #t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: #t3.a = #t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
