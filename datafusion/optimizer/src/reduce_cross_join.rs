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
    logical_plan::{Filter, Join, CrossJoin, JoinType, LogicalPlan},
    utils::from_plan, and, or, utils::can_hash
};
use datafusion_expr::{Expr, Operator};

use std::collections::HashSet;

//use std::collections::HashMap;
use std::sync::Arc;
use datafusion_expr::logical_plan::JoinConstraint;

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
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut possible_join_keys : Vec<(Column, Column)> = vec![];
        let mut all_join_keys = HashSet::new();

        reduce_cross_join(self, plan, &mut possible_join_keys, &mut all_join_keys, optimizer_config)
    }

    fn name(&self) -> &str {
        "reduce_cross_join"
    }
}

/// Attempt to reduce cross joins to inner joins.
/// for queries: select ... from a, b where a.x = b.y and b.xx = 100;
/// select ... from a, b where (a.x = b.y and b.xx = 100) or (a.x = b.y and b.xx = 200);
/// select ... from a, b, c where (a.x = b.y and b.xx = 100 and a.z = c.z)
/// or (a.x = b.y and b.xx = 200 and a.z=c.z);
/// For above queries, the join predicate is available in filters and they are moved to
/// join nodes appropriately
/// This fix helps to improve the performance of TPCH Q19. issue#78
///
fn reduce_cross_join(
    _optimizer: &ReduceCrossJoin,
    plan: &LogicalPlan,
    possible_join_keys: &mut Vec<(Column, Column)>,
    all_join_keys: &mut HashSet<(Column, Column)>,
    _optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { input, predicate }) => {
                extract_possible_join_keys(predicate, possible_join_keys)?;
                let left = reduce_cross_join(_optimizer, input, possible_join_keys, all_join_keys, _optimizer_config)?;

                // if there are no join keys then do nothing.
                if all_join_keys.is_empty() {
                    Ok(plan.clone())
                }
                else {
                    // remove join expressions from filter
                    match remove_join_expressions(predicate, all_join_keys)? {
                        Some(filter_expr) => {
                            Ok(LogicalPlan::Filter(Filter {
                                predicate: filter_expr,
                                input: Arc::new(left),
                            }))
                        }
                        _ => Ok(left)
                    }
                }
            }
            LogicalPlan::CrossJoin(cross_join) => {
                let left_plan = reduce_cross_join(
                    _optimizer,
                    &cross_join.left,
                    possible_join_keys,
                    all_join_keys,
                    _optimizer_config,
                )?;
                let right_plan = reduce_cross_join(
                    _optimizer,
                    &cross_join.right,
                    possible_join_keys,
                    all_join_keys,
                    _optimizer_config,
                )?;
                // can we find a match?
                let left_schema = left_plan.schema();
                let right_schema = right_plan.schema();
                let mut join_keys = vec![];

                for (l, r) in possible_join_keys {
                    if left_schema.field_from_column(l).is_ok()
                        && right_schema.field_from_column(r).is_ok()
                        && can_hash(
                        left_schema
                            .field_from_column(l)
                            .unwrap()
                            .data_type(),
                    )
                    {
                        join_keys.push((l.clone(), r.clone()));
                    } else if left_schema.field_from_column(r).is_ok()
                        && right_schema.field_from_column(l).is_ok()
                        && can_hash(
                        left_schema
                            .field_from_column(r)
                            .unwrap()
                            .data_type(),
                    )
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
                }
                else {
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
                        reduce_cross_join(
                            _optimizer,
                            plan,
                            possible_join_keys,
                            all_join_keys,
                            _optimizer_config,
                        )
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
) -> Result<()>  {

    for x1 in vec1.iter() {
        for x2 in vec2.iter() {
            if x1.0 == x2.0 && x1.1 == x2.1
                || x1.1 == x2.0 && x1.0 == x2.1
            {
                accum.push((x1.0.clone(), x1.1.clone()));
            }
        }
    }

    Ok(())
}

/// Extract join keys from a WHERE clause
fn extract_possible_join_keys(
    expr: &Expr,
    accum: &mut Vec<(Column, Column)>,
) -> Result<()> {
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => {
                    // Ensure that we don't add the same Join keys multiple times
                    if !(accum.contains(&(l.clone(), r.clone())) ||
                        accum.contains(&(r.clone(), l.clone()))) {
                        accum.push((l.clone(), r.clone()));
                    }
                    Ok(())
                }
                _ => Ok(()),
            },
            Operator::And => {
                extract_possible_join_keys(left, accum)?;
                extract_possible_join_keys(right, accum)
            },
            // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
            Operator::Or => {
                let mut left_join_keys = vec![];
                let mut right_join_keys = vec![];

                extract_possible_join_keys(left, &mut left_join_keys)?;
                extract_possible_join_keys(right, &mut right_join_keys)?;

                intersect( accum, &left_join_keys, &right_join_keys)
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

/// Remove join expressions from a filter expression
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
            },
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

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = ReduceCrossJoin::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimized_plan.schema());
    }

    #[test]
    fn reduce_cross_with_simple_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(
                &t2)?
            .filter(binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").lt(lit(20u32)),
                ))?
            .build()?;

        let expected = "\
        Filter: #t2.c < UInt32(20)\
        \n  Inner Join: #t1.a = #t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(
                &t2)?
            .filter(binary_expr(
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").lt(lit(20u32)),
                ),
                And,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(10u32)),
                )
            ))?
            .build()?;

        let expected = "\
        Filter: #t2.c < UInt32(20) AND #t2.c = UInt32(10)\
        \n  Inner Join: #t1.a = #t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(
                &t2)?
            .filter(binary_expr(
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").lt(lit(15u32)),
                ),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").eq(lit(688u32)),
                )
            ))?
            .build()?;

        let expected = "\
        Filter: #t2.c < UInt32(15) OR #t2.c = UInt32(688)\
        \n  Inner Join: #t1.a = #t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_cross_not_possible() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(
                &t2)?
            .filter(binary_expr(
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    And,
                    col("t2.c").lt(lit(15u32)),
                ),
                Or,
                binary_expr(
                    col("t1.a").eq(col("t2.a")),
                    Or,
                    col("t2.c").eq(lit(688u32)),
                )
            ))?
            .build()?;

        let expected = "\
        Filter: #t1.a = #t2.a AND #t2.c < UInt32(15) OR #t1.a = #t2.a OR #t2.c = UInt32(688)\
        \n  CrossJoin:\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
