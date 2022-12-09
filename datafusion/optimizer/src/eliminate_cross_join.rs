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

//! Optimizer rule to eliminate cross join to inner join if join predicates are available in filters.
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::expr::{BinaryExpr, Expr};
use datafusion_expr::logical_plan::{
    CrossJoin, Filter, Join, JoinConstraint, JoinType, LogicalPlan, Projection,
};
use datafusion_expr::utils::{can_hash, check_all_column_from_schema};
use datafusion_expr::{
    and, build_join_schema, or, wrap_projection_for_join_if_necessary, ExprSchemable,
    Operator,
};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Default)]
pub struct EliminateCrossJoin;

impl EliminateCrossJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Attempt to reorder join tp eliminate cross joins to inner joins.
/// for queries:
/// 'select ... from a, b where a.x = b.y and b.xx = 100;'
/// 'select ... from a, b where (a.x = b.y and b.xx = 100) or (a.x = b.y and b.xx = 200);'
/// 'select ... from a, b, c where (a.x = b.y and b.xx = 100 and a.z = c.z)
/// or (a.x = b.y and b.xx = 200 and a.z=c.z);'
/// For above queries, the join predicate is available in filters and they are moved to
/// join nodes appropriately
/// This fix helps to improve the performance of TPCH Q19. issue#78
///
impl OptimizerRule for EliminateCrossJoin {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let input = (**filter.input()).clone();

                let mut possible_join_keys: Vec<(Expr, Expr)> = vec![];
                let mut all_inputs: Vec<LogicalPlan> = vec![];
                match &input {
                    LogicalPlan::Join(join) if (join.join_type == JoinType::Inner) => {
                        flatten_join_inputs(
                            &input,
                            &mut possible_join_keys,
                            &mut all_inputs,
                        )?;
                    }
                    LogicalPlan::CrossJoin(_) => {
                        flatten_join_inputs(
                            &input,
                            &mut possible_join_keys,
                            &mut all_inputs,
                        )?;
                    }
                    _ => {
                        return utils::optimize_children(self, plan, _optimizer_config);
                    }
                }

                let predicate = filter.predicate();
                // join keys are handled locally
                let mut all_join_keys: HashSet<(Expr, Expr)> = HashSet::new();

                extract_possible_join_keys(predicate, &mut possible_join_keys)?;

                let mut left = all_inputs.remove(0);
                while !all_inputs.is_empty() {
                    left = find_inner_join(
                        &left,
                        &mut all_inputs,
                        &mut possible_join_keys,
                        &mut all_join_keys,
                    )?;
                }

                left = utils::optimize_children(self, &left, _optimizer_config)?;

                if plan.schema() != left.schema() {
                    left = LogicalPlan::Projection(Projection::new_from_schema(
                        Arc::new(left.clone()),
                        plan.schema().clone(),
                    ));
                }

                // if there are no join keys then do nothing.
                if all_join_keys.is_empty() {
                    Ok(LogicalPlan::Filter(Filter::try_new(
                        predicate.clone(),
                        Arc::new(left),
                    )?))
                } else {
                    // remove join expressions from filter
                    match remove_join_expressions(predicate, &all_join_keys)? {
                        Some(filter_expr) => Ok(LogicalPlan::Filter(Filter::try_new(
                            filter_expr,
                            Arc::new(left),
                        )?)),
                        _ => Ok(left),
                    }
                }
            }

            _ => utils::optimize_children(self, plan, _optimizer_config),
        }
    }

    fn name(&self) -> &str {
        "eliminate_cross_join"
    }
}

fn flatten_join_inputs(
    plan: &LogicalPlan,
    possible_join_keys: &mut Vec<(Expr, Expr)>,
    all_inputs: &mut Vec<LogicalPlan>,
) -> Result<()> {
    let children = match plan {
        LogicalPlan::Join(join) => {
            for join_keys in join.on.iter() {
                let join_keys = join_keys.clone();
                possible_join_keys
                    .push((Expr::Column(join_keys.0), Expr::Column(join_keys.1)));
            }
            let left = &*(join.left);
            let right = &*(join.right);
            Ok::<Vec<&LogicalPlan>, DataFusionError>(vec![left, right])
        }
        LogicalPlan::CrossJoin(join) => {
            let left = &*(join.left);
            let right = &*(join.right);
            Ok::<Vec<&LogicalPlan>, DataFusionError>(vec![left, right])
        }
        _ => {
            return Err(DataFusionError::Plan(
                "flatten_join_inputs just can call join/cross_join".to_string(),
            ));
        }
    }?;

    for child in children.iter() {
        match *child {
            LogicalPlan::Join(left_join) => {
                if left_join.join_type == JoinType::Inner {
                    flatten_join_inputs(child, possible_join_keys, all_inputs)?;
                } else {
                    all_inputs.push((*child).clone());
                }
            }
            LogicalPlan::CrossJoin(_) => {
                flatten_join_inputs(child, possible_join_keys, all_inputs)?;
            }
            _ => all_inputs.push((*child).clone()),
        }
    }
    Ok(())
}

fn find_inner_join(
    left_input: &LogicalPlan,
    rights: &mut Vec<LogicalPlan>,
    possible_join_keys: &mut Vec<(Expr, Expr)>,
    all_join_keys: &mut HashSet<(Expr, Expr)>,
) -> Result<LogicalPlan> {
    for (i, right_input) in rights.iter().enumerate() {
        let mut join_keys = vec![];

        for (l, r) in &mut *possible_join_keys {
            let left_using_columns = l.to_columns()?;
            let right_using_columns = r.to_columns()?;

            // Conditions like a = 10, will be treated as filter.
            if left_using_columns.is_empty() || right_using_columns.is_empty() {
                continue;
            }

            let l_is_left = check_all_column_from_schema(
                &left_using_columns,
                left_input.schema().clone(),
            )?;
            let r_is_right = check_all_column_from_schema(
                &right_using_columns,
                right_input.schema().clone(),
            )?;

            let r_is_left_and_l_is_right = || {
                let result = check_all_column_from_schema(
                    &right_using_columns,
                    left_input.schema().clone(),
                )? && check_all_column_from_schema(
                    &left_using_columns,
                    right_input.schema().clone(),
                )?;

                Result::Ok(result)
            };

            // Save join keys
            if l_is_left && r_is_right && can_hash(&l.get_type(left_input.schema())?) {
                join_keys.push((l.clone(), r.clone()));
            } else if r_is_left_and_l_is_right()?
                && can_hash(&l.get_type(right_input.schema())?)
            {
                join_keys.push((r.clone(), l.clone()));
            }
        }

        if !join_keys.is_empty() {
            all_join_keys.extend(join_keys.clone());
            let right_input = rights.remove(i);
            let join_schema = Arc::new(build_join_schema(
                left_input.schema(),
                right_input.schema(),
                &JoinType::Inner,
            )?);

            // Wrap projection
            let (left_on, right_on): (Vec<Expr>, Vec<Expr>) =
                join_keys.into_iter().unzip();
            let (new_left_input, new_left_on, _) =
                wrap_projection_for_join_if_necessary(&left_on, left_input.clone())?;
            let (new_right_input, new_right_on, _) =
                wrap_projection_for_join_if_necessary(&right_on, right_input)?;

            // Build new join on
            let join_on = new_left_on
                .into_iter()
                .zip(new_right_on.into_iter())
                .collect::<Vec<_>>();

            return Ok(LogicalPlan::Join(Join {
                left: Arc::new(new_left_input),
                right: Arc::new(new_right_input),
                join_type: JoinType::Inner,
                join_constraint: JoinConstraint::On,
                on: join_on,
                filter: None,
                schema: join_schema,
                null_equals_null: false,
            }));
        }
    }
    let right = rights.remove(0);
    let join_schema = Arc::new(build_join_schema(
        left_input.schema(),
        right.schema(),
        &JoinType::Inner,
    )?);

    Ok(LogicalPlan::CrossJoin(CrossJoin {
        left: Arc::new(left_input.clone()),
        right: Arc::new(right),
        schema: join_schema,
    }))
}

fn intersect(
    accum: &mut Vec<(Expr, Expr)>,
    vec1: &[(Expr, Expr)],
    vec2: &[(Expr, Expr)],
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
fn extract_possible_join_keys(expr: &Expr, accum: &mut Vec<(Expr, Expr)>) -> Result<()> {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        match op {
            Operator::Eq => {
                // Ensure that we don't add the same Join keys multiple times
                if !(accum.contains(&(*left.clone(), *right.clone()))
                    || accum.contains(&(*right.clone(), *left.clone())))
                {
                    accum.push((*left.clone(), *right.clone()));
                }
            }
            Operator::And => {
                extract_possible_join_keys(left, accum)?;
                extract_possible_join_keys(right, accum)?
            }
            // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
            Operator::Or => {
                let mut left_join_keys = vec![];
                let mut right_join_keys = vec![];

                extract_possible_join_keys(left, &mut left_join_keys)?;
                extract_possible_join_keys(right, &mut right_join_keys)?;

                intersect(accum, &left_join_keys, &right_join_keys)
            }
            _ => (),
        };
    }
    Ok(())
}

/// Remove join expressions from a filter expression
/// Returns Some() when there are few remaining predicates in filter_expr
/// Returns None otherwise
fn remove_join_expressions(
    expr: &Expr,
    join_keys: &HashSet<(Expr, Expr)>,
) -> Result<Option<Expr>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::Eq => {
                if join_keys.contains(&(*left.clone(), *right.clone()))
                    || join_keys.contains(&(*right.clone(), *left.clone()))
                {
                    Ok(None)
                } else {
                    Ok(Some(expr.clone()))
                }
            }
            Operator::And => {
                let l = remove_join_expressions(left, join_keys)?;
                let r = remove_join_expressions(right, join_keys)?;
                match (l, r) {
                    (Some(ll), Some(rr)) => Ok(Some(and(ll, rr))),
                    (Some(ll), _) => Ok(Some(ll)),
                    (_, Some(rr)) => Ok(Some(rr)),
                    _ => Ok(None),
                }
            }
            // Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
            Operator::Or => {
                let l = remove_join_expressions(left, join_keys)?;
                let r = remove_join_expressions(right, join_keys)?;
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
        let rule = EliminateCrossJoin::new();
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
    fn eliminate_cross_with_simple_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join since filter has Join predicates
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                col("t1.a").eq(col("t2.a")),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        let expected = vec![
            "Filter: t2.c < UInt32(20) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_simple_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join since filter OR expression and there is no common
        // Join predicates in left and right of OR expr.
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                col("t1.a").eq(col("t2.a")),
                Or,
                col("t2.b").eq(col("t1.a")),
            ))?
            .build()?;

        let expected = vec![
            "Filter: t1.a = t2.a OR t2.b = t1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(20u32))),
                And,
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").eq(lit(10u32))),
            ))?
            .build()?;

        let expected = vec![
            "Filter: t2.c < UInt32(20) AND t2.c = UInt32(10) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join since Or predicates have common Join predicates
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

        let expected = vec![
            "Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_not_possible_simple() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join
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

        let expected = vec![
            "Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.b = t2.b AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_not_possible() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(col("t1.a").eq(col("t2.a")), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(col("t1.a").eq(col("t2.a")), Or, col("t2.c").eq(lit(688u32))),
            ))?
            .build()?;

        let expected = vec![
            "Filter: t1.a = t2.a AND t2.c < UInt32(15) OR t1.a = t2.a OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    /// ```txt
    /// filter: a.id = b.id and a.id = c.id
    ///   cross_join a (bc)
    ///     cross_join b c
    /// ```
    /// Without reorder, it will be
    /// ```txt
    ///   inner_join a (bc) on a.id = b.id and a.id = c.id
    ///     cross_join b c
    /// ```
    /// Reorder it to be
    /// ```txt
    ///   inner_join (ab)c and a.id = c.id
    ///     inner_join a b on a.id = b.id
    /// ```
    fn reorder_join_to_eliminate_cross_join_multi_tables() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .cross_join(&t3)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t3.c").lt(lit(15u32))),
                And,
                binary_expr(col("t3.a").eq(col("t2.a")), And, col("t3.b").lt(lit(15u32))),
            ))?
            .build()?;

        let expected = vec![
            "Filter: t3.c < UInt32(15) AND t3.b < UInt32(15) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c, t3.a, t3.b, t3.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: t3.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
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

        let expected = vec![
            "Filter: t4.c < UInt32(15) OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_1() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
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

        // could eliminate to inner join
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

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(plan1)
            .cross_join(&plan2)?
            .filter(binary_expr(
                binary_expr(col("t3.a").eq(col("t1.a")), And, col("t4.c").lt(lit(15u32))),
                Or,
                binary_expr(col("t3.a").eq(col("t1.a")), Or, col("t4.c").eq(lit(688u32))),
            ))?
            .build()?;

        let expected = vec![
            "Filter: t3.a = t1.a AND t4.c < UInt32(15) OR t3.a = t1.a OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_2() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
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

        // could not eliminate to inner join
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

        // could eliminate to inner join
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

        let expected = vec![
            "Filter: t4.c < UInt32(15) OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t3.a = t4.a AND t4.c < UInt32(15) OR t3.a = t4.a AND t3.c = UInt32(688) OR t3.a = t4.a OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_3() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could not eliminate to inner join
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

        // could eliminate to inner join
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

        // could eliminate to inner join
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

        let expected = vec![
            "Filter: t4.c < UInt32(15) OR t4.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t1.a = t2.a OR t2.c < UInt32(15) OR t1.a = t2.a AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "    Filter: t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_4() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
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

        // could eliminate to inner join
        let plan2 = LogicalPlanBuilder::from(t3).cross_join(&t4)?.build()?;

        // could eliminate to inner join
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

        let expected = vec![
            "Filter: (t4.c < UInt32(15) OR t4.c = UInt32(688)) AND (t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Filter: t2.c < UInt32(15) AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "          TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "          TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_multi_tables_5() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;
        let t4 = test_table_scan_with_name("t4")?;

        // could eliminate to inner join
        let plan1 = LogicalPlanBuilder::from(t1).cross_join(&t2)?.build()?;

        // could eliminate to inner join
        let plan2 = LogicalPlanBuilder::from(t3).cross_join(&t4)?.build()?;

        // could eliminate to inner join
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

        let expected = vec![
            "Filter: (t4.c < UInt32(15) OR t4.c = UInt32(688)) AND (t4.c < UInt32(15) OR t3.c = UInt32(688) OR t3.b = t4.b) AND t2.c < UInt32(15) AND t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Inner Join: t3.a = t4.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: t1.a = t3.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "      Inner Join: t1.a = t2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
            "      TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "    TableScan: t4 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_join_with_expr_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join since filter has Join predicates
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                (col("t1.a") + lit(100u32)).eq(col("t2.a") * lit(2u32)),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;

        let expected = vec![
              "Filter: t2.c < UInt32(20) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
              "  Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
              "    Inner Join: t1.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, t1.a + UInt32(100):UInt32, a:UInt32, b:UInt32, c:UInt32, t2.a * UInt32(2):UInt32]",
              "      Projection: t1.a, t1.b, t1.c, t1.a + UInt32(100) [a:UInt32, b:UInt32, c:UInt32, t1.a + UInt32(100):UInt32]",
              "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
              "      Projection: t2.a, t2.b, t2.c, t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, t2.a * UInt32(2):UInt32]",
              "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_expr_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join since filter OR expression and there is no common
        // Join predicates in left and right of OR expr.
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                (col("t1.a") + lit(100u32)).eq(col("t2.a") * lit(2u32)),
                Or,
                col("t2.b").eq(col("t1.a")),
            ))?
            .build()?;

        let expected = vec![
              "Filter: t1.a + UInt32(100) = t2.a * UInt32(2) OR t2.b = t1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
              "  CrossJoin: [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
              "    TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
              "    TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_common_expr_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join
        let common_join_key = (col("t1.a") + lit(100u32)).eq(col("t2.a") * lit(2u32));
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(common_join_key.clone(), And, col("t2.c").lt(lit(20u32))),
                And,
                binary_expr(common_join_key, And, col("t2.c").eq(lit(10u32))),
            ))?
            .build()?;

        let expected = vec![
               "Filter: t2.c < UInt32(20) AND t2.c = UInt32(10) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
               "  Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
               "    Inner Join: t1.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, t1.a + UInt32(100):UInt32, a:UInt32, b:UInt32, c:UInt32, t2.a * UInt32(2):UInt32]",
               "      Projection: t1.a, t1.b, t1.c, t1.a + UInt32(100) [a:UInt32, b:UInt32, c:UInt32, t1.a + UInt32(100):UInt32]",
               "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
               "      Projection: t2.a, t2.b, t2.c, t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, t2.a * UInt32(2):UInt32]",
               "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn eliminate_cross_with_common_expr_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could eliminate to inner join since Or predicates have common Join predicates
        let common_join_key = (col("t1.a") + lit(100u32)).eq(col("t2.a") * lit(2u32));
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .filter(binary_expr(
                binary_expr(common_join_key.clone(), And, col("t2.c").lt(lit(15u32))),
                Or,
                binary_expr(common_join_key, And, col("t2.c").eq(lit(688u32))),
            ))?
            .build()?;

        let expected = vec![
               "Filter: t2.c < UInt32(15) OR t2.c = UInt32(688) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
               "  Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
               "    Inner Join: t1.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, t1.a + UInt32(100):UInt32, a:UInt32, b:UInt32, c:UInt32, t2.a * UInt32(2):UInt32]",
               "      Projection: t1.a, t1.b, t1.c, t1.a + UInt32(100) [a:UInt32, b:UInt32, c:UInt32, t1.a + UInt32(100):UInt32]",
               "        TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
               "      Projection: t2.a, t2.b, t2.c, t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, t2.a * UInt32(2):UInt32]",
               "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
       ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reorder_join_with_expr_key_multi_tables() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let t3 = test_table_scan_with_name("t3")?;

        // could eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(&t2)?
            .cross_join(&t3)?
            .filter(binary_expr(
                binary_expr(
                    (col("t3.a") + lit(100u32)).eq(col("t1.a") * lit(2u32)),
                    And,
                    col("t3.c").lt(lit(15u32)),
                ),
                And,
                binary_expr(
                    (col("t3.a") + lit(100u32)).eq(col("t2.a") * lit(2u32)),
                    And,
                    col("t3.b").lt(lit(15u32)),
                ),
            ))?
            .build()?;

        let expected = vec![
            "Filter: t3.c < UInt32(15) AND t3.b < UInt32(15) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "  Projection: t1.a, t1.b, t1.c, t2.a, t2.b, t2.c, t3.a, t3.b, t3.c [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]",
            "    Inner Join: t3.a + UInt32(100) = t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, t3.a + UInt32(100):UInt32, a:UInt32, b:UInt32, c:UInt32, t2.a * UInt32(2):UInt32]",
            "      Projection: t1.a, t1.b, t1.c, t3.a, t3.b, t3.c, t3.a + UInt32(100) [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32, t3.a + UInt32(100):UInt32]",
            "        Inner Join: t1.a * UInt32(2) = t3.a + UInt32(100) [a:UInt32, b:UInt32, c:UInt32, t1.a * UInt32(2):UInt32, a:UInt32, b:UInt32, c:UInt32, t3.a + UInt32(100):UInt32]",
            "          Projection: t1.a, t1.b, t1.c, t1.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, t1.a * UInt32(2):UInt32]",
            "            TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]",
            "          Projection: t3.a, t3.b, t3.c, t3.a + UInt32(100) [a:UInt32, b:UInt32, c:UInt32, t3.a + UInt32(100):UInt32]",
            "            TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]",
            "      Projection: t2.a, t2.b, t2.c, t2.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32, t2.a * UInt32(2):UInt32]",
            "        TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]",
        ];

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
