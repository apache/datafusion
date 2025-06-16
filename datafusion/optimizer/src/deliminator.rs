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

use crate::decorrelate_dependent_join::DecorrelateDependentJoin;
use crate::delim_candidates_collector::DelimCandidateVisitor;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{internal_err, Column, DataFusionError, Result};
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{Expr, Join, JoinType, LogicalPlan, Operator};

/// The Deliminator optimizer traverses the logical operator tree and removes any
/// redundant DelimScan/DelimJoins.
#[derive(Debug)]
pub struct Deliminator {}

impl Deliminator {
    pub fn new() -> Self {
        return Deliminator {};
    }
}

impl OptimizerRule for Deliminator {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let transformer = DecorrelateDependentJoin::new();
        let rewrite_result = transformer.rewrite(plan, config)?;

        let mut visitor = DelimCandidateVisitor::new();
        let _ = rewrite_result.data.visit(&mut visitor)?;
        for candidate in &visitor.candidates {
            println!("=== DelimCandidate ===");
            println!("  plan: {}", candidate.node.plan.display());
            println!("  delim_get_count: {}", candidate.delim_scan_count);
            println!("  joins: [");
            for join in &candidate.joins {
                println!("    JoinWithDelimGet {{");
                println!("      depth: {}", join.depth);
                println!("      join: {}", join.node.plan.display());
                println!("    }},");
            }
            println!("  ]");
            println!("==================\n");
        }

        if visitor.candidates.is_empty() {
            return Ok(rewrite_result);
        }

        for candidate in visitor.candidates.iter_mut() {
            let delim_join = &mut candidate.node.plan;

            // Sort these so the deepest are first.
            candidate.joins.sort_by(|a, b| b.depth.cmp(&a.depth));

            let mut all_removed = true;
            if !candidate.joins.is_empty() {
                let mut has_selection = false;
                delim_join.apply(|plan| {
                    match plan {
                        LogicalPlan::TableScan(table_scan) => {
                            for expr in &table_scan.filters {
                                if matches!(expr, Expr::IsNotNull(_)) {
                                    has_selection = true;
                                    return Ok(TreeNodeRecursion::Stop);
                                }
                            }
                        }
                        LogicalPlan::Filter(_) => {
                            has_selection = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                        _ => {}
                    }

                    Ok(TreeNodeRecursion::Continue)
                })?;

                if has_selection {
                    // Keep the deepest join with DelimScan in these cases,
                    // as the selection can greatly reduce the cost of the RHS child of the
                    // DelimJoin.
                    candidate.joins.remove(0);
                    all_removed = false;
                }

                let delim_join = if let LogicalPlan::Join(join) = delim_join {
                    join
                } else {
                    return internal_err!("unreachable");
                };

                let mut all_equality_conditions = true;
                for join in &candidate.joins {
                    all_removed = remove_join_with_delim_scan(
                        delim_join,
                        candidate.delim_scan_count,
                        &join.node.plan,
                        &mut all_equality_conditions,
                    )?;
                    // TODO remove join with delim scan.
                }

                // Change type if there are no more duplicate-eliminated columns.
                if candidate.joins.len() == candidate.delim_scan_count && all_removed {
                    // TODO: how we can change it.
                    // delim_join.join_kind = JoinKind::ComparisonJoin;
                }

                // Only DelimJoins are ever created as SINGLE joins, and we can switch from SINGLE
                // to LEFT if the RHS is de-deuplicated by an aggr.
                // TODO: add single join support.
            }
        }

        Ok(rewrite_result)
    }

    fn name(&self) -> &str {
        "deliminator"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

fn remove_join_with_delim_scan(
    delim_join: &mut Join,
    delim_scan_count: usize,
    join_plan: &LogicalPlan,
    all_equality_conditions: &mut bool,
) -> Result<bool> {
    if let LogicalPlan::Join(join) = join_plan {
        if !child_join_type_can_be_deliminated(join.join_type) {
            return Ok(false);
        }

        // Fetch filter (if any) and delim scan.
        let mut is_delim_side_left = true;
        let mut plan_pair = fetch_delim_scan(join.left.as_ref());
        if plan_pair.1.is_none() {
            is_delim_side_left = false;
            plan_pair = fetch_delim_scan(join.right.as_ref());
        }

        // Collect filter exprs.
        let mut filter_expressions = vec![];
        if let Some(plan) = plan_pair.0 {
            if let LogicalPlan::Filter(filter) = plan {
                for expr in split_conjunction(&filter.predicate) {
                    filter_expressions.push(expr.clone());
                }
            }
        }

        let delim_scan = plan_pair
            .1
            .ok_or_else(|| DataFusionError::Plan("No delim scan found".to_string()))?;
        let delim_scan = if let LogicalPlan::DelimGet(delim_scan) = delim_scan {
            delim_scan
        } else {
            return internal_err!("unreachable");
        };

        // Check if joining with the DelimScan is redundant, and collect relevant column
        // information.
        let mut replacement_cols = vec![];
        if let Some(filter) = &join.filter {
            let conditions = split_conjunction(filter);

            if conditions.len() != delim_scan.delim_types.len() {
                // Joining with delim scan adds new information.
                return Ok(false);
            }

            for condition in conditions {
                if let Expr::BinaryExpr(binary_expr) = condition {
                    *all_equality_conditions &= is_equality_join_condition(&condition);

                    if !matches!(*binary_expr.left, Expr::Column(_))
                        || !matches!(*binary_expr.right, Expr::Column(_))
                    {
                        return Ok(false);
                    }

                    let (left_col, right_col) =
                        if let (Expr::Column(left), Expr::Column(right)) =
                            (&*binary_expr.left, &*binary_expr.right)
                        {
                            (left.clone(), right.clone())
                        } else {
                            return internal_err!("unreachable");
                        };

                    if is_delim_side_left {
                        replacement_cols.push((left_col, right_col));
                    } else {
                        replacement_cols.push((right_col, left_col));
                    }

                    if !matches!(binary_expr.op, Operator::IsNotDistinctFrom) {
                        let is_not_null_expr = if is_delim_side_left {
                            binary_expr.right.clone().is_not_null()
                        } else {
                            binary_expr.left.clone().is_not_null()
                        };
                        filter_expressions.push(is_not_null_expr);
                    }
                }
            }

            if !*all_equality_conditions
                && !remove_inequality_join_with_delim_scan(
                    delim_join,
                    delim_scan_count,
                    join_plan,
                )?
            {
                return Ok(false);
            }

            // All conditions passed, we can eliminate this join + DelimScan
            return Ok(true);
        }

        // No join conditions, can't remove the join
        return Ok(false);
    } else {
        return internal_err!("current plan must be join in remove_join_with_delim_scan");
    }
}

fn is_equality_join_condition(expr: &Expr) -> bool {
    if let Expr::BinaryExpr(binary_expr) = expr {
        if matches!(binary_expr.op, Operator::IsNotDistinctFrom)
            || matches!(binary_expr.op, Operator::Eq)
        {
            return true;
        }
    }

    false
}

fn child_join_type_can_be_deliminated(join_type: JoinType) -> bool {
    match join_type {
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => true,
        _ => false,
    }
}

// fetch filter (if any) and delim scan
fn fetch_delim_scan(plan: &LogicalPlan) -> (Option<&LogicalPlan>, Option<&LogicalPlan>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            if let LogicalPlan::SubqueryAlias(alias) = filter.input.as_ref() {
                if let LogicalPlan::DelimGet(_) = alias.input.as_ref() {
                    return (Some(plan), Some(alias.input.as_ref()));
                };
            };
        }
        LogicalPlan::SubqueryAlias(alias) => {
            if let LogicalPlan::DelimGet(_) = alias.input.as_ref() {
                return (None, Some(alias.input.as_ref()));
            }
        }
        _ => {}
    }

    (None, None)
}

fn is_delim_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(filter) => {
            if let LogicalPlan::SubqueryAlias(alias) = filter.input.as_ref() {
                if let LogicalPlan::DelimGet(_) = alias.input.as_ref() {
                    return true;
                };
            };
        }
        LogicalPlan::SubqueryAlias(alias) => {
            if let LogicalPlan::DelimGet(_) = alias.input.as_ref() {
                return true;
            }
        }
        _ => return false,
    }

    false
}

fn remove_inequality_join_with_delim_scan(
    delim_join: &mut Join,
    delim_scan_count: usize,
    join_plan: &LogicalPlan,
) -> Result<bool> {
    if let LogicalPlan::Join(join) = join_plan {
        if delim_scan_count != 1
            || !inequality_delim_join_can_be_eliminated(&join.join_type)
        {
            return Ok(false);
        }

        let mut delim_conditions: Vec<Expr> = if let Some(filter) = &mut delim_join.filter
        {
            split_conjunction(filter).into_iter().cloned().collect()
        } else {
            return Ok(false);
        };
        let join_conditions = if let Some(filter) = &join.filter {
            split_conjunction(filter)
        } else {
            return Ok(false);
        };
        if delim_conditions.len() != join_conditions.len() {
            return Ok(false);
        }

        // TODO add single join support
        if delim_join.join_type == JoinType::LeftMark {
            let mut has_one_equality = false;
            for condition in &join_conditions {
                has_one_equality |= is_equality_join_condition(condition);
            }

            if !has_one_equality {
                return Ok(false);
            }
        }

        // We only support colref
        let mut traced_cols = vec![];
        for condition in &delim_conditions {
            if let Expr::BinaryExpr(binary_expr) = condition {
                if let Expr::Column(column) = &*binary_expr.right {
                    traced_cols.push(column.clone());
                } else {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }

        // Now we trace down the column to join (for now, we only trace it through a few
        // operators).
        let mut cur_op = delim_join.right.as_ref();
        while *cur_op != *join_plan {
            if cur_op.inputs().len() != 1 {
                return Ok(false);
            }

            match cur_op {
                LogicalPlan::Projection(_) => find_and_replace_cols(
                    &mut traced_cols,
                    &cur_op.expressions(),
                    &cur_op.schema().columns(),
                )?,
                LogicalPlan::Filter(_) => {
                    // Doesn't change bindings.
                    break;
                }
                _ => return Ok(false),
            };

            cur_op = *cur_op.inputs().get(0).ok_or_else(|| {
                DataFusionError::Plan("current plan has no child".to_string())
            })?;
        }

        let is_left_delim_scan = is_delim_scan(join.right.as_ref());

        let mut found_all = true;
        for (idx, delim_condition) in delim_conditions.iter_mut().enumerate() {
            let traced_col = traced_cols.get(idx).ok_or_else(|| {
                DataFusionError::Plan("get col under traced cols".to_string())
            })?;

            let delim_comparison =
                if let Expr::BinaryExpr(ref mut binary_expr) = delim_condition {
                    &mut binary_expr.op
                } else {
                    return internal_err!("expr must be binary");
                };

            let mut found = false;
            for join_condition in &join_conditions {
                if let Expr::BinaryExpr(binary_expr) = join_condition {
                    let delim_side = if is_left_delim_scan {
                        &*binary_expr.left
                    } else {
                        &*binary_expr.right
                    };

                    if let Expr::Column(column) = delim_side {
                        if *column == *traced_col {
                            let mut join_comparison = binary_expr.op;

                            if matches!(delim_comparison, Operator::IsDistinctFrom)
                                || matches!(delim_comparison, Operator::IsNotDistinctFrom)
                            {
                                // We need to compare Null values.
                                if matches!(join_comparison, Operator::Eq) {
                                    join_comparison = Operator::IsNotDistinctFrom;
                                } else if matches!(join_comparison, Operator::NotEq) {
                                    join_comparison = Operator::IsDistinctFrom;
                                } else if !matches!(
                                    join_comparison,
                                    Operator::IsDistinctFrom
                                ) && !matches!(
                                    join_comparison,
                                    Operator::IsNotDistinctFrom,
                                ) {
                                    // The optimization does not work here
                                    found = false;
                                    break;
                                }

                                // TODO how to change delim condition's comparison
                                *delim_comparison =
                                    flip_comparison_operator(join_comparison)?;

                                // Join condition was a not equal and filtered out all NULLs.
                                // Delim join need to do that for not delim scan side. Easiest way
                                // is to change the comparison expression type.
                                if delim_join.join_type != JoinType::LeftMark {
                                    if *delim_comparison == Operator::IsDistinctFrom {
                                        *delim_comparison = Operator::NotEq;
                                    }
                                    if *delim_comparison == Operator::IsNotDistinctFrom {
                                        *delim_comparison = Operator::Eq;
                                    }
                                }

                                found = true;
                                break;
                            }
                        }
                    } else {
                        return internal_err!("expr must be column");
                    }
                } else {
                    return internal_err!("expr must be binary");
                }
            }
            found_all &= found;
        }

        // Construct a new filter for delim join.
        if found_all {
            // If we found all conditions, combine them into a new filter.
            if !delim_conditions.is_empty() {
                let new_filter = conjunction(delim_conditions);
                delim_join.filter = new_filter;
            } else {
                delim_join.filter = None;
            }
        }

        Ok(found_all)
    } else {
        internal_err!(
            "current plan must be join in remove_inequality_join_with_delim_scan"
        )
    }
}

fn inequality_delim_join_can_be_eliminated(join_type: &JoinType) -> bool {
    // TODO add single join support
    *join_type == JoinType::LeftAnti
        || *join_type == JoinType::RightAnti
        || *join_type == JoinType::LeftSemi
        || *join_type == JoinType::RightSemi
}

fn find_and_replace_cols(
    traced_cols: &mut Vec<Column>,
    exprs: &Vec<Expr>,
    cur_cols: &Vec<Column>,
) -> Result<bool> {
    for col in traced_cols {
        let mut cur_idx = 0;
        for (idx, _) in exprs.iter().enumerate() {
            cur_idx = idx;
            if *col
                == *cur_cols.get(idx).ok_or_else(|| {
                    DataFusionError::Plan("no column at idx".to_string())
                })?
            {
                break;
            }
        }

        if cur_idx == exprs.len() {
            return Ok(false);
        }

        if let Expr::Column(column) = exprs
            .get(cur_idx)
            .ok_or_else(|| DataFusionError::Plan("no expr at cur_idx".to_string()))?
        {
            *col = column.clone();
        } else {
            return Ok(false);
        }
    }

    return Ok(true);
}

fn flip_comparison_operator(operator: Operator) -> Result<Operator> {
    match operator {
        Operator::Eq
        | Operator::NotEq
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => Ok(operator),
        Operator::Lt => Ok(Operator::Gt),
        Operator::LtEq => Ok(Operator::GtEq),
        Operator::Gt => Ok(Operator::Lt),
        Operator::GtEq => Ok(Operator::LtEq),
        _ => internal_err!("unsupported comparison type in flip"),
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use crate::deliminator::Deliminator;
    use crate::test::test_table_scan_with_name;
    use arrow::datatypes::DataType as ArrowDataType;
    use datafusion_common::Result;
    use datafusion_expr::{
        expr_fn::col, in_subquery, lit, out_ref_col, scalar_subquery, Expr,
        LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::count::count;
    use std::sync::Arc;

    macro_rules! assert_deliminator{
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(Deliminator::new());
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )?;
        }};
    }

    #[test]
    fn decorrelated_two_nested_subqueries() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let scalar_sq_level2 =
            Arc::new(
                LogicalPlanBuilder::from(inner_table_lv2)
                    .filter(
                        col("inner_table_lv2.a")
                            .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                            .and(col("inner_table_lv2.b").eq(out_ref_col(
                                ArrowDataType::UInt32,
                                "inner_table_lv1.b",
                            ))),
                    )?
                    .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
                    .build()?,
            );
        let scalar_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.c"))
                        .and(scalar_subquery(scalar_sq_level2).eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(scalar_subquery(scalar_sq_level1).eq(col("outer_table.a"))),
            )?
            .build()?;

        // Projection: outer_table.a, outer_table.b, outer_table.c
        //   Filter: outer_table.a > Int32(1) AND __scalar_sq_2.output = outer_table.a
        //     DependentJoin on [outer_table.a lvl 2, outer_table.c lvl 1] with expr (<subquery>) depth 1
        //       TableScan: outer_table
        //       Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //         Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c
        //           Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND __scalar_sq_1.output = Int32(1)
        //             DependentJoin on [inner_table_lv1.b lvl 2] with expr (<subquery>) depth 2
        //               TableScan: inner_table_lv1
        //               Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]]
        //                 Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b)
        //                   TableScan: inner_table_lv2
        assert_deliminator!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __scalar_sq_2.output = outer_table.a [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_2.output:Int32;N]
            Projection: outer_table.a, outer_table.b, outer_table.c, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END AS __scalar_sq_2.output [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_2.output:Int32;N]
              Left Join(ComparisonJoin):  Filter: outer_table.a IS NOT DISTINCT FROM delim_scan_4.outer_table_a AND outer_table.c IS NOT DISTINCT FROM delim_scan_4.outer_table_c [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a [CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                  Inner Join(DelimJoin):  Filter: delim_scan_4.outer_table_a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND delim_scan_4.outer_table_c IS NOT DISTINCT FROM delim_scan_1.outer_table_c [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                    Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                      Aggregate: groupBy=[[delim_scan_4.outer_table_a, delim_scan_4.outer_table_c]], aggr=[[count(inner_table_lv1.a)]] [outer_table_a:UInt32;N, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64]
                        Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, delim_scan_4.outer_table_a, delim_scan_4.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                          Filter: inner_table_lv1.c = delim_scan_4.outer_table_c AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1.output:Int32;N]
                            Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, delim_scan_2.outer_table_a, delim_scan_2.outer_table_c, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END AS __scalar_sq_1.output [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1.output:Int32;N]
                              Left Join(ComparisonJoin):  Filter: inner_table_lv1.b IS NOT DISTINCT FROM delim_scan_4.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                  TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                                  SubqueryAlias: delim_scan_2 [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                    DelimGet: outer_table.a, outer_table.c [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b [CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                  Inner Join(DelimJoin):  Filter: delim_scan_4.inner_table_lv1_b IS NOT DISTINCT FROM delim_scan_3.inner_table_lv1_b AND delim_scan_4.outer_table_a IS NOT DISTINCT FROM delim_scan_3.outer_table_a AND delim_scan_4.outer_table_c IS NOT DISTINCT FROM delim_scan_3.outer_table_c [count(inner_table_lv2.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                    Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b [count(inner_table_lv2.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                      Aggregate: groupBy=[[delim_scan_4.inner_table_lv1_b, delim_scan_4.outer_table_a, delim_scan_4.outer_table_c]], aggr=[[count(inner_table_lv2.a)]] [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N, count(inner_table_lv2.a):Int64]
                                        Filter: inner_table_lv2.a = delim_scan_4.outer_table_a AND inner_table_lv2.b = delim_scan_4.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                            TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
                                            SubqueryAlias: delim_scan_4 [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                              DelimGet: inner_table_lv1.b, outer_table.a, outer_table.c [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                    SubqueryAlias: delim_scan_3 [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                      DelimGet: inner_table_lv1.b, outer_table.a, outer_table.c [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                    SubqueryAlias: delim_scan_1 [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                      DelimGet: outer_table.a, outer_table.c [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
        ");

        Ok(())
    }

    #[test]
    fn decorrelate_join_in_subquery_with_count_depth_1() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.b")
                                .eq(col("inner_table_lv1.b")),
                        ),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        // Projection: outer_table.a, outer_table.b, outer_table.c
        //   Filter: outer_table.a > Int32(1) AND __in_sq_1.output
        //     DependentJoin on [outer_table.a lvl 1, outer_table.b lvl 1] with expr outer_table.c IN (<subquery>) depth 1
        //       TableScan: outer_table
        //       Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //         Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b
        //           TableScan: inner_table_lv1

        assert_deliminator!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, delim_scan_2.mark AS __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END AND outer_table.a IS NOT DISTINCT FROM delim_scan_2.outer_table_a AND outer_table.b IS NOT DISTINCT FROM delim_scan_2.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_2.outer_table_b, delim_scan_2.outer_table_a [CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                  Inner Join(DelimJoin):  Filter: delim_scan_2.outer_table_a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND delim_scan_2.outer_table_b IS NOT DISTINCT FROM delim_scan_1.outer_table_b [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_2.outer_table_b, delim_scan_2.outer_table_a [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                      Aggregate: groupBy=[[delim_scan_2.outer_table_a, delim_scan_2.outer_table_b]], aggr=[[count(inner_table_lv1.a)]] [outer_table_a:UInt32;N, outer_table_b:UInt32;N, count(inner_table_lv1.a):Int64]
                        Filter: inner_table_lv1.a = delim_scan_2.outer_table_a AND delim_scan_2.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND delim_scan_2.outer_table_b = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                            TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                            SubqueryAlias: delim_scan_2 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                              DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    SubqueryAlias: delim_scan_1 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                      DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
        ");
        Ok(())
    }
}
