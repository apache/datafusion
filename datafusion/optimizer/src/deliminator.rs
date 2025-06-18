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
use crate::delim_candidate_rewriter::DelimCandidateRewriter;
use crate::delim_candidates_collector::{
    DelimCandidateVisitor, JoinWithDelimScan, NodeVisitor,
};
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{internal_err, Column, DataFusionError, Result};
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    Expr, Filter, Join, JoinKind, JoinType, LogicalPlan, Operator, Projection,
};
use indexmap::IndexMap;

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

        let mut node_visitor = NodeVisitor::new();
        let _ = node_visitor.collect_nodes(&rewrite_result.data)?;
        let mut candidate_visitor = DelimCandidateVisitor::new(node_visitor);
        let _ = rewrite_result.data.visit(&mut candidate_visitor)?;
        for (_, candidate) in candidate_visitor.candidates.iter() {
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

        if candidate_visitor.candidates.is_empty() {
            return Ok(rewrite_result);
        }

        let mut replacement_cols: Vec<(Column, Column)> = vec![];
        for (_, candidate) in candidate_visitor.candidates.iter_mut() {
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
                                if !matches!(expr, Expr::IsNotNull(_)) {
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
                let mut is_transformed = false;
                for join in &mut candidate.joins {
                    all_removed = remove_join_with_delim_scan(
                        delim_join,
                        candidate.delim_scan_count,
                        join,
                        &mut all_equality_conditions,
                        &mut is_transformed,
                        &mut replacement_cols,
                    )?;
                }

                // Change type if there are no more duplicate-eliminated columns.
                if candidate.joins.len() == candidate.delim_scan_count && all_removed {
                    is_transformed |= true;
                    delim_join.join_kind = JoinKind::ComparisonJoin;
                    // TODO: clear duplicate eliminated columns if any, or should it have?
                }

                // Only DelimJoins are ever created as SINGLE joins, and we can switch from SINGLE
                // to LEFT if the RHS is de-deuplicated by an aggr.
                // TODO: add single join support and try switch single to left.

                candidate.is_transformed = is_transformed;
            }
        }

        // Replace all with candidate.
        let mut joins = IndexMap::new();
        for candidate in candidate_visitor.candidates.values() {
            for join in &candidate.joins {
                joins.insert(join.node.id, join.clone());
            }
        }
        let mut rewriter =
            DelimCandidateRewriter::new(candidate_visitor.candidates, joins);
        let rewrite_result = rewrite_result.data.rewrite(&mut rewriter)?;

        // Replace all columns.
        let mut rewriter = ColumnRewriter::new(replacement_cols);
        let rewrite_result = rewrite_result.data.rewrite(&mut rewriter)?;

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
    join_with_delim_scan: &mut JoinWithDelimScan,
    all_equality_conditions: &mut bool,
    is_transformed: &mut bool,
    replacement_cols: &mut Vec<(Column, Column)>,
) -> Result<bool> {
    let join_plan = &join_with_delim_scan.node.plan;
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
                    is_transformed,
                )?
            {
                return Ok(false);
            }

            // All conditions passed, we can eliminate this join + DelimScan
            join_with_delim_scan.can_be_eliminated = true;
            let mut replacement_plan = if is_delim_side_left {
                join.right.clone()
            } else {
                join.left.clone()
            };
            if !filter_expressions.is_empty() {
                replacement_plan = LogicalPlan::Filter(Filter::try_new(
                    conjunction(filter_expressions).ok_or_else(|| {
                        DataFusionError::Plan("filter expressions must exist".to_string())
                    })?,
                    replacement_plan,
                )?)
                .into();
                join_with_delim_scan.is_filter_generated = true;
            }
            join_with_delim_scan.replacement_plan = Some(replacement_plan);

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
    is_transformed: &mut bool,
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

            *is_transformed = true;
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

struct ColumnRewriter {
    // <old_col, new_col>
    replacement_cols: Vec<(Column, Column)>,
}

impl ColumnRewriter {
    fn new(replacement_cols: Vec<(Column, Column)>) -> Self {
        Self { replacement_cols }
    }
}

impl TreeNodeRewriter for ColumnRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(plan))
    }

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        // Helper closure to rewrite expressions
        let rewrite_expr = |expr: Expr| -> Result<Transformed<Expr>> {
            let mut transformed = false;
            let new_expr = expr.clone().transform_down(|expr| {
                Ok(match expr {
                    Expr::Column(col) => {
                        if let Some((_, new_col)) = self
                            .replacement_cols
                            .iter()
                            .find(|(old_col, _)| old_col == &col)
                        {
                            transformed = true;
                            Transformed::yes(Expr::Column(new_col.clone()))
                        } else {
                            Transformed::no(Expr::Column(col))
                        }
                    }
                    _ => Transformed::no(expr),
                })
            })?;

            Ok(if transformed {
                Transformed::yes(new_expr.data)
            } else {
                Transformed::no(expr)
            })
        };

        // Rewrite expressions in the plan
        // Apply the rewrite to all expressions in the plan node
        match plan {
            LogicalPlan::Filter(filter) => {
                let new_predicate = rewrite_expr(filter.predicate.clone())?;
                Ok(if new_predicate.transformed {
                    Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                        new_predicate.data,
                        filter.input,
                    )?))
                } else {
                    Transformed::no(LogicalPlan::Filter(filter))
                })
            }
            LogicalPlan::Projection(projection) => {
                let mut transformed = false;
                let new_exprs: Vec<Expr> = projection
                    .expr
                    .clone()
                    .into_iter()
                    .map(|expr| {
                        let res = rewrite_expr(expr)?;
                        transformed |= res.transformed;
                        Ok(res.data)
                    })
                    .collect::<Result<_>>()?;

                Ok(if transformed {
                    Transformed::yes(LogicalPlan::Projection(Projection::try_new(
                        new_exprs,
                        projection.input,
                    )?))
                } else {
                    Transformed::no(LogicalPlan::Projection(projection))
                })
            }
            // Add other cases as needed...
            _ => Ok(Transformed::no(plan)),
        }
    }
}
