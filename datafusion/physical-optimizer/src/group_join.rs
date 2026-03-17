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

//! GroupJoin optimizer rule.
//!
//! Implements the GroupJoin optimization from "Accelerating Queries with
//! Group-By and Join by Groupjoin" (Moerkotte & Neumann, VLDB 2011).

use std::collections::HashMap;
use std::sync::Arc;

use crate::PhysicalOptimizerRule;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::joins::group_join::{GroupBySide, GroupJoinExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};

/// Physical optimizer rule that fuses `AggregateExec` on top of `HashJoinExec`
/// into a single `GroupJoinExec` when the preconditions are met.
///
/// Matches patterns:
/// - `AggregateExec → HashJoinExec`
/// - `AggregateExec → ProjectionExec → HashJoinExec`
#[derive(Debug, Default)]
pub struct GroupJoinOptimization {}

impl GroupJoinOptimization {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for GroupJoinOptimization {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan: Arc<dyn ExecutionPlan>| {
            let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(plan));
            };

            // Try matching AggregateExec → HashJoinExec directly
            if let Some(hash_join) =
                agg_exec.input().as_any().downcast_ref::<HashJoinExec>()
                && let Some(group_join) =
                    try_create_group_join(agg_exec, hash_join, None)?
            {
                return Ok(Transformed::yes(Arc::new(group_join)));
            }

            // Try matching AggregateExec → ProjectionExec → HashJoinExec
            if let Some(proj_exec) =
                agg_exec.input().as_any().downcast_ref::<ProjectionExec>()
                && let Some(hash_join) =
                    proj_exec.input().as_any().downcast_ref::<HashJoinExec>()
                && let Some(group_join) =
                    try_create_group_join(agg_exec, hash_join, Some(proj_exec))?
            {
                return Ok(Transformed::yes(Arc::new(group_join)));
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &str {
        "GroupJoinOptimization"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Builds a column index resolver.
///
/// When there is no projection (or a direct HashJoinExec with its own projection):
/// - Resolves aggregate column indices to join schema indices.
///
/// When there is a ProjectionExec between aggregate and join:
/// - First maps through the ProjectionExec's expressions, then through the
///   HashJoinExec's own projection (if any).
fn build_resolver(
    hash_join: &HashJoinExec,
    proj_exec: Option<&ProjectionExec>,
) -> Box<dyn Fn(usize) -> Option<usize>> {
    match proj_exec {
        Some(proj) => {
            // ProjectionExec maps aggregate columns to join output columns.
            // Each projection expression should be a simple Column reference.
            let proj_mapping: Vec<Option<usize>> = proj
                .expr()
                .iter()
                .map(|pe| {
                    pe.expr
                        .as_any()
                        .downcast_ref::<Column>()
                        .map(|col| col.index())
                })
                .collect();

            // The HashJoinExec may also have its own projection
            let join_proj = hash_join.projection.as_deref().map(|p| p.to_vec());

            Box::new(move |agg_col_idx: usize| {
                // Step 1: map through the ProjectionExec
                let join_output_idx = proj_mapping.get(agg_col_idx)?.as_ref().copied()?;

                // Step 2: if the HashJoinExec has a projection, map through it
                match &join_proj {
                    Some(jp) => jp.get(join_output_idx).copied(),
                    None => Some(join_output_idx),
                }
            })
        }
        None => {
            // No ProjectionExec — map directly through HashJoinExec's projection
            let join_proj = hash_join.projection.as_deref().map(|p| p.to_vec());

            Box::new(move |agg_col_idx: usize| match &join_proj {
                Some(jp) => jp.get(agg_col_idx).copied(),
                None => Some(agg_col_idx),
            })
        }
    }
}

/// Attempts to create a `GroupJoinExec` from an `AggregateExec` on top of a `HashJoinExec`,
/// optionally with a `ProjectionExec` in between.
fn try_create_group_join(
    agg_exec: &AggregateExec,
    hash_join: &HashJoinExec,
    proj_exec: Option<&ProjectionExec>,
) -> Result<Option<GroupJoinExec>> {
    // Only support INNER joins
    if *hash_join.join_type() != JoinType::Inner {
        return Ok(None);
    }

    // No join filter
    if hash_join.filter().is_some() {
        return Ok(None);
    }

    let group_by = agg_exec.group_expr();
    if group_by.has_grouping_set() {
        return Ok(None);
    }

    // Empty GROUP BY (global aggregate like COUNT(*)) is not supported:
    // GroupValues::intern cannot determine row count from zero columns.
    if group_by.expr().is_empty() {
        return Ok(None);
    }

    if agg_exec.filter_expr().iter().any(|f| f.is_some()) {
        return Ok(None);
    }

    if agg_exec.aggr_expr().iter().any(|e| e.is_distinct()) {
        return Ok(None);
    }

    if agg_exec
        .aggr_expr()
        .iter()
        .any(|e| !e.order_bys().is_empty())
    {
        return Ok(None);
    }

    // The join output schema (before any projection) is [left_cols..., right_cols...]
    let left_field_count = hash_join.left().schema().fields().len();

    let resolve_index = build_resolver(hash_join, proj_exec);

    // Build a substitution map: right-side join-key (raw join schema index) → left-side key index.
    // When a GROUP BY column references a right join key `right_j`, and there is an ON condition
    // `left_i = right_j`, we can substitute it with `left_i` (they are equal by the join predicate).
    // This is Section 3.2 of Moerkotte & Neumann VLDB 2011.
    let key_subst: HashMap<usize, usize> = hash_join
        .on()
        .iter()
        .filter_map(|(l_expr, r_expr)| {
            let l_col = l_expr.as_any().downcast_ref::<Column>()?;
            let r_col = r_expr.as_any().downcast_ref::<Column>()?;
            Some((left_field_count + r_col.index(), l_col.index()))
        })
        .collect();

    // Resolver for group-by: after resolving through projections, substitute any right join key
    // with its equivalent left join key.
    let resolve_gby = |idx: usize| -> Option<usize> {
        let resolved = resolve_index(idx)?;
        Some(key_subst.get(&resolved).copied().unwrap_or(resolved))
    };

    // Classify each group-by column as left-side or right-side.
    // Left-side: resolves (with key substitution) to left schema.
    // Right-side: resolves to right schema (any right column, not just join keys).
    let mut group_by_order: Vec<GroupBySide> = Vec::new();
    for (expr, _alias) in group_by.expr() {
        if expr_columns_in_side(expr.as_ref(), &resolve_gby, 0, left_field_count) {
            group_by_order.push(GroupBySide::Left);
        } else if expr_columns_in_side(
            expr.as_ref(),
            &*resolve_index,
            left_field_count,
            usize::MAX,
        ) {
            group_by_order.push(GroupBySide::Right);
        } else {
            // Expression references neither side cleanly (e.g. spans both)
            return Ok(None);
        }
    }

    // Classify each aggregate's argument expressions: all must resolve to a single side.
    let mut aggr_arg_sides: Vec<GroupBySide> = Vec::new();
    for agg_expr in agg_exec.aggr_expr() {
        let args = agg_expr.expressions();
        if args.is_empty() {
            // COUNT(*) style — treat as right side (no actual args to evaluate)
            aggr_arg_sides.push(GroupBySide::Right);
            continue;
        }
        let all_right = args.iter().all(|arg| {
            expr_columns_in_side(arg.as_ref(), &*resolve_index, left_field_count, usize::MAX)
        });
        let all_left = args.iter().all(|arg| {
            expr_columns_in_side(arg.as_ref(), &*resolve_index, 0, left_field_count)
        });
        if all_right {
            aggr_arg_sides.push(GroupBySide::Right);
        } else if all_left {
            aggr_arg_sides.push(GroupBySide::Left);
        } else {
            // Args span both sides — cannot fuse
            return Ok(None);
        }
    }

    // Remap group-by into left and right lists, preserving the group_by_order mapping
    let mut left_gby: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let mut right_gby: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    for ((expr, alias), &side) in group_by.expr().iter().zip(group_by_order.iter()) {
        match side {
            GroupBySide::Left => {
                left_gby.push((remap_columns(expr, &resolve_gby, 0), alias.clone()));
            }
            GroupBySide::Right => {
                right_gby
                    .push((remap_columns(expr, &*resolve_index, left_field_count), alias.clone()));
            }
        }
    }

    // Remap aggregate arguments to reference the appropriate input schema
    let remapped_aggr_exprs: Option<Vec<_>> = agg_exec
        .aggr_expr()
        .iter()
        .zip(aggr_arg_sides.iter())
        .map(|(expr, &side)| {
            let offset = if side == GroupBySide::Right {
                left_field_count
            } else {
                0
            };
            let remapped_args: Vec<Arc<dyn PhysicalExpr>> = expr
                .expressions()
                .iter()
                .map(|a| remap_columns(a, &*resolve_index, offset))
                .collect();
            expr.with_new_expressions(remapped_args, vec![])
        })
        .collect();

    let Some(remapped_aggr_exprs) = remapped_aggr_exprs else {
        return Ok(None);
    };
    let remapped_aggr_exprs: Vec<_> =
        remapped_aggr_exprs.into_iter().map(Arc::new).collect();

    GroupJoinExec::try_new_extended(
        *agg_exec.mode(),
        Arc::clone(hash_join.left()),
        Arc::clone(hash_join.right()),
        hash_join.on().to_vec(),
        left_gby,
        right_gby,
        group_by_order,
        remapped_aggr_exprs,
        aggr_arg_sides,
        Arc::clone(&agg_exec.schema()),
    )
    .map(Some)
}

/// Check if all Column expressions in `expr`, after resolving, fall in [start, end).
fn expr_columns_in_side(
    expr: &dyn PhysicalExpr,
    resolve_index: &dyn Fn(usize) -> Option<usize>,
    start: usize,
    end: usize,
) -> bool {
    let mut valid = true;
    check_columns_resolved(expr, resolve_index, start, end, &mut valid);
    valid
}

fn check_columns_resolved(
    expr: &dyn PhysicalExpr,
    resolve_index: &dyn Fn(usize) -> Option<usize>,
    start: usize,
    end: usize,
    valid: &mut bool,
) {
    if !*valid {
        return;
    }
    if let Some(col) = expr.as_any().downcast_ref::<Column>()
        && !matches!(resolve_index(col.index()), Some(resolved) if resolved >= start && resolved < end)
    {
        *valid = false;
        return;
    }
    for child in expr.children() {
        check_columns_resolved(child.as_ref(), resolve_index, start, end, valid);
    }
}

/// Remap column expressions by resolving through the index resolver and subtracting `offset`.
fn remap_columns(
    expr: &Arc<dyn PhysicalExpr>,
    resolve_index: &dyn Fn(usize) -> Option<usize>,
    offset: usize,
) -> Arc<dyn PhysicalExpr> {
    Arc::clone(expr)
        .transform(|e: Arc<dyn PhysicalExpr>| {
            if let Some(col) = e.as_any().downcast_ref::<Column>() {
                let idx = col.index();
                let resolved = resolve_index(idx).unwrap_or(idx);
                Ok(Transformed::yes(Arc::new(Column::new(
                    col.name(),
                    resolved - offset,
                ))))
            } else {
                Ok(Transformed::no(e))
            }
        })
        .unwrap()
        .data
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_plan::PhysicalExpr;

    #[test]
    fn test_expr_columns_in_side_without_projection() {
        let col0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
        let col3: Arc<dyn PhysicalExpr> = Arc::new(Column::new("b", 3));
        let identity = |idx: usize| -> Option<usize> { Some(idx) };

        assert!(expr_columns_in_side(col0.as_ref(), &identity, 0, 4));
        assert!(expr_columns_in_side(col3.as_ref(), &identity, 0, 4));
        assert!(!expr_columns_in_side(col3.as_ref(), &identity, 0, 3));
    }

    #[test]
    fn test_expr_columns_with_projection() {
        // projection: [1, 3] means output col 0 → join col 1, output col 1 → join col 3
        let proj = [1usize, 3];
        let resolve = |idx: usize| -> Option<usize> { proj.get(idx).copied() };

        let col0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
        let col1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("b", 1));

        // col0 → join col 1, in left side [0, 2)
        assert!(expr_columns_in_side(col0.as_ref(), &resolve, 0, 2));
        // col1 → join col 3, in right side [2, MAX)
        assert!(expr_columns_in_side(col1.as_ref(), &resolve, 2, usize::MAX));
        assert!(!expr_columns_in_side(col1.as_ref(), &resolve, 0, 2));
    }

    #[test]
    fn test_remap_columns() {
        let proj = [1usize, 3];
        let resolve = |idx: usize| -> Option<usize> { proj.get(idx).copied() };

        // col1 in aggregate → resolve to join col 3 → right input col 3-2=1
        let col1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("amount", 1));
        let remapped = remap_columns(&col1, &resolve, 2);
        let col = remapped.as_any().downcast_ref::<Column>().unwrap();
        assert_eq!(col.index(), 1);
        assert_eq!(col.name(), "amount");
    }
}
