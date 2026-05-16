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

//! [`PushDownTopKThroughJoin`] pushes TopK (Sort with fetch) through outer joins
//!
//! When a `Sort` with a fetch limit sits above an outer join and all sort
//! expressions come from the **preserved** side, this rule inserts a copy
//! of the `Sort(fetch)` on that input to reduce the number of rows
//! entering the join.
//!
//! This is correct because:
//! - A LEFT JOIN preserves every left row (each appears at least once in the
//!   output). The final top-N by left-side columns must come from the top-N
//!   left rows.
//! - The same reasoning applies symmetrically for RIGHT JOIN and right-side
//!   columns.
//!
//! The top-level sort is kept for correctness since a 1-to-many join can
//! produce more than N output rows from N input rows.
//!
//! ## Example
//!
//! Before:
//! ```text
//! Sort: t1.b ASC, fetch=3
//!   Left Join: t1.a = t2.c
//!     Scan: t1     ← scans ALL rows
//!     Scan: t2
//! ```
//!
//! After:
//! ```text
//! Sort: t1.b ASC, fetch=3
//!   Left Join: t1.a = t2.c
//!     Sort: t1.b ASC, fetch=3  ← pushed down
//!       Scan: t1
//!     Scan: t2
//! ```

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use crate::utils::{has_all_column_refs, schema_columns};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result, internal_err};
use datafusion_expr::logical_plan::{
    JoinType, LogicalPlan, Projection, Sort as SortPlan, SubqueryAlias,
};
use datafusion_expr::{Expr, SortExpr};

/// Optimization rule that pushes TopK (Sort with fetch) through
/// LEFT / RIGHT outer joins when all sort expressions come from
/// the preserved side.
///
/// See module-level documentation for details.
#[derive(Default, Debug)]
pub struct PushDownTopKThroughJoin;

impl PushDownTopKThroughJoin {
    /// Create a new `PushDownTopKThroughJoin` rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownTopKThroughJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Match Sort with fetch (TopK)
        let LogicalPlan::Sort(sort) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(fetch) = sort.fetch else {
            return Ok(Transformed::no(plan));
        };

        // Don't push if any sort expression is non-deterministic (e.g. random()).
        // Duplicating such expressions would produce different values at each
        // evaluation point, potentially changing the result.
        if sort.expr.iter().any(|se| se.expr.is_volatile()) {
            return Ok(Transformed::no(plan));
        }

        // Peel through transparent nodes (SubqueryAlias, Projection) to find
        // the Join. Track intermediate nodes so we can reconstruct the tree
        // and resolve sort expressions through them.
        let mut current = sort.input.as_ref();
        let mut intermediates: Vec<&LogicalPlan> = Vec::new();
        let join = loop {
            match current {
                LogicalPlan::Join(join) => break join,
                LogicalPlan::Projection(proj) => {
                    intermediates.push(current);
                    current = proj.input.as_ref();
                }
                LogicalPlan::SubqueryAlias(sq) => {
                    intermediates.push(current);
                    current = sq.input.as_ref();
                }
                _ => return Ok(Transformed::no(plan)),
            }
        };

        // Only outer joins where the preserved side is known.
        // Semi/Anti joins are excluded: not all preserved-side rows appear in
        // the output (only matched/unmatched rows do), so pushing fetch=N to
        // the preserved child can drop rows that would have survived the filter.
        //
        // Non-equijoin filters in the ON clause are safe: outer joins guarantee
        // all preserved-side rows appear in the output regardless of the filter.
        // The filter only controls matching (which non-preserved rows pair up),
        // not which preserved rows survive.
        let preserved_is_left = match join.join_type {
            JoinType::Left => true,
            JoinType::Right => false,
            _ => return Ok(Transformed::no(plan)),
        };

        // Resolve sort expressions through all intermediate nodes (Projection,
        // SubqueryAlias) so that column references match the join's schema.
        let mut resolved_sort_exprs = sort.expr.clone();
        for node in &intermediates {
            match node {
                LogicalPlan::Projection(proj) => {
                    resolved_sort_exprs = resolve_sort_exprs_through_projection(
                        &resolved_sort_exprs,
                        proj,
                    )?;
                }
                LogicalPlan::SubqueryAlias(sq) => {
                    resolved_sort_exprs = resolve_sort_exprs_through_subquery_alias(
                        &resolved_sort_exprs,
                        sq,
                    )?;
                }
                _ => {
                    return internal_err!(
                        "PushDownTopKThroughJoin: unexpected intermediate node: {}",
                        node.display()
                    );
                }
            }
        }

        // After resolving through projections, the sort expressions may now
        // contain volatile functions (e.g. `random() AS col`). Duplicating
        // volatile expressions in the pushed Sort would produce different
        // values, changing results.
        if resolved_sort_exprs.iter().any(|se| se.expr.is_volatile()) {
            return Ok(Transformed::no(plan));
        }

        let preserved_schema = if preserved_is_left {
            join.left.schema()
        } else {
            join.right.schema()
        };
        let preserved_cols = schema_columns(preserved_schema);

        let all_from_preserved = resolved_sort_exprs
            .iter()
            .all(|sort_expr| has_all_column_refs(&sort_expr.expr, &preserved_cols));
        if !all_from_preserved {
            return Ok(Transformed::no(plan));
        }

        let preserved_child = if preserved_is_left {
            &join.left
        } else {
            &join.right
        };

        // Scan deep inside the preserved child (through SubqueryAlias and
        // Projection layers) to find an existing Sort. If found with same
        // exprs, tighten its fetch in-place. Otherwise, insert a new Sort
        // directly below the join as the preserved child's wrapper.
        let mut inner_child = preserved_child.as_ref();
        let mut deep_resolved_exprs = resolved_sort_exprs.clone();
        loop {
            match inner_child {
                LogicalPlan::SubqueryAlias(sq) => {
                    deep_resolved_exprs = resolve_sort_exprs_through_subquery_alias(
                        &deep_resolved_exprs,
                        sq,
                    )?;
                    inner_child = sq.input.as_ref();
                }
                LogicalPlan::Projection(proj) => {
                    deep_resolved_exprs = resolve_sort_exprs_through_projection(
                        &deep_resolved_exprs,
                        proj,
                    )?;
                    inner_child = proj.input.as_ref();
                }
                _ => break,
            }
        }

        // If the inner child is a Limit (PushDownLimit hasn't merged it with
        // the Sort yet), skip this iteration.  PushDownLimit will merge
        // Limit → Sort in the next pass, then our rule will tighten the Sort.
        if matches!(inner_child, LogicalPlan::Limit(_)) {
            return Ok(Transformed::no(plan));
        }

        // Determine action based on existing inner Sort:
        // - Same exprs, tighter fetch → skip (already optimal)
        // - Same exprs, larger/no fetch → tighten in-place
        // - Different exprs or no Sort → insert new Sort below the join
        //
        // Example (tighten): Sort(a ASC, fetch=5) → Join → Sort(a ASC, fetch=10)
        //   Child limits to 10, our tighter fetch=5 tightens it in-place.
        //
        // Example (tighten): Sort(a ASC, fetch=5) → Join → Sort(a ASC)
        //   Child has no fetch (full sort), tighten to fetch=5.
        //
        // Example (skip): Sort(a ASC, fetch=5) → Join → Sort(a ASC, fetch=3)
        //   Child already limits to 3 rows, pushing fetch=5 won't help.
        //
        // Example (new): Sort(b ASC, fetch=5) → Join → Sort(a ASC, fetch=10)
        //   Different exprs, insert Sort(b, fetch=5) above preserved child.
        let new_preserved_child = if let LogicalPlan::Sort(child_sort) = inner_child {
            let same_exprs = sort_exprs_equal(&child_sort.expr, &deep_resolved_exprs);
            let child_fetch_tighter = match child_sort.fetch {
                Some(child_fetch) => child_fetch <= fetch,
                None => false,
            };
            if same_exprs && child_fetch_tighter {
                return Ok(Transformed::no(plan));
            }
            if same_exprs {
                // Tighten existing Sort in-place by rebuilding the path
                // from preserved child down to the Sort.
                rebuild_with_tightened_sort(
                    preserved_child.as_ref(),
                    &deep_resolved_exprs,
                    fetch,
                )?
            } else {
                // Different exprs — insert new Sort above the preserved child.
                // If the inner Sort has no fetch, our pushed Sort is the only
                // row reduction. If it has a fetch, re-sorting a small set is
                // cheap and still reduces rows entering the join.
                Arc::new(LogicalPlan::Sort(SortPlan {
                    expr: resolved_sort_exprs,
                    input: Arc::clone(preserved_child),
                    fetch: Some(fetch),
                }))
            }
        } else {
            // No existing Sort — insert new Sort below the join.
            Arc::new(LogicalPlan::Sort(SortPlan {
                expr: resolved_sort_exprs,
                input: Arc::clone(preserved_child),
                fetch: Some(fetch),
            }))
        };

        // Reconstruct the join with the new child
        let mut new_join = join.clone();
        if preserved_is_left {
            new_join.left = new_preserved_child;
        } else {
            new_join.right = new_preserved_child;
        }

        // Rebuild the tree: join → intermediate nodes → top-level sort
        let mut new_sort_input = Arc::new(LogicalPlan::Join(new_join));
        for node in intermediates.into_iter().rev() {
            new_sort_input = Arc::new(match node {
                LogicalPlan::Projection(proj) => {
                    let mut new_proj = proj.clone();
                    new_proj.input = new_sort_input;
                    LogicalPlan::Projection(new_proj)
                }
                LogicalPlan::SubqueryAlias(sq) => LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(new_sort_input, sq.alias.clone())?,
                ),
                _ => {
                    return internal_err!(
                        "PushDownTopKThroughJoin: unexpected intermediate node: {}",
                        node.display()
                    );
                }
            });
        }

        Ok(Transformed::yes(LogicalPlan::Sort(SortPlan {
            expr: sort.expr.clone(),
            input: new_sort_input,
            fetch: sort.fetch,
        })))
    }

    fn name(&self) -> &str {
        "push_down_topk_through_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Replace column references in sort expressions using a name→expr map.
/// Uses `transform()` for deep replacement (handles nested expressions
/// like `-t1.b` where the column is inside a Negative wrapper).
///
/// Example with `replace_map = {"sub.b" → Column(t1.b)}`:
///
/// ```text
/// Input:  [sub.b ASC]           → Output: [t1.b ASC]         (simple column)
/// Input:  [(- sub.b) ASC]       → Output: [(- t1.b) ASC]     (nested column)
/// Input:  [sub.a ASC, sub.b ASC] → Output: [t1.a ASC, t1.b ASC] (multiple)
/// ```
fn replace_columns_in_sort_exprs(
    sort_exprs: &[SortExpr],
    replace_map: &std::collections::HashMap<String, Expr>,
) -> Result<Vec<SortExpr>> {
    sort_exprs
        .iter()
        .map(|sort_expr| {
            let new_expr = sort_expr.expr.clone().transform(|expr| {
                let replacement = match &expr {
                    Expr::Column(col) => replace_map.get(&col.flat_name()).cloned(),
                    _ => None,
                };
                Ok(replacement.map_or_else(|| Transformed::no(expr), Transformed::yes))
            })?;
            Ok(SortExpr {
                expr: new_expr.data,
                ..*sort_expr
            })
        })
        .collect()
}

/// Resolve sort expressions through a projection by replacing column
/// references with the underlying projection expressions.
///
/// Example: sort expr is `neg_b ASC` and projection has `-t1.b AS neg_b`:
///
/// ```text
/// Input sort exprs:  [neg_b ASC]
/// Output sort exprs: [(- t1.b) ASC]
/// ```
fn resolve_sort_exprs_through_projection(
    sort_exprs: &[SortExpr],
    projection: &Projection,
) -> Result<Vec<SortExpr>> {
    let replace_map: std::collections::HashMap<String, Expr> = projection
        .schema
        .iter()
        .zip(projection.expr.iter())
        .map(|((qualifier, field), expr)| {
            let key = Column::from((qualifier, field)).flat_name();
            (key, expr.clone().unalias())
        })
        .collect();

    replace_columns_in_sort_exprs(sort_exprs, &replace_map)
}

/// Compare two slices of `SortExpr` for equality.
///
/// Uses structural equality on the sort expressions (direction, nulls_first,
/// and the expression tree).
fn sort_exprs_equal(a: &[SortExpr], b: &[SortExpr]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(left, right)| {
            left.asc == right.asc
                && left.nulls_first == right.nulls_first
                && left.expr == right.expr
        })
}

/// Resolve sort expressions through a SubqueryAlias by replacing the alias
/// qualifier with the input schema's qualifier.
///
/// Example: SubqueryAlias is `sub` wrapping a join whose left input is `t1`:
///
/// ```text
/// Input sort exprs:  [sub.b ASC]
/// Output sort exprs: [t1.b ASC]
/// ```
fn resolve_sort_exprs_through_subquery_alias(
    sort_exprs: &[SortExpr],
    subquery_alias: &SubqueryAlias,
) -> Result<Vec<SortExpr>> {
    let replace_map: std::collections::HashMap<String, Expr> = subquery_alias
        .schema
        .iter()
        .zip(subquery_alias.input.schema().iter())
        .map(|((alias_qual, alias_field), (input_qual, input_field))| {
            let alias_col = Column::from((alias_qual, alias_field));
            let input_col = Column::from((input_qual, input_field));
            (alias_col.flat_name(), Expr::Column(input_col))
        })
        .collect();

    replace_columns_in_sort_exprs(sort_exprs, &replace_map)
}

/// Rebuild the tree from `root` down to an existing Sort whose expressions
/// match `target_exprs`, tightening its fetch to `new_fetch`. The path from
/// `root` to the target Sort may contain Projections and SubqueryAliases.
///
/// Before (new_fetch=2):
/// ```text
/// SubqueryAlias(t1)
///   Projection(a, b AS renamed_b)
///     Sort(t1.b ASC, fetch=10)       ← target, fetch too large
///       TableScan: t1
/// ```
///
/// After:
/// ```text
/// SubqueryAlias(t1)                  ← rebuilt
///   Projection(a, b AS renamed_b)    ← rebuilt
///     Sort(t1.b ASC, fetch=2)        ← tightened
///       TableScan: t1
/// ```
fn rebuild_with_tightened_sort(
    root: &LogicalPlan,
    target_exprs: &[SortExpr],
    new_fetch: usize,
) -> Result<Arc<LogicalPlan>> {
    match root {
        LogicalPlan::Sort(s) if sort_exprs_equal(&s.expr, target_exprs) => {
            Ok(Arc::new(LogicalPlan::Sort(SortPlan {
                expr: s.expr.clone(),
                input: Arc::clone(&s.input),
                fetch: Some(new_fetch),
            })))
        }
        LogicalPlan::Projection(proj) => {
            let new_input = rebuild_with_tightened_sort(
                proj.input.as_ref(),
                target_exprs,
                new_fetch,
            )?;
            let mut new_proj = proj.clone();
            new_proj.input = new_input;
            Ok(Arc::new(LogicalPlan::Projection(new_proj)))
        }
        LogicalPlan::SubqueryAlias(sq) => {
            let new_input =
                rebuild_with_tightened_sort(sq.input.as_ref(), target_exprs, new_fetch)?;
            Ok(Arc::new(LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(new_input, sq.alias.clone())?,
            )))
        }
        _ => internal_err!(
            "rebuild_with_tightened_sort: unexpected node: {}",
            root.display()
        ),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::*;

    use datafusion_expr::col;
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(PushDownTopKThroughJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    /// TopK on left-side columns above a LEFT JOIN → pushed to left child.
    #[test]
    fn topk_pushed_to_left_of_left_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Left Join: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=3
              TableScan: t1
            TableScan: t2
        "
        )
    }

    /// TopK on right-side columns above a RIGHT JOIN → pushed to right child.
    #[test]
    fn topk_pushed_to_right_of_right_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Right,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t2.b").sort(true, false)], Some(5))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t2.b ASC NULLS LAST, fetch=5
          Right Join: t1.a = t2.a
            TableScan: t1
            Sort: t2.b ASC NULLS LAST, fetch=5
              TableScan: t2
        "
        )
    }

    /// TopK pushed through a Projection between Sort and Join.
    #[test]
    fn topk_pushed_through_projection() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .project(vec![col("t1.a"), col("t1.b"), col("t2.c")])?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Projection: t1.a, t1.b, t2.c
            Left Join: t1.a = t2.a
              Sort: t1.b ASC NULLS LAST, fetch=3
                TableScan: t1
              TableScan: t2
        "
        )
    }

    /// INNER JOIN → no pushdown.
    #[test]
    fn topk_not_pushed_for_inner_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Inner,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// LEFT JOIN but sort on right-side columns → no pushdown.
    #[test]
    fn topk_not_pushed_for_wrong_side() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t2.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t2.b ASC NULLS LAST, fetch=3
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Join with a non-equijoin filter → pushdown still happens.
    /// Outer joins preserve all rows from the preserved side regardless
    /// of the ON filter.
    #[test]
    fn topk_pushed_with_join_filter() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join_on(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                vec![col("t1.a").eq(col("t2.a"))],
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Left Join:  Filter: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=3
              TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Sort without fetch (unbounded) → no pushdown.
    #[test]
    fn topk_not_pushed_without_fetch() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort(vec![col("t1.b").sort(true, false)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// LEFT SEMI JOIN: pushing fetch is unsafe (not all left rows appear in output).
    #[test]
    fn topk_not_pushed_for_left_semi_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::LeftSemi,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          LeftSemi Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// LEFT ANTI JOIN: pushing fetch is unsafe (not all left rows appear in output).
    #[test]
    fn topk_not_pushed_for_left_anti_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::LeftAnti,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          LeftAnti Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// RIGHT SEMI JOIN: pushing fetch is unsafe (not all right rows appear in output).
    #[test]
    fn topk_not_pushed_for_right_semi_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::RightSemi,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t2.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t2.b ASC NULLS LAST, fetch=3
          RightSemi Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// RIGHT ANTI JOIN: pushing fetch is unsafe (not all right rows appear in output).
    #[test]
    fn topk_not_pushed_for_right_anti_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::RightAnti,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t2.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t2.b ASC NULLS LAST, fetch=3
          RightAnti Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Multi-column sort with columns from both sides → no pushdown.
    #[test]
    fn topk_not_pushed_for_mixed_side_sort() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(
                vec![col("t1.b").sort(true, false), col("t2.b").sort(true, false)],
                Some(3),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, t2.b ASC NULLS LAST, fetch=3
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Preserved child has a larger fetch → push our tighter limit.
    #[test]
    fn topk_pushed_when_child_has_larger_fetch() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Child already has Sort(b ASC, fetch=10); our outer Sort has fetch=3 (tighter).
        let t1_with_sort = LogicalPlanBuilder::from(t1)
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(10))?
            .build()?;

        let plan = LogicalPlanBuilder::from(t1_with_sort)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Left Join: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=3
              TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Preserved child already has a tighter fetch → skip pushdown.
    #[test]
    fn topk_not_pushed_when_child_has_smaller_fetch() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // Child already has Sort(b ASC, fetch=2); our outer Sort has fetch=5 (looser).
        let t1_with_sort = LogicalPlanBuilder::from(t1)
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(2))?
            .build()?;

        let plan = LogicalPlanBuilder::from(t1_with_sort)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(5))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=5
          Left Join: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=2
              TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Projection passthrough: sort expr matches a projected column directly.
    #[test]
    fn resolve_through_projection_passthrough() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(t1)
            .project(vec![col("t1.a"), col("t1.b")])?
            .build()?;
        let LogicalPlan::Projection(proj) = &plan else {
            panic!("expected Projection");
        };
        let sort_exprs = vec![col("t1.b").sort(true, false)];
        let resolved = resolve_sort_exprs_through_projection(&sort_exprs, proj)?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].expr.to_string(), "t1.b");
        assert!(resolved[0].asc);
        Ok(())
    }

    /// Projection alias: sort expr references an alias that maps to a negation.
    #[test]
    fn resolve_through_projection_alias() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(t1)
            .project(vec![
                col("t1.a"),
                (Expr::Negative(Box::new(col("t1.b")))).alias("neg_b"),
            ])?
            .build()?;
        let LogicalPlan::Projection(proj) = &plan else {
            panic!("expected Projection");
        };
        let sort_exprs = vec![col("neg_b").sort(true, false)];
        let resolved = resolve_sort_exprs_through_projection(&sort_exprs, proj)?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].expr.to_string(), "(- t1.b)");
        Ok(())
    }

    /// Multi-column resolution preserves direction and nulls_first per column.
    #[test]
    fn resolve_through_projection_multi_column() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(t1)
            .project(vec![col("t1.a"), col("t1.b"), col("t1.c")])?
            .build()?;
        let LogicalPlan::Projection(proj) = &plan else {
            panic!("expected Projection");
        };
        let sort_exprs =
            vec![col("t1.a").sort(true, false), col("t1.b").sort(false, true)];
        let resolved = resolve_sort_exprs_through_projection(&sort_exprs, proj)?;
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].expr.to_string(), "t1.a");
        assert!(resolved[0].asc);
        assert_eq!(resolved[1].expr.to_string(), "t1.b");
        assert!(!resolved[1].asc);
        assert!(resolved[1].nulls_first);
        Ok(())
    }

    /// Stacked Projection + SubqueryAlias: resolve through both layers.
    #[test]
    fn resolve_through_projection_and_subquery_alias() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(t1)
            .alias("sub")?
            .project(vec![
                col("sub.a"),
                (Expr::Negative(Box::new(col("sub.b")))).alias("neg_b"),
            ])?
            .build()?;
        let LogicalPlan::Projection(proj) = &plan else {
            panic!("expected Projection");
        };
        let LogicalPlan::SubqueryAlias(sq) = proj.input.as_ref() else {
            panic!("expected SubqueryAlias");
        };
        let sort_exprs = vec![col("neg_b").sort(true, false)];
        let after_proj = resolve_sort_exprs_through_projection(&sort_exprs, proj)?;
        assert_eq!(after_proj[0].expr.to_string(), "(- sub.b)");
        let after_sq = resolve_sort_exprs_through_subquery_alias(&after_proj, sq)?;
        assert_eq!(after_sq[0].expr.to_string(), "(- t1.b)");
        assert!(after_sq[0].asc);
        assert!(!after_sq[0].nulls_first);
        Ok(())
    }

    /// Simple SubqueryAlias resolution: sub.b → t1.b.
    #[test]
    fn resolve_through_subquery_alias_simple() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(t1).alias("sub")?.build()?;
        let LogicalPlan::SubqueryAlias(sq) = &plan else {
            panic!("expected SubqueryAlias");
        };
        let sort_exprs = vec![col("sub.b").sort(true, false)];
        let resolved = resolve_sort_exprs_through_subquery_alias(&sort_exprs, sq)?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].expr.to_string(), "t1.b");
        assert!(resolved[0].asc);
        assert!(!resolved[0].nulls_first);
        Ok(())
    }

    /// Multi-column SubqueryAlias resolution preserves direction per column.
    #[test]
    fn resolve_through_subquery_alias_multi_column() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(t1).alias("sub")?.build()?;
        let LogicalPlan::SubqueryAlias(sq) = &plan else {
            panic!("expected SubqueryAlias");
        };
        let sort_exprs = vec![
            col("sub.a").sort(true, false),
            col("sub.b").sort(false, true),
        ];
        let resolved = resolve_sort_exprs_through_subquery_alias(&sort_exprs, sq)?;
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].expr.to_string(), "t1.a");
        assert!(resolved[0].asc);
        assert_eq!(resolved[1].expr.to_string(), "t1.b");
        assert!(!resolved[1].asc);
        assert!(resolved[1].nulls_first);
        Ok(())
    }

    /// SubqueryAlias with a different alias name (foo ≠ t1).
    #[test]
    fn resolve_through_subquery_alias_different_name() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(t1).alias("foo")?.build()?;
        let LogicalPlan::SubqueryAlias(sq) = &plan else {
            panic!("expected SubqueryAlias");
        };
        let sort_exprs = vec![col("foo.b").sort(true, false)];
        let resolved = resolve_sort_exprs_through_subquery_alias(&sort_exprs, sq)?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].expr.to_string(), "t1.b");
        Ok(())
    }

    /// SubqueryAlias with nested expression: (- sub.b) → (- t1.b).
    #[test]
    fn resolve_through_subquery_alias_nested_expr() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let plan = LogicalPlanBuilder::from(t1).alias("sub")?.build()?;
        let LogicalPlan::SubqueryAlias(sq) = &plan else {
            panic!("expected SubqueryAlias");
        };
        let sort_exprs = vec![Expr::Negative(Box::new(col("sub.b"))).sort(true, false)];
        let resolved = resolve_sort_exprs_through_subquery_alias(&sort_exprs, sq)?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].expr.to_string(), "(- t1.b)");
        assert!(resolved[0].asc);
        Ok(())
    }

    /// Inner Sort has different exprs WITH fetch → stacked sorts.
    /// Sort(b, fetch=2) is inserted above Sort(a, fetch=5). Re-sorting
    /// 5 rows is cheap and reduces join input from 5 to 2.
    #[test]
    fn topk_stacked_when_child_has_different_exprs_with_fetch() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let t1_with_sort = LogicalPlanBuilder::from(t1)
            .sort_with_limit(vec![col("t1.a").sort(true, false)], Some(5))?
            .build()?;

        let plan = LogicalPlanBuilder::from(t1_with_sort)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(2))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=2
          Left Join: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=2
              Sort: t1.a ASC NULLS LAST, fetch=5
                TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Inner Sort has different exprs WITHOUT fetch → stacked sorts.
    /// Full sort doesn't limit rows, so pushed Sort(fetch=2) is the
    /// only row reduction before the join.
    #[test]
    fn topk_stacked_when_child_has_different_exprs_no_fetch() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let t1_with_sort = LogicalPlanBuilder::from(t1)
            .sort(vec![col("t1.a").sort(true, false)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(t1_with_sort)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(2))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=2
          Left Join: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=2
              Sort: t1.a ASC NULLS LAST
                TableScan: t1
            TableScan: t2
        "
        )
    }
}
