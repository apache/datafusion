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

//! Sort(fetch) → Join pushdown — a sub-module of `push_down_limit`.
//!
//! When a `Sort` with a fetch limit (TopK) sits above a join whose
//! preserved side is known (LEFT / RIGHT / LeftMark / RightMark / CROSS)
//! and all sort expressions come from the preserved side, we insert a
//! copy of the `Sort(fetch)` onto that input to reduce rows entering
//! the join. The outer `Sort` is kept because a 1-to-many join can
//! produce more than N output rows from N preserved-side rows.
//!
//! Dispatched from `PushDownLimit::rewrite` when the plan node is
//! `LogicalPlan::Sort` with `fetch.is_some()`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::utils::{has_all_column_refs, schema_columns};

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result, internal_err};
use datafusion_expr::logical_plan::{
    JoinType, LogicalPlan, Projection, Sort as SortPlan, SubqueryAlias,
};
use datafusion_expr::{Expr, SortExpr};

/// Which child of a join is being treated as the preserved side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

/// Top-level pushdown for `Sort(fetch) → ... → Join` patterns. The plan
/// passed in is guaranteed by the caller to be `LogicalPlan::Sort` with
/// `fetch.is_some()`; we re-bind to a borrow inside.
pub(super) fn push_topk_through_join(
    plan: LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Sort(sort) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let Some(fetch) = sort.fetch else {
        return Ok(Transformed::no(plan));
    };

    // Don't push if any sort expression is non-deterministic (e.g.
    // `random()`). Duplicating such expressions would produce different
    // values at each evaluation point, potentially changing results.
    if sort.expr.iter().any(|se| se.expr.is_volatile()) {
        return Ok(Transformed::no(plan));
    }

    // Peel through transparent nodes (SubqueryAlias, Projection) to
    // find the Join. Track intermediates so we can reconstruct the tree
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

    // Determine which side(s) of the join are preserved.
    //
    // - LEFT / LeftMark: only left preserved.
    // - RIGHT / RightMark: symmetric.
    // - CROSS JOIN (Inner with no `on` keys and no filter):
    //   every row from both sides appears in the output (Cartesian
    //   product), so we can push to whichever side has all the sort
    //   columns.
    //
    // For LEFT/RIGHT, non-equijoin filters in the ON clause are safe:
    // outer joins guarantee all preserved-side rows appear in the
    // output regardless of the filter. For Inner joins (cross-join
    // detection), the filter check is strict (`filter.is_none()`) —
    // any filter on Inner can drop rows from either side.
    let preserved_candidates: &[Side] = match join.join_type {
        JoinType::Left | JoinType::LeftMark => &[Side::Left],
        JoinType::Right | JoinType::RightMark => &[Side::Right],
        JoinType::Inner if join.on.is_empty() && join.filter.is_none() => {
            &[Side::Left, Side::Right]
        }
        _ => return Ok(Transformed::no(plan)),
    };

    // Resolve sort expressions through all intermediate nodes
    // (Projection, SubqueryAlias) so column references match the
    // join's schema.
    let mut resolved_sort_exprs = sort.expr.clone();
    for node in &intermediates {
        match node {
            LogicalPlan::Projection(proj) => {
                resolved_sort_exprs =
                    resolve_sort_exprs_through_projection(&resolved_sort_exprs, proj)?;
            }
            LogicalPlan::SubqueryAlias(sq) => {
                resolved_sort_exprs =
                    resolve_sort_exprs_through_subquery_alias(&resolved_sort_exprs, sq)?;
            }
            _ => {
                return internal_err!(
                    "push_topk_through_join: unexpected intermediate node: {}",
                    node.display()
                );
            }
        }
    }

    // After resolving through projections, sort expressions may now
    // contain volatile functions (e.g. `random() AS col`). Duplicating
    // them would change results.
    if resolved_sort_exprs.iter().any(|se| se.expr.is_volatile()) {
        return Ok(Transformed::no(plan));
    }

    // Pick the first preserved-side candidate whose schema contains all
    // referenced sort columns. For LEFT/RIGHT this is the fixed side;
    // for CROSS we try both.
    let Some(preserved_side) = preserved_candidates.iter().copied().find(|&side| {
        let schema = match side {
            Side::Left => join.left.schema(),
            Side::Right => join.right.schema(),
        };
        let cols = schema_columns(schema);
        resolved_sort_exprs
            .iter()
            .all(|se| has_all_column_refs(&se.expr, &cols))
    }) else {
        return Ok(Transformed::no(plan));
    };

    let preserved_child = match preserved_side {
        Side::Left => &join.left,
        Side::Right => &join.right,
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
                deep_resolved_exprs =
                    resolve_sort_exprs_through_subquery_alias(&deep_resolved_exprs, sq)?;
                inner_child = sq.input.as_ref();
            }
            LogicalPlan::Projection(proj) => {
                deep_resolved_exprs =
                    resolve_sort_exprs_through_projection(&deep_resolved_exprs, proj)?;
                inner_child = proj.input.as_ref();
            }
            _ => break,
        }
    }

    // If the inner child is a Limit (PushDownLimit's own Limit handling
    // hasn't merged it with the Sort yet), skip this iteration.
    if matches!(inner_child, LogicalPlan::Limit(_)) {
        return Ok(Transformed::no(plan));
    }

    // Determine action based on existing inner Sort:
    // - Same exprs, tighter fetch → skip (already optimal)
    // - Same exprs, larger/no fetch → tighten in-place
    // - Different exprs or no Sort → insert new Sort below the join
    //
    // If `deep_resolved_exprs` became volatile while resolving through
    // projections inside the preserved child (e.g. `random() AS col`),
    // structural equality with an existing inner Sort is unsound: two
    // identical `random()` exprs evaluate to different values. Fall
    // back to inserting a new Sort with `resolved_sort_exprs`.
    let deep_exprs_volatile = deep_resolved_exprs.iter().any(|se| se.expr.is_volatile());
    let inner_sort = match inner_child {
        LogicalPlan::Sort(s) if !deep_exprs_volatile => Some(s),
        _ => None,
    };
    let new_preserved_child = if let Some(child_sort) = inner_sort {
        let same_exprs = sort_exprs_equal(&child_sort.expr, &deep_resolved_exprs);
        let child_fetch_tighter = match child_sort.fetch {
            Some(child_fetch) => child_fetch <= fetch,
            None => false,
        };
        if same_exprs && child_fetch_tighter {
            return Ok(Transformed::no(plan));
        }
        if same_exprs {
            rebuild_with_tightened_sort(
                preserved_child.as_ref(),
                &deep_resolved_exprs,
                fetch,
            )?
        } else {
            // Different exprs — insert new Sort above the preserved
            // child. If the inner Sort has no fetch, our pushed Sort
            // is the only row reduction. If it has a fetch, re-sorting
            // a small set is cheap and still reduces join input.
            Arc::new(LogicalPlan::Sort(SortPlan {
                expr: resolved_sort_exprs,
                input: Arc::clone(preserved_child),
                fetch: Some(fetch),
            }))
        }
    } else {
        Arc::new(LogicalPlan::Sort(SortPlan {
            expr: resolved_sort_exprs,
            input: Arc::clone(preserved_child),
            fetch: Some(fetch),
        }))
    };

    let mut new_join = join.clone();
    match preserved_side {
        Side::Left => new_join.left = new_preserved_child,
        Side::Right => new_join.right = new_preserved_child,
    }

    // Rebuild the tree: join → intermediate nodes → top-level sort.
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
                    "push_topk_through_join: unexpected intermediate node: {}",
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

/// Replace column references in sort expressions using a name→expr map.
fn replace_columns_in_sort_exprs(
    sort_exprs: &[SortExpr],
    replace_map: &HashMap<String, Expr>,
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
fn resolve_sort_exprs_through_projection(
    sort_exprs: &[SortExpr],
    projection: &Projection,
) -> Result<Vec<SortExpr>> {
    let replace_map: HashMap<String, Expr> = projection
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

/// Compare two slices of `SortExpr` for structural equality.
fn sort_exprs_equal(a: &[SortExpr], b: &[SortExpr]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(left, right)| {
            left.asc == right.asc
                && left.nulls_first == right.nulls_first
                && left.expr == right.expr
        })
}

/// Resolve sort expressions through a `SubqueryAlias` by replacing the
/// alias qualifier with the input schema's qualifier.
fn resolve_sort_exprs_through_subquery_alias(
    sort_exprs: &[SortExpr],
    subquery_alias: &SubqueryAlias,
) -> Result<Vec<SortExpr>> {
    let replace_map: HashMap<String, Expr> = subquery_alias
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
/// match `target_exprs`, tightening its fetch to `new_fetch`.
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
    use crate::push_down_limit::PushDownLimit;
    use crate::test::*;

    use datafusion_expr::col;
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(PushDownLimit::new())];
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

    /// CROSS JOIN sorted by left-side columns → pushed to left child.
    #[test]
    fn topk_pushed_to_left_of_cross_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(LogicalPlanBuilder::from(t2).build()?)?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Cross Join:
            Sort: t1.b ASC NULLS LAST, fetch=3
              TableScan: t1
            TableScan: t2
        "
        )
    }

    /// CROSS JOIN sorted by right-side columns → pushed to right child.
    #[test]
    fn topk_pushed_to_right_of_cross_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(LogicalPlanBuilder::from(t2).build()?)?
            .sort_with_limit(vec![col("t2.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t2.b ASC NULLS LAST, fetch=3
          Cross Join:
            TableScan: t1
            Sort: t2.b ASC NULLS LAST, fetch=3
              TableScan: t2
        "
        )
    }

    /// CROSS JOIN sorted by columns from both sides → no pushdown.
    #[test]
    fn topk_not_pushed_for_cross_join_mixed_side_sort() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(LogicalPlanBuilder::from(t2).build()?)?
            .sort_with_limit(
                vec![(col("t1.b") + col("t2.b")).sort(true, false)],
                Some(3),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b + t2.b ASC NULLS LAST, fetch=3
          Cross Join:
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Inner join with no equi-keys but a non-empty filter: filter can drop
    /// rows from either side, so pushing fetch=N is unsafe.
    #[test]
    fn topk_not_pushed_for_inner_with_filter_no_on() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join_on(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Inner,
                vec![col("t1.b").gt(col("t2.b"))],
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Inner Join:  Filter: t1.b > t2.b
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// LEFT MARK join: one record per left row → pushdown to left.
    #[test]
    fn topk_pushed_to_left_of_left_mark_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::LeftMark,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=3
          LeftMark Join: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=3
              TableScan: t1
            TableScan: t2
        "
        )
    }

    /// RIGHT MARK join: symmetric to LeftMark.
    #[test]
    fn topk_pushed_to_right_of_right_mark_join() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::RightMark,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t2.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t2.b ASC NULLS LAST, fetch=3
          RightMark Join: t1.a = t2.a
            TableScan: t1
            Sort: t2.b ASC NULLS LAST, fetch=3
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

    /// Join with non-equijoin filter → pushdown still happens.
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

    /// Sort without fetch → no pushdown.
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

    /// LEFT SEMI JOIN: not all left rows appear in output → no pushdown.
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

    /// LEFT ANTI JOIN: not all left rows appear in output → no pushdown.
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

    /// RIGHT SEMI JOIN: not all right rows appear in output → no pushdown.
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

    /// RIGHT ANTI JOIN: not all right rows appear in output → no pushdown.
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

    /// Projection alias: sort expr references an alias mapping to a negation.
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
