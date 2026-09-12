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
//! preserved side is known (LEFT / RIGHT / LeftMark / RightMark) and all
//! sort expressions come from the preserved side, we insert a copy of the
//! `Sort(fetch)` onto that input to reduce rows entering the join. The
//! outer `Sort` is kept because a 1-to-many join can produce more than N
//! output rows from N preserved-side rows.
//!
//! CROSS JOIN is deliberately excluded, even though every row from both
//! sides appears in its output when *both* sides are non-empty: if the
//! *other* side turns out to be empty, the join output is empty and the
//! original (unpushed) Sort never evaluates its expression, whereas a
//! pushed Sort would evaluate it eagerly regardless — which can surface an
//! error from a fallible sort expression that a correct, unoptimized
//! execution would never have hit. Proving the other side non-empty at
//! logical-plan time isn't something this optimizer (or DataFusion's
//! optimizer generally) has machinery for, so this rule stays conservative
//! and skips CROSS JOIN entirely rather than risk changing whether a query
//! errors.
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

/// If `node` is a transparent node (`Projection` or `SubqueryAlias`),
/// returns its child together with `exprs` resolved through it (column
/// references rewritten to match the child's schema). Returns `None` for
/// any other node, signaling the walk should stop.
///
/// Shared by both tree walks in [`push_topk_through_join`]: peeling down
/// from the `Sort` to find the `Join`, and scanning inside the preserved
/// child for an existing inner `Sort`.
fn peel_transparent_layer<'a>(
    node: &'a LogicalPlan,
    exprs: &[SortExpr],
) -> Result<Option<(&'a LogicalPlan, Vec<SortExpr>)>> {
    match node {
        LogicalPlan::Projection(proj) => Ok(Some((
            proj.input.as_ref(),
            resolve_sort_exprs_through_projection(exprs, proj)?,
        ))),
        LogicalPlan::SubqueryAlias(sq) => Ok(Some((
            sq.input.as_ref(),
            resolve_sort_exprs_through_subquery_alias(exprs, sq)?,
        ))),
        _ => Ok(None),
    }
}

/// Top-level pushdown for `Sort(fetch) → ... → Join` patterns. The caller
/// (`PushDownLimit::rewrite`) has already matched `LogicalPlan::Sort(sort)`
/// with `sort.fetch.is_some()`; taking `Sort` by value here (rather than
/// re-wrapping it into a `LogicalPlan` just to immediately unwrap it) means
/// early exits can hand `sort` straight back via `LogicalPlan::Sort(sort)`
/// without cloning.
pub(super) fn push_topk_through_join(sort: SortPlan) -> Result<Transformed<LogicalPlan>> {
    let Some(fetch) = sort.fetch else {
        return Ok(Transformed::no(LogicalPlan::Sort(sort)));
    };

    // Don't push if any sort expression is non-deterministic (e.g.
    // `random()`). Duplicating such expressions would produce different
    // values at each evaluation point, potentially changing results.
    if sort.expr.iter().any(|se| se.expr.is_volatile()) {
        return Ok(Transformed::no(LogicalPlan::Sort(sort)));
    }

    // Peel through transparent nodes (SubqueryAlias, Projection) to find
    // the Join, resolving sort expressions through each layer along the
    // way so column references end up matching the join's schema. Track
    // intermediates so we can reconstruct the tree afterward.
    let mut current = sort.input.as_ref();
    let mut intermediates: Vec<&LogicalPlan> = Vec::new();
    let mut resolved_sort_exprs = sort.expr.clone();
    let join = loop {
        if let LogicalPlan::Join(join) = current {
            break join;
        }
        match peel_transparent_layer(current, &resolved_sort_exprs)? {
            Some((next, new_exprs)) => {
                intermediates.push(current);
                current = next;
                resolved_sort_exprs = new_exprs;
            }
            None => return Ok(Transformed::no(LogicalPlan::Sort(sort))),
        }
    };

    // Determine which side of the join is preserved.
    //
    // - LEFT / LeftMark: only left preserved.
    // - RIGHT / RightMark: symmetric.
    //
    // Non-equijoin filters in the ON clause are safe: outer joins guarantee
    // all preserved-side rows appear in the output regardless of the
    // filter.
    //
    // CROSS JOIN and other Inner joins are deliberately excluded — see the
    // module doc comment for why CROSS JOIN specifically isn't safe to
    // push through here despite superficially preserving every row.
    let preserved_side = match join.join_type {
        JoinType::Left | JoinType::LeftMark => Side::Left,
        JoinType::Right | JoinType::RightMark => Side::Right,
        _ => return Ok(Transformed::no(LogicalPlan::Sort(sort))),
    };

    // After resolving through projections, sort expressions may now
    // contain volatile functions (e.g. `random() AS col`). Duplicating
    // them would change results.
    if resolved_sort_exprs.iter().any(|se| se.expr.is_volatile()) {
        return Ok(Transformed::no(LogicalPlan::Sort(sort)));
    }

    // A sort key with no column references at all (e.g. `ORDER BY 'x'
    // LIMIT 3`) can't distinguish any row from any other on either side.
    // `has_all_column_refs` is vacuously true for such a key (there are no
    // column refs to fail to find), so without this check we'd still push
    // a Sort onto a preserved child even though its "top N" doesn't depend
    // on that child's data at all — a legal result (any N rows satisfy an
    // all-ties ORDER BY), but wasted work duplicating a Sort node for no
    // row-reduction benefit.
    if resolved_sort_exprs
        .iter()
        .all(|se| se.expr.column_refs().is_empty())
    {
        return Ok(Transformed::no(LogicalPlan::Sort(sort)));
    }

    // Confirm the preserved side's schema contains all referenced sort
    // columns.
    //
    // Caveat: `schema_columns` adds an unqualified entry for every field
    // alongside its qualified one, so an unqualified sort column matches as
    // long as the preserved side has a same-named field — even if an
    // identically-named field also exists on the *other* side, in which
    // case an unqualified reference is genuinely ambiguous and this could
    // wrongly treat a reference to the other side's column as if it were
    // the preserved side's. SQL can never produce this: the planner always
    // qualifies column references (or rejects the query as ambiguous)
    // before this rule runs. Only a hand-built `LogicalPlan` with a
    // deliberately unqualified `Column` can hit this. `push_down_filter`
    // has the exact same caveat, for the same reason (it also calls
    // `schema_columns` to accept unqualified filter columns).
    let schema = match preserved_side {
        Side::Left => join.left.schema(),
        Side::Right => join.right.schema(),
    };
    let cols = schema_columns(schema);
    if !resolved_sort_exprs
        .iter()
        .all(|se| has_all_column_refs(&se.expr, &cols))
    {
        return Ok(Transformed::no(LogicalPlan::Sort(sort)));
    }

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
    while let Some((next, new_exprs)) =
        peel_transparent_layer(inner_child, &deep_resolved_exprs)?
    {
        inner_child = next;
        deep_resolved_exprs = new_exprs;
    }

    // If the inner child is a Limit, skip pushing down for now.
    //
    // This is only correct-but-conservative, not a correctness requirement:
    // inserting `Sort(fetch)` *above* the Limit (`Sort(fetch=N) -> Limit ->
    // ...`) would be sound regardless of what the Limit's rows are — it
    // only ever reduces which of the Limit's already-capped rows survive,
    // the same "insert a new Sort above the preserved child" move used
    // below when the exprs differ from an existing inner Sort. We decline
    // instead, which is a missed optimization, not a blocked one:
    // - If a `Sort` sits below the Limit (`Limit -> Sort`), `PushDownLimit`
    //   merges the two on a later pass into `Sort(fetch)`, and this rule
    //   revisits the node then and pushes down as normal.
    // - If there's no `Sort` at all (e.g. `LEFT JOIN (SELECT * FROM t LIMIT
    //   100)`), there is nothing for the Limit to ever merge with, so this
    //   case never gets pushed down at all, on any pass — a real gap, not
    //   just a later-pass one, but non-blocking: the join still runs
    //   correctly against the already-limited input, just without the
    //   extra row reduction a pushed Sort could have added.
    if matches!(inner_child, LogicalPlan::Limit(_)) {
        return Ok(Transformed::no(LogicalPlan::Sort(sort)));
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
    let same_exprs_sort = inner_sort
        .filter(|child_sort| sort_exprs_equal(&child_sort.expr, &deep_resolved_exprs));

    let new_preserved_child = if let Some(child_sort) = same_exprs_sort {
        let child_fetch_tighter = match child_sort.fetch {
            Some(child_fetch) => child_fetch <= fetch,
            None => false,
        };
        if child_fetch_tighter {
            return Ok(Transformed::no(LogicalPlan::Sort(sort)));
        }
        rebuild_with_tightened_sort(
            preserved_child.as_ref(),
            &deep_resolved_exprs,
            fetch,
        )?
    } else {
        // Different exprs, or no existing inner Sort — insert a new Sort
        // above the preserved child. If an inner Sort exists with no
        // fetch, our pushed Sort is the only row reduction. If it has a
        // fetch, re-sorting a small set is cheap and still reduces join
        // input.
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
        expr: sort.expr,
        input: new_sort_input,
        fetch: sort.fetch,
    })))
}

/// Replace column references in sort expressions using a structural
/// `Column`→expr map. Keying by `Column` (rather than `Column::flat_name()`)
/// avoids collisions between an unqualified column whose name happens to
/// contain a dot (e.g. a quoted alias `"t1.b"`) and a qualified column that
/// stringifies the same way (`t1.b`).
fn replace_columns_in_sort_exprs(
    sort_exprs: &[SortExpr],
    replace_map: &HashMap<Column, Expr>,
) -> Result<Vec<SortExpr>> {
    sort_exprs
        .iter()
        .map(|sort_expr| {
            let new_expr = sort_expr.expr.clone().transform(|expr| {
                let replacement = match &expr {
                    Expr::Column(col) => replace_map.get(col).cloned(),
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
    let replace_map: HashMap<Column, Expr> = projection
        .schema
        .iter()
        .zip(projection.expr.iter())
        .map(|((qualifier, field), expr)| {
            let key = Column::from((qualifier, field));
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
    let replace_map: HashMap<Column, Expr> = subquery_alias
        .schema
        .iter()
        .zip(subquery_alias.input.schema().iter())
        .map(|((alias_qual, alias_field), (input_qual, input_field))| {
            let alias_col = Column::from((alias_qual, alias_field));
            let input_col = Column::from((input_qual, input_field));
            (alias_col, Expr::Column(input_col))
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
    use datafusion_expr::lit;
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

    /// CROSS JOIN is never pushed through, even when the sort key comes
    /// entirely from one side: see the module doc comment for why (a
    /// pushed Sort would evaluate eagerly on a side whose relevance
    /// depends on the *other* side being non-empty, which this rule has no
    /// way to prove at logical-plan time).
    #[test]
    fn topk_not_pushed_for_cross_join_left_side_sort() -> Result<()> {
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
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Symmetric to the left-side case above.
    #[test]
    fn topk_not_pushed_for_cross_join_right_side_sort() -> Result<()> {
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
            TableScan: t2
        "
        )
    }

    /// CROSS JOIN sorted by columns from both sides → no pushdown (would
    /// still be excluded even if CROSS JOIN were otherwise supported).
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

    /// Regression test for a `flat_name()` collision: an unqualified alias
    /// literally named "t1.b" (as produced by a quoted identifier like
    /// `t2.y AS "t1.b"`) must not be confused with the qualified column
    /// `t1.b` that is also present in the same projection. Both used to
    /// stringify to the same `flat_name()` ("t1.b"), so the old
    /// `HashMap<String, Expr>` would silently resolve the alias to the
    /// wrong underlying expression.
    ///
    /// Built via `Projection::try_new` directly rather than
    /// `LogicalPlanBuilder::project`: the builder's `project()` rejects two
    /// expressions with the same `schema_name()` up front (so this exact
    /// shape can't come from a `SELECT` list), but nothing stops a
    /// `Projection` built by other means — e.g. a different optimizer rule,
    /// or a plan constructed directly via the expr API — from having two
    /// structurally distinct output columns whose *string* names collide.
    /// `resolve_sort_exprs_through_projection` must handle that shape
    /// correctly regardless of how the `Projection` was constructed.
    #[test]
    fn resolve_through_projection_quoted_alias_no_collision() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let join = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .build()?;
        let proj = Projection::try_new(
            vec![col("t1.a"), col("t2.b").alias("t1.b"), col("t1.b")],
            Arc::new(join),
        )?;

        // `"t1.b"` here is the unqualified alias output column, not the
        // qualified `t1.b` column also present in the projection.
        let quoted_alias = Expr::Column(Column::new_unqualified("t1.b"));
        let sort_exprs = vec![quoted_alias.sort(true, false)];
        let resolved = resolve_sort_exprs_through_projection(&sort_exprs, &proj)?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].expr.to_string(), "t2.b");
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

    /// End-to-end regression for the `flat_name()` collision: `t2.b AS
    /// "t1.b"` (an unqualified alias literally named "t1.b") sits next to
    /// the qualified column `t1.b` in the same projection. ORDER BY the
    /// quoted alias must push down sorted on `t2.b` (the right/preserved
    /// side of this RIGHT JOIN), not silently resolve to `t1.b` and push
    /// the wrong TopK onto the wrong side.
    #[test]
    fn topk_pushed_through_projection_quoted_alias_no_collision() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let join = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Right,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .build()?;
        // Built via `Projection::try_new` (see comment on
        // `resolve_through_projection_quoted_alias_no_collision`): the
        // builder's `project()` would reject this expression list outright.
        let proj = LogicalPlan::Projection(Projection::try_new(
            vec![col("t1.a"), col("t2.b").alias("t1.b"), col("t1.b")],
            Arc::new(join),
        )?);
        let plan = LogicalPlanBuilder::from(proj)
            .sort_with_limit(
                vec![Expr::Column(Column::new_unqualified("t1.b")).sort(true, false)],
                Some(3),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r#"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Projection: t1.a, t2.b AS t1.b, t1.b
            Right Join: t1.a = t2.a
              TableScan: t1
              Sort: t2.b ASC NULLS LAST, fetch=3
                TableScan: t2
        "#
        )
    }

    /// Plan-level regression for the CROSS JOIN empty-side hazard: a
    /// division (fallible) sort key over a CROSS JOIN must never be pushed
    /// down, since doing so would evaluate it eagerly on rows that a
    /// correct, unoptimized execution would never reach if the other side
    /// turned out to be empty (see `topk_pushed_for_cross_join_with_empty_other_side_does_not_error`
    /// in the SLT suite for the corresponding end-to-end execution case).
    #[test]
    fn topk_not_pushed_for_cross_join_with_fallible_sort_expr() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .cross_join(LogicalPlanBuilder::from(t2).build()?)?
            .sort_with_limit(
                vec![(col("t1.a") / col("t1.b")).sort(true, false)],
                Some(3),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.a / t1.b ASC NULLS LAST, fetch=3
          Cross Join:
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// A sort key with no column references at all (e.g. a literal) can't
    /// distinguish any row from any other on either side. `has_all_column_refs`
    /// is vacuously true for it, so without an explicit guard this would
    /// still push a Sort onto the first candidate side — legal (any N rows
    /// satisfy an all-ties ORDER BY) but wasted work, since the pushed
    /// Sort's "top N" doesn't depend on that side's data at all.
    #[test]
    fn topk_not_pushed_for_sort_key_with_no_column_refs() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![lit(1i64).sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: Int64(1) ASC NULLS LAST, fetch=3
          Left Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    /// `Limit(skip=0) → Sort(no fetch) → Join`: `rewrite_limit` merges the
    /// Limit's fetch into the Sort on one visit, but only strips the now-
    /// redundant `Limit(skip=0)` wrapper on a *later* visit to that same
    /// node (it only recognizes the wrapper as redundant once the Sort
    /// already carries the matching fetch). Confirms the TopK-through-join
    /// pushdown and the Limit/Sort merge both still converge to the fully
    /// collapsed form with two passes.
    #[test]
    fn topk_pushed_through_limit_then_sort_with_two_passes() -> Result<()> {
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
            .limit(0, Some(3))?
            .build()?;

        let optimizer_ctx = OptimizerContext::new().with_max_passes(2);
        let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
            vec![Arc::new(PushDownLimit::new())];
        assert_optimized_plan_eq_snapshot!(
            optimizer_ctx,
            rules,
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

    /// Running the optimizer a second time on its own output must be a
    /// no-op: a rule that keeps rewriting an already-optimized plan (e.g.
    /// oscillating between two equivalent forms) would never let the
    /// overall optimizer converge.
    #[test]
    fn topk_pushed_through_join_is_idempotent() -> Result<()> {
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

        let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
            vec![Arc::new(PushDownLimit::new())];
        let optimizer = crate::Optimizer::with_rules(rules);
        let ctx = OptimizerContext::new().with_max_passes(1);

        let once = optimizer.optimize(plan, &ctx, |_, _| {})?;
        let twice = optimizer.optimize(once.clone(), &ctx, |_, _| {})?;
        assert_eq!(once, twice);
        Ok(())
    }

    /// `fetch = 0` is a degenerate but valid TopK: the pushed Sort still
    /// carries `fetch=0` rather than being special-cased away.
    #[test]
    fn topk_pushed_with_fetch_zero() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(0))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: t1.b ASC NULLS LAST, fetch=0
          Left Join: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=0
              TableScan: t1
            TableScan: t2
        "
        )
    }

    /// Preserved child is a bare `Limit` with no `Sort` beneath it at all
    /// (`LEFT JOIN (SELECT * FROM t1 LIMIT 5)`) — there is nothing for the
    /// Limit to ever merge with, so this is a permanent missed
    /// optimization, not a transient one that resolves on a later pass (see
    /// the comment at the `matches!(inner_child, LogicalPlan::Limit(_))`
    /// check). It must still skip cleanly here rather than push past the
    /// Limit and silently ignore the existing row cap.
    #[test]
    fn topk_not_pushed_when_child_is_bare_limit() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let t1_with_limit = LogicalPlanBuilder::from(t1).limit(0, Some(5))?.build()?;

        let plan = LogicalPlanBuilder::from(t1_with_limit)
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
            Limit: skip=0, fetch=5
              TableScan: t1, fetch=5
            TableScan: t2
        "
        )
    }

    /// A `Filter` between the `Sort` and the `Join` blocks pushdown: `Filter`
    /// is not one of the transparent nodes this rewrite peels through (a
    /// `Filter` can drop preserved-side rows, which would change which rows
    /// survive to be sorted). Running `PushDownLimit` alone can never push
    /// through it, however many passes are allowed.
    #[test]
    fn topk_not_pushed_through_filter_with_push_down_limit_alone() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .filter(col("t1.c").eq(lit("foo")))?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r#"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Filter: t1.c = Utf8("foo")
            Left Join: t1.a = t2.a
              TableScan: t1
              TableScan: t2
        "#
        )
    }

    /// Same plan as above, but with `PushDownFilter` also in the rule set
    /// and two passes allowed: pass 1 runs `push_down_limit` first (declines,
    /// same as the single-rule case above) and then `push_down_filter` sinks
    /// the Filter onto `t1` (the only side it references) below the Join;
    /// pass 2 revisits the Sort, which now sits directly above the Join and
    /// pushes through. One pass alone is not enough — see
    /// `topk_not_pushed_through_filter_with_push_down_limit_alone` and the
    /// module doc comment's note on this ordering dependency.
    #[test]
    fn topk_pushed_through_filter_after_push_down_filter_with_two_passes() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .filter(col("t1.c").eq(lit("foo")))?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        let optimizer_ctx = OptimizerContext::new().with_max_passes(2);
        let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![
            Arc::new(PushDownLimit::new()),
            Arc::new(crate::push_down_filter::PushDownFilter::new()),
        ];
        assert_optimized_plan_eq_snapshot!(
            optimizer_ctx,
            rules,
            plan,
            @r#"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Left Join: t1.a = t2.a
            Sort: t1.b ASC NULLS LAST, fetch=3
              TableScan: t1, full_filters=[t1.c = Utf8("foo")]
            TableScan: t2
        "#
        )
    }

    /// Regression test using the *real* default rule set and order
    /// (`Optimizer::new()`), not the 2-rule subset above: with
    /// `max_passes(1)` — e.g. `datafusion.optimizer.max_passes = 1` — the
    /// TopK is never pushed down for `Sort(fetch) -> Filter -> Left Join`,
    /// even though `PushDownFilter` does sink the filter into the scan
    /// within that same single pass. This is because `push_down_limit`
    /// (which currently owns the TopK-through-join rewrite) runs *before*
    /// `push_down_filter` in `Optimizer::new()`'s rule list, so by the time
    /// this rewrite's tree walk reaches the Filter, the Filter hasn't been
    /// sunk below the Join yet — and `push_down_limit` is never revisited
    /// again within the same pass. A later fix should let the specialized
    /// TopK-through-join transformation run after filter pushdown without
    /// moving the ordinary Limit transformation past filters (see the
    /// module doc comment).
    #[test]
    fn topk_not_pushed_with_default_optimizer_and_one_pass() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                LogicalPlanBuilder::from(t2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .filter(col("t1.c").eq(lit("foo")))?
            .sort_with_limit(vec![col("t1.b").sort(true, false)], Some(3))?
            .build()?;

        let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
        let optimizer = crate::Optimizer::new();
        let optimized = optimizer.optimize(plan, &optimizer_ctx, |_, _| {})?;

        insta::assert_snapshot!(optimized, @r#"
        Sort: t1.b ASC NULLS LAST, fetch=3
          Left Join: t1.a = t2.a
            TableScan: t1 projection=[a, b, c], full_filters=[t1.c = Utf8("foo")]
            TableScan: t2 projection=[a, b, c]
        "#);
        Ok(())
    }
}
