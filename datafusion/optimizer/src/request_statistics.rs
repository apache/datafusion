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

//! [`RequestStatistics`] walks the optimized logical plan once and
//! attaches a `Vec<StatisticsRequest>` to each `TableScan` describing
//! what stats the surrounding plan shape would benefit from. The
//! physical planner reads these and threads them into
//! `ScanArgs::with_statistics_requests`, where the `TableProvider`
//! decides what it can answer cheaply.
//!
//! This rule is meant to run **last** in the optimizer pipeline, after
//! every other rule has finished rewriting — that way the requests
//! reflect the plan shape the physical planner is actually going to
//! plan, not an intermediate one that a later rule reshaped.
//!
//! The rule itself never changes plan structure; it only annotates
//! `TableScan` nodes. Idempotent: running it twice yields the same
//! result.

use std::collections::{BTreeSet, HashMap, HashSet};

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result, TableReference};
use datafusion_expr::LogicalPlan;
use datafusion_expr_common::statistics::StatisticsRequest;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Optimizer rule that annotates each `TableScan` with the statistics
/// the optimizer / physical planner / table provider could benefit
/// from, derived from the surrounding plan shape.
///
/// Heuristics (one entry per relevant node):
/// - `Sort`   → `Min` / `Max` / `NullCount` on each sort key.
/// - `Filter` → `Min` / `Max` / `NullCount` / `DistinctCount` on every
///   column referenced in the predicate.
/// - `Join`   → `DistinctCount` / `NullCount` on join keys (both sides).
/// - Always   → `RowCount` per scan.
///
/// Columns are attributed back to a source `TableScan` by walking each
/// `TableScan`'s output schema; an unqualified column with a unique
/// name in the plan resolves to its source. Ambiguous names (same
/// column name in multiple TableScans) and renames through projections
/// are accepted as a known POC limitation — the worst case is "we
/// over-request" or "we miss a column", never incorrectness.
#[derive(Default, Debug)]
pub struct RequestStatistics;

impl RequestStatistics {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for RequestStatistics {
    fn name(&self) -> &str {
        "request_statistics"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // We need the whole plan to derive per-table requests, so we
        // run our own walk inside `rewrite` instead of letting the
        // framework descend.
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let requests = derive_requests(&plan);
        if requests.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let plan = plan.transform_down(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let new_requests =
                    requests.get(&scan.table_name).cloned().unwrap_or_default();
                if new_requests == scan.statistics_requests {
                    return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
                }
                let mut scan = scan;
                scan.statistics_requests = new_requests;
                Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
            } else {
                Ok(Transformed::no(node))
            }
        })?;
        Ok(plan)
    }
}

/// Walk the plan and build a `(TableReference -> Vec<StatisticsRequest>)`
/// map. The result is sorted/de-duplicated for stability.
fn derive_requests(
    plan: &LogicalPlan,
) -> HashMap<TableReference, Vec<StatisticsRequest>> {
    // Per-table accumulators. We use a BTreeSet so requests come out in
    // a stable, deterministic order regardless of plan-walk order.
    let mut acc: HashMap<TableReference, BTreeSet<RequestKey>> = HashMap::new();

    // Fallback name->table map for the rare case where an expression
    // surfaces an unqualified `Column { relation: None, .. }` after
    // analysis. By the time this rule runs (last in the pipeline), the
    // analyzer has qualified almost every column reference; if a name
    // were truly ambiguous the planner would have already errored. So
    // this map is purely defensive.
    //
    // Ambiguity is tracked explicitly: if two distinct scans project
    // the same column name (e.g. both sides of a join projecting `id`),
    // we mark the entry `None` and `resolve()` will refuse to attribute
    // unqualified refs to either side — losing some stats coverage is
    // fine (these requests are advisory) but misattribution is not.
    //
    // Note that for join keys we use *per-side* origin maps below, so
    // the global ambiguity guard isn't a coverage hazard there.
    let origin = collect_origin(plan);

    // RowCount request per scan.
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            acc.entry(scan.table_name.clone())
                .or_default()
                .insert(RequestKey::RowCount);
        }
        Ok::<_, datafusion_common::DataFusionError>(TreeNodeRecursion::Continue)
    });

    // Pass 2: walk every interesting node and add per-column requests.
    let _ = plan.apply(|node| {
        match node {
            LogicalPlan::Sort(sort) => {
                let mut cols = HashSet::new();
                for s in &sort.expr {
                    s.expr.add_column_refs(&mut cols);
                }
                for c in cols {
                    if let Some(t) = resolve(c, &origin) {
                        let entry = acc.entry(t).or_default();
                        entry.insert(RequestKey::Min(c.name.clone()));
                        entry.insert(RequestKey::Max(c.name.clone()));
                        entry.insert(RequestKey::NullCount(c.name.clone()));
                    }
                }
            }
            LogicalPlan::Filter(filter) => {
                let mut cols = HashSet::new();
                filter.predicate.add_column_refs(&mut cols);
                for c in cols {
                    if let Some(t) = resolve(c, &origin) {
                        let entry = acc.entry(t).or_default();
                        entry.insert(RequestKey::Min(c.name.clone()));
                        entry.insert(RequestKey::Max(c.name.clone()));
                        entry.insert(RequestKey::NullCount(c.name.clone()));
                        entry.insert(RequestKey::DistinctCount(c.name.clone()));
                    }
                }
            }
            LogicalPlan::Join(join) => {
                // Each join-key pair `(l_expr, r_expr)` is structurally
                // tied to a side: `l_expr`'s columns are provided by the
                // left subtree, `r_expr`'s by the right. We use per-side
                // origin maps so we ask each table only about the
                // columns it itself projects — never asking the right
                // side for a left-side key, or vice-versa, even when an
                // unqualified `Column` survives analysis.
                let left_origin = collect_origin(&join.left);
                let right_origin = collect_origin(&join.right);

                let mut add_join_key = |c: &Column,
                                        side_origin: &HashMap<
                    String,
                    Option<TableReference>,
                >| {
                    if let Some(t) = resolve(c, side_origin) {
                        let entry = acc.entry(t).or_default();
                        entry.insert(RequestKey::DistinctCount(c.name.clone()));
                        entry.insert(RequestKey::NullCount(c.name.clone()));
                    }
                };

                for (l_expr, r_expr) in &join.on {
                    let mut l_cols = HashSet::new();
                    l_expr.add_column_refs(&mut l_cols);
                    for c in l_cols {
                        add_join_key(c, &left_origin);
                    }
                    let mut r_cols = HashSet::new();
                    r_expr.add_column_refs(&mut r_cols);
                    for c in r_cols {
                        add_join_key(c, &right_origin);
                    }
                }

                // `join.filter` spans both sides; route each column by
                // which subtree's schema actually contains it. Anything
                // that doesn't belong to a single side (e.g. a literal
                // or a column from neither schema) is skipped.
                if let Some(f) = &join.filter {
                    let mut cols = HashSet::new();
                    f.add_column_refs(&mut cols);
                    for c in cols {
                        let in_left = join.left.schema().has_column(c);
                        let in_right = join.right.schema().has_column(c);
                        let side_origin = match (in_left, in_right) {
                            (true, false) => &left_origin,
                            (false, true) => &right_origin,
                            _ => continue,
                        };
                        add_join_key(c, side_origin);
                    }
                }
            }
            _ => {}
        }
        Ok::<_, datafusion_common::DataFusionError>(TreeNodeRecursion::Continue)
    });

    // Materialize: convert each table's BTreeSet of `RequestKey`s into
    // a `Vec<StatisticsRequest>`.
    acc.into_iter()
        .map(|(t, keys)| {
            let v = keys
                .into_iter()
                .map(|k| k.into_request())
                .collect::<Vec<_>>();
            (t, v)
        })
        .collect()
}

/// Walk `plan` and return a `name -> Option<TableReference>` map
/// covering every column projected by a `TableScan` reachable from
/// `plan`. `Some(t)` means the name unambiguously belongs to scan `t`
/// within this subtree; `None` means at least two distinct scans in
/// the subtree project the same name and the resolver should refuse
/// to attribute unqualified refs.
///
/// Called once for the whole plan (defensive global fallback) and
/// once per side of every Join (so join keys are scoped to the side
/// that actually provides them).
fn collect_origin(plan: &LogicalPlan) -> HashMap<String, Option<TableReference>> {
    use std::collections::hash_map::Entry;
    let mut origin: HashMap<String, Option<TableReference>> = HashMap::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            for f in scan.projected_schema.fields() {
                match origin.entry(f.name().clone()) {
                    Entry::Vacant(v) => {
                        v.insert(Some(scan.table_name.clone()));
                    }
                    Entry::Occupied(mut o) => {
                        if o.get().as_ref() != Some(&scan.table_name) {
                            // Different scan in this subtree claims the
                            // same name -> ambiguous.
                            o.insert(None);
                        }
                    }
                }
            }
        }
        Ok::<_, datafusion_common::DataFusionError>(TreeNodeRecursion::Continue)
    });
    origin
}

/// Internal de-dup key. We can't `Hash + Ord` `StatisticsRequest`
/// directly (it carries a `Column` whose `Ord` isn't necessarily
/// stable across `relation` shapes), so we project to a small enum
/// keyed only by column-name and kind.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RequestKey {
    RowCount,
    Min(String),
    Max(String),
    NullCount(String),
    DistinctCount(String),
}

impl RequestKey {
    fn into_request(self) -> StatisticsRequest {
        match self {
            RequestKey::RowCount => StatisticsRequest::RowCount,
            RequestKey::Min(c) => StatisticsRequest::Min(Column::new_unqualified(c)),
            RequestKey::Max(c) => StatisticsRequest::Max(Column::new_unqualified(c)),
            RequestKey::NullCount(c) => {
                StatisticsRequest::NullCount(Column::new_unqualified(c))
            }
            RequestKey::DistinctCount(c) => {
                StatisticsRequest::DistinctCount(Column::new_unqualified(c))
            }
        }
    }
}

fn resolve(
    c: &Column,
    origin: &HashMap<String, Option<TableReference>>,
) -> Option<TableReference> {
    // Prefer the fully-qualified ref the planner attached. Fall back to
    // the origin map only for unqualified columns, and only when the
    // name unambiguously points at a single scan.
    c.relation
        .clone()
        .or_else(|| origin.get(&c.name).cloned().flatten())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::test::test_table_scan_with_name;
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
    use datafusion_expr::{col, lit};

    fn rule_apply(plan: LogicalPlan) -> LogicalPlan {
        let cfg = OptimizerContext::new();
        RequestStatistics::new()
            .rewrite(plan, &cfg)
            .expect("rule succeeded")
            .data
    }

    /// Pick the requests off the (single) `TableScan` in `plan`.
    fn requests_for(plan: &LogicalPlan) -> Vec<StatisticsRequest> {
        let mut out = Vec::new();
        let _ = plan.apply(|n| {
            if let LogicalPlan::TableScan(s) = n {
                out = s.statistics_requests.clone();
            }
            Ok::<_, datafusion_common::DataFusionError>(TreeNodeRecursion::Continue)
        });
        out
    }

    fn has_min(rs: &[StatisticsRequest], col: &str) -> bool {
        rs.iter()
            .any(|r| matches!(r, StatisticsRequest::Min(c) if c.name == col))
    }
    fn has_distinct(rs: &[StatisticsRequest], col: &str) -> bool {
        rs.iter()
            .any(|r| matches!(r, StatisticsRequest::DistinctCount(c) if c.name == col))
    }
    fn has_row_count(rs: &[StatisticsRequest]) -> bool {
        rs.iter().any(|r| matches!(r, StatisticsRequest::RowCount))
    }

    #[test]
    fn always_requests_row_count_per_scan() {
        let scan = test_table_scan_with_name("t").unwrap();
        let plan = LogicalPlanBuilder::from(scan).build().unwrap();
        let rs = requests_for(&rule_apply(plan));
        assert!(has_row_count(&rs), "expected RowCount, got {rs:?}");
    }

    #[test]
    fn filter_columns_request_min_max_null_distinct() {
        let scan = test_table_scan_with_name("t").unwrap();
        let plan = LogicalPlanBuilder::from(scan)
            .filter(col("a").gt(lit(5_i32)))
            .unwrap()
            .build()
            .unwrap();
        let rs = requests_for(&rule_apply(plan));
        assert!(has_min(&rs, "a"), "expected Min(a), got {rs:?}");
        assert!(
            has_distinct(&rs, "a"),
            "expected DistinctCount(a), got {rs:?}"
        );
    }

    #[test]
    fn join_keys_request_distinct_count() {
        let l = test_table_scan_with_name("l").unwrap();
        let r = test_table_scan_with_name("r").unwrap();
        let plan = LogicalPlanBuilder::from(l)
            .join_on(
                r,
                datafusion_expr::JoinType::Inner,
                vec![col("l.a").eq(col("r.a"))],
            )
            .unwrap()
            .build()
            .unwrap();
        // Walk both TableScans.
        let mut all = Vec::new();
        let _ = plan.apply(|n| {
            if let LogicalPlan::TableScan(s) = n {
                all.push((s.table_name.to_string(), s.statistics_requests.clone()));
            }
            Ok::<_, datafusion_common::DataFusionError>(TreeNodeRecursion::Continue)
        });
        let plan = rule_apply(plan);
        let mut by_name: HashMap<String, Vec<StatisticsRequest>> = HashMap::new();
        let _ = plan.apply(|n| {
            if let LogicalPlan::TableScan(s) = n {
                by_name.insert(s.table_name.to_string(), s.statistics_requests.clone());
            }
            Ok::<_, datafusion_common::DataFusionError>(TreeNodeRecursion::Continue)
        });
        for (name, rs) in &by_name {
            assert!(
                has_distinct(rs, "a"),
                "{name}: expected DistinctCount(a), got {rs:?}"
            );
        }
    }

    /// Symmetric join keys with the same column name on both sides
    /// (`t1.a = t2.a`). Each table should be asked exactly once for
    /// `DistinctCount(a)` / `NullCount(a)`, attributed to itself — not
    /// to the other side, and not duplicated.
    #[test]
    fn join_on_same_name_routes_per_side_without_dupes() {
        let l = test_table_scan_with_name("t1").unwrap();
        let r = test_table_scan_with_name("t2").unwrap();
        let plan = LogicalPlanBuilder::from(l)
            .join_on(
                r,
                datafusion_expr::JoinType::Inner,
                vec![col("t1.a").eq(col("t2.a"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let plan = rule_apply(plan);

        let mut by_name: HashMap<String, Vec<StatisticsRequest>> = HashMap::new();
        let _ = plan.apply(|n| {
            if let LogicalPlan::TableScan(s) = n {
                by_name.insert(s.table_name.to_string(), s.statistics_requests.clone());
            }
            Ok::<_, datafusion_common::DataFusionError>(TreeNodeRecursion::Continue)
        });

        // Both scans must be present.
        assert!(by_name.contains_key("t1"));
        assert!(by_name.contains_key("t2"));

        // Each side gets DistinctCount(a) + NullCount(a) attributed to
        // itself. The Column carried on the request itself is
        // unqualified by design (the request is already keyed under the
        // owning TableScan), so we count by (kind, name).
        for (name, rs) in &by_name {
            let distinct_a = rs
                .iter()
                .filter(
                    |r| matches!(r, StatisticsRequest::DistinctCount(c) if c.name == "a"),
                )
                .count();
            let null_a = rs
                .iter()
                .filter(|r| matches!(r, StatisticsRequest::NullCount(c) if c.name == "a"))
                .count();
            assert_eq!(distinct_a, 1, "{name}: DistinctCount(a) count, got {rs:?}");
            assert_eq!(null_a, 1, "{name}: NullCount(a) count, got {rs:?}");

            // Sanity: we should not be asking the same scan about the
            // other side's columns. With identical schemas (`a`, `b`,
            // `c`) on both scans this is the strict check that proves
            // join keys did not bleed across sides — the only `a`-keyed
            // request must be from this side's own key.
            let total_a = rs
                .iter()
                .filter(|r| match r {
                    StatisticsRequest::DistinctCount(c)
                    | StatisticsRequest::NullCount(c)
                    | StatisticsRequest::Min(c)
                    | StatisticsRequest::Max(c)
                    | StatisticsRequest::Sum(c)
                    | StatisticsRequest::ByteSize(c) => c.name == "a",
                    _ => false,
                })
                .count();
            assert_eq!(
                total_a, 2,
                "{name}: expected exactly DistinctCount(a)+NullCount(a), got {rs:?}"
            );

            // No requests on `b` or `c` — there's no other plan node
            // referencing them, so the only way they'd appear is a
            // bug in routing.
            for col in ["b", "c"] {
                let any = rs.iter().any(|r| match r {
                    StatisticsRequest::DistinctCount(c)
                    | StatisticsRequest::NullCount(c)
                    | StatisticsRequest::Min(c)
                    | StatisticsRequest::Max(c)
                    | StatisticsRequest::Sum(c)
                    | StatisticsRequest::ByteSize(c) => c.name == col,
                    _ => false,
                });
                assert!(!any, "{name}: unexpected request on `{col}`, got {rs:?}");
            }
        }
    }

    /// Asymmetric join keys: `l.a = r.b`. Left table should be asked
    /// about `a`, right table about `b` — neither side gets the other
    /// side's column.
    #[test]
    fn asymmetric_join_keys_attribute_per_side() {
        let l = test_table_scan_with_name("l").unwrap();
        let r = test_table_scan_with_name("r").unwrap();
        let plan = LogicalPlanBuilder::from(l)
            .join_on(
                r,
                datafusion_expr::JoinType::Inner,
                vec![col("l.a").eq(col("r.b"))],
            )
            .unwrap()
            .build()
            .unwrap();
        let plan = rule_apply(plan);
        let mut by_name: HashMap<String, Vec<StatisticsRequest>> = HashMap::new();
        let _ = plan.apply(|n| {
            if let LogicalPlan::TableScan(s) = n {
                by_name.insert(s.table_name.to_string(), s.statistics_requests.clone());
            }
            Ok::<_, datafusion_common::DataFusionError>(TreeNodeRecursion::Continue)
        });
        let l_rs = &by_name["l"];
        let r_rs = &by_name["r"];
        assert!(
            has_distinct(l_rs, "a"),
            "l: expected DistinctCount(a), got {l_rs:?}"
        );
        assert!(
            !has_distinct(l_rs, "b"),
            "l: must NOT have DistinctCount(b), got {l_rs:?}"
        );
        assert!(
            has_distinct(r_rs, "b"),
            "r: expected DistinctCount(b), got {r_rs:?}"
        );
        assert!(
            !has_distinct(r_rs, "a"),
            "r: must NOT have DistinctCount(a), got {r_rs:?}"
        );
    }

    /// Direct test of the origin map's ambiguity guard: when two scans
    /// project the same column name, an unqualified column reference
    /// must NOT resolve to either side. (In real plans the planner
    /// qualifies refs and this fallback rarely fires, but we still want
    /// it to fail closed.)
    #[test]
    fn ambiguous_unqualified_column_does_not_misattribute() {
        let mut origin: HashMap<String, Option<TableReference>> = HashMap::new();
        origin.insert("id".to_string(), Some(TableReference::bare("users")));
        // Second scan projects same name -> mark ambiguous.
        let entry = origin.get_mut("id").unwrap();
        if entry.as_ref() != Some(&TableReference::bare("orders")) {
            *entry = None;
        }
        let unqualified = Column::new_unqualified("id");
        assert_eq!(resolve(&unqualified, &origin), None);

        // A qualified ref still resolves directly via Column.relation,
        // bypassing the ambiguous origin entry.
        let qualified = Column::new(Some(TableReference::bare("users")), "id");
        assert_eq!(
            resolve(&qualified, &origin),
            Some(TableReference::bare("users"))
        );
    }
}
