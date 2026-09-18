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

//! The "no new evaluations" optimizer invariant.
//!
//! Several optimizer rules move a plan node below another node that *defines*
//! one of the columns it uses. To do that the rule inlines the definition of
//! that column. If the definition is volatile, the two copies disagree and the
//! query returns wrong results. If the definition is expensive, the work is
//! done two times.
//!
//! Guards against this exist per rule and per call site. Each one was added
//! after a bug report:
//!
//! - `would_duplicate_volatile` in `extract_leaf_expressions`
//!   (<https://github.com/apache/datafusion/issues/24678>)
//! - the volatile checks in the `Projection` and `Aggregate` branches of
//!   `push_down_filter` (<https://github.com/apache/datafusion/issues/25415>)
//! - `merge_would_duplicate_kept_expr` in `extract_leaf_expressions`
//!   (<https://github.com/apache/datafusion/issues/23655>)
//!
//! This module checks the same property one level up, as a plan invariant that
//! holds for every rule. [`EvaluationSites::check_no_new_evaluations`] runs
//! after each optimizer rule, next to the schema invariant.
//!
//! # What is counted
//!
//! A *site* is one [`Expr::ScalarFunction`] node that is
//!
//! - volatile, or
//! - has placement [`ExpressionPlacement::KeepInPlace`],
//!
//! A call that is not volatile and that takes no argument, or only literal
//! arguments, is not a site. It is the same value for the whole query, and the
//! constant folder evaluates it one time whatever the plan looks like.
//!
//! The placement of a call is asked for with every argument reduced to a
//! literal or to a column, so that the class of a call does not depend on what
//! its arguments cost. A nested call is a site of its own.
//!
//! Sites are keyed by the text of the call, and only a call that is already in
//! the plan before the rule is checked. A rule is free to rewrite `concat_ws`
//! into `concat`, or one Spark function into two DataFusion functions. That is
//! a new call, not a second evaluation of an old one.
//!
//! # Row lineage
//!
//! A whole plan count has false positives: a predicate is legitimately
//! duplicated across the branches of a `Union` and across the two sides of a
//! join. So the count is taken per *row lineage*: the number of times a call
//! is evaluated on the way from one leaf to the root.
//!
//! ```text
//! sites(node) = own_sites(node) + max over inputs i of sites(i)
//! ```
//!
//! The maximum, not the sum, because a row that enters the left input of a
//! join never meets the nodes of the right input.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::{
    Result, internal_err,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_expr::expr::{Exists, InSubquery, ScalarFunction, SetComparison};
use datafusion_expr::{Expr, LogicalPlan, Volatility};
use datafusion_expr_common::placement::ExpressionPlacement;

/// Report a new evaluation site of a volatile call as an error.
///
/// A second evaluation of a volatile call gives a second answer, so this is
/// always a wrong results bug. Off because two shapes still fail on `main`:
///
/// - <https://github.com/apache/datafusion/issues/25457>, `BETWEEN` over a
///   volatile operand
/// - `optimize_projections` inlining a `CommonSubexprEliminate` column
///
/// Turn it on once both are fixed. The check costs about 12% of the CPU of a
/// sqllogictest run, so it is not free.
const CHECK_VOLATILE_EVALUATIONS: bool = false;

/// Report a new evaluation site of an [`ExpressionPlacement::KeepInPlace`]
/// call as an error.
///
/// Off, because the rules still add sites on `main`. The pull request that
/// added this file lists the shapes that the sqllogictest suite reports. One
/// of them is an open bug:
///
/// - <https://github.com/apache/datafusion/issues/25329>
/// - <https://github.com/apache/datafusion/issues/23655>
///
/// In the others a rule trades one more evaluation for a chance to prune at
/// the scan. Turn this on once every shape has a decision.
const CHECK_KEPT_EVALUATIONS: bool = false;

/// Why a call is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum SiteKind {
    /// The function is volatile. Two evaluations give two different answers.
    Volatile,
    /// The function has placement [`ExpressionPlacement::KeepInPlace`]. Two
    /// evaluations give the same answer, at two times the cost.
    Kept,
}

impl SiteKind {
    fn enabled(self) -> bool {
        match self {
            SiteKind::Volatile => CHECK_VOLATILE_EVALUATIONS,
            SiteKind::Kept => CHECK_KEPT_EVALUATIONS,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            SiteKind::Volatile => "volatile",
            SiteKind::Kept => "KeepInPlace",
        }
    }
}

/// The number of evaluation sites of one call, per row lineage.
type Counts = HashMap<(SiteKind, String), usize>;

/// The evaluation sites of a [`LogicalPlan`].
///
/// See the [module documentation](self) for the definition.
#[derive(Debug, Default)]
pub(crate) struct EvaluationSites {
    counts: Counts,
}

impl EvaluationSites {
    /// Count the evaluation sites of `plan`.
    pub(crate) fn of(plan: &LogicalPlan) -> Self {
        let mut counts = Counts::new();
        if Self::is_enabled() {
            count_plan(plan, &mut counts);
        }
        Self { counts }
    }

    /// True while at least one kind of site is checked.
    fn is_enabled() -> bool {
        CHECK_VOLATILE_EVALUATIONS || CHECK_KEPT_EVALUATIONS
    }

    /// Fail if `plan` evaluates a call more times than `self` does.
    ///
    /// `self` must be the count of the plan before the rule ran.
    pub(crate) fn check_no_new_evaluations(&self, plan: &LogicalPlan) -> Result<()> {
        if !Self::is_enabled() {
            return Ok(());
        }
        let after = Self::of(plan);
        let mut offenders: Vec<(&(SiteKind, String), usize, usize)> = after
            .counts
            .iter()
            .filter_map(|(key, count)| {
                if !key.0.enabled() {
                    return None;
                }
                // A call that the rule invented is not a second evaluation of
                // a call that was already there.
                let before = *self.counts.get(key)?;
                (*count > before).then_some((key, before, *count))
            })
            .collect();
        if offenders.is_empty() {
            return Ok(());
        }
        // Sorted so that the message does not depend on the hash order.
        offenders.sort();

        let details = offenders
            .iter()
            .map(|((kind, call), before, after)| {
                format!(
                    "{} expression `{call}` is evaluated at {after} sites, \
                     against {before} before the rule",
                    kind.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        internal_err!(
            "Optimizer rule added evaluation sites: {details}. A rule must not \
             make a query evaluate a volatile or an expensive expression more \
             times than the plan it was given does. Counts are per row \
             lineage, see `EvaluationSites`"
        )
    }
}

/// `own_sites(plan) + max over inputs of count_plan(input)`.
fn count_plan(plan: &LogicalPlan, acc: &mut Counts) {
    count_own(plan, acc);

    let mut branch_max = Counts::new();
    let mut branch = Counts::new();
    for input in plan.inputs() {
        merge_branch(input, &mut branch, &mut branch_max);
    }
    // A subquery runs over its own rows, so it is another lineage, not a
    // continuation of this one.
    for subquery in subquery_plans(plan) {
        merge_branch(&subquery, &mut branch, &mut branch_max);
    }

    for (key, count) in branch_max {
        *acc.entry(key).or_insert(0) += count;
    }
}

/// Count `branch_plan` into `branch`, then keep the elementwise maximum in
/// `branch_max`. `branch` is reused to save an allocation.
fn merge_branch(branch_plan: &LogicalPlan, branch: &mut Counts, branch_max: &mut Counts) {
    branch.clear();
    count_plan(branch_plan, branch);
    for (key, count) in branch.drain() {
        let slot = branch_max.entry(key).or_insert(0);
        *slot = (*slot).max(count);
    }
}

/// The evaluation sites in the expressions of `plan` itself.
fn count_own(plan: &LogicalPlan, acc: &mut Counts) {
    // `TableScan::filters` is a copy of a predicate that the source evaluates.
    // Unless the source reports `Exact` support the `Filter` node stays above
    // the scan, so counting both would report every filter pushdown.
    if matches!(plan, LogicalPlan::TableScan(_)) {
        return;
    }
    let _ = plan.apply_expressions(|expr| {
        count_expr(expr, acc);
        Ok(TreeNodeRecursion::Continue)
    });
}

fn count_expr(expr: &Expr, acc: &mut Counts) {
    let _ = expr.apply(|expr| {
        if let Expr::ScalarFunction(func) = expr
            && let Some(kind) = site_kind(func)
        {
            *acc.entry((kind, expr.to_string())).or_insert(0) += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// Classify one call, or `None` if it does not need a guard.
fn site_kind(func: &ScalarFunction) -> Option<SiteKind> {
    // A volatile call is never constant, whatever its arguments are. `random()`
    // takes no argument at all.
    if func.func.signature().volatility == Volatility::Volatile {
        return Some(SiteKind::Volatile);
    }
    // Every argument is reduced to a literal or to a column. A nested call is
    // a site of its own and is counted on its own, so the cost of the
    // arguments must not change the class of the parent. Without this,
    // inlining `f(x)` into `get_field(c, 'k')` would turn a
    // `MoveTowardsLeafNodes` `get_field` into a `KeepInPlace` one and report a
    // duplication that did not happen.
    let arg_placements: Vec<_> = func
        .args
        .iter()
        .map(|arg| match arg.placement() {
            ExpressionPlacement::Literal => ExpressionPlacement::Literal,
            _ => ExpressionPlacement::Column,
        })
        .collect();
    // A call with no argument, or on literals only, does not depend on the
    // row. It is the same value for the whole query.
    if arg_placements
        .iter()
        .all(|p| *p == ExpressionPlacement::Literal)
    {
        return None;
    }
    (func.func.placement(&arg_placements) == ExpressionPlacement::KeepInPlace)
        .then_some(SiteKind::Kept)
}

/// The plans of the subqueries referenced by the expressions of `plan`.
fn subquery_plans(plan: &LogicalPlan) -> Vec<Arc<LogicalPlan>> {
    let mut plans = Vec::new();
    let _ = plan.apply_expressions(|expr| {
        let _ = expr.apply(|expr| {
            match expr {
                Expr::Exists(Exists { subquery, .. })
                | Expr::InSubquery(InSubquery { subquery, .. })
                | Expr::SetComparison(SetComparison { subquery, .. })
                | Expr::ScalarSubquery(subquery) => {
                    plans.push(Arc::clone(&subquery.subquery));
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(TreeNodeRecursion::Continue)
    });
    plans
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::udfs::PlacementTestUDF;
    use crate::test::{test_table_scan, test_table_scan_with_name};
    use datafusion_common::Result;
    use datafusion_expr::{
        ExpressionPlacement, JoinType, LogicalPlanBuilder, ScalarUDF, col, exists, lit,
    };

    /// `keep_in_place_udf(arg)`, the shape the invariant is about.
    fn kept(arg: Expr) -> Expr {
        let udf = ScalarUDF::new_from_impl(
            PlacementTestUDF::new().with_placement(ExpressionPlacement::KeepInPlace),
        );
        udf.call(vec![arg])
    }

    /// The same call, made volatile.
    fn volatile(arg: Expr) -> Expr {
        let udf = ScalarUDF::new_from_impl(
            PlacementTestUDF::new()
                .with_placement(ExpressionPlacement::KeepInPlace)
                .with_volatility(Volatility::Volatile),
        );
        udf.call(vec![arg])
    }

    fn counts_of(plan: &LogicalPlan) -> Counts {
        let mut counts = Counts::new();
        count_plan(plan, &mut counts);
        counts
    }

    fn sites(plan: &LogicalPlan, kind: SiteKind, call: &str) -> usize {
        counts_of(plan)
            .get(&(kind, call.to_string()))
            .copied()
            .unwrap_or_default()
    }

    #[test]
    fn counts_every_site_in_one_node() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![kept(col("a")), kept(col("a")).alias("b")])?
            .build()?;
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(test.a)"), 2);
        Ok(())
    }

    #[test]
    fn adds_up_along_a_chain() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![kept(col("a")).alias("x"), col("b")])?
            .filter(kept(col("b")).eq(lit(1u32)))?
            .build()?;
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(test.a)"), 1);
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(test.b)"), 1);
        Ok(())
    }

    /// The same predicate in both branches of a union is one evaluation for
    /// any given row, not two.
    #[test]
    fn union_branches_do_not_add_up() -> Result<()> {
        let branch = |name: &str| -> Result<LogicalPlan> {
            LogicalPlanBuilder::from(test_table_scan_with_name(name)?)
                .filter(kept(col("a")).eq(lit(1u32)))?
                .project(vec![col("a"), col("b"), col("c")])?
                .build()
        };
        let plan = LogicalPlanBuilder::from(branch("t1")?)
            .union(branch("t2")?)?
            .build()?;
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(t1.a)"), 1);
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(t2.a)"), 1);
        Ok(())
    }

    /// A row that enters the left input of a join never meets the nodes of the
    /// right input.
    #[test]
    fn join_sides_do_not_add_up() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .filter(kept(col("a")).eq(lit(1u32)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
            .filter(kept(col("a")).eq(lit(1u32)))?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_on(right, JoinType::Inner, [col("t1.b").eq(col("t2.b"))])?
            .build()?;
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(t1.a)"), 1);
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(t2.a)"), 1);
        Ok(())
    }

    /// Inlining the definition of a column into a node below it is the shape
    /// the invariant reports.
    #[test]
    fn reports_an_inlined_definition() -> Result<()> {
        let before = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![volatile(col("a")).alias("x"), col("b")])?
            .filter(col("x").eq(lit(1u32)))?
            .build()?;
        // What a rule that pushes the filter below the projection produces.
        let after = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(volatile(col("a")).eq(lit(1u32)))?
            .project(vec![volatile(col("a")).alias("x"), col("b")])?
            .build()?;

        let key = (SiteKind::Volatile, "keep_in_place_udf(test.a)".to_string());
        assert_eq!(counts_of(&before).get(&key), Some(&1));
        assert_eq!(counts_of(&after).get(&key), Some(&2));
        Ok(())
    }

    /// A call on literals only is one evaluation for the whole query.
    #[test]
    fn a_constant_call_is_not_a_site() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![kept(lit(1u32)).alias("x"), kept(lit(1u32)).alias("y")])?
            .build()?;
        assert!(counts_of(&plan).is_empty());
        Ok(())
    }

    /// The class of a call must not change when a rule inlines a costly
    /// argument into it.
    #[test]
    fn the_class_of_a_call_ignores_its_arguments() -> Result<()> {
        let leaf = ScalarUDF::new_from_impl(
            PlacementTestUDF::new()
                .with_placement(ExpressionPlacement::MoveTowardsLeafNodes),
        );
        let over_column = leaf.call(vec![col("a")]);
        let over_call = leaf.call(vec![kept(col("a"))]);
        let Expr::ScalarFunction(over_column) = &over_column else {
            unreachable!()
        };
        let Expr::ScalarFunction(over_call) = &over_call else {
            unreachable!()
        };
        assert_eq!(site_kind(over_column), None);
        assert_eq!(site_kind(over_call), None);
        Ok(())
    }

    /// A subquery runs over its own rows.
    #[test]
    fn a_subquery_is_another_lineage() -> Result<()> {
        let subquery = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
                .filter(kept(col("a")).eq(lit(1u32)))?
                .project(vec![col("a")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .filter(kept(col("a")).eq(lit(1u32)).and(exists(subquery)))?
            .build()?;
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(t1.a)"), 1);
        assert_eq!(sites(&plan, SiteKind::Kept, "keep_in_place_udf(t2.a)"), 1);
        Ok(())
    }
}
