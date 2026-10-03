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

//! The "no new volatile evaluations" optimizer invariant.
//!
//! Several optimizer rules move a plan node below another node that *defines*
//! one of the columns it uses. To do that the rule inlines the definition of
//! that column. If the definition is volatile, the two copies give two
//! different answers and the query returns wrong results. Guards against this
//! exist per rule and per call site, and each one was added after a bug
//! report: <https://github.com/apache/datafusion/issues/24678>,
//! <https://github.com/apache/datafusion/issues/25415>,
//! <https://github.com/apache/datafusion/issues/23655>.
//!
//! This module states the same property one level up, as an invariant of the
//! whole optimizer run.
//!
//! # An end of run invariant, by design
//!
//! [`EvaluationSites::check_no_new_evaluations`] runs one time, at the end of
//! [`crate::Optimizer::optimize`], and compares the final plan against the
//! plan the optimizer was given. Intermediate plans are not checked, because
//! rules cooperate: `simplify_expressions` expands `x BETWEEN a AND b` into
//! two references to `x`, and `CommonSubexprEliminate` hoists the duplicate
//! back out in the same pass. Only the plan that the user pays for matters,
//! so only that plan is checked.
//!
//! The price is that the invariant does not name the rule at fault. That is a
//! debugging aid, not a gate, and the before and after site counts in the
//! message are enough to start from.
//!
//! # What is counted
//!
//! A *site* is one volatile [`Expr::ScalarFunction`] node. Volatile calls
//! only: two evaluations of a volatile call give two different answers, so a
//! new site is always a wrong results bug.
//!
//! A new evaluation site of an
//! [`ExpressionPlacement::KeepInPlace`](datafusion_expr_common::placement::ExpressionPlacement::KeepInPlace)
//! call is not counted, because it is not an invariant: pushing a predicate
//! through a projection that computes such an expression one time is a decided
//! trade-off, since the predicate can then reach the scan. See the decisions
//! table of <https://github.com/apache/datafusion/issues/25459>.
//!
//! Sites are keyed by a structural form of the call, in which every column is
//! reduced to its unqualified name. A rule that duplicates a call and
//! re-qualifies its columns, for example from `f(a)` to `f(test.a)`, must not
//! escape the check. Only a call that is already in the input plan is checked.
//! A rule is free to rewrite `concat_ws` into `concat`, or one Spark function
//! into two DataFusion functions. That is a new call, not a second evaluation
//! of an old one.
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
    Column, Result, internal_err,
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
};
use datafusion_expr::expr::{Exists, InSubquery, ScalarFunction, SetComparison};
use datafusion_expr::{Expr, LogicalPlan, Volatility};

/// Report a volatile call that the optimizer run made the plan evaluate at
/// more sites than the input plan does.
///
/// Off, because one shape still fails on `main`:
///
/// - `random() BETWEEN 0.0 AND 1.0` in `expr.slt`, which is
///   <https://github.com/apache/datafusion/issues/25457>, a wrong results bug
///   with a fix of its own
///
/// Turn it on once that is fixed. One other shape is worth recording here,
/// because it is what an end of run check buys: `file_row_index()` re-inlined
/// by `optimize_projections` in `file_row_index.slt`. A per rule check reports
/// it, this check does not, because `CommonSubexprEliminate` hoists the call
/// and `optimize_projections` puts it back, which leaves the final plan with
/// the site count the input plan has. The classification order that lets a
/// volatile call be inlined at all is fixed by
/// <https://github.com/apache/datafusion/pull/25456>.
const ENFORCE_NO_NEW_VOLATILE_EVALUATIONS: bool = false;

/// True while the invariant is checked.
///
/// Debug builds only. The check walks the plan two times per optimizer run,
/// which is cheap but not free, and the other plan invariants are checked
/// there too.
const fn enabled() -> bool {
    cfg!(debug_assertions) && ENFORCE_NO_NEW_VOLATILE_EVALUATIONS
}

/// The number of evaluation sites of one call, per row lineage, keyed by
/// [`structural_key`].
type Counts = HashMap<String, usize>;

/// The volatile evaluation sites of a [`LogicalPlan`].
///
/// See the [module documentation](self) for the definition.
#[derive(Debug, Default)]
pub(crate) struct EvaluationSites {
    counts: Counts,
}

impl EvaluationSites {
    /// Count the evaluation sites of `plan`.
    ///
    /// Empty, and free, while the invariant is off.
    pub(crate) fn of(plan: &LogicalPlan) -> Self {
        if !enabled() {
            return Self::default();
        }
        Self::count(plan)
    }

    /// Count the evaluation sites of `plan`, whatever the switch says.
    fn count(plan: &LogicalPlan) -> Self {
        let mut counts = Counts::new();
        count_plan(plan, &mut counts);
        Self { counts }
    }

    /// Fail if `plan` evaluates a volatile call more times than `self` does.
    ///
    /// `self` must be the count of the plan that the optimizer was given.
    pub(crate) fn check_no_new_evaluations(&self, plan: &LogicalPlan) -> Result<()> {
        if !enabled() {
            return Ok(());
        }
        self.check(plan)
    }

    /// The body of [`Self::check_no_new_evaluations`], without the switch, so
    /// that the machinery has a test while the switch is off.
    fn check(&self, plan: &LogicalPlan) -> Result<()> {
        let after = Self::count(plan);
        let mut offenders: Vec<(&String, usize, usize)> = after
            .counts
            .iter()
            .filter_map(|(key, count)| {
                // A call that the optimizer invented is not a second
                // evaluation of a call that was already there.
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
            .map(|(call, before, after)| {
                format!(
                    "volatile expression `{call}` is evaluated at {after} sites, \
                     against {before} in the input plan"
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        internal_err!(
            "Optimizer added volatile evaluation sites: {details}. Two \
             evaluations of a volatile call give two different answers, so the \
             optimized plan must not evaluate one more times than the plan it \
             was given does. Counts are per row lineage, see `EvaluationSites`"
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
            && is_site(func)
        {
            *acc.entry(structural_key(expr)).or_insert(0) += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// True while a call needs a guard.
///
/// A volatile call is a site whatever its arguments are. `random()` takes no
/// argument at all, and the constant folder does not fold it.
fn is_site(func: &ScalarFunction) -> bool {
    func.func.signature().volatility == Volatility::Volatile
}

/// The key of a call: its text, with every column reduced to its unqualified
/// name.
///
/// A rule may re-qualify the columns of a call while it moves it, so the
/// printed text of a call is not stable enough to key by. `f(a)` and
/// `f(test.a)` are the same call.
fn structural_key(expr: &Expr) -> String {
    expr.clone()
        .transform(|expr| {
            Ok(match expr {
                Expr::Column(column) => {
                    Transformed::yes(Expr::Column(Column::new_unqualified(column.name)))
                }
                other => Transformed::no(other),
            })
        })
        .map(|transformed| transformed.data.to_string())
        // The rewrite above never fails. Fall back to the printed text rather
        // than fail an invariant check for it.
        .unwrap_or_else(|_| expr.to_string())
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

    /// `leaf_udf(arg)`, made volatile. Its placement says that it may move to
    /// the leaves, and its volatility says that it may not be duplicated.
    fn volatile(arg: Expr) -> Expr {
        let udf = ScalarUDF::new_from_impl(
            PlacementTestUDF::new()
                .with_placement(ExpressionPlacement::MoveTowardsLeafNodes)
                .with_volatility(Volatility::Volatile),
        );
        udf.call(vec![arg])
    }

    /// The same call, not volatile.
    fn immutable(arg: Expr) -> Expr {
        let udf = ScalarUDF::new_from_impl(
            PlacementTestUDF::new().with_placement(ExpressionPlacement::KeepInPlace),
        );
        udf.call(vec![arg])
    }

    fn counts_of(plan: &LogicalPlan) -> Counts {
        EvaluationSites::count(plan).counts
    }

    fn sites(plan: &LogicalPlan, call: &str) -> usize {
        counts_of(plan).get(call).copied().unwrap_or_default()
    }

    #[test]
    fn counts_every_site_in_one_node() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![volatile(col("a")), volatile(col("a")).alias("b")])?
            .build()?;
        assert_eq!(sites(&plan, "leaf_udf(a)"), 2);
        Ok(())
    }

    #[test]
    fn adds_up_along_a_chain() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![volatile(col("a")).alias("x"), col("b")])?
            .filter(volatile(col("b")).eq(lit(1u32)))?
            .build()?;
        assert_eq!(sites(&plan, "leaf_udf(a)"), 1);
        assert_eq!(sites(&plan, "leaf_udf(b)"), 1);
        Ok(())
    }

    /// A `KeepInPlace` call that is not volatile is a cost, not a wrong
    /// answer. It is not an invariant and it is not counted.
    #[test]
    fn a_kept_call_is_not_a_site() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![
                immutable(col("a")).alias("x"),
                immutable(col("a")).alias("y"),
            ])?
            .build()?;
        assert!(counts_of(&plan).is_empty());
        Ok(())
    }

    /// The same predicate in both branches of a union is one evaluation for
    /// any given row, not two.
    #[test]
    fn union_branches_do_not_add_up() -> Result<()> {
        let branch = |name: &str| -> Result<LogicalPlan> {
            LogicalPlanBuilder::from(test_table_scan_with_name(name)?)
                .filter(volatile(col("a")).eq(lit(1u32)))?
                .project(vec![col("a"), col("b"), col("c")])?
                .build()
        };
        let plan = LogicalPlanBuilder::from(branch("t1")?)
            .union(branch("t2")?)?
            .build()?;
        assert_eq!(sites(&plan, "leaf_udf(a)"), 1);
        Ok(())
    }

    /// A row that enters the left input of a join never meets the nodes of the
    /// right input.
    #[test]
    fn join_sides_do_not_add_up() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .filter(volatile(col("a")).eq(lit(1u32)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
            .filter(volatile(col("a")).eq(lit(1u32)))?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_on(right, JoinType::Inner, [col("t1.b").eq(col("t2.b"))])?
            .build()?;
        assert_eq!(sites(&plan, "leaf_udf(a)"), 1);
        Ok(())
    }

    /// A subquery runs over its own rows, so it is another lineage than the
    /// input of the node that references it, not a continuation of it.
    #[test]
    fn a_subquery_is_another_lineage() -> Result<()> {
        let subquery = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
                .filter(volatile(col("a")).eq(lit(1u32)))?
                .project(vec![col("a")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .filter(volatile(col("a")).eq(lit(1u32)))?
            .filter(exists(subquery))?
            .build()?;
        assert_eq!(sites(&plan, "leaf_udf(a)"), 1);
        Ok(())
    }

    /// A call keyed by its printed text would change key when a rule
    /// re-qualifies its columns, and the duplication would be invisible.
    #[test]
    fn the_key_ignores_column_qualifiers() {
        assert_eq!(
            structural_key(&volatile(col("test.a"))),
            structural_key(&volatile(col("a")))
        );
    }

    /// Inlining the definition of a column into a node below it is the shape
    /// the invariant reports. The call is re-qualified on the way, which a key
    /// of printed text would not survive.
    #[test]
    fn reports_an_inlined_definition() -> Result<()> {
        let input = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![volatile(col("a")).alias("x"), col("b")])?
            .filter(col("x").eq(lit(1u32)))?
            .build()?;
        // What an optimizer run that pushes the filter below the projection
        // produces.
        let optimized = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(volatile(col("test.a")).eq(lit(1u32)))?
            .project(vec![volatile(col("a")).alias("x"), col("b")])?
            .build()?;

        assert_eq!(sites(&input, "leaf_udf(a)"), 1);
        assert_eq!(sites(&optimized, "leaf_udf(a)"), 2);

        let before = EvaluationSites::count(&input);
        before.check(&input)?;
        let error = before.check(&optimized).unwrap_err().to_string();
        assert!(
            error.contains(
                "volatile expression `leaf_udf(a)` is evaluated at 2 sites, \
                 against 1 in the input plan"
            ),
            "{error}"
        );
        Ok(())
    }

    /// A call that the optimizer invented is not a second evaluation of a call
    /// that was already there.
    #[test]
    fn a_new_call_is_not_a_new_site() -> Result<()> {
        let input = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a")])?
            .build()?;
        let optimized = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![volatile(col("a")).alias("a")])?
            .build()?;
        EvaluationSites::count(&input).check(&optimized)?;
        Ok(())
    }
}
