// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`LimitPushdown`] pushes `LIMIT` down through `ExecutionPlan`s to reduce
//! data transfer as much as possible.
//!
//! # Plan Limit Absorption
//! In addition to pushing down `GlobalLimitExec` and `LocalLimitExec` nodes in
//! the plan, some operators can "absorb" a limit and stop early during
//! execution.
//!
//! ## Background: vectorized volcano execution model
//! DataFusion uses a batched volcano model. For most operators, output is
//! produced in batches of `datafusion.execution.batch_size` (default 8192), so
//! the batch sizes typically look like:
//! ```text
//! 8192, 8192, ..., 8192, 100 (the final batch may be partial)
//! ```
//!
//! ## Example
//! For a join with an expensive, selective predicate:
//! ```text
//! GlobalLimitExec: skip=0, fetch=10
//! -- NestedLoopJoinExec(on=expr_expensive_and_selective)
//! --- DataSourceExec()
//! --- DataSourceExec()
//! ```
//!
//! Under this model, `NestedLoopJoinExec` would keep working until it can emit
//! a full batch (8192 rows), even though the query only needs 10. If the limit
//! cannot be pushed below the join, we can still embed it inside the join so it
//! stops once the limit is satisfied. The transformed plan looks like:
//!
//! ```text
//! NestedLoopJoinExec(on=expr_expensive_and_selective, fetch=10)
//! --- DataSourceExec()
//! --- DataSourceExec()
//! ```
//!
//! ## Implementation
//! The current optimizer rule optionally pushes `fetch` requirements into
//! operators via [`ExecutionPlan::with_fetch`].
//!
//! To support early termination in operators, [`LimitedBatchCoalescer`](https://docs.rs/datafusion/latest/datafusion/physical_plan/coalesce/struct.LimitedBatchCoalescer.html)
//! can help manage the output buffer.
//!
//! Reference implementation in Hash Join: <https://github.com/apache/datafusion/pull/20228>

use std::fmt::Debug;
use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use crate::optimizer::{
    ConfigOnlyContext, PhysicalOptimizerContext, with_statistics_context,
};
use crate::row_bound::{RowBound, limit_node, place};

use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties, LimitBehavior};
/// This rule inspects [`ExecutionPlan`]'s and pushes down the fetch limit from
/// the parent to the child if applicable.
#[derive(Default, Debug)]
pub struct LimitPushdown {}

impl LimitPushdown {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for LimitPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(plan, &ConfigOnlyContext::new(config))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        with_statistics_context(context, |stats_ctx| {
            push_limits(plan, Above::Nothing, stats_ctx)
        })
    }

    fn name(&self) -> &str {
        "LimitPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The row bound a node receives from its ancestors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Above {
    /// No bound.
    Nothing,
    /// `OFFSET skip LIMIT fetch` that no ancestor enforces yet: it must be
    /// placed at or above the first node it can't pass.
    Open(RowBound),
    /// A bound an ancestor already enforces, with its `skip` applied. A node
    /// below may stop after `fetch` rows; nothing else is required of it. A
    /// limit node further down is still folded in and placed again.
    Enforced(Option<usize>),
}

impl Above {
    fn open(bound: RowBound) -> Self {
        if bound == RowBound::default() {
            Above::Nothing
        } else {
            Above::Open(bound)
        }
    }

    fn bound(self) -> RowBound {
        match self {
            Above::Nothing => RowBound::default(),
            Above::Open(bound) => bound,
            Above::Enforced(fetch) => RowBound::fetch(fetch),
        }
    }

    /// `self` above a node that enforces its own `fetch` (a TopK, a scan with
    /// a row limit, ...). The node enforces the combined bound unless rows
    /// still have to be skipped above it.
    fn above_fetching_node(self, fetch: usize) -> Self {
        let bound = self.bound().over(RowBound::fetch(Some(fetch)));
        if bound.skip > 0 {
            Above::Open(bound)
        } else {
            Above::Enforced(bound.fetch)
        }
    }
}

/// What a node does with a bound from its parent.
enum Step {
    /// A limit node: its bound is folded in and the node removed.
    Limit(RowBound),
    /// Passes the bound to every child, and drops it here. Below a node with
    /// several output partitions the bound applies to each partition, so this
    /// keeps the same rows only when each output partition is one input
    /// partition (`UnionExec`, `ProjectionExec`).
    Pass,
    /// Combines partitions, so it sees every row and can enforce the whole
    /// bound itself.
    Merge,
    /// The bound can't go below this node.
    Stop,
}

fn step(plan: &Arc<dyn ExecutionPlan>) -> Step {
    match plan.limit_behavior() {
        LimitBehavior::Limit { skip, fetch } => Step::Limit(RowBound { skip, fetch }),
        // Below a node whose children have more partitions than it outputs,
        // the bound would apply to each of them and keep too many rows. The
        // node takes the bound like a merge: it stays above, and each child
        // gets a copy.
        LimitBehavior::CopyToChildren | LimitBehavior::MoveToInput
            if combines_partitions(plan) =>
        {
            Step::Merge
        }
        LimitBehavior::CopyToChildren | LimitBehavior::MoveToInput => Step::Pass,
        LimitBehavior::Merge => Step::Merge,
        // LimitPushPastWindows handles windows.
        LimitBehavior::Stops | LimitBehavior::Window(_) => Step::Stop,
    }
}

/// Whether the children of `plan` have more partitions in total than it
/// outputs.
fn combines_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let input_partitions: usize = plan
        .children()
        .iter()
        .map(|child| child.output_partitioning().partition_count())
        .sum();
    input_partitions > plan.output_partitioning().partition_count()
}

/// Pushes the bounds of all limit nodes in `plan` down as far as they pass,
/// and writes each one once, at the first node that can enforce it:
///
/// - a merge, which takes it as its own fetch;
/// - otherwise the first node the bound can't pass, which takes it as its
///   fetch or gets a limit node on top.
///
/// Below the place a bound is enforced, nodes that can stop early are given
/// it as a fetch too. Operators can't skip rows, so a bound with an `OFFSET`
/// always keeps a [`GlobalLimitExec`] where it is enforced.
///
/// [`GlobalLimitExec`]: datafusion_physical_plan::limit::GlobalLimitExec
fn push_limits(
    mut plan: Arc<dyn ExecutionPlan>,
    mut above: Above,
    stats_ctx: &StatisticsContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    while let Some((input, folded)) = fold_limit(&plan, above, stats_ctx)? {
        (plan, above) = (input, folded);
    }
    let (plan, below) = apply_bound(plan, above);

    let children = plan.children();
    let mut changed = false;
    let new_children = children
        .into_iter()
        .map(|child| {
            let new_child = push_limits(Arc::clone(child), below, stats_ctx)?;
            changed |= !Arc::ptr_eq(child, &new_child);
            Ok(new_child)
        })
        .collect::<Result<_>>()?;
    if changed {
        replace_children_if_necessary(plan, new_children)
    } else {
        Ok(plan)
    }
}

/// If `plan` is a limit node, returns its input and `above` combined with the
/// limit's bound. Otherwise returns `None`.
fn fold_limit(
    plan: &Arc<dyn ExecutionPlan>,
    above: Above,
    stats_ctx: &StatisticsContext,
) -> Result<Option<(Arc<dyn ExecutionPlan>, Above)>> {
    let Step::Limit(limit) = step(plan) else {
        return Ok(None);
    };
    let input = Arc::clone(plan.children()[0]);
    let bound = above.bound().over(limit);
    // Exact statistics may prove the input already has few enough rows. The
    // bound is kept so a nested limit can still be combined with it.
    let above = if limit_satisfied_by_input(&input, bound, stats_ctx)? {
        Above::Enforced(bound.fetch)
    } else {
        Above::open(bound)
    };
    Ok(Some((input, above)))
}

/// Applies `above` to `plan`, which is not a limit node. Returns the new plan
/// and the bound its children receive.
fn apply_bound(
    plan: Arc<dyn ExecutionPlan>,
    above: Above,
) -> (Arc<dyn ExecutionPlan>, Above) {
    let above = match plan.fetch() {
        Some(fetch) => above.above_fetching_node(fetch),
        None => above,
    };
    match (step(&plan), above) {
        (_, Above::Nothing | Above::Enforced(None)) => (plan, Above::Nothing),
        // Only an OFFSET: it is placed here, operators can't skip.
        (_, Above::Open(bound)) if bound.fetch.is_none() => {
            (limit_node(plan, bound), Above::Nothing)
        }
        (Step::Limit(_), _) => unreachable!("limit nodes are folded before apply_bound"),
        (Step::Pass, above) => (plan, above),
        (Step::Merge, Above::Open(bound)) => {
            (place(plan, bound), Above::Enforced(bound.input_rows()))
        }
        (Step::Merge, Above::Enforced(fetch)) => (
            plan.with_fetch(fetch).unwrap_or(plan),
            Above::Enforced(fetch),
        ),
        (Step::Stop, Above::Open(bound)) => (place(plan, bound), Above::Nothing),
        (Step::Stop, Above::Enforced(fetch)) => {
            (plan.with_fetch(fetch).unwrap_or(plan), Above::Nothing)
        }
    }
}

/// The state [`pushdown_limit_helper`] carries from one node to the next.
#[deprecated(
    since = "56.0.0",
    note = "LimitPushdown no longer works one node at a time; run the `LimitPushdown` rule"
)]
#[derive(Default, Clone, Debug)]
pub struct GlobalRequirements {
    fetch: Option<usize>,
    skip: usize,
    satisfied: bool,
}

#[expect(deprecated)]
impl From<GlobalRequirements> for Above {
    fn from(state: GlobalRequirements) -> Self {
        let bound = RowBound {
            skip: state.skip,
            fetch: state.fetch,
        };
        if state.satisfied && bound.skip == 0 {
            Above::Enforced(bound.fetch)
        } else {
            Above::open(bound)
        }
    }
}

#[expect(deprecated)]
impl From<Above> for GlobalRequirements {
    fn from(above: Above) -> Self {
        match above {
            Above::Nothing => GlobalRequirements::default(),
            Above::Open(bound) => GlobalRequirements {
                fetch: bound.fetch,
                skip: bound.skip,
                satisfied: false,
            },
            Above::Enforced(fetch) => GlobalRequirements {
                fetch,
                skip: 0,
                satisfied: true,
            },
        }
    }
}

/// Applies the [`LimitPushdown`] rule to one node. A limit node is removed and
/// [`TreeNodeRecursion::Stop`] is returned; call again on its input.
///
/// Computes statistics with a new [`StatisticsContext`] that has no statistics
/// providers.
#[deprecated(
    since = "56.0.0",
    note = "LimitPushdown no longer works one node at a time; run the `LimitPushdown` rule"
)]
#[expect(deprecated)]
pub fn pushdown_limit_helper(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    global_state: GlobalRequirements,
) -> Result<(Transformed<Arc<dyn ExecutionPlan>>, GlobalRequirements)> {
    pushdown_limit_helper_with_stats(
        pushdown_plan,
        global_state,
        &StatisticsContext::new(),
    )
}

/// Like [`pushdown_limit_helper`], but computes statistics with `stats_ctx`.
#[deprecated(
    since = "56.0.0",
    note = "LimitPushdown no longer works one node at a time; run the `LimitPushdown` rule"
)]
#[expect(deprecated)]
pub fn pushdown_limit_helper_with_stats(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    global_state: GlobalRequirements,
    stats_ctx: &StatisticsContext,
) -> Result<(Transformed<Arc<dyn ExecutionPlan>>, GlobalRequirements)> {
    let above = Above::from(global_state);
    if let Some((input, above)) = fold_limit(&pushdown_plan, above, stats_ctx)? {
        return Ok((
            Transformed::new(input, true, TreeNodeRecursion::Stop),
            above.into(),
        ));
    }
    let original = Arc::clone(&pushdown_plan);
    let (plan, below) = apply_bound(pushdown_plan, above);
    let transformed = !Arc::ptr_eq(&original, &plan);
    Ok((
        Transformed::new_transformed(plan, transformed),
        below.into(),
    ))
}

/// Returns true if exact input statistics prove that applying the limit would
/// not remove any rows.
fn limit_satisfied_by_input(
    plan: &Arc<dyn ExecutionPlan>,
    bound: RowBound,
    stats_ctx: &StatisticsContext,
) -> Result<bool> {
    let (0, Some(fetch)) = (bound.skip, bound.fetch) else {
        return Ok(false);
    };

    if plan.output_partitioning().partition_count() != 1 {
        return Ok(false);
    }

    let Some(num_rows) = limit_eliminable_exact_num_rows(plan, stats_ctx)? else {
        return Ok(false);
    };

    Ok(num_rows <= fetch)
}

/// Returns exact row counts only from a conservative whitelist of operators
/// whose row-count guarantees are strong enough to remove a limit.
fn limit_eliminable_exact_num_rows(
    plan: &Arc<dyn ExecutionPlan>,
    stats_ctx: &StatisticsContext,
) -> Result<Option<usize>> {
    // Unwrap any wrapping ProjectionExec layers; projections preserve row count
    // but may derive statistics in ways that are not trustworthy, so we peek
    // through them to the underlying producer.
    let mut current = plan;
    while let Some(projection) = current.downcast_ref::<ProjectionExec>() {
        current = projection.input();
    }

    if current.is::<EmptyExec>() {
        return Ok(Some(0));
    }

    if current.is::<PlaceholderRowExec>() {
        return Ok(Some(1));
    }

    if matches!(
        stats_ctx
            .compute_arc(current, &StatisticsArgs::new())?
            .num_rows,
        Precision::Exact(0)
    ) {
        return Ok(Some(0));
    }

    Ok(None)
}

// See tests in datafusion/core/tests/physical_optimizer
