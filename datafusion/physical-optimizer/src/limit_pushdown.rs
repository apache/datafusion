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

use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_common::utils::combine_limit;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
/// This rule inspects [`ExecutionPlan`]'s and pushes down the fetch limit from
/// the parent to the child if applicable.
#[derive(Default, Debug)]
pub struct LimitPushdown {}

/// State carried through [`LimitPushdown`] while it pushes limits down the plan.
///
/// `fetch` and `skip` hold the limit currently being pushed down. `pending`
/// says whether a local or global limit still has to be enforced. After it
/// becomes `None`, `fetch` may still be copied to children to help them stop
/// early, but it is no longer responsible for correctness.
///
/// [`LimitPushdown`]: crate::limit_pushdown::LimitPushdown
#[derive(Default, Clone, Debug)]
pub struct GlobalRequirements {
    fetch: Option<usize>,
    skip: usize,
    preserve_order: bool,
    pending: Option<LimitScope>,
}

/// The scope of a row limit: what the limited row count applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LimitScope {
    /// The limit caps each output partition independently, as enforced by
    /// [`LocalLimitExec`].
    Local,
    /// The limit caps the combined output of all partitions, as enforced by
    /// [`GlobalLimitExec`] over a single-partition input. A fetch on a
    /// multi-partition operator cannot satisfy this scope; the partitions
    /// must first be merged into one stream.
    Global,
}

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
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let global_state = GlobalRequirements {
            fetch: None,
            skip: 0,
            preserve_order: false,
            pending: None,
        };
        pushdown_limits(plan, global_state)
    }

    fn name(&self) -> &str {
        "LimitPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This function is the main helper function of the `LimitPushDown` rule.
/// The helper takes an `ExecutionPlan` and a global (algorithm) state which is
/// an instance of `GlobalRequirements` and modifies these parameters while
/// checking if the limits can be pushed down or not.
///
/// If a limit is encountered, a [`TreeNodeRecursion::Stop`] is returned. Otherwise,
/// return a [`TreeNodeRecursion::Continue`].
pub fn pushdown_limit_helper(
    mut pushdown_plan: Arc<dyn ExecutionPlan>,
    mut global_state: GlobalRequirements,
) -> Result<(Transformed<Arc<dyn ExecutionPlan>>, GlobalRequirements)> {
    if global_state.pending == Some(LimitScope::Local)
        && pushdown_plan.output_partitioning().partition_count() == 1
    {
        // A local limit on one output partition also limits the total output.
        // Treat it as global before descending because this node may have
        // multi-partition children.
        global_state.pending = Some(LimitScope::Global);
    }

    if let Some(global_limit) = pushdown_plan.downcast_ref::<GlobalLimitExec>()
        && global_limit.skip() == 0
        && global_limit.fetch().is_none()
    {
        // A `GlobalLimitExec` with no skip and no fetch enforces nothing, so
        // remove it and keep the carried state unchanged. General limit handling
        // would mark a global limit as pending even when none is needed. Keep the
        // local-to-global promotion above: it remains valid because this node has
        // one output partition.
        return Ok((
            Transformed {
                data: Arc::clone(global_limit.input()),
                transformed: true,
                tnr: TreeNodeRecursion::Stop,
            },
            global_state,
        ));
    }

    if global_state.pending == Some(LimitScope::Global)
        && pushdown_plan.output_partitioning().partition_count() > 1
    {
        // Handle this before the generic `fetch` case: on a multi-partition plan,
        // `fetch` limits partitions separately and cannot limit their combined output.
        // Keep it for early stopping, then combine the partitions to enforce the
        // global limit.
        let hint = global_state.fetch.map(|fetch| fetch + global_state.skip);
        if let Some(hint) = hint {
            let hint = pushdown_plan.fetch().map_or(hint, |fetch| fetch.min(hint));
            if pushdown_plan.fetch() != Some(hint)
                && let Some(plan_with_fetch) = pushdown_plan.with_fetch(Some(hint))
            {
                pushdown_plan = plan_with_fetch;
            }
        }

        let plan = materialize_global_requirement(
            pushdown_plan,
            global_state.skip,
            global_state.fetch,
            global_state.preserve_order,
        );
        global_state.fetch = hint;
        global_state.skip = 0;
        global_state.pending = None;
        return Ok((Transformed::yes(plan), global_state));
    }

    if let Some(global_limit) = pushdown_plan.downcast_ref::<GlobalLimitExec>() {
        let input = Arc::clone(global_limit.input());
        let skip = global_limit.skip();
        let fetch = global_limit.fetch();

        (global_state.skip, global_state.fetch) =
            combine_limit(global_state.skip, global_state.fetch, skip, fetch);
        global_state.preserve_order |= global_limit.required_ordering().is_some();
        global_state.pending = Some(LimitScope::Global);
        if let Some(fetch) = global_state.fetch
            && limit_satisfied_by_input(&input, global_state.skip, fetch)?
        {
            global_state.pending = None;
        }
        return Ok((
            Transformed {
                data: input,
                transformed: true,
                tnr: TreeNodeRecursion::Stop,
            },
            global_state,
        ));
    }

    if let Some(local_limit) = pushdown_plan.downcast_ref::<LocalLimitExec>() {
        let input = Arc::clone(local_limit.input());
        (global_state.skip, global_state.fetch) = combine_limit(
            global_state.skip,
            global_state.fetch,
            0,
            Some(local_limit.fetch()),
        );
        global_state.preserve_order |= local_limit.required_ordering().is_some();
        global_state.pending = if input.output_partitioning().partition_count() == 1 {
            Some(LimitScope::Global)
        } else {
            Some(LimitScope::Local)
        };
        if let Some(fetch) = global_state.fetch
            && limit_satisfied_by_input(&input, global_state.skip, fetch)?
        {
            global_state.pending = None;
        }
        return Ok((
            Transformed {
                data: input,
                transformed: true,
                tnr: TreeNodeRecursion::Stop,
            },
            global_state,
        ));
    }

    // An existing `fetch` satisfies the carried limit only if there is no
    // offset and it is no larger than the requested fetch. For example,
    // `fetch=10` does not enforce `LIMIT 5`; if the operator cannot be changed to
    // `fetch=5`, the explicit limit must remain. Still combine the fetch values
    // below so descendants can use the tighter value to stop early.
    if let Some(existing_fetch) = pushdown_plan.fetch() {
        if global_state.skip == 0
            && let Some(required_fetch) = global_state.fetch
            && existing_fetch <= required_fetch
        {
            global_state.pending = None;
        }
        (global_state.skip, global_state.fetch) = combine_limit(
            global_state.skip,
            global_state.fetch,
            0,
            Some(existing_fetch),
        );
    }

    let Some(global_fetch) = global_state.fetch else {
        // There's no valid fetch information, exit early:
        return if global_state.skip > 0 && global_state.pending.is_some() {
            // There might be a case with only offset, if so add a global limit:
            let new_plan = add_global_limit(pushdown_plan, global_state.skip, None);
            global_state.pending = None;
            Ok((Transformed::yes(new_plan), global_state))
        } else {
            // There's no info on offset or fetch, nothing to do:
            Ok((Transformed::no(pushdown_plan), global_state))
        };
    };

    let skip_and_fetch = Some(global_fetch + global_state.skip);

    if pushdown_plan.supports_limit_pushdown() {
        // A pending limit cannot be replicated to every child of a multi-child
        // node: each child would enforce it and their merged output could
        // exceed it. Only a unary node can pass it through transparently, plus
        // `UnionExec` with `Local` (each output partition comes from one child).
        let can_delegate_pending = global_state.pending.is_none()
            || pushdown_plan.children().len() <= 1
            || (global_state.pending == Some(LimitScope::Local)
                && pushdown_plan.is::<UnionExec>());
        if !can_delegate_pending {
            // Enforce the limit at this node's output, then let children stop
            // early with the same fetch hint.
            let new_plan = if global_state.skip > 0 {
                add_limit(pushdown_plan, global_state.skip, global_fetch)
            } else if let Some(plan_with_fetch) = pushdown_plan.with_fetch(skip_and_fetch)
            {
                plan_with_fetch
            } else {
                add_limit(pushdown_plan, 0, global_fetch)
            };
            global_state.fetch = skip_and_fetch;
            global_state.skip = 0;
            global_state.pending = None;
            return Ok((Transformed::yes(new_plan), global_state));
        }
        if !combines_input_partitions(&pushdown_plan) {
            // We have information in the global state and the plan pushes down,
            // continue:
            Ok((Transformed::no(pushdown_plan), global_state))
        } else if let Some(plan_with_fetch) = pushdown_plan.with_fetch(skip_and_fetch) {
            // This partition-combining operator accepted `fetch`, so the fetch now
            // bounds its combined output.
            let mut new_plan = plan_with_fetch;
            // Execution plans can't (yet) handle skip, so if we have one,
            // we still need to add a global limit.
            if global_state.skip > 0 {
                new_plan =
                    add_global_limit(new_plan, global_state.skip, global_state.fetch);
            }
            global_state.fetch = skip_and_fetch;
            global_state.skip = 0;
            global_state.pending = None;
            Ok((Transformed::yes(new_plan), global_state))
        } else if global_state.pending.is_none() {
            // The required limit is already enforced, so do not add another one.
            Ok((Transformed::no(pushdown_plan), global_state))
        } else {
            let new_plan = add_limit(pushdown_plan, global_state.skip, global_fetch);
            global_state.pending = None;
            Ok((Transformed::yes(new_plan), global_state))
        }
    } else {
        // This operator stops limit pushdown. If the limit is already enforced, try
        // only to add a fetch for early stopping; otherwise enforce the limit here.

        // There's no push down, change fetch & skip to default values:
        let global_skip = global_state.skip;
        global_state.fetch = None;
        global_state.skip = 0;

        let maybe_fetchable = pushdown_plan.with_fetch(skip_and_fetch);
        if global_state.pending.is_none() {
            if let Some(plan_with_fetch) = maybe_fetchable {
                let plan_with_preserve_order = plan_with_fetch
                    .with_preserve_order(global_state.preserve_order)
                    .unwrap_or(plan_with_fetch);
                Ok((Transformed::yes(plan_with_preserve_order), global_state))
            } else {
                Ok((Transformed::no(pushdown_plan), global_state))
            }
        } else {
            pushdown_plan = if let Some(plan_with_fetch) = maybe_fetchable {
                let plan_with_preserve_order = plan_with_fetch
                    .with_preserve_order(global_state.preserve_order)
                    .unwrap_or(plan_with_fetch);

                if global_skip > 0 {
                    add_global_limit(
                        plan_with_preserve_order,
                        global_skip,
                        Some(global_fetch),
                    )
                } else {
                    plan_with_preserve_order
                }
            } else {
                add_limit(pushdown_plan, global_skip, global_fetch)
            };
            global_state.pending = None;
            Ok((Transformed::yes(pushdown_plan), global_state))
        }
    }
}

/// Returns true if exact input statistics prove that applying the limit would
/// not remove any rows.
fn limit_satisfied_by_input(
    plan: &Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: usize,
) -> Result<bool> {
    if skip > 0 {
        return Ok(false);
    }

    if plan.output_partitioning().partition_count() != 1 {
        return Ok(false);
    }

    let Some(num_rows) = limit_eliminable_exact_num_rows(plan)? else {
        return Ok(false);
    };

    Ok(num_rows <= fetch)
}

/// Returns exact row counts only from a conservative whitelist of operators
/// whose row-count guarantees are strong enough to remove a limit.
fn limit_eliminable_exact_num_rows(
    plan: &Arc<dyn ExecutionPlan>,
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
        StatisticsContext::new()
            .compute(current.as_ref(), &StatisticsArgs::new())?
            .num_rows,
        Precision::Exact(0)
    ) {
        return Ok(Some(0));
    }

    Ok(None)
}

/// Pushes down the limit through the plan.
pub(crate) fn pushdown_limits(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    global_state: GlobalRequirements,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Call pushdown_limit_helper.
    // This will either extract the limit node (returning the child), or apply the limit pushdown.
    let (mut new_node, mut global_state) =
        pushdown_limit_helper(pushdown_plan, global_state)?;

    // While limits exist, continue combining the global_state.
    while new_node.tnr == TreeNodeRecursion::Stop {
        (new_node, global_state) = pushdown_limit_helper(new_node.data, global_state)?;
    }

    // Once the limit is enforced, clear `skip` before visiting children so it
    // cannot combine with a nested limit and skip rows twice. Each child may
    // receive the same `fetch` for early stopping; those values are not added
    // together because the limit has already been enforced above.
    if global_state.pending.is_none() {
        global_state.skip = 0;
    }

    // Apply pushdown limits in children
    let children = new_node.data.children();
    let mut changed = false;
    let new_children = children
        .into_iter()
        .map(|child: &Arc<dyn ExecutionPlan>| {
            let new_child = pushdown_limits(
                Arc::<dyn ExecutionPlan>::clone(child),
                global_state.clone(),
            )?;
            // Tracking if any of the children changed
            changed |= !Arc::ptr_eq(child, &new_child);
            Ok(new_child)
        })
        .collect::<Result<_>>()?;

    if changed {
        replace_children_if_necessary(new_node.data, new_children)
    } else {
        Ok(new_node.data)
    }
}

/// Checks if the given plan combines input partitions.
fn combines_input_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<CoalescePartitionsExec>() || plan.is::<SortPreservingMergeExec>()
}

/// Adds a limit to the plan, chooses between global and local limits based on
/// skip value and the number of partitions.
fn add_limit(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: usize,
) -> Arc<dyn ExecutionPlan> {
    if skip > 0 || pushdown_plan.output_partitioning().partition_count() == 1 {
        add_global_limit(pushdown_plan, skip, Some(fetch))
    } else {
        Arc::new(LocalLimitExec::new(pushdown_plan, fetch + skip)) as _
    }
}

/// Enforces one limit across all output partitions. Multiple partitions are
/// merged first, preserving order when required, because a fetch on each
/// partition cannot limit their combined output.
fn materialize_global_requirement(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: Option<usize>,
    preserve_order: bool,
) -> Arc<dyn ExecutionPlan> {
    if pushdown_plan.output_partitioning().partition_count() == 1 {
        return add_global_limit(pushdown_plan, skip, fetch);
    }

    let skip_and_fetch = fetch.map(|fetch| fetch + skip);
    let limited: Arc<dyn ExecutionPlan> = if preserve_order
        && let Some(ordering) = pushdown_plan.output_ordering().cloned()
    {
        Arc::new(
            SortPreservingMergeExec::new(ordering, pushdown_plan)
                .with_fetch(skip_and_fetch),
        )
    } else {
        Arc::new(CoalescePartitionsExec::new(pushdown_plan).with_fetch(skip_and_fetch))
    };

    if skip > 0 {
        add_global_limit(limited, skip, fetch)
    } else {
        limited
    }
}

/// Adds a global limit to the plan.
fn add_global_limit(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(pushdown_plan, skip, fetch)) as _
}

// See tests in datafusion/core/tests/physical_optimizer
