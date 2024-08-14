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

use std::fmt::Debug;
use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_common::utils::combine_limit;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// This rule inspects [`ExecutionPlan`]'s and pushes down the fetch limit from
/// the parent to the child if applicable.
#[derive(Default)]
pub struct LimitPushdown {}

/// This is a "data class" we use within the [`LimitPushdown`] rule to push
/// down [`LimitExec`] in the plan. GlobalRequirements are hold as a rule-wide state
/// and holds the fetch and skip information. The struct also has a field named
/// satisfied which means if the "current" plan is valid in terms of limits or not.
///
/// For example: If the plan is satisfied with current fetch info, we decide to not add a LocalLimit
///
/// [`LimitPushdown`]: crate::limit_pushdown::LimitPushdown
/// [`LimitExec`]: crate::limit_pushdown::LimitExec
#[derive(Default, Clone, Debug)]
pub struct GlobalRequirements {
    fetch: Option<usize>,
    skip: usize,
    satisfied: bool,
}

impl LimitPushdown {
    #[allow(missing_docs)]
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
            satisfied: false,
        };
        let updated_plan = pushdown_limits(plan, global_state)?;
        Ok(updated_plan)
    }

    fn name(&self) -> &str {
        "LimitPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This enumeration makes `skip` and `fetch` calculations easier by providing
/// a single API for both local and global limit operators.
#[derive(Debug)]
pub enum LimitExec {
    Global(GlobalLimitExec),
    Local(LocalLimitExec),
}

impl LimitExec {
    fn input(&self) -> &Arc<dyn ExecutionPlan> {
        match self {
            Self::Global(global) => global.input(),
            Self::Local(local) => local.input(),
        }
    }

    fn fetch(&self) -> Option<usize> {
        match self {
            Self::Global(global) => global.fetch(),
            Self::Local(local) => Some(local.fetch()),
        }
    }

    fn skip(&self) -> usize {
        match self {
            Self::Global(global) => global.skip(),
            Self::Local(_) => 0,
        }
    }
}

impl From<LimitExec> for Arc<dyn ExecutionPlan> {
    fn from(limit_exec: LimitExec) -> Self {
        match limit_exec {
            LimitExec::Global(global) => Arc::new(global),
            LimitExec::Local(local) => Arc::new(local),
        }
    }
}

/// pushdown_limit_helper function is the main helper function of the `LimitPushDown` rule
/// The helper takes the `ExecutionPlan` and a global state which is an instance of `GlobalRequirements`
/// and modifies these parameters while checking if the limits can be pushed down or not.
pub fn pushdown_limit_helper(
    mut pushdown_plan: Arc<dyn ExecutionPlan>,
    mut global_state: GlobalRequirements,
) -> Result<(Transformed<Arc<dyn ExecutionPlan>>, GlobalRequirements)> {
    if let Some(limit_exec) = extract_limit(&pushdown_plan) {
        // If we have fetch/skip info in global state already we need to decide which one to continue with
        let (skip, fetch) = combine_limit(
            global_state.skip,
            global_state.fetch,
            limit_exec.skip(),
            limit_exec.fetch(),
        );
        global_state.skip = skip;
        global_state.fetch = fetch;

        // Now we have info in the global_state we can remove the LimitExec plan.
        // We will decide later if we should add it again or not
        return Ok((
            Transformed {
                data: Arc::<dyn ExecutionPlan>::clone(limit_exec.input()),
                transformed: true,
                tnr: TreeNodeRecursion::Stop,
            },
            global_state,
        ));
    }

    // The plan is not LimitExec
    if pushdown_plan.fetch().is_some() {
        // Plan has its own fetch info, decide which one to use in global_state
        if global_state.fetch.is_none() {
            global_state.satisfied = true;
        }
        (global_state.skip, global_state.fetch) = combine_limit(
            global_state.skip,
            global_state.fetch,
            0,
            pushdown_plan.fetch(),
        );
    }

    if global_state.fetch.is_none() {
        // There's no valid Fetch info
        if global_state.skip > 0 {
            // There might be a case with only offset, if so add a global limit
            if !global_state.satisfied {
                global_state.satisfied = true;
                return Ok((
                    Transformed::yes(add_global_limit(
                        pushdown_plan,
                        global_state.skip,
                        global_state.fetch,
                    )),
                    global_state,
                ));
            }
        }
        // There's no info on fetch, nothing to do
        return Ok((Transformed::no(pushdown_plan), global_state));
    }

    if pushdown_plan.supports_limit_pushdown() {
        if !combines_input_partitions(&pushdown_plan) {
            // We have the info in global state and the plan is pushing down, continue
            return Ok((Transformed::no(pushdown_plan), global_state));
        }

        // This plan is combining input partitions we need to add the fetch info to plan if possible
        // if not, we must add a LimitExec with the global_state info
        let skip_and_fetch = Some(global_state.fetch.unwrap() + global_state.skip);
        if let Some(plan_with_fetch) = pushdown_plan.with_fetch(skip_and_fetch) {
            global_state.fetch = skip_and_fetch;
            global_state.skip = 0;
            global_state.satisfied = true;
            pushdown_plan = plan_with_fetch;
            return Ok((Transformed::yes(pushdown_plan), global_state));
        }

        if global_state.satisfied {
            // If the plan is already satisfied do not add a limit info
            return Ok((Transformed::no(pushdown_plan), global_state));
        }

        // Add LimitExec
        global_state.satisfied = true;
        return Ok((
            Transformed::yes(add_limit(
                pushdown_plan,
                global_state.skip,
                global_state.fetch.unwrap(),
            )),
            global_state,
        ));
    }

    // The plan does not support push down and not a LimitExec
    // We will need to add a LimitExec or Fetch info if the plan is not satisfied
    // If the plan is already satisfied we will try to add the Fetch info and return the plan

    let fetch = global_state.fetch;
    let skip = global_state.skip;
    let skip_and_fetch = Some(fetch.unwrap() + skip);

    // There's no push down change fetch & skip to default values
    global_state.fetch = None;
    global_state.skip = 0;
    if global_state.satisfied {
        if let Some(plan_with_fetch) = pushdown_plan.with_fetch(skip_and_fetch) {
            pushdown_plan = plan_with_fetch;
            return Ok((Transformed::yes(pushdown_plan), global_state));
        }
        return Ok((Transformed::no(pushdown_plan), global_state));
    }

    // Add Fetch / LimitExec
    global_state.satisfied = true;
    if let Some(plan_with_fetch) = pushdown_plan.with_fetch(skip_and_fetch) {
        pushdown_plan = plan_with_fetch;
        if skip > 0 {
            return Ok((
                Transformed::yes(add_global_limit(pushdown_plan, skip, fetch)),
                global_state,
            ));
        }
        return Ok((Transformed::yes(pushdown_plan), global_state));
    }

    Ok((
        Transformed::yes(add_limit(pushdown_plan, skip, fetch.unwrap())),
        global_state,
    ))
}

/// Pushes down the limit through the plan.
pub(crate) fn pushdown_limits(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    global_state: GlobalRequirements,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (mut new_node, mut global_state) =
        pushdown_limit_helper(pushdown_plan, global_state)?;

    while new_node.tnr == TreeNodeRecursion::Stop {
        (new_node, global_state) = pushdown_limit_helper(new_node.data, global_state)?;
    }

    let children = new_node.data.children();
    let new_children = children
        .into_iter()
        .map(|child| {
            pushdown_limits(Arc::<dyn ExecutionPlan>::clone(child), global_state.clone())
        })
        .collect::<Result<_>>()?;

    new_node.data.with_new_children(new_children)
}

/// Transforms the [`ExecutionPlan`] into a [`LimitExec`] if it is a
/// [`GlobalLimitExec`] or a [`LocalLimitExec`].
fn extract_limit(plan: &Arc<dyn ExecutionPlan>) -> Option<LimitExec> {
    if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        Some(LimitExec::Global(GlobalLimitExec::new(
            Arc::<dyn ExecutionPlan>::clone(global_limit.input()),
            global_limit.skip(),
            global_limit.fetch(),
        )))
    } else {
        plan.as_any()
            .downcast_ref::<LocalLimitExec>()
            .map(|local_limit| {
                LimitExec::Local(LocalLimitExec::new(
                    Arc::<dyn ExecutionPlan>::clone(local_limit.input()),
                    local_limit.fetch(),
                ))
            })
    }
}

fn combines_input_partitions(exec: &Arc<dyn ExecutionPlan>) -> bool {
    let exec = exec.as_any();
    exec.is::<CoalescePartitionsExec>() || exec.is::<SortPreservingMergeExec>()
}

fn add_limit(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: usize,
) -> Arc<dyn ExecutionPlan> {
    if skip > 0 || pushdown_plan.output_partitioning().partition_count() == 1 {
        return add_global_limit(pushdown_plan, skip, Some(fetch));
    }
    add_local_limit(pushdown_plan, fetch + skip)
}

fn add_local_limit(
    mut pushdown_plan: Arc<dyn ExecutionPlan>,
    fetch: usize,
) -> Arc<dyn ExecutionPlan> {
    let new_plan: Arc<dyn ExecutionPlan> =
        Arc::new(LocalLimitExec::new(pushdown_plan, fetch));
    pushdown_plan = new_plan;
    pushdown_plan
}

fn add_global_limit(
    mut pushdown_plan: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    let new_plan: Arc<dyn ExecutionPlan> =
        Arc::new(GlobalLimitExec::new(pushdown_plan, skip, fetch));
    pushdown_plan = new_plan;
    pushdown_plan
}

// See tests in datafusion/core/tests/physical_optimizer
