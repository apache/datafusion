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
use datafusion_common::plan_datafusion_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::utils::combine_limit;
use datafusion_common::Result;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::ExecutionPlan;

/// This rule inspects [`ExecutionPlan`]'s and pushes down the fetch limit from
/// the parent to the child if applicable.
#[derive(Default)]
pub struct LimitPushdown {}

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
        plan.transform_down(push_down_limits).data()
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
enum LimitExec {
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

    fn with_child(&self, child: Arc<dyn ExecutionPlan>) -> Self {
        match self {
            Self::Global(global) => {
                Self::Global(GlobalLimitExec::new(child, global.skip(), global.fetch()))
            }
            Self::Local(local) => Self::Local(LocalLimitExec::new(child, local.fetch())),
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

/// Pushes down the limit through the plan.
pub fn push_down_limits(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let maybe_modified = if let Some(limit_exec) = extract_limit(&plan) {
        let child = limit_exec.input();
        if let Some(child_limit) = extract_limit(child) {
            let merged = merge_limits(&limit_exec, &child_limit);
            // Revisit current node in case of consecutive pushdowns
            Some(push_down_limits(merged)?.data)
        } else if child.supports_limit_pushdown() {
            try_push_down_limit(&limit_exec, Arc::clone(child))?
        } else {
            add_fetch_to_child(&limit_exec, Arc::clone(child))
        }
    } else {
        None
    };

    Ok(maybe_modified.map_or(Transformed::no(plan), Transformed::yes))
}

/// Transforms the [`ExecutionPlan`] into a [`LimitExec`] if it is a
/// [`GlobalLimitExec`] or a [`LocalLimitExec`].
fn extract_limit(plan: &Arc<dyn ExecutionPlan>) -> Option<LimitExec> {
    if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        Some(LimitExec::Global(GlobalLimitExec::new(
            Arc::clone(global_limit.input()),
            global_limit.skip(),
            global_limit.fetch(),
        )))
    } else {
        plan.as_any()
            .downcast_ref::<LocalLimitExec>()
            .map(|local_limit| {
                LimitExec::Local(LocalLimitExec::new(
                    Arc::clone(local_limit.input()),
                    local_limit.fetch(),
                ))
            })
    }
}

/// Merge the limits of the parent and the child. If at least one of them is a
/// [`GlobalLimitExec`], the result is also a [`GlobalLimitExec`]. Otherwise,
/// the result is a [`LocalLimitExec`].
fn merge_limits(
    parent_limit_exec: &LimitExec,
    child_limit_exec: &LimitExec,
) -> Arc<dyn ExecutionPlan> {
    // We can use the logic in `combine_limit` from the logical optimizer:
    let (skip, fetch) = combine_limit(
        parent_limit_exec.skip(),
        parent_limit_exec.fetch(),
        child_limit_exec.skip(),
        child_limit_exec.fetch(),
    );
    match (parent_limit_exec, child_limit_exec) {
        (LimitExec::Local(_), LimitExec::Local(_)) => {
            // The fetch is present in this case, can unwrap.
            Arc::new(LocalLimitExec::new(
                Arc::clone(child_limit_exec.input()),
                fetch.unwrap(),
            ))
        }
        _ => Arc::new(GlobalLimitExec::new(
            Arc::clone(child_limit_exec.input()),
            skip,
            fetch,
        )),
    }
}

/// Pushes down the limit through the child. If the child has a single input
/// partition, simply swaps the parent and the child. Otherwise, adds a
/// [`LocalLimitExec`] after in between in addition to swapping, because of
/// multiple input partitions.
fn try_push_down_limit(
    limit_exec: &LimitExec,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let grandchildren = child.children();
    if let Some(&grandchild) = grandchildren.first() {
        // GlobalLimitExec and LocalLimitExec must have an input after pushdown
        if combines_input_partitions(&child) {
            // We still need a LocalLimitExec after the child
            if let Some(fetch) = limit_exec.fetch() {
                let new_local_limit = Arc::new(LocalLimitExec::new(
                    Arc::clone(grandchild),
                    fetch + limit_exec.skip(),
                ));
                let new_child =
                    Arc::clone(&child).with_new_children(vec![new_local_limit])?;
                Ok(Some(limit_exec.with_child(new_child).into()))
            } else {
                Ok(None)
            }
        } else {
            // Swap current with child
            let new_limit = limit_exec.with_child(Arc::clone(grandchild));
            let new_child = child.with_new_children(vec![new_limit.into()])?;
            Ok(Some(new_child))
        }
    } else {
        // Operators supporting limit push down must have a child.
        Err(plan_datafusion_err!(
            "{:#?} must have a child to push down limit",
            child
        ))
    }
}

fn combines_input_partitions(exec: &Arc<dyn ExecutionPlan>) -> bool {
    let exec = exec.as_any();
    exec.is::<CoalescePartitionsExec>() || exec.is::<SortPreservingMergeExec>()
}

/// Transforms child to the fetching version if supported. Removes the parent if
/// skip is zero. Otherwise, keeps the parent.
fn add_fetch_to_child(
    limit_exec: &LimitExec,
    child: Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let fetch = limit_exec.fetch();
    let skip = limit_exec.skip();

    let child_fetch = fetch.map(|f| f + skip);

    if let Some(child_with_fetch) = child.with_fetch(child_fetch) {
        if skip > 0 {
            Some(limit_exec.with_child(child_with_fetch).into())
        } else {
            Some(child_with_fetch)
        }
    } else {
        None
    }
}

// See tests in datafusion/core/tests/physical_optimizer
