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

//! [`SwapBuildSideIfInverted`] — first concrete [`RuntimeRule`].
//!
//! When a HashJoinExec's current build side ends up larger at runtime
//! than the probe side, the static planner made the wrong choice — it
//! picked build based on (Inexact) estimates. This rule walks the plan
//! looking for joins whose children are [`StageBoundaryBuffer`]s in the
//! just-completed state (`is_ready && !streaming_started`); if `l > r`
//! it calls [`HashJoinExec::swap_inputs`] and patches the result to
//! preserve the join's distribution invariants.
//!
//! [`RuntimeRule`]: crate::runtime_optimizer::RuntimeRule

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use log::info;

use crate::coalesce_partitions::CoalescePartitionsExec;
use crate::joins::{HashJoinExec, PartitionMode};
use crate::runtime_optimizer::RuntimeRule;
use crate::stage_boundary_buffer::StageBoundaryBuffer;
use crate::{ExecutionPlan, ExecutionPlanProperties};

#[derive(Default, Debug)]
pub struct SwapBuildSideIfInverted;

impl SwapBuildSideIfInverted {
    pub fn new() -> Self {
        Self
    }
}

impl RuntimeRule for SwapBuildSideIfInverted {
    fn name(&self) -> &str {
        "SwapBuildSideIfInverted"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if just_completed_stage_of_join(join).is_none() {
                return Ok(Transformed::no(node));
            }
            let children = join.children();
            // Current HashJoinExec: LEFT child is the build side.
            let left = side_runtime_rows(children[0]);
            let right = side_runtime_rows(children[1]);
            let (Some(l), Some(r)) = (left, right) else {
                return Ok(Transformed::no(node));
            };
            if l <= r {
                return Ok(Transformed::no(node));
            }
            info!(
                "SwapBuildSideIfInverted: flipping HashJoinExec — current \
                 build (left) = {l} rows, probe (right) = {r} rows. \
                 Calling swap_inputs to make the smaller side the new build."
            );
            let mode = *join.partition_mode();
            let swapped = join.swap_inputs(mode)?;
            let swapped = ensure_collect_left_single_partition(swapped)?;
            Ok(Transformed::yes(swapped))
        })
        .map(|t| t.data)
    }
}

/// Ensures the (possibly already-coalesced) plan satisfies HashJoin's
/// CollectLeft invariant: under `PartitionMode::CollectLeft`, the left
/// child must report exactly one output partition. After
/// [`HashJoinExec::swap_inputs`], the new left side is whatever used
/// to be on the right — frequently multi-partition (e.g. behind a
/// RepartitionExec). Wrap it in [`CoalescePartitionsExec`] when needed.
///
/// `swap_inputs` may also have wrapped the result in a `ProjectionExec`
/// to preserve output column order; in that case the HashJoinExec is
/// one level down. We walk through that via `transform_up`.
fn ensure_collect_left_single_partition(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(join) = node.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        if *join.partition_mode() != PartitionMode::CollectLeft {
            return Ok(Transformed::no(node));
        }
        let children = join.children();
        if children[0].output_partitioning().partition_count() == 1 {
            return Ok(Transformed::no(node));
        }
        let coalesced: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(Arc::clone(children[0])));
        let new_children = vec![coalesced, Arc::clone(children[1])];
        Ok(Transformed::yes(node.with_new_children(new_children)?))
    })
    .map(|t| t.data)
}

/// Returns `Some(stage)` if `join`'s two children are both
/// [`StageBoundaryBuffer`]s at the same stage in the just-completed
/// state (`is_ready && !streaming_started`). Otherwise `None`.
fn just_completed_stage_of_join(join: &HashJoinExec) -> Option<usize> {
    let children = join.children();
    if children.len() != 2 {
        return None;
    }
    let left = children[0].downcast_ref::<StageBoundaryBuffer>()?;
    let right = children[1].downcast_ref::<StageBoundaryBuffer>()?;
    if left.stage() != right.stage() {
        return None;
    }
    if left.is_ready()
        && !left.streaming_started()
        && right.is_ready()
        && !right.streaming_started()
    {
        Some(left.stage())
    } else {
        None
    }
}

/// Total runtime row count across all output partitions of a
/// HashJoin input. The input is always a `StageBoundaryBuffer`
/// (`InsertHashJoinBoundaries` inserts one above each side), and the
/// buffer materializes its input — so once `is_ready` flips,
/// `runtime_row_count(p)` returns the true post-input cardinality.
/// No static-stat fallback needed.
///
/// Returns `None` if any partition's count is unavailable, which the
/// rule treats as "don't try to decide yet."
fn side_runtime_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let n = plan.output_partitioning().partition_count();
    let mut total: usize = 0;
    for p in 0..n {
        total += plan.runtime_row_count(p)?;
    }
    Some(total)
}
