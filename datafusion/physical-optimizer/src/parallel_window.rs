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

//! Parallelize bounded RANGE-frame window functions that have an ORDER BY
//! but no PARTITION BY by re-shaping the plan above the window's input.
//!
//! Runs *before* `EnsureRequirements`. For each eligible
//! `BoundedWindowAggExec`, this rule:
//!  - inserts a `SortExec(preserve_partitioning=true)` on the ORDER BY
//!    key above the window's existing input;
//!  - inserts a `RangeRepartitionExec` carrying the halo distances above
//!    that sort;
//!  - rebuilds the `BoundedWindowAggExec` on top of the result with
//!    `parallel_aware = true`, so its `required_input_distribution()`
//!    returns `UnspecifiedDistribution` instead of `SinglePartition` —
//!    which is what would otherwise force `EnsureRequirements` to insert
//!    a `SortPreservingMergeExec` and collapse our parallelism.
//!
//! By owning the structural decisions before `EnsureRequirements` runs,
//! this rule avoids the post-hoc surgery of stripping an inserted
//! `SortPreservingMergeExec`. All boundary / global-extremes logic lives
//! in `RangeRepartitionExec`'s coordinator and runs against runtime
//! stats rather than plan-time `Statistics`.

use crate::PhysicalOptimizerRule;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_expr::{WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::halo_drop::HaloDropExec;
use datafusion_physical_plan::range_repartition::RangeRepartitionExec;
use datafusion_physical_plan::windows::BoundedWindowAggExec;
use log::info;
use std::sync::Arc;

#[derive(Default, Clone, Debug)]
pub struct ParallelWindow;

impl ParallelWindow {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for ParallelWindow {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let out = plan.transform_down(|node| {
            let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some((halo_preceding, halo_following)) = candidate_halo(window)
            else {
                return Ok(Transformed::no(node));
            };
            info!(
                "ParallelWindow: candidate BoundedWindowAggExec (RANGE frame, no PARTITION BY); \
                 halo: {halo_preceding} preceding, {halo_following} following"
            );
            // `candidate_halo` already verified order_by.len()==1.
            let sort_key = window.window_expr()[0].order_by()[0].clone();
            let lex = LexOrdering::new(vec![sort_key])
                .expect("candidate_halo guarantees one sort key");
            let original_input = Arc::clone(&node.children()[0]);
            // Don't pre-insert a SortExec; RangeRepartitionExec now declares
            // its required input ordering, so EnsureRequirements will plant
            // the pipeline-breaking sort beneath us. Doing both would just
            // produce a redundant SortExec that the optimizer collapses.
            let range = Arc::new(RangeRepartitionExec::new(
                original_input,
                lex.clone(),
                halo_preceding,
                halo_following,
            ));
            // `parallel_aware = true` flips BWAG's required_input_distribution
            // to UnspecifiedDistribution, so EnsureRequirements won't wrap
            // us in an SPM. `can_repartition` is vacuous because
            // candidate_halo already required partition_keys empty.
            let new_window: Arc<dyn ExecutionPlan> = Arc::new(
                BoundedWindowAggExec::try_new(
                    window.window_expr().to_vec(),
                    range,
                    window.input_order_mode.clone(),
                    true,
                )?
                .with_parallel_aware(true),
            );
            // Drop halo rows above the per-partition window. HaloDropExec
            // reads its primary range from `input.runtime_sort_extremes`,
            // which BWAG passes through and RangeRepartitionExec populates.
            let drop_halo: Arc<dyn ExecutionPlan> =
                Arc::new(HaloDropExec::try_new(new_window, &lex)?);
            // Jump past the result's children: the BWAG we just emitted is
            // still a candidate by shape (RANGE frame, no PARTITION BY) and
            // `transform_down` would otherwise re-wrap it forever.
            Ok(Transformed::new(drop_halo, true, TreeNodeRecursion::Jump))
        })?;
        Ok(out.data)
    }

    fn name(&self) -> &str {
        "ParallelWindow"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns `(halo_preceding, halo_following)` if the window matches the
/// v1 shape we know how to parallelize: no PARTITION BY, a single
/// `Column` sort key, RANGE frame, finite Int64 bounds (or CurrentRow).
/// Returns `None` otherwise so the rule leaves the plan alone.
fn candidate_halo(window: &BoundedWindowAggExec) -> Option<(i64, i64)> {
    if !window.partition_keys().is_empty() {
        return None;
    }
    let order_by = window.window_expr()[0].order_by();
    if order_by.len() != 1 {
        return None;
    }
    if order_by[0].expr.downcast_ref::<Column>().is_none() {
        return None;
    }
    let frame = window.window_expr()[0].get_window_frame();
    if frame.units != WindowFrameUnits::Range {
        return None;
    }
    i64_halo(&frame.start_bound, &frame.end_bound)
}

/// Extract `(halo_preceding, halo_following)` in order-key units from a
/// RANGE window frame. Returns `None` for UNBOUNDED bounds or non-`Int64`
/// distances. v1 scope: only `Preceding(Int64)` / `CurrentRow` for the
/// start bound, and `CurrentRow` / `Following(Int64)` for the end bound.
fn i64_halo(start: &WindowFrameBound, end: &WindowFrameBound) -> Option<(i64, i64)> {
    let preceding = match start {
        WindowFrameBound::Preceding(ScalarValue::Int64(Some(n))) => *n,
        WindowFrameBound::CurrentRow => 0,
        _ => return None,
    };
    let following = match end {
        WindowFrameBound::Following(ScalarValue::Int64(Some(n))) => *n,
        WindowFrameBound::CurrentRow => 0,
        _ => return None,
    };
    Some((preceding, following))
}
