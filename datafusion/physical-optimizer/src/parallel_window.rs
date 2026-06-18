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
//! but no PARTITION BY by inserting a `RangeRepartitionExec` between the
//! window's `SortPreservingMergeExec`/`SortExec` chain and its child sort.
//!
//! This rule's responsibilities are now narrow: detect the eligible
//! window shape, pull the halo distances out of the window frame, and
//! wrap the per-partition `SortExec` with a `RangeRepartitionExec`
//! carrying those halo values. All boundary / global-extremes logic
//! lives in `RangeRepartitionExec`'s coordinator and runs against
//! runtime stats rather than plan-time `Statistics`.

use crate::PhysicalOptimizerRule;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::range_repartition::RangeRepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
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
            // Descend through whatever EnsureRequirements stacked on top of
            // the sort (typically SortPreservingMergeExec) until we find the
            // SortExec, and wrap it with RangeRepartitionExec(halo…).
            let new_child = wrap_first_sort_descendant(
                Arc::clone(&node.children()[0]),
                halo_preceding,
                halo_following,
            )?;
            let new_window = Arc::clone(&node).with_new_children(vec![new_child])?;
            Ok(Transformed::yes(new_window))
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

/// Walk down through single-child operators until we hit a `SortExec`;
/// replace it in place with `RangeRepartitionExec` wrapping the same
/// `SortExec`, and rebuild the chain back up. If no `SortExec` is found,
/// leave the subtree alone.
fn wrap_first_sort_descendant(
    node: Arc<dyn ExecutionPlan>,
    halo_preceding: i64,
    halo_following: i64,
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    if node.downcast_ref::<SortExec>().is_some() {
        return Ok(Arc::new(RangeRepartitionExec::new(
            node,
            halo_preceding,
            halo_following,
        )));
    }
    let children = node.children();
    if children.len() != 1 {
        return Ok(node);
    }
    let new_child = wrap_first_sort_descendant(
        Arc::clone(children[0]),
        halo_preceding,
        halo_following,
    )?;
    node.with_new_children(vec![new_child])
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
