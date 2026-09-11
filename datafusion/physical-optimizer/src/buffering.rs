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

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{JoinSide, Result, Statistics};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::buffer::BufferExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use std::sync::Arc;

/// The [`BufferInsertion`] optimizer rule places [`BufferExec`] nodes in the physical plan to
/// compute multiple parts of a single partition in parallel to cut down on latency. This is done
/// in two scenarios:
///
/// 1. **Hash Join Probe Buffering**: For [`HashJoinExec`], buffers the probe side with
///    capacity `config.execution.hash_join_buffering_capacity` so that the probe side can
///    be eagerly polled while the build side is concurrently being built.
/// 2. **Small Scan Buffering**: For data source scans whose statistics indicate that they
///    are smaller than `config.execution.small_scan_buffering_threshold`, we wrap them in
///    a [`BufferExec`].
///
/// ## `HashJoinExec` Buffering
///
/// Looks for all the [HashJoinExec]s in the plan and places a [BufferExec] node with the
/// configured capacity in the probe side:
///
/// ```text
///            ┌───────────────────┐
///            │   HashJoinExec    │
///            └─────▲────────▲────┘
///          ┌───────┘        └─────────┐
///          │                          │
/// ┌────────────────┐         ┌─────────────────┐
/// │   Build side   │       + │   BufferExec    │
/// └────────────────┘         └────────▲────────┘
///                                     │
///                            ┌────────┴────────┐
///                            │   Probe side    │
///                            └─────────────────┘
/// ```
///
/// Which allows eagerly pulling it even before the build side has completely finished.
///
/// ## Small Scan Buffering
///
/// Whenever a small "scan" (leaf node) is detected, a [`BufferExec`] is inserted with the goal of
/// reducing query latency as the I/O of the small scan is executed eagerly. As the scan is
/// considered small, dynamic filters may not yield significant improvements that warrant delaying
/// the I/O until they are fully available.
///
/// ```text
///            ┌───────────────────┐
///            │   MyOtherExec     │
///            └─────▲────────▲────┘
///          ┌───────┘        └─────────┐
///          │                          │
/// ┌────────────────┐         ┌─────────────────┐
/// │ Scan (200 MiB) │       + │   BufferExec    │
/// └────────────────┘         └────────▲────────┘
///                                     │
///                            ┌────────┴────────┐
///                            │   Scan (1 MiB)  │
///                            └─────────────────┘
/// ```
#[derive(Debug, Default)]
pub struct BufferInsertion {}

impl BufferInsertion {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for BufferInsertion {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let hash_join_capacity = config.execution.hash_join_buffering_capacity;
        let small_scan_threshold = config.execution.small_scan_buffering_threshold;

        if hash_join_capacity == 0 && small_scan_threshold == 0 {
            return Ok(plan);
        }

        let stats_ctx = StatisticsContext::new();

        transform_plan(
            plan,
            hash_join_capacity,
            small_scan_threshold,
            &stats_ctx,
            false,
        )
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "BufferInsertion"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn transform_plan(
    plan: Arc<dyn ExecutionPlan>,
    hash_join_capacity: usize,
    small_scan_threshold: usize,
    stats_ctx: &StatisticsContext,
    in_buffer: bool,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if plan.is::<BufferExec>() {
        // Prevent stacking BufferExec nodes together and avoid double-buffering scans within it.
        return plan.map_children(|child| {
            transform_plan(
                child,
                hash_join_capacity,
                small_scan_threshold,
                stats_ctx,
                true,
            )
        });
    }

    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        let probe_is_left = HashJoinExec::probe_side() == JoinSide::Left;
        let probe_child = if probe_is_left {
            &join.left
        } else {
            &join.right
        };
        let build_child = if probe_is_left {
            &join.right
        } else {
            &join.left
        };

        let transformed_build = transform_plan(
            Arc::clone(build_child),
            hash_join_capacity,
            small_scan_threshold,
            stats_ctx,
            in_buffer,
        )?;

        let (transformed_probe, probe_transformed) =
            if hash_join_capacity > 0 && !probe_child.is::<BufferExec>() {
                // Buffer the probe side. Since the probe child is wrapped in BufferExec,
                // scans within that probe side should NOT be double-buffered (`in_buffer: true`).
                let probe_inner = transform_plan(
                    Arc::clone(probe_child),
                    hash_join_capacity,
                    small_scan_threshold,
                    stats_ctx,
                    true,
                )?;
                let buffered: Arc<dyn ExecutionPlan> =
                    Arc::new(BufferExec::new(probe_inner.data, hash_join_capacity));
                (buffered, true)
            } else {
                let probe_res = transform_plan(
                    Arc::clone(probe_child),
                    hash_join_capacity,
                    small_scan_threshold,
                    stats_ctx,
                    in_buffer,
                )?;
                (probe_res.data, probe_res.transformed)
            };

        if transformed_build.transformed || probe_transformed {
            let (new_left, new_right) = if probe_is_left {
                (transformed_probe, transformed_build.data)
            } else {
                (transformed_build.data, transformed_probe)
            };
            let new_plan =
                replace_children_if_necessary(plan, vec![new_left, new_right])?;
            return Ok(Transformed::yes(new_plan));
        } else {
            return Ok(Transformed::no(plan));
        }
    }

    if !in_buffer
        && is_scan(&plan)
        && is_small_scan(&plan, small_scan_threshold, stats_ctx)?
    {
        let buffered: Arc<dyn ExecutionPlan> =
            Arc::new(BufferExec::new(plan, small_scan_threshold));
        return Ok(Transformed::yes(buffered));
    }

    plan.map_children(|child| {
        transform_plan(
            child,
            hash_join_capacity,
            small_scan_threshold,
            stats_ctx,
            in_buffer,
        )
    })
}

fn is_scan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.children().is_empty()
        && !plan.is::<BufferExec>()
        && !plan.is::<EmptyExec>()
        && !plan.is::<PlaceholderRowExec>()
}

fn is_small_scan(
    plan: &Arc<dyn ExecutionPlan>,
    threshold: usize,
    stats_ctx: &StatisticsContext,
) -> Result<bool> {
    if threshold == 0 {
        return Ok(false);
    }

    let overall_stats = stats_ctx.compute(plan.as_ref(), &StatisticsArgs::default())?;
    Ok(get_total_byte_size(&overall_stats)
        .is_some_and(|total_size| total_size > 0 && total_size <= threshold))
}

fn get_total_byte_size(stats: &Statistics) -> Option<usize> {
    if let Some(&size) = stats.total_byte_size.get_value() {
        return Some(size);
    }
    if !stats.column_statistics.is_empty() {
        let mut sum = 0usize;
        for col in &stats.column_statistics {
            let bytes = col.byte_size.get_value()?;
            sum = sum.saturating_add(*bytes);
        }
        return Some(sum);
    }
    None
}
