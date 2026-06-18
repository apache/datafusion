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

//! Parallelize bounded RANGE-frame window functions that have an ORDER BY but
//! no PARTITION BY by range-partitioning their input.
//!
//! Skeleton: this pass finds candidate `BoundedWindowAggExec` nodes and probes
//! the child's `partition_statistics(Some(i))` for per-partition min/max on the
//! single ORDER BY column. It does not transform the plan yet.

use crate::PhysicalOptimizerRule;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::WindowFrameUnits;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::windows::BoundedWindowAggExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
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
        config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = config.execution.target_partitions;
        let out = plan.transform_down(|node| {
            if let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() {
                probe_candidate(window, target_partitions)?;
            }
            Ok(Transformed::no(node))
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

fn probe_candidate(
    window: &BoundedWindowAggExec,
    target_partitions: usize,
) -> datafusion_common::Result<()> {
    // v1 scope: single ORDER BY column, no PARTITION BY, RANGE frame.
    if !window.partition_keys().is_empty() {
        return Ok(());
    }
    let order_by = window.window_expr()[0].order_by();
    if order_by.len() != 1 {
        return Ok(());
    }
    let frame = window.window_expr()[0].get_window_frame();
    if frame.units != WindowFrameUnits::Range {
        return Ok(());
    }

    let child = window.input();
    let Some(col) = order_by[0].expr.downcast_ref::<Column>() else {
        return Ok(());
    };
    let col_idx = col.index();
    let col_name = col.name().to_string();
    let n_in = child.output_partitioning().partition_count();

    info!(
        "ParallelWindow: candidate BoundedWindowAggExec on `{col_name}` (RANGE frame, no PARTITION BY); \
         child has {n_in} partitions, target_partitions={target_partitions}"
    );

    let mut global_min: Precision<ScalarValue> = Precision::Absent;
    let mut global_max: Precision<ScalarValue> = Precision::Absent;
    for i in 0..n_in {
        let stats = child.partition_statistics(Some(i))?;
        let col_stats = &stats.column_statistics[col_idx];
        global_min = match global_min {
            Precision::Absent => col_stats.min_value.clone(),
            other => other.min(&col_stats.min_value),
        };
        global_max = match global_max {
            Precision::Absent => col_stats.max_value.clone(),
            other => other.max(&col_stats.max_value),
        };
    }

    let (Precision::Exact(min), Precision::Exact(max)) = (&global_min, &global_max)
    else {
        info!("  global bounds not Exact (min={global_min:?}, max={global_max:?}); skip");
        return Ok(());
    };

    let Some(boundaries) = equal_width_boundaries(min, max, target_partitions) else {
        info!(
            "  cannot split [{min:?}, {max:?}] into {target_partitions} buckets (type not yet supported)"
        );
        return Ok(());
    };
    info!("  global min={min:?} max={max:?}; interior boundaries: {boundaries:?}");

    Ok(())
}

/// Split the closed interval `[min, max]` into `n` equal-width buckets and
/// return the `n - 1` interior cut points.
/// v1: Int64 only — keeps the API small while we settle the optimizer shape.
fn equal_width_boundaries(
    min: &ScalarValue,
    max: &ScalarValue,
    n: usize,
) -> Option<Vec<ScalarValue>> {
    if n <= 1 {
        return Some(vec![]);
    }
    let (ScalarValue::Int64(Some(lo)), ScalarValue::Int64(Some(hi))) = (min, max) else {
        return None;
    };
    let span = hi.checked_sub(*lo)?;
    let n_i = i64::try_from(n).ok()?;
    let mut cuts = Vec::with_capacity(n - 1);
    for i in 1..n_i {
        cuts.push(ScalarValue::Int64(Some(lo + span * i / n_i)));
    }
    Some(cuts)
}
