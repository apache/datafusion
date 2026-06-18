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
use datafusion_common::config::ConfigOptions;
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
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let out = plan.transform_down(|node| {
            if let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() {
                probe_candidate(window)?;
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

fn probe_candidate(window: &BoundedWindowAggExec) -> datafusion_common::Result<()> {
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
    let n = child.output_partitioning().partition_count();

    info!(
        "ParallelWindow: candidate BoundedWindowAggExec on `{col_name}` (RANGE frame, no PARTITION BY); \
         child has {n} partitions"
    );

    for i in 0..n {
        let stats = child.partition_statistics(Some(i))?;
        let col_stats = &stats.column_statistics[col_idx];
        info!(
            "  partition {i}: min={:?}  max={:?}  rows={:?}",
            col_stats.min_value, col_stats.max_value, stats.num_rows
        );
    }

    Ok(())
}
