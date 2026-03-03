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

//! Sort Pushdown Optimization
//!
//! This optimizer attempts to push sort requirements down through the execution plan
//! tree to data sources that can natively handle them (e.g., by scanning files in
//! reverse order).
//!
//! ## How it works
//!
//! 1. Detects `SortExec` nodes in the plan
//! 2. Calls `try_pushdown_sort()` on the input to recursively push the sort requirement
//! 3. Each node type defines its own pushdown behavior:
//!    - **Transparent nodes** (CoalesceBatchesExec, RepartitionExec, etc.) delegate to
//!      their children and wrap the result
//!    - **Data sources** (DataSourceExec) check if they can optimize for the ordering
//!    - **Blocking nodes** return `Unsupported` to stop pushdown
//! 4. Based on the result:
//!    - `Exact`: Remove the Sort operator (data source guarantees perfect ordering)
//!    - `Inexact`: Keep Sort but use optimized input (enables early termination for TopK)
//!    - `Unsupported`: No change
//!
//! ## Current capabilities (Phase 1)
//!
//! - Reverse scan optimization: when required sort is the reverse of the data source's
//!   natural ordering, enable reverse scanning (reading row groups in reverse order)
//! - Supports prefix matching: if data has ordering [A DESC, B ASC] and query needs
//!   [A ASC], reversing gives [A ASC, B DESC] which satisfies the requirement
//!
//! TODO Issue: <https://github.com/apache/datafusion/issues/19329>
//! ## Future enhancements (Phase 2),
//!
//! - File reordering based on statistics
//! - Return `Exact` when files are known to be perfectly sorted
//! - Complete Sort elimination when ordering is guaranteed

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::SortOrderPushdownResult;
use datafusion_physical_plan::sorts::sort::SortExec;
use std::sync::Arc;

/// A PhysicalOptimizerRule that attempts to push down sort requirements to data sources.
///
/// See module-level documentation for details.
#[derive(Debug, Clone, Default)]
pub struct PushdownSort;

impl PushdownSort {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for PushdownSort {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Check if sort pushdown optimization is enabled
        if !config.optimizer.enable_sort_pushdown {
            return Ok(plan);
        }

        // Use transform_down to find and optimize all SortExec nodes (including nested ones)
        plan.transform_down(|plan: Arc<dyn ExecutionPlan>| {
            // Check if this is a SortExec
            let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(plan));
            };

            let sort_input = Arc::clone(sort_exec.input());
            let required_ordering = sort_exec.expr();

            // Try to push the sort requirement down through the plan tree
            // Each node type defines its own pushdown behavior via try_pushdown_sort()
            match sort_input.try_pushdown_sort(required_ordering)? {
                SortOrderPushdownResult::Exact { inner } => {
                    // Data source guarantees perfect ordering - remove the Sort operator
                    Ok(Transformed::yes(inner))
                }
                SortOrderPushdownResult::Inexact { inner } => {
                    // Data source is optimized for the ordering but not perfectly sorted
                    // Keep the Sort operator but use the optimized input
                    // Benefits: TopK queries can terminate early, better cache locality
                    Ok(Transformed::yes(Arc::new(
                        SortExec::new(required_ordering.clone(), inner)
                            .with_fetch(sort_exec.fetch())
                            .with_preserve_partitioning(
                                sort_exec.preserve_partitioning(),
                            ),
                    )))
                }
                SortOrderPushdownResult::Unsupported => {
                    // Cannot optimize for this ordering - no change
                    Ok(Transformed::no(plan))
                }
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "PushdownSort"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
