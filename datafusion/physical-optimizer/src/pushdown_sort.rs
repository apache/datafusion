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
//! ## Capabilities
//!
//! - **Sort elimination**: when a data source's natural ordering satisfies the
//!   request, return `Exact` and remove the `SortExec` entirely. Preserves
//!   `fetch` (LIMIT) from the eliminated `SortExec` for early termination.
//! - **Statistics-based file sorting**: sort files within each partition by
//!   min/max statistics. When files are non-overlapping but listed in wrong
//!   order (e.g., alphabetical order ≠ sort key order), this fixes the ordering
//!   and enables sort elimination. Works for both single-partition and
//!   multi-partition plans with multi-file groups.
//! - **Reverse scan optimization**: when required sort is the reverse of the data source's
//!   natural ordering, enable reverse scanning (reading row groups in reverse order)
//! - **Prefix matching**: if data has ordering [A DESC, B ASC] and query needs
//!   [A DESC], the existing ordering satisfies the requirement (`Exact`).
//!   If the query needs [A ASC] (reverse of the prefix), a reverse scan is
//!   used (`Inexact`, `SortExec` retained)
//!
//! Related issue: <https://github.com/apache/datafusion/issues/17348>

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::SortOrderPushdownResult;
use datafusion_physical_plan::buffer::BufferExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
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

        let buffer_capacity = config.execution.sort_pushdown_buffer_capacity;

        // Use transform_down to find and optimize all SortExec nodes (including nested ones)
        // Also handles SPM → SortExec pattern to insert BufferExec when sort is eliminated
        plan.transform_down(|plan: Arc<dyn ExecutionPlan>| {
            // Pattern 1: SPM → SortExec(preserve_partitioning)
            // When we eliminate the SortExec, SPM loses its memory buffer and reads
            // directly from I/O-bound sources. Insert a BufferExec to compensate.
            if let Some(spm) = plan.downcast_ref::<SortPreservingMergeExec>()
                && let Some(sort_child) = spm.input().downcast_ref::<SortExec>()
                && sort_child.preserve_partitioning()
            {
                let sort_input = Arc::clone(sort_child.input());
                let required_ordering = sort_child.expr();
                match sort_input.try_pushdown_sort(required_ordering)? {
                    SortOrderPushdownResult::Exact { inner } => {
                        // Preserve fetch (LIMIT) from the eliminated SortExec.
                        // Use LocalLimitExec (not Global) since input is multi-partition.
                        let inner = if let Some(fetch) = sort_child.fetch() {
                            inner.with_fetch(Some(fetch)).unwrap_or_else(|| {
                                Arc::new(LocalLimitExec::new(inner, fetch))
                            })
                        } else {
                            inner
                        };
                        // Insert BufferExec to replace SortExec's buffering role.
                        // SortExec buffered all data in memory; BufferExec provides
                        // bounded buffering so SPM doesn't stall on I/O.
                        let buffered: Arc<dyn ExecutionPlan> =
                            Arc::new(BufferExec::new(inner, buffer_capacity));
                        let new_spm =
                            SortPreservingMergeExec::new(spm.expr().clone(), buffered)
                                .with_fetch(spm.fetch());
                        return Ok(Transformed::yes(Arc::new(new_spm)));
                    }
                    SortOrderPushdownResult::Inexact { inner } => {
                        let new_sort = SortExec::new(required_ordering.clone(), inner)
                            .with_fetch(sort_child.fetch())
                            .with_preserve_partitioning(true);
                        let new_spm = SortPreservingMergeExec::new(
                            spm.expr().clone(),
                            Arc::new(new_sort),
                        )
                        .with_fetch(spm.fetch());
                        return Ok(Transformed::yes(Arc::new(new_spm)));
                    }
                    SortOrderPushdownResult::Unsupported => {
                        return Ok(Transformed::no(plan));
                    }
                }
            }

            // Pattern 2: Standalone SortExec (no SPM parent)
            let Some(sort_exec) = plan.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(plan));
            };

            let sort_input = Arc::clone(sort_exec.input());
            let required_ordering = sort_exec.expr();

            // Try to push the sort requirement down through the plan tree
            // Each node type defines its own pushdown behavior via try_pushdown_sort()
            match sort_input.try_pushdown_sort(required_ordering)? {
                SortOrderPushdownResult::Exact { inner } => {
                    // Data source guarantees perfect ordering - remove the Sort operator.
                    //
                    // If the SortExec carried a fetch (LIMIT), we must preserve it.
                    // First try pushing the limit into the source via `with_fetch()`.
                    // If the source doesn't support `with_fetch`, fall back to
                    // wrapping with GlobalLimitExec.
                    if let Some(fetch) = sort_exec.fetch() {
                        let inner = inner.with_fetch(Some(fetch)).unwrap_or_else(|| {
                            Arc::new(GlobalLimitExec::new(inner, 0, Some(fetch)))
                        });
                        Ok(Transformed::yes(inner))
                    } else {
                        Ok(Transformed::yes(inner))
                    }
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
