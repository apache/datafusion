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

//! Sort Pushdown Optimization (Phase 1)
//!
//! Phase 1 supports reverse scan optimization: when the required sort order is
//! the reverse of the data source's output ordering (or a prefix of it), we perform
//! a reverse scan at the data source level (reading row groups in reverse order).
//!
//! **Prefix Matching**: If the data has ordering [A DESC, B ASC] and the query needs
//! [A ASC], reversing gives [A ASC, B DESC] which satisfies the requirement.
//!
//! This optimization:
//! 1. Detects SortExec nodes that require a specific ordering
//! 2. Recursively traverses through transparent nodes to find data sources
//! 3. Checks if required order is reverse of output order (supports prefix matching)
//! 4. If yes, pushes down reverse scan to data source
//! 5. Returns **Inexact** ordering (keeps Sort but enables early termination)
//! 6. Phase 2 will support more complex scenarios (file reordering) and detect perfect ordering

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

/// A PhysicalOptimizerRule that attempts to push down sort requirements to data sources.
///
/// # Phase 1 Behavior (Current)
///
/// This optimization handles the case where the required sort order is the reverse
/// of the data source's output ordering (or a prefix of it):
/// - Detects when sort order is the reverse of natural output order
/// - **Supports prefix matching**: e.g., if data is [A DESC, B ASC] and query needs
///   [A ASC], reverse scan produces [A ASC, B DESC] which satisfies the requirement
/// - Pushes down reverse scan to data source (row groups read in reverse)
/// - Returns **Inexact** ordering (keeps Sort but enables early termination)
///
/// Benefits:
/// - TopK queries with reverse order: huge faster due to early termination
/// - Memory: No additional overhead (only changes read order)
/// - Works with single column or multi-column reverse ordering
/// - Prefix matching allows partial sort pushdown for better performance
///
/// # Phase 2 (Future)
///
/// Will support more complex scenarios:
/// - File reordering based on statistics
/// - Partial ordering optimizations
/// - Detect when files are perfectly sorted and:
///   - Return **Exact** ordering guarantees
///   - Completely eliminate the Sort operator
///
/// # Implementation
///
/// 1. Detects SortExec nodes
/// 2. Recursively pushes through transparent nodes (CoalesceBatches, Repartition, etc.)
/// 3. Asks data sources to optimize via `try_pushdown_sort()`
/// 4. Data source checks if required order is reverse of natural order (with prefix matching)
/// 5. If yes, performs reverse scan; if no, returns None
/// 6. Keeps Sort operator (Phase 1 returns Inexact)
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
        let enable_sort_pushdown = config.execution.parquet.enable_sort_pushdown;

        // Return early if not enabled
        if !enable_sort_pushdown {
            return Ok(plan);
        }

        // Search for any SortExec nodes and try to optimize them
        plan.transform_down(&|plan: Arc<dyn ExecutionPlan>| {
            // Check if this is a SortExec
            let sort_exec = match plan.as_any().downcast_ref::<SortExec>() {
                Some(sort_exec) => sort_exec,
                None => return Ok(Transformed::no(plan)),
            };

            optimize_sort(sort_exec)
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

/// Optimize a SortExec by potentially pushing the sort down to the data source
///
/// Phase 1: Optimizes when sort order is the reverse of natural output order.
/// Supports **prefix matching**: required order can be a prefix of the reversed order.
/// The data source will perform a reverse scan (read row groups in reverse).
fn optimize_sort(sort_exec: &SortExec) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let sort_input = Arc::clone(sort_exec.input());
    let required_ordering = sort_exec.expr();

    // Try to push the sort requirement down to the data source (with recursive traversal)
    // Phase 1: Data source will only accept if required order is reverse of natural order
    if let Some(optimized_input) = try_pushdown_sort(&sort_input, required_ordering)? {
        // Phase 1: Always keep the Sort operator
        // Even though we optimized the input (reverse scan),
        // we return Inexact ordering to maintain correctness
        //
        // Benefits:
        // - TopK queries with reverse order can terminate early
        // - Less data needs to be sorted (data is approximately ordered)
        // - Better cache locality
        return Ok(Transformed::yes(Arc::new(
            SortExec::new(required_ordering.clone(), optimized_input)
                .with_fetch(sort_exec.fetch())
                .with_preserve_partitioning(sort_exec.preserve_partitioning()),
        )));
    }

    Ok(Transformed::no(Arc::new(sort_exec.clone())))
}

/// Try to push down a sort requirement to an execution plan.
///
/// This function recursively traverses through "transparent" nodes - nodes that don't
/// fundamentally change the ordering of data - to find data sources that can natively
/// handle the sort.
///
/// **Transparent nodes** include:
/// - `CoalesceBatchesExec`: Combines small batches, preserves ordering
/// - `RepartitionExec`: May preserve ordering (if configured)
/// - `CoalescePartitionsExec`: Merges partitions, preserves ordering within partitions
///
/// # Phase 1 Behavior
///
/// In Phase 1, data sources will accept the pushdown if:
/// - The required ordering is the reverse of their natural output ordering
/// - **Supports prefix matching**: required ordering can be a prefix of the reversed order
///   (e.g., if data is [A DESC, B ASC], query needs [A ASC], reverse gives [A ASC, B DESC])
/// - They can perform a reverse scan (read row groups in reverse order)
///
/// If accepted, this returns `Some(optimized_plan)` with reverse scan enabled,
/// but does NOT guarantee perfect ordering (returns Inexact). The caller (optimize_sort)
/// will keep the Sort operator.
///
/// # Returns
/// - `Ok(Some(plan))` - Successfully pushed sort down (reverse scan) and rebuilt the tree
/// - `Ok(None)` - Cannot push sort down through this node (not reverse order case)
/// - `Err(e)` - Error occurred during optimization
fn try_pushdown_sort(
    plan: &Arc<dyn ExecutionPlan>,
    required_ordering: &[PhysicalSortExpr],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Base case: Check if the plan can natively handle the sort requirement
    // Phase 1: Data source will check if required_ordering is reverse of natural order
    let pushdown_result = plan.try_pushdown_sort(required_ordering)?;

    match pushdown_result {
        Some(optimized) => {
            // Phase 1: We got an optimized plan (reverse scan enabled)
            // In future Phase 2, we could check if result is Exact and remove Sort
            return Ok(Some(optimized));
        }
        None => {
            // Continue to recursive case
        }
    }

    // Recursive case: Try to push through transparent nodes

    // CoalesceBatchesExec - just combines batches, doesn't affect ordering
    if let Some(coalesce_batches) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        let input = coalesce_batches.input();
        if let Some(optimized_input) = try_pushdown_sort(input, required_ordering)? {
            return Ok(Some(Arc::new(CoalesceBatchesExec::new(
                optimized_input,
                coalesce_batches.target_batch_size(),
            ))));
        }
    }

    // RepartitionExec - may preserve ordering in some cases
    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        let input = repartition.input();
        if let Some(optimized_input) = try_pushdown_sort(input, required_ordering)? {
            // Rebuild the repartition with optimized input
            let new_repartition = RepartitionExec::try_new(
                optimized_input,
                repartition.partitioning().clone(),
            )?;

            // Preserve the preserve_order flag if it was set
            if repartition.maintains_input_order()[0] {
                return Ok(Some(Arc::new(new_repartition.with_preserve_order())));
            }
            return Ok(Some(Arc::new(new_repartition)));
        }
    }

    // CoalescePartitionsExec - merges partitions
    if let Some(coalesce_parts) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        let input = coalesce_parts.input();
        if let Some(optimized_input) = try_pushdown_sort(input, required_ordering)? {
            return Ok(Some(Arc::new(CoalescePartitionsExec::new(optimized_input))));
        }
    }

    // If we reach here, the node is not transparent or we couldn't optimize
    // Phase 1: Most likely the required order is not the reverse of natural order
    // (even considering prefix matching)
    Ok(None)
}
