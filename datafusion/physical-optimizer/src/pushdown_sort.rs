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

//! Sort Pushdown Optimization (Enhanced Version)
//!
//! This enhanced version supports pushing sorts through multiple layers of transparent nodes.

use crate::{OptimizerContext, PhysicalOptimizerRule};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// A PhysicalOptimizerRule that attempts to push down sort requirements to data sources
/// that can natively handle them (e.g., by reversing scan direction).
///
/// **Enhanced Features**:
/// - Recursively pushes through transparent nodes (CoalesceBatches, Repartition, etc.)
/// - Supports multi-layer nesting of operators
///
/// This optimization:
/// 1. Detects SortExec nodes that require a specific ordering
/// 2. Recursively traverses through transparent nodes to find data sources
/// 3. Pushes the sort requirement down to the data source when possible
/// 4. Rebuilds the entire operator tree with optimized nodes
/// 5. Removes unnecessary sort operations when the input already satisfies the requirement
#[derive(Debug, Clone, Default)]
pub struct PushdownSort;

impl PushdownSort {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for PushdownSort {
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Check if sort pushdown optimization is enabled
        let enable_sort_pushdown = context
            .session_config()
            .options()
            .execution
            .parquet
            .enable_sort_pushdown;

        // Return early if not enabled
        if !enable_sort_pushdown {
            return Ok(plan);
        }

        // Search for any SortExec nodes and try to optimize them
        plan.transform_down(&|plan: Arc<dyn ExecutionPlan>| {
            // First check if this is a GlobalLimitExec -> SortExec pattern
            if let Some(limit_exec) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
                if let Some(sort_exec) =
                    limit_exec.input().as_any().downcast_ref::<SortExec>()
                {
                    return optimize_limit_sort(limit_exec, sort_exec);
                }
            }

            // Otherwise, check if this is just a SortExec
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
fn optimize_sort(sort_exec: &SortExec) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let sort_input = Arc::clone(sort_exec.input());
    let required_ordering = sort_exec.expr();

    // First, check if the sort is already satisfied by input ordering
    if let Some(_input_ordering) = sort_input.output_ordering() {
        let input_eq_properties = sort_input.equivalence_properties();

        if input_eq_properties.ordering_satisfy(required_ordering.clone())? {
            return remove_unnecessary_sort(sort_exec, sort_input);
        }
    }

    // Try to push the sort requirement down to the data source (with recursive traversal)
    if let Some(optimized_input) = try_pushdown_sort(&sort_input, required_ordering)? {
        // Verify that the optimized input satisfies the required ordering
        if optimized_input
            .equivalence_properties()
            .ordering_satisfy(required_ordering.clone())?
        {
            return remove_unnecessary_sort(sort_exec, optimized_input);
        }

        // If not fully satisfied, keep the sort but with optimized input
        return Ok(Transformed::yes(Arc::new(
            SortExec::new(required_ordering.clone(), optimized_input)
                .with_fetch(sort_exec.fetch())
                .with_preserve_partitioning(sort_exec.preserve_partitioning()),
        )));
    }

    Ok(Transformed::no(Arc::new(sort_exec.clone())))
}

/// Handle the GlobalLimitExec -> SortExec pattern
fn optimize_limit_sort(
    limit_exec: &GlobalLimitExec,
    sort_exec: &SortExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let sort_input = Arc::clone(sort_exec.input());
    let required_ordering = sort_exec.expr();

    // Check if input is already sorted
    if let Some(_input_ordering) = sort_input.output_ordering() {
        let input_eq_properties = sort_input.equivalence_properties();
        if input_eq_properties.ordering_satisfy(required_ordering.clone())? {
            // Input is already sorted correctly, remove sort and keep limit
            return Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                sort_input,
                limit_exec.skip(),
                limit_exec.fetch(),
            ))));
        }
    }

    // Try to push down the sort requirement
    if let Some(optimized_input) = try_pushdown_sort(&sort_input, required_ordering)? {
        if optimized_input
            .equivalence_properties()
            .ordering_satisfy(required_ordering.clone())?
        {
            // Successfully pushed down sort, now handle the limit
            let total_fetch = limit_exec.skip() + limit_exec.fetch().unwrap_or(0);

            // Try to push limit down as well if the source supports it
            if let Some(with_fetch) = optimized_input.with_fetch(Some(total_fetch)) {
                if limit_exec.skip() > 0 {
                    return Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                        with_fetch,
                        limit_exec.skip(),
                        limit_exec.fetch(),
                    ))));
                } else {
                    return Ok(Transformed::yes(with_fetch));
                }
            }

            return Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                optimized_input,
                limit_exec.skip(),
                limit_exec.fetch(),
            ))));
        }
    }

    // Can't optimize, return original pattern
    Ok(Transformed::no(Arc::new(GlobalLimitExec::new(
        Arc::new(sort_exec.clone()),
        limit_exec.skip(),
        limit_exec.fetch(),
    ))))
}

/// Remove unnecessary sort based on the logic from EnforceSorting::analyze_immediate_sort_removal
fn remove_unnecessary_sort(
    sort_exec: &SortExec,
    sort_input: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let new_plan = if !sort_exec.preserve_partitioning()
        && sort_input.output_partitioning().partition_count() > 1
    {
        // Replace the sort with a sort-preserving merge
        Arc::new(
            SortPreservingMergeExec::new(sort_exec.expr().clone(), sort_input)
                .with_fetch(sort_exec.fetch()),
        ) as _
    } else {
        // Remove the sort entirely
        if let Some(fetch) = sort_exec.fetch() {
            // If the sort has a fetch, add a limit instead
            if sort_input.output_partitioning().partition_count() == 1 {
                // Try to push the limit down to the source
                if let Some(with_fetch) = sort_input.with_fetch(Some(fetch)) {
                    return Ok(Transformed::yes(with_fetch));
                }
                Arc::new(GlobalLimitExec::new(sort_input, 0, Some(fetch)))
                    as Arc<dyn ExecutionPlan>
            } else {
                Arc::new(LocalLimitExec::new(sort_input, fetch)) as Arc<dyn ExecutionPlan>
            }
        } else {
            sort_input
        }
    };

    Ok(Transformed::yes(new_plan))
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
/// # Returns
/// - `Ok(Some(plan))` - Successfully pushed sort down and rebuilt the tree
/// - `Ok(None)` - Cannot push sort down through this node
/// - `Err(e)` - Error occurred during optimization
fn try_pushdown_sort(
    plan: &Arc<dyn ExecutionPlan>,
    required_ordering: &[PhysicalSortExpr],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Base case: Check if the plan can natively handle the sort requirement
    if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        return data_source_exec.try_pushdown_sort(required_ordering);
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
    Ok(None)
}
