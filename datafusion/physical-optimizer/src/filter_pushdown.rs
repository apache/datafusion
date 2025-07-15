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

//! Filter Pushdown Optimization Process
//!
//! The filter pushdown mechanism involves four key steps:
//! 1. **Optimizer Asks Parent for a Filter Pushdown Plan**: The optimizer calls [`ExecutionPlan::gather_filters_for_pushdown`]
//!    on the parent node, passing in parent predicates and phase. The parent node creates a [`FilterDescription`]
//!    by inspecting its logic and children's schemas, determining which filters can be pushed to each child.
//! 2. **Optimizer Executes Pushdown**: The optimizer recursively calls `push_down_filters` in this module on each child,
//!    passing the appropriate filters (`Vec<Arc<dyn PhysicalExpr>>`) for that child.
//! 3. **Optimizer Gathers Results**: The optimizer collects [`FilterPushdownPropagation`] results from children,
//!    containing information about which filters were successfully pushed down vs. unsupported.
//! 4. **Parent Responds**: The optimizer calls [`ExecutionPlan::handle_child_pushdown_result`] on the parent,
//!    passing a [`ChildPushdownResult`] containing the aggregated pushdown outcomes. The parent decides
//!    how to handle filters that couldn't be pushed down (e.g., keep them as FilterExec nodes).
//!
//! [`FilterDescription`]: datafusion_physical_plan::filter_pushdown::FilterDescription

use std::sync::Arc;

use crate::PhysicalOptimizerRule;

use datafusion_common::{config::ConfigOptions, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::filter_pushdown::{
    ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDown,
};
use datafusion_physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use itertools::{izip, Itertools};

/// Attempts to recursively push given filters from the top of the tree into leafs.
///
/// # Default Implementation
///
/// The default implementation in [`ExecutionPlan::gather_filters_for_pushdown`]
/// and [`ExecutionPlan::handle_child_pushdown_result`] assumes that:
///
/// * Parent filters can't be passed onto children (determined by [`ExecutionPlan::gather_filters_for_pushdown`])
/// * This node has no filters to contribute (determined by [`ExecutionPlan::gather_filters_for_pushdown`]).
/// * Any filters that could not be pushed down to the children are marked as unsupported (determined by [`ExecutionPlan::handle_child_pushdown_result`]).
///
/// # Example: Push filter into a `DataSourceExec`
///
/// For example, consider the following plan:
///
/// ```text
/// ┌──────────────────────┐
/// │ CoalesceBatchesExec  │
/// └──────────────────────┘
///             │
///             ▼
/// ┌──────────────────────┐
/// │      FilterExec      │
/// │  filters = [ id=1]   │
/// └──────────────────────┘
///             │
///             ▼
/// ┌──────────────────────┐
/// │    DataSourceExec    │
/// │    projection = *    │
/// └──────────────────────┘
/// ```
///
/// Our goal is to move the `id = 1` filter from the [`FilterExec`] node to the `DataSourceExec` node.
///
/// If this filter is selective pushing it into the scan can avoid massive
/// amounts of data being read from the source (the projection is `*` so all
/// matching columns are read).
///
/// The new plan looks like:
///
/// ```text
/// ┌──────────────────────┐
/// │ CoalesceBatchesExec  │
/// └──────────────────────┘
///           │
///           ▼
/// ┌──────────────────────┐
/// │    DataSourceExec    │
/// │    projection = *    │
/// │   filters = [ id=1]  │
/// └──────────────────────┘
/// ```
///
/// # Example: Push filters with `ProjectionExec`
///
/// Let's consider a more complex example involving a [`ProjectionExec`]
/// node in between the [`FilterExec`] and `DataSourceExec` nodes that
/// creates a new column that the filter depends on.
///
/// ```text
/// ┌──────────────────────┐
/// │ CoalesceBatchesExec  │
/// └──────────────────────┘
///             │
///             ▼
/// ┌──────────────────────┐
/// │      FilterExec      │
/// │    filters =         │
/// │     [cost>50,id=1]   │
/// └──────────────────────┘
///             │
///             ▼
/// ┌──────────────────────┐
/// │    ProjectionExec    │
/// │ cost = price * 1.2   │
/// └──────────────────────┘
///             │
///             ▼
/// ┌──────────────────────┐
/// │    DataSourceExec    │
/// │    projection = *    │
/// └──────────────────────┘
/// ```
///
/// We want to push down the filters `[id=1]` to the `DataSourceExec` node,
/// but can't push down `cost>50` because it requires the [`ProjectionExec`]
/// node to be executed first. A simple thing to do would be to split up the
/// filter into two separate filters and push down the first one:
///
/// ```text
/// ┌──────────────────────┐
/// │ CoalesceBatchesExec  │
/// └──────────────────────┘
///             │
///             ▼
/// ┌──────────────────────┐
/// │      FilterExec      │
/// │    filters =         │
/// │     [cost>50]        │
/// └──────────────────────┘
///             │
///             ▼
/// ┌──────────────────────┐
/// │    ProjectionExec    │
/// │ cost = price * 1.2   │
/// └──────────────────────┘
///             │
///             ▼
/// ┌──────────────────────┐
/// │    DataSourceExec    │
/// │    projection = *    │
/// │   filters = [ id=1]  │
/// └──────────────────────┘
/// ```
///
/// We can actually however do better by pushing down `price * 1.2 > 50`
/// instead of `cost > 50`:
///
/// ```text
/// ┌──────────────────────┐
/// │ CoalesceBatchesExec  │
/// └──────────────────────┘
///            │
///            ▼
/// ┌──────────────────────┐
/// │    ProjectionExec    │
/// │ cost = price * 1.2   │
/// └──────────────────────┘
///            │
///            ▼
/// ┌──────────────────────┐
/// │    DataSourceExec    │
/// │    projection = *    │
/// │   filters = [id=1,   │
/// │   price * 1.2 > 50]  │
/// └──────────────────────┘
/// ```
///
/// # Example: Push filters within a subtree
///
/// There are also cases where we may be able to push down filters within a
/// subtree but not the entire tree. A good example of this is aggregation
/// nodes:
///
/// ```text
/// ┌──────────────────────┐
/// │ ProjectionExec       │
/// │ projection = *       │
/// └──────────────────────┘
///           │
///           ▼
/// ┌──────────────────────┐
/// │ FilterExec           │
/// │ filters = [sum > 10] │
/// └──────────────────────┘
///           │
///           ▼
/// ┌───────────────────────┐
/// │     AggregateExec     │
/// │    group by = [id]    │
/// │    aggregate =        │
/// │      [sum(price)]     │
/// └───────────────────────┘
///           │
///           ▼
/// ┌──────────────────────┐
/// │ FilterExec           │
/// │ filters = [id=1]     │
/// └──────────────────────┘
///          │
///          ▼
/// ┌──────────────────────┐
/// │ DataSourceExec       │
/// │ projection = *       │
/// └──────────────────────┘
/// ```
///
/// The transformation here is to push down the `id=1` filter to the
/// `DataSourceExec` node:
///
/// ```text
/// ┌──────────────────────┐
/// │ ProjectionExec       │
/// │ projection = *       │
/// └──────────────────────┘
///           │
///           ▼
/// ┌──────────────────────┐
/// │ FilterExec           │
/// │ filters = [sum > 10] │
/// └──────────────────────┘
///           │
///           ▼
/// ┌───────────────────────┐
/// │     AggregateExec     │
/// │    group by = [id]    │
/// │    aggregate =        │
/// │      [sum(price)]     │
/// └───────────────────────┘
///           │
///           ▼
/// ┌──────────────────────┐
/// │ DataSourceExec       │
/// │ projection = *       │
/// │ filters = [id=1]     │
/// └──────────────────────┘
/// ```
///
/// The point here is that:
/// 1. We cannot push down `sum > 10` through the [`AggregateExec`] node into the `DataSourceExec` node.
///    Any filters above the [`AggregateExec`] node are not pushed down.
///    This is determined by calling [`ExecutionPlan::gather_filters_for_pushdown`] on the [`AggregateExec`] node.
/// 2. We need to keep recursing into the tree so that we can discover the other [`FilterExec`] node and push
///    down the `id=1` filter.
///
/// # Example: Push filters through Joins
///
/// It is also possible to push down filters through joins and filters that
/// originate from joins. For example, a hash join where we build a hash
/// table of the left side and probe the right side (ignoring why we would
/// choose this order, typically it depends on the size of each table,
/// etc.).
///
/// ```text
///              ┌─────────────────────┐
///              │     FilterExec      │
///              │ filters =           │
///              │  [d.size > 100]     │
///              └─────────────────────┘
///                         │
///                         │
///              ┌──────────▼──────────┐
///              │                     │
///              │    HashJoinExec     │
///              │ [u.dept@hash(d.id)] │
///              │                     │
///              └─────────────────────┘
///                         │
///            ┌────────────┴────────────┐
/// ┌──────────▼──────────┐   ┌──────────▼──────────┐
/// │   DataSourceExec    │   │   DataSourceExec    │
/// │  alias [users as u] │   │  alias [dept as d]  │
/// │                     │   │                     │
/// └─────────────────────┘   └─────────────────────┘
/// ```
///
/// There are two pushdowns we can do here:
/// 1. Push down the `d.size > 100` filter through the `HashJoinExec` node to the `DataSourceExec`
///    node for the `departments` table.
/// 2. Push down the hash table state from the `HashJoinExec` node to the `DataSourceExec` node to avoid reading
///    rows from the `users` table that will be eliminated by the join.
///    This can be done via a bloom filter or similar and is not (yet) supported
///    in DataFusion. See <https://github.com/apache/datafusion/issues/7955>.
///
/// ```text
///              ┌─────────────────────┐
///              │                     │
///              │    HashJoinExec     │
///              │ [u.dept@hash(d.id)] │
///              │                     │
///              └─────────────────────┘
///                         │
///            ┌────────────┴────────────┐
/// ┌──────────▼──────────┐   ┌──────────▼──────────┐
/// │   DataSourceExec    │   │   DataSourceExec    │
/// │  alias [users as u] │   │  alias [dept as d]  │
/// │ filters =           │   │  filters =          │
/// │   [depg@hash(d.id)] │   │    [ d.size > 100]  │
/// └─────────────────────┘   └─────────────────────┘
/// ```
///
/// You may notice in this case that the filter is *dynamic*: the hash table
/// is built _after_ the `departments` table is read and at runtime. We
/// don't have a concrete `InList` filter or similar to push down at
/// optimization time. These sorts of dynamic filters are handled by
/// building a specialized [`PhysicalExpr`] that can be evaluated at runtime
/// and internally maintains a reference to the hash table or other state.
///
/// To make working with these sorts of dynamic filters more tractable we have the method [`PhysicalExpr::snapshot`]
/// which attempts to simplify a dynamic filter into a "basic" non-dynamic filter.
/// For a join this could mean converting it to an `InList` filter or a min/max filter for example.
/// See `datafusion/physical-plan/src/dynamic_filters.rs` for more details.
///
/// # Example: Push TopK filters into Scans
///
/// Another form of dynamic filter is pushing down the state of a `TopK`
/// operator for queries like `SELECT * FROM t ORDER BY id LIMIT 10`:
///
/// ```text
/// ┌──────────────────────┐
/// │       TopK           │
/// │     limit = 10       │
/// │   order by = [id]    │
/// └──────────────────────┘
///            │
///            ▼
/// ┌──────────────────────┐
/// │    DataSourceExec    │
/// │    projection = *    │
/// └──────────────────────┘
/// ```
///
/// We can avoid large amounts of data processing by transforming this into:
///
/// ```text
/// ┌──────────────────────┐
/// │       TopK           │
/// │     limit = 10       │
/// │   order by = [id]    │
/// └──────────────────────┘
///            │
///            ▼
/// ┌──────────────────────┐
/// │    DataSourceExec    │
/// │    projection = *    │
/// │ filters =            │
/// │    [id < @ TopKHeap] │
/// └──────────────────────┘
/// ```
///
/// Now as we fill our `TopK` heap we can push down the state of the heap to
/// the `DataSourceExec` node to avoid reading files / row groups / pages /
/// rows that could not possibly be in the top 10.
///
/// This is not yet implemented in DataFusion. See
/// <https://github.com/apache/datafusion/issues/15037>
///
/// [`PhysicalExpr`]: datafusion_physical_plan::PhysicalExpr
/// [`PhysicalExpr::snapshot`]: datafusion_physical_plan::PhysicalExpr::snapshot
/// [`FilterExec`]: datafusion_physical_plan::filter::FilterExec
/// [`ProjectionExec`]: datafusion_physical_plan::projection::ProjectionExec
/// [`AggregateExec`]: datafusion_physical_plan::aggregates::AggregateExec
#[derive(Debug)]
pub struct FilterPushdown {
    phase: FilterPushdownPhase,
    name: String,
}

impl FilterPushdown {
    fn new_with_phase(phase: FilterPushdownPhase) -> Self {
        let name = match phase {
            FilterPushdownPhase::Pre => "FilterPushdown",
            FilterPushdownPhase::Post => "FilterPushdown(Post)",
        }
        .to_string();
        Self { phase, name }
    }

    /// Create a new [`FilterPushdown`] optimizer rule that runs in the pre-optimization phase.
    /// See [`FilterPushdownPhase`] for more details.
    pub fn new() -> Self {
        Self::new_with_phase(FilterPushdownPhase::Pre)
    }

    /// Create a new [`FilterPushdown`] optimizer rule that runs in the post-optimization phase.
    /// See [`FilterPushdownPhase`] for more details.
    pub fn new_post_optimization() -> Self {
        Self::new_with_phase(FilterPushdownPhase::Post)
    }
}

impl Default for FilterPushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for FilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(
            push_down_filters(Arc::clone(&plan), vec![], config, self.phase)?
                .updated_node
                .unwrap_or(plan),
        )
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn schema_check(&self) -> bool {
        true // Filter pushdown does not change the schema of the plan
    }
}

fn push_down_filters(
    node: Arc<dyn ExecutionPlan>,
    parent_predicates: Vec<Arc<dyn PhysicalExpr>>,
    config: &ConfigOptions,
    phase: FilterPushdownPhase,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
    let mut parent_filter_pushdown_supports: Vec<Vec<PushedDown>> =
        vec![vec![]; parent_predicates.len()];
    let mut self_filters_pushdown_supports = vec![];
    let mut new_children = Vec::with_capacity(node.children().len());

    let children = node.children();
    let filter_description =
        node.gather_filters_for_pushdown(phase, parent_predicates.clone(), config)?;

    let filter_description_parent_filters = filter_description.parent_filters();
    let filter_description_self_filters = filter_description.self_filters();
    if filter_description_parent_filters.len() != children.len() {
        return Err(datafusion_common::DataFusionError::Internal(
            format!(
                "Filter pushdown expected FilterDescription to have parent filters for {expected_num_children}, but got {actual_num_children} for node {node_name}",
                expected_num_children = children.len(),
                actual_num_children = filter_description_parent_filters.len(),
                node_name = node.name(),
            ),
        ));
    }
    if filter_description_self_filters.len() != children.len() {
        return Err(datafusion_common::DataFusionError::Internal(
            format!(
                "Filter pushdown expected FilterDescription to have self filters for {expected_num_children}, but got {actual_num_children} for node {node_name}",
                expected_num_children = children.len(),
                actual_num_children = filter_description_self_filters.len(),
                node_name = node.name(),
            ),
        ));
    }

    for (child_idx, (child, parent_filters, self_filters)) in izip!(
        children,
        filter_description.parent_filters(),
        filter_description.self_filters()
    )
    .enumerate()
    {
        // Here, `parent_filters` are the predicates which are provided by the parent node of
        // the current node, and tried to be pushed down over the child which the loop points
        // currently. `self_filters` are the predicates which are provided by the current node,
        // and tried to be pushed down over the child similarly.

        let num_self_filters = self_filters.len();
        let mut all_predicates = self_filters.clone();

        // Track which parent filters are supported for this child
        let mut parent_filter_indices = vec![];

        // Iterate over each predicate coming from the parent
        for (parent_filter_idx, filter) in parent_filters.into_iter().enumerate() {
            // Check if we can push this filter down to our child.
            // These supports are defined in `gather_filters_for_pushdown()`
            match filter.discriminant {
                PushedDown::Yes => {
                    // Queue this filter up for pushdown to this child
                    all_predicates.push(filter.predicate);
                    parent_filter_indices.push(parent_filter_idx);
                }
                PushedDown::No => {
                    // This filter won't be pushed down to this child
                    // Will be marked as unsupported later in the initialization loop
                }
            }
        }

        let num_parent_filters = all_predicates.len() - num_self_filters;

        // Any filters that could not be pushed down to a child are marked as not-supported to our parents
        let result = push_down_filters(Arc::clone(child), all_predicates, config, phase)?;

        if let Some(new_child) = result.updated_node {
            // If we have a filter pushdown result, we need to update our children
            new_children.push(new_child);
        } else {
            // If we don't have a filter pushdown result, we need to update our children
            new_children.push(Arc::clone(child));
        }

        // Our child doesn't know the difference between filters that were passed down
        // from our parents and filters that the current node injected. We need to de-entangle
        // this since we do need to distinguish between them.
        let mut all_filters = result.filters.into_iter().collect_vec();
        if all_filters.len() != num_self_filters + num_parent_filters {
            return Err(datafusion_common::DataFusionError::Internal(
                format!(
                    "Filter pushdown did not return the expected number of filters: expected {num_self_filters} self filters and {num_parent_filters} parent filters, but got {num_filters_from_child}. Likely culprit is {child}",
                    num_self_filters = num_self_filters,
                    num_parent_filters = num_parent_filters,
                    num_filters_from_child = all_filters.len(),
                    child = child.name(),
                ),
            ));
        }
        let parent_filters = all_filters
            .split_off(num_self_filters)
            .into_iter()
            .collect_vec();
        self_filters_pushdown_supports.push(
            all_filters
                .into_iter()
                .zip(self_filters)
                .map(|(s, f)| s.wrap_expression(f))
                .collect(),
        );

        // Start by marking all parent filters as unsupported for this child
        for parent_filter_pushdown_support in parent_filter_pushdown_supports.iter_mut() {
            parent_filter_pushdown_support.push(PushedDown::No);
            assert_eq!(
                parent_filter_pushdown_support.len(),
                child_idx + 1,
                "Parent filter pushdown supports should have the same length as the number of children"
            );
        }
        // Map results from pushed-down filters back to original parent filter indices
        for (result_idx, parent_filter_support) in parent_filters.into_iter().enumerate()
        {
            let original_parent_idx = parent_filter_indices[result_idx];
            parent_filter_pushdown_supports[original_parent_idx][child_idx] =
                parent_filter_support;
        }
    }

    // Re-create this node with new children
    let updated_node = with_new_children_if_necessary(Arc::clone(&node), new_children)?;

    // TODO: by calling `handle_child_pushdown_result` we are assuming that the
    // `ExecutionPlan` implementation will not change the plan itself.
    // Should we have a separate method for dynamic pushdown that does not allow modifying the plan?
    let mut res = updated_node.handle_child_pushdown_result(
        phase,
        ChildPushdownResult {
            parent_filters: parent_predicates
                .into_iter()
                .enumerate()
                .map(
                    |(parent_filter_idx, parent_filter)| ChildFilterPushdownResult {
                        filter: parent_filter,
                        child_results: parent_filter_pushdown_supports[parent_filter_idx]
                            .clone(),
                    },
                )
                .collect(),
            self_filters: self_filters_pushdown_supports,
        },
        config,
    )?;
    // Compare pointers for new_node and node, if they are different we must replace
    // ourselves because of changes in our children.
    if res.updated_node.is_none() && !Arc::ptr_eq(&updated_node, &node) {
        res.updated_node = Some(updated_node)
    }
    Ok(res)
}
