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

use crate::{OptimizerContext, PhysicalOptimizerRule};

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{assert_eq_or_internal_err, config::ConfigOptions, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::physical_expr::is_volatile;
use datafusion_physical_plan::filter_pushdown::{
    ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase,
    FilterPushdownPropagation, PushedDown,
};
use datafusion_physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use itertools::{izip, Itertools};

/// Attempts to recursively push given filters from the top of the tree into leaves.
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
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.session_config().options();
        Ok(
            push_down_filters(&Arc::clone(&plan), vec![], config, self.phase)?
                .updated_node
                .unwrap_or(plan),
        )
    }

    #[allow(deprecated)]
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(
            push_down_filters(&Arc::clone(&plan), vec![], config, self.phase)?
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
    node: &Arc<dyn ExecutionPlan>,
    parent_predicates: Vec<Arc<dyn PhysicalExpr>>,
    config: &ConfigOptions,
    phase: FilterPushdownPhase,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
    let mut parent_filter_pushdown_supports: Vec<Vec<PushedDown>> =
        vec![vec![]; parent_predicates.len()];
    let mut self_filters_pushdown_supports = vec![];
    let mut new_children = Vec::with_capacity(node.children().len());

    let children = node.children();

    // Filter out expressions that are not allowed for pushdown
    let parent_filtered = FilteredVec::new(&parent_predicates, allow_pushdown_for_expr);

    let filter_description = node.gather_filters_for_pushdown(
        phase,
        parent_filtered.items().to_vec(),
        config,
    )?;

    let filter_description_parent_filters = filter_description.parent_filters();
    let filter_description_self_filters = filter_description.self_filters();
    assert_eq_or_internal_err!(
        filter_description_parent_filters.len(),
        children.len(),
        "Filter pushdown expected parent filters count to match number of children for node {}",
        node.name()
    );
    assert_eq_or_internal_err!(
        filter_description_self_filters.len(),
        children.len(),
        "Filter pushdown expected self filters count to match number of children for node {}",
        node.name()
    );

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

        // Filter out self_filters that contain volatile expressions and track indices
        let self_filtered = FilteredVec::new(&self_filters, allow_pushdown_for_expr);

        let num_self_filters = self_filtered.len();
        let mut all_predicates = self_filtered.items().to_vec();

        // Apply second filter pass: collect indices of parent filters that can be pushed down
        let parent_filters_for_child = parent_filtered
            .chain_filter_slice(&parent_filters, |filter| {
                matches!(filter.discriminant, PushedDown::Yes)
            });

        // Add the filtered parent predicates to all_predicates
        for filter in parent_filters_for_child.items() {
            all_predicates.push(Arc::clone(&filter.predicate));
        }

        let num_parent_filters = all_predicates.len() - num_self_filters;

        // Any filters that could not be pushed down to a child are marked as not-supported to our parents
        let result =
            push_down_filters(&Arc::clone(child), all_predicates, config, phase)?;

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
        assert_eq_or_internal_err!(
            all_filters.len(),
            num_self_filters + num_parent_filters,
            "Filter pushdown did not return the expected number of filters from {}",
            child.name()
        );
        let parent_filters = all_filters
            .split_off(num_self_filters)
            .into_iter()
            .collect_vec();
        // Map the results from filtered self filters back to their original positions using FilteredVec
        let mapped_self_results =
            self_filtered.map_results_to_original(all_filters, PushedDown::No);

        // Wrap each result with its corresponding expression
        let self_filter_results: Vec<_> = mapped_self_results
            .into_iter()
            .zip(self_filters)
            .map(|(support, filter)| support.wrap_expression(filter))
            .collect();

        self_filters_pushdown_supports.push(self_filter_results);

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
        let mapped_parent_results = parent_filters_for_child
            .map_results_to_original(parent_filters, PushedDown::No);

        // Update parent_filter_pushdown_supports with the mapped results
        // mapped_parent_results already has the results at their original indices
        for (idx, support) in parent_filter_pushdown_supports.iter_mut().enumerate() {
            support[child_idx] = mapped_parent_results[idx];
        }
    }

    // Re-create this node with new children
    let updated_node = with_new_children_if_necessary(Arc::clone(node), new_children)?;

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
    if res.updated_node.is_none() && !Arc::ptr_eq(&updated_node, node) {
        res.updated_node = Some(updated_node)
    }
    Ok(res)
}

/// A helper structure for filtering elements from a vector through multiple passes while
/// tracking their original indices, allowing results to be mapped back to the original positions.
struct FilteredVec<T> {
    items: Vec<T>,
    // Chain of index mappings: each Vec maps from current level to previous level
    // index_mappings[0] maps from first filter to original indices
    // index_mappings[1] maps from second filter to first filter indices, etc.
    index_mappings: Vec<Vec<usize>>,
    original_len: usize,
}

impl<T: Clone> FilteredVec<T> {
    /// Creates a new FilteredVec by filtering items based on the given predicate
    fn new<F>(items: &[T], predicate: F) -> Self
    where
        F: Fn(&T) -> bool,
    {
        let mut filtered_items = Vec::new();
        let mut original_indices = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            if predicate(item) {
                filtered_items.push(item.clone());
                original_indices.push(idx);
            }
        }

        Self {
            items: filtered_items,
            index_mappings: vec![original_indices],
            original_len: items.len(),
        }
    }

    /// Returns a reference to the filtered items
    fn items(&self) -> &[T] {
        &self.items
    }

    /// Returns the number of filtered items
    fn len(&self) -> usize {
        self.items.len()
    }

    /// Maps results from the filtered items back to their original positions
    /// Returns a vector with the same length as the original input, filled with default_value
    /// and updated with results at their original positions
    fn map_results_to_original<R: Clone>(
        &self,
        results: Vec<R>,
        default_value: R,
    ) -> Vec<R> {
        let mut mapped_results = vec![default_value; self.original_len];

        for (result_idx, result) in results.into_iter().enumerate() {
            let original_idx = self.trace_to_original_index(result_idx);
            mapped_results[original_idx] = result;
        }

        mapped_results
    }

    /// Traces a filtered index back to its original index through all filter passes
    fn trace_to_original_index(&self, mut current_idx: usize) -> usize {
        // Work backwards through the chain of index mappings
        for mapping in self.index_mappings.iter().rev() {
            current_idx = mapping[current_idx];
        }
        current_idx
    }

    /// Apply a filter to a new set of items while chaining the index mapping from self (parent)
    /// This is useful when you have filtered items and then get a transformed slice
    /// (e.g., from gather_filters_for_pushdown) that you need to filter again
    fn chain_filter_slice<U: Clone, F>(&self, items: &[U], predicate: F) -> FilteredVec<U>
    where
        F: Fn(&U) -> bool,
    {
        let mut filtered_items = Vec::new();
        let mut filtered_indices = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            if predicate(item) {
                filtered_items.push(item.clone());
                filtered_indices.push(idx);
            }
        }

        // Chain the index mappings from parent (self)
        let mut index_mappings = self.index_mappings.clone();
        index_mappings.push(filtered_indices);

        FilteredVec {
            items: filtered_items,
            index_mappings,
            original_len: self.original_len,
        }
    }
}

fn allow_pushdown_for_expr(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let mut allow_pushdown = true;
    expr.apply(|e| {
        allow_pushdown = allow_pushdown && !is_volatile(e);
        if allow_pushdown {
            Ok(TreeNodeRecursion::Continue)
        } else {
            Ok(TreeNodeRecursion::Stop)
        }
    })
    .expect("Infallible traversal of PhysicalExpr tree failed");
    allow_pushdown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filtered_vec_single_pass() {
        let items = vec![1, 2, 3, 4, 5, 6];
        let filtered = FilteredVec::new(&items, |&x| x % 2 == 0);

        // Check filtered items
        assert_eq!(filtered.items(), &[2, 4, 6]);
        assert_eq!(filtered.len(), 3);

        // Check index mapping
        let results = vec!["a", "b", "c"];
        let mapped = filtered.map_results_to_original(results, "default");
        assert_eq!(mapped, vec!["default", "a", "default", "b", "default", "c"]);
    }

    #[test]
    fn test_filtered_vec_empty_filter() {
        let items = vec![1, 3, 5];
        let filtered = FilteredVec::new(&items, |&x| x % 2 == 0);

        assert_eq!(filtered.items(), &[] as &[i32]);
        assert_eq!(filtered.len(), 0);

        let results: Vec<&str> = vec![];
        let mapped = filtered.map_results_to_original(results, "default");
        assert_eq!(mapped, vec!["default", "default", "default"]);
    }

    #[test]
    fn test_filtered_vec_all_pass() {
        let items = vec![2, 4, 6];
        let filtered = FilteredVec::new(&items, |&x| x % 2 == 0);

        assert_eq!(filtered.items(), &[2, 4, 6]);
        assert_eq!(filtered.len(), 3);

        let results = vec!["a", "b", "c"];
        let mapped = filtered.map_results_to_original(results, "default");
        assert_eq!(mapped, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_chain_filter_slice_different_types() {
        // First pass: filter numbers
        let numbers = vec![1, 2, 3, 4, 5, 6];
        let first_pass = FilteredVec::new(&numbers, |&x| x > 3);
        assert_eq!(first_pass.items(), &[4, 5, 6]);

        // Transform to strings (simulating gather_filters_for_pushdown transformation)
        let strings = vec!["four", "five", "six"];

        // Second pass: filter strings that contain 'i'
        let second_pass = first_pass.chain_filter_slice(&strings, |s| s.contains('i'));
        assert_eq!(second_pass.items(), &["five", "six"]);

        // Map results back to original indices
        let results = vec![100, 200];
        let mapped = second_pass.map_results_to_original(results, 0);
        // "five" was at index 4 (1-based: 5), "six" was at index 5 (1-based: 6)
        assert_eq!(mapped, vec![0, 0, 0, 0, 100, 200]);
    }

    #[test]
    fn test_chain_filter_slice_complex_scenario() {
        // Simulating the filter pushdown scenario
        // Parent predicates: [A, B, C, D, E]
        let parent_predicates = vec!["A", "B", "C", "D", "E"];

        // First pass: filter out some predicates (simulating allow_pushdown_for_expr)
        let first_pass = FilteredVec::new(&parent_predicates, |s| *s != "B" && *s != "D");
        assert_eq!(first_pass.items(), &["A", "C", "E"]);

        // After gather_filters_for_pushdown, we get transformed results for a specific child
        // Let's say child gets [A_transformed, C_transformed, E_transformed]
        // but only C and E can be pushed down
        #[derive(Clone, Debug, PartialEq)]
        struct TransformedPredicate {
            name: String,
            can_push: bool,
        }

        let child_predicates = vec![
            TransformedPredicate {
                name: "A_transformed".to_string(),
                can_push: false,
            },
            TransformedPredicate {
                name: "C_transformed".to_string(),
                can_push: true,
            },
            TransformedPredicate {
                name: "E_transformed".to_string(),
                can_push: true,
            },
        ];

        // Second pass: filter based on can_push
        let second_pass =
            first_pass.chain_filter_slice(&child_predicates, |p| p.can_push);
        assert_eq!(second_pass.len(), 2);
        assert_eq!(second_pass.items()[0].name, "C_transformed");
        assert_eq!(second_pass.items()[1].name, "E_transformed");

        // Simulate getting results back from child
        let child_results = vec!["C_result", "E_result"];
        let mapped = second_pass.map_results_to_original(child_results, "no_result");

        // Results should be at original positions: C was at index 2, E was at index 4
        assert_eq!(
            mapped,
            vec![
                "no_result",
                "no_result",
                "C_result",
                "no_result",
                "E_result"
            ]
        );
    }

    #[test]
    fn test_trace_to_original_index() {
        let items = vec![10, 20, 30, 40, 50];
        let filtered = FilteredVec::new(&items, |&x| x != 20 && x != 40);

        // filtered items are [10, 30, 50] at original indices [0, 2, 4]
        assert_eq!(filtered.trace_to_original_index(0), 0); // 10 was at index 0
        assert_eq!(filtered.trace_to_original_index(1), 2); // 30 was at index 2
        assert_eq!(filtered.trace_to_original_index(2), 4); // 50 was at index 4
    }

    #[test]
    fn test_chain_filter_preserves_original_len() {
        let items = vec![1, 2, 3, 4, 5];
        let first = FilteredVec::new(&items, |&x| x > 2);

        let strings = vec!["three", "four", "five"];
        let second = first.chain_filter_slice(&strings, |s| s.len() == 4);

        // Original length should still be 5
        let results = vec!["x", "y"];
        let mapped = second.map_results_to_original(results, "-");
        assert_eq!(mapped.len(), 5);
    }
}
