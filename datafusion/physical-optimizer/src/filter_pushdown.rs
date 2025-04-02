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

use std::sync::Arc;

use datafusion_common::{config::ConfigOptions, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    execution_plan::{
        ExecutionPlanFilterPushdownResult, FilterPushdownAllowed, FilterSupport,
    },
    with_new_children_if_necessary, ExecutionPlan,
};

use crate::PhysicalOptimizerRule;

/// The state of filter pushdown support for a given filter.
#[derive(Clone, Copy, Debug)]
enum ChildPushdownState {
    /// A child said it can handle the filter exactly.
    ChildExact,
    /// A child exists and took a look at the filter.
    /// It may partially handle it or not handle it at all.
    /// The parent still needs to re-apply the filter.
    ChildInexact,
    /// No child exists, there is no one to handle the filter.
    /// This is the default / initial state.
    NoChild,
}

impl ChildPushdownState {
    /// Combine the current state with another state.
    /// This is used to combine the results of multiple children.
    fn combine_with_other(&self, other: &FilterSupport) -> ChildPushdownState {
        match (other, self) {
            (FilterSupport::HandledExact, ChildPushdownState::NoChild) => {
                ChildPushdownState::ChildExact
            }
            (FilterSupport::HandledExact, ChildPushdownState::ChildInexact) => {
                ChildPushdownState::ChildInexact
            }
            (FilterSupport::Unhandled, ChildPushdownState::NoChild) => {
                ChildPushdownState::ChildInexact
            }
            (FilterSupport::Unhandled, ChildPushdownState::ChildExact) => {
                ChildPushdownState::ChildInexact
            }
            (FilterSupport::Unhandled, ChildPushdownState::ChildInexact) => {
                ChildPushdownState::ChildInexact
            }
            (FilterSupport::HandledExact, ChildPushdownState::ChildExact) => {
                // If both are exact, keep it as exact
                ChildPushdownState::ChildExact
            }
        }
    }
}

fn push_down_into_children(
    node: &Arc<dyn ExecutionPlan>,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<ExecutionPlanFilterPushdownResult> {
    let children = node.children();
    let mut new_children = Vec::with_capacity(children.len());
    let mut filter_pushdown_result = vec![ChildPushdownState::NoChild; filters.len()];
    for child in children {
        if let Some(result) = pushdown_filters(child, &filters)? {
            new_children.push(result.inner);
            for (idx, support) in result.support.iter().enumerate() {
                filter_pushdown_result[idx] =
                    filter_pushdown_result[idx].combine_with_other(support)
            }
        } else {
            new_children.push(Arc::clone(child));
        }
    }
    let support = filter_pushdown_result
        .iter()
        .map(|s| match s {
            ChildPushdownState::ChildExact => FilterSupport::HandledExact,
            ChildPushdownState::ChildInexact => FilterSupport::Unhandled,
            ChildPushdownState::NoChild => FilterSupport::Unhandled,
        })
        .collect::<Vec<_>>();
    let node = with_new_children_if_necessary(Arc::clone(node), new_children)?;
    Ok(ExecutionPlanFilterPushdownResult::new(node, support))
}

/// Recursively a collection of filters down through the execution plan tree in a depth-first manner.
///
/// For each filter we try to push it down to children as far down as possible, keeping track of if the children
/// can handle the filter or not.
///
/// If a child can handle the filter, we mark it as handled exact and parent nodes (including the source of the filter)
/// can decide to discard it / not re-apply it themselves.
/// If a child cannot handle the filter or may return false positives (aka "inexact" handling) we mark it as handled inexact.
/// If a child does not allow filter pushdown at all (e.g. an aggregation node) we keep recursing but clear the current set of filters
/// we are pushing down.
///
/// As we recurse back up the tree we combine the results of the children to determine if the overall result is exact or inexact:
/// - For nodes with a single child we just take the child's result.
/// - For nodes with multiple children we combine the results of the children to determine if the overall result is exact or inexact.
///   We do this by checking if all children are exact (we return exact up) or if any child is inexact (we return inexact).
/// - If a node has no children this is equivalent to inexact handling (there is no child to handle the filter).
///
/// See [`FilterPushdown`] for more details on how this works in practice.
fn pushdown_filters(
    node: &Arc<dyn ExecutionPlan>,
    parent_filters: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<ExecutionPlanFilterPushdownResult>> {
    // Gather the filters from the current node.
    // These are the filters the current node "owns" or "produces" and wants to push down.
    let node_filters = node.filters_for_pushdown()?;
    // Check which nodes from parents this node is okay with us trying to push down to it's children.
    let parent_pushdown_request_result = node.filter_pushdown_request(&parent_filters)?;
    // Do some index masking so that we only ever call nodes with the filters relevant to them / that they're allowed to touch.
    // But we still need to reconstruct the full result for our caller.
    let parent_filter_for_pushdown_indices = parent_pushdown_request_result
        .iter()
        .enumerate()
        .filter_map(|(i, s)| {
            if matches!(s, FilterPushdownAllowed::Allowed(_)) {
                Some(i)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let parent_filters_to_push_down = parent_filter_for_pushdown_indices
        .iter()
        .map(|&i| Arc::clone(&parent_filters[i]))
        .collect::<Vec<_>>();
    let all_filters_to_push_down = node_filters
        .iter()
        .chain(parent_filters_to_push_down.iter())
        .map(|f| Arc::clone(f))
        .collect::<Vec<_>>();
    // Push down into children
    let child_pushdown_result = push_down_into_children(node, &all_filters_to_push_down)?;
    let mut node = child_pushdown_result.inner;
    // A bit more index masking to construct the final result for our caller.
    let node_filters_pushdown_result =
        child_pushdown_result.support[..node_filters.len()].to_vec();
    let mut parent_filter_pushdown_result =
        vec![FilterSupport::Unhandled; parent_filters.len()];
    for (parent_filter_idx, support) in parent_filter_for_pushdown_indices
        .iter()
        .zip(child_pushdown_result.support[node_filters.len()..].iter())
    {
        parent_filter_pushdown_result[*parent_filter_idx] = *support;
    }
    // Collect the remaining unhandled parent filters
    let unhandled_parent_filter_indices = (0..parent_filters.len())
        .filter(|&i| matches!(parent_filter_pushdown_result[i], FilterSupport::Unhandled))
        .collect::<Vec<_>>();
    let unhandled_parent_filters = unhandled_parent_filter_indices
        .iter()
        .map(|&i| Arc::clone(&parent_filters[i]))
        .collect::<Vec<_>>();
    // Check if the node can handle the filters
    if let Some(result) = Arc::clone(&node).with_filter_pushdown_result(
        &node_filters_pushdown_result,
        &unhandled_parent_filters,
    )? {
        node = result.inner;
        for (parent_filter_index, support) in
            unhandled_parent_filter_indices.iter().zip(result.support)
        {
            parent_filter_pushdown_result[*parent_filter_index] = support;
        }
    }
    Ok(Some(ExecutionPlanFilterPushdownResult::new(
        node,
        parent_filter_pushdown_result,
    )))
}

/// A physical optimizer rule that pushes down filters in the execution plan.
/// For example, consider the following plan:
///
/// ```text
// ┌──────────────────────┐
// │ CoalesceBatchesExec  │
// └──────────────────────┘
//             │
//             ▼
// ┌──────────────────────┐
// │      FilterExec      │
// │  filters = [ id=1]   │
// └──────────────────────┘
//             │
//             ▼
// ┌──────────────────────┐
// │    DataSourceExec    │
// │    projection = *    │
// └──────────────────────┘
/// ```
///
/// Our goal is to move the `id = 1` filter from the `FilterExec` node to the `DataSourceExec` node.
/// If this filter is selective it can avoid massive amounts of data being read from the source (the projection is `*` so all matching columns are read).
/// In this simple case we:
/// 1. Enter the recursion with no filters.
/// 2. We find the `FilterExec` node and it tells us that it has a filter (see [`ExecutionPlan::filters_for_pushdown`] and `datafusion::physical_plan::filter::FilterExec`).
/// 3. We recurse down into it's children (the `DataSourceExec` node) now carrying the filters `[id = 1]`.
/// 4. The `DataSourceExec` node tells us that it can handle the filter and we mark it as handled exact (see [`ExecutionPlan::push_down_filters_from_parents`]).
/// 5. Since the `DataSourceExec` node has no children we recurse back up the tree.
/// 6. We now tell the `FilterExec` node that it has a child that can handle the filter and we mark it as handled exact (see [`ExecutionPlan::with_filter_pushdown_result`]).
///    The `FilterExec` node can now return a new execution plan, either a copy of itself without that filter or if has no work left to do it can even return the child node directly.
/// 7. We recurse back up to `CoalesceBatchesExec` and do nothing there since it had no filters to push down.
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
//  │    projection = *    │
//  │   filters = [ id=1]  │
/// └──────────────────────┘
/// ```
///
/// Let's consider a more complex example involving a `ProjectionExec` node in betweeen the `FilterExec` and `DataSourceExec` nodes that creates a new column that the filter depends on.
///
/// ```text
// ┌──────────────────────┐
// │ CoalesceBatchesExec  │
// └──────────────────────┘
//             │
//             ▼
// ┌──────────────────────┐
// │      FilterExec      │
// │    filters =         │
// │     [cost>50,id=1]   │
// └──────────────────────┘
//             │
//             ▼
// ┌──────────────────────┐
// │    ProjectionExec    │
// │ cost = price * 1.2   │
// └──────────────────────┘
//             │
//             ▼
// ┌──────────────────────┐
// │    DataSourceExec    │
// │    projection = *    │
// └──────────────────────┘
/// ```
///
/// We want to push down the filters `[id=1]` to the [`DataSourceExec`] node, but can't push down `[cost>50]` because it requires the `ProjectionExec` node to be executed first:
///
/// ```text
// ┌──────────────────────┐
// │ CoalesceBatchesExec  │
// └──────────────────────┘
//             │
//             ▼
// ┌──────────────────────┐
// │      FilterExec      │
// │    filters =         │
// │     [cost>50]        │
// └──────────────────────┘
//             │
//             ▼
// ┌──────────────────────┐
// │    ProjectionExec    │
// │ cost = price * 1.2   │
// └──────────────────────┘
//             │
//             ▼
// ┌──────────────────────┐
// │    DataSourceExec    │
// │    projection = *    │
// │   filters = [ id=1]  │
// └──────────────────────┘
/// ```
///
/// There are also cases where we may be able to push down filters within a subtree but not the entire tree.
/// A good exmaple of this is aggreagation nodes:
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
/// The transformation here is to push down the `[id=1]` filter to the `DataSourceExec` node:
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
/// 1. We cannot push down `sum > 10` through the `AggregateExec` node into the `DataSourceExec` node.
///    Any filters above the `AggregateExec` node are not pushed down.
///    This is determined by calling [`ExecutionPlan::supports_filter_pushdown`] on the `AggregateExec` node.
/// 2. We need to keep recursing into the tree so that we can discover the other `FilterExec` node and push down the `[id=1]` filter.
///
/// It is also possible to push down filters through joins and from joins.
/// For example, a hash join where we build a hash table of the left side and probe the right side
/// (ignoring why we would choose this order, typically it depends on the size of each table, etc.).
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
/// 1. Push down the `[d.size > 100]` filter through the `HashJoinExec` node to the `DataSourceExec` node for the `departments` table.
/// 2. Push down the hash table state from the `HashJoinExec` node to the `DataSourceExec` node to avoid reading
///    rows from teh `users` table that will be eliminated by the join.
///    This can be done via a bloom filter or similar.
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
/// You may notice in this case that the filter is *dynamic*: the hash table is built
/// _after_ the `departments` table is read and at runtime.
/// We don't have a concrete `InList` filter or similar to push down at optimization time.
/// These sorts of dynamic filters are handled by building a specialized [`PhysicalExpr`] that
/// internally maintains a reference to the hash table or other state.
/// To make working with these sorts of dynamic filters more tractable we have the method [`PhysicalExpr::snapshot`]
/// which attempts to simplify a dynamic filter into a "basic" non-dynamic filter.
/// For a join this could mean converting it to an `InList` filter or a min/max filter for example.
/// See `datafusion/physical-plan/src/dynamic_filters.rs` for more details.
///
/// Another form of dyanmic filter is pushing down the state of a `TopK` operator for queries like
/// `SELECT * FROM t ORDER BY id LIMIT 10`:
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
/// Now as we fill our `TopK` heap we can push down the state of the heap to the `DataSourceExec` node
/// to avoid reading files / row groups / pages / rows that could not possibly be in the top 10.
/// This is implemented in datafusion/physical-plan/src/sorts/sort_filters.rs.
#[derive(Debug)]
pub struct FilterPushdown {}

impl Default for FilterPushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterPushdown {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for FilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(result) = pushdown_filters(&plan, &[])? {
            println!("new plan: {:?}", result.inner);
            Ok(result.inner)
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "FilterPushdown"
    }

    fn schema_check(&self) -> bool {
        true // Filter pushdown does not change the schema of the plan
    }
}
