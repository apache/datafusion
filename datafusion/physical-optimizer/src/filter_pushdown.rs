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

use crate::PhysicalOptimizerRule;

use datafusion_common::{config::ConfigOptions, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPropagation, PredicateSupport, PredicateSupports,
};
use datafusion_physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use itertools::izip;

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
pub struct FilterPushdown {}

impl FilterPushdown {
    pub fn new() -> Self {
        Self {}
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
        Ok(push_down_filters(Arc::clone(&plan), vec![], config)?
            .updated_node
            .unwrap_or(plan))
    }

    fn name(&self) -> &str {
        "FilterPushdown"
    }

    fn schema_check(&self) -> bool {
        true // Filter pushdown does not change the schema of the plan
    }
}

/// Support state of each predicate for the children of the node.
/// These predicates are coming from the parent node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentPredicateStates {
    NoChildren,
    Unsupported,
    Supported,
}

fn push_down_filters(
    node: Arc<dyn ExecutionPlan>,
    parent_predicates: Vec<Arc<dyn PhysicalExpr>>,
    config: &ConfigOptions,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
    // If the node has any child, these will be rewritten as supported or unsupported
    let mut parent_predicates_pushdown_states =
        vec![ParentPredicateStates::NoChildren; parent_predicates.len()];
    let mut self_filters_pushdown_supports = vec![];
    let mut new_children = Vec::with_capacity(node.children().len());

    let children = node.children();
    let filter_description =
        node.gather_filters_for_pushdown(parent_predicates.clone(), config)?;

    for (child, parent_filters, self_filters) in izip!(
        children,
        filter_description.parent_filters(),
        filter_description.self_filters()
    ) {
        // Here, `parent_filters` are the predicates which are provided by the parent node of
        // the current node, and tried to be pushed down over the child which the loop points
        // currently. `self_filters` are the predicates which are provided by the current node,
        // and tried to be pushed down over the child similarly.

        let num_self_filters = self_filters.len();
        let mut parent_supported_predicate_indices = vec![];
        let mut all_predicates = self_filters;

        // Iterate over each predicate coming from the parent
        for (idx, filter) in parent_filters.into_iter().enumerate() {
            // Check if we can push this filter down to our child.
            // These supports are defined in `gather_filters_for_pushdown()`
            match filter {
                PredicateSupport::Supported(predicate) => {
                    // Queue this filter up for pushdown to this child
                    all_predicates.push(predicate);
                    parent_supported_predicate_indices.push(idx);
                    // Mark this filter as supported by our children if no child has marked it as unsupported
                    if parent_predicates_pushdown_states[idx]
                        != ParentPredicateStates::Unsupported
                    {
                        parent_predicates_pushdown_states[idx] =
                            ParentPredicateStates::Supported;
                    }
                }
                PredicateSupport::Unsupported(_) => {
                    // Mark as unsupported by our children
                    parent_predicates_pushdown_states[idx] =
                        ParentPredicateStates::Unsupported;
                }
            }
        }

        // Any filters that could not be pushed down to a child are marked as not-supported to our parents
        let result = push_down_filters(Arc::clone(child), all_predicates, config)?;

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
        let mut all_filters = result.filters.into_inner();
        let parent_predicates = all_filters.split_off(num_self_filters);
        let self_predicates = all_filters;
        self_filters_pushdown_supports.push(PredicateSupports::new(self_predicates));

        for (idx, result) in parent_supported_predicate_indices
            .iter()
            .zip(parent_predicates)
        {
            let current_node_state = match result {
                PredicateSupport::Supported(_) => ParentPredicateStates::Supported,
                PredicateSupport::Unsupported(_) => ParentPredicateStates::Unsupported,
            };
            match (current_node_state, parent_predicates_pushdown_states[*idx]) {
                (r, ParentPredicateStates::NoChildren) => {
                    // If we have no result, use the current state from this child
                    parent_predicates_pushdown_states[*idx] = r;
                }
                (ParentPredicateStates::Supported, ParentPredicateStates::Supported) => {
                    // If the current child and all previous children are supported,
                    // the filter continues to support it
                    parent_predicates_pushdown_states[*idx] =
                        ParentPredicateStates::Supported;
                }
                _ => {
                    // Either the current child or a previous child marked this filter as unsupported
                    parent_predicates_pushdown_states[*idx] =
                        ParentPredicateStates::Unsupported;
                }
            }
        }
    }
    // Re-create this node with new children
    let updated_node = with_new_children_if_necessary(Arc::clone(&node), new_children)?;
    // Remap the result onto the parent filters as they were given to us.
    // Any filters that were not pushed down to any children are marked as unsupported.
    let parent_pushdown_result = PredicateSupports::new(
        parent_predicates_pushdown_states
            .into_iter()
            .zip(parent_predicates)
            .map(|(state, filter)| match state {
                ParentPredicateStates::NoChildren => {
                    PredicateSupport::Unsupported(filter)
                }
                ParentPredicateStates::Unsupported => {
                    PredicateSupport::Unsupported(filter)
                }
                ParentPredicateStates::Supported => PredicateSupport::Supported(filter),
            })
            .collect(),
    );
    // Check what the current node wants to do given the result of pushdown to it's children
    let mut res = updated_node.handle_child_pushdown_result(
        ChildPushdownResult {
            parent_filters: parent_pushdown_result,
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
