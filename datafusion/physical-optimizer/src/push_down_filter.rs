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

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{config::ConfigOptions, Result};
use datafusion_physical_expr::conjunction;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::filter_pushdown::{
    FilterDescription, FilterPushdownResult, FilterPushdownSupport,
};
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::ExecutionPlan;

/// Attempts to recursively push given filters from the top of the tree into leafs.
///
/// # Default Implementation
///
/// The default implementation in [`ExecutionPlan::try_pushdown_filters`] is a no-op
/// that assumes that:
///
/// * Parent filters can't be passed onto children.
/// * This node has no filters to contribute.
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
///    This is determined by calling [`ExecutionPlan::try_pushdown_filters`] on the [`AggregateExec`] node.
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
pub struct PushdownFilter {}

impl Default for PushdownFilter {
    fn default() -> Self {
        Self::new()
    }
}

pub type FilterDescriptionContext = PlanContext<FilterDescription>;

impl PhysicalOptimizerRule for PushdownFilter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let context = FilterDescriptionContext::new_default(plan);

        context
            .transform_up(|node| {
                if node.plan.as_any().downcast_ref::<FilterExec>().is_some() {
                    let initial_plan = Arc::clone(&node.plan);
                    let mut accept_updated = false;
                    let updated_node = node.transform_down(|filter_node| {
                        Self::try_pushdown(filter_node, config, &mut accept_updated)
                    });

                    if accept_updated {
                        updated_node
                    } else {
                        Ok(Transformed::no(FilterDescriptionContext::new_default(
                            initial_plan,
                        )))
                    }
                }
                // Other filter introducing operators extends here
                else {
                    Ok(Transformed::no(node))
                }
            })
            .map(|updated| updated.data.plan)
    }

    fn name(&self) -> &str {
        "PushdownFilter"
    }

    fn schema_check(&self) -> bool {
        true // Filter pushdown does not change the schema of the plan
    }
}

impl PushdownFilter {
    pub fn new() -> Self {
        Self {}
    }

    fn try_pushdown(
        mut node: FilterDescriptionContext,
        config: &ConfigOptions,
        accept_updated: &mut bool,
    ) -> Result<Transformed<FilterDescriptionContext>> {
        let initial_description = FilterDescription {
            filters: node.data.take_description(),
        };

        let FilterPushdownResult {
            support,
            remaining_description,
        } = node
            .plan
            .try_pushdown_filters(initial_description, config)?;

        match support {
            FilterPushdownSupport::Supported {
                mut child_descriptions,
                op,
                revisit,
            } => {
                if revisit {
                    // This check handles cases where the current operator is entirely removed
                    // from the plan and replaced with its child. In such cases, to not skip
                    // over the new node, we need to explicitly re-apply this pushdown logic
                    // to the new node.
                    //
                    // TODO: If TreeNodeRecursion supports a Revisit mechanism in the future,
                    //       this manual recursion could be removed.

                    // If the operator is removed, it should not leave any filters as remaining
                    debug_assert!(remaining_description.filters.is_empty());
                    // Operators having 2 children cannot be removed
                    debug_assert_eq!(child_descriptions.len(), 1);
                    debug_assert_eq!(node.children.len(), 1);

                    node.plan = op;
                    node.data = child_descriptions.swap_remove(0);
                    node.children = node.children.swap_remove(0).children;
                    Self::try_pushdown(node, config, accept_updated)
                } else {
                    if remaining_description.filters.is_empty() {
                        // Filter can be pushed down safely
                        node.plan = op;
                        if node.children.is_empty() {
                            *accept_updated = true;
                        } else {
                            for (child, descr) in
                                node.children.iter_mut().zip(child_descriptions)
                            {
                                child.data = descr;
                            }
                        }
                    } else {
                        // Filter cannot be pushed down
                        node = insert_filter_exec(
                            node,
                            child_descriptions,
                            remaining_description,
                        )?;
                    }
                    Ok(Transformed::yes(node))
                }
            }
            FilterPushdownSupport::NotSupported => {
                if remaining_description.filters.is_empty() {
                    Ok(Transformed {
                        data: node,
                        transformed: false,
                        tnr: TreeNodeRecursion::Stop,
                    })
                } else {
                    node = insert_filter_exec(
                        node,
                        vec![FilterDescription::empty(); 1],
                        remaining_description,
                    )?;
                    Ok(Transformed {
                        data: node,
                        transformed: true,
                        tnr: TreeNodeRecursion::Stop,
                    })
                }
            }
        }
    }
}

fn insert_filter_exec(
    node: FilterDescriptionContext,
    mut child_descriptions: Vec<FilterDescription>,
    remaining_description: FilterDescription,
) -> Result<FilterDescriptionContext> {
    let mut new_child_node = node;

    // Filter has one child
    if !child_descriptions.is_empty() {
        debug_assert_eq!(child_descriptions.len(), 1);
        new_child_node.data = child_descriptions.swap_remove(0);
    }
    let new_plan = Arc::new(FilterExec::try_new(
        conjunction(remaining_description.filters),
        Arc::clone(&new_child_node.plan),
    )?);
    let new_children = vec![new_child_node];
    let new_data = FilterDescription::empty();

    Ok(FilterDescriptionContext::new(
        new_plan,
        new_data,
        new_children,
    ))
}
