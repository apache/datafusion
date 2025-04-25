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

use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// The result of or a plan for pushing down a filter into a child node.
/// This contains references to filters so that nodes can mutate a filter
/// before pushing it down to a child node (e.g. to adjust a projection)
/// or can directly take ownership of `Unsupported` filters that their children
/// could not handle.
#[derive(Debug, Clone)]
pub enum FilterPushdown {
    Supported(Arc<dyn PhysicalExpr>),
    Unsupported(Arc<dyn PhysicalExpr>),
}

/// A thin wrapper around [`FilterPushdown`]s that allows for easy collection of
/// supported and unsupported filters.
#[derive(Debug, Clone)]
pub struct FilterPushdowns(Vec<FilterPushdown>);

impl FilterPushdowns {
    /// Create a new FilterPushdowns with the given filters and their pushdown status.
    pub fn new(pushdowns: Vec<FilterPushdown>) -> Self {
        Self(pushdowns)
    }

    /// Create a new FilterPushdowns with all filters as supported.
    pub fn all_supported(filters: &[Arc<dyn PhysicalExpr>]) -> Self {
        let pushdowns = filters
            .iter()
            .map(|f| FilterPushdown::Supported(Arc::clone(f)))
            .collect();
        Self::new(pushdowns)
    }

    /// Create a new FilterPushdowns with all filters as unsupported.
    pub fn all_unsupported(filters: &[Arc<dyn PhysicalExpr>]) -> Self {
        let pushdowns = filters
            .iter()
            .map(|f| FilterPushdown::Unsupported(Arc::clone(f)))
            .collect();
        Self::new(pushdowns)
    }

    /// Transform all filters to supported, returning a new FilterPushdowns.
    /// This does not modify the original FilterPushdowns.
    pub fn as_supported(&self) -> Self {
        let pushdowns = self
            .0
            .iter()
            .map(|f| match f {
                FilterPushdown::Supported(expr) => {
                    FilterPushdown::Supported(Arc::clone(expr))
                }
                FilterPushdown::Unsupported(expr) => {
                    FilterPushdown::Supported(Arc::clone(expr))
                }
            })
            .collect();
        Self::new(pushdowns)
    }

    /// Collect unsupported filters into a Vec, without removing them from the original
    /// FilterPushdowns.
    pub fn collect_unsupported(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.0
            .iter()
            .filter_map(|f| match f {
                FilterPushdown::Unsupported(expr) => Some(Arc::clone(expr)),
                FilterPushdown::Supported(_) => None,
            })
            .collect()
    }

    /// Collect all filters as PhysicalExprs into a Vec, without removing them from the original
    /// FilterPushdowns.
    pub fn into_inner_filters(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.0
            .iter()
            .map(|f| match f {
                FilterPushdown::Supported(expr) => Arc::clone(expr),
                FilterPushdown::Unsupported(expr) => Arc::clone(expr),
            })
            .collect()
    }

    /// Return the inner `Vec<FilterPushdown>` without modifying the original FilterPushdowns.
    pub fn into_inner(&self) -> Vec<FilterPushdown> {
        self.0.clone()
    }

    /// Return an iterator over the inner `Vec<FilterPushdown>`.
    pub fn iter(&self) -> impl Iterator<Item = &FilterPushdown> {
        self.0.iter()
    }

    /// Return the number of filters in the inner `Vec<FilterPushdown>`.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the inner `Vec<FilterPushdown>` is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The result of pushing down filters into a child node.
/// This is the result provided to nodes in [`ExecutionPlan::handle_child_pushdown_result`].
/// Nodes process this result and convert it into a [`FilterPushdownPropagation`]
/// that is returned to their parent.
///
/// [`ExecutionPlan::handle_child_pushdown_result`]: crate::ExecutionPlan::handle_child_pushdown_result
#[derive(Debug, Clone)]
pub struct ChildPushdownResult {
    /// The combined result of pushing down each parent filter into each child.
    /// For example, given the fitlers `[a, b]` and children `[1, 2, 3]` the matrix of responses:
    ///
    // | filter | child 1     | child 2   | child 3   | result      |
    // |--------|-------------|-----------|-----------|-------------|
    // | a      | Supported   | Supported | Supported | Supported   |
    // | b      | Unsupported | Supported | Supported | Unsupported |
    ///
    /// That is: if any child marks a filter as unsupported or if the filter was not pushed
    /// down into any child then the result is unsupported.
    /// If at least one children and all children that received the filter mark it as supported
    /// then the result is supported.
    pub parent_filters: FilterPushdowns,
    /// The result of pushing down each filter this node provided into each of it's children.
    /// This is not combined with the parent filters so that nodes can treat each child independently.
    pub self_filters: Vec<FilterPushdowns>,
}

/// The result of pushing down filters into a node that it returns to its parent.
/// This is what nodes return from [`ExecutionPlan::handle_child_pushdown_result`] to communicate
/// to the optimizer:
///
/// 1. What to do with any parent filters that were not completely handled by the children.
/// 2. If the node needs to be replaced in the execution plan with a new node or not.
///
/// [`ExecutionPlan::handle_child_pushdown_result`]: crate::ExecutionPlan::handle_child_pushdown_result
#[derive(Debug, Clone)]
pub struct FilterPushdownPropagation<T> {
    pub parent_filter_result: FilterPushdowns,
    pub new_node: Option<T>,
}

impl<T> FilterPushdownPropagation<T> {
    /// Create a new [`FilterPushdownPropagation`] that tells the parent node
    /// that echoes back up to the parent the result of pushing down the filters
    /// into the children.
    pub fn transparent(child_pushdown_result: ChildPushdownResult) -> Self {
        Self {
            parent_filter_result: child_pushdown_result.parent_filters,
            new_node: None,
        }
    }

    /// Create a new [`FilterPushdownPropagation`] that tells the parent node
    /// that none of the parent filters were not pushed down.
    pub fn unsupported(parent_filters: &[Arc<dyn PhysicalExpr>]) -> Self {
        let unsupported = FilterPushdowns::all_unsupported(parent_filters);
        Self {
            parent_filter_result: unsupported,
            new_node: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterPushdownPlan {
    pub parent_filters_for_children: Vec<FilterPushdowns>,
    pub self_filters_for_children: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

impl FilterPushdownPlan {
    pub fn all_unsupported(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        num_children: usize,
    ) -> Self {
        let unsupported = FilterPushdowns::all_unsupported(parent_filters);
        Self {
            parent_filters_for_children: vec![unsupported; num_children],
            self_filters_for_children: vec![vec![]; num_children],
        }
    }

    pub fn all_supported(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        num_children: usize,
    ) -> Self {
        let supported = FilterPushdowns::all_supported(parent_filters);
        Self {
            parent_filters_for_children: vec![supported; num_children],
            self_filters_for_children: vec![vec![]; num_children],
        }
    }

    pub fn with_self_filters_for_children(
        mut self,
        self_filters_for_children: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    ) -> Self {
        self.self_filters_for_children = self_filters_for_children;
        self
    }
}
