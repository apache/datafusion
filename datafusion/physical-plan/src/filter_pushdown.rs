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
//! 2. **Optimizer Executes Pushdown**: The optimizer recursively pushes down filters for each child,
//!    passing the appropriate filters (`Vec<Arc<dyn PhysicalExpr>>`) for that child.
//! 3. **Optimizer Gathers Results**: The optimizer collects [`FilterPushdownPropagation`] results from children,
//!    containing information about which filters were successfully pushed down vs. unsupported.
//! 4. **Parent Responds**: The optimizer calls [`ExecutionPlan::handle_child_pushdown_result`] on the parent,
//!    passing a [`ChildPushdownResult`] containing the aggregated pushdown outcomes. The parent decides
//!    how to handle filters that couldn't be pushed down (e.g., keep them as FilterExec nodes).
//!
//! [`ExecutionPlan::gather_filters_for_pushdown`]: crate::ExecutionPlan::gather_filters_for_pushdown
//! [`ExecutionPlan::handle_child_pushdown_result`]: crate::ExecutionPlan::handle_child_pushdown_result
//!
//! See also datafusion/physical-optimizer/src/filter_pushdown.rs.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_expr::utils::{collect_columns, reassign_predicate_columns};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

#[derive(Debug, Clone, Copy)]
pub enum FilterPushdownPhase {
    /// Pushdown that happens before most other optimizations.
    /// This pushdown allows static filters that do not reference any [`ExecutionPlan`]s to be pushed down.
    /// Filters that reference an [`ExecutionPlan`] cannot be pushed down at this stage since the whole plan tree may be rewritten
    /// by other optimizations.
    /// Implementers are however allowed to modify the execution plan themselves during this phase, for example by returning a completely
    /// different [`ExecutionPlan`] from [`ExecutionPlan::handle_child_pushdown_result`].
    ///
    /// Pushdown of [`FilterExec`] into `DataSourceExec` is an example of a pre-pushdown.
    /// Unlike filter pushdown in the logical phase, which operates on the logical plan to push filters into the logical table scan,
    /// the `Pre` phase in the physical plan targets the actual physical scan, pushing filters down to specific data source implementations.
    /// For example, Parquet supports filter pushdown to reduce data read during scanning, while CSV typically does not.
    ///
    /// [`ExecutionPlan`]: crate::ExecutionPlan
    /// [`FilterExec`]: crate::filter::FilterExec
    /// [`ExecutionPlan::handle_child_pushdown_result`]: crate::ExecutionPlan::handle_child_pushdown_result
    Pre,
    /// Pushdown that happens after most other optimizations.
    /// This stage of filter pushdown allows filters that reference an [`ExecutionPlan`] to be pushed down.
    /// Since subsequent optimizations should not change the structure of the plan tree except for calling [`ExecutionPlan::with_new_children`]
    /// (which generally preserves internal references) it is safe for references between [`ExecutionPlan`]s to be established at this stage.
    ///
    /// This phase is used to link a [`SortExec`] (with a TopK operator) or a [`HashJoinExec`] to a `DataSourceExec`.
    ///
    /// [`ExecutionPlan`]: crate::ExecutionPlan
    /// [`ExecutionPlan::with_new_children`]: crate::ExecutionPlan::with_new_children
    /// [`SortExec`]: crate::sorts::sort::SortExec
    /// [`HashJoinExec`]: crate::joins::HashJoinExec
    /// [`ExecutionPlan::handle_child_pushdown_result`]: crate::ExecutionPlan::handle_child_pushdown_result
    Post,
}

impl std::fmt::Display for FilterPushdownPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterPushdownPhase::Pre => write!(f, "Pre"),
            FilterPushdownPhase::Post => write!(f, "Post"),
        }
    }
}

/// The result of a plan for pushing down a filter into a child node.
/// This contains references to filters so that nodes can mutate a filter
/// before pushing it down to a child node (e.g. to adjust a projection)
/// or can directly take ownership of `Unsupported` filters that their children
/// could not handle.
#[derive(Debug, Clone)]
pub enum PredicateSupport {
    Supported(Arc<dyn PhysicalExpr>),
    Unsupported(Arc<dyn PhysicalExpr>),
}

impl PredicateSupport {
    pub fn into_inner(self) -> Arc<dyn PhysicalExpr> {
        match self {
            PredicateSupport::Supported(expr) | PredicateSupport::Unsupported(expr) => {
                expr
            }
        }
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
    pub parent_filters: Vec<PredicateSupport>,
    /// The result of pushing down each filter this node provided into each of it's children.
    /// This is not combined with the parent filters so that nodes can treat each child independently.
    pub self_filters: Vec<Vec<PredicateSupport>>,
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
    pub filters: Vec<PredicateSupport>,
    pub updated_node: Option<T>,
}

impl<T> FilterPushdownPropagation<T> {
    /// Create a new [`FilterPushdownPropagation`] that tells the parent node
    /// that echoes back up to the parent the result of pushing down the filters
    /// into the children.
    pub fn transparent(child_pushdown_result: ChildPushdownResult) -> Self {
        Self {
            filters: child_pushdown_result.parent_filters,
            updated_node: None,
        }
    }

    /// Create a new [`FilterPushdownPropagation`] with the specified filter support.
    pub fn with_filters(filters: Vec<PredicateSupport>) -> Self {
        Self {
            filters,
            updated_node: None,
        }
    }

    /// Bind an updated node to the [`FilterPushdownPropagation`].
    pub fn with_updated_node(mut self, updated_node: T) -> Self {
        self.updated_node = Some(updated_node);
        self
    }
}

#[derive(Debug, Clone)]
pub struct ChildFilterDescription {
    /// Description of which parent filters can be pushed down into this node.
    /// Since we need to transmit filter pushdown results back to this node's parent
    /// we need to track each parent filter for each child, even those that are unsupported / won't be pushed down.
    /// We do this using a [`PredicateSupport`] which simplifies manipulating supported/unsupported filters.
    pub(crate) parent_filters: Vec<PredicateSupport>,
    /// Description of which filters this node is pushing down to its children.
    /// Since this is not transmitted back to the parents we can have variable sized inner arrays
    /// instead of having to track supported/unsupported.
    pub(crate) self_filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl ChildFilterDescription {
    /// Build a child filter description by analyzing which parent filters can be pushed to a specific child.
    ///
    /// See [`FilterDescription::from_children`] for more details
    pub fn from_child(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        child: &Arc<dyn crate::ExecutionPlan>,
    ) -> Result<Self> {
        let child_schema = child.schema();

        // Get column names from child schema for quick lookup
        let child_column_names: HashSet<&str> = child_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        // Analyze each parent filter
        let mut child_parent_filters = Vec::with_capacity(parent_filters.len());

        for filter in parent_filters {
            // Check which columns the filter references
            let referenced_columns = collect_columns(filter);

            // Check if all referenced columns exist in the child schema
            let all_columns_exist = referenced_columns
                .iter()
                .all(|col| child_column_names.contains(col.name()));

            if all_columns_exist {
                // All columns exist in child - we can push down
                // Need to reassign column indices to match child schema
                let reassigned_filter =
                    reassign_predicate_columns(Arc::clone(filter), &child_schema, false)?;
                child_parent_filters.push(PredicateSupport::Supported(reassigned_filter));
            } else {
                // Some columns don't exist in child - cannot push down
                child_parent_filters
                    .push(PredicateSupport::Unsupported(Arc::clone(filter)));
            }
        }

        Ok(Self {
            parent_filters: child_parent_filters,
            self_filters: vec![],
        })
    }

    /// Add a self filter (from the current node) to be pushed down to this child.
    pub fn with_self_filter(mut self, filter: Arc<dyn PhysicalExpr>) -> Self {
        self.self_filters.push(filter);
        self
    }

    /// Add multiple self filters.
    pub fn with_self_filters(mut self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        self.self_filters.extend(filters);
        self
    }
}

#[derive(Debug, Clone)]
pub struct FilterDescription {
    /// A filter description for each child.
    /// This includes which parent filters and which self filters (from the node in question)
    /// will get pushed down to each child.
    child_filter_descriptions: Vec<ChildFilterDescription>,
}

impl Default for FilterDescription {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterDescription {
    /// Create a new empty FilterDescription
    pub fn new() -> Self {
        Self {
            child_filter_descriptions: vec![],
        }
    }

    /// Add a child filter description
    pub fn with_child(mut self, child: ChildFilterDescription) -> Self {
        self.child_filter_descriptions.push(child);
        self
    }

    /// Build a filter description by analyzing which parent filters can be pushed to each child.
    /// This method automatically determines filter routing based on column analysis:
    /// - If all columns referenced by a filter exist in a child's schema, it can be pushed down
    /// - Otherwise, it cannot be pushed down to that child
    pub fn from_children(
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        children: &[&Arc<dyn crate::ExecutionPlan>],
    ) -> Result<Self> {
        let mut desc = Self::new();

        // For each child, create a ChildFilterDescription
        for child in children {
            desc = desc
                .with_child(ChildFilterDescription::from_child(&parent_filters, child)?);
        }

        Ok(desc)
    }

    pub fn parent_filters(&self) -> Vec<Vec<PredicateSupport>> {
        self.child_filter_descriptions
            .iter()
            .map(|d| &d.parent_filters)
            .cloned()
            .collect()
    }

    pub fn self_filters(&self) -> Vec<Vec<Arc<dyn PhysicalExpr>>> {
        self.child_filter_descriptions
            .iter()
            .map(|d| &d.self_filters)
            .cloned()
            .collect()
    }
}
