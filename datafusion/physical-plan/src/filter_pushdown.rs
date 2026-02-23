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

use arrow_schema::SchemaRef;
use datafusion_common::{
    Result,
    tree_node::{Transformed, TreeNode},
};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
/// or can directly take ownership of filters that their children
/// could not handle.
#[derive(Debug, Clone)]
pub struct PushedDownPredicate {
    pub discriminant: PushedDown,
    pub predicate: Arc<dyn PhysicalExpr>,
}

impl PushedDownPredicate {
    /// Return the wrapped [`PhysicalExpr`], discarding whether it is supported or unsupported.
    pub fn into_inner(self) -> Arc<dyn PhysicalExpr> {
        self.predicate
    }

    /// Create a new [`PushedDownPredicate`] with supported pushdown.
    pub fn supported(predicate: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            discriminant: PushedDown::Yes,
            predicate,
        }
    }

    /// Create a new [`PushedDownPredicate`] with unsupported pushdown.
    pub fn unsupported(predicate: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            discriminant: PushedDown::No,
            predicate,
        }
    }
}

/// Discriminant for the result of pushing down a filter into a child node.
#[derive(Debug, Clone, Copy)]
pub enum PushedDown {
    /// The predicate was successfully pushed down into the child node.
    Yes,
    /// The predicate could not be pushed down into the child node.
    No,
}

impl PushedDown {
    /// Logical AND operation: returns `Yes` only if both operands are `Yes`.
    pub fn and(self, other: PushedDown) -> PushedDown {
        match (self, other) {
            (PushedDown::Yes, PushedDown::Yes) => PushedDown::Yes,
            _ => PushedDown::No,
        }
    }

    /// Logical OR operation: returns `Yes` if either operand is `Yes`.
    pub fn or(self, other: PushedDown) -> PushedDown {
        match (self, other) {
            (PushedDown::Yes, _) | (_, PushedDown::Yes) => PushedDown::Yes,
            (PushedDown::No, PushedDown::No) => PushedDown::No,
        }
    }

    /// Wrap a [`PhysicalExpr`] with this pushdown result.
    pub fn wrap_expression(self, expr: Arc<dyn PhysicalExpr>) -> PushedDownPredicate {
        PushedDownPredicate {
            discriminant: self,
            predicate: expr,
        }
    }
}

/// The result of pushing down a single parent filter into all children.
#[derive(Debug, Clone)]
pub struct ChildFilterPushdownResult {
    pub filter: Arc<dyn PhysicalExpr>,
    pub child_results: Vec<PushedDown>,
}

impl ChildFilterPushdownResult {
    /// Combine all child results using OR logic.
    /// Returns `Yes` if **any** child supports the filter.
    /// Returns `No` if **all** children reject the filter or if there are no children.
    pub fn any(&self) -> PushedDown {
        if self.child_results.is_empty() {
            // If there are no children, filters cannot be supported
            PushedDown::No
        } else {
            self.child_results
                .iter()
                .fold(PushedDown::No, |acc, result| acc.or(*result))
        }
    }

    /// Combine all child results using AND logic.
    /// Returns `Yes` if **all** children support the filter.
    /// Returns `No` if **any** child rejects the filter or if there are no children.
    pub fn all(&self) -> PushedDown {
        if self.child_results.is_empty() {
            // If there are no children, filters cannot be supported
            PushedDown::No
        } else {
            self.child_results
                .iter()
                .fold(PushedDown::Yes, |acc, result| acc.and(*result))
        }
    }
}

/// The result of pushing down filters into a child node.
///
/// This is the result provided to nodes in [`ExecutionPlan::handle_child_pushdown_result`].
/// Nodes process this result and convert it into a [`FilterPushdownPropagation`]
/// that is returned to their parent.
///
/// [`ExecutionPlan::handle_child_pushdown_result`]: crate::ExecutionPlan::handle_child_pushdown_result
#[derive(Debug, Clone)]
pub struct ChildPushdownResult {
    /// The parent filters that were pushed down as received by the current node when [`ExecutionPlan::gather_filters_for_pushdown`](crate::ExecutionPlan::handle_child_pushdown_result) was called.
    /// Note that this may *not* be the same as the filters that were passed to the children as the current node may have modified them
    /// (e.g. by reassigning column indices) when it returned them from [`ExecutionPlan::gather_filters_for_pushdown`](crate::ExecutionPlan::handle_child_pushdown_result) in a [`FilterDescription`].
    /// Attached to each filter is a [`PushedDown`] *per child* that indicates whether the filter was supported or unsupported by each child.
    /// To get combined results see [`ChildFilterPushdownResult::any`] and [`ChildFilterPushdownResult::all`].
    pub parent_filters: Vec<ChildFilterPushdownResult>,
    /// The result of pushing down each filter this node provided into each of it's children.
    /// The outer vector corresponds to each child, and the inner vector corresponds to each filter.
    /// Since this node may have generated a different filter for each child the inner vector may have different lengths or the expressions may not match at all.
    /// It is up to each node to interpret this result based on the filters it provided for each child in [`ExecutionPlan::gather_filters_for_pushdown`](crate::ExecutionPlan::handle_child_pushdown_result).
    pub self_filters: Vec<Vec<PushedDownPredicate>>,
}

/// The result of pushing down filters into a node.
///
/// Returned from [`ExecutionPlan::handle_child_pushdown_result`] to communicate
/// to the optimizer:
///
/// 1. What to do with any parent filters that could not be pushed down into the children.
/// 2. If the node needs to be replaced in the execution plan with a new node or not.
///
/// [`ExecutionPlan::handle_child_pushdown_result`]: crate::ExecutionPlan::handle_child_pushdown_result
#[derive(Debug, Clone)]
pub struct FilterPushdownPropagation<T> {
    /// Which parent filters were pushed down into this node's children.
    pub filters: Vec<PushedDown>,
    /// The updated node, if it was updated during pushdown
    pub updated_node: Option<T>,
}

impl<T> FilterPushdownPropagation<T> {
    /// Create a new [`FilterPushdownPropagation`] that tells the parent node that each parent filter
    /// is supported if it was supported by *all* children.
    pub fn if_all(child_pushdown_result: ChildPushdownResult) -> Self {
        let filters = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|result| result.all())
            .collect();
        Self {
            filters,
            updated_node: None,
        }
    }

    /// Create a new [`FilterPushdownPropagation`] that tells the parent node that each parent filter
    /// is supported if it was supported by *any* child.
    pub fn if_any(child_pushdown_result: ChildPushdownResult) -> Self {
        let filters = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|result| result.any())
            .collect();
        Self {
            filters,
            updated_node: None,
        }
    }

    /// Create a new [`FilterPushdownPropagation`] that tells the parent node that no filters were pushed down regardless of the child results.
    pub fn all_unsupported(child_pushdown_result: ChildPushdownResult) -> Self {
        let filters = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|_| PushedDown::No)
            .collect();
        Self {
            filters,
            updated_node: None,
        }
    }

    /// Create a new [`FilterPushdownPropagation`] with the specified filter support.
    /// This transmits up to our parent node what the result of pushing down the filters into our node and possibly our subtree was.
    pub fn with_parent_pushdown_result(filters: Vec<PushedDown>) -> Self {
        Self {
            filters,
            updated_node: None,
        }
    }

    /// Bind an updated node to the [`FilterPushdownPropagation`].
    /// Use this when the current node wants to update itself in the tree or replace itself with a new node (e.g. one of it's children).
    /// You do not need to call this if one of the children of the current node may have updated itself, that is handled by the optimizer.
    pub fn with_updated_node(mut self, updated_node: T) -> Self {
        self.updated_node = Some(updated_node);
        self
    }
}

/// Describes filter pushdown for a single child node.
///
/// This structure contains two types of filters:
/// - **Parent filters**: Filters received from the parent node, marked as supported or unsupported
/// - **Self filters**: Filters generated by the current node to be pushed down to this child
#[derive(Debug, Clone)]
pub struct ChildFilterDescription {
    /// Description of which parent filters can be pushed down into this node.
    /// Since we need to transmit filter pushdown results back to this node's parent
    /// we need to track each parent filter for each child, even those that are unsupported / won't be pushed down.
    pub(crate) parent_filters: Vec<PushedDownPredicate>,
    /// Description of which filters this node is pushing down to its children.
    /// Since this is not transmitted back to the parents we can have variable sized inner arrays
    /// instead of having to track supported/unsupported.
    pub(crate) self_filters: Vec<Arc<dyn PhysicalExpr>>,
}

/// Validates and remaps filter column references to a target schema in one step.
///
/// When pushing filters from a parent to a child node, we need to:
/// 1. Verify that all columns referenced by the filter exist in the target
/// 2. Remap column indices to match the target schema
///
/// `allowed_indices` controls which column indices (in the parent schema) are
/// considered valid. For single-input nodes this defaults to
/// `0..child_schema.len()` (all columns are reachable). For join nodes it is
/// restricted to the subset of output columns that map to the target child,
/// which is critical when different sides have same-named columns.
pub(crate) struct FilterRemapper {
    /// The target schema to remap column indices into.
    child_schema: SchemaRef,
    /// Only columns at these indices (in the *parent* schema) are considered
    /// valid. For non-join nodes this defaults to `0..child_schema.len()`.
    allowed_indices: HashSet<usize>,
}

impl FilterRemapper {
    /// Create a remapper that accepts any column whose index falls within
    /// `0..child_schema.len()` and whose name exists in the target schema.
    pub(crate) fn new(child_schema: SchemaRef) -> Self {
        let allowed_indices = (0..child_schema.fields().len()).collect();
        Self {
            child_schema,
            allowed_indices,
        }
    }

    /// Create a remapper that only accepts columns at the given indices.
    /// This is used by join nodes to restrict pushdown to one side of the
    /// join when both sides have same-named columns.
    fn with_allowed_indices(
        child_schema: SchemaRef,
        allowed_indices: HashSet<usize>,
    ) -> Self {
        Self {
            child_schema,
            allowed_indices,
        }
    }

    /// Try to remap a filter's column references to the target schema.
    ///
    /// Validates and remaps in a single tree traversal: for each column,
    /// checks that its index is in the allowed set and that
    /// its name exists in the target schema, then remaps the index.
    /// Returns `Some(remapped)` if all columns are valid, or `None` if any
    /// column fails validation.
    pub(crate) fn try_remap(
        &self,
        filter: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let mut all_valid = true;
        let transformed = Arc::clone(filter).transform_down(|expr| {
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                if self.allowed_indices.contains(&col.index())
                    && let Ok(new_index) = self.child_schema.index_of(col.name())
                {
                    Ok(Transformed::yes(Arc::new(Column::new(
                        col.name(),
                        new_index,
                    ))))
                } else {
                    all_valid = false;
                    Ok(Transformed::complete(expr))
                }
            } else {
                Ok(Transformed::no(expr))
            }
        })?;

        Ok(all_valid.then_some(transformed.data))
    }
}

impl ChildFilterDescription {
    /// Build a child filter description by analyzing which parent filters can be pushed to a specific child.
    ///
    /// This method performs column analysis to determine which filters can be pushed down:
    /// - If all columns referenced by a filter exist in the child's schema, it can be pushed down
    /// - Otherwise, it cannot be pushed down to that child
    ///
    /// See [`FilterDescription::from_children`] for more details
    pub fn from_child(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        child: &Arc<dyn crate::ExecutionPlan>,
    ) -> Result<Self> {
        let remapper = FilterRemapper::new(child.schema());
        Self::remap_filters(parent_filters, &remapper)
    }

    /// Like [`Self::from_child`], but restricts which parent-level columns are
    /// considered reachable through this child.
    ///
    /// `allowed_indices` is the set of column indices (in the *parent*
    /// schema) that map to this child's side of a join. A filter is only
    /// eligible for pushdown when **every** column index it references
    /// appears in `allowed_indices`.
    ///
    /// This prevents incorrect pushdown when different join sides have
    /// columns with the same name: matching on index ensures a filter
    /// referencing the right side's `k@2` is not pushed to the left side
    /// which also has a column named `k` but at a different index.
    pub fn from_child_with_allowed_indices(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        allowed_indices: HashSet<usize>,
        child: &Arc<dyn crate::ExecutionPlan>,
    ) -> Result<Self> {
        let remapper =
            FilterRemapper::with_allowed_indices(child.schema(), allowed_indices);
        Self::remap_filters(parent_filters, &remapper)
    }

    fn remap_filters(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        remapper: &FilterRemapper,
    ) -> Result<Self> {
        let mut child_parent_filters = Vec::with_capacity(parent_filters.len());
        for filter in parent_filters {
            if let Some(remapped) = remapper.try_remap(filter)? {
                child_parent_filters.push(PushedDownPredicate::supported(remapped));
            } else {
                child_parent_filters
                    .push(PushedDownPredicate::unsupported(Arc::clone(filter)));
            }
        }

        Ok(Self {
            parent_filters: child_parent_filters,
            self_filters: vec![],
        })
    }

    /// Mark all parent filters as unsupported for this child.
    pub fn all_unsupported(parent_filters: &[Arc<dyn PhysicalExpr>]) -> Self {
        Self {
            parent_filters: parent_filters
                .iter()
                .map(|f| PushedDownPredicate::unsupported(Arc::clone(f)))
                .collect(),
            self_filters: vec![],
        }
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

/// Describes how filters should be pushed down to children.
///
/// This structure contains filter descriptions for each child node, specifying:
/// - Which parent filters can be pushed down to each child
/// - Which self-generated filters should be pushed down to each child
///
/// The filter routing is determined by column analysis - filters can only be pushed
/// to children whose schemas contain all the referenced columns.
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
    #[expect(clippy::needless_pass_by_value)]
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

    /// Mark all parent filters as unsupported for all children.
    pub fn all_unsupported(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        children: &[&Arc<dyn crate::ExecutionPlan>],
    ) -> Self {
        let mut desc = Self::new();
        for _ in 0..children.len() {
            desc =
                desc.with_child(ChildFilterDescription::all_unsupported(parent_filters));
        }
        desc
    }

    pub fn parent_filters(&self) -> Vec<Vec<PushedDownPredicate>> {
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
