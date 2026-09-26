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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_common::{
    Result,
    tree_node::{Transformed, TreeNode},
};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::filter::FilterConjunct;
use datafusion_physical_expr::utils::collect_columns;
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
///
/// This tells the parent whether it still has to evaluate the filter. It does
/// not tell the parent how, or if, the child uses the filter.
///
/// For example, a child that replies [`PushedDown::No`] can still keep the
/// filter and use it in an inexact way, such as to prune files, row groups or
/// pages with statistics.
#[derive(Debug, Clone, Copy)]
pub enum PushedDown {
    /// The child guarantees that it applies the predicate exactly: it never
    /// produces a row for which the predicate is not true. The parent does
    /// not need to evaluate the predicate again.
    Yes,
    /// The child does not guarantee that it applies the predicate exactly,
    /// so the parent must still evaluate it.
    ///
    /// The child may ignore the predicate, or it may use it in an inexact
    /// way, for example for statistics pruning.
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
    /// `true` if the node that created this filter does not need it for
    /// correctness. See [`FilterConjunct::is_optional`].
    optional: bool,
}

impl ChildFilterPushdownResult {
    /// Create a result for `filter`, with the properties of the conjunct
    /// (for example, the optional flag).
    pub fn new(filter: FilterConjunct, child_results: Vec<PushedDown>) -> Self {
        let optional = filter.is_optional();
        Self {
            filter: filter.into_expr(),
            child_results,
            optional,
        }
    }

    /// This filter as a [`FilterConjunct`], with its properties.
    pub fn conjunct(&self) -> FilterConjunct {
        let filter = Arc::clone(&self.filter);
        if self.optional {
            FilterConjunct::optional(filter)
        } else {
            FilterConjunct::required(filter)
        }
    }

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
    /// The entries must stay in the same order as the input parent filters: the
    /// filter pushdown optimizer maps child results back to parent filters by
    /// position.
    pub(crate) parent_filters: Vec<PushedDownPredicate>,
    /// Description of which filters this node is pushing down to its children.
    /// Since this is not transmitted back to the parents we can have variable sized inner arrays
    /// instead of having to track supported/unsupported.
    pub(crate) self_filters: Vec<FilterConjunct>,
}

/// How a parent output position resolves to a child input position.
enum ColumnMapping {
    /// Output position `i` reads child position `i`, and the child field at
    /// that position must carry the same name. Used by schema-preserving
    /// nodes such as sort, repartition and coalesce.
    Identity,
    /// Explicit output -> input positions supplied by a node that projects,
    /// reorders or pairs columns (joins, aggregates, projected filters).
    /// Names may differ, so the caller is trusted.
    Explicit(HashMap<usize, usize>),
}

/// Validates and remaps filter column references to a target schema in one step.
///
/// When pushing filters from a parent to a child node, we need to:
/// 1. Verify that every column referenced by the filter is reachable in the child
/// 2. Remap column indices to match the child schema
///
/// Columns are always resolved by position, never by name: a child schema can
/// contain several fields with the same name (for example the output of nested
/// joins), so a name lookup could silently redirect a predicate to the wrong
/// column.
pub(crate) struct FilterRemapper {
    /// The target schema to remap column indices into.
    child_schema: SchemaRef,
    mapping: ColumnMapping,
}

impl FilterRemapper {
    /// Create a remapper for a node whose output has the same column positions
    /// as `child_schema`. Each column is resolved to the same index, provided
    /// the child field at that position has the same name.
    pub(crate) fn new(child_schema: SchemaRef) -> Self {
        Self {
            child_schema,
            mapping: ColumnMapping::Identity,
        }
    }

    /// Create a remapper with an explicit parent-output to child-input mapping.
    pub(crate) fn with_column_mapping(
        child_schema: SchemaRef,
        column_mapping: HashMap<usize, usize>,
    ) -> Self {
        Self {
            child_schema,
            mapping: ColumnMapping::Explicit(column_mapping),
        }
    }

    /// Resolve a parent column to its position in the child schema.
    fn remap_column(&self, col: &Column) -> Option<Column> {
        let index = match &self.mapping {
            ColumnMapping::Identity => {
                let field = self.child_schema.fields().get(col.index())?;
                (field.name() == col.name()).then_some(col.index())?
            }
            ColumnMapping::Explicit(mapping) => *mapping.get(&col.index())?,
        };
        let field = self.child_schema.fields().get(index)?;
        Some(Column::new(field.name(), index))
    }

    /// Try to remap a filter's column references to the target schema.
    /// Returns `Some(remapped)` if all columns are reachable, or `None` if any
    /// column fails validation.
    pub(crate) fn try_remap(
        &self,
        filter: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let mut all_valid = true;
        let transformed = Arc::clone(filter).transform_down(|expr| {
            if let Some(col) = expr.downcast_ref::<Column>() {
                if let Some(remapped) = self.remap_column(col) {
                    // Keep the same `Arc` when the column does not change, so
                    // that an unchanged filter keeps its identity.
                    if remapped == *col {
                        Ok(Transformed::no(expr))
                    } else {
                        Ok(Transformed::yes(Arc::new(remapped)))
                    }
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
    /// Build a child filter description for a node whose output has the same
    /// column positions as `child`, such as a filter, sort or repartition.
    ///
    /// Every column referenced by a filter is resolved at the same index in the
    /// child schema, so same-named columns stay distinct. A filter is only
    /// pushed down when all of its columns resolve. Nodes that project or
    /// reorder columns must use [`Self::from_child_with_column_mapping`].
    ///
    /// See [`FilterDescription::from_children`] for more details
    pub fn from_child(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        child: &Arc<dyn crate::ExecutionPlan>,
    ) -> Result<Self> {
        if parent_filters.is_empty() {
            return Ok(Self::empty());
        }
        let remapper = FilterRemapper::new(child.schema());
        Self::remap_filters(parent_filters, &remapper)
    }

    /// Forwards filters whose columns all appear in `allowed_indices` and
    /// resolve by name in the child schema.
    ///
    /// Preserves the historical name-based resolution to the first matching
    /// child field. This is ambiguous when the child has duplicate field names;
    /// use [`Self::from_child_with_column_mapping`] to specify positions explicitly.
    #[deprecated(
        since = "56.0.0",
        note = "use `from_child` for matching schemas or `from_child_with_column_mapping` when positions differ"
    )]
    pub fn from_child_with_allowed_indices(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        allowed_indices: HashSet<usize>,
        child: &Arc<dyn crate::ExecutionPlan>,
    ) -> Result<Self> {
        if parent_filters.is_empty() {
            return Ok(Self::empty());
        }
        // Keep legacy name resolution local to this deprecated API. New callers
        // must supply positions explicitly to avoid ambiguous column names.
        let child_schema = child.schema();
        let column_mapping = parent_filters
            .iter()
            .flat_map(collect_columns)
            .filter(move |col| allowed_indices.contains(&col.index()))
            .filter_map(|col| {
                child_schema
                    .index_of(col.name())
                    .ok()
                    .map(|child_index| (col.index(), child_index))
            })
            .collect();
        Self::from_child_with_column_mapping(parent_filters, column_mapping, child)
    }

    /// Remap parent filters using an explicit parent-output to child-input
    /// column mapping. Columns absent from the mapping cannot be pushed down.
    ///
    /// Joins, aggregates and filters with an embedded projection use this:
    /// their output positions differ from the child's, and a child can contain
    /// duplicate field names, so positions cannot be recovered from names.
    /// Join keys may also be mapped to a differently named column on the
    /// other side.
    pub fn from_child_with_column_mapping(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        column_mapping: HashMap<usize, usize>,
        child: &Arc<dyn crate::ExecutionPlan>,
    ) -> Result<Self> {
        if parent_filters.is_empty() {
            return Ok(Self::empty());
        }
        let remapper =
            FilterRemapper::with_column_mapping(child.schema(), column_mapping);
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

    /// A description carrying no filters in either direction.
    pub(crate) fn empty() -> Self {
        Self {
            parent_filters: vec![],
            self_filters: vec![],
        }
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

    /// Add a required self filter (from the current node) to be pushed down
    /// to this child.
    pub fn with_self_filter(self, filter: Arc<dyn PhysicalExpr>) -> Self {
        self.with_self_conjunct(FilterConjunct::required(filter))
    }

    /// Add an optional self filter: a filter that the current node does not
    /// need for correctness (for example, a dynamic filter). The consumer
    /// that accepts it can skip it.
    pub fn with_optional_self_filter(self, filter: Arc<dyn PhysicalExpr>) -> Self {
        self.with_self_conjunct(FilterConjunct::optional(filter))
    }

    /// Add a self filter with its properties.
    pub fn with_self_conjunct(mut self, conjunct: FilterConjunct) -> Self {
        self.self_filters.push(conjunct);
        self
    }

    /// Add multiple required self filters.
    pub fn with_self_filters(mut self, filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        self.self_filters
            .extend(filters.into_iter().map(FilterConjunct::required));
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
            .map(|d| {
                d.self_filters
                    .iter()
                    .map(|c| Arc::clone(c.expr()))
                    .collect()
            })
            .collect()
    }

    /// The self filters for each child, with their properties.
    pub fn self_conjuncts(&self) -> Vec<Vec<FilterConjunct>> {
        self.child_filter_descriptions
            .iter()
            .map(|d| &d.self_filters)
            .cloned()
            .collect()
    }
}
