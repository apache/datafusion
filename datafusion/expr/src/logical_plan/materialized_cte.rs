// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Logical nodes for `WITH x AS MATERIALIZED (...)`.
//!
//! A materialized CTE is planned as one [`MaterializedCte`] node, which owns
//! the CTE body and the rest of the query (the "continuation"), and one
//! [`MaterializedCteScan`] leaf for each reference to the CTE inside the
//! continuation. Both are in the [`LogicalPlan::MaterializedCte`] variant,
//! as cases of [`MaterializedCtePlan`]. The body is therefore optimized and executed once, and
//! the scans read its buffered output.
//!
//! The scans are leaves, so the filters of one reference are not pushed into
//! the shared body. This is the same optimization fence that PostgreSQL
//! applies to `MATERIALIZED` CTEs.
//!
//! A scan can read a subset of the columns of the body (see
//! [`MaterializedCteScan::projection`]). The optimizer removes from the body
//! the columns that no scan reads.
//!
//! # Checks
//!
//! A scan reads the columns of the body by position, so the scans must
//! match the body. [`MaterializedCteBuilder::build`] and the methods of
//! [`MaterializedCte`] that change an input check this.
//!
//! The generic tree APIs do not check it: [`LogicalPlan::with_new_exprs`]
//! and [`TreeNode::map_children`] replace the inputs as they are given.
//! [`LogicalPlan::check_invariants`] checks the scans of every
//! [`MaterializedCte`] in a plan. The optimizer runs it before and after
//! the rules.
//!
//! [`TreeNode::map_children`]: datafusion_common::tree_node::TreeNode::map_children

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, Result, internal_datafusion_err, internal_err,
};

use crate::{Expr, LogicalPlan, Projection};

/// Identifies one materialized CTE and binds its scans to it.
///
/// CTE names are not unique (two sibling subqueries can each declare `WITH t`),
/// so the planner allocates a process-unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaterializedCteId(u64);

impl MaterializedCteId {
    /// Allocate a new unique id.
    pub(crate) fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, AtomicOrdering::Relaxed))
    }

    /// The numeric value of this id.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for MaterializedCteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A node of a materialized CTE, in [`LogicalPlan::MaterializedCte`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum MaterializedCtePlan {
    /// Computes the body of the CTE once and evaluates the continuation
    Cte(MaterializedCte),
    /// Reads the buffered output of the CTE. This is a leaf.
    Scan(MaterializedCteScan),
}

/// Computes `cte` once and evaluates `continuation`, in which every
/// [`MaterializedCteScan`] with the same `id` reads the buffered output of
/// `cte`. The output of this node is the output of `continuation`.
///
/// The node has two inputs with different roles, in this order:
///
/// 1. `cte`: the body of the CTE. It is shared by all the scans, so a rule
///    must not push into it a filter, limit or projection that belongs to one
///    scan only. A rule that changes its schema must also change the
///    [`projection`](MaterializedCteScan::projection) of every scan, because a
///    scan reads the columns of the body by position.
/// 2. `continuation`: the query that declared the CTE.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedCte {
    /// Binds the scans in `continuation` to this node
    pub(crate) id: MaterializedCteId,
    /// Name of the CTE, used for display and error messages only
    pub(crate) name: String,
    /// The body of the CTE
    pub(crate) cte: Arc<LogicalPlan>,
    /// The query that declared the CTE and reads it
    pub(crate) continuation: Arc<LogicalPlan>,
}

/// Builds a [`MaterializedCte`]: the body first, then one
/// [`MaterializedCteScan`] for each reference to the CTE, then the
/// continuation that holds the references.
///
/// The builder allocates the id that binds the scans to the node. It is not
/// `Clone`, so two nodes built from it cannot share an id.
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion_common::{DFSchema, Result};
/// # use datafusion_expr::{EmptyRelation, LogicalPlan, MaterializedCteBuilder};
/// # fn main() -> Result<()> {
/// let body = LogicalPlan::EmptyRelation(EmptyRelation {
///     produce_one_row: true,
///     schema: Arc::new(DFSchema::empty()),
/// });
/// let builder = MaterializedCteBuilder::new("c", body);
/// let continuation = LogicalPlan::from(builder.scan());
/// let cte = builder.build(continuation)?;
/// assert_eq!(cte.name(), "c");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MaterializedCteBuilder {
    id: MaterializedCteId,
    name: String,
    body: Arc<LogicalPlan>,
}

impl MaterializedCteBuilder {
    /// Start a materialized CTE with the name `name` and the body `body`.
    pub fn new(name: impl Into<String>, body: LogicalPlan) -> Self {
        Self {
            id: MaterializedCteId::next(),
            name: name.into(),
            body: Arc::new(body),
        }
    }

    /// A scan of all the columns of the body. Call this once for each
    /// reference to the CTE in the continuation.
    pub fn scan(&self) -> MaterializedCteScan {
        MaterializedCteScan::new(
            self.id,
            self.name.clone(),
            Arc::clone(self.body.schema()),
        )
    }

    /// Build the node that computes the body once and evaluates
    /// `continuation`.
    ///
    /// Returns an error if a scan of this CTE in `continuation` does not have
    /// the names and types of the columns of the body that it reads.
    pub fn build(self, continuation: LogicalPlan) -> Result<MaterializedCte> {
        MaterializedCte::try_new(self.id, self.name, self.body, Arc::new(continuation))
    }
}

impl From<MaterializedCte> for LogicalPlan {
    fn from(cte: MaterializedCte) -> Self {
        LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte))
    }
}

impl From<MaterializedCteScan> for LogicalPlan {
    fn from(scan: MaterializedCteScan) -> Self {
        LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(scan))
    }
}

impl MaterializedCte {
    /// Create a new node.
    ///
    /// Returns an error if a scan of this CTE in `continuation` (also in a
    /// subquery expression) does not have the names and types of the columns
    /// of `cte` that it reads.
    pub(crate) fn try_new(
        id: MaterializedCteId,
        name: impl Into<String>,
        cte: Arc<LogicalPlan>,
        continuation: Arc<LogicalPlan>,
    ) -> Result<Self> {
        let name = name.into();
        for scan in MaterializedCteScan::find_in(&continuation, id)? {
            scan.check_body_schema(cte.schema())
                .map_err(|e| {
                    e.context(format!(
                        "A scan of the materialized CTE {name} does not have the schema of the CTE"
                    ))
                })?;
        }
        Ok(Self {
            id,
            name,
            cte,
            continuation,
        })
    }

    /// The id that binds the scans in the continuation to this node.
    pub fn id(&self) -> MaterializedCteId {
        self.id
    }

    /// The name of the CTE.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The body of the CTE, which is computed once.
    pub fn body(&self) -> &Arc<LogicalPlan> {
        &self.cte
    }

    /// The query that declared the CTE. It gives this node its output.
    pub fn continuation(&self) -> &Arc<LogicalPlan> {
        &self.continuation
    }

    /// The schema of this node, which is the schema of the continuation.
    pub fn schema(&self) -> &DFSchemaRef {
        self.continuation.schema()
    }

    /// Whether a scan in the continuation reads this CTE, also in a subquery
    /// expression. A CTE that nothing reads does not need to be computed.
    pub fn is_read(&self) -> Result<bool> {
        self.scans().map(|scans| !scans.is_empty())
    }

    /// The scans in the continuation that read this CTE, also in subquery
    /// expressions. See [`MaterializedCteScan::find_in`].
    pub(crate) fn scans(&self) -> Result<Vec<MaterializedCteScan>> {
        MaterializedCteScan::find_in(&self.continuation, self.id)
    }

    /// Call `f` with the body, then with the continuation, so that a rule can
    /// change them in place. Return `true` if a call of `f` returns `true`.
    ///
    /// Returns an error if `f` changes the names or the types of the columns
    /// of the body, because the scans read them by position. The continuation
    /// is not checked: its schema can change, and the optimizer then
    /// recomputes the schema of this node.
    pub fn map_inputs_mut(
        &mut self,
        mut f: impl FnMut(&mut LogicalPlan) -> Result<bool>,
    ) -> Result<bool> {
        let schema = Arc::clone(self.cte.schema());
        let body = f(Arc::make_mut(&mut self.cte))?;
        if !Arc::ptr_eq(&schema, self.cte.schema()) {
            schema
                .has_equivalent_names_and_types(self.cte.schema())
                .map_err(|e| {
                    e.context(format!(
                        "The body of the materialized CTE {} changed its columns",
                        self.name
                    ))
                })?;
        }
        let continuation = f(Arc::make_mut(&mut self.continuation))?;
        Ok(body || continuation)
    }

    /// Replace the continuation with the result of `f`.
    ///
    /// Returns an error if a scan of this CTE in the new continuation does
    /// not have the names and types of the columns of the body that it reads.
    pub fn map_continuation(
        self,
        f: impl FnOnce(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
    ) -> Result<Transformed<Self>> {
        let Self {
            id,
            name,
            cte,
            continuation,
        } = self;
        f(Arc::unwrap_or_clone(continuation))?
            .map_data(|continuation| Self::try_new(id, name, cte, Arc::new(continuation)))
    }

    /// Replace the body with the result of `f`. The scans do not change.
    ///
    /// Returns an error if a scan of this CTE does not have the names and
    /// types of the columns of the new body that it reads.
    pub fn map_body(
        self,
        f: impl FnOnce(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
    ) -> Result<Transformed<Self>> {
        let Self {
            id,
            name,
            cte,
            continuation,
        } = self;
        f(Arc::unwrap_or_clone(cte))?
            .map_data(|cte| Self::try_new(id, name, Arc::new(cte), continuation))
    }

    /// Replace the body with `body`, which has the column names of the
    /// current body and maybe other types, for example after type coercion.
    /// Each scan gets the new types of the columns that it reads, and keeps
    /// its qualifiers.
    ///
    /// Returns an error if `body` does not have the column names of the
    /// current body that the scans read.
    pub fn with_coerced_body(self, body: LogicalPlan) -> Result<Self> {
        let body_schema = Arc::clone(body.schema());
        let continuation = Self::map_scans(self.id, self.continuation, |scan| {
            let new_scan = scan.with_body_schema(&body_schema)?;
            Ok(if new_scan.schema().inner() == scan.schema().inner() {
                Transformed::no(scan)
            } else {
                Transformed::yes(new_scan)
            })
        })?;
        Self::try_new(self.id, self.name, Arc::new(body), continuation)
    }

    /// Remove from the body the columns that no scan of this CTE reads. The
    /// body becomes a [`Projection`] of the other columns, and each scan
    /// reads the new positions of its columns. The output of each scan does
    /// not change.
    ///
    /// The body keeps one column if no scan reads a column, as
    /// `SELECT count(*) FROM c` does, so that it still has its rows. The node
    /// does not change if nothing reads the CTE or if every column is read.
    pub fn prune_body_columns(self) -> Result<Transformed<Self>> {
        let body_len = self.cte.schema().fields().len();
        let scans = self.scans()?;
        let mut is_read = vec![false; body_len];
        for scan in &scans {
            match scan.projection() {
                None => is_read.fill(true),
                Some(projection) => {
                    for index in projection {
                        let Some(is_read) = is_read.get_mut(*index) else {
                            return internal_err!(
                                "A scan of the materialized CTE {} reads the column {index}, but the CTE has {body_len} columns",
                                self.name
                            );
                        };
                        *is_read = true;
                    }
                }
            }
        }
        if !is_read.contains(&true)
            && let Some(first) = is_read.first_mut()
        {
            *first = true;
        }
        let kept: Vec<usize> = (0..body_len).filter(|index| is_read[*index]).collect();
        // A CTE that nothing reads is not planned, so its body is left as it is.
        if scans.is_empty() || kept.len() == body_len {
            return Ok(Transformed::no(self));
        }

        let schema = self.cte.schema();
        let exprs = kept
            .iter()
            .map(|index| Expr::Column(Column::from(schema.qualified_field(*index))))
            .collect();
        let body = Projection::try_new(exprs, Arc::clone(&self.cte))
            .map(LogicalPlan::Projection)?;
        let continuation = Self::map_scans(self.id, self.continuation, |scan| {
            scan.with_body_columns(&kept).map(Transformed::yes)
        })?;
        Self::try_new(self.id, self.name, Arc::new(body), continuation)
            .map(Transformed::yes)
    }

    /// Apply `f` to each scan of the CTE `id` in `continuation`, also in
    /// subquery expressions, and return the new continuation.
    ///
    /// The traversal does not go below a [`MaterializedCte`] with the same id.
    /// Such a node is a separate occurrence of the CTE and owns its scans.
    fn map_scans(
        id: MaterializedCteId,
        continuation: Arc<LogicalPlan>,
        mut f: impl FnMut(MaterializedCteScan) -> Result<Transformed<MaterializedCteScan>>,
    ) -> Result<Arc<LogicalPlan>> {
        Arc::unwrap_or_clone(continuation)
            .transform_down_with_subqueries(|node| match node {
                LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(ref nested))
                    if nested.id == id =>
                {
                    Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
                }
                LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(scan))
                    if scan.id == id =>
                {
                    f(scan).map(|scan| scan.update_data(LogicalPlan::from))
                }
                _ => Ok(Transformed::no(node)),
            })
            .map(|t| Arc::new(t.data))
    }

    /// Return a new node with the same id and name, and new inputs. The
    /// scans are not checked. See the [module documentation](self).
    pub(crate) fn with_new_inputs_unchecked(
        &self,
        cte: Arc<LogicalPlan>,
        continuation: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            cte,
            continuation,
        }
    }
}

/// Orders by `id` and `name`. Nodes with the same `id` and `name` but a
/// different `cte` or `continuation` are not comparable, so `Some(Equal)` is
/// returned only for equal nodes.
impl PartialOrd for MaterializedCte {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self
            .id
            .cmp(&other.id)
            .then_with(|| self.name.cmp(&other.name))
        {
            Ordering::Equal if self != other => None,
            ord => Some(ord),
        }
    }
}

/// Give each occurrence of a [`MaterializedCte`] in `plan` a new id, and
/// update the scans in its continuation, including the scans in subqueries.
///
/// The id is copied when a logical plan is cloned, so one CTE can occur more
/// than once, for example when a view that declares it is referenced two
/// times, or when a DataFrame is joined to itself. Each occurrence is a
/// separate CTE, as in PostgreSQL.
///
/// The new ids are also unique across calls. A physical planner can thus
/// call this function for each planning run of the same saved plan, and put
/// the results in one physical plan.
///
/// A new id does not change a schema, so the scans are not checked again.
pub fn renumber_materialized_ctes(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut has_cte = false;
    plan.apply_with_subqueries(|node| {
        if matches!(
            node,
            LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(_))
        ) {
            has_cte = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !has_cte {
        return Ok(Transformed::no(plan));
    }
    plan.transform_down_with_subqueries(|node| {
        let LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte)) = node else {
            return Ok(Transformed::no(node));
        };
        let id = MaterializedCteId::next();
        let continuation =
            MaterializedCte::map_scans(cte.id, cte.continuation, |mut scan| {
                scan.id = id;
                Ok(Transformed::yes(scan))
            })?;
        Ok(Transformed::yes(LogicalPlan::from(MaterializedCte {
            id,
            continuation,
            ..cte
        })))
    })
}

/// Reads the buffered output of the [`MaterializedCte`] with the same `id`.
///
/// This is a leaf. It outputs all the columns of the CTE body, or the columns
/// that `projection` selects.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedCteScan {
    /// The id of the [`MaterializedCte`] that this scan reads
    pub(crate) id: MaterializedCteId,
    /// Name of the CTE, used for display and error messages only
    pub(crate) name: String,
    /// The schema of this scan: the columns of the CTE body that
    /// `projection` selects
    schema: DFSchemaRef,
    /// The indexes of the columns of the CTE body that this scan outputs, in
    /// output order. `None` means all the columns, in the order of the body.
    ///
    /// The indexes refer to the schema of the body of the [`MaterializedCte`]
    /// with the same id, and the scan reads the buffered batches by position.
    projection: Option<Vec<usize>>,
}

impl MaterializedCteScan {
    /// Create a scan of all the columns of the CTE `id`, whose body has the
    /// schema `schema`. See [`MaterializedCteBuilder::scan`].
    pub(crate) fn new(
        id: MaterializedCteId,
        name: impl Into<String>,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            schema,
            projection: None,
        }
    }

    /// The id of the [`MaterializedCte`] that this scan reads.
    pub fn id(&self) -> MaterializedCteId {
        self.id
    }

    /// The name of the CTE.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The schema of this scan.
    pub fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    /// The indexes of the columns of the CTE body that this scan outputs.
    /// `None` means all the columns.
    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    /// Return a scan that outputs only the columns `indices` of this scan.
    ///
    /// The indexes refer to the schema of this scan, not to the schema of the
    /// CTE body.
    pub fn with_projection(&self, indices: &[usize]) -> Result<Self> {
        let len = self.schema.fields().len();
        if let Some(index) = indices.iter().find(|index| **index >= len) {
            return internal_err!(
                "Column {index} is not in the scan of the materialized CTE {}, which has {len} columns",
                self.name
            );
        }
        if indices.iter().copied().eq(0..len) {
            return Ok(self.clone());
        }
        let projection = match &self.projection {
            Some(projection) => indices.iter().map(|index| projection[*index]).collect(),
            None => indices.to_vec(),
        };
        Ok(Self {
            id: self.id,
            name: self.name.clone(),
            schema: Arc::new(project_schema(&self.schema, indices)?),
            projection: Some(projection),
        })
    }

    /// Return a scan with the same output, for a CTE body that keeps only its
    /// columns `kept`, in this order.
    ///
    /// Returns an error if this scan reads a column that is not in `kept`.
    pub(crate) fn with_body_columns(&self, kept: &[usize]) -> Result<Self> {
        let old: Vec<usize> = match &self.projection {
            Some(projection) => projection.clone(),
            // The scan reads all the columns, so `kept` must be all of them.
            None => (0..self.schema.fields().len()).collect(),
        };
        let projection = old
            .iter()
            .map(|index| {
                kept.iter().position(|k| k == index).ok_or_else(|| {
                    internal_datafusion_err!(
                        "A scan of the materialized CTE {} reads the column {index}, which was removed from the CTE",
                        self.name
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // A scan that reads all the columns in order has no projection.
        let projection =
            (!projection.iter().copied().eq(0..kept.len())).then_some(projection);
        Ok(Self {
            id: self.id,
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            projection,
        })
    }

    /// Return this scan with the fields that it reads from a CTE body with
    /// the schema `body`. The scan keeps its qualifiers and its projection.
    pub(crate) fn with_body_schema(&self, body: &DFSchema) -> Result<Self> {
        // The analyzer can change the types of the body, so only the indexes
        // and the names are checked here.
        self.check_body_names(body)?;
        let body = match &self.projection {
            Some(projection) => project_schema(body, projection)?,
            None => body.clone(),
        };
        let qualifiers = self.schema.iter().map(|(q, _)| q.cloned()).collect();
        let schema =
            DFSchema::from_field_specific_qualified_schema(qualifiers, body.inner())?
                .with_functional_dependencies(body.functional_dependencies().clone())?;
        Ok(Self {
            schema: Arc::new(schema),
            ..self.clone()
        })
    }

    /// Check that the columns that this scan reads exist in a CTE body with
    /// the schema `body`, and have the names of the columns of this scan.
    fn check_body_names(&self, body: &DFSchema) -> Result<()> {
        let len = body.fields().len();
        let indices: Vec<usize> = match &self.projection {
            Some(projection) => projection.clone(),
            None => (0..len).collect(),
        };
        if indices.len() != self.schema.fields().len() {
            return internal_err!(
                "A scan of the materialized CTE {} has {} columns, but reads {} columns of the CTE",
                self.name,
                self.schema.fields().len(),
                indices.len()
            );
        }
        for (index, field) in indices.iter().zip(self.schema.fields()) {
            let Some(body_field) = body.fields().get(*index) else {
                return internal_err!(
                    "A scan of the materialized CTE {} reads the column {index}, but the CTE has {len} columns",
                    self.name
                );
            };
            if body_field.name() != field.name() {
                return internal_err!(
                    "A scan of the materialized CTE {} reads the column {index} as {}, but the CTE names it {}",
                    self.name,
                    field.name(),
                    body_field.name()
                );
            }
        }
        Ok(())
    }

    /// Check that this scan has the names and types of the columns that it
    /// reads from a CTE body with the schema `body`.
    pub(crate) fn check_body_schema(&self, body: &DFSchema) -> Result<()> {
        match &self.projection {
            None => self.schema.has_equivalent_names_and_types(body),
            Some(projection) => {
                let len = body.fields().len();
                if let Some(index) = projection.iter().find(|index| **index >= len) {
                    return internal_err!(
                        "A scan of the materialized CTE {} reads the column {index}, but the CTE has {len} columns",
                        self.name
                    );
                }
                self.schema
                    .has_equivalent_names_and_types(&project_schema(body, projection)?)
            }
        }
    }

    /// Return a copy of each scan of the CTE `id` in `plan`, also in subquery
    /// expressions.
    ///
    /// The search does not go below a [`MaterializedCte`] with the same `id`
    /// in `plan`. Such a node is a separate occurrence of the CTE (the id is
    /// copied when a plan is cloned) and owns the scans below it.
    pub(crate) fn find_in(
        plan: &LogicalPlan,
        id: MaterializedCteId,
    ) -> Result<Vec<Self>> {
        let mut scans = vec![];
        plan.apply_with_subqueries(|node| match node {
            LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte))
                if cte.id == id =>
            {
                Ok(TreeNodeRecursion::Jump)
            }
            LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(scan))
                if scan.id == id =>
            {
                scans.push(scan.clone());
                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        })?;
        Ok(scans)
    }
}

/// The columns `indices` of `schema`, with their qualifiers.
fn project_schema(schema: &DFSchema, indices: &[usize]) -> Result<DFSchema> {
    let fields = indices
        .iter()
        .map(|index| {
            let (qualifier, field) = schema.qualified_field(*index);
            (qualifier.cloned(), Arc::clone(field))
        })
        .collect();
    let dependencies = schema
        .functional_dependencies()
        .project_functional_dependencies(indices, indices.len());
    DFSchema::new_with_metadata(fields, schema.metadata().clone())?
        .with_functional_dependencies(dependencies)
}

/// Orders by `id` and `name`. Scans with the same `id` and `name` but a
/// different `schema` or `projection` are not comparable.
impl PartialOrd for MaterializedCteScan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self
            .id
            .cmp(&other.id)
            .then_with(|| self.name.cmp(&other.name))
        {
            Ordering::Equal if self != other => None,
            ord => Some(ord),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::builder::LogicalPlanBuilder;
    use crate::{EmptyRelation, exists};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Constraint, Constraints, DFSchema, FunctionalDependencies};
    use std::collections::HashMap;

    fn empty(produce_one_row: bool) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: Arc::new(DFSchema::empty()),
        }))
    }

    fn schema(data_type: DataType) -> DFSchemaRef {
        let schema = Schema::new(vec![Field::new("a", data_type, true)]);
        Arc::new(DFSchema::try_from_qualified_schema("c", &schema).unwrap())
    }

    fn body(data_type: DataType) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: schema(data_type),
        }))
    }

    fn scan(id: MaterializedCteId, data_type: DataType) -> LogicalPlan {
        LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(MaterializedCteScan::new(
            id,
            "c",
            schema(data_type),
        )))
    }

    #[test]
    fn partial_cmp_is_equal_only_for_equal_nodes() {
        let id = MaterializedCteId::next();
        let a = MaterializedCte {
            id,
            name: "c".to_string(),
            cte: empty(false),
            continuation: empty(false),
        };
        let mut b = a.clone();
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));

        b.continuation = empty(true);
        assert_ne!(a, b);
        assert_eq!(a.partial_cmp(&b), None);

        let c = MaterializedCte {
            id: MaterializedCteId::next(),
            ..a.clone()
        };
        assert_eq!(a.partial_cmp(&c), Some(Ordering::Less));
    }

    #[test]
    fn try_new_checks_the_schema_of_the_scans() -> Result<()> {
        let id = MaterializedCteId::next();
        let cte = MaterializedCte::try_new(
            id,
            "c",
            body(DataType::Int32),
            Arc::new(scan(id, DataType::Int32)),
        )?;
        assert_eq!(cte.id(), id);
        assert_eq!(cte.name(), "c");
        assert_eq!(cte.schema(), cte.continuation().schema());
        assert_eq!(cte.scans()?.len(), 1);

        let plan = LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte));
        assert_eq!(plan.inputs().len(), 2);
        assert_eq!(
            plan.display_indent().to_string(),
            "MaterializedCte: name=c\n  EmptyRelation: rows=0\n  MaterializedCteScan: name=c"
        );

        let err = MaterializedCte::try_new(
            id,
            "c",
            body(DataType::Int32),
            Arc::new(scan(id, DataType::Utf8)),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains(
                "A scan of the materialized CTE c does not have the schema of the CTE"
            ),
            "{err}"
        );

        // A scan of another CTE is not checked.
        MaterializedCte::try_new(
            id,
            "c",
            body(DataType::Int32),
            Arc::new(scan(MaterializedCteId::next(), DataType::Utf8)),
        )?;
        Ok(())
    }

    fn wide_schema() -> DFSchemaRef {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
            Field::new("d", DataType::Boolean, true),
        ]);
        Arc::new(DFSchema::try_from_qualified_schema("c", &schema).unwrap())
    }

    fn names(scan: &MaterializedCteScan) -> Vec<String> {
        scan.schema()
            .columns()
            .iter()
            .map(|c| c.flat_name())
            .collect()
    }

    #[test]
    fn scan_projection_refers_to_the_body() -> Result<()> {
        let id = MaterializedCteId::next();
        let body = wide_schema();
        let all = MaterializedCteScan::new(id, "c", Arc::clone(&body));
        assert_eq!(all.projection(), None);
        all.check_body_schema(&body)?;

        // All the columns in order is not a projection.
        assert_eq!(all.with_projection(&[0, 1, 2, 3])?, all);
        assert!(all.with_projection(&[4]).is_err());

        let bcd = all.with_projection(&[1, 2, 3])?;
        assert_eq!(bcd.projection(), Some([1, 2, 3].as_slice()));
        assert_eq!(names(&bcd), ["c.b", "c.c", "c.d"]);
        bcd.check_body_schema(&body)?;

        // The indexes of a second projection refer to the output of the scan.
        let bd = bcd.with_projection(&[0, 2])?;
        assert_eq!(bd.projection(), Some([1, 3].as_slice()));
        assert_eq!(names(&bd), ["c.b", "c.d"]);
        bd.check_body_schema(&body)?;

        let none = bd.with_projection(&[])?;
        assert_eq!(none.projection(), Some([].as_slice()));
        assert!(none.schema().fields().is_empty());
        none.check_body_schema(&body)?;

        // The body keeps `b`, `c` and `d`: the output of the scans is the
        // same, and the indexes change.
        let kept = [1, 2, 3];
        let new_body = project_schema(&body, &kept)?;
        let new_bd = bd.with_body_columns(&kept)?;
        assert_eq!(new_bd.projection(), Some([0, 2].as_slice()));
        assert_eq!(new_bd.schema(), bd.schema());
        new_bd.check_body_schema(&new_body)?;
        assert!(bd.check_body_schema(&new_body).is_err());

        let new_bcd = bcd.with_body_columns(&kept)?;
        assert_eq!(new_bcd.projection(), None);
        new_bcd.check_body_schema(&new_body)?;

        let new_none = none.with_body_columns(&kept)?;
        assert_eq!(new_none.projection(), Some([].as_slice()));

        // A scan cannot lose a column that it reads.
        assert!(all.with_body_columns(&kept).is_err());
        assert!(bd.with_body_columns(&[1, 2]).is_err());
        Ok(())
    }

    /// A schema with field and schema metadata, mixed nullability, and a
    /// primary key on `c`, which is at index 2.
    fn attributed_schema() -> DFSchemaRef {
        let field = |name: &str, data_type, nullable| {
            Field::new(name, data_type, nullable)
                .with_metadata(HashMap::from([("k".to_string(), name.to_string())]))
        };
        let schema = Schema::new_with_metadata(
            vec![
                field("a", DataType::Int32, true),
                field("b", DataType::Utf8, false),
                field("c", DataType::Float64, false),
                field("d", DataType::Boolean, true),
            ],
            HashMap::from([("s".to_string(), "v".to_string())]),
        );
        let dependencies = FunctionalDependencies::new_from_constraints(
            Some(&Constraints::new_unverified(vec![Constraint::PrimaryKey(
                vec![2],
            )])),
            4,
        );
        Arc::new(
            DFSchema::try_from_qualified_schema("c", &schema)
                .unwrap()
                .with_functional_dependencies(dependencies)
                .unwrap(),
        )
    }

    #[test]
    fn scan_projection_keeps_schema_attributes() -> Result<()> {
        let id = MaterializedCteId::next();
        let body = attributed_schema();
        let all = MaterializedCteScan::new(id, "c", Arc::clone(&body));

        // `[3, 2, 1]` then `[1, 2]` selects `c` and `b`, in that order.
        let cb = all.with_projection(&[3, 2, 1])?.with_projection(&[1, 2])?;
        assert_eq!(cb.projection(), Some([2, 1].as_slice()));
        let expected = Schema::new_with_metadata(
            vec![
                body.field(2).as_ref().clone(),
                body.field(1).as_ref().clone(),
            ],
            body.metadata().clone(),
        );
        assert_eq!(cb.schema().as_arrow(), &expected);
        assert!(!cb.schema().field(0).is_nullable());
        assert_eq!(cb.schema().field(0).metadata()["k"], "c");
        // The primary key moves from index 2 to index 0.
        let dependencies = cb.schema().functional_dependencies();
        assert_eq!(dependencies.len(), 1);
        assert_eq!(dependencies.iter().next().unwrap().source_indices, vec![0]);

        // The body keeps `b`, `c` and `d`. The scan schema does not change.
        let kept = [1, 2, 3];
        let new_body = project_schema(&body, &kept)?;
        let new_cb = cb.with_body_columns(&kept)?;
        assert_eq!(new_cb.projection(), Some([1, 0].as_slice()));
        assert_eq!(new_cb.schema(), cb.schema());
        assert_eq!(
            new_cb.schema().functional_dependencies(),
            cb.schema().functional_dependencies()
        );
        new_cb.check_body_schema(&new_body)?;
        Ok(())
    }

    #[test]
    fn with_body_schema_rejects_a_body_of_another_width() -> Result<()> {
        let id = MaterializedCteId::next();
        let all = MaterializedCteScan::new(id, "c", schema(DataType::Int32));
        // The analyzer can change the types of the body, so a new type is accepted.
        let utf8 = all.with_body_schema(&schema(DataType::Utf8))?;
        assert_eq!(utf8.schema().field(0).data_type(), &DataType::Utf8);
        let err = all.with_body_schema(&wide_schema()).unwrap_err();
        assert!(
            err.to_string()
                .contains("has 1 columns, but reads 4 columns of the CTE"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn with_body_schema_accepts_new_types_and_rejects_new_names() -> Result<()> {
        let id = MaterializedCteId::next();
        // The scan reads `c` and `a`.
        let ca =
            MaterializedCteScan::new(id, "c", wide_schema()).with_projection(&[2, 0])?;
        let field = |name: &str, data_type: DataType| Field::new(name, data_type, true);
        let qualified = |fields: Vec<Field>| {
            DFSchema::try_from_qualified_schema("c", &Schema::new(fields)).unwrap()
        };

        // The analyzer changes the type of `c` from Float64 to Int64.
        let coerced = qualified(vec![
            field("a", DataType::Int32),
            field("b", DataType::Utf8),
            field("c", DataType::Int64),
            field("d", DataType::Boolean),
        ]);
        let new_ca = ca.with_body_schema(&coerced)?;
        assert_eq!(new_ca.projection(), Some([2, 0].as_slice()));
        assert_eq!(new_ca.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(new_ca.schema().field(1).data_type(), &DataType::Int32);
        new_ca.check_body_schema(&coerced)?;
        // The strict check still rejects the old scan against the new body.
        assert!(ca.check_body_schema(&coerced).is_err());

        // A body that names the column `c` differently is rejected.
        let renamed = qualified(vec![
            field("a", DataType::Int32),
            field("b", DataType::Utf8),
            field("z", DataType::Float64),
            field("d", DataType::Boolean),
        ]);
        let err = ca.with_body_schema(&renamed).unwrap_err();
        assert!(
            err.to_string()
                .contains("reads the column 2 as c, but the CTE names it z"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn map_inputs_mut_rejects_a_body_with_other_columns() -> Result<()> {
        let id = MaterializedCteId::next();
        let mut cte = MaterializedCte::try_new(
            id,
            "c",
            body(DataType::Int32),
            Arc::new(scan(id, DataType::Int32)),
        )?;
        assert!(!cte.map_inputs_mut(|_| Ok(false))?);

        // A new body with the same names and types is accepted.
        let mut visited = 0;
        assert!(cte.map_inputs_mut(|input| {
            visited += 1;
            if visited == 1 {
                *input = LogicalPlan::clone(&body(DataType::Int32));
            }
            Ok(visited == 1)
        })?);
        assert_eq!(visited, 2);

        let err = cte
            .map_inputs_mut(|input| {
                if matches!(input, LogicalPlan::EmptyRelation(_)) {
                    *input = LogicalPlan::clone(&body(DataType::Utf8));
                }
                Ok(true)
            })
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("The body of the materialized CTE c changed its columns"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn check_invariants_rejects_a_scan_that_does_not_match_the_body() -> Result<()> {
        use crate::logical_plan::InvariantLevel;
        let id = MaterializedCteId::next();
        let cte = MaterializedCte::try_new(
            id,
            "c",
            body(DataType::Int32),
            Arc::new(scan(id, DataType::Int32)),
        )?;
        LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte.clone()))
            .check_invariants(InvariantLevel::Executable)?;

        // `with_new_exprs` does not check the scans, so a rule can build
        // this plan. The invariant check rejects it.
        let invalid = cte.with_new_inputs_unchecked(
            body(DataType::Int32),
            Arc::new(scan(id, DataType::Utf8)),
        );
        let invalid = LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(invalid));
        let err = invalid
            .check_invariants(InvariantLevel::Executable)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("A scan of the materialized CTE c does not match the CTE"),
            "{err}"
        );

        // A scan whose CTE is not in the plan is not checked.
        scan(id, DataType::Utf8).check_invariants(InvariantLevel::Executable)?;
        Ok(())
    }

    #[test]
    fn try_new_checks_the_projection_of_the_scans() -> Result<()> {
        let id = MaterializedCteId::next();
        let body = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: wide_schema(),
        }));
        let scan = MaterializedCteScan::new(id, "c", wide_schema());
        let narrow = scan.with_projection(&[1, 3])?;
        let plan =
            LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(narrow.clone()));
        assert_eq!(
            plan.display_indent().to_string(),
            "MaterializedCteScan: name=c projection=[b, d]"
        );
        MaterializedCte::try_new(id, "c", Arc::clone(&body), Arc::new(plan))?;

        // The projection does not select the columns of the scan schema.
        let wrong = MaterializedCteScan {
            projection: Some(vec![0, 1]),
            ..narrow.clone()
        };
        let plan = Arc::new(LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(
            wrong,
        )));
        assert!(MaterializedCte::try_new(id, "c", Arc::clone(&body), plan).is_err());

        let out_of_range = MaterializedCteScan {
            projection: Some(vec![1, 9]),
            ..narrow
        };
        let plan = Arc::new(LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(
            out_of_range,
        )));
        assert!(MaterializedCte::try_new(id, "c", body, plan).is_err());
        Ok(())
    }

    #[test]
    fn find_in_looks_in_subqueries_and_stops_at_the_same_cte() -> Result<()> {
        let id = MaterializedCteId::next();
        let other = MaterializedCteId::next();
        // The union holds one scan, one scan in a subquery expression, one
        // scan of another CTE, and a nested occurrence of the same CTE, which
        // owns its scan.
        let plan = LogicalPlanBuilder::from(scan(id, DataType::Int32))
            .filter(exists(Arc::new(scan(id, DataType::Int32))))?
            .union(scan(other, DataType::Int32))?
            .union(LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(
                MaterializedCte::try_new(
                    id,
                    "c",
                    body(DataType::Int32),
                    Arc::new(scan(id, DataType::Int32)),
                )?,
            )))?
            .build()?;
        assert_eq!(MaterializedCteScan::find_in(&plan, id)?.len(), 2);
        assert_eq!(MaterializedCteScan::find_in(&plan, other)?.len(), 1);
        assert!(
            MaterializedCteScan::find_in(&plan, MaterializedCteId::next())?.is_empty()
        );
        Ok(())
    }
}
