// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Column identity in logical plans: helpers over the [`ColumnId`]s of a
//! [`LogicalPlan`].
//!
//! A column of a plan can have a [`ColumnId`]: the fields of each node's
//! schema record the id of the column they are ([`DFSchema::field_ids`]), and
//! a [`Column`] read in an expression records the id of the column it reads
//! ([`Column::id`]). A lookup of a column with an id finds the field with that
//! id, and falls back to the qualifier and name otherwise.
//!
//! Ids are allocated by a [`ColumnIdGenerator`] shared by a session. Every
//! `LogicalPlanBuilder` has a generator: the session's, or else its own,
//! whose ids follow the highest id of its inputs (see
//! [`LogicalPlan::max_column_id`]).
//!
//! # Definitions and references
//!
//! A column is *defined* once in a plan, subqueries included: by the leaf
//! that produces it (`TableScan`, `Values`, `MaterializedCteScan`, the work
//! table scan of a recursive query, which is a `TableScan`, and an
//! `EmptyRelation` whose columns have ids), and by the
//! nodes that compute outputs: an output of a `Projection` or a
//! `DistinctOn`, a window expression of a `Window`, or a grouping or
//! aggregate expression of an `Aggregate` (a [`NamedExpr`]) defines the
//! column of its id, unless it passes a column through (a bare column with
//! the same id). Other nodes pass their inputs' ids through: a
//! schema can have the same id twice (`SELECT a, a`), and the fields hold the
//! same values. A plan value that is used twice (a view referenced twice, a
//! self-join of a DataFrame) is renumbered where it is blended in.
//!
//! Some nodes define columns without expressions: the outputs of a `Union`
//! and of a `RecursiveQuery` (they are not the columns of any input), the
//! unnested columns of an `Unnest` (the others pass through), and the mark
//! column of a mark join. Their ids are stored in the schema of the node,
//! allocated when it is built and kept when it is rebuilt.
//!
//! An aggregate with grouping sets also defines its grouping id column
//! (`__grouping_id`), whose id its grouping sets hold; a bare grouping column
//! that one of the sets omits defines a column (its output is NULL in the
//! rows of that set).
//!
//! [`NamedExpr`]: crate::NamedExpr
//!
//! The leaves built by `LogicalPlanBuilder` allocate ids (from the generator
//! they are given with `LogicalPlanBuilder::scan_with_column_id_generator`
//! and `LogicalPlanBuilder::values_with_column_id_generator`, as the SQL
//! planner does, or from a new one).
//! A `TableScan` defines every column of its source, projected or not
//! (`TableScan::source_column_ids`, allocated once when it is created): the
//! references of its filters resolve against them, and a scan rebuilt with
//! another projection (`TableScanBuilder::from`) shares them, so a column
//! pruned from the projection and added back keeps its id.
//!
//! # Binding
//!
//! A reference gets the id of the field it resolves to when it is resolved
//! by name: by the SQL planner (identifiers and outer references), by
//! `normalize_col` and the builder's normalization (unqualified columns, and
//! qualified columns without an id), and by `expr_as_column_expr`. Code that
//! builds a reference by name gets no id, and resolves as without ids.
//!
//! # Renumbering
//!
//! [`LogicalPlan::with_fresh_column_ids`] gives new ids to every column a
//! plan defines, and renames the references inside it to match. The blending
//! points renumber the incoming plan: inlining a view or provider plan
//! (always), the right side of a join or set operation built by
//! `LogicalPlanBuilder` (when its definitions overlap the left side's), a
//! subquery built from another plan (when its definitions overlap the outer
//! plan's), and each reference to a CTE in the SQL planner. Node
//! constructors never renumber: during optimization, the nodes above depend
//! on the ids of their children.
//!
//! After blending, an id that became ambiguous is stripped, never guessed:
//! the references that a builder join receives from its caller (keys,
//! filter) lose the ids defined on both sides before the right side was
//! renumbered, and so do the outer references of a renumbered subquery to
//! the ids it shared with the outer plan. They resolve by name, as without
//! ids.
//!
//! # Verification
//!
//! [`LogicalPlan::column_id_issues`] resolves every column reference against
//! the input schemas of its node (outer references against the enclosing
//! scopes) and reports what does not hold, see [`ColumnIdIssueKind`]. The
//! analyzer and the optimizer can report the issues each rule introduces,
//! and, when verification is on (`DATAFUSION_COLUMN_ID_VERIFY=1`, on in the
//! tests of the optimizer and in the sqllogictest runner, off for users),
//! fail a query whose rules break the ids (see the `column_id_check` module
//! of `datafusion-optimizer`, whose `check_column_ids_kept` compares the
//! issues of a plan before and after a rewrite, for the tests of downstream
//! rules).
//!
//! An extension node that does not support column ids
//! ([`UserDefinedLogicalNode::supports_column_ids`]) is a boundary: the
//! references of its expressions are not checked, nor the references above
//! it to the columns of its schema that have no id.
//!
//! [`UserDefinedLogicalNode::supports_column_ids`]: crate::UserDefinedLogicalNode::supports_column_ids
//!
//! [`DFSchema::field_ids`]: datafusion_common::DFSchema::field_ids
//! [`ColumnIdGenerator`]: datafusion_common::ColumnIdGenerator

use datafusion_common::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::logical_plan::{
    Aggregate, Distinct, DistinctOn, DmlStatement, Filter, Limit, MaterializedCtePlan,
    Projection, Repartition, Sort, Subquery, SubqueryAlias, TableScan, Window,
};
use crate::{Expr, LogicalPlan, WriteOp};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion,
};
use datafusion_common::{
    Column, ColumnId, ColumnIdGenerator, DFSchema, DFSchemaRef, JoinType, Result,
    TableReference,
};

/// Calls `f` on every column read by the expressions of `plan` (not of its
/// inputs, nor of its subqueries), with `true` for outer references.
pub(crate) fn apply_node_column_refs(
    plan: &LogicalPlan,
    mut f: impl FnMut(&Column, bool),
) -> Result<()> {
    plan.apply_expressions(|expr| {
        expr.apply(|expr| {
            match expr {
                Expr::Column(col) => f(col, false),
                Expr::OuterReferenceColumn(_, col) => f(col, true),
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(())
}

/// Returns `schema` where every field has a new id from `generator`, if no
/// field has an id yet (the schema of a leaf that defines its columns)
pub(crate) fn with_new_field_ids(
    schema: DFSchemaRef,
    generator: &ColumnIdGenerator,
) -> Result<DFSchemaRef> {
    if schema.field_ids().iter().any(Option::is_some) {
        return Ok(schema);
    }
    let ids = generator.next_n(schema.fields().len()).map(Some).collect();
    Ok(Arc::new(Arc::unwrap_or_clone(schema).with_field_ids(ids)?))
}

impl LogicalPlan {
    /// The highest [`ColumnId`] in this plan, including its subqueries: in the
    /// schemas of its nodes and in the columns its expressions read.
    ///
    /// The optimizer moves its [`ColumnIdGenerator`] past it before running
    /// the rules, so that a plan built elsewhere (decoded, or built with
    /// another generator) is safe to allocate into.
    ///
    /// [`ColumnIdGenerator`]: datafusion_common::ColumnIdGenerator
    pub fn max_column_id(&self) -> Option<ColumnId> {
        let mut max: Option<ColumnId> = None;
        let mut update = |id: Option<ColumnId>| {
            if id > max {
                max = id;
            }
        };
        // the closures never fail
        let _ = self.apply_with_subqueries(|plan| {
            plan.schema().field_ids().iter().for_each(|id| update(*id));
            // a scan defines the columns its projection omits too
            if let LogicalPlan::TableScan(TableScan {
                source_column_ids: Some(ids),
                ..
            }) = plan
            {
                ids.iter().for_each(|id| update(Some(*id)));
            }
            // so does a MERGE INTO with the columns of its target
            if let Some(ids) = merge_target_column_ids(plan) {
                ids.iter().for_each(|id| update(Some(*id)));
            }
            apply_node_column_refs(plan, |col, _| update(col.id))?;
            Ok(TreeNodeRecursion::Continue)
        });
        max
    }
}

/// The kind of a [`ColumnIdIssue`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ColumnIdIssueKind {
    /// A reference without an id
    Missing,
    /// A reference whose id is not in its input: it does not resolve (a
    /// reference with an id is never resolved by name)
    Stale,
    /// A reference whose id is found in its input, but whose name resolves to
    /// another field (a name that resolves to nothing is fine)
    Disagree,
    /// An id defined twice in the plan, subqueries included
    DuplicateDefinition,
}

impl ColumnIdIssueKind {
    /// All the kinds, in order
    pub const ALL: [ColumnIdIssueKind; 4] = [
        Self::Missing,
        Self::Stale,
        Self::Disagree,
        Self::DuplicateDefinition,
    ];

    /// The name of this kind, in snake case
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Stale => "stale",
            Self::Disagree => "disagree",
            Self::DuplicateDefinition => "duplicate_definition",
        }
    }
}

impl fmt::Display for ColumnIdIssueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A column reference or definition of a plan whose [`ColumnId`] is not
/// consistent, see [`LogicalPlan::column_id_issues`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnIdIssue {
    /// What is wrong
    pub kind: ColumnIdIssueKind,
    /// The reference, or for a [`ColumnIdIssueKind::DuplicateDefinition`],
    /// the field that defines the id again (with its id)
    pub column: Column,
    /// The node where it was found, displayed on one line
    pub node: String,
}

impl fmt::Display for ColumnIdIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.column.flat_name())?;
        if let Some(id) = self.column.id {
            write!(f, "{id}")?;
        }
        write!(f, " in {}", self.node)
    }
}

impl LogicalPlan {
    /// Checks the [`ColumnId`]s of this plan, including its subqueries, and
    /// returns what does not hold (see [`ColumnIdIssueKind`]).
    ///
    /// Every column reference in the expressions of a node is resolved
    /// against the input schemas of the node (against its own schema for a
    /// leaf), and every outer reference against the input schemas of the
    /// nodes whose subqueries enclose it, innermost first. "The name
    /// resolves" means the lookup by qualifier and name that is the fallback
    /// of a lookup by id.
    ///
    /// The definitions checked for uniqueness are the fields of the leaves
    /// (`TableScan`, `Values`, `MaterializedCteScan`), the outputs of
    /// projections and `DistinctOn`s that do not pass a column through, the
    /// window expressions of windows, and the columns that nodes define
    /// without expressions (see the [module documentation](self)).
    ///
    /// The expressions of an extension node that does not support column
    /// ids, and the references above it to its columns without an id, are not
    /// checked (see [`UserDefinedLogicalNode::supports_column_ids`]).
    ///
    /// [`UserDefinedLogicalNode::supports_column_ids`]: crate::UserDefinedLogicalNode::supports_column_ids
    pub fn column_id_issues(&self) -> Vec<ColumnIdIssue> {
        let mut verifier = Verifier {
            describe: true,
            ..Default::default()
        };
        verifier.check(self, &mut vec![]);
        verifier.issues
    }

    /// The number of [`Self::column_id_issues`] of each kind, indexed like
    /// [`ColumnIdIssueKind::ALL`] (by `kind as usize`). Cheaper than
    /// collecting the issues: the nodes are not described.
    pub fn column_id_issue_counts(&self) -> [usize; ColumnIdIssueKind::ALL.len()] {
        let mut verifier = Verifier::default();
        verifier.check(self, &mut vec![]);
        let mut counts = [0; ColumnIdIssueKind::ALL.len()];
        for issue in &verifier.issues {
            counts[issue.kind as usize] += 1;
        }
        counts
    }

    /// Calls `f` with the index in the schema and the id of each column this
    /// node defines itself (not those it passes through from its inputs):
    /// the columns of a leaf, the outputs of a projection, a `DistinctOn` or
    /// an aggregate that do not pass a column through, the window
    /// expressions of a window, the grouping id of grouping sets, the outputs of a union or a recursive query, the unnested
    /// columns of an unnest, and the mark column of a mark join.
    ///
    /// A scan with [`TableScan::source_column_ids`] defines every column of
    /// its source: the index is then the index in the source schema (see
    /// [`defined_column`]).
    fn for_each_node_defined_column_id(&self, mut f: impl FnMut(usize, ColumnId)) {
        match self {
            LogicalPlan::TableScan(TableScan {
                source_column_ids: Some(ids),
                ..
            }) => {
                for (i, id) in ids.iter().enumerate() {
                    f(i, *id)
                }
            }
            LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(_))
            | LogicalPlan::Union(_)
            | LogicalPlan::RecursiveQuery(_) => {
                for (i, id) in self.schema().field_ids().iter().enumerate() {
                    if let Some(id) = id {
                        f(i, *id)
                    }
                }
            }
            LogicalPlan::Projection(Projection { expr: entries, .. })
            | LogicalPlan::Distinct(Distinct::On(DistinctOn {
                select_expr: entries,
                ..
            })) => {
                for (i, entry) in entries.iter().enumerate() {
                    if let Some(id) = entry.defined_id() {
                        f(i, id)
                    }
                }
            }
            // the input columns pass through, the window expressions follow
            LogicalPlan::Window(window) => {
                let input_len = window.input.schema().fields().len();
                for (i, entry) in window.window_expr.iter().enumerate() {
                    if let Some(id) = entry.defined_id() {
                        f(input_len + i, id)
                    }
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                for (i, id) in aggregate.defined_column_ids() {
                    f(i, id)
                }
            }
            LogicalPlan::Unnest(unnest) => {
                for i in unnest.unnested_output_indices() {
                    if let Some(id) = unnest.schema.field_id(i) {
                        f(i, id)
                    }
                }
            }
            LogicalPlan::Join(join) => {
                if let Some(id) = join.mark_column_id() {
                    f(join.schema.fields().len() - 1, id)
                }
            }
            // the target columns of a MERGE INTO, which is not an input:
            // the index is the index in the target schema (see
            // [`defined_column`])
            LogicalPlan::Dml(_) => {
                if let Some(ids) = merge_target_column_ids(self) {
                    for (i, id) in ids.iter().enumerate() {
                        f(i, *id)
                    }
                }
            }
            _ => {}
        }
    }
}

/// The ids of the target columns of `plan`, if it is a MERGE INTO with ids
/// (see [`MergeIntoOp::target_column_ids`])
///
/// [`MergeIntoOp::target_column_ids`]: crate::dml::MergeIntoOp::target_column_ids
fn merge_target_column_ids(plan: &LogicalPlan) -> Option<&Arc<[ColumnId]>> {
    match plan {
        LogicalPlan::Dml(DmlStatement {
            op: WriteOp::MergeInto(merge_op),
            ..
        }) => merge_op.target_column_ids(),
        _ => None,
    }
}

impl LogicalPlan {
    /// The ids of the columns this plan defines, including in its subqueries
    /// (see the [module documentation](self)). The ids it only passes through
    /// or refers to from an outer query are not included.
    pub fn defined_column_ids(&self) -> HashSet<ColumnId> {
        let mut ids = HashSet::new();
        // the closure never fails
        let _ = self.apply_with_subqueries(|plan| {
            plan.for_each_node_defined_column_id(|_, id| {
                ids.insert(id);
            });
            Ok(TreeNodeRecursion::Continue)
        });
        ids
    }

    /// The lowest and the highest id of the columns this plan defines (see
    /// [`Self::defined_column_ids`]), `None` if it defines none. Cheaper than
    /// collecting them: nothing is hashed.
    fn defined_column_id_range(&self) -> Option<(ColumnId, ColumnId)> {
        let mut range: Option<(ColumnId, ColumnId)> = None;
        // the closure never fails
        let _ = self.apply_with_subqueries(|plan| {
            plan.for_each_node_defined_column_id(|_, id| {
                range = Some(match range {
                    Some((min, max)) => (min.min(id), max.max(id)),
                    None => (id, id),
                });
            });
            Ok(TreeNodeRecursion::Continue)
        });
        range
    }

    /// [`Self::defined_column_ids`], in the order in which they are found,
    /// without duplicates, so that renumbering is deterministic
    fn defined_column_ids_in_order(&self) -> Vec<ColumnId> {
        let mut ids = vec![];
        // the closure never fails
        let _ = self.apply_with_subqueries(|plan| {
            plan.for_each_node_defined_column_id(|_, id| ids.push(id));
            Ok(TreeNodeRecursion::Continue)
        });
        // without the duplicates (an id defined twice), sized once
        let mut seen = HashSet::with_capacity(ids.len());
        ids.retain(|id| seen.insert(*id));
        ids
    }

    /// Returns this plan where every column it defines has a new id from
    /// `generator`, and the references to them inside it (in schemas and
    /// expressions, subqueries included) are renamed to match. References to
    /// ids that the plan does not define, such as outer references to an
    /// enclosing query, are left alone.
    ///
    /// Used where a plan value is blended into another plan that may already
    /// contain it (see the [module documentation](self)), so that no id is
    /// defined twice. `LogicalPlanBuilder` does it where it combines plans
    /// (joins, set operations, subqueries) and where it inlines a view; code
    /// that reuses a plan fragment otherwise (builds a node over two copies of
    /// it by hand, or puts a copy of a subtree in a second place, as a rule
    /// that duplicates a subtree does) renumbers the copy with this method.
    /// In an optimizer rule, the generator is the
    /// `OptimizerConfig::column_id_generator`.
    ///
    /// # Example
    ///
    /// ```
    /// # use datafusion_common::{ColumnIdGenerator, Result};
    /// # use datafusion_expr::logical_plan::builder::table_scan;
    /// # use datafusion_expr::{JoinType, LogicalPlan, Union};
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use std::sync::Arc;
    /// # fn main() -> Result<()> {
    /// let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    /// let plan = table_scan(Some("t"), &schema, None)?.build()?;
    /// // a generator whose ids follow those of the plan
    /// let generator = ColumnIdGenerator::new_after(plan.max_column_id());
    /// // a union of the plan with itself, built by hand: the second input is
    /// // a copy, with ids of its own
    /// let copy = plan.clone().with_fresh_column_ids(&generator)?;
    /// assert!(plan.defined_column_ids().is_disjoint(&copy.defined_column_ids()));
    /// let union = LogicalPlan::Union(Union::try_new(
    ///     vec![Arc::new(plan), Arc::new(copy)],
    ///     &generator,
    /// )?);
    /// assert!(union.column_id_issues().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_fresh_column_ids(self, generator: &ColumnIdGenerator) -> Result<Self> {
        self.renumber(generator, &HashSet::new())
    }

    /// Whether this plan and `other` compute the same columns in the same
    /// way, up to the ids of the columns they define: two scans of the same
    /// table, or two plans of the same subquery, are equivalent although
    /// `==` tells them apart (it compares ids, see [`Column`]).
    ///
    /// References to columns that the plans do not define (outer references)
    /// must have the same ids. Used where "computes the same rows" is meant
    /// (common subexpressions, merging union branches, subquery equality).
    ///
    /// [`Column`]: datafusion_common::Column
    pub fn is_equivalent_up_to_column_ids(&self, other: &LogicalPlan) -> bool {
        if self == other {
            return true;
        }
        if self.schema().fields().len() != other.schema().fields().len() {
            return false;
        }
        // both plans renumbered from the same id, in the order in which they
        // define their columns
        let after = self.max_column_id().max(other.max_column_id());
        let canonical = |plan: &LogicalPlan| {
            plan.clone()
                .with_fresh_column_ids(&ColumnIdGenerator::new_after(after))
        };
        match (canonical(self), canonical(other)) {
            (Ok(mine), Ok(theirs)) => mine == theirs,
            _ => false,
        }
    }

    /// The id that `other` defines in place of each id this plan defines, if
    /// both plans define the same number of columns: for two plans equal up
    /// to their ids (`==` ignores them), the definitions are found in the
    /// same order, so this pairs each column of this plan with the same
    /// column of `other`.
    ///
    /// Used where a rule keeps one of two equal plans and moves expressions
    /// that read the other onto it (`UnionsToFilter`).
    pub fn corresponding_column_ids(
        &self,
        other: &LogicalPlan,
    ) -> Option<HashMap<ColumnId, ColumnId>> {
        let (mine, theirs) = (
            self.defined_column_ids_in_order(),
            other.defined_column_ids_in_order(),
        );
        (mine.len() == theirs.len()).then(|| mine.into_iter().zip(theirs).collect())
    }

    /// Returns this plan, where the columns of its output take the ids `ids`
    /// (by position): each output id that this plan defines is renamed to
    /// the id at its position, in the schemas and the references inside the
    /// plan (subqueries included).
    ///
    /// Used where this plan replaces a node that defined `ids`, as the only
    /// remaining input of a union replaces the union: the references above
    /// it keep finding their columns. The nodes inside the plan do not refer
    /// to its outputs, so renaming their definitions is safe.
    ///
    /// A position is left as it is when it cannot take its id: its output
    /// has no id, or one that this plan does not define (an outer
    /// reference), or one that an earlier position renamed to another id
    /// (`SELECT a, a`), or the new id is already defined in this plan.
    /// Returns [`Transformed::no`] if no position changed.
    pub fn with_output_column_ids(
        self,
        ids: &[Option<ColumnId>],
    ) -> Result<Transformed<Self>> {
        if ids.len() != self.schema().fields().len() {
            return Ok(Transformed::no(self));
        }
        let defined = self.defined_column_ids();
        let mut renames: HashMap<ColumnId, ColumnId> = HashMap::new();
        for (old, new) in self.schema().field_ids().iter().zip(ids) {
            let (Some(old), Some(new)) = (old, new) else {
                continue;
            };
            if old == new {
                renames.entry(*old).or_insert(*new);
                continue;
            }
            if !defined.contains(old) || defined.contains(new) {
                continue;
            }
            renames.entry(*old).or_insert(*new);
        }
        renames.retain(|old, new| old != new);
        if renames.is_empty() {
            return Ok(Transformed::no(self));
        }
        let renames = Renames { ids: renames };
        self.transform_up_with_subqueries(|plan| rename_node_column_ids(plan, &renames))
            .map(|transformed| Transformed::yes(transformed.data))
    }

    /// [`Self::with_fresh_column_ids`], where the outer references of this
    /// plan (not those of its subqueries, which can refer to this plan) to
    /// the ids in `strip_outer` lose their id instead of being renamed
    fn renumber(
        self,
        generator: &ColumnIdGenerator,
        strip_outer: &HashSet<ColumnId>,
    ) -> Result<Self> {
        let defined = self.defined_column_ids_in_order();
        if defined.is_empty() {
            return Ok(self);
        }
        let plan = if strip_outer.is_empty() {
            self
        } else {
            self.transform_up(|plan| {
                plan.map_expressions(|expr| {
                    expr.transform(|expr| match expr {
                        Expr::OuterReferenceColumn(field, col)
                            if col.id.is_some_and(|id| strip_outer.contains(&id)) =>
                        {
                            Ok(Transformed::yes(Expr::OuterReferenceColumn(
                                field,
                                col.with_id(None),
                            )))
                        }
                        expr => Ok(Transformed::no(expr)),
                    })
                })
            })?
            .data
        };
        // the new ids in one block: as many as `next` would give, in order
        let fresh = generator.next_n(defined.len());
        let renames = Renames {
            ids: defined.into_iter().zip(fresh).collect(),
        };
        plan.transform_up_with_subqueries(|plan| rename_node_column_ids(plan, &renames))
            .map(|transformed| transformed.data)
    }

    /// Returns this plan, renumbered with [`Self::with_fresh_column_ids`] if
    /// it defines an id that `existing` also defines: used where this plan is
    /// blended into a plan that contains `existing` (the right side of a
    /// join, a subquery of `existing`). The generator is first moved past
    /// the ids of both plans, which may have been built with other
    /// generators.
    pub fn with_fresh_column_ids_if_overlapping(
        self,
        existing: &LogicalPlan,
        generator: &ColumnIdGenerator,
    ) -> Result<Self> {
        self.blend_into(existing, generator).map(|(plan, _)| plan)
    }

    /// [`Self::with_fresh_column_ids_if_overlapping`], which also returns the
    /// ids that this plan defined and `existing` also defines (empty if it
    /// was not renumbered). After blending, a reference to one of them is
    /// ambiguous: the caller strips the ids of its own references to them
    /// (see [`strip_column_ids`]), so that they resolve by name.
    pub(crate) fn blend_into(
        self,
        existing: &LogicalPlan,
        generator: &ColumnIdGenerator,
    ) -> Result<(Self, HashSet<ColumnId>)> {
        // the ids that two plans built with one generator define are usually
        // in disjoint ranges (one side of a join was built before the other):
        // compare the ranges before collecting and looking up the ids, which
        // costs a hash per column of each side
        let Some((incoming_min, incoming_max)) = self.defined_column_id_range() else {
            return Ok((self, HashSet::new()));
        };
        match existing.defined_column_id_range() {
            Some((existing_min, existing_max))
                if incoming_max >= existing_min && existing_max >= incoming_min => {}
            _ => return Ok((self, HashSet::new())),
        }
        // the incoming plan is usually the smaller one: look at it first
        let defined = self.defined_column_ids();
        let mut overlap = HashSet::new();
        // the closure never fails
        let _ = existing.apply_with_subqueries(|plan| {
            plan.for_each_node_defined_column_id(|_, id| {
                if defined.contains(&id) {
                    overlap.insert(id);
                }
            });
            Ok(TreeNodeRecursion::Continue)
        });
        if overlap.is_empty() {
            return Ok((self, overlap));
        }
        // the plans may have been built with another generator: the new ids
        // must not collide with theirs
        for max in [self.max_column_id(), existing.max_column_id()]
            .into_iter()
            .flatten()
        {
            generator.advance_past(max);
        }
        Ok((self.with_fresh_column_ids(generator)?, overlap))
    }

    /// Returns this plan where each output of a subquery's top projection
    /// whose id is defined more than once in the plan gets a new id from
    /// `generator`. No reference is renamed: nothing refers by id to the
    /// outputs of a subquery, neither inside it (they are its top) nor
    /// outside it.
    ///
    /// Used after casting a scalar subquery with `ExprSchemable::cast_to`,
    /// which has no generator: the outputs that the cast defines get ids
    /// that are new within the subquery only.
    pub fn with_fresh_ids_for_colliding_subquery_outputs(
        self,
        generator: &ColumnIdGenerator,
    ) -> Result<Transformed<Self>> {
        // only the outputs that the top projection of a subquery defines are
        // given new ids: count the definitions of these ids only, if there
        // are any (counting every definition costs a hash per column of each
        // scan)
        let mut outputs = HashSet::new();
        // the closure never fails
        let _ = self.apply_with_subqueries(|plan| {
            if let LogicalPlan::Subquery(subquery) = plan
                && let LogicalPlan::Projection(projection) = subquery.subquery.as_ref()
            {
                outputs.extend(projection.expr.iter().filter_map(|e| e.defined_id()));
            }
            Ok(TreeNodeRecursion::Continue)
        });
        let (Some(min), Some(max)) = (outputs.iter().min(), outputs.iter().max()) else {
            return Ok(Transformed::no(self));
        };
        let (min, max) = (*min, *max);
        let mut counts: HashMap<ColumnId, usize> = HashMap::new();
        // the closure never fails
        let _ = self.apply_with_subqueries(|plan| {
            plan.for_each_node_defined_column_id(|_, id| {
                if (min..=max).contains(&id) && outputs.contains(&id) {
                    *counts.entry(id).or_default() += 1;
                }
            });
            Ok(TreeNodeRecursion::Continue)
        });
        if counts.values().all(|count| *count < 2) {
            return Ok(Transformed::no(self));
        }
        if let Some(max) = self.max_column_id() {
            generator.advance_past(max);
        }
        self.transform_down_with_subqueries(|plan| {
            let LogicalPlan::Subquery(mut subquery) = plan else {
                return Ok(Transformed::no(plan));
            };
            let LogicalPlan::Projection(projection) = subquery.subquery.as_ref() else {
                return Ok(Transformed::no(LogicalPlan::Subquery(subquery)));
            };
            let collides = |entry: &crate::NamedExpr| {
                entry
                    .defined_id()
                    .is_some_and(|id| counts.get(&id).is_some_and(|count| *count > 1))
            };
            if !projection.expr.iter().any(collides) {
                return Ok(Transformed::no(LogicalPlan::Subquery(subquery)));
            }
            let mut entries = projection.expr.clone();
            for entry in &mut entries {
                if let Some(id) = entry.defined_id()
                    && let Some(count) = counts.get_mut(&id)
                    && *count > 1
                {
                    *count -= 1;
                    entry.id = generator.next();
                }
            }
            subquery.subquery = Arc::new(LogicalPlan::Projection(Projection::try_new(
                entries,
                Arc::clone(&projection.input),
            )?));
            Ok(Transformed::yes(LogicalPlan::Subquery(subquery)))
        })
    }

    /// The schema stored in this node, if it stores one (other nodes return
    /// the schema of their input)
    fn stored_schema_mut(&mut self) -> Option<&mut DFSchemaRef> {
        use crate::logical_plan::*;
        match self {
            LogicalPlan::EmptyRelation(EmptyRelation { schema, .. })
            | LogicalPlan::Values(Values { schema, .. })
            | LogicalPlan::TableScan(TableScan {
                projected_schema: schema,
                ..
            })
            | LogicalPlan::Projection(Projection { schema, .. })
            | LogicalPlan::Distinct(Distinct::On(DistinctOn { schema, .. }))
            | LogicalPlan::Window(Window { schema, .. })
            | LogicalPlan::Aggregate(Aggregate { schema, .. })
            | LogicalPlan::Join(Join { schema, .. })
            | LogicalPlan::AsOfJoin(AsOfJoin { schema, .. })
            | LogicalPlan::SubqueryAlias(SubqueryAlias { schema, .. })
            | LogicalPlan::Union(Union { schema, .. })
            | LogicalPlan::Unnest(Unnest { schema, .. })
            | LogicalPlan::RecursiveQuery(RecursiveQuery { schema, .. })
            | LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(
                MaterializedCteScan { schema, .. },
            )) => Some(schema),
            // the schema of an extension is computed by the node from its
            // inputs, which are renamed before it; the other nodes store
            // fixed schemas without ids, or the schema of an input
            _ => None,
        }
    }
}

/// A new generator for a builder that was given none: its ids follow the
/// highest id of `plans` (see [`LogicalPlan::max_column_id`]), so that it
/// can allocate into them
pub(crate) fn own_column_id_generator<'a>(
    plans: impl IntoIterator<Item = &'a LogicalPlan>,
) -> Arc<ColumnIdGenerator> {
    let generator = ColumnIdGenerator::new();
    for plan in plans {
        if let Some(max) = plan.max_column_id() {
            generator.advance_past(max);
        }
    }
    Arc::new(generator)
}

/// Renumbers the subqueries of expressions about to be added to the plan
/// `outer`, when they define ids that `outer` or a subquery seen before
/// also define, as when a subquery is built from the plan it filters.
///
/// The generator is the one of the builder of `outer`: created (see
/// [`own_column_id_generator`]) when a subquery that defines ids is seen,
/// if the builder has none yet, and moved past the ids of each such
/// subquery, so that the ids the builder allocates next do not collide with
/// them.
pub(crate) struct SubqueryBlender<'a> {
    outer: &'a LogicalPlan,
    generator: &'a mut Option<Arc<ColumnIdGenerator>>,
    /// The ids defined by `outer` and the subqueries seen so far, computed
    /// when a subquery that defines ids may overlap them
    taken: Option<HashSet<ColumnId>>,
    /// Until `taken` is computed: the ranges of the ids defined by `outer`
    /// (computed when the first subquery that defines ids is seen) and by
    /// the subqueries seen so far, which are kept in `seen`
    taken_ranges: Option<Vec<(ColumnId, ColumnId)>>,
    seen: Vec<Arc<LogicalPlan>>,
}

impl<'a> SubqueryBlender<'a> {
    pub(crate) fn new(
        outer: &'a LogicalPlan,
        generator: &'a mut Option<Arc<ColumnIdGenerator>>,
    ) -> Self {
        Self {
            outer,
            generator,
            taken: None,
            taken_ranges: None,
            seen: vec![],
        }
    }

    /// Returns `expr` with its subqueries renumbered where needed
    pub(crate) fn blend(&mut self, expr: Expr) -> Result<Expr> {
        if !expr.exists(|e| Ok(subquery_of(e).is_some()))? {
            return Ok(expr);
        }
        expr.transform_down(|mut expr| {
            let Some(subquery) = subquery_mut(&mut expr) else {
                return Ok(Transformed::no(expr));
            };
            let Some((min, max)) = subquery.subquery.defined_column_id_range() else {
                return Ok(Transformed::no(expr));
            };
            let outer = self.outer;
            let generator = self
                .generator
                .get_or_insert_with(|| own_column_id_generator([outer]));
            if let Some(max) = subquery.subquery.max_column_id() {
                generator.advance_past(max);
            }
            // a subquery built with the generator of the outer plan defines
            // ids in a range of its own: compare the ranges before collecting
            // the ids, which costs a hash per column of each plan
            if self.taken.is_none() {
                let ranges = self.taken_ranges.get_or_insert_with(|| {
                    outer.defined_column_id_range().into_iter().collect()
                });
                if ranges.iter().all(|(lo, hi)| max < *lo || *hi < min) {
                    ranges.push((min, max));
                    self.seen.push(Arc::clone(&subquery.subquery));
                    return Ok(Transformed::no(expr));
                }
            }
            let taken = self.taken.get_or_insert_with(|| {
                let mut taken = outer.defined_column_ids();
                for plan in &self.seen {
                    taken.extend(plan.defined_column_ids());
                }
                taken
            });
            let defined = subquery.subquery.defined_column_ids();
            let overlap: HashSet<ColumnId> =
                defined.intersection(taken).copied().collect();
            let transformed = !overlap.is_empty();
            if transformed {
                // the outer references to the overlapping ids cannot tell the
                // outer column from the inner one: they lose their ids, and
                // resolve by name
                let plan = Arc::unwrap_or_clone(Arc::clone(&subquery.subquery));
                subquery.subquery = Arc::new(plan.renumber(generator, &overlap)?);
                for expr in &mut subquery.outer_ref_columns {
                    *expr = strip_column_ids(std::mem::take(expr), &overlap)?;
                }
                taken.extend(subquery.subquery.defined_column_ids());
            } else {
                taken.extend(defined);
            }
            Ok(Transformed::new_transformed(expr, transformed))
        })
        .map(|transformed| transformed.data)
    }
}

impl Expr {
    /// Returns this expression where the plan of every subquery expression in
    /// it has new ids from `generator` (see
    /// [`LogicalPlan::with_fresh_column_ids`]; their outer references are
    /// left alone).
    ///
    /// Used where a rule copies an expression that has subqueries to a second
    /// place in a plan: the copy is renumbered, so that no id is defined
    /// twice.
    pub fn with_fresh_subquery_column_ids(
        self,
        generator: &ColumnIdGenerator,
    ) -> Result<Transformed<Expr>> {
        self.transform_down(|mut expr| {
            let Some(subquery) = subquery_mut(&mut expr) else {
                return Ok(Transformed::no(expr));
            };
            let plan = Arc::unwrap_or_clone(Arc::clone(&subquery.subquery));
            subquery.subquery = Arc::new(plan.with_fresh_column_ids(generator)?);
            Ok(Transformed::new(expr, true, TreeNodeRecursion::Jump))
        })
    }

    /// Returns this expression where each subquery expression that defines
    /// an id in `seen` gets new ids from `generator` (see
    /// [`Self::with_fresh_subquery_column_ids`]); the ids that the subqueries
    /// define are added to `seen`, in order.
    ///
    /// Used after a rewrite that can put one subquery in two places in an
    /// expression (simplifying `coalesce(x, y)` into a `CASE` that repeats
    /// `x`): the second one is renumbered.
    pub fn with_fresh_column_ids_in_repeated_subqueries(
        self,
        seen: &mut HashSet<ColumnId>,
        generator: &ColumnIdGenerator,
    ) -> Result<Transformed<Expr>> {
        // most expressions have no subquery: look before rebuilding
        if !self.exists(|expr| Ok(subquery_of(expr).is_some()))? {
            return Ok(Transformed::no(self));
        }
        self.transform_down(|mut expr| {
            let Some(subquery) = subquery_mut(&mut expr) else {
                return Ok(Transformed::no(expr));
            };
            let defined = subquery.subquery.defined_column_ids();
            let transformed = !seen.is_disjoint(&defined);
            if transformed {
                let plan = Arc::unwrap_or_clone(Arc::clone(&subquery.subquery));
                subquery.subquery = Arc::new(plan.with_fresh_column_ids(generator)?);
                seen.extend(subquery.subquery.defined_column_ids());
            } else {
                seen.extend(defined);
            }
            Ok(Transformed::new(expr, transformed, TreeNodeRecursion::Jump))
        })
    }
}

/// The subquery of `expr`, if it is a subquery expression
fn subquery_of(expr: &Expr) -> Option<&Subquery> {
    use crate::expr::{Exists, InSubquery, SetComparison};
    match expr {
        Expr::Exists(Exists { subquery, .. })
        | Expr::InSubquery(InSubquery { subquery, .. })
        | Expr::SetComparison(SetComparison { subquery, .. })
        | Expr::ScalarSubquery(subquery) => Some(subquery),
        _ => None,
    }
}

/// The subquery of `expr`, if it is a subquery expression
fn subquery_mut(expr: &mut Expr) -> Option<&mut Subquery> {
    use crate::expr::{Exists, InSubquery, SetComparison};
    match expr {
        Expr::Exists(Exists { subquery, .. })
        | Expr::InSubquery(InSubquery { subquery, .. })
        | Expr::SetComparison(SetComparison { subquery, .. })
        | Expr::ScalarSubquery(subquery) => Some(subquery),
        _ => None,
    }
}

/// Returns `expr` where, if it is a subquery expression, the outer
/// references of its subquery (nested subqueries included) that have no id
/// and resolve by qualifier and name in `outer` (the schema of the input of
/// the node that holds `expr`) get the id of the field they resolve to.
///
/// This binds the outer references of a subquery built by name (with
/// `out_ref_col`) where it is placed in its outer query, as normalizing binds
/// the other columns of an expression.
pub(crate) fn bind_outer_references(
    mut expr: Expr,
    outer: &DFSchema,
) -> Result<Transformed<Expr>> {
    let Some(subquery) = subquery_mut(&mut expr) else {
        return Ok(Transformed::no(expr));
    };
    if !outer.field_ids().iter().any(Option::is_some) {
        return Ok(Transformed::no(expr));
    }
    let bind = |expr: Expr| match expr {
        Expr::OuterReferenceColumn(field, col) if col.id.is_none() => {
            match outer.maybe_index_of_column(&col) {
                Some(i) if outer.field_id(i).is_some() => {
                    let id = outer.field_id(i);
                    Ok(Transformed::yes(Expr::OuterReferenceColumn(
                        field,
                        col.with_id(id),
                    )))
                }
                _ => Ok(Transformed::no(Expr::OuterReferenceColumn(field, col))),
            }
        }
        expr => Ok(Transformed::no(expr)),
    };
    let plan = Arc::unwrap_or_clone(Arc::clone(&subquery.subquery))
        .transform_up_with_subqueries(|plan| {
            plan.map_expressions(|expr| expr.transform(bind))
        })?;
    let outer_ref_columns = std::mem::take(&mut subquery.outer_ref_columns)
        .into_iter()
        .map(|expr| expr.transform(bind))
        .collect::<Result<Vec<_>>>()?;
    let transformed = plan.transformed || outer_ref_columns.iter().any(|t| t.transformed);
    subquery.outer_ref_columns = outer_ref_columns.into_iter().map(|t| t.data).collect();
    if plan.transformed {
        subquery.subquery = Arc::new(plan.data);
    }
    Ok(Transformed::new_transformed(expr, transformed))
}

impl Expr {
    /// Returns this expression where no column reference (outer references
    /// included, but not the plans of subqueries) has an id.
    ///
    /// Equality includes ids (see [`Column`]): this compares an expression
    /// with one written by name, as tests do. Inside a plan, a column without
    /// an id does not refer to anything.
    pub fn without_column_ids(self) -> Expr {
        self.transform(|expr| match expr {
            Expr::Column(col) if col.id.is_some() => {
                Ok(Transformed::yes(Expr::Column(col.with_id(None))))
            }
            Expr::OuterReferenceColumn(field, col) if col.id.is_some() => Ok(
                Transformed::yes(Expr::OuterReferenceColumn(field, col.with_id(None))),
            ),
            _ => Ok(Transformed::no(expr)),
        })
        .map(|transformed| transformed.data)
        .expect("the closure never fails")
    }
}

/// Returns `expr` where the column references (outer references included)
/// to the ids in `ids` have no id, so that they resolve by qualifier and name
pub(crate) fn strip_column_ids(expr: Expr, ids: &HashSet<ColumnId>) -> Result<Expr> {
    if ids.is_empty() {
        return Ok(expr);
    }
    let strip = |col: &mut Column| {
        let stripped = col.id.is_some_and(|id| ids.contains(&id));
        if stripped {
            col.id = None;
        }
        stripped
    };
    expr.transform(|expr| match expr {
        Expr::Column(mut col) => {
            let stripped = strip(&mut col);
            Ok(Transformed::new_transformed(Expr::Column(col), stripped))
        }
        Expr::OuterReferenceColumn(field, mut col) => {
            let stripped = strip(&mut col);
            Ok(Transformed::new_transformed(
                Expr::OuterReferenceColumn(field, col),
                stripped,
            ))
        }
        _ => Ok(Transformed::no(expr)),
    })
    .map(|transformed| transformed.data)
}

/// The renames of [`LogicalPlan::renumber`]
struct Renames {
    /// The new id of each id that the plan defines
    ids: HashMap<ColumnId, ColumnId>,
}

impl Renames {
    /// Renames the id of `col`, returning true if it changed
    fn rename(&self, col: &mut Column) -> bool {
        match col.id.and_then(|id| self.ids.get(&id)) {
            Some(new_id) => {
                col.id = Some(*new_id);
                true
            }
            None => false,
        }
    }

    /// Renames the column references in `expr`, including the outer
    /// references listed by its subqueries
    fn rename_expr(&self, expr: Expr) -> Result<Transformed<Expr>> {
        expr.transform(|mut expr| {
            let transformed = match &mut expr {
                Expr::Column(col) => self.rename(col),
                Expr::OuterReferenceColumn(_, col) => self.rename(col),
                _ => match subquery_mut(&mut expr) {
                    Some(subquery) => {
                        let mut transformed = false;
                        for expr in &mut subquery.outer_ref_columns {
                            if let Expr::Column(col)
                            | Expr::OuterReferenceColumn(_, col) = expr
                            {
                                transformed |= self.rename(col);
                            }
                        }
                        transformed
                    }
                    None => false,
                },
            };
            Ok(Transformed::new_transformed(expr, transformed))
        })
    }
}

/// [`LogicalPlan::map_expressions`] for a rewrite that changes only what the
/// column references say (their ids or labels), not what they read: a
/// projection keeps its stored schema instead of deriving it again from its
/// expressions, as the stored one can differ by more than that (see
/// `align_plan_to_schema`)
fn map_expressions_keeping_schema<F>(
    plan: LogicalPlan,
    f: F,
) -> Result<Transformed<LogicalPlan>>
where
    F: FnMut(Expr) -> Result<Transformed<Expr>>,
{
    match plan {
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
        }) => Ok(expr.map_elements(f)?.update_data(|expr| {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
            })
        })),
        plan => plan.map_expressions(f),
    }
}

/// Renames the ids of the schema and the column references of `plan` (not of
/// its inputs) according to `renames`
fn rename_node_column_ids(
    mut plan: LogicalPlan,
    renames: &Renames,
) -> Result<Transformed<LogicalPlan>> {
    let rename = |id: Option<ColumnId>| -> Option<Option<ColumnId>> {
        id.and_then(|id| renames.ids.get(&id).map(|new_id| Some(*new_id)))
    };

    let mut transformed = false;
    if let Some(schema) = plan.stored_schema_mut()
        && schema.field_ids().iter().any(|id| rename(*id).is_some())
    {
        let ids = schema
            .field_ids()
            .iter()
            .map(|id| rename(*id).unwrap_or(*id))
            .collect();
        let old = std::mem::replace(schema, Arc::new(DFSchema::empty()));
        *schema = Arc::new(Arc::unwrap_or_clone(old).with_field_ids(ids)?);
        transformed = true;
    }

    // the columns of a scan's source, which its projected schema (renamed
    // above) shares
    if let LogicalPlan::TableScan(TableScan {
        source_column_ids: Some(ids),
        ..
    }) = &mut plan
        && ids.iter().any(|id| renames.ids.contains_key(id))
    {
        *ids = ids
            .iter()
            .map(|id| renames.ids.get(id).copied().unwrap_or(*id))
            .collect();
        transformed = true;
    }

    // the target columns of a MERGE INTO
    if let LogicalPlan::Dml(DmlStatement {
        op: WriteOp::MergeInto(merge_op),
        ..
    }) = &mut plan
        && let Some(ids) = merge_op.target_column_ids_mut()
        && ids.iter().any(|id| renames.ids.contains_key(id))
    {
        *ids = ids
            .iter()
            .map(|id| renames.ids.get(id).copied().unwrap_or(*id))
            .collect();
        transformed = true;
    }

    // columns stored outside of expressions
    match &mut plan {
        LogicalPlan::Subquery(Subquery {
            outer_ref_columns, ..
        }) => {
            for expr in outer_ref_columns {
                if let Expr::Column(col) | Expr::OuterReferenceColumn(_, col) = expr {
                    transformed |= renames.rename(col);
                }
            }
        }
        LogicalPlan::Unnest(unnest) => {
            for (_, list) in &mut unnest.list_type_columns {
                transformed |= renames.rename(&mut list.output_column);
            }
        }
        _ => {}
    }

    let mut result =
        map_expressions_keeping_schema(plan, |expr| renames.rename_expr(expr))?;
    result.transformed |= transformed;

    // the ids of the outputs of a projection, a window or a `DistinctOn`,
    // after its expressions (which keep the ids of their entries): a
    // pass-through follows its column
    let (entries, more_entries) = match &mut result.data {
        LogicalPlan::Projection(Projection { expr, .. })
        | LogicalPlan::Window(Window {
            window_expr: expr, ..
        })
        | LogicalPlan::Distinct(Distinct::On(DistinctOn {
            select_expr: expr, ..
        })) => (expr.as_mut_slice(), &mut [][..]),
        LogicalPlan::Aggregate(Aggregate {
            group_expr,
            aggr_expr,
            ..
        }) => (group_expr.as_mut_slice(), aggr_expr.as_mut_slice()),
        _ => (&mut [][..], &mut [][..]),
    };
    for entry in entries.iter_mut().chain(more_entries) {
        if let Some(new_id) = renames.ids.get(&entry.id) {
            entry.id = *new_id;
            result.transformed = true;
        }
    }
    // the grouping id column of grouping sets
    if let LogicalPlan::Aggregate(Aggregate {
        grouping_sets: Some(grouping_sets),
        ..
    }) = &mut result.data
        && let Some(new_id) = renames.ids.get(&grouping_sets.grouping_id)
    {
        grouping_sets.grouping_id = *new_id;
        result.transformed = true;
    }
    Ok(result)
}

#[derive(Default)]
struct Verifier {
    issues: Vec<ColumnIdIssue>,
    defined: HashSet<ColumnId>,
    /// Whether the issues describe their node (otherwise `node` is empty)
    describe: bool,
}

impl Verifier {
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn check(&mut self, plan: &LogicalPlan, outer_scopes: &mut Vec<Vec<DFSchemaRef>>) {
        // the description of the node, computed once if it has issues
        let mut node: Option<String> = None;
        let describe = self.describe;
        let mut report = |issues: &mut Vec<ColumnIdIssue>, kind, column: Column| {
            let node = if describe {
                node.get_or_insert_with(|| plan.display().to_string())
                    .clone()
            } else {
                String::new()
            };
            issues.push(ColumnIdIssue { kind, column, node });
        };

        let defined = &mut self.defined;
        let issues = &mut self.issues;
        plan.for_each_node_defined_column_id(|i, id| {
            if !defined.insert(id) {
                let column = defined_column(plan, i).with_id(Some(id));
                report(issues, ColumnIdIssueKind::DuplicateDefinition, column);
            }
        });

        let inputs = plan.inputs();
        let scopes = expression_scopes(plan);

        // the expressions of an extension node that does not support column
        // ids are not checked: its inputs are boundaries
        let check_expressions = match plan {
            LogicalPlan::Extension(extension) => extension.node.supports_column_ids(),
            _ => true,
        };
        if check_expressions {
            // `apply_node_column_refs` only fails if its closure does
            let _ = apply_node_column_refs(plan, |col, is_outer| {
                let kind = if is_outer {
                    classify(col, outer_scopes.iter().rev().flatten().map(|s| s.as_ref()))
                } else {
                    classify(col, scopes.iter().map(|s| s.as_ref()))
                };
                // a column without an id of an extension node that does not
                // support column ids is read by name
                if kind == Some(ColumnIdIssueKind::Missing)
                    && !is_outer
                    && reads_extension_column_without_id(&inputs, col)
                {
                    return;
                }
                if let Some(kind) = kind {
                    report(&mut self.issues, kind, col.clone());
                }
            });
        }

        outer_scopes.push(scopes);
        let _ = plan.apply_subqueries(|subquery| {
            self.check(subquery, outer_scopes);
            Ok(TreeNodeRecursion::Continue)
        });
        outer_scopes.pop();

        for input in inputs {
            self.check(input, outer_scopes);
        }
    }
}

/// The schemas in which the column references of the expressions of `plan`
/// (not its outer references) resolve, searched in order: its inputs', the
/// source of a scan for its filters, the target and source of a MERGE INTO,
/// or its own schema for another leaf
fn expression_scopes(plan: &LogicalPlan) -> Vec<DFSchemaRef> {
    // the expressions of a MERGE INTO read the columns of its target and of
    // its source (its input)
    if let LogicalPlan::Dml(
        dml @ DmlStatement {
            op: WriteOp::MergeInto(_),
            ..
        },
    ) = plan
        && let Ok(merge_schema) = dml.merge_schema()
    {
        return vec![Arc::new(merge_schema)];
    }
    match plan {
        // the filters of a scan read the columns of its source
        LogicalPlan::TableScan(scan) => match scan.filters.is_empty() {
            true => vec![Arc::clone(plan.schema())],
            false => vec![
                scan.source_schema()
                    .unwrap_or_else(|_| Arc::clone(plan.schema())),
            ],
        },
        _ => {
            let inputs = plan.inputs();
            if inputs.is_empty() {
                vec![Arc::clone(plan.schema())]
            } else {
                inputs
                    .iter()
                    .map(|input| Arc::clone(input.schema()))
                    .collect()
            }
        }
    }
}

/// Labels `col` with the qualifier and name of the field it reads by id, in
/// the first of `schemas` that has its id. Returns true if the label changed.
fn relabel_column<'a>(
    col: &mut Column,
    schemas: impl IntoIterator<Item = &'a DFSchema>,
) -> bool {
    let Some(id) = col.id else {
        return false;
    };
    let Some((qualifier, field)) = schemas.into_iter().find_map(|schema| {
        schema
            .index_of_column_id(id)
            .map(|i| schema.qualified_field(i))
    }) else {
        return false;
    };
    if col.relation.as_ref() == qualifier && col.name == *field.name() {
        return false;
    }
    col.relation = qualifier.cloned();
    col.name.clone_from(field.name());
    true
}

impl Expr {
    /// Returns this expression where each column reference with an id that
    /// one of `schemas` has (searched in order) is labelled with the
    /// qualifier and name of the field with that id.
    ///
    /// A reference reads its column by id, and its label (qualifier and
    /// name) is only display. This makes the label match the definition, for
    /// code that reads labels: display, the unparser, and the filters handed
    /// to a table provider. Outer references and references without an id
    /// (or whose id `schemas` do not have) are left alone.
    pub fn with_relabeled_columns(
        self,
        schemas: &[&DFSchema],
    ) -> Result<Transformed<Expr>> {
        self.transform(|expr| match expr {
            Expr::Column(mut col) => {
                let relabeled = relabel_column(&mut col, schemas.iter().copied());
                Ok(Transformed::new_transformed(Expr::Column(col), relabeled))
            }
            _ => Ok(Transformed::no(expr)),
        })
    }
}

impl LogicalPlan {
    /// Returns this plan where the label (qualifier and name) of every
    /// column reference with an id, outer references included, is the
    /// qualifier and name of the field it reads (see
    /// [`Expr::with_relabeled_columns`]).
    ///
    /// Rules move expressions between scopes and rename outputs; references
    /// keep resolving by id, but their labels can go out of date. The
    /// optimizer relabels the plan once when it ends, so that plans display
    /// what they read. An output computed by an expression without an alias
    /// is named after the expression (`t.a + Int64(1)`), so relabeling can
    /// rename it: the schemas of the nodes are recomputed (keeping their
    /// ids), bottom up, and the references above follow.
    pub fn with_relabeled_columns(self) -> Result<Transformed<LogicalPlan>> {
        relabel_plan(self, &mut vec![])
    }
}

impl LogicalPlan {
    /// Returns this plan where every column reference without an id (outer
    /// references included) gets the id of the field it resolves to by
    /// qualifier and name in the scope of its node, if that field has one.
    ///
    /// This binds the references of a plan whose definitions have ids but
    /// whose references were written by name, such as a plan encoded before
    /// column ids existed: a reference with an id is only looked up by id.
    pub fn with_bound_column_references(self) -> Result<Transformed<LogicalPlan>> {
        bind_plan(self, &mut vec![])
    }
}

/// See [`LogicalPlan::with_bound_column_references`]
#[cfg_attr(feature = "recursive_protection", recursive::recursive)]
fn bind_plan(
    plan: LogicalPlan,
    outer_scopes: &mut Vec<Vec<DFSchemaRef>>,
) -> Result<Transformed<LogicalPlan>> {
    plan.map_children(|input| bind_plan(input, outer_scopes))?
        .transform_data(|plan| {
            let scopes = expression_scopes(&plan);
            outer_scopes.push(scopes);
            let plan = plan.map_subqueries(|subquery| bind_plan(subquery, outer_scopes));
            let scopes = outer_scopes.pop().unwrap_or_default();
            plan?.transform_data(|plan| {
                // only a node with a reference without an id is rebuilt (not
                // an unnest, whose columns are rebuilt with it)
                if matches!(plan, LogicalPlan::Unnest(_)) {
                    return Ok(Transformed::no(plan));
                }
                let mut unbound = false;
                plan.apply_expressions(|expr| {
                    expr.apply(|expr| {
                        unbound = matches!(
                            expr,
                            Expr::Column(col) | Expr::OuterReferenceColumn(_, col)
                                if col.id.is_none()
                        );
                        Ok(if unbound {
                            TreeNodeRecursion::Stop
                        } else {
                            TreeNodeRecursion::Continue
                        })
                    })
                })?;
                if !unbound {
                    return Ok(Transformed::no(plan));
                }
                map_expressions_keeping_schema(plan, |expr| {
                    expr.transform(|expr| match expr {
                        Expr::Column(mut col) if col.id.is_none() => {
                            let bound =
                                bind_by_name(&mut col, scopes.iter().map(|s| s.as_ref()));
                            Ok(Transformed::new_transformed(Expr::Column(col), bound))
                        }
                        Expr::OuterReferenceColumn(field, mut col)
                            if col.id.is_none() =>
                        {
                            let bound = bind_by_name(
                                &mut col,
                                outer_scopes.iter().rev().flatten().map(|s| s.as_ref()),
                            );
                            Ok(Transformed::new_transformed(
                                Expr::OuterReferenceColumn(field, col),
                                bound,
                            ))
                        }
                        expr => Ok(Transformed::no(expr)),
                    })
                })
            })
        })
}

/// Gives `col` the id of the field it resolves to by qualifier and name in
/// the first of `schemas` that has it, if that field has an id. Returns true
/// if it got one.
fn bind_by_name<'a>(
    col: &mut Column,
    schemas: impl IntoIterator<Item = &'a DFSchema>,
) -> bool {
    let id = schemas.into_iter().find_map(|schema| {
        schema
            .maybe_index_of_column(col)
            .map(|i| schema.field_id(i))
    });
    match id {
        Some(Some(id)) => {
            col.id = Some(id);
            true
        }
        _ => false,
    }
}

/// See [`LogicalPlan::with_relabeled_columns`]. `outer_scopes` are the
/// scopes of the enclosing nodes of a subquery, innermost last.
#[cfg_attr(feature = "recursive_protection", recursive::recursive)]
fn relabel_plan(
    plan: LogicalPlan,
    outer_scopes: &mut Vec<Vec<DFSchemaRef>>,
) -> Result<Transformed<LogicalPlan>> {
    plan.map_children(|input| relabel_plan(input, outer_scopes))?
        .transform_data(|plan| {
            let scopes = expression_scopes(&plan);
            // the subqueries of the expressions are enclosed by this node
            outer_scopes.push(scopes);
            let plan =
                plan.map_subqueries(|subquery| relabel_plan(subquery, outer_scopes));
            let scopes = outer_scopes.pop().unwrap_or_default();
            plan?.transform_data(|plan| relabel_node(plan, &scopes, outer_scopes))
        })
        .and_then(|plan| {
            if !plan.transformed {
                return Ok(plan);
            }
            // the names of the outputs may follow the new labels (or the
            // renamed outputs of the inputs); the ids do not change
            plan.map_data(|plan| {
                let ids = plan.schema().field_ids().to_vec();
                let plan = keep_names_that_would_collide(plan)?;
                let mut plan = plan.recompute_schema()?;
                if let Some(schema) = plan.stored_schema_mut()
                    && schema.fields().len() == ids.len()
                    && schema.field_ids() != ids.as_slice()
                {
                    let old = std::mem::replace(schema, Arc::new(DFSchema::empty()));
                    *schema = Arc::new(Arc::unwrap_or_clone(old).with_field_ids(ids)?);
                }
                Ok(plan)
            })
        })
}

/// A projection whose relabeled outputs would have the same name (two
/// expressions that read the same column under different labels, `x.a + 1`
/// and `t.a + 1`) keeps the previous names of those outputs, as an alias:
/// names are unique in a schema
fn keep_names_that_would_collide(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(mut projection) = plan else {
        return Ok(plan);
    };
    let names = projection
        .expr
        .iter()
        .map(|entry| entry.expr.qualified_name())
        .collect::<Vec<_>>();
    let mut counts: HashMap<&(Option<TableReference>, String), usize> = HashMap::new();
    for name in &names {
        *counts.entry(name).or_default() += 1;
    }
    if counts.values().all(|count| *count == 1) {
        return Ok(LogicalPlan::Projection(projection));
    }
    let collides = names
        .iter()
        .map(|name| counts[name] > 1)
        .collect::<Vec<_>>();
    for (((entry, collides), name), (qualifier, field)) in projection
        .expr
        .iter_mut()
        .zip(collides)
        .zip(&names)
        .zip(projection.schema.iter())
    {
        // a renamed output (a column is named after the field it reads,
        // which relabeling does not change)
        let renamed = name.0.as_ref() != qualifier || name.1 != *field.name();
        if collides && renamed {
            let expr = std::mem::take(&mut entry.expr);
            entry.expr = expr.alias_qualified(qualifier.cloned(), field.name());
        }
    }
    Ok(LogicalPlan::Projection(projection))
}

/// Relabels the column references of the expressions of `plan` (not of its
/// inputs or subqueries): in `scopes`, or for outer references in
/// `outer_scopes`, innermost first
fn relabel_node(
    mut plan: LogicalPlan,
    scopes: &[DFSchemaRef],
    outer_scopes: &[Vec<DFSchemaRef>],
) -> Result<Transformed<LogicalPlan>> {
    let outer = || outer_scopes.iter().rev().flatten().map(|s| s.as_ref());
    let mut transformed = false;
    // the outer references a subquery lists, stored outside of expressions
    if let LogicalPlan::Subquery(Subquery {
        outer_ref_columns, ..
    }) = &mut plan
    {
        for expr in outer_ref_columns {
            if let Expr::Column(col) | Expr::OuterReferenceColumn(_, col) = expr {
                transformed |= relabel_column(col, outer());
            }
        }
    }
    // the expressions of an extension node that does not support column
    // ids may read its inputs by name
    if let LogicalPlan::Extension(extension) = &plan
        && !extension.node.supports_column_ids()
    {
        return Ok(Transformed::new_transformed(plan, transformed));
    }
    let mut result = map_expressions_keeping_schema(plan, |expr| {
        expr.transform(|expr| match expr {
            Expr::Column(mut col) => {
                let relabeled =
                    relabel_column(&mut col, scopes.iter().map(|s| s.as_ref()));
                Ok(Transformed::new_transformed(Expr::Column(col), relabeled))
            }
            Expr::OuterReferenceColumn(field, mut col) => {
                let relabeled = relabel_column(&mut col, outer());
                Ok(Transformed::new_transformed(
                    Expr::OuterReferenceColumn(field, col),
                    relabeled,
                ))
            }
            _ => Ok(Transformed::no(expr)),
        })
    })?;
    result.transformed |= transformed;
    Ok(result)
}

/// The column that `plan` defines at index `i` (see
/// [`LogicalPlan::for_each_node_defined_column_id`]), without its id
fn defined_column(plan: &LogicalPlan, i: usize) -> Column {
    match plan {
        LogicalPlan::TableScan(TableScan {
            table_name,
            source,
            source_column_ids: Some(_),
            ..
        }) => Column::new(Some(table_name.clone()), source.schema().field(i).name()),
        LogicalPlan::Dml(DmlStatement {
            target,
            op: WriteOp::MergeInto(merge_op),
            ..
        }) => Column::new(
            Some(merge_op.target_qualifier().clone()),
            target.schema().field(i).name(),
        ),
        _ => Column::from(plan.schema().qualified_field(i)),
    }
}

/// Whether `col` reads by name, in one of `inputs`, a column without an id
/// of an extension node that does not support column ids (see
/// [`UserDefinedLogicalNode::supports_column_ids`]), passed through
///
/// [`UserDefinedLogicalNode::supports_column_ids`]: crate::UserDefinedLogicalNode::supports_column_ids
fn reads_extension_column_without_id(inputs: &[&LogicalPlan], col: &Column) -> bool {
    inputs.iter().any(|input| {
        let schema = input.schema();
        schema
            .index_of_column_by_name(col.relation.as_ref(), &col.name)
            .is_some_and(|i| {
                schema.field_id(i).is_none() && is_extension_column_without_id(input, i)
            })
    })
}

/// Whether the column at index `i` of the schema of `plan` is a column
/// without an id of an extension node that does not support column ids,
/// passed through by `plan` and the nodes below it
fn is_extension_column_without_id(plan: &LogicalPlan, i: usize) -> bool {
    match plan {
        LogicalPlan::Extension(extension) => {
            !extension.node.supports_column_ids()
                && extension.node.schema().field_id(i).is_none()
        }
        LogicalPlan::Filter(Filter { input, .. })
        | LogicalPlan::Sort(Sort { input, .. })
        | LogicalPlan::Limit(Limit { input, .. })
        | LogicalPlan::Repartition(Repartition { input, .. })
        | LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. })
        | LogicalPlan::Distinct(Distinct::All(input)) => {
            is_extension_column_without_id(input, i)
        }
        LogicalPlan::Window(window) => {
            i < window.input.schema().fields().len()
                && is_extension_column_without_id(&window.input, i)
        }
        LogicalPlan::Join(join) => {
            let left_len = join.left.schema().fields().len();
            let right_len = join.right.schema().fields().len();
            match join.join_type {
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                    i < left_len && is_extension_column_without_id(&join.left, i)
                }
                JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                    i < right_len && is_extension_column_without_id(&join.right, i)
                }
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    if i < left_len {
                        is_extension_column_without_id(&join.left, i)
                    } else {
                        is_extension_column_without_id(&join.right, i - left_len)
                    }
                }
            }
        }
        _ => false,
    }
}

/// Classifies the reference `col` against `schemas`, searched in order.
/// Returns `None` if it is consistent.
fn classify<'a>(
    col: &Column,
    mut schemas: impl Iterator<Item = &'a DFSchema>,
) -> Option<ColumnIdIssueKind> {
    let Some(id) = col.id else {
        return Some(ColumnIdIssueKind::Missing);
    };
    // the ids of the fields the name resolves to in `schema` (several fields
    // can have the same name if they have ids)
    let ids_by_name = |schema: &DFSchema| {
        schema
            .iter()
            .zip(schema.field_ids())
            .filter(|((qualifier, field), _)| {
                field.name() == &col.name
                    && match (&col.relation, qualifier) {
                        (Some(r), Some(q)) => r.resolved_eq(q),
                        (Some(_), None) => false,
                        (None, _) => true,
                    }
            })
            .map(|(_, id)| *id)
            .collect::<Vec<_>>()
    };
    // A reference found by id must agree with its name in the schema where
    // it is found: the inputs of a join can both have a field with the same
    // qualified name (a join of two unnamed tables), and each join key
    // resolves in its own side. The name agrees if it does not resolve, or
    // if one of the fields it resolves to has the id.
    if let Some(schema) = schemas.find(|schema| schema.index_of_column_id(id).is_some()) {
        let ids = ids_by_name(schema);
        return (!ids.is_empty() && !ids.contains(&Some(id)))
            .then_some(ColumnIdIssueKind::Disagree);
    }
    // not found by id: it does not resolve (not by name either)
    Some(ColumnIdIssueKind::Stale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::builder::table_scan;
    use crate::{LogicalPlanBuilder, NamedExpr, col, exists, lit};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ColumnIdGenerator;
    use std::sync::Arc;

    #[test]
    fn max_column_id_without_ids() -> Result<()> {
        let plan = filter(scan_with_ids("t", [None, None])?, col("a"))?;
        assert_eq!(plan.max_column_id(), None);
        Ok(())
    }

    #[test]
    fn max_column_id_finds_references_in_subqueries() -> Result<()> {
        let high = id(100);
        // a subquery reading an outer reference with the highest id
        let outer_ref = Expr::OuterReferenceColumn(
            Arc::new(Field::new("a", DataType::Int32, false)),
            Column::new(Some("t"), "a").with_id(Some(high)),
        );
        let inner_ref = Expr::Column(Column::new(Some("s"), "a").with_id(Some(id(1))));
        let subquery = filter(
            scan_with_ids("s", [Some(1), Some(2)])?,
            outer_ref.eq(inner_ref),
        )?;
        let plan = LogicalPlan::Filter(Filter::try_new(
            exists(Arc::new(subquery)),
            Arc::new(scan_with_ids("t", [Some(3), Some(4)])?),
        )?);
        assert_eq!(plan.max_column_id(), Some(high));
        Ok(())
    }

    /// The id `n`
    fn id(n: u64) -> ColumnId {
        ColumnId::new(std::num::NonZeroU64::new(n).unwrap())
    }

    /// A scan of `name` with Int32 columns `a` and `b` that have the given
    /// ids
    fn scan_with_ids(name: &str, ids: [Option<u64>; 2]) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let LogicalPlan::TableScan(mut scan) =
            table_scan(Some(name), &schema, None)?.build()?
        else {
            unreachable!()
        };
        let projected = Arc::unwrap_or_clone(scan.projected_schema)
            .with_field_ids(ids.iter().map(|n| n.map(id)).collect())?;
        // the ids are set by hand: the scan defines its projected columns
        scan.source_column_ids = None;
        scan.projected_schema = Arc::new(projected);
        Ok(LogicalPlan::TableScan(scan))
    }

    /// A reference to `relation.name` with the id `n`
    fn col_with_id(relation: &str, name: &str, n: Option<u64>) -> Expr {
        Expr::Column(Column::new(Some(relation), name).with_id(n.map(id)))
    }

    /// A filter `expr = 1` over `input`
    fn filter(input: LogicalPlan, expr: Expr) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Filter(Filter::try_new(
            expr.eq(lit(1)),
            Arc::new(input),
        )?))
    }

    fn kinds(plan: &LogicalPlan) -> Vec<ColumnIdIssueKind> {
        plan.column_id_issues().iter().map(|i| i.kind).collect()
    }

    #[test]
    fn consistent_plan_has_no_issues() -> Result<()> {
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        // a name that resolves to nothing is fine
        let plan = filter(scan, col_with_id("t", "a", Some(1)))?;
        assert_eq!(kinds(&plan), vec![]);
        // so is an unqualified name that finds the same field
        let plan = filter(
            plan,
            Expr::Column(Column::from_name("b").with_id(Some(id(2)))),
        )?;
        assert_eq!(kinds(&plan), vec![]);
        Ok(())
    }

    #[test]
    fn relabel_names_references_after_their_fields() -> Result<()> {
        // `x.a` reads `t.a` by id, below the alias `x`
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let input = filter(scan, col_with_id("x", "a", Some(1)))?;
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![
                NamedExpr::new(col_with_id("x", "a", Some(1)) + lit(1), id(3)),
                NamedExpr::new(col_with_id("x", "b", Some(2)), id(2)),
            ],
            Arc::new(input),
        )?);
        let plan =
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(plan), "x")?);
        // above the alias, the label is right
        let plan = filter(plan, col_with_id("x", "b", Some(2)))?;
        let relabeled = plan.clone().with_relabeled_columns()?;
        assert!(relabeled.transformed);
        let relabeled = relabeled.data;
        // the computed output is named after the new label
        assert_eq!(
            relabeled.display_indent().to_string(),
            "Filter: x.b = Int32(1)\
            \n  SubqueryAlias: x\
            \n    Projection: t.a + Int32(1), t.b\
            \n      Filter: t.a = Int32(1)\
            \n        TableScan: t"
        );
        assert_eq!(output_ids(&relabeled), output_ids(&plan));
        assert_eq!(kinds(&relabeled), vec![]);
        // relabeling again changes nothing
        assert!(!relabeled.with_relabeled_columns()?.transformed);
        Ok(())
    }

    #[test]
    fn relabel_keeps_names_that_would_collide() -> Result<()> {
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![
                NamedExpr::new(col_with_id("x", "a", Some(1)) + lit(1), id(3)),
                NamedExpr::new(col_with_id("t", "a", Some(1)) + lit(1), id(4)),
            ],
            Arc::new(scan),
        )?);
        let relabeled = plan.clone().with_relabeled_columns()?.data;
        assert_eq!(
            relabeled.display_indent().to_string(),
            "Projection: t.a + Int32(1) AS x.a + Int32(1), t.a + Int32(1)\
            \n  TableScan: t"
        );
        assert_eq!(output_ids(&relabeled), output_ids(&plan));
        assert_eq!(
            relabeled.schema().field_names(),
            plan.schema().field_names()
        );
        Ok(())
    }

    #[test]
    fn missing() -> Result<()> {
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = filter(scan, col("t.a"))?;
        let issues = plan.column_id_issues();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].kind, ColumnIdIssueKind::Missing);
        assert_eq!(
            issues[0].to_string(),
            "missing: t.a in Filter: t.a = Int32(1)"
        );
        Ok(())
    }

    #[test]
    fn stale() -> Result<()> {
        // the name resolves to a field without an id
        let scan = scan_with_ids("t", [None, None])?;
        let plan = filter(scan, col_with_id("t", "a", Some(7)))?;
        assert_eq!(kinds(&plan), vec![ColumnIdIssueKind::Stale]);
        Ok(())
    }

    #[test]
    fn stale_whatever_the_name() -> Result<()> {
        // the name resolves to a field with another id: a reference with an
        // id is not resolved by name
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = filter(scan, col_with_id("t", "a", Some(7)))?;
        let issues = plan.column_id_issues();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].kind, ColumnIdIssueKind::Stale);
        assert_eq!(
            issues[0].to_string(),
            "stale: t.a#7 in Filter: t.a = Int32(1)"
        );
        Ok(())
    }

    #[test]
    fn disagree() -> Result<()> {
        // the id finds `a`, the name finds `b`
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = filter(scan, col_with_id("t", "b", Some(1)))?;
        assert_eq!(kinds(&plan), vec![ColumnIdIssueKind::Disagree]);
        Ok(())
    }

    #[test]
    fn duplicate_definition() -> Result<()> {
        // the same scan twice, as when a plan value is used twice
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let left = LogicalPlanBuilder::from(scan.clone()).alias("l")?.build()?;
        let right = LogicalPlanBuilder::from(scan).alias("r")?.build()?;
        let join = crate::Join::try_new(
            Arc::new(left),
            Arc::new(right),
            vec![],
            None,
            JoinType::Inner,
            crate::JoinConstraint::On,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )?;
        let plan = LogicalPlan::Join(join);
        assert_eq!(
            kinds(&plan),
            vec![
                ColumnIdIssueKind::DuplicateDefinition,
                ColumnIdIssueKind::DuplicateDefinition
            ]
        );

        // passing an id through twice is not a definition
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = LogicalPlanBuilder::from(scan)
            .project(vec![col("t.a"), col("t.a").alias("a2")])?
            .build()?;
        assert!(!kinds(&plan).contains(&ColumnIdIssueKind::DuplicateDefinition));
        Ok(())
    }

    #[test]
    fn outer_references_resolve_against_enclosing_scopes() -> Result<()> {
        let outer_ref = |n| {
            Expr::OuterReferenceColumn(
                Arc::new(Field::new("a", DataType::Int32, false)),
                Column::new(Some("t"), "a").with_id(Some(id(n))),
            )
        };
        let subquery_plan = |n| -> Result<LogicalPlan> {
            let inner = scan_with_ids("s", [Some(3), Some(4)])?;
            LogicalPlanBuilder::from(inner)
                .filter(outer_ref(n).eq(col_with_id("s", "a", Some(3))))?
                .build()
        };

        // the outer reference finds `t.a#1` in the enclosing scope
        let plan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = LogicalPlanBuilder::from(plan)
            .filter(exists(Arc::new(subquery_plan(1)?)))?
            .build()?;
        assert_eq!(kinds(&plan), vec![]);

        // an outer reference to an id that is not in the enclosing scope
        let plan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = LogicalPlanBuilder::from(plan)
            .filter(exists(Arc::new(subquery_plan(9)?)))?
            .build()?;
        assert_eq!(kinds(&plan), vec![ColumnIdIssueKind::Stale]);
        Ok(())
    }

    /// A scan of `name` with columns `a` and `b` whose ids come from
    /// `generator`
    fn scan_from(name: &str, generator: &ColumnIdGenerator) -> Result<LogicalPlan> {
        scan_with_ids(
            name,
            [
                Some(generator.next().get().get()),
                Some(generator.next().get().get()),
            ],
        )
    }

    /// The ids of the fields of the output of `plan`
    fn output_ids(plan: &LogicalPlan) -> Vec<Option<ColumnId>> {
        plan.schema().field_ids().to_vec()
    }

    #[test]
    fn self_join_through_builder_gets_distinct_ids() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let scan = scan_from("t", &generator)?;
        let right = LogicalPlanBuilder::from(scan.clone()).alias("r")?.build()?;
        let plan = LogicalPlanBuilder::from(scan)
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .alias("l")?
            .cross_join(right)?
            .build()?;

        let inputs = plan.inputs();
        assert_eq!(inputs[0].defined_column_ids(), [id(1), id(2)].into());
        assert_eq!(inputs[1].defined_column_ids(), [id(3), id(4)].into());
        assert_eq!(
            output_ids(&plan),
            vec![Some(id(1)), Some(id(2)), Some(id(3)), Some(id(4))]
        );
        assert!(!kinds(&plan).contains(&ColumnIdIssueKind::DuplicateDefinition));
        Ok(())
    }

    #[test]
    fn pass_through_ids_in_node_schemas() -> Result<()> {
        let t = || scan_with_ids("t", [Some(1), Some(2)]);
        let s = || scan_with_ids("s", [Some(3), Some(4)]);
        let ids = |ids: &[Option<u64>]| -> Vec<Option<ColumnId>> {
            ids.iter().map(|n| n.map(id)).collect()
        };

        // filters, sorts, limits and distincts reuse the schema of their input
        let plan = LogicalPlanBuilder::from(t()?)
            .filter(col("t.a").gt(lit(1)))?
            .sort(vec![col("t.b").sort(true, false)])?
            .limit(0, Some(1))?
            .distinct()?
            .build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(1), Some(2)]));

        // a projection passes bare columns through, other outputs (an alias of
        // a column too) define columns, with new ids from the builder
        let plan = LogicalPlanBuilder::from(t()?)
            .project(vec![col("t.b"), col("t.a").alias("x"), col("t.a") + lit(1)])?
            .build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(2), Some(3), Some(4)]));
        assert_eq!(
            plan.defined_column_ids(),
            [id(1), id(2), id(3), id(4)].into()
        );

        // so does the grouping of an aggregate; its other outputs define
        // columns
        let plan = LogicalPlanBuilder::from(t()?)
            .aggregate(
                vec![col("t.b"), col("t.a") + lit(1)],
                vec![col("t.a").gt(lit(0))],
            )?
            .build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(2), Some(3), Some(4)]));
        assert_eq!(
            plan.defined_column_ids(),
            [id(1), id(2), id(3), id(4)].into()
        );

        // an alias requalifies and keeps the ids
        let plan = LogicalPlanBuilder::from(t()?).alias("u")?.build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(1), Some(2)]));
        // when it has to rename duplicate names, the projection it adds
        // passes the renamed columns through under their new names, with
        // their ids
        let plan = LogicalPlanBuilder::from(t()?)
            .cross_join(s()?)?
            .alias("u")?
            .build()?;
        assert_eq!(
            output_ids(&plan),
            ids(&[Some(1), Some(2), Some(3), Some(4)])
        );
        assert_eq!(kinds(&plan), vec![]);

        // joins concatenate, semi joins keep one side, mark joins add a mark
        // column, defined by the join with a new id from the builder
        for (join_type, expected) in [
            (JoinType::Left, ids(&[Some(1), Some(2), Some(3), Some(4)])),
            (JoinType::LeftSemi, ids(&[Some(1), Some(2)])),
            (JoinType::RightAnti, ids(&[Some(3), Some(4)])),
            (JoinType::LeftMark, ids(&[Some(1), Some(2), Some(5)])),
        ] {
            let plan = LogicalPlanBuilder::from(t()?)
                .join(s()?, join_type, (vec!["t.a"], vec!["s.a"]), None)?
                .build()?;
            assert_eq!(output_ids(&plan), expected, "{join_type}");
            // rebuilt from its expressions, it keeps them
            let plan = plan.with_new_exprs(plan.expressions(), {
                plan.inputs().into_iter().cloned().collect()
            })?;
            assert_eq!(output_ids(&plan), expected, "{join_type}");
            let plan = plan.recompute_schema()?;
            assert_eq!(output_ids(&plan), expected, "{join_type}");
        }

        // a mark join defines its mark column
        let plan = LogicalPlanBuilder::from(t()?)
            .join(s()?, JoinType::LeftMark, (vec!["t.a"], vec!["s.a"]), None)?
            .build()?;
        assert_eq!(plan.defined_column_ids(), [1, 2, 3, 4, 5].map(id).into());
        assert_eq!(kinds(&plan), vec![]);

        // a union defines its outputs, with new ids from the builder, kept
        // when it is rebuilt
        let plan = LogicalPlanBuilder::from(t()?).union(s()?)?.build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(5), Some(6)]));
        assert_eq!(plan.defined_column_ids(), [1, 2, 3, 4, 5, 6].map(id).into());
        let rebuilt =
            plan.with_new_exprs(vec![], plan.inputs().into_iter().cloned().collect())?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        assert_eq!(output_ids(&rebuilt.recompute_schema()?), output_ids(&plan));
        Ok(())
    }

    #[test]
    fn pass_through_ids_in_window_and_unnest() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new_list("l", Field::new_list_field(DataType::Int32, true), true),
        ]);
        let LogicalPlan::TableScan(mut scan) =
            table_scan(Some("t"), &schema, None)?.build()?
        else {
            unreachable!()
        };
        // the ids are set by hand: the scan defines its projected columns
        scan.source_column_ids = None;
        scan.projected_schema = Arc::new(
            Arc::unwrap_or_clone(scan.projected_schema)
                .with_field_ids(vec![Some(id(1)), Some(id(2))])?,
        );
        let scan = LogicalPlan::TableScan(scan);

        // the unnested column is defined by the unnest, with a new id; the
        // others pass through
        let plan = LogicalPlanBuilder::from(scan.clone())
            .unnest_column("t.l")?
            .build()?;
        assert_eq!(output_ids(&plan), vec![Some(id(1)), Some(id(3))]);
        assert_eq!(plan.defined_column_ids(), [1, 2, 3].map(id).into());
        // rebuilt, it keeps its id
        let rebuilt = plan.with_new_exprs(plan.expressions(), {
            plan.inputs().into_iter().cloned().collect()
        })?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        assert_eq!(output_ids(&rebuilt.recompute_schema()?), output_ids(&plan));
        // renumbered, it gets a new one
        let generator = ColumnIdGenerator::new_after(plan.max_column_id());
        let renumbered = plan.with_fresh_column_ids(&generator)?;
        assert_eq!(output_ids(&renumbered), vec![Some(id(5)), Some(id(4))]);
        // (the unnested column is referenced by name)
        assert_eq!(kinds(&renumbered), vec![ColumnIdIssueKind::Missing]);

        // (the window functions are in other crates: a window without any)
        let window = Window::try_new(vec![], Arc::new(scan))?;
        assert_eq!(
            output_ids(&LogicalPlan::Window(window)),
            vec![Some(id(1)), Some(id(2))]
        );
        Ok(())
    }

    #[test]
    fn window_and_distinct_on_outputs_are_definitions() -> Result<()> {
        use crate::expr::WindowFunction;
        use crate::test::function_stub::count_udaf;
        let count = || {
            Expr::WindowFunction(Box::new(WindowFunction::new(
                crate::WindowFunctionDefinition::AggregateUDF(count_udaf()),
                vec![col("t.a")],
            )))
        };
        // the input columns pass through, the window expression defines #3
        let plan = LogicalPlanBuilder::from(scan_with_ids("t", [Some(1), Some(2)])?)
            .window(vec![count()])?
            .build()?;
        assert_eq!(
            output_ids(&plan),
            vec![Some(id(1)), Some(id(2)), Some(id(3))]
        );
        assert_eq!(plan.defined_column_ids(), [id(1), id(2), id(3)].into());
        // rebuilt from its expressions, it keeps them
        let rebuilt = plan.with_new_exprs(
            plan.expressions(),
            plan.inputs().into_iter().cloned().collect(),
        )?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        let rebuilt = rebuilt.recompute_schema()?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        // renumbered, the window output is renamed with its references
        let count_col =
            Column::from(plan.schema().qualified_field(2)).with_id(Some(id(3)));
        let plan = LogicalPlanBuilder::from(plan)
            .filter(Expr::Column(count_col).gt(lit(0)))?
            .build()?;
        assert_eq!(kinds(&plan), vec![]);
        let generator = ColumnIdGenerator::new_after(plan.max_column_id());
        let plan = plan.with_fresh_column_ids(&generator)?;
        assert_eq!(
            output_ids(&plan),
            vec![Some(id(5)), Some(id(6)), Some(id(4))]
        );
        assert_eq!(kinds(&plan), vec![]);

        // a `DistinctOn` defines all its outputs, a bare column too: they are
        // computed per group of the `ON` expressions
        let plan = LogicalPlanBuilder::from(scan_with_ids("t", [Some(1), Some(2)])?)
            .distinct_on(
                vec![col("a")],
                vec![col("b"), col_with_id("t", "a", Some(1)) + lit(1)],
                None,
            )?
            .build()?;
        assert_eq!(output_ids(&plan), vec![Some(id(3)), Some(id(4))]);
        assert_eq!(
            plan.defined_column_ids(),
            [id(1), id(2), id(3), id(4)].into()
        );
        let rebuilt = plan.with_new_exprs(
            plan.expressions(),
            plan.inputs().into_iter().cloned().collect(),
        )?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        let generator = ColumnIdGenerator::new_after(plan.max_column_id());
        let plan = plan.with_fresh_column_ids(&generator)?;
        assert_eq!(output_ids(&plan), vec![Some(id(5)), Some(id(6))]);
        assert_eq!(kinds(&plan), vec![]);
        Ok(())
    }

    #[test]
    fn aggregate_outputs_are_definitions() -> Result<()> {
        use crate::test::function_stub::sum;
        let t = || scan_with_ids("t", [Some(1), Some(2)]);
        // a bare grouping column passes through, the other grouping
        // expressions and the aggregates define columns; equal grouping
        // expressions have one output
        let plan = LogicalPlanBuilder::from(t()?)
            .aggregate(
                vec![col("a"), col("t.b") + lit(1), col("a"), col("t.b") + lit(1)],
                vec![sum(col("t.b"))],
            )?
            .build()?;
        let LogicalPlan::Aggregate(aggregate) = &plan else {
            unreachable!()
        };
        let entry_ids = aggregate
            .group_expr
            .iter()
            .map(|e| e.id)
            .collect::<Vec<_>>();
        assert_eq!(entry_ids, vec![id(1), id(3), id(1), id(3)]);
        assert_eq!(
            output_ids(&plan),
            vec![Some(id(1)), Some(id(3)), Some(id(4))]
        );
        assert_eq!(
            plan.defined_column_ids(),
            [id(1), id(2), id(3), id(4)].into()
        );
        // same-shape rebuilds keep them
        let rebuilt = plan.with_new_exprs(
            plan.expressions(),
            plan.inputs().into_iter().cloned().collect(),
        )?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        let rebuilt = rebuilt.recompute_schema()?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        // renumbered, the outputs are renamed with their references
        let sum_col = Column::from(plan.schema().qualified_field(2)).with_id(Some(id(4)));
        let plan = LogicalPlanBuilder::from(plan)
            .filter(Expr::Column(sum_col).gt(lit(0)))?
            .build()?;
        assert_eq!(kinds(&plan), vec![]);
        let generator = ColumnIdGenerator::new_after(plan.max_column_id());
        let plan = plan.with_fresh_column_ids(&generator)?;
        assert_eq!(
            output_ids(&plan),
            vec![Some(id(7)), Some(id(5)), Some(id(6))]
        );
        assert_eq!(kinds(&plan), vec![]);

        // under grouping sets, a bare column passes through only if it is in
        // every set (`ROLLUP` has the empty set); the grouping id is defined
        // by the aggregate
        let plan = LogicalPlanBuilder::from(t()?)
            .aggregate(
                vec![Expr::GroupingSet(crate::GroupingSet::Rollup(vec![
                    col("a"),
                    col("t.b") + lit(1),
                ]))],
                vec![sum(col("t.b"))],
            )?
            .build()?;
        assert_eq!(
            output_ids(&plan),
            vec![Some(id(3)), Some(id(4)), Some(id(5)), Some(id(6))]
        );
        assert_eq!(
            plan.defined_column_ids(),
            [id(1), id(2), id(3), id(4), id(5), id(6)].into()
        );
        let plan = LogicalPlanBuilder::from(t()?)
            .aggregate(
                vec![Expr::GroupingSet(crate::GroupingSet::GroupingSets(vec![
                    vec![col("a"), col("b")],
                    vec![col("a")],
                ]))],
                vec![sum(col("t.b"))],
            )?
            .build()?;
        assert_eq!(
            output_ids(&plan),
            vec![Some(id(1)), Some(id(3)), Some(id(4)), Some(id(5))]
        );
        // rebuilt, it keeps them; renumbered, they are renamed
        let rebuilt = plan.with_new_exprs(
            plan.expressions(),
            plan.inputs().into_iter().cloned().collect(),
        )?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        let rebuilt = rebuilt.recompute_schema()?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        let generator = ColumnIdGenerator::new_after(plan.max_column_id());
        let renumbered = plan.clone().with_fresh_column_ids(&generator)?;
        assert_eq!(kinds(&renumbered), vec![]);
        assert!(
            renumbered
                .defined_column_ids()
                .is_disjoint(&plan.defined_column_ids())
        );
        Ok(())
    }

    #[test]
    fn recursive_query_outputs_are_definitions() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let static_term = scan_from("t", &generator)?;
        let recursive_term = scan_from("w", &generator)?;
        let plan = LogicalPlanBuilder::from(static_term)
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .to_recursive_query("r".to_string(), recursive_term, false)?
            .build()?;
        assert_eq!(output_ids(&plan), vec![Some(id(5)), Some(id(6))]);
        assert_eq!(plan.defined_column_ids(), [1, 2, 3, 4, 5, 6].map(id).into());
        let rebuilt =
            plan.with_new_exprs(vec![], plan.inputs().into_iter().cloned().collect())?;
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        assert_eq!(output_ids(&rebuilt.recompute_schema()?), output_ids(&plan));

        // the query owns the names of its outputs: a static term with other
        // names does not rename them
        let names = plan.schema().field_names();
        let inputs = plan.inputs();
        let renamed_static_term = LogicalPlanBuilder::from(inputs[0].clone())
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .project(vec![col("a").alias("x"), col("b").alias("y")])?
            .build()?;
        let rebuilt = plan.with_new_exprs(
            vec![],
            vec![renamed_static_term.clone(), inputs[1].clone()],
        )?;
        assert_eq!(rebuilt.schema().field_names(), names);
        assert_eq!(output_ids(&rebuilt), output_ids(&plan));
        let LogicalPlan::RecursiveQuery(mut query) = plan.clone() else {
            unreachable!()
        };
        query.static_term = Arc::new(renamed_static_term);
        let recomputed = LogicalPlan::RecursiveQuery(query).recompute_schema()?;
        assert_eq!(recomputed.schema().field_names(), names);
        assert_eq!(output_ids(&recomputed), output_ids(&plan));

        // a recursive term that repeats the static term is renumbered
        let static_term = scan_from("t", &generator)?;
        let plan = LogicalPlanBuilder::from(static_term.clone())
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .to_recursive_query("r".to_string(), static_term, false)?
            .build()?;
        assert_eq!(kinds(&plan), vec![]);
        Ok(())
    }

    #[test]
    fn output_column_ids_are_taken_over() -> Result<()> {
        let generator = ColumnIdGenerator::new();
        let scan = scan_from("t", &generator)?;
        let plan = filter(scan, col_with_id("t", "a", Some(1)))?;
        // the filter outputs the scan's #1 and #2: they become #7 and #8,
        // and so does the reference of the filter
        let renamed = plan.with_output_column_ids(&[Some(id(7)), Some(id(8))])?;
        assert!(renamed.transformed);
        let plan = renamed.data;
        assert_eq!(output_ids(&plan), vec![Some(id(7)), Some(id(8))]);
        assert_eq!(ref_ids(&plan), vec![Some(id(7))]);
        assert_eq!(kinds(&plan), vec![]);

        // an id at two positions takes the first new id only
        let plan = LogicalPlanBuilder::from(scan_from("s", &generator)?)
            .project(vec![col("s.a"), col("s.a").alias("a2"), col("s.b")])?
            .build()?;
        let ids_before = output_ids(&plan);
        let plan = plan
            .with_output_column_ids(&[Some(id(20)), Some(id(21)), Some(id(22))])?
            .data;
        assert_eq!(
            output_ids(&plan),
            vec![Some(id(20)), Some(id(21)), Some(id(22))]
        );
        assert_ne!(ids_before, output_ids(&plan));
        assert_eq!(kinds(&plan), vec![]);
        Ok(())
    }

    #[test]
    fn self_union_through_builder_gets_distinct_ids() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let scan = scan_from("t", &generator)?;
        let plan = LogicalPlanBuilder::from(scan.clone())
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .union(scan)?
            .build()?;
        let LogicalPlan::Union(union) = &plan else {
            unreachable!()
        };
        assert_eq!(output_ids(&union.inputs[0]), vec![Some(id(1)), Some(id(2))]);
        assert_eq!(output_ids(&union.inputs[1]), vec![Some(id(3)), Some(id(4))]);
        assert!(!kinds(&plan).contains(&ColumnIdIssueKind::DuplicateDefinition));
        Ok(())
    }

    #[test]
    fn disjoint_join_is_not_renumbered() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let left = scan_from("l", &generator)?;
        let right = scan_from("r", &generator)?;
        let plan = LogicalPlanBuilder::from(left)
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .cross_join(right)?
            .build()?;
        let inputs = plan.inputs();
        assert_eq!(inputs[0].defined_column_ids(), [id(1), id(2)].into());
        assert_eq!(inputs[1].defined_column_ids(), [id(3), id(4)].into());
        // nothing was allocated
        assert_eq!(generator.next(), id(5));
        Ok(())
    }

    #[test]
    fn renumbering_keeps_references_consistent() -> Result<()> {
        let generator = ColumnIdGenerator::new();
        let scan = scan_from("t", &generator)?;
        let plan = filter(scan, col_with_id("t", "a", Some(1)))?;
        let plan = LogicalPlanBuilder::from(plan)
            .sort(vec![col_with_id("t", "b", Some(2)).sort(true, false)])?
            .build()?;
        assert_eq!(kinds(&plan), vec![]);

        let plan = plan.with_fresh_column_ids(&generator)?;
        assert_eq!(output_ids(&plan), vec![Some(id(3)), Some(id(4))]);
        assert_eq!(plan.defined_column_ids(), [id(3), id(4)].into());
        // the references were renamed with the definitions
        assert_eq!(kinds(&plan), vec![]);
        let mut refs = vec![];
        plan.apply_with_subqueries(|node| {
            apply_node_column_refs(node, |col, _| refs.push(col.id))?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(refs, vec![Some(id(4)), Some(id(3))]);
        Ok(())
    }

    #[test]
    fn merge_into_defines_its_target_columns() -> Result<()> {
        use crate::dml::{
            MergeIntoAction, MergeIntoClause, MergeIntoClauseKind, MergeIntoOp,
        };
        use crate::{DmlStatement, WriteOp};

        fn target_ids(plan: &LogicalPlan) -> Option<Vec<ColumnId>> {
            match plan {
                LogicalPlan::Dml(DmlStatement {
                    op: WriteOp::MergeInto(op),
                    ..
                }) => op.target_column_ids().map(|ids| ids.to_vec()),
                _ => None,
            }
        }

        let generator = ColumnIdGenerator::new();
        let target = source();
        // the source of the MERGE (its input), ids 1 and 2
        let input = scan_from("s", &generator)?;
        // the target columns, ids 3 and 4
        let ids = MergeIntoOp::new_target_column_ids(&target.schema(), &generator);
        let merge = |on: Expr, ids: Option<Arc<[ColumnId]>>| {
            let op = MergeIntoOp::new(
                "t",
                on,
                vec![MergeIntoClause {
                    kind: MergeIntoClauseKind::Matched,
                    predicate: None,
                    action: MergeIntoAction::Update(vec![(
                        "b".to_string(),
                        col_with_id("s", "b", Some(2)),
                    )]),
                }],
            )
            .with_target_column_ids(ids);
            LogicalPlan::Dml(DmlStatement::new(
                "t".into(),
                Arc::clone(&target),
                WriteOp::MergeInto(Box::new(op)),
                Arc::new(input.clone()),
            ))
        };
        let on = col_with_id("t", "a", Some(3)).eq(col_with_id("s", "a", Some(1)));

        // the references to the target read the ids the operation defines
        let plan = merge(on.clone(), Some(Arc::clone(&ids)));
        assert_eq!(kinds(&plan), vec![]);
        assert_eq!(
            plan.defined_column_ids(),
            [id(1), id(2), id(3), id(4)].into()
        );
        assert_eq!(plan.max_column_id(), Some(id(4)));
        // equality ignores them
        assert_eq!(plan, merge(on.clone(), None));
        // without them, a reference to the target has nothing to read
        assert_eq!(
            kinds(&merge(on.clone(), None)),
            vec![ColumnIdIssueKind::Stale]
        );
        // an id defined by the target and by the source is defined twice
        assert_eq!(
            kinds(&merge(on, Some([id(3), id(1)].into()))),
            vec![ColumnIdIssueKind::DuplicateDefinition]
        );

        // a rebuild keeps them
        let rebuilt = plan.with_new_exprs(plan.expressions(), vec![input.clone()])?;
        assert_eq!(target_ids(&rebuilt), Some(vec![id(3), id(4)]));
        assert_eq!(kinds(&rebuilt), vec![]);

        // renumbering renames them, and the references to them
        let plan = plan.with_fresh_column_ids(&generator)?;
        assert_eq!(kinds(&plan), vec![]);
        let new_ids = target_ids(&plan).unwrap();
        assert!(new_ids.iter().all(|id| id.get().get() > 4), "{new_ids:?}");
        assert_eq!(plan.defined_column_ids().len(), 4);
        assert!(ref_ids(&plan).contains(&Some(new_ids[0])));
        Ok(())
    }

    #[test]
    fn renumbering_renames_projection_outputs() -> Result<()> {
        let generator = ColumnIdGenerator::new();
        let scan = scan_from("t", &generator)?;
        // `t.a` passes #1 through, `t.b + 1 AS c` defines #3
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![
                NamedExpr::new(col_with_id("t", "a", Some(1)), id(1)),
                NamedExpr::new(
                    (col_with_id("t", "b", Some(2)) + lit(1)).alias("c"),
                    generator.next(),
                ),
            ],
            Arc::new(scan),
        )?);
        assert_eq!(plan.defined_column_ids(), [id(1), id(2), id(3)].into());
        assert_eq!(kinds(&plan), vec![]);

        // the projection's definition is found first
        let plan = plan.with_fresh_column_ids(&generator)?;
        assert_eq!(output_ids(&plan), vec![Some(id(5)), Some(id(4))]);
        let LogicalPlan::Projection(projection) = &plan else {
            unreachable!()
        };
        assert!(projection.expr[0].is_pass_through());
        assert_eq!(projection.expr[1].defined_id(), Some(id(4)));
        assert_eq!(kinds(&plan), vec![]);
        Ok(())
    }

    #[test]
    fn projection_outputs_are_definitions() -> Result<()> {
        let generator = ColumnIdGenerator::new();
        let scan = scan_from("t", &generator)?;
        // a computed output that reuses the id of a scan column defines it
        // twice
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![NamedExpr::new(
                (col_with_id("t", "a", Some(1)) + lit(1)).alias("a"),
                id(1),
            )],
            Arc::new(scan),
        )?);
        assert_eq!(kinds(&plan), vec![ColumnIdIssueKind::DuplicateDefinition]);
        Ok(())
    }

    #[test]
    fn renumbering_leaves_outer_references_alone() -> Result<()> {
        let generator = ColumnIdGenerator::new();
        // t.a is #1 in the enclosing query; the subquery defines #3 and #4
        let _outer = scan_from("t", &generator)?;
        let inner = scan_from("s", &generator)?;
        let outer_ref = Expr::OuterReferenceColumn(
            Arc::new(Field::new("a", DataType::Int32, false)),
            Column::new(Some("t"), "a").with_id(Some(id(1))),
        );
        let subquery = filter(inner, outer_ref.eq(col_with_id("s", "a", Some(3))))?;

        let subquery = subquery.with_fresh_column_ids(&generator)?;
        assert_eq!(output_ids(&subquery), vec![Some(id(5)), Some(id(6))]);
        let mut refs = vec![];
        apply_node_column_refs(&subquery, |col, outer| refs.push((col.id, outer)))?;
        assert_eq!(refs, vec![(Some(id(1)), true), (Some(id(5)), false)]);
        Ok(())
    }

    #[test]
    fn subquery_built_from_the_outer_plan_is_renumbered() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let scan = scan_from("t", &generator)?;
        let plan = LogicalPlanBuilder::from(scan.clone())
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .filter(exists(Arc::new(scan)))?
            .build()?;
        assert!(!kinds(&plan).contains(&ColumnIdIssueKind::DuplicateDefinition));
        assert_eq!(plan.defined_column_ids().len(), 4);
        Ok(())
    }

    /// A view: a table source that provides a logical plan
    struct View(LogicalPlan);

    impl crate::TableSource for View {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(self.0.schema().inner())
        }

        fn get_logical_plan(&self) -> Option<std::borrow::Cow<'_, LogicalPlan>> {
            Some(std::borrow::Cow::Borrowed(&self.0))
        }
    }

    #[test]
    fn view_is_inlined_with_fresh_ids() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let view: Arc<dyn crate::TableSource> =
            Arc::new(View(scan_from("t", &generator)?));
        let scan = |name: &str| {
            LogicalPlanBuilder::scan_with_column_id_generator(
                name,
                Arc::clone(&view),
                None,
                Some(Arc::clone(&generator)),
            )
        };
        let left = scan("v1")?;
        let right = scan("v2")?.build()?;
        let plan = left.cross_join(right)?.build()?;
        let inputs = plan.inputs();
        assert_eq!(inputs[0].defined_column_ids(), [id(3), id(4)].into());
        assert_eq!(inputs[1].defined_column_ids(), [id(5), id(6)].into());
        assert!(!kinds(&plan).contains(&ColumnIdIssueKind::DuplicateDefinition));

        // without a generator, the builder renumbers the inlined plan with
        // its own
        let plan = LogicalPlanBuilder::scan("v", Arc::clone(&view), None)?.build()?;
        assert_eq!(plan.defined_column_ids(), [id(3), id(4)].into());
        Ok(())
    }

    /// A table source with Int32 columns `a` and `b`
    fn source() -> Arc<dyn crate::TableSource> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        Arc::new(crate::logical_plan::builder::LogicalTableSource::new(
            Arc::new(schema),
        ))
    }

    /// The id of each column reference read by the expressions of `plan`
    /// (not of its inputs)
    fn ref_ids(plan: &LogicalPlan) -> Vec<Option<ColumnId>> {
        let mut refs = vec![];
        apply_node_column_refs(plan, |col, _| refs.push(col.id)).unwrap();
        refs
    }

    #[test]
    fn scans_built_with_a_generator_allocate() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let scan = LogicalPlanBuilder::scan_with_column_id_generator(
            "t",
            source(),
            None,
            Some(Arc::clone(&generator)),
        )?
        .build()?;
        assert_eq!(output_ids(&scan), vec![Some(id(1)), Some(id(2))]);

        // without a generator, the builder allocates from its own
        let scan_without = LogicalPlanBuilder::scan("t", source(), None)?.build()?;
        assert_eq!(output_ids(&scan_without), vec![Some(id(1)), Some(id(2))]);

        // a scan rebuilt with another projection keeps the ids of the columns
        // it retains
        let LogicalPlan::TableScan(table_scan) = scan else {
            unreachable!()
        };
        let pruned = crate::TableScanBuilder::from(table_scan)
            .with_projection(Some(vec![1]))
            .build()?;
        assert_eq!(pruned.projected_schema.field_ids(), &[Some(id(2))]);
        let rebuilt = crate::TableScanBuilder::from(pruned)
            .with_fetch(Some(1))
            .build()?;
        assert_eq!(rebuilt.projected_schema.field_ids(), &[Some(id(2))]);
        Ok(())
    }

    #[test]
    fn scans_define_the_columns_they_do_not_project() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        // a scan created with a projection allocates ids for its whole
        // source, and binds its filters to them
        let table_scan = crate::TableScanBuilder::new("t", source())
            .with_projection(Some(vec![0]))
            .with_filters(vec![col("t.b").eq(lit(1))])
            .build()?;
        let scan = LogicalPlanBuilder::table_scan_with_column_id_generator(
            table_scan,
            Some(Arc::clone(&generator)),
        )?
        .build()?;
        assert_eq!(output_ids(&scan), vec![Some(id(1))]);
        assert_eq!(scan.defined_column_ids(), [1, 2].map(id).into());
        assert_eq!(scan.max_column_id(), Some(id(2)));
        assert_eq!(ref_ids(&scan), vec![Some(id(2))]);
        assert!(scan.column_id_issues().is_empty());

        // pruned, then widened again, it keeps the ids of its source
        let LogicalPlan::TableScan(table_scan) = scan else {
            unreachable!()
        };
        let pruned = crate::TableScanBuilder::from(table_scan)
            .with_projection(Some(vec![]))
            .build()?;
        let pruned = LogicalPlan::TableScan(pruned);
        assert!(pruned.column_id_issues().is_empty());
        let LogicalPlan::TableScan(pruned_scan) = pruned.clone() else {
            unreachable!()
        };
        let widened = crate::TableScanBuilder::from(pruned_scan)
            .with_projection(Some(vec![1, 0]))
            .build()?;
        assert_eq!(
            widened.projected_schema.field_ids(),
            &[Some(id(2)), Some(id(1))]
        );

        // renumbered, the filters follow
        let renumbered = pruned.with_fresh_column_ids(&generator)?;
        assert_eq!(renumbered.defined_column_ids(), [3, 4].map(id).into());
        assert_eq!(ref_ids(&renumbered), vec![Some(id(4))]);
        assert!(renumbered.column_id_issues().is_empty());
        Ok(())
    }

    #[test]
    fn values_built_with_a_generator_allocate() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let values = LogicalPlanBuilder::values_with_column_id_generator(
            vec![vec![lit(1), lit("x")]],
            None,
            Some(Arc::clone(&generator)),
        )?
        .build()?;
        assert_eq!(output_ids(&values), vec![Some(id(1)), Some(id(2))]);
        assert_eq!(values.defined_column_ids(), [id(1), id(2)].into());
        Ok(())
    }

    #[test]
    fn normalization_binds_references() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let scan = LogicalPlanBuilder::scan_with_column_id_generator(
            "t",
            source(),
            None,
            Some(Arc::clone(&generator)),
        )?;
        // unqualified, qualified, and through a projection
        let plan = scan
            .filter(col("a").eq(col("t.b")))?
            .project(vec![col("b"), col("a").alias("x")])?
            .sort(vec![col("t.b").sort(true, false)])?
            .build()?;
        assert_eq!(kinds(&plan), vec![]);
        assert_eq!(ref_ids(&plan), vec![Some(id(2))]);
        let projection = plan.inputs()[0];
        assert_eq!(ref_ids(projection), vec![Some(id(2)), Some(id(1))]);
        assert_eq!(
            ref_ids(projection.inputs()[0]),
            vec![Some(id(1)), Some(id(2))]
        );

        // a qualified name that does not resolve is left as is
        let column = Column::new(Some("u"), "a")
            .normalize_with_schemas_and_ambiguity_check(&[&[plan.schema()]], &[])?;
        assert_eq!(column.id, None);
        Ok(())
    }

    #[test]
    fn self_join_strips_ambiguous_references() -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let scan = LogicalPlanBuilder::scan_with_column_id_generator(
            "t",
            source(),
            None,
            Some(Arc::clone(&generator)),
        )?
        .build()?;
        // references bound to the scan before it is joined with itself
        let a = Expr::Column(Column::new(Some("l"), "a").with_id(Some(id(1))));
        let b = Expr::Column(Column::new(Some("r"), "b").with_id(Some(id(2))));
        let right = LogicalPlanBuilder::from(scan.clone()).alias("r")?.build()?;
        let plan = LogicalPlanBuilder::from(scan)
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .alias("l")?
            .join_on(right, JoinType::Inner, vec![a.eq(b)])?
            .build()?;
        // the right side was renumbered: `r.b#2` would find `l.b#2`, so it
        // lost its id, and so did `l.a#1`; they were then bound again by
        // qualified name
        assert_eq!(output_ids(plan.inputs()[1]), vec![Some(id(3)), Some(id(4))]);
        assert_eq!(ref_ids(&plan), vec![Some(id(1)), Some(id(4))]);
        assert_eq!(kinds(&plan), vec![]);

        // keys too
        let scan = LogicalPlanBuilder::scan_with_column_id_generator(
            "t",
            source(),
            None,
            Some(Arc::clone(&generator)),
        )?
        .build()?;
        let right = LogicalPlanBuilder::from(scan.clone()).alias("r")?.build()?;
        let plan = LogicalPlanBuilder::from(scan)
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .alias("l")?
            .join(
                right,
                JoinType::Inner,
                (
                    vec![Column::new(Some("l"), "a").with_id(Some(id(5)))],
                    vec![Column::new(Some("r"), "a").with_id(Some(id(5)))],
                ),
                None,
            )?
            .build()?;
        let LogicalPlan::Join(join) = &plan else {
            unreachable!()
        };
        // stripped, then bound again to their sides by name
        let (l, r) = &join.on[0];
        assert_eq!(l.get_as_join_column().unwrap().id, Some(id(5)));
        assert_eq!(r.get_as_join_column().unwrap().id, Some(id(7)));
        assert_eq!(kinds(&plan), vec![]);
        Ok(())
    }

    #[test]
    fn subquery_built_from_the_outer_plan_binds_ambiguous_outer_references_by_name()
    -> Result<()> {
        let generator = Arc::new(ColumnIdGenerator::new());
        let scan = LogicalPlanBuilder::scan_with_column_id_generator(
            "t",
            source(),
            None,
            Some(Arc::clone(&generator)),
        )?
        .build()?;
        // `t.a#1` is defined both by the subquery and by the outer plan
        let outer_ref = Expr::OuterReferenceColumn(
            Arc::new(Field::new("a", DataType::Int32, false)),
            Column::new(Some("t"), "a").with_id(Some(id(1))),
        );
        let subquery = LogicalPlanBuilder::from(scan.clone())
            .alias("s")?
            .filter(col("s.b").eq(outer_ref))?
            .build()?;
        let plan = LogicalPlanBuilder::from(scan)
            .with_column_id_generator(Some(Arc::clone(&generator)))
            .filter(exists(Arc::new(subquery)))?
            .build()?;
        assert!(!kinds(&plan).contains(&ColumnIdIssueKind::DuplicateDefinition));
        let mut refs = vec![];
        plan.apply_with_subqueries(|node| {
            apply_node_column_refs(node, |col, outer| {
                if outer {
                    refs.push(col.id)
                }
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        // the subquery was renumbered, and the outer reference, which could
        // not tell the outer column from the subquery's, was bound again by
        // name to the outer column
        let outer_a = plan.inputs()[0].schema().field_id(0);
        assert_eq!(outer_a, Some(id(1)));
        assert_eq!(refs, vec![outer_a]);
        Ok(())
    }

    #[test]
    fn repeated_subqueries_are_renumbered() -> Result<()> {
        let generator = ColumnIdGenerator::new();
        let outer = scan_from("t", &generator)?;
        let subquery = Arc::new(filter(
            scan_from("s", &generator)?,
            col_with_id("s", "a", Some(3)),
        )?);
        // one subquery in two places, as a rewrite may leave it
        let predicate = exists(Arc::clone(&subquery)).and(exists(subquery));
        let plan = LogicalPlan::Filter(Filter::try_new(
            predicate.clone(),
            Arc::new(outer.clone()),
        )?);
        assert!(kinds(&plan).contains(&ColumnIdIssueKind::DuplicateDefinition));

        let mut seen = HashSet::new();
        let renumbered = predicate
            .with_fresh_column_ids_in_repeated_subqueries(&mut seen, &generator)?;
        assert!(renumbered.transformed);
        let plan = LogicalPlan::Filter(Filter::try_new(
            renumbered.data,
            Arc::new(outer.clone()),
        )?);
        assert_eq!(kinds(&plan), vec![]);
        assert_eq!(
            plan.defined_column_ids(),
            [id(1), id(2), id(3), id(4), id(5), id(6)].into()
        );

        // a copy renumbered as a whole
        let LogicalPlan::Filter(filter) = plan else {
            unreachable!()
        };
        let copy = filter
            .predicate
            .clone()
            .with_fresh_subquery_column_ids(&generator)?;
        assert!(copy.transformed);
        let both = LogicalPlan::Filter(Filter::try_new(
            filter.predicate.and(copy.data),
            Arc::new(outer),
        )?);
        assert_eq!(kinds(&both), vec![]);
        Ok(())
    }

    /// An extension node over `input` that outputs `input`'s columns and a
    /// column `x` (without an id) and reads `expr`
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct ExtensionNode {
        input: LogicalPlan,
        expr: Expr,
        schema: DFSchemaRef,
        supports_column_ids: bool,
    }

    impl PartialOrd for ExtensionNode {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            (&self.input, &self.expr).partial_cmp(&(&other.input, &other.expr))
        }
    }

    impl crate::UserDefinedLogicalNodeCore for ExtensionNode {
        fn name(&self) -> &str {
            "ExtensionNode"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.input]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![self.expr.clone()]
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "ExtensionNode: {}", self.expr)
        }

        fn with_exprs_and_inputs(
            &self,
            mut exprs: Vec<Expr>,
            mut inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            Ok(Self {
                input: inputs.swap_remove(0),
                expr: exprs.swap_remove(0),
                schema: Arc::clone(&self.schema),
                supports_column_ids: self.supports_column_ids,
            })
        }

        fn supports_column_ids(&self) -> bool {
            self.supports_column_ids
        }
    }

    fn extension(
        input: LogicalPlan,
        expr: Expr,
        supports_column_ids: bool,
    ) -> Result<LogicalPlan> {
        let x = DFSchema::from_unqualified_fields(
            vec![Field::new("x", DataType::Int32, false)].into(),
            std::collections::HashMap::new(),
        )?;
        let schema = Arc::new(input.schema().join(&x)?);
        Ok(LogicalPlan::Extension(crate::Extension {
            node: Arc::new(ExtensionNode {
                input,
                expr,
                schema,
                supports_column_ids,
            }),
        }))
    }

    #[test]
    fn extension_without_column_ids_is_a_boundary() -> Result<()> {
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        // its expressions read its input by name, and the references above
        // it read its column without an id by name, also through a node that
        // passes it through
        let plan = extension(scan, col("t.a"), false)?;
        let plan = filter(plan, col("x"))?;
        let plan = LogicalPlanBuilder::from(plan).alias("s")?.build()?;
        let plan = filter(plan, col("s.x"))?;
        assert_eq!(kinds(&plan), vec![]);
        // the columns it passes through keep their ids and are checked
        let plan = filter(plan, col("s.a"))?;
        assert_eq!(kinds(&plan), vec![ColumnIdIssueKind::Missing]);
        Ok(())
    }

    #[test]
    fn extension_with_column_ids_is_checked() -> Result<()> {
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = extension(scan, col("t.a"), true)?;
        let plan = filter(plan, col("x"))?;
        assert_eq!(
            kinds(&plan),
            vec![ColumnIdIssueKind::Missing, ColumnIdIssueKind::Missing]
        );
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = extension(scan, col_with_id("t", "a", Some(1)), true)?;
        assert_eq!(kinds(&plan), vec![]);
        Ok(())
    }
}
