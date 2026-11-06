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
//! Ids are allocated by a [`ColumnIdGenerator`] shared by a session. No
//! generator, no ids: plans built without one have no ids, and resolve by
//! qualifier and name.
//!
//! # Definitions and references
//!
//! A column is *defined* once in a plan, subqueries included: by the leaf
//! that produces it (`TableScan`, `Values`, `MaterializedCteScan`, the work
//! table scan of a recursive query, which is a `TableScan`), and later by the
//! nodes that compute outputs. Other nodes pass their inputs' ids through: a
//! schema can have the same id twice (`SELECT a, a`), and the fields hold the
//! same values. A plan value that is used twice (a view referenced twice, a
//! self-join of a DataFrame) is renumbered where it is blended in.
//!
//! The leaves allocate ids when they are built with a generator
//! (`LogicalPlanBuilder::scan_with_column_id_generator`,
//! `LogicalPlanBuilder::values_with_column_id_generator`, the SQL planner).
//! A `TableScan` rebuilt with another projection (`TableScanBuilder::from`)
//! keeps the ids of the source columns it retains.
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
//! of `datafusion-optimizer`).
//!
//! [`DFSchema::field_ids`]: datafusion_common::DFSchema::field_ids
//! [`ColumnIdGenerator`]: datafusion_common::ColumnIdGenerator

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::logical_plan::{MaterializedCtePlan, Projection, Subquery};
use crate::{Expr, LogicalPlan};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion,
};
use datafusion_common::{
    Column, ColumnId, ColumnIdGenerator, DFSchema, DFSchemaRef, Result,
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

/// The ids of the outputs of a node that computes one output per expression
/// of `exprs` over `input` (a projection, the grouping of an aggregate): the
/// id of the input field that an expression passes through if it is a bare
/// column, `None` for the outputs it computes (they get ids with `NamedExpr`).
pub(crate) fn pass_through_column_ids<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    input: &DFSchema,
) -> Vec<Option<ColumnId>> {
    let has_ids = input.field_ids().iter().any(Option::is_some);
    exprs
        .into_iter()
        .map(|expr| match expr {
            Expr::Column(col) if has_ids => input
                .maybe_index_of_column(col)
                .and_then(|i| input.field_id(i)),
            _ => None,
        })
        .collect()
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
    let ids = schema
        .fields()
        .iter()
        .map(|_| Some(generator.next()))
        .collect();
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
    /// A reference whose id is not in its input, and whose name resolves to a
    /// field without an id, or to nothing: it resolves by name as without
    /// an id
    Stale,
    /// A reference whose id is not in its input, and whose name resolves to a
    /// field with a *different* id: it resolves by name to a column that is
    /// not the one it meant
    Misresolved,
    /// A reference whose id is found in its input, but whose name resolves to
    /// another field (a name that resolves to nothing is fine)
    Disagree,
    /// An id defined twice in the plan, subqueries included
    DuplicateDefinition,
}

impl ColumnIdIssueKind {
    /// All the kinds, in order
    pub const ALL: [ColumnIdIssueKind; 5] = [
        Self::Missing,
        Self::Stale,
        Self::Misresolved,
        Self::Disagree,
        Self::DuplicateDefinition,
    ];

    /// The name of this kind, in snake case
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Stale => "stale",
            Self::Misresolved => "misresolved",
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
    /// (`TableScan`, `Values`, `MaterializedCteScan`).
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

    /// The ids of the columns this node defines itself (not those it passes
    /// through from its inputs)
    fn node_defined_column_ids(&self) -> &[Option<ColumnId>] {
        match self {
            LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(_)) => {
                self.schema().field_ids()
            }
            _ => &[],
        }
    }
}

impl LogicalPlan {
    /// The ids of the columns this plan defines, including in its subqueries
    /// (see the [module documentation](self)). The ids it only passes through
    /// or refers to from an outer query are not included.
    pub fn defined_column_ids(&self) -> HashSet<ColumnId> {
        self.defined_column_ids_in_order().into_iter().collect()
    }

    /// [`Self::defined_column_ids`], in the order in which they are found,
    /// without duplicates, so that renumbering is deterministic
    fn defined_column_ids_in_order(&self) -> Vec<ColumnId> {
        let mut seen = HashSet::new();
        let mut ids = vec![];
        // the closure never fails
        let _ = self.apply_with_subqueries(|plan| {
            for id in plan.node_defined_column_ids().iter().flatten() {
                if seen.insert(*id) {
                    ids.push(*id);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        ids
    }

    /// Returns this plan where every column it defines has a new id from
    /// `generator`, and the references to them inside it (in schemas and
    /// expressions, subqueries included) are renamed to match. References to
    /// ids that the plan does not define, such as outer references to an
    /// enclosing query, are left alone.
    ///
    /// Without a generator, it strips the ids instead: the columns it defines
    /// and the references to them get no id (no generator, no ids). This can
    /// never collide.
    ///
    /// Used where a plan value is blended into another plan that may already
    /// contain it (see the [module documentation](self)), so that no id is
    /// defined twice.
    pub fn with_fresh_column_ids(
        self,
        generator: Option<&ColumnIdGenerator>,
    ) -> Result<Self> {
        self.renumber(generator, &HashSet::new())
    }

    /// [`Self::with_fresh_column_ids`], where the outer references to the
    /// ids in `strip_outer` lose their id instead of being renamed
    fn renumber(
        self,
        generator: Option<&ColumnIdGenerator>,
        strip_outer: &HashSet<ColumnId>,
    ) -> Result<Self> {
        let defined = self.defined_column_ids_in_order();
        if defined.is_empty() {
            return Ok(self);
        }
        let renames = Renames {
            ids: defined
                .into_iter()
                .map(|id| (id, generator.map(ColumnIdGenerator::next)))
                .collect(),
            strip_outer,
        };
        self.transform_up_with_subqueries(|plan| rename_node_column_ids(plan, &renames))
            .map(|transformed| transformed.data)
    }

    /// Returns this plan, renumbered with [`Self::with_fresh_column_ids`] if
    /// it defines an id that `existing` also defines: used where this plan is
    /// blended into a plan that contains `existing` (the right side of a
    /// join, a subquery of `existing`)
    pub fn with_fresh_column_ids_if_overlapping(
        self,
        existing: &LogicalPlan,
        generator: Option<&ColumnIdGenerator>,
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
        generator: Option<&ColumnIdGenerator>,
    ) -> Result<(Self, HashSet<ColumnId>)> {
        // the incoming plan is usually the smaller one: look at it first
        let defined = self.defined_column_ids();
        if defined.is_empty() {
            return Ok((self, HashSet::new()));
        }
        let mut overlap = HashSet::new();
        // the closure never fails
        let _ = existing.apply_with_subqueries(|plan| {
            overlap.extend(
                plan.node_defined_column_ids()
                    .iter()
                    .flatten()
                    .filter(|id| defined.contains(*id)),
            );
            Ok(TreeNodeRecursion::Continue)
        });
        if overlap.is_empty() {
            Ok((self, overlap))
        } else {
            Ok((self.with_fresh_column_ids(generator)?, overlap))
        }
    }

    /// Returns this plan where every subquery expression that defines an id
    /// already defined before it in the plan (a node before its inputs, and
    /// before the subqueries of its inputs) is renumbered with
    /// [`Self::with_fresh_column_ids`], as after a rule that put one subquery
    /// in two places. Its outer references to the ids it shared lose them
    /// (they would be ambiguous).
    ///
    /// Renumbering a subquery expression is safe: the plan around it does
    /// not refer to the columns defined inside it.
    pub fn with_fresh_column_ids_in_repeated_subqueries(
        self,
        generator: Option<&ColumnIdGenerator>,
    ) -> Result<Transformed<Self>> {
        renumber_repeated_subqueries(self, &mut HashSet::new(), generator)
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

/// See [`LogicalPlan::with_fresh_column_ids_in_repeated_subqueries`]. `seen`
/// holds the ids defined so far.
fn renumber_repeated_subqueries(
    plan: LogicalPlan,
    seen: &mut HashSet<ColumnId>,
    generator: Option<&ColumnIdGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    plan.transform_down(|node| {
        seen.extend(node.node_defined_column_ids().iter().flatten().copied());
        node.map_subqueries(|subquery| {
            let LogicalPlan::Subquery(mut subquery) = subquery else {
                return Ok(Transformed::no(subquery));
            };
            let mut plan = Arc::unwrap_or_clone(subquery.subquery);
            let overlap: HashSet<ColumnId> = plan
                .defined_column_ids()
                .into_iter()
                .filter(|id| seen.contains(id))
                .collect();
            let renumbered = !overlap.is_empty();
            if renumbered {
                plan = plan.renumber(generator, &overlap)?;
                for expr in &mut subquery.outer_ref_columns {
                    *expr = strip_column_ids(std::mem::take(expr), &overlap)?;
                }
            }
            // the subqueries inside it may repeat too
            let inner = renumber_repeated_subqueries(plan, seen, generator)?;
            let transformed = renumbered || inner.transformed;
            subquery.subquery = Arc::new(inner.data);
            Ok(Transformed::new_transformed(
                LogicalPlan::Subquery(subquery),
                transformed,
            ))
        })
    })
}

/// Renumbers the subqueries of expressions about to be added to the plan
/// `outer`, when they define ids that `outer` or a subquery seen before
/// also define, as when a subquery is built from the plan it filters
pub(crate) struct SubqueryBlender<'a> {
    outer: &'a LogicalPlan,
    generator: Option<&'a ColumnIdGenerator>,
    /// The ids defined by `outer` and the subqueries seen so far, computed
    /// when the first subquery that defines ids is seen
    taken: Option<HashSet<ColumnId>>,
}

impl<'a> SubqueryBlender<'a> {
    pub(crate) fn new(
        outer: &'a LogicalPlan,
        generator: Option<&'a ColumnIdGenerator>,
    ) -> Self {
        Self {
            outer,
            generator,
            taken: None,
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
            let defined = subquery.subquery.defined_column_ids();
            if defined.is_empty() {
                return Ok(Transformed::no(expr));
            }
            let taken = self
                .taken
                .get_or_insert_with(|| self.outer.defined_column_ids());
            let overlap: HashSet<ColumnId> =
                defined.intersection(taken).copied().collect();
            let transformed = !overlap.is_empty();
            if transformed {
                // the outer references to the overlapping ids cannot tell the
                // outer column from the inner one: they lose their ids, and
                // resolve by name
                let plan = Arc::unwrap_or_clone(Arc::clone(&subquery.subquery));
                subquery.subquery = Arc::new(plan.renumber(self.generator, &overlap)?);
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
struct Renames<'a> {
    /// The new id of each id that the plan defines (`None` to strip it)
    ids: HashMap<ColumnId, Option<ColumnId>>,
    /// The ids whose outer references lose their id instead
    strip_outer: &'a HashSet<ColumnId>,
}

impl Renames<'_> {
    /// Renames the id of `col` (an outer reference if `is_outer`), returning
    /// true if it changed
    fn rename(&self, col: &mut Column, is_outer: bool) -> bool {
        let Some(id) = col.id else {
            return false;
        };
        let new_id = if is_outer && self.strip_outer.contains(&id) {
            None
        } else {
            match self.ids.get(&id) {
                Some(new_id) => *new_id,
                None => return false,
            }
        };
        col.id = new_id;
        true
    }

    /// Renames the column references in `expr`, including the outer
    /// references listed by its subqueries
    fn rename_expr(&self, expr: Expr) -> Result<Transformed<Expr>> {
        expr.transform(|mut expr| {
            let transformed = match &mut expr {
                Expr::Column(col) => self.rename(col, false),
                Expr::OuterReferenceColumn(_, col) => self.rename(col, true),
                _ => match subquery_mut(&mut expr) {
                    Some(subquery) => {
                        let mut transformed = false;
                        for expr in &mut subquery.outer_ref_columns {
                            if let Expr::Column(col)
                            | Expr::OuterReferenceColumn(_, col) = expr
                            {
                                transformed |= self.rename(col, true);
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
        id.and_then(|id| renames.ids.get(&id).copied())
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

    // columns stored outside of expressions
    match &mut plan {
        LogicalPlan::Subquery(Subquery {
            outer_ref_columns, ..
        }) => {
            for expr in outer_ref_columns {
                if let Expr::Column(col) | Expr::OuterReferenceColumn(_, col) = expr {
                    transformed |= renames.rename(col, true);
                }
            }
        }
        LogicalPlan::Unnest(unnest) => {
            for (_, list) in &mut unnest.list_type_columns {
                transformed |= renames.rename(&mut list.output_column, false);
            }
        }
        _ => {}
    }

    let mut result =
        map_expressions_keeping_schema(plan, |expr| renames.rename_expr(expr))?;
    result.transformed |= transformed;
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

        for (i, id) in plan.node_defined_column_ids().iter().enumerate() {
            if let Some(id) = id
                && !self.defined.insert(*id)
            {
                let column =
                    Column::from(plan.schema().qualified_field(i)).with_id(Some(*id));
                report(
                    &mut self.issues,
                    ColumnIdIssueKind::DuplicateDefinition,
                    column,
                );
            }
        }

        let inputs = plan.inputs();
        let scopes: Vec<DFSchemaRef> = if inputs.is_empty() {
            vec![Arc::clone(plan.schema())]
        } else {
            inputs
                .iter()
                .map(|input| Arc::clone(input.schema()))
                .collect()
        };

        // `apply_node_column_refs` only fails if its closure does
        let _ = apply_node_column_refs(plan, |col, is_outer| {
            let kind = if is_outer {
                classify(col, outer_scopes.iter().rev().flatten().map(|s| s.as_ref()))
            } else {
                classify(col, scopes.iter().map(|s| s.as_ref()))
            };
            if let Some(kind) = kind {
                report(&mut self.issues, kind, col.clone());
            }
        });

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

/// Classifies the reference `col` against `schemas`, searched in order.
/// Returns `None` if it is consistent.
fn classify<'a>(
    col: &Column,
    schemas: impl Iterator<Item = &'a DFSchema> + Clone,
) -> Option<ColumnIdIssueKind> {
    let Some(id) = col.id else {
        return Some(ColumnIdIssueKind::Missing);
    };
    let found_by_id = schemas
        .clone()
        .any(|schema| schema.index_of_column_id(id).is_some());
    // the id of the field the name resolves to, if it resolves
    let id_by_name = schemas.into_iter().find_map(|schema| {
        schema
            .index_of_column_by_name(col.relation.as_ref(), &col.name)
            .map(|i| schema.field_id(i))
    });
    match (found_by_id, id_by_name) {
        (true, Some(id_by_name)) if id_by_name != Some(id) => {
            Some(ColumnIdIssueKind::Disagree)
        }
        (true, _) => None,
        (false, Some(Some(id_by_name))) if id_by_name != id => {
            Some(ColumnIdIssueKind::Misresolved)
        }
        (false, _) => Some(ColumnIdIssueKind::Stale),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::builder::table_scan;
    use crate::{LogicalPlanBuilder, col, exists, lit};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ColumnIdGenerator;
    use std::sync::Arc;

    #[test]
    fn max_column_id_without_ids() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let plan = table_scan(Some("t"), &schema, None)?
            .filter(col("a").gt(lit(1)))?
            .build()?;
        assert_eq!(plan.max_column_id(), None);
        Ok(())
    }

    #[test]
    fn max_column_id_finds_references_in_subqueries() -> Result<()> {
        let generator = ColumnIdGenerator::new();
        let low = generator.next();
        let high = generator.next();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        // a subquery reading an outer reference with the highest id
        let outer_ref = Expr::OuterReferenceColumn(
            Arc::new(Field::new("a", DataType::Int32, false)),
            Column::new(Some("t"), "a").with_id(Some(high)),
        );
        let inner_ref = Expr::Column(Column::new(Some("s"), "a").with_id(Some(low)));
        let subquery = table_scan(Some("s"), &schema, None)?
            .filter(outer_ref.eq(inner_ref))?
            .build()?;
        let plan = table_scan(Some("t"), &schema, None)?
            .filter(exists(Arc::new(subquery)))?
            .build()?;
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
        scan.projected_schema = Arc::new(projected);
        Ok(LogicalPlan::TableScan(scan))
    }

    /// A reference to `relation.name` with the id `n`
    fn col_with_id(relation: &str, name: &str, n: Option<u64>) -> Expr {
        Expr::Column(Column::new(Some(relation), name).with_id(n.map(id)))
    }

    /// A filter `expr = 1` over `input`
    fn filter(input: LogicalPlan, expr: Expr) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Filter(crate::Filter::try_new(
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
    fn misresolved() -> Result<()> {
        // the name resolves to a field with another id
        let scan = scan_with_ids("t", [Some(1), Some(2)])?;
        let plan = filter(scan, col_with_id("t", "a", Some(7)))?;
        let issues = plan.column_id_issues();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].kind, ColumnIdIssueKind::Misresolved);
        assert_eq!(
            issues[0].to_string(),
            "misresolved: t.a#7 in Filter: t.a = Int32(1)"
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
            crate::JoinType::Inner,
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
        assert_eq!(kinds(&plan), vec![ColumnIdIssueKind::Misresolved]);
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

        // a projection passes bare columns through, computed outputs have none
        let plan = LogicalPlanBuilder::from(t()?)
            .project(vec![col("t.b"), col("t.a").alias("x"), col("t.a") + lit(1)])?
            .build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(2), None, None]));

        // so do the grouping of an aggregate and a window (with its input)
        let plan = LogicalPlanBuilder::from(t()?)
            .aggregate(
                vec![col("t.b"), col("t.a") + lit(1)],
                vec![col("t.a").gt(lit(0))],
            )?
            .build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(2), None, None]));

        // an alias requalifies and keeps the ids
        let plan = LogicalPlanBuilder::from(t()?).alias("u")?.build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(1), Some(2)]));
        // when it has to rename duplicate names, the renamed columns are
        // computed (aliased) outputs, without ids yet
        let plan = LogicalPlanBuilder::from(t()?)
            .cross_join(s()?)?
            .alias("u")?
            .build()?;
        assert_eq!(output_ids(&plan), ids(&[Some(1), Some(2), None, None]));

        // joins concatenate, semi joins keep one side, mark joins add a mark
        // column without an id
        for (join_type, expected) in [
            (
                crate::JoinType::Left,
                ids(&[Some(1), Some(2), Some(3), Some(4)]),
            ),
            (crate::JoinType::LeftSemi, ids(&[Some(1), Some(2)])),
            (crate::JoinType::RightAnti, ids(&[Some(3), Some(4)])),
            (crate::JoinType::LeftMark, ids(&[Some(1), Some(2), None])),
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

        // a union defines its outputs: they have no id yet
        let plan = LogicalPlanBuilder::from(t()?).union(s()?)?.build()?;
        assert_eq!(output_ids(&plan), ids(&[None, None]));
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
        scan.projected_schema = Arc::new(
            Arc::unwrap_or_clone(scan.projected_schema)
                .with_field_ids(vec![Some(id(1)), Some(id(2))])?,
        );
        let scan = LogicalPlan::TableScan(scan);

        let plan = LogicalPlanBuilder::from(scan.clone())
            .unnest_column("t.l")?
            .build()?;
        assert_eq!(output_ids(&plan), vec![Some(id(1)), None]);

        // (the window functions are in other crates: a window without any)
        let window = crate::Window::try_new(vec![], Arc::new(scan))?;
        assert_eq!(
            output_ids(&LogicalPlan::Window(window)),
            vec![Some(id(1)), Some(id(2))]
        );
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

        let plan = plan.with_fresh_column_ids(Some(&generator))?;
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
    fn renumbering_without_generator_strips() -> Result<()> {
        let generator = ColumnIdGenerator::new();
        let scan = scan_from("t", &generator)?;
        let plan = filter(scan, col_with_id("t", "a", Some(1)))?;

        let plan = plan.with_fresh_column_ids(None)?;
        assert_eq!(output_ids(&plan), vec![None, None]);
        assert!(plan.defined_column_ids().is_empty());
        assert_eq!(kinds(&plan), vec![ColumnIdIssueKind::Missing]);
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

        let subquery = subquery.with_fresh_column_ids(Some(&generator))?;
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

        // without a generator, the inlined plan has no ids
        let plan = LogicalPlanBuilder::scan("v", Arc::clone(&view), None)?.build()?;
        assert!(plan.defined_column_ids().is_empty());
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

        // without a generator, no ids
        let scan_without = LogicalPlanBuilder::scan("t", source(), None)?.build()?;
        assert_eq!(output_ids(&scan_without), vec![None, None]);

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
            .join_on(right, crate::JoinType::Inner, vec![a.eq(b)])?
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
                crate::JoinType::Inner,
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
    fn subquery_built_from_the_outer_plan_strips_ambiguous_outer_references() -> Result<()>
    {
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
        assert_eq!(refs, vec![None]);
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
        // one subquery in two places, as a rule may leave it
        let predicate = exists(Arc::clone(&subquery)).and(exists(subquery));
        let plan =
            LogicalPlan::Filter(crate::Filter::try_new(predicate, Arc::new(outer))?);
        assert!(kinds(&plan).contains(&ColumnIdIssueKind::DuplicateDefinition));

        let renumbered =
            plan.with_fresh_column_ids_in_repeated_subqueries(Some(&generator))?;
        assert!(renumbered.transformed);
        let plan = renumbered.data;
        assert_eq!(kinds(&plan), vec![]);
        assert_eq!(
            plan.defined_column_ids(),
            [id(1), id(2), id(3), id(4), id(5), id(6)].into()
        );

        // nothing to do the second time
        let again =
            plan.with_fresh_column_ids_in_repeated_subqueries(Some(&generator))?;
        assert!(!again.transformed);
        Ok(())
    }
}
