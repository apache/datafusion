// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The identity of the columns of a [`LogicalPlan`].
//!
//! A [`ColumnId`] identifies a column independently of its name. The id is
//! created where the column is defined, and stored with the definition:
//!
//! * A [`TableScan`] or [`Values`] defines its columns. Each gets a new
//!   ([`ColumnId::next`]) id when the node is built, and keeps it when the node
//!   is rebuilt from the old node (see `TableScanBuilder::from`).
//! * An [`Alias`] defines a column, with the id in [`Alias::id`]. Every output
//!   of a [`Projection`], [`Aggregate`] and [`Window`] is either a column it
//!   passes through, or an alias: the constructors wrap any other expression
//!   in an alias named after it ([`named_output_expr`]). So the id of a
//!   computed column is part of its expression, and survives when an optimizer
//!   rule rewrites the expression or rebuilds the node.
//! * A [`Union`] (and a `RecursiveQuery`) defines its output columns: each
//!   gets a new id when the node is built, stored in the node's schema, and
//!   kept when the node is rebuilt (also when an optimizer rule replaces,
//!   removes or flattens its inputs).
//! * A column that a node computes from one input column, without an
//!   expression to hold an id (the mark column of a join, a grouping set
//!   column, an unnested column), gets a derived ([`ColumnId::derived`]) id: a
//!   hash of the node kind and the id of the input column. A node rebuilt from
//!   the same input gets the same ids.
//!
//! Every other node ([`Filter`], [`Sort`], [`Join`], [`SubqueryAlias`], ...)
//! passes the ids of its input columns through.
//!
//! A column reference gets its id when its name is resolved against a schema,
//! which happens once, when the plan is built (`normalize_col`,
//! `expr_as_column_expr`). Schema lookups use the id when it is set (see
//! [`DFSchema::maybe_index_of_column`]), and the qualifier and name otherwise.
//!
//! [`LogicalPlan::column_id_issues`] checks that the references of a plan are
//! consistent with the ids of the columns they read.
//!
//! [`Alias`]: crate::expr::Alias
//! [`Alias::id`]: crate::expr::Alias::id
//! [`Filter`]: crate::logical_plan::Filter
//! [`Sort`]: crate::logical_plan::Sort

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use datafusion_common::cse::HashNode;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    Column, ColumnId, DFSchema, DFSchemaRef, JoinType, Result, TableReference,
};

use crate::Expr;
use crate::expr::{Alias, AliasKind, Exists, InSubquery, SetComparison};
use crate::logical_plan::Subquery;
use crate::logical_plan::{Distinct, LogicalPlan};
use crate::utils::grouping_set_to_exprlist;

/// Returns `expr` as an output of a [`Projection`], [`Aggregate`] or
/// [`Window`]: a column that the node passes through, or an alias that
/// defines a column.
///
/// Any other expression is wrapped in an alias with the expression's own
/// name, so the output has the same name as before, and an id. `id` is used
/// if given, a new id otherwise. Such an alias is not shown when the plan is
/// displayed (see the `Display` implementation of [`Expr`]).
///
/// [`Projection`]: crate::logical_plan::Projection
/// [`Aggregate`]: crate::logical_plan::Aggregate
/// [`Window`]: crate::logical_plan::Window
pub fn named_output_expr(expr: Expr, id: Option<ColumnId>) -> Expr {
    #[expect(deprecated)]
    match expr {
        // An implicit alias around an alias with the same name (left when a
        // rewrite turned the named expression into an alias) is redundant: the
        // inner alias names the output, with the outer alias's id
        Expr::Alias(outer)
            if outer.kind != AliasKind::Explicit
                && matches!(outer.expr.as_ref(), Expr::Alias(inner) if inner.name == outer.name) =>
        {
            let Expr::Alias(inner) = *outer.expr else {
                unreachable!()
            };
            Expr::Alias(inner.with_id(id.unwrap_or(outer.id)))
        }
        Expr::Column(_)
        | Expr::Alias(_)
        | Expr::Wildcard { .. }
        | Expr::GroupingSet(_) => expr,
        _ => {
            let name = expr.schema_name().to_string();
            let alias =
                Alias::new(expr, None::<TableReference>, name).with_implicit(true);
            Expr::Alias(match id {
                Some(id) => alias.with_id(id),
                None => alias,
            })
        }
    }
}

/// Returns `exprs` as outputs, see [`named_output_expr`]. The ids of
/// `schema`, if given and of the same length, are used for the new aliases.
pub fn named_output_exprs(exprs: Vec<Expr>, schema: Option<&DFSchema>) -> Vec<Expr> {
    let ids = schema
        .filter(|schema| schema.fields().len() == exprs.len())
        .map(|schema| schema.field_ids());
    exprs
        .into_iter()
        .enumerate()
        .map(|(i, expr)| named_output_expr(expr, ids.and_then(|ids| ids[i])))
        .collect()
}

/// Gives the output expressions of a node that replaces another node the ids
/// of the replaced node's output columns (`schema`), so that the references
/// to those columns keep resolving.
///
/// An output that passes a column through keeps that column's id.
pub fn with_output_ids(exprs: Vec<Expr>, schema: &DFSchema) -> Vec<Expr> {
    if exprs.len() != schema.fields().len() {
        return exprs;
    }
    exprs
        .into_iter()
        .enumerate()
        .map(|(i, expr)| match (expr, schema.field_id(i)) {
            (Expr::Alias(alias), Some(id)) => Expr::Alias(alias.with_id(id)),
            (expr @ Expr::Column(_), _) | (expr, None) => expr,
            (expr, Some(id)) => named_output_expr(expr, Some(id)),
        })
        .collect()
}

/// Returns the [`ColumnId`] of the column that an output expression of a node
/// defines or passes through, if known.
///
/// * an [`Expr::Alias`] defines a column with the alias's id
/// * an [`Expr::Column`] passes through the input column it refers to
/// * any other expression (an output that is not named yet) gets a derived
///   id, see [`derived_expr_id`]
pub fn output_column_id(expr: &Expr, input_schema: &DFSchema) -> Option<ColumnId> {
    match expr {
        Expr::Alias(alias) => Some(alias.id),
        Expr::Column(col) => resolve_column_id(col, input_schema),
        _ => derived_expr_id(expr, input_schema),
    }
}

/// Returns the [`ColumnId`]s of the columns defined by a list of output
/// expressions, see [`output_column_id`]
pub fn output_column_ids<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    input_schema: &DFSchema,
) -> Vec<Option<ColumnId>> {
    exprs
        .into_iter()
        .map(|expr| output_column_id(expr, input_schema))
        .collect()
}

/// The id of the field of `schema` that `col` refers to, if known
fn resolve_column_id(col: &Column, schema: &DFSchema) -> Option<ColumnId> {
    schema
        .maybe_index_of_column(col)
        .and_then(|idx| schema.field_id(idx))
}

/// Returns the derived [`ColumnId`] of the value that `expr` computes from the
/// columns of `input_schema`: a hash of the expression, with each column
/// reference replaced by the id of the column it reads.
///
/// Returns `None` if the expression is volatile (two evaluations are different
/// values), or reads a column whose id is not known.
pub fn derived_expr_id(expr: &Expr, input_schema: &DFSchema) -> Option<ColumnId> {
    if expr.is_volatile() {
        return None;
    }
    let mut hasher = DefaultHasher::new();
    let mut complete = true;
    expr.apply(|expr| {
        match expr {
            Expr::Column(col) => {
                match col.id.or_else(|| resolve_column_id(col, input_schema)) {
                    Some(id) => id.hash(&mut hasher),
                    None => complete = false,
                }
            }
            Expr::OuterReferenceColumn(_, col) => match col.id {
                Some(id) => id.hash(&mut hasher),
                None => complete = false,
            },
            _ => expr.hash_node(&mut hasher),
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("infallible");
    complete.then(|| ColumnId::derived(("expr", hasher.finish())))
}

/// Returns the [`ColumnId`]s of the output columns of a join, in the order of
/// [`build_join_schema`](crate::logical_plan::builder::build_join_schema)
pub fn join_output_column_ids(
    left: &DFSchema,
    right: &DFSchema,
    join_type: &JoinType,
) -> Vec<Option<ColumnId>> {
    let left_ids = left.field_ids().iter().copied();
    let right_ids = right.field_ids().iter().copied();
    // the mark column is computed from the rows of the other side
    let mark_id =
        |marked: &DFSchema| Some(ColumnId::derived(("join_mark", marked.field_ids())));
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            left_ids.chain(right_ids).collect()
        }
        JoinType::LeftSemi | JoinType::LeftAnti => left_ids.collect(),
        JoinType::LeftMark => left_ids.chain(std::iter::once(mark_id(right))).collect(),
        JoinType::RightSemi | JoinType::RightAnti => right_ids.collect(),
        JoinType::RightMark => right_ids.chain(std::iter::once(mark_id(left))).collect(),
    }
}

/// Returns the [`ColumnId`]s of the output columns of an aggregate.
///
/// A grouping column keeps the id of its input column, unless the aggregate
/// has grouping sets: there, a grouping column is NULL in the rows of the sets
/// that do not group by it, so it is a different column, with an id derived
/// from the input column's (see [`grouping_set_column_id`]). The internal
/// grouping id column also gets a derived id.
pub fn aggregate_output_column_ids(
    group_expr: &[Expr],
    aggr_expr: &[Expr],
    input_schema: &DFSchema,
) -> Result<Vec<Option<ColumnId>>> {
    let is_grouping_set = matches!(group_expr, [Expr::GroupingSet(_)]);
    let grouping_expr = grouping_set_to_exprlist(group_expr)?;
    let mut ids = output_column_ids(grouping_expr, input_schema);
    if is_grouping_set {
        let grouping_id = ColumnId::derived(("grouping_id", &ids));
        ids = ids
            .into_iter()
            .map(|id| id.map(grouping_set_column_id))
            .collect();
        ids.push(Some(grouping_id));
    }
    ids.extend(output_column_ids(aggr_expr, input_schema));
    Ok(ids)
}

/// The id of the output column of an aggregate with grouping sets that groups
/// by the input column `input_id`
pub fn grouping_set_column_id(input_id: ColumnId) -> ColumnId {
    ColumnId::derived(("grouping_set", input_id))
}

/// Returns new [`ColumnId`]s for the `len` output columns of a node that
/// defines all of its output columns itself, such as a [`Union`]
///
/// [`Union`]: crate::logical_plan::Union
pub fn new_output_column_ids(len: usize) -> Vec<Option<ColumnId>> {
    (0..len).map(|_| Some(ColumnId::next())).collect()
}

/// A map from columns to values, for example from the output columns of a
/// node to the expressions that compute them.
///
/// A column is looked up by its [`ColumnId`] when it has one that is in the
/// map. Otherwise it is looked up by its qualifier and name, unless two
/// entries have them: such a name is ambiguous and matches nothing, instead of
/// matching whichever entry was inserted last. An id that two entries have
/// (for example after a subtree was copied without new ids) is ambiguous in
/// the same way.
///
/// The qualifier and name are not flattened into one string: an unqualified
/// field named `t.a` (such as the output of `CAST(t.a AS ...)`) is a different
/// field than `a` qualified by `t`.
#[derive(Debug, Clone)]
pub struct ColumnMap<T> {
    /// `None` marks an id that more than one entry has
    by_id: HashMap<ColumnId, Option<T>>,
    /// `None` marks a name that more than one entry has
    by_name: HashMap<(Option<TableReference>, String), Option<T>>,
}

impl<T> Default for ColumnMap<T> {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            by_name: HashMap::new(),
        }
    }
}

impl<T: Clone> ColumnMap<T> {
    /// Create an empty map
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an empty map with space for `capacity` entries
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            by_id: HashMap::with_capacity(capacity),
            by_name: HashMap::with_capacity(capacity),
        }
    }

    /// Insert a value for the column with the given id (if known),
    /// qualifier and name
    pub fn insert(
        &mut self,
        id: Option<ColumnId>,
        relation: Option<TableReference>,
        name: String,
        value: T,
    ) {
        if let Some(id) = id {
            match self.by_id.entry(id) {
                Entry::Occupied(mut entry) => {
                    entry.insert(None);
                }
                Entry::Vacant(entry) => {
                    entry.insert(Some(value.clone()));
                }
            }
        }
        match self.by_name.entry((relation, name)) {
            Entry::Occupied(mut entry) => {
                entry.insert(None);
            }
            Entry::Vacant(entry) => {
                entry.insert(Some(value));
            }
        }
    }

    /// Insert a value for the field at index `i` of `schema`
    pub fn insert_field(&mut self, schema: &DFSchema, i: usize, value: T) {
        let Column {
            relation, name, id, ..
        } = schema.column(i);
        self.insert(id, relation, name, value);
    }

    /// Look up the value for a column
    pub fn get(&self, column: &Column) -> Option<&T> {
        if let Some(id) = column.id
            && let Some(value) = self.by_id.get(&id)
        {
            // an ambiguous id falls back to the name
            if let Some(value) = value {
                return Some(value);
            }
        }
        self.by_name
            .get(&(column.relation.clone(), column.name.clone()))
            .and_then(|value| value.as_ref())
    }

    /// Does the map have a value for a column?
    pub fn contains(&self, column: &Column) -> bool {
        self.get(column).is_some()
    }

    /// Does the map have no entries?
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty() && self.by_id.is_empty()
    }

    /// Remove all entries
    pub fn clear(&mut self) {
        self.by_id.clear();
        self.by_name.clear();
    }
}

/// Moves each column reference (and outer reference) of `expr` whose id
/// `new_id` maps to the new id, for example when an expression moves from
/// below a node to above it, where the node defines other columns for the
/// ones it reads
pub fn remap_column_ids(
    expr: Expr,
    new_id: impl Fn(ColumnId) -> Option<ColumnId>,
) -> Expr {
    remap_column_ids_dyn(expr, &new_id)
}

fn remap_column_ids_dyn(
    expr: Expr,
    new_id: &dyn Fn(ColumnId) -> Option<ColumnId>,
) -> Expr {
    // The outer references a subquery lists (not children of the expression)
    let remap_outer_refs = |subquery: Subquery| Subquery {
        outer_ref_columns: subquery
            .outer_ref_columns
            .into_iter()
            .map(|expr| remap_column_ids_dyn(expr, new_id))
            .collect(),
        ..subquery
    };
    expr.transform_up(|expr| match expr {
        Expr::ScalarSubquery(subquery) => Ok(Transformed::yes(Expr::ScalarSubquery(
            remap_outer_refs(subquery),
        ))),
        Expr::Exists(Exists { subquery, negated }) => {
            Ok(Transformed::yes(Expr::Exists(Exists {
                subquery: remap_outer_refs(subquery),
                negated,
            })))
        }
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated,
        }) => Ok(Transformed::yes(Expr::InSubquery(InSubquery {
            expr,
            subquery: remap_outer_refs(subquery),
            negated,
        }))),
        Expr::SetComparison(set_comparison) => {
            Ok(Transformed::yes(Expr::SetComparison(SetComparison {
                subquery: remap_outer_refs(set_comparison.subquery),
                ..set_comparison
            })))
        }
        Expr::Column(col) => match col.id.and_then(new_id) {
            Some(id) => Ok(Transformed::yes(Expr::Column(col.with_id(Some(id))))),
            None => Ok(Transformed::no(Expr::Column(col))),
        },
        Expr::OuterReferenceColumn(field, col) => match col.id.and_then(new_id) {
            Some(id) => Ok(Transformed::yes(Expr::OuterReferenceColumn(
                field,
                col.with_id(Some(id)),
            ))),
            None => Ok(Transformed::no(Expr::OuterReferenceColumn(field, col))),
        },
        _ => Ok(Transformed::no(expr)),
    })
    .data()
    .expect("infallible")
}

/// Replaces each column of `expr` that is in `map` with the mapped expression
pub fn replace_columns(
    expr: Expr,
    map: &ColumnMap<impl AsRef<Expr> + Clone>,
) -> Result<Expr> {
    expr.transform_up(|expr| {
        if let Expr::Column(c) = &expr
            && let Some(new_expr) = map.get(c)
        {
            Ok(Transformed::yes(new_expr.as_ref().clone()))
        } else {
            Ok(Transformed::no(expr))
        }
    })
    .data()
}

/// A column reference whose id is not consistent with the columns it can
/// read, see [`LogicalPlan::column_id_issues`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnIdIssue {
    /// The reference has no id
    Unresolved { node: String, column: String },
    /// The id is not the id of a column of the node's inputs
    Stale { node: String, column: String },
    /// The id and the name refer to different columns of the node's inputs
    Mismatch {
        node: String,
        column: String,
        by_id: String,
        by_name: String,
    },
    /// The id refers to an input column with another name. The physical
    /// planner finds a column by its index and checks its name.
    Renamed {
        node: String,
        column: String,
        by_id: String,
    },
}

impl std::fmt::Display for ColumnIdIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unresolved { node, column } => {
                write!(f, "unresolved: {column} in {node}")
            }
            Self::Stale { node, column } => write!(f, "stale: {column} in {node}"),
            Self::Mismatch {
                node,
                column,
                by_id,
                by_name,
            } => write!(
                f,
                "mismatch: {column} in {node} is {by_id} by id, {by_name} by name"
            ),
            Self::Renamed {
                node,
                column,
                by_id,
            } => write!(f, "renamed: {column} in {node} is {by_id} by id"),
        }
    }
}

/// Checks one column reference of a node against the node's inputs
fn check_column_ref(
    node: &LogicalPlan,
    col: &Column,
    input_schemas: &[&DFSchema],
    issues: &mut Vec<ColumnIdIssue>,
) {
    let node_name = || node.display().to_string();
    let Some(id) = col.id else {
        issues.push(ColumnIdIssue::Unresolved {
            node: node_name(),
            column: col.flat_name(),
        });
        return;
    };
    let by_id = input_schemas
        .iter()
        .find_map(|schema| schema.index_of_column_id(id).map(|idx| schema.column(idx)));
    let Some(by_id) = by_id else {
        issues.push(ColumnIdIssue::Stale {
            node: node_name(),
            column: col.flat_name(),
        });
        return;
    };
    let by_name = input_schemas.iter().find_map(|schema| {
        schema
            .index_of_column_by_name(col.relation.as_ref(), &col.name)
            .map(|idx| schema.column(idx))
    });
    if let Some(by_name) = by_name
        && by_name.id != by_id.id
    {
        issues.push(ColumnIdIssue::Mismatch {
            node: node_name(),
            column: col.flat_name(),
            by_id: by_id.flat_name(),
            by_name: by_name.flat_name(),
        });
    } else if by_id.name != col.name {
        issues.push(ColumnIdIssue::Renamed {
            node: node_name(),
            column: col.flat_name(),
            by_id: by_id.flat_name(),
        });
    }
}

/// Records in `new_ids` the new derived ids of the outputs of `node`, which
/// is about to be renamed: the ids its outputs get when they are derived again
/// from its (renamed) inputs. Ids that `new_ids` already renames are kept.
fn rederive_output_ids(
    node: &LogicalPlan,
    new_ids: &mut HashMap<ColumnId, ColumnId>,
) -> Result<()> {
    let defines_derived_ids = matches!(
        node,
        LogicalPlan::Unnest(_) | LogicalPlan::Join(_) | LogicalPlan::Aggregate(_)
    );
    let old_ids = node.schema().field_ids();
    if !defines_derived_ids
        || !old_ids
            .iter()
            .flatten()
            .any(|id| id.is_derived() && !new_ids.contains_key(id))
    {
        return Ok(());
    }
    let rederived = node.clone().recompute_schema()?;
    for (old, new) in old_ids.iter().zip(rederived.schema().field_ids()) {
        if let (Some(old), Some(new)) = (old, new)
            && old.is_derived()
            && old != new
        {
            new_ids.entry(*old).or_insert(*new);
        }
    }
    Ok(())
}

/// Returns `plan` with the field ids of its own output schema (if it has
/// one) replaced by `f`
fn map_output_ids(
    plan: LogicalPlan,
    f: impl Fn(Option<ColumnId>) -> Option<ColumnId>,
) -> Result<LogicalPlan> {
    let remap = |schema: &DFSchemaRef| -> Result<DFSchemaRef> {
        let ids = schema.field_ids().iter().map(|id| f(*id)).collect();
        Ok(Arc::new(schema.as_ref().clone().with_field_ids(ids)?))
    };
    Ok(match plan {
        LogicalPlan::Projection(mut p) => {
            p.schema = remap(&p.schema)?;
            LogicalPlan::Projection(p)
        }
        LogicalPlan::Window(mut w) => {
            w.schema = remap(&w.schema)?;
            LogicalPlan::Window(w)
        }
        LogicalPlan::Aggregate(mut a) => {
            a.schema = remap(&a.schema)?;
            LogicalPlan::Aggregate(a)
        }
        LogicalPlan::SubqueryAlias(mut a) => {
            a.schema = remap(&a.schema)?;
            LogicalPlan::SubqueryAlias(a)
        }
        LogicalPlan::Join(mut j) => {
            j.schema = remap(&j.schema)?;
            LogicalPlan::Join(j)
        }
        LogicalPlan::TableScan(mut t) => {
            t.projected_schema = remap(&t.projected_schema)?;
            LogicalPlan::TableScan(t)
        }
        LogicalPlan::Values(mut v) => {
            v.schema = remap(&v.schema)?;
            LogicalPlan::Values(v)
        }
        LogicalPlan::EmptyRelation(mut e) => {
            e.schema = remap(&e.schema)?;
            LogicalPlan::EmptyRelation(e)
        }
        LogicalPlan::Union(mut u) => {
            u.schema = remap(&u.schema)?;
            LogicalPlan::Union(u)
        }
        LogicalPlan::Unnest(mut u) => {
            u.schema = remap(&u.schema)?;
            LogicalPlan::Unnest(u)
        }
        LogicalPlan::Distinct(Distinct::On(mut d)) => {
            d.schema = remap(&d.schema)?;
            LogicalPlan::Distinct(Distinct::On(d))
        }
        LogicalPlan::RecursiveQuery(mut r) => {
            r.schema = remap(&r.schema)?;
            LogicalPlan::RecursiveQuery(r)
        }
        plan => plan,
    })
}

impl LogicalPlan {
    /// The ids of the columns this plan (or one of its subqueries) defines,
    /// in the order the nodes are visited (top down), without repetitions.
    ///
    /// Derived ids ([`ColumnId::derived`]) are not included: they are
    /// computed from the ids of the node's inputs, and change with them.
    pub fn defined_column_ids(&self) -> Vec<ColumnId> {
        let mut ids = vec![];
        let mut seen = std::collections::HashSet::new();
        let _ = self.apply_with_subqueries(|node| {
            for id in node.schema().field_ids().iter().flatten() {
                if !id.is_derived() && seen.insert(*id) {
                    ids.push(*id);
                }
            }
            node.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if let Expr::Alias(alias) = expr
                        && seen.insert(alias.id)
                    {
                        ids.push(alias.id);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        });
        ids
    }

    /// Returns this plan with each column id in `new_ids` changed to the
    /// mapped id, in the columns it defines and in the references to them.
    /// Other ids, such as those of outer references to columns defined
    /// outside the plan, are kept.
    pub fn rename_column_ids(
        self,
        new_ids: &HashMap<ColumnId, ColumnId>,
    ) -> Result<LogicalPlan> {
        if new_ids.is_empty() {
            return Ok(self);
        }
        // The derived ids of a node are computed again from its renamed
        // inputs, and the references to them renamed to match: nodes are
        // visited bottom up, so a node's inputs are renamed before it
        let new_ids = std::cell::RefCell::new(new_ids.clone());
        let new_id =
            |id: Option<ColumnId>| id.map(|id| *new_ids.borrow().get(&id).unwrap_or(&id));
        self.transform_up_with_subqueries(|node| {
            let node = node.map_expressions(|expr| {
                Ok(Transformed::yes(
                    remap_column_ids(expr, |id| new_ids.borrow().get(&id).copied())
                        .transform_up(|expr| match expr {
                            Expr::Alias(alias) => {
                                let id = new_id(Some(alias.id)).expect("alias has an id");
                                Ok(Transformed::yes(Expr::Alias(alias.with_id(id))))
                            }
                            _ => Ok(Transformed::no(expr)),
                        })?
                        .data,
                ))
            })?;
            node.transform_data(|node| {
                rederive_output_ids(&node, &mut new_ids.borrow_mut())?;
                map_output_ids(node, new_id).map(Transformed::yes)
            })
        })
        .data()
    }

    /// Returns this plan with a new id for every column that it (or one of
    /// its subqueries) defines, and the references to those columns changed
    /// to match. References to columns defined outside the plan, such as
    /// outer references, keep their ids.
    ///
    /// Use this when the same plan is placed twice in a larger plan, for
    /// example for each reference to a common table expression: otherwise
    /// the two copies define columns with the same ids, and the ids cannot
    /// tell them apart.
    pub fn with_fresh_column_ids(self) -> Result<LogicalPlan> {
        let new_ids = self
            .defined_column_ids()
            .into_iter()
            .map(|id| (id, ColumnId::next()))
            .collect();
        self.rename_column_ids(&new_ids)
    }

    /// Returns this plan with the ids of the columns it defines replaced by
    /// ids that depend only on the plan's structure: the n-th id defined (in
    /// the order of [`Self::defined_column_ids`]) becomes the same id in every
    /// plan.
    ///
    /// Two plans that compute the same columns, but give them different ids
    /// (such as two scans of the same table), are equal after this: compare
    /// canonical plans to decide whether two plans compute the same rows.
    pub fn with_canonical_column_ids(self) -> Result<LogicalPlan> {
        let new_ids = self
            .defined_column_ids()
            .into_iter()
            .enumerate()
            .map(|(n, id)| (id, ColumnId::derived(("canonical", n))))
            .collect();
        self.rename_column_ids(&new_ids)
    }

    /// Do this plan and `other` compute the same columns, in the same way, up
    /// to the ids they give the columns they define?
    pub fn is_equivalent_up_to_column_ids(&self, other: &LogicalPlan) -> Result<bool> {
        if self == other {
            return Ok(true);
        }
        Ok(self.clone().with_canonical_column_ids()?
            == other.clone().with_canonical_column_ids()?)
    }

    /// Returns the plan in indented form, with the ids of each node's output
    /// columns and of the columns its expressions reference
    pub fn display_column_ids(&self) -> String {
        use std::fmt::Write;
        fn fmt_id(id: Option<ColumnId>) -> String {
            id.map(|id| id.to_string())
                .unwrap_or_else(|| "#?".to_string())
        }
        fn visit(plan: &LogicalPlan, indent: usize, out: &mut String) {
            let pad = "  ".repeat(indent);
            let _ = writeln!(out, "{pad}{}", plan.display());
            let outputs: Vec<String> = plan
                .schema()
                .columns()
                .iter()
                .map(|c| format!("{}{}", c.flat_name(), fmt_id(c.id)))
                .collect();
            let _ = writeln!(out, "{pad}  out: {}", outputs.join(", "));
            let mut refs = vec![];
            let _ = plan.apply_expressions(|expr| {
                expr.apply(|expr| {
                    match expr {
                        Expr::Column(c) => {
                            refs.push(format!("{}{}", c.flat_name(), fmt_id(c.id)))
                        }
                        Expr::Alias(alias) => refs.push(format!(
                            "AS {}{}{}",
                            alias.name,
                            alias.id,
                            match alias.kind {
                                AliasKind::Explicit => "",
                                AliasKind::Implicit => "(implicit)",
                                AliasKind::Extraction => "(extraction)",
                            }
                        )),
                        _ => {}
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            });
            if !refs.is_empty() {
                let _ = writeln!(out, "{pad}  refs: {}", refs.join(", "));
            }
            for input in plan.inputs() {
                visit(input, indent + 1, out);
            }
            let _ = plan.apply_subqueries(|subquery| {
                let _ = writeln!(out, "{pad}  subquery:");
                visit(subquery, indent + 2, out);
                Ok(TreeNodeRecursion::Continue)
            });
        }
        let mut out = String::new();
        visit(self, 0, &mut out);
        out
    }

    /// Checks that the column references of this plan, and of its subqueries,
    /// carry the ids of the columns they read.
    ///
    /// Returns one issue for each reference that has no id, whose id is not
    /// the id of an input column, or whose id and name refer to different
    /// input columns. Outer references are not checked.
    pub fn column_id_issues(&self) -> Vec<ColumnIdIssue> {
        let mut issues = vec![];
        let _ = self.apply_with_subqueries(|node| {
            let inputs = node.inputs();
            if inputs.is_empty() {
                return Ok(TreeNodeRecursion::Continue);
            }
            let input_schemas: Vec<&DFSchema> =
                inputs.iter().map(|input| input.schema().as_ref()).collect();
            node.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if let Expr::Column(col) = expr {
                        check_column_ref(node, col, &input_schemas, &mut issues);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        });
        issues
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::TableScanBuilder;
    use crate::logical_plan::builder::{LogicalPlanBuilder, table_scan};
    use crate::{col, lit};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::JoinType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ])
    }

    fn scan(name: &str) -> Result<LogicalPlanBuilder> {
        table_scan(Some(name), &test_schema(), None)
    }

    fn ids(plan: &LogicalPlan) -> Vec<Option<ColumnId>> {
        plan.schema().field_ids().to_vec()
    }

    #[test]
    fn scan_columns_get_ids() -> Result<()> {
        let plan = scan("t")?.build()?;
        let ids = ids(&plan);
        assert!(ids.iter().all(Option::is_some));
        assert_ne!(ids[0], ids[1]);
        Ok(())
    }

    #[test]
    fn built_plans_have_consistent_ids() -> Result<()> {
        let plan = scan("t")?
            .filter(col("a").gt(lit(1)))?
            .alias("x")?
            .project(vec![col("x.a"), (col("x.b") + lit(1)).alias("c")])?
            .aggregate(
                vec![col("a")],
                vec![crate::test::function_stub::count(col("c"))],
            )?
            .build()?;
        assert_eq!(plan.column_id_issues(), vec![]);
        Ok(())
    }

    #[test]
    fn pass_through_keeps_ids_and_alias_defines_new_id() -> Result<()> {
        let plan = scan("t")?
            .filter(col("a").gt(lit(1)))?
            .alias("x")?
            .project(vec![col("x.a"), col("x.a").alias("renamed")])?
            .build()?;

        let LogicalPlan::Projection(projection) = &plan else {
            panic!("expected a projection");
        };
        let scan_ids = projection.input.inputs()[0].inputs()[0]
            .schema()
            .field_ids();
        let output_ids = ids(&plan);
        // `x.a` is the scan's `a`, through the filter and the subquery alias
        assert_eq!(output_ids[0], scan_ids[0]);
        // the alias defines a new column, although it renames `x.a`
        assert!(output_ids[1].is_some());
        assert_ne!(output_ids[1], scan_ids[0]);
        // the reference to `x.a` carries the id
        let Expr::Column(c) = &projection.expr[0] else {
            panic!("expected a column");
        };
        assert_eq!(c.id, scan_ids[0]);
        Ok(())
    }

    #[test]
    fn computed_output_is_named_with_an_id() -> Result<()> {
        let plan = scan("t")?.project(vec![col("a") + lit(1)])?.build()?;
        let LogicalPlan::Projection(projection) = &plan else {
            panic!("expected a projection");
        };
        let Expr::Alias(alias) = &projection.expr[0] else {
            panic!("expected an alias");
        };
        assert_eq!(alias.name, "t.a + Int32(1)");
        assert_eq!(ids(&plan), vec![Some(alias.id)]);
        // the alias is not displayed
        assert_eq!(format!("{}", projection.expr[0]), "t.a + Int32(1)");

        // rebuilding the projection from its expressions keeps the id
        let rebuilt = crate::logical_plan::Projection::try_new(
            projection.expr.clone(),
            Arc::clone(&projection.input),
        )?;
        assert_eq!(rebuilt.schema.field_ids(), &[Some(alias.id)]);
        Ok(())
    }

    #[test]
    fn self_join_scans_get_different_ids() -> Result<()> {
        let left = scan("t")?.alias("l")?.build()?;
        let right = scan("t")?.alias("r")?.build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["l.a"], vec!["r.a"]), None)?
            .build()?;
        let ids = ids(&plan);
        assert_eq!(ids.len(), 4);
        assert_ne!(ids[0], ids[2]);
        Ok(())
    }

    #[test]
    fn rebuilt_scan_keeps_ids() -> Result<()> {
        let plan = scan("t")?.build()?;
        let original = ids(&plan);
        let LogicalPlan::TableScan(scan) = plan else {
            panic!("expected a table scan");
        };
        // keep only `b`
        let narrowed = TableScanBuilder::from(scan)
            .with_projection(Some(vec![1]))
            .build()?;
        assert_eq!(narrowed.projected_schema.field_ids(), &original[1..]);
        Ok(())
    }

    #[test]
    fn derived_ids_depend_on_input_ids() -> Result<()> {
        let t1 = scan("t")?.build()?;
        let t2 = scan("t")?.build()?;
        let a_plus_1 = col("t.a") + lit(1);
        let id1 = derived_expr_id(&a_plus_1, t1.schema());
        let id2 = derived_expr_id(&a_plus_1, t2.schema());
        assert!(id1.is_some());
        // the same expression over the same scan is the same value
        assert_eq!(id1, derived_expr_id(&a_plus_1, t1.schema()));
        // over another scan of the same table, it is another value
        assert_ne!(id1, id2);
        // a volatile expression has no derived id
        let random_udf = crate::create_udf(
            "random",
            vec![],
            DataType::Float64,
            crate::Volatility::Volatile,
            Arc::new(|_| unimplemented!()),
        );
        let random = random_udf.call(vec![]);
        assert_eq!(derived_expr_id(&random, t1.schema()), None);
        Ok(())
    }

    #[test]
    fn column_map_prefers_id_over_name() {
        let id_a = ColumnId::next();
        let id_b = ColumnId::next();
        let mut map = ColumnMap::new();
        map.insert(Some(id_a), Some("t".into()), "a".to_string(), 1);
        map.insert(Some(id_b), None, "x".to_string(), 2);

        // the id wins, although the name refers to another entry
        let col = Column::new(Some("t"), "a").with_id(Some(id_b));
        assert_eq!(map.get(&col), Some(&2));
        // without an id, the qualifier and name are used
        assert_eq!(map.get(&Column::new(Some("t"), "a")), Some(&1));
    }

    #[test]
    fn column_map_does_not_flatten_names() {
        let mut map = ColumnMap::new();
        // `t.a`, and an unqualified field named `t.a` (such as `CAST(t.a AS ..)`)
        map.insert(None, Some("t".into()), "a".to_string(), 1);
        map.insert(None, None, "t.a".to_string(), 2);

        assert_eq!(map.get(&Column::new(Some("t"), "a")), Some(&1));
        assert_eq!(map.get(&Column::new_unqualified("t.a")), Some(&2));
    }

    #[test]
    fn column_map_ambiguous_name_matches_nothing() {
        let mut map = ColumnMap::new();
        map.insert(None, None, "a".to_string(), 1);
        map.insert(None, None, "a".to_string(), 2);
        assert_eq!(map.get(&Column::new_unqualified("a")), None);
    }
}
