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
//! # Verification
//!
//! [`LogicalPlan::column_id_issues`] resolves every column reference against
//! the input schemas of its node (outer references against the enclosing
//! scopes) and reports what does not hold, see [`ColumnIdIssueKind`]. The
//! analyzer and the optimizer can report the issues each rule introduces;
//! they never fail a query for them.
//!
//! [`DFSchema::field_ids`]: datafusion_common::DFSchema::field_ids
//! [`ColumnIdGenerator`]: datafusion_common::ColumnIdGenerator

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use crate::logical_plan::MaterializedCtePlan;
use crate::{Expr, LogicalPlan};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, ColumnId, DFSchema, DFSchemaRef, Result};

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
        let mut verifier = Verifier::default();
        verifier.check(self, &mut vec![]);
        verifier.issues
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

#[derive(Default)]
struct Verifier {
    issues: Vec<ColumnIdIssue>,
    defined: HashSet<ColumnId>,
}

impl Verifier {
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn check(&mut self, plan: &LogicalPlan, outer_scopes: &mut Vec<Vec<DFSchemaRef>>) {
        // the description of the node, computed once if it has issues
        let mut node: Option<String> = None;
        let mut report = |issues: &mut Vec<ColumnIdIssue>, kind, column: Column| {
            let node = node
                .get_or_insert_with(|| plan.display().to_string())
                .clone();
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
}
