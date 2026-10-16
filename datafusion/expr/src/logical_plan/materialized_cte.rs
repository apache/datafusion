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

//! Logical nodes for `WITH x AS MATERIALIZED (...)`.
//!
//! A materialized CTE is planned as one [`MaterializedCte`] node, which owns
//! the CTE body and the rest of the query (the "continuation"), and one
//! [`MaterializedCteScan`] leaf for each reference to the CTE inside the
//! continuation. Both are in the [`LogicalPlan::MaterializedCte`] variant,
//! as cases of [`MaterializedCtePlan`]. The body is therefore optimized and executed once, and
//! the scans read its buffered output.
//!
//! The scans are leaves, so filters and projections of one reference are not
//! pushed into the shared body. This is the same optimization fence that
//! PostgreSQL applies to `MATERIALIZED` CTEs.

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_common::{Column, DFSchemaRef, Result};

use crate::{Expr, LogicalPlan, Projection};

/// Identifies one materialized CTE and binds its scans to it.
///
/// CTE names are not unique (two sibling subqueries can each declare `WITH t`),
/// so the planner allocates a process-unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaterializedCteId(u64);

impl MaterializedCteId {
    /// Allocate a new unique id.
    pub fn next() -> Self {
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
///    must not change its schema, and must not push into it a filter, limit
///    or projection that belongs to one scan only.
/// 2. `continuation`: the query that declared the CTE.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedCte {
    /// Binds the scans in `continuation` to this node
    pub id: MaterializedCteId,
    /// Name of the CTE, used for display and error messages only
    pub name: String,
    /// The body of the CTE
    pub cte: Arc<LogicalPlan>,
    /// The query that declared the CTE and reads it
    pub continuation: Arc<LogicalPlan>,
    /// `false` when the query asked for this node with `AS MATERIALIZED`.
    /// `true` when the planner created the node without a directive from the
    /// user (see `datafusion.optimizer.enable_automatic_cte_materialization`).
    /// The optimizer can replace an automatic node with [`Self::inline`], and
    /// never does that for an explicit node.
    pub automatic: bool,
}

impl MaterializedCte {
    /// Create a new node.
    ///
    /// Returns an error if a scan of this CTE in `continuation` (also in a
    /// subquery expression) does not have the column names and types of `cte`.
    pub fn try_new(
        id: MaterializedCteId,
        name: impl Into<String>,
        cte: Arc<LogicalPlan>,
        continuation: Arc<LogicalPlan>,
    ) -> Result<Self> {
        let name = name.into();
        for scan in MaterializedCteScan::find_in(&continuation, id)? {
            scan.schema
                .has_equivalent_names_and_types(cte.schema())
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
            automatic: false,
        })
    }

    /// Mark this node as automatic or explicit. See [`Self::automatic`].
    pub fn with_automatic(mut self, automatic: bool) -> Self {
        self.automatic = automatic;
        self
    }

    /// Returns `true` when the user did not ask for this node with `AS
    /// MATERIALIZED`, so the optimizer is free to inline it.
    pub fn is_automatic(&self) -> bool {
        self.automatic
    }

    /// Return the continuation with a copy of the body in place of each scan
    /// of this CTE, also in subquery expressions. The result does not contain
    /// this node, and computes the body once for each scan.
    ///
    /// Each copy has the schema of the scan that it replaces, so the parents
    /// of the scans stay valid.
    pub fn inline(&self) -> Result<LogicalPlan> {
        let id = self.id;
        self.continuation
            .as_ref()
            .clone()
            .transform_down_with_subqueries(|node| match node {
                // A nested occurrence of the same CTE owns the scans below it.
                LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(ref nested))
                    if nested.id == id =>
                {
                    Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
                }
                LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(scan))
                    if scan.id == id =>
                {
                    Ok(Transformed::new(
                        self.body_with_schema_of(&scan)?,
                        true,
                        TreeNodeRecursion::Jump,
                    ))
                }
                _ => Ok(Transformed::no(node)),
            })
            .map(|t| t.data)
    }

    /// Return a copy of the body with the qualified column names of `scan`.
    fn body_with_schema_of(&self, scan: &MaterializedCteScan) -> Result<LogicalPlan> {
        let body = self.cte.as_ref().clone();
        let same_names = scan
            .schema
            .iter()
            .zip(body.schema().iter())
            .all(|((sq, sf), (bq, bf))| sq == bq && sf.name() == bf.name());
        if same_names {
            return Ok(body);
        }
        // A rule changed the names of the body after the scan was created.
        let exprs = scan
            .schema
            .iter()
            .zip(body.schema().iter())
            .map(|((sq, sf), (bq, bf))| {
                Expr::Column(Column::from((bq, bf)))
                    .alias_qualified(sq.cloned(), sf.name())
            })
            .collect();
        Projection::try_new(exprs, Arc::new(body)).map(LogicalPlan::Projection)
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

    /// The scans in the continuation that read this CTE, also in subquery
    /// expressions. See [`MaterializedCteScan::find_in`].
    pub fn scans(&self) -> Result<Vec<MaterializedCteScan>> {
        MaterializedCteScan::find_in(&self.continuation, self.id)
    }

    /// Return a node with the same id and name, and new inputs. The schemas
    /// of the scans are not checked again.
    pub fn with_new_inputs(
        &self,
        cte: Arc<LogicalPlan>,
        continuation: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            cte,
            continuation,
            automatic: self.automatic,
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

/// Reads the buffered output of the [`MaterializedCte`] with the same `id`.
///
/// This is a leaf. Its schema is the schema of the CTE body and is fixed when
/// the scan is created.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedCteScan {
    /// The id of the [`MaterializedCte`] that this scan reads
    pub id: MaterializedCteId,
    /// Name of the CTE, used for display and error messages only
    pub name: String,
    /// The schema of the CTE body
    pub schema: DFSchemaRef,
}

impl MaterializedCteScan {
    /// Create a scan of the CTE `id`, whose body has the schema `schema`.
    pub fn new(
        id: MaterializedCteId,
        name: impl Into<String>,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            schema,
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

    /// Return a copy of each scan of the CTE `id` in `plan`, also in subquery
    /// expressions.
    ///
    /// The search does not go below a [`MaterializedCte`] with the same `id`
    /// in `plan`. Such a node is a separate occurrence of the CTE (the id is
    /// copied when a plan is cloned) and owns the scans below it.
    pub fn find_in(plan: &LogicalPlan, id: MaterializedCteId) -> Result<Vec<Self>> {
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

/// Orders by `id` and `name`. Scans with the same `id` and `name` but a
/// different `schema` are not comparable.
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
    use datafusion_common::DFSchema;

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
            automatic: false,
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

    #[test]
    fn inline_replaces_the_scans_and_keeps_their_schema() -> Result<()> {
        let id = MaterializedCteId::next();
        // The scan in the subquery expression has another qualifier than the
        // body, so its copy of the body gets a projection.
        let other_schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let other = LogicalPlan::MaterializedCte(MaterializedCtePlan::Scan(
            MaterializedCteScan::new(
                id,
                "c",
                Arc::new(DFSchema::try_from_qualified_schema("d", &other_schema)?),
            ),
        ));
        let continuation = LogicalPlanBuilder::from(scan(id, DataType::Int32))
            .filter(exists(Arc::new(other)))?
            .build()?;
        let cte = MaterializedCte::try_new(
            id,
            "c",
            body(DataType::Int32),
            Arc::new(continuation),
        )?
        .with_automatic(true);
        assert!(cte.is_automatic());
        assert_eq!(
            LogicalPlan::MaterializedCte(MaterializedCtePlan::Cte(cte.clone()))
                .display_indent()
                .to_string()
                .lines()
                .next(),
            Some("MaterializedCte: name=c, automatic=true")
        );

        let inlined = cte.inline()?;
        assert_eq!(inlined.schema(), cte.schema());
        assert!(MaterializedCteScan::find_in(&inlined, id)?.is_empty());
        assert_eq!(
            inlined.display_indent().to_string(),
            "Filter: EXISTS (<subquery>)\
            \n  Subquery:\
            \n    Projection: c.a AS a\
            \n      EmptyRelation: rows=0\
            \n  EmptyRelation: rows=0"
        );
        Ok(())
    }
}
