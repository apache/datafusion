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
//! The scans are leaves, so the filters of one reference are not pushed into
//! the shared body. This is the same optimization fence that PostgreSQL
//! applies to `MATERIALIZED` CTEs.
//!
//! A scan can read a subset of the columns of the body (see
//! [`MaterializedCteScan::projection`]). The optimizer removes from the body
//! the columns that no scan reads.

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    DFSchema, DFSchemaRef, Result, internal_datafusion_err, internal_err,
};

use crate::LogicalPlan;

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
///    must not push into it a filter, limit or projection that belongs to one
///    scan only. A rule that changes its schema must also change the
///    [`projection`](MaterializedCteScan::projection) of every scan, because a
///    scan reads the columns of the body by position.
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
}

impl MaterializedCte {
    /// Create a new node.
    ///
    /// Returns an error if a scan of this CTE in `continuation` (also in a
    /// subquery expression) does not have the names and types of the columns
    /// of `cte` that it reads.
    pub fn try_new(
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
/// This is a leaf. It outputs all the columns of the CTE body, or the columns
/// that `projection` selects.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedCteScan {
    /// The id of the [`MaterializedCte`] that this scan reads
    pub id: MaterializedCteId,
    /// Name of the CTE, used for display and error messages only
    pub name: String,
    /// The schema of this scan: the columns of the CTE body that
    /// `projection` selects
    pub schema: DFSchemaRef,
    /// The indexes of the columns of the CTE body that this scan outputs, in
    /// output order. `None` means all the columns, in the order of the body.
    ///
    /// The indexes refer to the schema of the body of the [`MaterializedCte`]
    /// with the same id, and the scan reads the buffered batches by position.
    pub projection: Option<Vec<usize>>,
}

impl MaterializedCteScan {
    /// Create a scan of all the columns of the CTE `id`, whose body has the
    /// schema `schema`.
    pub fn new(
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
    pub fn with_body_columns(&self, kept: &[usize]) -> Result<Self> {
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

    /// Check that this scan has the names and types of the columns that it
    /// reads from a CTE body with the schema `body`.
    pub fn check_body_schema(&self, body: &DFSchema) -> Result<()> {
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
