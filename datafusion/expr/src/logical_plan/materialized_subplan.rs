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

//! Logical plan nodes for materialized subplans.
//!
//! A *materialized subplan* is a subplan that is computed once, cached, and
//! shared across multiple references in the surrounding plan instead of being
//! recomputed (re-planned) at every reference. The first user of this
//! infrastructure is multiply-referenced CTEs (wrapped by the SQL planner), but
//! the nodes are deliberately CTE-agnostic so that a future
//! `CommonSubplanEliminate` optimizer rule can wrap *any* duplicated subplan
//! (see the discussion on the "Subplan Materialization" epic,
//! <https://github.com/apache/datafusion/issues/22676>).
//!
//! Each producer/reader pair is identified by a unique [`SubplanId`] assigned at
//! planning time. The id — **not** the human-readable name — is what binds a
//! [`MaterializedSubplanReader`] to its [`MaterializedSubplanProducer`] and what
//! keys the shared cache at physical-planning time. Names are not unique under
//! nesting (e.g. two distinct CTEs both called `t` in sibling subqueries), so
//! keying anything by name is unsafe once subplans from different scopes can
//! collide. The `name` field is retained for display/debugging only.

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, DFSchemaRef, Result};

/// A globally-unique identifier for a materialized subplan.
///
/// A producer and all of the readers that consume its cached result share the
/// same `SubplanId`. Ids are unique within a process, which is all that is
/// required: the id only needs to disambiguate subplans *within a single
/// physical plan* (where the name-collision hazard lives), and a monotonic
/// counter trivially guarantees that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubplanId(pub u64);

impl SubplanId {
    /// Allocate a fresh, process-unique [`SubplanId`].
    pub fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        SubplanId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for SubplanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn get_all_columns_from_schema(schema: &DFSchema) -> HashSet<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// A logical plan node that materializes a subplan and makes it available to a
/// continuation plan. The subplan is executed once, its results cached, and any
/// [`MaterializedSubplanReader`] nodes in the continuation with the same
/// [`SubplanId`] read from that cache.
#[derive(Debug, Clone)]
pub struct MaterializedSubplanProducer {
    /// Unique id binding this producer to its readers and keying the cache.
    pub id: SubplanId,
    /// Human-readable name of the materialized subplan (display/debug only).
    /// For CTEs this is the CTE name; it is **not** required to be unique.
    pub name: String,
    /// The plan that computes the subplan
    pub plan: Arc<LogicalPlan>,
    /// The plan that uses the materialized subplan (continuation)
    pub continuation: Arc<LogicalPlan>,
    /// The output schema (same as continuation's schema)
    pub schema: DFSchemaRef,
    /// If true, the subplan was explicitly marked MATERIALIZED and must not be
    /// inlined by the optimizer.
    pub force_materialized: bool,
}

impl PartialEq for MaterializedSubplanProducer {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.plan == other.plan
            && self.continuation == other.continuation
    }
}

impl Eq for MaterializedSubplanProducer {}

impl PartialOrd for MaterializedSubplanProducer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Hash for MaterializedSubplanProducer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.name.hash(state);
        self.plan.hash(state);
        self.continuation.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for MaterializedSubplanProducer {
    fn name(&self) -> &str {
        "MaterializedSubplanProducer"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.plan.as_ref(), self.continuation.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        get_all_columns_from_schema(self.schema())
    }

    fn necessary_children_exprs(
        &self,
        output_columns: &[usize],
    ) -> Option<Vec<Vec<usize>>> {
        // Child 0 (plan): need all columns because multiple readers in the
        // continuation may reference different subsets. We cannot safely prune
        // without inspecting every reader.
        let plan_all_columns: Vec<usize> =
            (0..self.plan.schema().fields().len()).collect();
        // Child 1 (continuation): pass through the requested output columns
        // since the producer's output schema equals the continuation's output schema.
        let continuation_columns = output_columns.to_vec();
        Some(vec![plan_all_columns, continuation_columns])
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MaterializedSubplanProducer: id={}, name={}",
            self.id, self.name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 2);
        let plan = inputs[0].clone();
        let plan_schema = Arc::clone(plan.schema());
        let id = self.id;
        let continuation = inputs[1]
            .clone()
            .transform_down(move |node| {
                if let LogicalPlan::Extension(Extension {
                    node: extension_node,
                }) = &node
                    && let Some(reader) = extension_node
                        .as_any()
                        .downcast_ref::<MaterializedSubplanReader>()
                    && reader.id == id
                {
                    let reader = MaterializedSubplanReader {
                        id: reader.id,
                        name: reader.name.clone(),
                        schema: Arc::clone(&plan_schema),
                    };
                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(reader),
                    })));
                }
                Ok(Transformed::no(node))
            })?
            .data;
        Ok(Self {
            id: self.id,
            name: self.name.clone(),
            plan: Arc::new(plan),
            schema: Arc::clone(continuation.schema()),
            continuation: Arc::new(continuation),
            force_materialized: self.force_materialized,
        })
    }
}

/// A logical plan node that reads from a previously materialized subplan cache.
/// This is a leaf node (no inputs) that will be wired to the cache at
/// physical planning time, matched to its producer by [`SubplanId`].
#[derive(Debug, Clone)]
pub struct MaterializedSubplanReader {
    /// Id of the producer this reader reads from.
    pub id: SubplanId,
    /// Human-readable name of the materialized subplan (display/debug only).
    pub name: String,
    /// The schema of the subplan output
    pub schema: DFSchemaRef,
}

impl PartialEq for MaterializedSubplanReader {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.name == other.name && self.schema == other.schema
    }
}

impl Eq for MaterializedSubplanReader {}

impl PartialOrd for MaterializedSubplanReader {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Hash for MaterializedSubplanReader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.name.hash(state);
        self.schema.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for MaterializedSubplanReader {
    fn name(&self) -> &str {
        "MaterializedSubplanReader"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        get_all_columns_from_schema(self.schema())
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MaterializedSubplanReader: id={}, name={}",
            self.id, self.name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            id: self.id,
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
        })
    }
}
