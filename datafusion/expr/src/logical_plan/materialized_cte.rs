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
//! A materialized CTE is planned as one [`MaterializedCte`] node, which owns the
//! CTE body and the rest of the query (the "continuation"), and one
//! [`MaterializedCteScan`] leaf for each reference to the CTE inside the
//! continuation. The body is therefore optimized and executed once, and the
//! scans read its buffered output.
//!
//! The scans are leaves, so filters and projections of one reference are not
//! pushed into the shared body. This is the same optimization fence that
//! PostgreSQL applies to `MATERIALIZED` CTEs.

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use datafusion_common::{DFSchemaRef, Result, assert_eq_or_internal_err};

use crate::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

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

/// Computes `cte` once and evaluates `continuation`, in which every
/// [`MaterializedCteScan`] with the same `id` reads the buffered output of
/// `cte`. The output of this node is the output of `continuation`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedCte {
    pub id: MaterializedCteId,
    pub name: String,
    pub cte: LogicalPlan,
    pub continuation: LogicalPlan,
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

impl UserDefinedLogicalNodeCore for MaterializedCte {
    fn name(&self) -> &str {
        "MaterializedCte"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.cte, &self.continuation]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.continuation.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MaterializedCte: name={}", self.name)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq_or_internal_err!(exprs.len(), 0, "MaterializedCte takes no exprs");
        assert_eq_or_internal_err!(inputs.len(), 2, "MaterializedCte takes 2 inputs");
        let continuation = inputs.pop().unwrap();
        let cte = inputs.pop().unwrap();
        Ok(Self {
            id: self.id,
            name: self.name.clone(),
            cte,
            continuation,
        })
    }

    fn necessary_children_exprs(
        &self,
        output_columns: &[usize],
    ) -> Option<Vec<Vec<usize>>> {
        // The scans need every column of the body, since they are leaves whose
        // schema is fixed at planning time. The continuation only needs the
        // columns the parent asks for.
        let all_cte_columns = (0..self.cte.schema().fields().len()).collect();
        Some(vec![all_cte_columns, output_columns.to_vec()])
    }
}

/// Reads the buffered output of the [`MaterializedCte`] with the same `id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaterializedCteScan {
    pub id: MaterializedCteId,
    pub name: String,
    pub schema: DFSchemaRef,
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

impl UserDefinedLogicalNodeCore for MaterializedCteScan {
    fn name(&self) -> &str {
        "MaterializedCteScan"
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

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MaterializedCteScan: name={}", self.name)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq_or_internal_err!(exprs.len(), 0, "MaterializedCteScan takes no exprs");
        assert_eq_or_internal_err!(
            inputs.len(),
            0,
            "MaterializedCteScan takes no inputs"
        );
        Ok(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EmptyRelation;
    use std::sync::Arc;

    fn empty(produce_one_row: bool) -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: Arc::new(datafusion_common::DFSchema::empty()),
        })
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
}
