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

//! Logical plan nodes for materialized CTEs.

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_common::{DFSchema, DFSchemaRef, Result};

fn get_all_columns_from_schema(schema: &DFSchema) -> HashSet<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// A logical plan node that materializes a CTE and makes it available
/// to a continuation plan. The CTE is executed once, its results cached,
/// and any `MaterializedCteReader` nodes in the continuation plan read
/// from that cache.
#[derive(Debug, Clone)]
pub struct MaterializedCteProducer {
    /// Name of the CTE being materialized
    pub name: String,
    /// The plan that computes the CTE
    pub cte_plan: Arc<LogicalPlan>,
    /// The plan that uses the materialized CTE (continuation)
    pub continuation: Arc<LogicalPlan>,
    /// The output schema (same as continuation's schema)
    pub schema: DFSchemaRef,
}

impl PartialEq for MaterializedCteProducer {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.cte_plan == other.cte_plan
            && self.continuation == other.continuation
    }
}

impl Eq for MaterializedCteProducer {}

impl PartialOrd for MaterializedCteProducer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Hash for MaterializedCteProducer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.cte_plan.hash(state);
        self.continuation.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for MaterializedCteProducer {
    fn name(&self) -> &str {
        "MaterializedCteProducer"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.cte_plan.as_ref(), self.continuation.as_ref()]
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
        write!(f, "MaterializedCteProducer: name={}", self.name)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 2);
        let continuation = &inputs[1];
        Ok(Self {
            name: self.name.clone(),
            cte_plan: Arc::new(inputs[0].clone()),
            continuation: Arc::new(continuation.clone()),
            schema: Arc::clone(continuation.schema()),
        })
    }
}

/// A logical plan node that reads from a previously materialized CTE cache.
/// This is a leaf node (no inputs) that will be wired to the cache at
/// physical planning time.
#[derive(Debug, Clone)]
pub struct MaterializedCteReader {
    /// Name of the CTE to read from
    pub name: String,
    /// The schema of the CTE output
    pub schema: DFSchemaRef,
}

impl PartialEq for MaterializedCteReader {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.schema == other.schema
    }
}

impl Eq for MaterializedCteReader {}

impl PartialOrd for MaterializedCteReader {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Hash for MaterializedCteReader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.schema.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for MaterializedCteReader {
    fn name(&self) -> &str {
        "MaterializedCteReader"
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
        write!(f, "MaterializedCteReader: name={}", self.name)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
        })
    }
}
