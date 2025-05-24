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

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{DFSchema, DFSchemaRef, HashMap, Result};

use crate::LogicalPlan;

/// Struct to store the states used by the Planner. The Planner will leverage the states
/// to resolve CTEs, Views, subqueries and PREPARE statements. The states include
/// Common Table Expression (CTE) provided with WITH clause and
/// Parameter Data Types provided with PREPARE statement and the query schema of the
/// outer query plan.
///
/// # Cloning
///
/// Only the `ctes` are truly cloned when the `PlannerContext` is cloned.
/// This helps resolve scoping issues of CTEs.
/// By using cloning, a subquery can inherit CTEs from the outer query
/// and can also define its own private CTEs without affecting the outer query.
///
#[derive(Debug, Clone)]
pub struct PlannerContext {
    /// Data types for numbered parameters ($1, $2, etc), if supplied
    /// in `PREPARE` statement
    prepare_param_data_types: Arc<Vec<DataType>>,
    /// Map of CTE name to logical plan of the WITH clause.
    /// Use `Arc<LogicalPlan>` to allow cheap cloning
    ctes: HashMap<String, Arc<LogicalPlan>>,
    /// The joined schemas of all FROM clauses planned so far. When planning LATERAL
    /// FROM clauses, this should become a suffix of the `outer_query_schema`.
    outer_from_schema: Option<DFSchemaRef>,
    /// The query schema defined by the table
    create_table_schema: Option<DFSchemaRef>,

    // TODO: take outer_from_schema and create_table_schema into consideration.
    /// All levels query schema of the outer query plan, used to resolve the columns in subquery.
    /// Use `depth` to index different level outer_query_schema.
    /// For example:
    /// SELECT name <---------------------------------------- depth = 0
    /// FROM employees e
    /// WHERE salary > (
    ///     SELECT AVG(salary) <----------------------------- depth = 1
    ///     FROM employees e2
    ///     WHERE e2.department_id = e.department_id
    ///       AND e.department_id IN (
    ///           SELECT department_id <--------------------- depth = 2
    ///           FROM employees
    ///           GROUP BY department_id
    ///           HAVING AVG(salary) > (
    ///               SELECT AVG(salary)
    ///               FROM employees
    ///           )
    ///       )
    /// );
    outer_query_schemas: Vec<Option<DFSchemaRef>>,
    /// Current depth of query, starting from 0.
    cur_depth: usize,
}

impl Default for PlannerContext {
    fn default() -> Self {
        Self::new()
    }
}

impl PlannerContext {
    /// Create an empty PlannerContext
    pub fn new() -> Self {
        Self {
            prepare_param_data_types: Arc::new(vec![]),
            ctes: HashMap::new(),
            outer_from_schema: None,
            create_table_schema: None,
            outer_query_schemas: vec![None], // depth 0 has no outer query schema
            cur_depth: 0,
        }
    }

    /// Update the PlannerContext with provided prepare_param_data_types
    pub fn with_prepare_param_data_types(
        mut self,
        prepare_param_data_types: Vec<DataType>,
    ) -> Self {
        self.prepare_param_data_types = prepare_param_data_types.into();
        self
    }

    // TODO: replace all places with outer_query_schemas()
    // Return a reference to the outer query's schema
    pub fn outer_query_schema(&self) -> Option<&DFSchema> {
        self.outer_query_schemas[self.cur_depth]
            .as_ref()
            .map(|s| s.as_ref())
    }

    pub fn outer_query_schemas(&self) -> &[Option<DFSchemaRef>] {
        &self.outer_query_schemas
    }

    /// Returns an iterator over the outer query schemas from back to front,
    /// along with their indices.
    pub fn iter_outer_query_schemas_rev(
        &self,
    ) -> impl Iterator<Item = (usize, Option<&DFSchema>)> {
        self.outer_query_schemas
            .iter()
            .enumerate()
            .rev()
            .map(|(i, schema_ref)| (i, schema_ref.as_ref().map(|s| s.as_ref())))
    }

    /// Sets the outer query schema, returning the existing one, if
    /// any
    pub fn push_outer_query_schema(&mut self, schema: Option<DFSchemaRef>) {
        self.outer_query_schemas.push(schema);
    }

    pub fn increase_depth(&mut self) {
        self.cur_depth += 1;
    }

    pub fn decrease_depth(&mut self) {
        self.cur_depth -= 1;
    }

    pub fn cur_depth(&self) -> usize {
        self.cur_depth
    }

    pub fn set_table_schema(
        &mut self,
        mut schema: Option<DFSchemaRef>,
    ) -> Option<DFSchemaRef> {
        std::mem::swap(&mut self.create_table_schema, &mut schema);
        schema
    }

    pub fn table_schema(&self) -> Option<DFSchemaRef> {
        self.create_table_schema.clone()
    }

    // Return a clone of the outer FROM schema
    pub fn outer_from_schema(&self) -> Option<Arc<DFSchema>> {
        self.outer_from_schema.clone()
    }

    /// Sets the outer FROM schema, returning the existing one, if any
    pub fn set_outer_from_schema(
        &mut self,
        mut schema: Option<DFSchemaRef>,
    ) -> Option<DFSchemaRef> {
        std::mem::swap(&mut self.outer_from_schema, &mut schema);
        schema
    }

    /// Extends the FROM schema, returning the existing one, if any
    pub fn extend_outer_from_schema(&mut self, schema: &DFSchemaRef) -> Result<()> {
        match self.outer_from_schema.as_mut() {
            Some(from_schema) => Arc::make_mut(from_schema).merge(schema),
            None => self.outer_from_schema = Some(Arc::clone(schema)),
        };
        Ok(())
    }

    /// Return the types of parameters (`$1`, `$2`, etc) if known
    pub fn prepare_param_data_types(&self) -> &[DataType] {
        &self.prepare_param_data_types
    }

    /// Returns true if there is a Common Table Expression (CTE) /
    /// Subquery for the specified name
    pub fn contains_cte(&self, cte_name: &str) -> bool {
        self.ctes.contains_key(cte_name)
    }

    /// Inserts a LogicalPlan for the Common Table Expression (CTE) /
    /// Subquery for the specified name
    pub fn insert_cte(&mut self, cte_name: impl Into<String>, plan: LogicalPlan) {
        let cte_name = cte_name.into();
        self.ctes.insert(cte_name, Arc::new(plan));
    }

    /// Return a plan for the Common Table Expression (CTE) / Subquery for the
    /// specified name
    pub fn get_cte(&self, cte_name: &str) -> Option<&LogicalPlan> {
        self.ctes.get(cte_name).map(|cte| cte.as_ref())
    }

    /// Remove the plan of CTE / Subquery for the specified name
    pub fn remove_cte(&mut self, cte_name: &str) {
        self.ctes.remove(cte_name);
    }
}
