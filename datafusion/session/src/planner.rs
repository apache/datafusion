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

//! Query planner interfaces.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::{DFSchema, Result, not_impl_err};
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Expr, LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};

use crate::Session;

/// A planner that creates a physical plan for a query.
#[async_trait]
pub trait QueryPlanner: Any + Debug {
    /// Create an [`ExecutionPlan`] from a [`LogicalPlan`].
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// A query planner that reports that planning is not implemented.
///
/// [`Session`] implementations that do not expose a query planner can return
/// this planner explicitly.
#[derive(Debug, Default)]
pub struct UnsupportedQueryPlanner;

#[async_trait]
impl QueryPlanner for UnsupportedQueryPlanner {
    async fn create_physical_plan(
        &self,
        _logical_plan: &LogicalPlan,
        _session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("This session does not expose its query planner")
    }
}

/// Physical query planner that converts a [`LogicalPlan`] to an
/// [`ExecutionPlan`] suitable for execution.
#[async_trait]
pub trait PhysicalPlanner: Send + Sync {
    /// Create a physical plan from a logical plan.
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a physical expression from a logical expression.
    ///
    /// `planning_ctx` resolves scalar subqueries for the plan subtree being
    /// converted. Callers outside plan conversion should pass
    /// `&PhysicalPlanningContext::default()`.
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// Plans user-defined logical nodes and table sources.
#[async_trait]
pub trait ExtensionPlanner {
    /// Create a physical plan for a [`UserDefinedLogicalNode`].
    ///
    /// Return `Ok(None)` when this planner does not support `node`.
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;

    /// Create a physical plan for a [`TableScan`].
    ///
    /// Return `Ok(None)` when this planner does not support `scan`.
    async fn plan_table_scan(
        &self,
        _planner: &dyn PhysicalPlanner,
        _scan: &TableScan,
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}
