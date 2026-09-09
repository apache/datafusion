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

use datafusion_common::{DFSchema, Result, not_impl_err};
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Expr, LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use futures::future::BoxFuture;

use crate::Session;

/// A planner that creates a physical plan for a query.
pub trait QueryPlanner: Any + Debug {
    /// Given a [`LogicalPlan`], create an [`ExecutionPlan`] suitable for execution
    fn create_physical_plan<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session: &'a dyn Session,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>>;
}

/// A query planner that reports that planning is not implemented.
///
/// [`Session`] implementations that do not expose a query planner can return
/// this planner explicitly.
#[derive(Debug, Default)]
pub struct UnsupportedQueryPlanner;

impl QueryPlanner for UnsupportedQueryPlanner {
    fn create_physical_plan<'a>(
        &'a self,
        _logical_plan: &'a LogicalPlan,
        _session: &'a dyn Session,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(async {
            not_impl_err!("This session does not expose its query planner")
        })
    }
}

/// Physical query planner that converts a [`LogicalPlan`] to an
/// [`ExecutionPlan`] suitable for execution.
pub trait PhysicalPlanner: Send + Sync {
    /// Create a physical plan from a logical plan
    fn create_physical_plan<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session: &'a dyn Session,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>>;

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `expr`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `expr`
    ///
    /// `planning_ctx`: the [`PhysicalPlanningContext`] used to resolve
    /// `Expr::ScalarSubquery` nodes. During physical planning the planner
    /// threads the context of the plan currently being converted to a physical
    /// plan (for example into [`ExtensionPlanner::plan_extension`], which
    /// should forward it here). Callers creating physical expressions outside
    /// of a plan should pass `&PhysicalPlanningContext::default()`.
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// This trait exposes the ability to plan an [`ExecutionPlan`] out of a [`LogicalPlan`].
pub trait ExtensionPlanner {
    /// Create a physical plan for a [`UserDefinedLogicalNode`].
    ///
    /// `input_dfschema`: the logical plan schema for the inputs to this node
    ///
    /// Returns an error when the planner knows how to plan the concrete
    /// implementation of `node` but errors while doing so.
    ///
    /// Returns `None` when the planner does not know how to plan the
    /// `node` and wants to delegate the planning to another
    /// [`ExtensionPlanner`].
    ///
    /// `planning_ctx` is the [`PhysicalPlanningContext`] of the plan subtree
    /// currently being converted to a physical plan. Forward it to
    /// [`PhysicalPlanner::create_physical_expr`] when creating this node's
    /// physical expressions so that scalar subqueries resolve against the same
    /// subquery state as the rest of the plan.
    fn plan_extension<'a>(
        &'a self,
        planner: &'a dyn PhysicalPlanner,
        node: &'a dyn UserDefinedLogicalNode,
        logical_inputs: &'a [&'a LogicalPlan],
        physical_inputs: &'a [Arc<dyn ExecutionPlan>],
        session: &'a dyn Session,
        planning_ctx: &'a PhysicalPlanningContext,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn ExecutionPlan>>>>;

    /// Create a physical plan for a [`LogicalPlan::TableScan`].
    ///
    /// This is useful for planning valid [`TableSource`]s that are not `TableProvider`s.
    ///
    /// Returns:
    /// * `Ok(Some(plan))` if the planner knows how to plan the `scan`
    /// * `Ok(None)` if the planner does not know how to plan the `scan` and wants to delegate the planning to another [`ExtensionPlanner`]
    /// * `Err` if the planner knows how to plan the `scan` but errors while doing so
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use datafusion::physical_plan::ExecutionPlan;
    /// use datafusion::logical_expr::TableScan;
    /// use datafusion::catalog::Session;
    /// use datafusion::error::Result;
    /// use datafusion_session::{ExtensionPlanner, PhysicalPlanner};
    ///
    /// // Your custom table source type
    /// struct MyCustomTableSource { /* ... */ }
    ///
    /// // Your custom execution plan
    /// struct MyCustomExec { /* ... */ }
    ///
    /// struct MyExtensionPlanner;
    ///
    /// impl ExtensionPlanner for MyExtensionPlanner {
    ///     fn plan_extension<'a>(
    ///         &'a self,
    ///         _planner: &'a dyn PhysicalPlanner,
    ///         _node: &'a dyn UserDefinedLogicalNode,
    ///         _logical_inputs: &'a [&'a LogicalPlan],
    ///         _physical_inputs: &'a [Arc<dyn ExecutionPlan>],
    ///         _session: &'a dyn Session,
    ///         _planning_ctx: &'a PhysicalPlanningContext,
    ///     ) -> BoxFuture<'a, Result<Option<Arc<dyn ExecutionPlan>>>> {
    ///         Box::pin(async { Ok(None) })
    ///     }
    ///
    ///     fn plan_table_scan<'a>(
    ///         &'a self,
    ///         _planner: &'a dyn PhysicalPlanner,
    ///         scan: &'a TableScan,
    ///         _session: &'a dyn Session,
    ///         _planning_ctx: &'a PhysicalPlanningContext,
    ///     ) -> BoxFuture<'a, Result<Option<Arc<dyn ExecutionPlan>>>> {
    ///         Box::pin(async move {
    ///             // Check if this is your custom table source
    ///             if scan.source.is::<MyCustomTableSource>() {
    ///                 // Create a custom execution plan for your table source
    ///                 let exec = MyCustomExec::new(
    ///                     scan.table_name.clone(),
    ///                     Arc::clone(scan.projected_schema.inner()),
    ///                 );
    ///                 Ok(Some(Arc::new(exec)))
    ///             } else {
    ///                 // Return None to let other extension planners handle it
    ///                 Ok(None)
    ///             }
    ///         })
    ///     }
    /// }
    /// ```
    ///
    /// [`TableSource`]: datafusion_expr::TableSource
    fn plan_table_scan<'a>(
        &'a self,
        _planner: &'a dyn PhysicalPlanner,
        _scan: &'a TableScan,
        _session: &'a dyn Session,
        _planning_ctx: &'a PhysicalPlanningContext,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn ExecutionPlan>>>> {
        Box::pin(async { Ok(None) })
    }
}
