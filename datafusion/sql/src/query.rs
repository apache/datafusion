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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use datafusion_common::{not_impl_err, Constraints, DFSchema, Result};
use datafusion_expr::expr::Sort;
use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Distinct, LogicalPlan, LogicalPlanBuilder,
};
use sqlparser::ast::{
    Expr as SQLExpr, Offset as SQLOffset, OrderBy, OrderByExpr, Query, SelectInto,
    SetExpr,
};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Generate a logical plan from an SQL query/subquery
    pub(crate) fn query_to_plan(
        &self,
        query: Query,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Each query has its own planner context, including CTEs that are visible within that query.
        // It also inherits the CTEs from the outer query by cloning the outer planner context.
        let mut query_plan_context = outer_planner_context.clone();
        let planner_context = &mut query_plan_context;

        if let Some(with) = query.with {
            self.plan_with_clause(with, planner_context)?;
        }

        let set_expr = *query.body;
        match set_expr {
            SetExpr::Select(mut select) => {
                let select_into = select.into.take();
                // Order-by expressions may refer to columns in the `FROM` clause,
                // so we need to process `SELECT` and `ORDER BY` together.
                let oby_exprs = to_order_by_exprs(query.order_by)?;
                let plan = self.select_to_plan(*select, oby_exprs, planner_context)?;
                let plan =
                    self.limit(plan, query.offset, query.limit, planner_context)?;
                // Process the `SELECT INTO` after `LIMIT`.
                self.select_into(plan, select_into)
            }
            other => {
                let plan = self.set_expr_to_plan(other, planner_context)?;
                let oby_exprs = to_order_by_exprs(query.order_by)?;
                let order_by_rex = self.order_by_to_sort_expr(
                    oby_exprs,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                let plan = self.order_by(plan, order_by_rex)?;
                self.limit(plan, query.offset, query.limit, planner_context)
            }
        }
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        skip: Option<SQLOffset>,
        fetch: Option<SQLExpr>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }

        // skip and fetch expressions are not allowed to reference columns from the input plan
        let empty_schema = DFSchema::empty();

        let skip = skip
            .map(|o| self.sql_to_expr(o.value, &empty_schema, planner_context))
            .transpose()?;
        let fetch = fetch
            .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
            .transpose()?;
        LogicalPlanBuilder::from(input)
            .limit_by_expr(skip, fetch)?
            .build()
    }

    /// Wrap the logical in a sort
    pub(super) fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<Sort>,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        if let LogicalPlan::Distinct(Distinct::On(ref distinct_on)) = plan {
            // In case of `DISTINCT ON` we must capture the sort expressions since during the plan
            // optimization we're effectively doing a `first_value` aggregation according to them.
            let distinct_on = distinct_on.clone().with_sort_expr(order_by)?;
            Ok(LogicalPlan::Distinct(Distinct::On(distinct_on)))
        } else {
            LogicalPlanBuilder::from(plan).sort(order_by)?.build()
        }
    }

    /// Wrap the logical plan in a `SelectInto`
    fn select_into(
        &self,
        plan: LogicalPlan,
        select_into: Option<SelectInto>,
    ) -> Result<LogicalPlan> {
        match select_into {
            Some(into) => Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                CreateMemoryTable {
                    name: self.object_name_to_table_reference(into.name)?,
                    constraints: Constraints::empty(),
                    input: Arc::new(plan),
                    if_not_exists: false,
                    or_replace: false,
                    temporary: false,
                    column_defaults: vec![],
                },
            ))),
            _ => Ok(plan),
        }
    }
}

/// Returns the order by expressions from the query.
fn to_order_by_exprs(order_by: Option<OrderBy>) -> Result<Vec<OrderByExpr>> {
    let Some(OrderBy { exprs, interpolate }) = order_by else {
        // If no order by, return an empty array.
        return Ok(vec![]);
    };
    if let Some(_interpolate) = interpolate {
        return not_impl_err!("ORDER BY INTERPOLATE is not supported");
    }
    Ok(exprs)
}
