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

use datafusion_common::{
    not_impl_err, plan_err, sql_err, Constraints, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Distinct, Expr, LogicalPlan, LogicalPlanBuilder,
};
use sqlparser::ast::{
    Expr as SQLExpr, Offset as SQLOffset, OrderByExpr, Query, SetExpr, Value,
};

use sqlparser::parser::ParserError::ParserError;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Generate a logical plan from an SQL query
    pub(crate) fn query_to_plan(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.query_to_plan_with_schema(query, planner_context)
    }

    /// Generate a logic plan from an SQL query.
    /// It's implementation of `subquery_to_plan` and `query_to_plan`.
    /// It shouldn't be invoked directly.
    fn query_to_plan_with_schema(
        &self,
        query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let set_expr = query.body;
        if let Some(with) = query.with {
            // Process CTEs from top to bottom
            // do not allow self-references
            if with.recursive {
                return not_impl_err!("Recursive CTEs are not supported");
            }

            for cte in with.cte_tables {
                // A `WITH` block can't use the same name more than once
                let cte_name = self.normalizer.normalize(cte.alias.name.clone());
                if planner_context.contains_cte(&cte_name) {
                    return sql_err!(ParserError(format!(
                        "WITH query name {cte_name:?} specified more than once"
                    )));
                }
                // create logical plan & pass backreferencing CTEs
                // CTE expr don't need extend outer_query_schema
                let logical_plan =
                    self.query_to_plan(*cte.query, &mut planner_context.clone())?;

                // Each `WITH` block can change the column names in the last
                // projection (e.g. "WITH table(t1, t2) AS SELECT 1, 2").
                let logical_plan = self.apply_table_alias(logical_plan, cte.alias)?;

                planner_context.insert_cte(cte_name, logical_plan);
            }
        }
        let plan = self.set_expr_to_plan(*(set_expr.clone()), planner_context)?;
        let plan = self.order_by(plan, query.order_by, planner_context)?;
        let plan = self.limit(plan, query.offset, query.limit)?;

        let plan = match *set_expr {
            SetExpr::Select(select) if select.into.is_some() => {
                let select_into = select.into.unwrap();
                LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                    name: self.object_name_to_table_reference(select_into.name)?,
                    constraints: Constraints::empty(),
                    input: Arc::new(plan),
                    if_not_exists: false,
                    or_replace: false,
                    column_defaults: vec![],
                }))
            }
            _ => plan,
        };

        Ok(plan)
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        skip: Option<SQLOffset>,
        fetch: Option<SQLExpr>,
    ) -> Result<LogicalPlan> {
        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }

        let skip = match skip {
            Some(skip_expr) => match self.sql_to_expr(
                skip_expr.value,
                input.schema(),
                &mut PlannerContext::new(),
            )? {
                Expr::Literal(ScalarValue::Int64(Some(s))) => {
                    if s < 0 {
                        return plan_err!("Offset must be >= 0, '{s}' was provided.");
                    }
                    Ok(s as usize)
                }
                _ => plan_err!("Unexpected expression in OFFSET clause"),
            }?,
            _ => 0,
        };

        let fetch = match fetch {
            Some(limit_expr)
                if limit_expr != sqlparser::ast::Expr::Value(Value::Null) =>
            {
                let n = match self.sql_to_expr(
                    limit_expr,
                    input.schema(),
                    &mut PlannerContext::new(),
                )? {
                    Expr::Literal(ScalarValue::Int64(Some(n))) if n >= 0 => {
                        Ok(n as usize)
                    }
                    _ => plan_err!("LIMIT must not be negative"),
                }?;
                Some(n)
            }
            _ => None,
        };

        LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
    }

    /// Wrap the logical in a sort
    fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<OrderByExpr>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        let order_by_rex =
            self.order_by_to_sort_expr(&order_by, plan.schema(), planner_context)?;

        if let LogicalPlan::Distinct(Distinct::On(ref distinct_on)) = plan {
            // In case of `DISTINCT ON` we must capture the sort expressions since during the plan
            // optimization we're effectively doing a `first_value` aggregation according to them.
            let distinct_on = distinct_on.clone().with_sort_expr(order_by_rex)?;
            Ok(LogicalPlan::Distinct(Distinct::On(distinct_on)))
        } else {
            LogicalPlanBuilder::from(plan).sort(order_by_rex)?.build()
        }
    }
}
