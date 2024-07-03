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

use datafusion_common::{plan_err, Constraints, Result, ScalarValue};
use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Distinct, Expr, LogicalPlan, LogicalPlanBuilder,
    Operator,
};
use sqlparser::ast::{
    Expr as SQLExpr, Offset as SQLOffset, Query, SelectInto, SetExpr, Value,
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
                let plan =
                    self.select_to_plan(*select, query.order_by, planner_context)?;
                let plan = self.limit(plan, query.offset, query.limit)?;
                // Process the `SELECT INTO` after `LIMIT`.
                self.select_into(plan, select_into)
            }
            other => {
                let plan = self.set_expr_to_plan(other, planner_context)?;
                let order_by_rex = self.order_by_to_sort_expr(
                    &query.order_by,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                let plan = self.order_by(plan, order_by_rex)?;
                self.limit(plan, query.offset, query.limit)
            }
        }
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
            Some(skip_expr) => {
                let expr = self.sql_to_expr(
                    skip_expr.value,
                    input.schema(),
                    &mut PlannerContext::new(),
                )?;
                let n = get_constant_result(&expr, "OFFSET")?;
                convert_usize_with_check(n, "OFFSET")
            }
            _ => Ok(0),
        }?;

        let fetch = match fetch {
            Some(limit_expr)
                if limit_expr != sqlparser::ast::Expr::Value(Value::Null) =>
            {
                let expr = self.sql_to_expr(
                    limit_expr,
                    input.schema(),
                    &mut PlannerContext::new(),
                )?;
                let n = get_constant_result(&expr, "LIMIT")?;
                Some(convert_usize_with_check(n, "LIMIT")?)
            }
            _ => None,
        };

        LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
    }

    /// Wrap the logical in a sort
    pub(super) fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<Expr>,
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
                    column_defaults: vec![],
                },
            ))),
            _ => Ok(plan),
        }
    }
}

/// Retrieves the constant result of an expression, evaluating it if possible.
///
/// This function takes an expression and an argument name as input and returns
/// a `Result<i64>` indicating either the constant result of the expression or an
/// error if the expression cannot be evaluated.
///
/// # Arguments
///
/// * `expr` - An `Expr` representing the expression to evaluate.
/// * `arg_name` - The name of the argument for error messages.
///
/// # Returns
///
/// * `Result<i64>` - An `Ok` variant containing the constant result if evaluation is successful,
///   or an `Err` variant containing an error message if evaluation fails.
///
/// <https://github.com/apache/datafusion/issues/9821> tracks a more general solution
fn get_constant_result(expr: &Expr, arg_name: &str) -> Result<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(s))) => Ok(*s),
        Expr::BinaryExpr(binary_expr) => {
            let lhs = get_constant_result(&binary_expr.left, arg_name)?;
            let rhs = get_constant_result(&binary_expr.right, arg_name)?;
            let res = match binary_expr.op {
                Operator::Plus => lhs + rhs,
                Operator::Minus => lhs - rhs,
                Operator::Multiply => lhs * rhs,
                _ => return plan_err!("Unsupported operator for {arg_name} clause"),
            };
            Ok(res)
        }
        _ => plan_err!("Unexpected expression in {arg_name} clause"),
    }
}

/// Converts an `i64` to `usize`, performing a boundary check.
fn convert_usize_with_check(n: i64, arg_name: &str) -> Result<usize> {
    if n < 0 {
        plan_err!("{arg_name} must be >= 0, '{n}' was provided.")
    } else {
        Ok(n as usize)
    }
}
