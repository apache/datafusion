// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::ast::{
    Expr as SQLExpr, ExprKind as SQLExprKind, Literal, ObjectName, OrderByItem,
    PipeAggregate, PipeOperator, Query, QueryDistribution, SelectInto, SetExpr,
    SetOperator, SetQuantifier, TableAlias,
};
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use datafusion_common::{Constraints, DFSchema, Result, not_impl_err};
use datafusion_expr::expr::{Sort, WildcardOptions};

use crate::ast::BuiltinExpr;
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Distinct, Expr, LogicalPlan, LogicalPlanBuilder,
};
use sqlparser::dialect::duckdb::DuckDbExpr;

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Generate a logical plan from an SQL query/subquery
    pub(crate) fn query_to_plan(
        &self,
        query: Query<'_>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Each query has its own planner context, including CTEs that are visible within that query.
        // It also inherits the CTEs from the outer query by cloning the outer planner context.
        let mut query_plan_context = outer_planner_context.clone();
        let planner_context = &mut query_plan_context;

        let Query {
            with,
            body,
            order_by,
            distribution,
            limit,
            fetch,
            locks: _,
            tail: _,
            pipes,
        } = query;

        if fetch.is_some() {
            return not_impl_err!("FETCH clause is not supported yet");
        }

        let materialized_ctes = match with {
            Some(with) => self.plan_with_clause(*with, planner_context)?,
            None => vec![],
        };

        let plan = match body {
            SetExpr::Select(mut select) => {
                let select_into = select.into.take();
                let plan = self.select_to_plan(
                    *select,
                    order_by,
                    distribution.map(|d| *d),
                    planner_context,
                )?;
                let plan = self.limit(plan, limit.map(|l| *l), planner_context)?;
                // Process the `SELECT INTO` after `LIMIT`.
                self.select_into(plan, select_into.map(|i| *i))
            }
            other => {
                check_no_distribution(distribution.as_deref())?;
                // The functions called from `set_expr_to_plan()` need more than 128KB
                // stack in debug builds as investigated in:
                // https://github.com/apache/datafusion/pull/13310#discussion_r1836813902
                let plan = crate::stack::maybe_grow(|| {
                    self.set_expr_to_plan(other, planner_context)
                })?;
                let oby_exprs =
                    to_order_by_exprs(order_by, plan.schema().fields().len())?;
                let order_by_rex = self.order_by_to_sort_expr(
                    oby_exprs,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                let plan = self.order_by(plan, order_by_rex)?;
                self.limit(plan, limit.map(|l| *l), planner_context)
            }
        }?;

        let plan = self.pipe_operators(plan, pipes, planner_context)?;
        if !materialized_ctes.is_empty() && matches!(plan, LogicalPlan::Ddl(_)) {
            return not_impl_err!("MATERIALIZED CTEs are not supported with SELECT INTO");
        }
        // A later CTE may read an earlier one, so the first CTE is the outermost.
        materialized_ctes
            .into_iter()
            .rev()
            .try_fold(plan, |plan, cte| cte.wrap(plan))
    }

    /// Apply pipe operators to a plan
    fn pipe_operators(
        &self,
        mut plan: LogicalPlan,
        pipe_operators: Vec<PipeOperator<'_>>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        for pipe_operator in pipe_operators {
            plan = self.pipe_operator(plan, pipe_operator, planner_context)?;
        }
        Ok(plan)
    }

    /// Apply a pipe operator to a plan
    fn pipe_operator(
        &self,
        plan: LogicalPlan,
        pipe_operator: PipeOperator<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match pipe_operator {
            PipeOperator::Where(expr) => {
                self.plan_selection(Some(expr), plan, planner_context)
            }
            PipeOperator::OrderBy(exprs) => {
                let sort_exprs = self.order_by_to_sort_expr(
                    exprs,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                self.order_by(plan, sort_exprs)
            }
            PipeOperator::Limit { count, offset } => {
                self.limit_exprs(plan, Some(count), offset, vec![], planner_context)
            }
            PipeOperator::Select(exprs) => {
                let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
                let select_exprs =
                    self.prepare_select_exprs(&plan, exprs, empty_from, planner_context)?;
                self.project(plan, select_exprs, None)
            }
            PipeOperator::Extend(exprs) => {
                let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
                let extend_exprs =
                    self.prepare_select_exprs(&plan, exprs, empty_from, planner_context)?;
                let all_exprs =
                    std::iter::once(SelectExpr::Wildcard(WildcardOptions::default()))
                        .chain(extend_exprs)
                        .collect();
                self.project(plan, all_exprs, None)
            }
            PipeOperator::As(alias) => self.apply_table_alias(
                plan,
                TableAlias {
                    name: alias,
                    explicit: true,
                    // Apply to all fields
                    columns: vec![],
                    column_types: vec![],
                },
            ),
            PipeOperator::SetOperation {
                op:
                    op @ (SetOperator::Union | SetOperator::Intersect | SetOperator::Except),
                quantifier,
                by_name,
                queries,
            } => self.pipe_operator_set(
                plan,
                op,
                quantifier,
                by_name,
                queries,
                planner_context,
            ),
            PipeOperator::Aggregate {
                aggregates,
                group_by,
            } => {
                self.pipe_operator_aggregate(plan, aggregates, group_by, planner_context)
            }
            PipeOperator::Join(join) => {
                self.parse_relation_join(plan, *join, planner_context)
            }

            x => not_impl_err!("`{x}` pipe operator is not supported yet"),
        }
    }

    /// Handle Union/Intersect/Except pipe operators
    fn pipe_operator_set(
        &self,
        mut plan: LogicalPlan,
        set_operator: SetOperator,
        set_quantifier: SetQuantifier,
        by_name: bool,
        queries: Vec<Query<'_>>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        for query in queries {
            let right_plan = self.query_to_plan(query, planner_context)?;
            plan = self.set_operation_to_plan(
                set_operator,
                plan,
                right_plan,
                set_quantifier,
                by_name,
            )?;
        }

        Ok(plan)
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        limit: Option<crate::ast::Limit<'_>>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let Some(limit) = limit else {
            return Ok(input);
        };
        self.limit_exprs(input, limit.count, limit.offset, limit.by, planner_context)
    }

    /// Wrap a plan in a limit with the given count, offset and `LIMIT BY` exprs
    fn limit_exprs(
        &self,
        input: LogicalPlan,
        count: Option<SQLExpr<'_>>,
        offset: Option<SQLExpr<'_>>,
        limit_by: Vec<SQLExpr<'_>>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let empty_schema = DFSchema::empty();

        let skip = offset
            .map(|o| self.sql_to_expr(o, &empty_schema, planner_context))
            .transpose()?;

        let fetch = count
            .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
            .transpose()?;

        let limit_by_exprs = limit_by
            .into_iter()
            .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
            .collect::<Result<Vec<_>>>()?;

        if !limit_by_exprs.is_empty() {
            return not_impl_err!("LIMIT BY clause is not supported yet");
        }

        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }

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

    /// Handle AGGREGATE pipe operator
    fn pipe_operator_aggregate(
        &self,
        plan: LogicalPlan,
        full_table_exprs: Vec<PipeAggregate<'_>>,
        group_by_expr: Vec<PipeAggregate<'_>>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let plan_schema = plan.schema();
        let process_expr =
            |pipe_aggregate: PipeAggregate<'_>, planner_context: &mut PlannerContext| {
                let expr_with_alias = pipe_aggregate.expr;
                let sql_expr = expr_with_alias.expr;
                let alias = expr_with_alias.alias;

                let df_expr = self.sql_to_expr(sql_expr, plan_schema, planner_context)?;

                match alias {
                    Some(alias_ident) => {
                        df_expr.alias_if_changed(alias_ident.value().into_owned())
                    }
                    None => Ok(df_expr),
                }
            };

        let aggr_exprs: Vec<Expr> = full_table_exprs
            .into_iter()
            .map(|e| process_expr(e, planner_context))
            .collect::<Result<Vec<_>>>()?;

        let group_by_exprs: Vec<Expr> = group_by_expr
            .into_iter()
            .map(|e| process_expr(e, planner_context))
            .collect::<Result<Vec<_>>>()?;

        LogicalPlanBuilder::from(plan)
            .aggregate(group_by_exprs, aggr_exprs)?
            .build()
    }

    /// Wrap the logical plan in a `SelectInto`
    fn select_into(
        &self,
        plan: LogicalPlan,
        select_into: Option<SelectInto<'_>>,
    ) -> Result<LogicalPlan> {
        match select_into {
            Some(into) => {
                let mut targets = into.targets;
                let name = match targets.as_mut_slice() {
                    [target] => match std::mem::replace(
                        &mut target.kind,
                        SQLExprKind::Literal(Literal::Null),
                    ) {
                        SQLExprKind::Ident(ident) => ObjectName { parts: vec![ident] },
                        SQLExprKind::Compound(name) => name,
                        _ => {
                            return not_impl_err!(
                                "SELECT INTO only supports a single table name target"
                            );
                        }
                    },
                    _ => {
                        return not_impl_err!(
                            "SELECT INTO only supports a single table name target"
                        );
                    }
                };
                Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                    CreateMemoryTable {
                        name: self.object_name_to_table_reference(name)?,
                        constraints: Constraints::default(),
                        input: Arc::new(plan),
                        if_not_exists: false,
                        or_replace: false,
                        temporary: false,
                        column_defaults: vec![],
                    },
                )))
            }
            _ => Ok(plan),
        }
    }
}

/// Returns an error for Spark's `CLUSTER BY` / `DISTRIBUTE BY` / `SORT BY`
/// on a query whose body is not a plain `SELECT`.
fn check_no_distribution(distribution: Option<&QueryDistribution<'_>>) -> Result<()> {
    match distribution {
        Some(d) if !d.cluster_by.is_empty() => not_impl_err!("CLUSTER BY"),
        Some(d) if !d.sort_by.is_empty() => not_impl_err!("SORT BY"),
        Some(d) if !d.distribute_by.is_empty() => {
            not_impl_err!("DISTRIBUTE BY is only supported on a SELECT")
        }
        _ => Ok(()),
    }
}

/// True if `item` is DuckDB's `ALL` in `ORDER BY ALL`.
fn is_order_by_all(item: &OrderByItem<'_>) -> bool {
    matches!(
        item.expr.kind,
        SQLExprKind::Ext(BuiltinExpr::DuckDb(DuckDbExpr::AllColumns))
    )
}

/// Returns the order by expressions from the query.
fn to_order_by_exprs(
    order_by: Vec<OrderByItem<'_>>,
    order_by_all_column_count: usize,
) -> Result<Vec<OrderByItem<'_>>> {
    if order_by.iter().any(|item| item.ext.interpolate.is_some()) {
        return not_impl_err!("ORDER BY INTERPOLATE is not supported");
    }
    match order_by.as_slice() {
        [item] if is_order_by_all(item) => {
            let item = order_by.into_iter().next().expect("one item");
            Ok((1..=order_by_all_column_count)
                .map(|position| OrderByItem {
                    expr: SQLExprKind::Literal(Literal::Number(
                        position.to_string().into(),
                    ))
                    .into(),
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                    using: None,
                    ext: Default::default(),
                })
                .collect())
        }
        _ => Ok(order_by),
    }
}

/// Returns the order by expressions from the query with the select expressions.
pub(crate) fn to_order_by_exprs_with_select<'s>(
    order_by: Vec<OrderByItem<'s>>,
    select_exprs: Option<&Vec<Expr>>,
) -> Result<Vec<OrderByItem<'s>>> {
    let Some(select_exprs) = select_exprs else {
        return to_order_by_exprs(order_by, 0);
    };
    to_order_by_exprs(order_by, select_exprs.len())
}
