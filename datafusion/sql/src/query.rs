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

use crate::stack::StackGuard;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Constraints, DFSchema, DFSchemaRef, Result, not_impl_err};
use datafusion_expr::expr::{Sort, WildcardOptions};
use datafusion_expr::logical_plan::{
    Extension, MaterializedCteProducer, MaterializedCteReader,
};

use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Distinct, Expr, LogicalPlan, LogicalPlanBuilder,
};
use sqlparser::ast::{
    Expr as SQLExpr, ExprWithAliasAndOrderBy, Ident, LimitClause, Offset, OffsetRows,
    OrderBy, OrderByExpr, OrderByKind, PipeOperator, Query, SelectInto, SetExpr,
    SetOperator, SetQuantifier, TableAlias,
};
use sqlparser::tokenizer::Span;

impl<S: ContextProvider> SqlToRel<'_, S> {
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

        let Query {
            with,
            body,
            order_by,
            limit_clause,
            fetch,
            locks: _,
            for_clause: _,
            settings: _,
            format_clause: _,
            pipe_operators,
        } = query;

        if fetch.is_some() {
            return not_impl_err!("FETCH clause is not supported yet");
        }

        let has_with = with.is_some();
        if let Some(with) = with {
            self.plan_with_clause(with, planner_context)?;
        }

        let set_expr = *body;
        let plan = match set_expr {
            SetExpr::Select(mut select) => {
                let select_into = select.into.take();
                let plan =
                    self.select_to_plan(*select, order_by.clone(), planner_context)?;
                let plan = self.limit(plan, limit_clause.clone(), planner_context)?;
                // Process the `SELECT INTO` after `LIMIT`.
                self.select_into(plan, select_into)
            }
            other => {
                // The functions called from `set_expr_to_plan()` need more than 128KB
                // stack in debug builds as investigated in:
                // https://github.com/apache/datafusion/pull/13310#discussion_r1836813902
                let plan = {
                    // scope for dropping _guard
                    let _guard = StackGuard::new(256 * 1024);
                    self.set_expr_to_plan(other, planner_context)
                }?;
                let oby_exprs = to_order_by_exprs(order_by)?;
                let order_by_rex = self.order_by_to_sort_expr(
                    oby_exprs,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                let plan = self.order_by(plan, order_by_rex)?;
                self.limit(plan, limit_clause, planner_context)
            }
        }?;

        let plan = self.pipe_operators(plan, pipe_operators, planner_context)?;

        // Apply CTE materialization if this query had a WITH clause
        if has_with {
            self.apply_cte_materialization(plan, planner_context)
        } else {
            Ok(plan)
        }
    }

    /// Apply CTE materialization to the plan.
    ///
    /// Materialize ALL multi-referenced CTEs upfront (DuckDB-style).
    ///
    /// The SQL planner wraps every multi-ref CTE in MaterializedCteProducer/Reader
    /// nodes. The `InlineCte` optimizer rule then selectively inlines ones where
    /// materialization is not beneficial (cheap CTEs, CTEs under LIMIT, etc.).
    ///
    /// This approach ensures:
    /// 1. The optimizer has full context (explicit CTE nodes in the plan)
    /// 2. The inlining decision can be revisited after other optimizer passes
    /// 3. DataFrame API users benefit via the optimizer rule
    fn apply_cte_materialization(
        &self,
        plan: LogicalPlan,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if !self
            .context_provider
            .options()
            .execution
            .enable_materialized_ctes
        {
            return Ok(plan);
        }

        let cte_names: Vec<String> = planner_context.cte_names().cloned().collect();
        let mut ctes_to_materialize: Vec<(String, LogicalPlan, bool)> = Vec::new();

        for cte_name in &cte_names {
            if planner_context.is_recursive_cte(cte_name) {
                continue;
            }
            if planner_context.is_not_materialized_cte(cte_name) {
                continue;
            }

            let ref_count = count_cte_references(&plan, cte_name);
            let force = planner_context.is_materialized_cte(cte_name);

            // Materialize multi-ref CTEs and explicitly MATERIALIZED CTEs.
            // Skip cheap CTEs (literals/empty) — not worth materializing.
            // The optimizer's InlineCte rule handles further inlining decisions.
            if (ref_count > 1 || force)
                && let Some(cte_plan) = planner_context.get_cte(cte_name)
                && (force
                    || !is_cheap_to_inline(cte_plan)
                    || plan_contains_volatile_functions(cte_plan))
            {
                ctes_to_materialize.push((cte_name.clone(), cte_plan.clone(), force));
            }
        }

        if ctes_to_materialize.is_empty() {
            return Ok(plan);
        }

        // Sort by dependency order
        ctes_to_materialize.sort_by(|(name_a, _, _), (name_b, _, _)| {
            let a_deps_on_b = planner_context
                .get_cte(name_a)
                .is_some_and(|p| plan_references_cte(p, name_b));
            let b_deps_on_a = planner_context
                .get_cte(name_b)
                .is_some_and(|p| plan_references_cte(p, name_a));
            if a_deps_on_b {
                std::cmp::Ordering::Less
            } else if b_deps_on_a {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });

        let mut result_plan = plan;
        for (cte_name, cte_plan, force) in ctes_to_materialize {
            result_plan =
                replace_cte_with_reader(result_plan, &cte_name, cte_plan.schema())?;

            let producer = MaterializedCteProducer {
                name: cte_name.clone(),
                cte_plan: Arc::new(cte_plan),
                continuation: Arc::new(result_plan.clone()),
                schema: Arc::clone(result_plan.schema()),
                force_materialized: force,
            };
            result_plan = LogicalPlan::Extension(Extension {
                node: Arc::new(producer),
            });
        }

        Ok(result_plan)
    }

    /// Apply pipe operators to a plan
    fn pipe_operators(
        &self,
        mut plan: LogicalPlan,
        pipe_operators: Vec<PipeOperator>,
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
        pipe_operator: PipeOperator,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match pipe_operator {
            PipeOperator::Where { expr } => {
                self.plan_selection(Some(expr), plan, planner_context)
            }
            PipeOperator::OrderBy { exprs } => {
                let sort_exprs = self.order_by_to_sort_expr(
                    exprs,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                self.order_by(plan, sort_exprs)
            }
            PipeOperator::Limit { expr, offset } => self.limit(
                plan,
                Some(LimitClause::LimitOffset {
                    limit: Some(expr),
                    offset: offset.map(|offset| Offset {
                        value: offset,
                        rows: OffsetRows::None,
                    }),
                    limit_by: vec![],
                }),
                planner_context,
            ),
            PipeOperator::Select { exprs } => {
                let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
                let select_exprs =
                    self.prepare_select_exprs(&plan, exprs, empty_from, planner_context)?;
                self.project(plan, select_exprs, None)
            }
            PipeOperator::Extend { exprs } => {
                let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
                let extend_exprs =
                    self.prepare_select_exprs(&plan, exprs, empty_from, planner_context)?;
                let all_exprs =
                    std::iter::once(SelectExpr::Wildcard(WildcardOptions::default()))
                        .chain(extend_exprs)
                        .collect();
                self.project(plan, all_exprs, None)
            }
            PipeOperator::As { alias } => self.apply_table_alias(
                plan,
                TableAlias {
                    name: alias,
                    // Apply to all fields
                    columns: vec![],
                    explicit: true,
                    at: None,
                },
            ),
            PipeOperator::Union {
                set_quantifier,
                queries,
            } => self.pipe_operator_set(
                plan,
                SetOperator::Union,
                set_quantifier,
                queries,
                planner_context,
            ),
            PipeOperator::Intersect {
                set_quantifier,
                queries,
            } => self.pipe_operator_set(
                plan,
                SetOperator::Intersect,
                set_quantifier,
                queries,
                planner_context,
            ),
            PipeOperator::Except {
                set_quantifier,
                queries,
            } => self.pipe_operator_set(
                plan,
                SetOperator::Except,
                set_quantifier,
                queries,
                planner_context,
            ),
            PipeOperator::Aggregate {
                full_table_exprs,
                group_by_expr,
            } => self.pipe_operator_aggregate(
                plan,
                full_table_exprs,
                group_by_expr,
                planner_context,
            ),
            PipeOperator::Join(join) => {
                self.parse_relation_join(plan, join, planner_context)
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
        queries: Vec<Query>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        for query in queries {
            let right_plan = self.query_to_plan(query, planner_context)?;
            plan = self.set_operation_to_plan(
                set_operator,
                plan,
                right_plan,
                set_quantifier,
            )?;
        }

        Ok(plan)
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        limit_clause: Option<LimitClause>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let Some(limit_clause) = limit_clause else {
            return Ok(input);
        };

        let empty_schema = DFSchema::empty();

        let (skip, fetch, limit_by_exprs) = match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                let skip = offset
                    .map(|o| self.sql_to_expr(o.value, &empty_schema, planner_context))
                    .transpose()?;

                let fetch = limit
                    .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
                    .transpose()?;

                let limit_by_exprs = limit_by
                    .into_iter()
                    .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
                    .collect::<Result<Vec<_>>>()?;

                (skip, fetch, limit_by_exprs)
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                let skip =
                    Some(self.sql_to_expr(offset, &empty_schema, planner_context)?);
                let fetch =
                    Some(self.sql_to_expr(limit, &empty_schema, planner_context)?);
                (skip, fetch, vec![])
            }
        };

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
        full_table_exprs: Vec<ExprWithAliasAndOrderBy>,
        group_by_expr: Vec<ExprWithAliasAndOrderBy>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let plan_schema = plan.schema();
        let process_expr =
            |expr_with_alias_and_order_by: ExprWithAliasAndOrderBy,
             planner_context: &mut PlannerContext| {
                let expr_with_alias = expr_with_alias_and_order_by.expr;
                let sql_expr = expr_with_alias.expr;
                let alias = expr_with_alias.alias;

                let df_expr = self.sql_to_expr(sql_expr, plan_schema, planner_context)?;

                match alias {
                    Some(alias_ident) => df_expr.alias_if_changed(alias_ident.value),
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
        select_into: Option<SelectInto>,
    ) -> Result<LogicalPlan> {
        match select_into {
            Some(into) => Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                CreateMemoryTable {
                    name: self.object_name_to_table_reference(into.name)?,
                    constraints: Constraints::default(),
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

fn plan_contains_volatile_functions(plan: &LogicalPlan) -> bool {
    let mut has_volatile = false;
    plan.apply(|node| {
        for expr in node.expressions() {
            if expr.is_volatile() {
                has_volatile = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    has_volatile
}

fn is_cheap_to_inline(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::EmptyRelation(_) => true,
        LogicalPlan::SubqueryAlias(alias) => is_cheap_to_inline(alias.input.as_ref()),
        _ => {
            let inputs = plan.inputs();
            inputs.len() == 1 && is_cheap_to_inline(inputs[0])
        }
    }
}

/// Check if a plan contains a SubqueryAlias reference to a given CTE name.
fn plan_references_cte(plan: &LogicalPlan, cte_name: &str) -> bool {
    let mut found = false;
    plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && alias.alias.table() == cte_name
        {
            found = true;
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

/// Count how many times a CTE (by SubqueryAlias name) is referenced in the plan tree.
fn count_cte_references(plan: &LogicalPlan, cte_name: &str) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if let LogicalPlan::SubqueryAlias(alias) = node
            && alias.alias.table() == cte_name
        {
            count += 1;
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

/// Replace SubqueryAlias nodes matching a CTE name with a MaterializedCteReader.
fn replace_cte_with_reader(
    plan: LogicalPlan,
    cte_name: &str,
    cte_schema: &DFSchemaRef,
) -> Result<LogicalPlan> {
    plan.transform_down(|node| {
        if let LogicalPlan::SubqueryAlias(ref alias) = node
            && alias.alias.table() == cte_name
        {
            let reader = MaterializedCteReader {
                name: cte_name.to_string(),
                schema: Arc::clone(cte_schema),
            };
            let extension = LogicalPlan::Extension(Extension {
                node: Arc::new(reader),
            });
            return Ok(datafusion_common::tree_node::Transformed::yes(extension));
        }
        Ok(datafusion_common::tree_node::Transformed::no(node))
    })
    .map(|t| t.data)
}

/// Returns the order by expressions from the query.
fn to_order_by_exprs(order_by: Option<OrderBy>) -> Result<Vec<OrderByExpr>> {
    to_order_by_exprs_with_select(order_by, None)
}

/// Returns the order by expressions from the query with the select expressions.
pub(crate) fn to_order_by_exprs_with_select(
    order_by: Option<OrderBy>,
    select_exprs: Option<&Vec<Expr>>,
) -> Result<Vec<OrderByExpr>> {
    let Some(OrderBy { kind, interpolate }) = order_by else {
        // If no order by, return an empty array.
        return Ok(vec![]);
    };
    if let Some(_interpolate) = interpolate {
        return not_impl_err!("ORDER BY INTERPOLATE is not supported");
    }
    match kind {
        OrderByKind::All(order_by_options) => {
            let Some(exprs) = select_exprs else {
                return Ok(vec![]);
            };
            let order_by_exprs = exprs
                .iter()
                .map(|select_expr| match select_expr {
                    Expr::Column(column) => Ok(OrderByExpr {
                        expr: SQLExpr::Identifier(Ident {
                            value: column.name.clone(),
                            quote_style: None,
                            span: Span::empty(),
                        }),
                        options: order_by_options,
                        with_fill: None,
                    }),
                    // TODO: Support other types of expressions
                    _ => not_impl_err!(
                        "ORDER BY ALL is not supported for non-column expressions"
                    ),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(order_by_exprs)
        }
        OrderByKind::Expressions(order_by_exprs) => Ok(order_by_exprs),
    }
}
