// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::ast::{
    Expr as SQLExpr, ExprKind, FunctionArg, Ident, ObjectName, TableFactor, TableRef,
};
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    DFSchema, Diagnostic, Result, Span, Spans, TableReference, not_impl_err, plan_err,
};
use datafusion_expr::builder::subquery_alias;
use datafusion_expr::planner::{RelationPlannerContext, RelationPlanning};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, expr::Unnest};
use datafusion_expr::{Subquery, SubqueryAlias};

mod join;

struct SqlToRelRelationContext<'a, 'b, S: ContextProvider> {
    planner: &'a SqlToRel<'b, S>,
    planner_context: &'a mut PlannerContext,
}

/// Look up a CTE using the same string key as DataFusion's built-in relation
/// planning.
fn get_cte_for_table_reference<'a>(
    planner_context: &'a PlannerContext,
    name: &TableReference,
) -> Option<&'a LogicalPlan> {
    planner_context.get_cte(&name.to_string())
}

// Implement RelationPlannerContext
impl<S: ContextProvider> RelationPlannerContext for SqlToRelRelationContext<'_, '_, S> {
    fn context_provider(&self) -> &dyn ContextProvider {
        self.planner.context_provider
    }

    fn plan(&mut self, relation: TableFactor<'_>) -> Result<LogicalPlan> {
        // The factor has no alias of its own: aliases live on the enclosing
        // `TableRef`, which the calling relation planner handles itself.
        self.planner.create_relation(
            TableRef {
                factor: relation,
                alias: None,
                span: Default::default(),
            },
            self.planner_context,
        )
    }

    fn get_cte(&self, name: &TableReference) -> Option<&LogicalPlan> {
        get_cte_for_table_reference(self.planner_context, name)
    }

    fn sql_to_expr(&mut self, expr: SQLExpr<'_>, schema: &DFSchema) -> Result<Expr> {
        self.planner.sql_to_expr(expr, schema, self.planner_context)
    }

    fn sql_expr_to_logical_expr(
        &mut self,
        expr: SQLExpr<'_>,
        schema: &DFSchema,
    ) -> Result<Expr> {
        self.planner
            .sql_expr_to_logical_expr(expr, schema, self.planner_context)
    }

    fn normalize_ident(&self, ident: Ident<'_>) -> String {
        self.planner.ident_normalizer.normalize(ident)
    }

    fn object_name_to_table_reference(
        &self,
        name: ObjectName<'_>,
    ) -> Result<TableReference> {
        self.planner.object_name_to_table_reference(name)
    }
}

/// If `call` is a call of `UNNEST`, returns its arguments.
fn as_unnest_call<'s, 'c>(call: &'c SQLExpr<'s>) -> Option<&'c [FunctionArg<'s>]> {
    match &call.kind {
        ExprKind::Function { name, args, .. }
            if name.parts.len() == 1
                && name.parts[0].quote.is_none()
                && name.parts[0].text.eq_ignore_ascii_case("unnest") =>
        {
            Some(args)
        }
        _ => None,
    }
}

/// True if `factor` is `UNNEST(...)`, with or without `LATERAL`.
pub(crate) fn is_unnest_factor(factor: &TableFactor<'_>) -> bool {
    match factor {
        TableFactor::Function { call, .. } => as_unnest_call(call).is_some(),
        TableFactor::Lateral(inner) => is_unnest_factor(inner),
        _ => false,
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Create a `LogicalPlan` that scans the named relation.
    ///
    /// First tries any registered extension planners. If no extension handles
    /// the relation, falls back to the default planner.
    ///
    /// Extension planners see only the [`TableFactor`]. The alias of the
    /// [`TableRef`] is applied after the plan they return (and after any alias
    /// they return in [`datafusion_expr::planner::PlannedRelation`]).
    fn create_relation(
        &self,
        relation: TableRef<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let TableRef {
            factor,
            alias: table_alias,
            span,
        } = relation;
        let (plan, planned_alias) =
            match self.create_extension_relation(factor, planner_context)? {
                RelationPlanning::Planned(planned) => (planned.plan, planned.alias),
                RelationPlanning::Original(original) => (
                    self.create_default_relation(*original, span, planner_context)?,
                    None,
                ),
            };

        let mut plan = optimize_subquery_sort(
            plan,
            self.context_provider
                .options()
                .sql_parser
                .enable_subquery_sort_elimination,
        )?
        .data;
        for alias in [planned_alias, table_alias].into_iter().flatten() {
            plan = self.apply_table_alias(plan, alias)?;
        }
        Ok(plan)
    }

    fn create_extension_relation<'s>(
        &self,
        relation: TableFactor<'s>,
        planner_context: &mut PlannerContext,
    ) -> Result<RelationPlanning<'s>> {
        let planners = self.context_provider.get_relation_planners();
        if planners.is_empty() {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }

        let mut current_relation = relation;
        for planner in planners.iter() {
            let mut context = SqlToRelRelationContext {
                planner: self,
                planner_context,
            };

            match planner.plan_relation(current_relation, &mut context)? {
                RelationPlanning::Planned(planned) => {
                    return Ok(RelationPlanning::Planned(planned));
                }
                RelationPlanning::Original(original) => {
                    current_relation = *original;
                }
            }
        }

        Ok(RelationPlanning::Original(Box::new(current_relation)))
    }

    /// Plans a table-valued function call such as `generate_series(1, 3)`.
    fn plan_table_function(
        &self,
        call: SQLExpr<'_>,
        lateral: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let ExprKind::Function {
            name,
            args,
            options,
        } = call.kind
        else {
            return not_impl_err!(
                "Table function {call} is not yet supported with the new SQL parser"
            );
        };
        if options.is_some() {
            return plan_err!("Unsupported table function call: {name}(...)");
        }
        if !lateral {
            let tbl_func_name = name.parts[0].to_string();
            let args = args
                .into_iter()
                .map(|arg| {
                    if let FunctionArg::Expr(expr) = arg {
                        self.sql_expr_to_logical_expr(
                            expr,
                            &DFSchema::empty(),
                            planner_context,
                        )
                    } else {
                        plan_err!("Unsupported function argument type: {}", arg)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let provider = self
                .context_provider
                .get_table_function_source(&tbl_func_name, args)?;
            LogicalPlanBuilder::scan(
                TableReference::Bare {
                    table: format!("{tbl_func_name}()").into(),
                },
                provider,
                None,
            )?
            .build()
        } else {
            let tbl_func_ref = self.object_name_to_table_reference(name)?;
            let schema = planner_context
                .outer_queries_schemas()
                .last()
                .cloned()
                .unwrap_or_else(|| Arc::new(DFSchema::empty()));
            let func_args = args
                .into_iter()
                .map(|arg| match arg {
                    FunctionArg::Expr(expr)
                    | FunctionArg::Named { value: expr, .. }
                    | FunctionArg::NamedWith { value: expr, .. } => {
                        self.sql_expr_to_logical_expr(expr, &schema, planner_context)
                    }
                    _ => plan_err!("Unsupported function argument: {arg:?}"),
                })
                .collect::<Result<Vec<Expr>>>()?;
            let provider = self
                .context_provider
                .get_table_function_source(tbl_func_ref.table(), func_args)?;
            LogicalPlanBuilder::scan(tbl_func_ref.table(), provider, None)?.build()
        }
    }

    /// Plans `UNNEST(a, b, ...)` in `FROM`.
    fn plan_unnest_relation(
        &self,
        args: Vec<FunctionArg<'_>>,
        with_ordinality: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if with_ordinality {
            return not_impl_err!("UNNEST with ordinality is not supported yet");
        }

        // Unnest table factor has empty input
        let schema = DFSchema::empty();
        let input = LogicalPlanBuilder::empty(true).build()?;
        // Unnest table factor can have multiple arguments.
        // We treat each argument as a separate unnest expression.
        let unnest_exprs = args
            .into_iter()
            .map(|arg| {
                let FunctionArg::Expr(sql_expr) = arg else {
                    return plan_err!("Unsupported UNNEST argument: {arg}");
                };
                let expr =
                    self.sql_expr_to_logical_expr(sql_expr, &schema, planner_context)?;
                Self::check_unnest_arg(&expr, &schema)?;
                Ok(Expr::Unnest(Unnest::new(expr)))
            })
            .collect::<Result<Vec<_>>>()?;
        if unnest_exprs.is_empty() {
            return plan_err!("UNNEST must have at least one argument");
        }
        self.try_process_unnest(input, unnest_exprs)
    }

    fn create_default_relation(
        &self,
        relation: TableFactor<'_>,
        relation_span: sqlparser::ast::Span,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match relation {
            TableFactor::Named(name) => {
                // Normalize name and alias
                let table_ref = self.object_name_to_table_reference(name)?;
                let cte = get_cte_for_table_reference(planner_context, &table_ref);
                match (
                    cte,
                    self.context_provider.get_table_source(table_ref.clone()),
                ) {
                    (Some(cte_plan), _) => Ok(cte_plan.clone()),
                    (_, Ok(provider)) => {
                        LogicalPlanBuilder::scan(table_ref.clone(), provider, None)?
                            .build()
                    }
                    (None, Err(e)) => {
                        let e = e.with_diagnostic(Diagnostic::new_error(
                            format!("table '{table_ref}' not found"),
                            Span::try_from_sqlparser_span(relation_span),
                        ));
                        Err(e)
                    }
                }
            }
            TableFactor::Function {
                call,
                with_ordinality,
            } => {
                if as_unnest_call(&call).is_some() {
                    let ExprKind::Function { args, .. } = call.kind else {
                        unreachable!("checked by as_unnest_call")
                    };
                    self.plan_unnest_relation(args, with_ordinality, planner_context)
                } else {
                    self.plan_table_function(*call, false, planner_context)
                }
            }
            TableFactor::Lateral(inner) => match *inner {
                TableFactor::Derived(subquery) => {
                    self.query_to_plan(*subquery, planner_context)
                }
                TableFactor::Function {
                    call,
                    with_ordinality,
                } => {
                    if as_unnest_call(&call).is_some() {
                        let ExprKind::Function { args, .. } = call.kind else {
                            unreachable!("checked by as_unnest_call")
                        };
                        self.plan_unnest_relation(args, with_ordinality, planner_context)
                    } else {
                        self.plan_table_function(*call, true, planner_context)
                    }
                }
                other => not_impl_err!(
                    "Unsupported ast node LATERAL {other} in create_relation"
                ),
            },
            TableFactor::Derived(subquery) => {
                self.query_to_plan(*subquery, planner_context)
            }
            TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(*table_with_joins, planner_context)
            }
            TableFactor::Values(rows) => self.sql_values_to_plan(rows, planner_context),
            // @todo Support TableFactory::TableFunction?
            _ => {
                not_impl_err!("Unsupported ast node {relation:?} in create_relation")
            }
        }
    }

    pub(crate) fn create_relation_subquery(
        &self,
        subquery: TableRef<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // At this point for a syntactically valid query the outer_from_schema is
        // guaranteed to be set, so the `.unwrap()` call will never panic. This
        // is the case because we only call this method for lateral table
        // factors, and those can never be the first factor in a FROM list. This
        // means we arrived here through the `for` loop in `plan_from_tables` or
        // the `for` loop in `plan_table_with_joins`.
        let old_from_schema = planner_context
            .set_outer_from_schema(None)
            .unwrap_or_else(|| Arc::new(DFSchema::empty()));
        let outer_query_schema = planner_context.pop_outer_query_schema();
        let new_query_schema = match outer_query_schema {
            Some(ref old_query_schema) => {
                let mut new_query_schema = old_from_schema.as_ref().clone();
                new_query_schema.merge(old_query_schema.as_ref());
                Arc::new(new_query_schema)
            }
            None => Arc::clone(&old_from_schema),
        };
        planner_context.append_outer_query_schema(new_query_schema);

        let plan = self.create_relation(subquery, planner_context)?;
        let outer_ref_columns = plan.all_out_ref_exprs();

        planner_context.pop_outer_query_schema();
        if let Some(schema) = outer_query_schema {
            planner_context.append_outer_query_schema(schema);
        }
        planner_context.set_outer_from_schema(Some(old_from_schema));

        // We can omit the subquery wrapper if there are no columns
        // referencing the outer scope.
        if outer_ref_columns.is_empty() {
            return Ok(plan);
        }

        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                subquery_alias(
                    LogicalPlan::Subquery(Subquery {
                        subquery: input,
                        outer_ref_columns,
                        spans: Spans::new(),
                    }),
                    alias,
                )
            }
            plan => Ok(LogicalPlan::Subquery(Subquery {
                subquery: Arc::new(plan),
                outer_ref_columns,
                spans: Spans::new(),
            })),
        }
    }
}

fn optimize_subquery_sort(
    plan: LogicalPlan,
    enable_subquery_sort_elimination: bool,
) -> Result<Transformed<LogicalPlan>> {
    if !enable_subquery_sort_elimination {
        return Ok(Transformed::no(plan));
    }

    // When initializing subqueries, we examine sort options since they might be unnecessary.
    // They are only important if the subquery result is affected by the ORDER BY statement,
    // which can happen when we have:
    // 1. DISTINCT ON / ARRAY_AGG ... => Handled by an `Aggregate` and its requirements.
    // 2. RANK / ROW_NUMBER ... => Handled by a `WindowAggr` and its requirements.
    // 3. LIMIT => Handled by a `Sort`, so we need to search for it.
    let mut has_limit = false;

    plan.transform_down(|c| {
        if let LogicalPlan::Limit(_) = c {
            has_limit = true;
            return Ok(Transformed::no(c));
        }
        match c {
            LogicalPlan::Sort(s) => {
                if !has_limit {
                    has_limit = false;
                    return Ok(Transformed::yes(Arc::unwrap_or_clone(s.input)));
                }
                Ok(Transformed::no(LogicalPlan::Sort(s)))
            }
            _ => Ok(Transformed::no(c)),
        }
    })
}
