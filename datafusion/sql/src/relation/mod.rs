// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::cell::Cell;
use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::resolve::ResolvedReference;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    DFSchema, Diagnostic, Result, ScalarValue, Span, Spans, TableReference,
    TableSelector, internal_err, not_impl_err, plan_err,
};
use datafusion_expr::builder::subquery_alias;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlannerContext, RelationPlanning, TableSelectorContext,
    TableSelectorPlanning,
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Volatility, expr::Unnest};
use datafusion_expr::{Subquery, SubqueryAlias};
use sqlparser::ast::{
    Expr as SQLExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Spanned, Statement,
    TableFactor, TableVersion,
};

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

/// Returns the argument name and value of an `AT (<name> => <expr>)` time
/// travel clause, or `None` if `func` has any other form.
fn at_clause_argument(func: &SQLExpr) -> Option<(&str, &SQLExpr)> {
    let SQLExpr::Function(func) = func else {
        return None;
    };
    let is_at = matches!(
        func.name.0.as_slice(),
        [part] if part.as_ident().is_some_and(|i| i.value.eq_ignore_ascii_case("AT"))
    );
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    if !is_at || list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }
    match list.args.as_slice() {
        [
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(expr),
                ..
            },
        ]
        | [
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(name),
                arg: FunctionArgExpr::Expr(expr),
                ..
            },
        ] => Some((name.value.as_str(), expr)),
        _ => None,
    }
}

// Implement RelationPlannerContext
impl<S: ContextProvider> RelationPlannerContext for SqlToRelRelationContext<'_, '_, S> {
    fn context_provider(&self) -> &dyn ContextProvider {
        self.planner.context_provider
    }

    fn plan(&mut self, relation: TableFactor) -> Result<LogicalPlan> {
        self.planner.create_relation(relation, self.planner_context)
    }

    fn get_cte(&self, name: &TableReference) -> Option<&LogicalPlan> {
        get_cte_for_table_reference(self.planner_context, name)
    }

    fn sql_to_expr(&mut self, expr: SQLExpr, schema: &DFSchema) -> Result<Expr> {
        self.planner.sql_to_expr(expr, schema, self.planner_context)
    }

    fn sql_expr_to_logical_expr(
        &mut self,
        expr: SQLExpr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        self.planner
            .sql_expr_to_logical_expr(expr, schema, self.planner_context)
    }

    fn normalize_ident(&self, ident: sqlparser::ast::Ident) -> String {
        self.planner.ident_normalizer.normalize(ident)
    }

    fn object_name_to_table_reference(
        &self,
        name: sqlparser::ast::ObjectName,
    ) -> Result<TableReference> {
        self.planner.object_name_to_table_reference(name)
    }
}

struct SqlToRelTableSelectorContext<'a, 'b, S: ContextProvider> {
    planner: &'a SqlToRel<'b, S>,
    /// The table factor whose selector is determined
    relation: &'a TableFactor,
    /// Whether the statement is being prepared, in which case expressions
    /// must not depend on the time of execution
    prepared: bool,
    /// The error message for an expression that depends on the time of
    /// execution, in a prepared statement
    execution_dependent: Cell<Option<String>>,
}

impl<'a, 'b, S: ContextProvider> SqlToRelTableSelectorContext<'a, 'b, S> {
    fn new(
        planner: &'a SqlToRel<'b, S>,
        relation: &'a TableFactor,
        prepared: bool,
    ) -> Self {
        Self {
            planner,
            relation,
            prepared,
            execution_dependent: Cell::new(None),
        }
    }
}

impl<S: ContextProvider> TableSelectorContext
    for SqlToRelTableSelectorContext<'_, '_, S>
{
    fn evaluate_const_expr(&self, expr: SQLExpr) -> Result<ScalarValue> {
        // A fresh planner context, so planning the expression can neither
        // depend on nor affect the planning of the query that references the
        // table, and the selector is the same before and during planning
        let expr = self.planner.sql_to_expr(
            expr,
            &DFSchema::empty(),
            &mut PlannerContext::new(),
        )?;
        if expr.exists(|e| Ok(matches!(e, Expr::Placeholder(_))))? {
            return not_impl_err!(
                "Parameters are not supported in table selectors: {}",
                self.relation
            );
        }
        // The selector is determined when the statement is prepared, so
        // functions such as `now()` would return the time of preparation
        // rather than of execution
        if self.prepared
            && expr.exists(|e| {
                Ok(matches!(
                    e,
                    Expr::ScalarFunction(f)
                        if f.func.signature().volatility != Volatility::Immutable
                ))
            })?
        {
            let message = format!(
                "Table selectors in prepared statements must not depend on the \
                 time of execution, such as with now(): {}",
                self.relation
            );
            self.execution_dependent.set(Some(message.clone()));
            return not_impl_err!("{message}");
        }
        self.planner.context_provider.evaluate_const_expr(expr)
    }
}

/// Determines the [`TableSelector`] of a table factor with a time travel
/// clause, as DataFusion does by default. See [`SqlToRel::table_selector`]
/// for the mapping.
fn default_table_selector(
    relation: &TableFactor,
    context: &dyn TableSelectorContext,
) -> Result<Option<TableSelector>> {
    let TableFactor::Table {
        version: Some(version),
        ..
    } = relation
    else {
        return Ok(None);
    };
    let (is_timestamp, expr) = match version {
        TableVersion::VersionAsOf(expr) => (false, expr),
        TableVersion::TimestampAsOf(expr) | TableVersion::ForSystemTimeAsOf(expr) => {
            (true, expr)
        }
        TableVersion::Function(func) => match at_clause_argument(func) {
            Some((name, expr)) if name.eq_ignore_ascii_case("VERSION") => (false, expr),
            Some((name, expr)) if name.eq_ignore_ascii_case("TIMESTAMP") => (true, expr),
            _ => return not_impl_err!("Unsupported time travel clause: {func}"),
        },
        TableVersion::Changes { .. } => {
            return not_impl_err!("CHANGES clauses are not supported");
        }
    };
    let value = context.evaluate_const_expr(expr.clone())?;
    if value.is_null() {
        let kind = if is_timestamp { "timestamp" } else { "version" };
        return plan_err!(
            "The {kind} of a time travel clause must not be NULL: {relation}"
        );
    }
    if !is_timestamp {
        return Ok(Some(TableSelector::Version(value)));
    }

    let timestamp_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    match value.cast_to(&timestamp_type)? {
        ScalarValue::TimestampNanosecond(Some(ns), _) => {
            Ok(Some(TableSelector::Timestamp(ns)))
        }
        value => internal_err!("Expected a timestamp, got: {value:?}"),
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Create a `LogicalPlan` that scans the named relation.
    ///
    /// First tries any registered extension planners. If no extension handles
    /// the relation, falls back to the default planner.
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let planned_relation =
            match self.create_extension_relation(relation, planner_context)? {
                RelationPlanning::Planned(planned) => planned,
                RelationPlanning::Original(original) => {
                    Box::new(self.create_default_relation(*original, planner_context)?)
                }
            };

        let optimized_plan = optimize_subquery_sort(
            planned_relation.plan,
            self.context_provider
                .options()
                .sql_parser
                .enable_subquery_sort_elimination,
        )?
        .data;
        if let Some(alias) = planned_relation.alias {
            self.apply_table_alias(optimized_plan, alias)
        } else {
            Ok(optimized_plan)
        }
    }

    fn create_extension_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<RelationPlanning> {
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

    fn create_default_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<PlannedRelation> {
        let relation_span = relation.span();
        // Normalize the name of a referenced table or view, and determine its
        // selector. References to CTEs are read without a selector.
        let (table_ref, selector) = match &relation {
            TableFactor::Table {
                name, args: None, ..
            } => {
                let table_ref = self.object_name_to_table_reference(name.clone())?;
                let selector =
                    match get_cte_for_table_reference(planner_context, &table_ref) {
                        Some(_) => None,
                        None => self.table_selector(&table_ref, &relation)?,
                    };
                (Some(table_ref), selector)
            }
            _ => (None, None),
        };
        let (plan, alias) = match relation {
            TableFactor::Table {
                name,
                alias,
                args,
                version,
                ..
            } => {
                if let Some(func_args) = args {
                    if version.is_some() {
                        return not_impl_err!(
                            "Time travel is not supported for table functions"
                        );
                    }
                    let tbl_func_name =
                        name.0.first().unwrap().as_ident().unwrap().to_string();
                    let args = func_args
                        .args
                        .into_iter()
                        .map(|arg| {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg
                            {
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
                    let plan = LogicalPlanBuilder::scan(
                        TableReference::Bare {
                            table: format!("{tbl_func_name}()").into(),
                        },
                        provider,
                        None,
                    )?
                    .build()?;
                    (plan, alias)
                } else {
                    let Some(table_ref) = table_ref else {
                        return internal_err!(
                            "The name of table '{name}' was not normalized"
                        );
                    };
                    let cte = get_cte_for_table_reference(planner_context, &table_ref);
                    if cte.is_some() && version.is_some() {
                        return plan_err!(
                            "Time travel is not supported for CTE '{table_ref}'"
                        );
                    }
                    if let Some(selector) = selector {
                        let plan = self.selected_table_scan(
                            table_ref,
                            &selector,
                            relation_span,
                        )?;
                        return Ok(PlannedRelation::new(plan, alias));
                    }
                    (
                        match (
                            cte,
                            self.context_provider.get_table_source(table_ref.clone()),
                        ) {
                            (Some(cte_plan), _) => Ok(cte_plan.clone()),
                            (_, Ok(provider)) => LogicalPlanBuilder::scan(
                                table_ref.clone(),
                                provider,
                                None,
                            )?
                            .build(),
                            (None, Err(e)) => {
                                let e = e.with_diagnostic(Diagnostic::new_error(
                                    format!("table '{table_ref}' not found"),
                                    Span::try_from_sqlparser_span(relation_span),
                                ));
                                Err(e)
                            }
                        }?,
                        alias,
                    )
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self.query_to_plan(*subquery, planner_context)?;
                (logical_plan, alias)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => (
                self.plan_table_with_joins(*table_with_joins, planner_context)?,
                alias,
            ),
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality,
            } => {
                if with_ordinality {
                    return not_impl_err!("UNNEST with ordinality is not supported yet");
                }

                // Unnest table factor has empty input
                let schema = DFSchema::empty();
                let input = LogicalPlanBuilder::empty(true).build()?;
                // Unnest table factor can have multiple arguments.
                // We treat each argument as a separate unnest expression.
                let unnest_exprs = array_exprs
                    .into_iter()
                    .map(|sql_expr| {
                        let expr = self.sql_expr_to_logical_expr(
                            sql_expr,
                            &schema,
                            planner_context,
                        )?;
                        Self::check_unnest_arg(&expr, &schema)?;
                        Ok(Expr::Unnest(Unnest::new(expr)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if unnest_exprs.is_empty() {
                    return plan_err!("UNNEST must have at least one argument");
                }
                let logical_plan = self.try_process_unnest(input, unnest_exprs)?;
                (logical_plan, alias)
            }
            TableFactor::UNNEST { .. } => {
                return not_impl_err!(
                    "UNNEST table factor with offset is not supported yet"
                );
            }
            TableFactor::Function {
                name, args, alias, ..
            } => {
                let tbl_func_ref = self.object_name_to_table_reference(name)?;
                let schema = planner_context
                    .outer_queries_schemas()
                    .last()
                    .cloned()
                    .unwrap_or_else(|| Arc::new(DFSchema::empty()));
                let func_args = args
                    .into_iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        } => {
                            self.sql_expr_to_logical_expr(expr, &schema, planner_context)
                        }
                        _ => plan_err!("Unsupported function argument: {arg:?}"),
                    })
                    .collect::<Result<Vec<Expr>>>()?;
                let provider = self
                    .context_provider
                    .get_table_function_source(tbl_func_ref.table(), func_args)?;
                let plan =
                    LogicalPlanBuilder::scan(tbl_func_ref.table(), provider, None)?
                        .build()?;
                (plan, alias)
            }
            // @todo Support TableFactory::TableFunction?
            _ => {
                return not_impl_err!(
                    "Unsupported ast node {relation:?} in create_relation"
                );
            }
        };
        Ok(PlannedRelation::new(plan, alias))
    }

    /// Derives a [`TableSelector`] from a table factor that references
    /// `table`. The returned [`TableSelector`], if present, describes
    /// which state of the table is being requested by the table factor.
    ///
    /// The registered [`RelationPlanner`]s are asked first, in order. If none
    /// of them handles the table factor, its time travel clause, if any, is
    /// mapped to a selector. The different syntaxes used by SQL dialects are
    /// normalized to either a version or a timestamp:
    ///
    /// | SQL                                          | `TableSelector` |
    /// |----------------------------------------------|-----------------|
    /// | `t VERSION AS OF 3`                          | `Version`       |
    /// | `t AT (VERSION => 'v1')`                     | `Version`       |
    /// | `t TIMESTAMP AS OF '2024-01-01'`             | `Timestamp`     |
    /// | `t FOR SYSTEM_TIME AS OF now()`              | `Timestamp`     |
    /// | `t AT (TIMESTAMP => now() - INTERVAL '1 h')` | `Timestamp`     |
    ///
    /// The expressions in these clauses are evaluated during planning and are
    /// required to be constant expressions. Other clauses,
    /// such as Snowflake's `BEFORE (...)`, `AT (OFFSET => ...)` or
    /// `CHANGES (...)`, are not supported.
    ///
    /// Returns `None` if the table is to be read without a selector.
    ///
    /// [`RelationPlanner`]: datafusion_expr::planner::RelationPlanner
    pub fn table_selector(
        &self,
        table: &TableReference,
        relation: &TableFactor,
    ) -> Result<Option<TableSelector>> {
        let context = SqlToRelTableSelectorContext::new(self, relation, false);
        self.plan_table_selector(table, relation, &context)
    }

    fn plan_table_selector(
        &self,
        table: &TableReference,
        relation: &TableFactor,
        context: &SqlToRelTableSelectorContext<'_, '_, S>,
    ) -> Result<Option<TableSelector>> {
        for planner in self.context_provider.get_relation_planners() {
            match planner.plan_table_selector(table, relation, context)? {
                TableSelectorPlanning::Planned(selector) => return Ok(selector),
                TableSelectorPlanning::Original => {}
            }
        }
        default_table_selector(relation, context)
    }

    /// Plans a scan of a table with a [`TableSelector`].
    fn selected_table_scan(
        &self,
        table_ref: TableReference,
        selector: &TableSelector,
        relation_span: sqlparser::tokenizer::Span,
    ) -> Result<LogicalPlan> {
        let provider = self
            .context_provider
            .get_table_source_with_selector(table_ref.clone(), selector)
            .map_err(|e| {
                e.with_diagnostic(Diagnostic::new_error(
                    format!("cannot read table '{table_ref}@{selector}'"),
                    Span::try_from_sqlparser_span(relation_span),
                ))
            })?;
        // The source reports the selector it reads, which may differ from the
        // requested selector, e.g. a snapshot id for a timestamp
        if provider.selector().is_none() {
            return plan_err!(
                "The source of table '{table_ref}@{selector}' must report the \
                 selector it reads"
            );
        }
        LogicalPlanBuilder::scan(table_ref, provider, None)?.build()
    }

    /// Rejects table selectors in a prepared statement that depend on the
    /// time of execution, such as `AT (TIMESTAMP => now())`.
    ///
    /// Table selectors are determined, and the table is retrieved from the
    /// catalog, when the statement is prepared. Functions such as `now()`
    /// would therefore return the time of preparation rather than of
    /// execution.
    pub(crate) fn check_prepared_table_selectors(
        &self,
        statement: &Statement,
    ) -> Result<()> {
        // Visit the same table factors as are retrieved from the catalog
        // before planning, which excludes references to CTEs
        let mut execution_dependent = false;
        let result = crate::resolve::resolve_sql_references(
            statement,
            self.options.enable_ident_normalization,
            |reference| {
                let ResolvedReference::TableFactor(table, table_factor) = reference
                else {
                    return Ok(());
                };
                let context = SqlToRelTableSelectorContext::new(self, table_factor, true);
                // Other errors are reported when the statement is planned
                let _ = self.plan_table_selector(&table, table_factor, &context);
                match context.execution_dependent.into_inner() {
                    Some(message) => {
                        execution_dependent = true;
                        not_impl_err!("{message}")
                    }
                    None => Ok(()),
                }
            },
        );
        // Other errors, such as for invalid table names, are reported when the
        // statement is planned
        if execution_dependent {
            result.map(|_| ())
        } else {
            Ok(())
        }
    }

    pub(crate) fn create_relation_subquery(
        &self,
        subquery: TableFactor,
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
