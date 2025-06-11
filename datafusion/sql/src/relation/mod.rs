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

use crate::planner::{ContextProvider, MatchRecognizeClause, PlannerContext, SqlToRel};
use crate::utils::rebase_expr;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    not_impl_err, plan_err, DFSchema, DFSchemaRef, Diagnostic, Result, Span, Spans,
    TableReference,
};
use datafusion_expr::builder::subquery_alias;
use datafusion_expr::col;
use datafusion_expr::logical_plan::MatchRecognizePattern;
use datafusion_expr::utils::find_window_exprs;
use datafusion_expr::{
    expr::Alias,
    match_recognize::{
        AfterMatchSkip, EmptyMatchesMode, Pattern, RepetitionQuantifier, RowsPerMatch,
        Symbol,
    },
};
use datafusion_expr::{expr::Unnest, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::{Subquery, SubqueryAlias};
use sqlparser::ast::{
    Expr as SQLExpr, FunctionArg, FunctionArgExpr, OrderByExpr,
    RowsPerMatch as SQLRowsPerMatch, Spanned, TableFactor, Value, ValueWithSpan,
};
use sqlparser::tokenizer::Span as SQLSpan;
use datafusion_expr::lit;

mod join;

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Create a `LogicalPlan` that scans the named relation
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let relation_span = relation.span();
        let (plan, alias) = match relation {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if let Some(func_args) = args {
                    let tbl_func_name =
                        name.0.first().unwrap().as_ident().unwrap().to_string();
                    let args = func_args
                        .args
                        .into_iter()
                        .flat_map(|arg| {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg
                            {
                                self.sql_expr_to_logical_expr(
                                    expr,
                                    &DFSchema::empty(),
                                    planner_context,
                                )
                            } else {
                                plan_err!("Unsupported function argument type: {:?}", arg)
                            }
                        })
                        .collect::<Vec<_>>();
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
                    // Normalize name and alias
                    let table_ref = self.object_name_to_table_reference(name)?;
                    let table_name = table_ref.to_string();
                    let cte = planner_context.get_cte(&table_name);
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
                    .outer_query_schema()
                    .cloned()
                    .unwrap_or_else(DFSchema::empty);
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
            TableFactor::MatchRecognize {
                table,
                partition_by,
                order_by,
                measures,
                rows_per_match,
                after_match_skip,
                pattern,
                symbols,
                alias: _,
            } => {
                // Create the input plan from the nested table
                let input = self.create_relation(*table, planner_context)?;

                let table_schema = Arc::clone(&input.schema());

                // Extract symbols from the pattern before converting it
                let pattern_symbols = self.extract_symbols_from_pattern(&pattern)?;

                // Create a map of defined symbols for efficient lookup
                let defined_symbols: std::collections::HashMap<_, _> = symbols
                    .iter()
                    .map(|symbol| {
                        (symbol.symbol.value.clone(), symbol.definition.clone())
                    })
                    .collect();

                // Build defines vector based on pattern symbols
                let mut defines = Vec::new();
                for symbol_name in pattern_symbols {
                    if let Some(definition) = defined_symbols.get(&symbol_name) {
                        // Use the actual definition
                        let expr = self.sql_to_expr_with_match_recognize_define_context(
                            definition.clone(),
                            &table_schema,
                            &partition_by,
                            &order_by,
                            planner_context,
                        )?;
                        defines.push((expr, symbol_name));
                    } else {
                        // Create a TRUE expression for the missing symbol
                        let true_expr = SQLExpr::Value(ValueWithSpan {
                            value: Value::Boolean(true),
                            span: SQLSpan::empty(),
                        });
                        let expr = self.sql_to_expr_with_match_recognize_define_context(
                            true_expr,
                            &table_schema,
                            &partition_by,
                            &order_by,
                            planner_context,
                        )?;
                        defines.push((expr, symbol_name));
                    }
                }

                // Convert rows per match
                let rows_per_match_opt = rows_per_match.clone().map(|rpm| match rpm {
                    SQLRowsPerMatch::OneRow => RowsPerMatch::OneRow,
                    SQLRowsPerMatch::AllRows(mode) => {
                        let converted_mode = mode.map(|m| match m {
                            sqlparser::ast::EmptyMatchesMode::Show => {
                                EmptyMatchesMode::Show
                            }
                            sqlparser::ast::EmptyMatchesMode::Omit => {
                                EmptyMatchesMode::Omit
                            }
                            sqlparser::ast::EmptyMatchesMode::WithUnmatched => {
                                EmptyMatchesMode::WithUnmatched
                            }
                        });
                        RowsPerMatch::AllRows(converted_mode)
                    }
                });

                // Convert after match skip
                let after_match_skip_opt = after_match_skip.map(|ams| match ams {
                    sqlparser::ast::AfterMatchSkip::PastLastRow => {
                        AfterMatchSkip::PastLastRow
                    }
                    sqlparser::ast::AfterMatchSkip::ToNextRow => {
                        AfterMatchSkip::ToNextRow
                    }
                    sqlparser::ast::AfterMatchSkip::ToFirst(symbol) => {
                        AfterMatchSkip::ToFirst(symbol.value)
                    }
                    sqlparser::ast::AfterMatchSkip::ToLast(symbol) => {
                        AfterMatchSkip::ToLast(symbol.value)
                    }
                });

                let window_exprs = find_window_exprs(
                    defines
                        .iter()
                        .map(|(expr, _)| expr.clone())
                        .collect::<Vec<_>>()
                        .as_slice(),
                );

                let input = if window_exprs.is_empty() {
                    input
                } else {
                    let window_plan =
                        LogicalPlanBuilder::window_plan(input, window_exprs.clone())?;

                    // Re-write the projection
                    defines = defines
                        .iter()
                        .map(|(expr, name)| {
                            let rebased_expr =
                                rebase_expr(expr, &window_exprs, &window_plan)?;
                            Ok((rebased_expr, name.clone()))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    window_plan
                };

                let symbols = defines
                    .iter()
                    .map(|(_, name)| name.clone())
                    .collect::<Vec<_>>();

                // Build a Projection that extends the input with DEFINE symbol predicates
                let projection_plan = LogicalPlanBuilder::from(input.clone())
                    .project(
                        input
                            .schema()
                            .columns()
                            .iter()
                            .map(|c| Expr::Column(c.clone()))
                            .chain(defines.into_iter().map(|(expr, symbol)| {
                                expr.alias(format!("__mr_symbol_{}", symbol))
                            })),
                    )?
                    .build()?;

                // Convert pattern
                let pattern = self.convert_match_recognize_pattern(
                    pattern,
                    &projection_plan.schema(),
                    planner_context,
                )?;

                // Convert partition_by expressions using sql_to_expr
                let partition_by_exprs = partition_by
                    .clone()
                    .into_iter()
                    .map(|expr: SQLExpr| {
                        self.sql_to_expr(expr, &projection_plan.schema(), planner_context)
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Convert order_by expressions preserving sort direction information
                let order_by_exprs = self
                    .order_by_to_sort_expr(
                        order_by.clone(),
                        &projection_plan.schema(),
                        planner_context,
                        false, // literal_to_column: numeric literals are treated as constants
                        None,  // additional_schema: no additional schema needed
                    )?
                    .into_iter()
                    .map(|sort| sort.with_expr(sort.expr.clone()))
                    .collect::<Vec<_>>();

                let mr_pattern = MatchRecognizePattern::try_new(
                    Arc::new(projection_plan),
                    partition_by_exprs.clone(),
                    order_by_exprs.clone(),
                    after_match_skip_opt,
                    rows_per_match_opt.clone(),
                    pattern,
                    symbols.clone(),
                )?;

                // Convert measures expressions using sql_to_expr with MATCH_RECOGNIZE MEASURES context
                // Note: SQL standard requires every MEASURES expression to have an explicit alias.
                // Each measure is stored as (expr, alias) tuple to enforce this requirement.
                let mut measures_exprs = measures
                    .into_iter()
                    .map(|measure| {
                        // First, plan the SQL expression into a logical Expr
                        let expr = self
                            .sql_to_expr_with_match_recognize_measures_context(
                                measure.expr,
                                &mr_pattern.schema,
                                &partition_by,
                                &order_by,
                                rows_per_match.clone(),
                                planner_context,
                            )?;

                        Ok((expr, measure.alias.value))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let window_exprs = find_window_exprs(
                    measures_exprs
                        .iter()
                        .map(|(expr, _)| expr.clone())
                        .collect::<Vec<_>>()
                        .as_slice(),
                );

                let input = if window_exprs.is_empty() {
                    LogicalPlan::MatchRecognizePattern(mr_pattern)
                } else {
                    let window_plan: LogicalPlan = LogicalPlanBuilder::window_plan(
                        LogicalPlan::MatchRecognizePattern(mr_pattern),
                        window_exprs.clone(),
                    )?;

                    // Re-write the projection
                    measures_exprs = measures_exprs
                        .iter()
                        .map(|(expr, name)| {
                            let rebased_expr =
                                rebase_expr(expr, &window_exprs, &window_plan)?;
                            Ok((rebased_expr, name.clone()))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    window_plan
                };

                // =====================================================================
                // Build PLAN according to ROWS PER MATCH semantics using helpers
                // =====================================================================

                // 1.  Start with the input plan
                let mut plan_builder = LogicalPlanBuilder::from(input.clone());

                // 2.  Apply the optional filter derived from ROWS PER MATCH
                if let Some(pred) = rows_filter(&rows_per_match_opt) {
                    plan_builder = plan_builder.filter(pred)?;
                }

                // 3.  Determine the projection list
                let projection_exprs = rows_projection(
                    &rows_per_match_opt,
                    &table_schema,
                    &partition_by_exprs,
                    &measures_exprs,
                );

                // 4.  Finish the plan
                let final_plan = plan_builder.project(projection_exprs)?.build()?;

                (final_plan, None)
            }
            // @todo Support TableFactory::TableFunction?
            _ => {
                return not_impl_err!(
                    "Unsupported ast node {relation:?} in create_relation"
                );
            }
        };

        let optimized_plan = optimize_subquery_sort(plan)?.data;
        if let Some(alias) = alias {
            self.apply_table_alias(optimized_plan, alias)
        } else {
            Ok(optimized_plan)
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
        let new_query_schema = match planner_context.outer_query_schema() {
            Some(old_query_schema) => {
                let mut new_query_schema = old_from_schema.as_ref().clone();
                new_query_schema.merge(old_query_schema);
                Some(Arc::new(new_query_schema))
            }
            None => Some(Arc::clone(&old_from_schema)),
        };
        let old_query_schema = planner_context.set_outer_query_schema(new_query_schema);

        let plan = self.create_relation(subquery, planner_context)?;
        let outer_ref_columns = plan.all_out_ref_exprs();

        planner_context.set_outer_query_schema(old_query_schema);
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

    /// Convert expression using sql_to_expr with MATCH_RECOGNIZE context
    fn sql_to_expr_with_match_recognize_measures_context(
        &self,
        sql_expr: sqlparser::ast::Expr,
        schema: &DFSchemaRef,
        partition_by: &Vec<SQLExpr>,
        order_by: &Vec<OrderByExpr>,
        rows_per_match: Option<SQLRowsPerMatch>,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Set a flag in the planner context to indicate we're in MATCH_RECOGNIZE MEASURES context
        planner_context.enable_match_recognize_context(
            MatchRecognizeClause::Measures,
            rows_per_match,
            partition_by,
            order_by,
        )?;

        // Convert the expression using sql_to_expr (which includes validation)
        let result = self.sql_to_expr(sql_expr, schema, planner_context);

        // Restore the previous context
        planner_context.disable_match_recognize_context();

        let mut expr = result?;

        use datafusion_expr::planner::PlannerResult;

        // Give expression planners a chance to post-process MR expressions
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_match_recognize(expr)? {
                PlannerResult::Planned(e) | PlannerResult::Original(e) => expr = e,
            }
        }

        // Remove the eventual alias from the expression as it is redundant with the String alias in the measures clause
        match expr {
            Expr::Alias(Alias { expr, .. }) => Ok(*expr),
            _ => Ok(expr),
        }
    }

    /// Convert expression using sql_to_expr with MATCH_RECOGNIZE DEFINE context
    fn sql_to_expr_with_match_recognize_define_context(
        &self,
        sql_expr: sqlparser::ast::Expr,
        schema: &DFSchemaRef,
        partition_by: &Vec<SQLExpr>,
        order_by: &Vec<OrderByExpr>,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Set flag to indicate we're in MATCH_RECOGNIZE DEFINE context
        planner_context.enable_match_recognize_context(
            MatchRecognizeClause::Define,
            None,
            partition_by,
            order_by,
        )?;

        // Convert the expression using sql_to_expr (which includes validation)
        let result: std::result::Result<Expr, datafusion_common::DataFusionError> =
            self.sql_to_expr(sql_expr, schema, planner_context);

        // Restore the previous context
        planner_context.disable_match_recognize_context();

        let mut expr = result?;

        use datafusion_expr::planner::PlannerResult;

        // Give expression planners a chance to post-process MR expressions
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_match_recognize(expr)? {
                PlannerResult::Planned(e) | PlannerResult::Original(e) => expr = e,
            }
        }

        Ok(expr)
    }

    /// Convert a SQL MATCH_RECOGNIZE pattern to a DataFusion MatchRecognizePattern
    fn convert_match_recognize_pattern(
        &self,
        pattern: sqlparser::ast::MatchRecognizePattern,
        _input_schema: &DFSchemaRef,
        _planner_context: &mut PlannerContext,
    ) -> Result<Pattern> {
        use sqlparser::ast::{
            MatchRecognizePattern as SqlPattern, MatchRecognizeSymbol as SqlSymbol,
        };

        match pattern {
            SqlPattern::Symbol(symbol) => {
                let converted_symbol = match symbol {
                    SqlSymbol::Named(ident) => Symbol::Named(ident.value),
                    SqlSymbol::Start => Symbol::Start,
                    SqlSymbol::End => Symbol::End,
                };
                Ok(Pattern::Symbol(converted_symbol))
            }
            SqlPattern::Exclude(symbol) => {
                let converted_symbol = match symbol {
                    SqlSymbol::Named(ident) => Symbol::Named(ident.value),
                    SqlSymbol::Start => Symbol::Start,
                    SqlSymbol::End => Symbol::End,
                };
                Ok(Pattern::Exclude(converted_symbol))
            }
            SqlPattern::Permute(symbols) => {
                let converted_symbols = symbols
                    .into_iter()
                    .map(|symbol| match symbol {
                        SqlSymbol::Named(ident) => Symbol::Named(ident.value),
                        SqlSymbol::Start => Symbol::Start,
                        SqlSymbol::End => Symbol::End,
                    })
                    .collect();
                Ok(Pattern::Permute(converted_symbols))
            }
            SqlPattern::Concat(patterns) => {
                let converted_patterns = patterns
                    .into_iter()
                    .map(|p| {
                        self.convert_match_recognize_pattern(
                            p,
                            _input_schema,
                            _planner_context,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Pattern::Concat(converted_patterns))
            }
            SqlPattern::Group(pattern) => {
                let converted_pattern = self.convert_match_recognize_pattern(
                    *pattern,
                    _input_schema,
                    _planner_context,
                )?;
                Ok(Pattern::Group(Box::new(converted_pattern)))
            }
            SqlPattern::Alternation(patterns) => {
                let converted_patterns = patterns
                    .into_iter()
                    .map(|p| {
                        self.convert_match_recognize_pattern(
                            p,
                            _input_schema,
                            _planner_context,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Pattern::Alternation(converted_patterns))
            }
            SqlPattern::Repetition(pattern, quantifier) => {
                let converted_pattern = self.convert_match_recognize_pattern(
                    *pattern,
                    _input_schema,
                    _planner_context,
                )?;
                let converted_quantifier = match quantifier {
                    sqlparser::ast::RepetitionQuantifier::ZeroOrMore => {
                        RepetitionQuantifier::ZeroOrMore
                    }
                    sqlparser::ast::RepetitionQuantifier::OneOrMore => {
                        RepetitionQuantifier::OneOrMore
                    }
                    sqlparser::ast::RepetitionQuantifier::AtMostOne => {
                        RepetitionQuantifier::AtMostOne
                    }
                    sqlparser::ast::RepetitionQuantifier::Exactly(n) => {
                        RepetitionQuantifier::Exactly(n)
                    }
                    sqlparser::ast::RepetitionQuantifier::AtLeast(n) => {
                        RepetitionQuantifier::AtLeast(n)
                    }
                    sqlparser::ast::RepetitionQuantifier::AtMost(n) => {
                        RepetitionQuantifier::AtMost(n)
                    }
                    sqlparser::ast::RepetitionQuantifier::Range(n, m) => {
                        RepetitionQuantifier::Range(n, m)
                    }
                };
                Ok(Pattern::Repetition(
                    Box::new(converted_pattern),
                    converted_quantifier,
                ))
            }
        }
    }

    /// Extract all named symbols from a MATCH_RECOGNIZE pattern
    fn extract_symbols_from_pattern(
        &self,
        pattern: &sqlparser::ast::MatchRecognizePattern,
    ) -> Result<std::collections::BTreeSet<String>> {
        use sqlparser::ast::{
            MatchRecognizePattern as SqlPattern, MatchRecognizeSymbol as SqlSymbol,
        };

        let mut symbols = std::collections::BTreeSet::new();

        fn collect_symbols(
            pattern: &SqlPattern,
            symbols: &mut std::collections::BTreeSet<String>,
        ) {
            match pattern {
                SqlPattern::Symbol(symbol) => {
                    if let SqlSymbol::Named(ident) = symbol {
                        symbols.insert(ident.value.clone());
                    }
                }
                SqlPattern::Exclude(symbol) => {
                    if let SqlSymbol::Named(ident) = symbol {
                        symbols.insert(ident.value.clone());
                    }
                }
                SqlPattern::Permute(symbols_list) => {
                    for symbol in symbols_list {
                        if let SqlSymbol::Named(ident) = symbol {
                            symbols.insert(ident.value.clone());
                        }
                    }
                }
                SqlPattern::Concat(patterns) => {
                    for p in patterns {
                        collect_symbols(p, symbols);
                    }
                }
                SqlPattern::Group(pattern) => {
                    collect_symbols(pattern, symbols);
                }
                SqlPattern::Alternation(patterns) => {
                    for p in patterns {
                        collect_symbols(p, symbols);
                    }
                }
                SqlPattern::Repetition(pattern, _) => {
                    collect_symbols(pattern, symbols);
                }
            }
        }

        collect_symbols(pattern, &mut symbols);
        Ok(symbols)
    }
}

fn optimize_subquery_sort(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // When initializing subqueries, we examine sort options since they might be unnecessary.
    // They are only important if the subquery result is affected by the ORDER BY statement,
    // which can happen when we have:
    // 1. DISTINCT ON / ARRAY_AGG ... => Handled by an `Aggregate` and its requirements.
    // 2. RANK / ROW_NUMBER ... => Handled by a `WindowAggr` and its requirements.
    // 3. LIMIT => Handled by a `Sort`, so we need to search for it.
    let mut has_limit = false;
    let new_plan = plan.transform_down(|c| {
        if let LogicalPlan::Limit(_) = c {
            has_limit = true;
            return Ok(Transformed::no(c));
        }
        match c {
            LogicalPlan::Sort(s) => {
                if !has_limit {
                    has_limit = false;
                    return Ok(Transformed::yes(s.input.as_ref().clone()));
                }
                Ok(Transformed::no(LogicalPlan::Sort(s)))
            }
            _ => Ok(Transformed::no(c)),
        }
    });
    new_plan
}

/// Helper: determine optional filter predicate based on ROWS PER MATCH
fn rows_filter(rpm: &Option<RowsPerMatch>) -> Option<Expr> {
    match rpm {
        // ONE ROW PER MATCH (default): keep last match row only
        None | Some(RowsPerMatch::OneRow) => Some(col("__mr_is_last_match_row")),

        // ALL ROWS PER MATCH SHOW (default): keep included rows only
        Some(RowsPerMatch::AllRows(None))
        | Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::Show))) => {
            Some(col("__mr_is_included_row"))
        }
        // ALL ROWS PER MATCH OMIT EMPTY MATCHES
        Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::Omit))) => Some(
            col("__mr_is_included_row").and(
                col("__mr_classifier").not_eq(lit("(empty)")),
            ),
        ),
        // ALL ROWS PER MATCH WITH UNMATCHED ROWS – no extra filter
        Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched))) => None,
    }
}

/// Helper: build projection list based on ROWS PER MATCH semantics
fn rows_projection(
    rpm: &Option<RowsPerMatch>,
    table_schema: &DFSchemaRef,
    partition_by_exprs: &[Expr],
    measures_exprs: &[(Expr, String)],
) -> Vec<Expr> {
    let mut exprs: Vec<Expr> = match rpm {
        None | Some(RowsPerMatch::OneRow) => partition_by_exprs.iter().cloned().collect(),
        Some(RowsPerMatch::AllRows(_)) => table_schema
            .columns()
            .iter()
            .map(|c| Expr::Column(c.clone()))
            .collect(),
    };

    exprs.extend(
        measures_exprs
            .iter()
            .map(|(expr, alias)| expr.clone().alias(alias.clone())),
    );
    exprs
}
