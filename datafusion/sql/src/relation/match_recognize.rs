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

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::match_recognize::pattern_convert::sql_pattern_to_df;
use crate::planner::{ContextProvider, MatchRecognizeClause, PlannerContext, SqlToRel};
use crate::utils::rebase_expr;
use datafusion_common::{plan_err, DFSchemaRef, HashMap, Result};
use datafusion_expr::col;
use datafusion_expr::expr::{AggregateFunction, Alias};
use datafusion_expr::logical_plan::MatchRecognize;
use datafusion_expr::match_recognize::columns::MrMetadataColumn;
use datafusion_expr::match_recognize::Pattern as MrPattern;
use datafusion_expr::match_recognize::{
    rows_filter_expr, rows_projection_expr, AfterMatchSkip, EmptyMatchesMode,
    RowsPerMatch,
};
use datafusion_expr::planner::PlannerResult;
use datafusion_expr::utils::{find_aggregate_exprs, find_window_exprs};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    AfterMatchSkip as SQLAfterMatchSkip, EmptyMatchesMode as SQLEmptyMatchesMode,
    Expr as SQLExpr, MatchRecognizePattern, MatchRecognizeSymbol, Measure, OrderByExpr,
    RowsPerMatch as SQLRowsPerMatch, SymbolDefinition, TableFactor, Value, ValueWithSpan,
};
use sqlparser::tokenizer::Span;

impl<S: ContextProvider> SqlToRel<'_, S> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn plan_match_recognize(
        &self,
        table: Box<TableFactor>,
        partition_by: Vec<SQLExpr>,
        order_by: Vec<OrderByExpr>,
        measures: Vec<Measure>,
        rows_per_match_sql: Option<SQLRowsPerMatch>,
        after_match_skip_sql: Option<SQLAfterMatchSkip>,
        pattern: MatchRecognizePattern,
        symbols: Vec<SymbolDefinition>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // 1) Build input and initial schema
        let input = self.create_relation(*table, planner_context)?;
        let table_schema = Arc::clone(input.schema());

        // 2) Build DEFINE list, applying windowing if needed
        let (input, defines, symbols) = self.plan_defines_with_optional_window(
            input,
            &table_schema,
            &partition_by,
            &order_by,
            &pattern,
            &symbols,
            planner_context,
        )?;

        // 3) Convert ROWS PER MATCH and AFTER MATCH SKIP
        let rows_per_match = Self::convert_rows_per_match(rows_per_match_sql.clone());
        let after_match_skip = Self::convert_after_match_skip(after_match_skip_sql);

        // 4) Convert pattern to DF representation
        let pattern_df = sql_pattern_to_df(&pattern);

        // 5) Convert PARTITION BY and ORDER BY
        let partition_by_exprs = partition_by
            .clone()
            .into_iter()
            .map(|expr: SQLExpr| self.sql_to_expr(expr, input.schema(), planner_context))
            .collect::<Result<Vec<_>>>()?;
        let order_by_exprs = self
            .order_by_to_sort_expr(
                order_by.clone(),
                input.schema(),
                planner_context,
                false,
                None,
            )?
            .into_iter()
            .map(|sort| sort.with_expr(sort.expr.clone()))
            .collect::<Vec<_>>();

        // 6) Build MR node to enable planning of MEASURES
        let match_recognize = MatchRecognize::try_new(
            Arc::new(input.clone()),
            partition_by_exprs.clone(),
            order_by_exprs.clone(),
            after_match_skip.clone(),
            rows_per_match.clone(),
            pattern_df.clone(),
            symbols.clone(),
            defines.clone(),
        )?;

        // 7) Plan MEASURES expressions
        let measures_exprs = self.plan_measures_exprs(
            measures,
            &match_recognize.schema,
            &partition_by,
            &order_by,
            rows_per_match_sql.clone(),
            planner_context,
        )?;

        // 8) Use MR with all metadata; pruning will be handled by optimizer
        let plan = LogicalPlan::MatchRecognize(match_recognize);

        // 10) Apply windowing for MEASURES and rebase if needed
        let (plan, measures_exprs) =
            Self::maybe_apply_window_and_rebase_measures(plan, measures_exprs.clone())?;

        // 11) Apply aggregate and rebase if needed
        let (plan, measures_exprs) = match rows_per_match {
            RowsPerMatch::OneRow => self.apply_aggregate_and_rebase_measures(
                plan,
                measures_exprs,
                &partition_by_exprs,
            )?,
            _ => (plan, measures_exprs),
        };

        // 12) Apply ROWS PER MATCH semantics (filter + projection)
        let plan = Self::apply_rows_per_match_semantics(
            plan,
            rows_per_match,
            &pattern_df,
            &table_schema,
            &partition_by_exprs,
            &measures_exprs,
        )?;

        Ok(plan)
    }

    /// Plan DEFINE expressions and optionally insert a window plan if any DEFINE
    /// expression contains window functions. Re-bases DEFINE expressions when
    /// windowing is introduced and returns the updated input plan, planned
    /// DEFINE pairs `(Expr, symbol_name)`, and the list of symbol names.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    fn plan_defines_with_optional_window(
        &self,
        input: LogicalPlan,
        table_schema: &DFSchemaRef,
        partition_by: &[SQLExpr],
        order_by: &[OrderByExpr],
        pattern: &MatchRecognizePattern,
        symbols: &[SymbolDefinition],
        planner_context: &mut PlannerContext,
    ) -> Result<(LogicalPlan, Vec<(Expr, String)>, Vec<String>)> {
        // Extract symbols from the pattern before converting it
        let pattern_symbols = self.extract_symbols_from_pattern(pattern)?;

        // Create a map of defined symbols for efficient lookup
        let defined_symbols: HashMap<_, _> = symbols
            .iter()
            .map(|symbol| (symbol.symbol.value.clone(), symbol.definition.clone()))
            .collect();

        // Build defines vector based on pattern symbols (use TRUE for undefined)
        let mut defines = Vec::new();
        for symbol_name in pattern_symbols {
            let sql_expr = if let Some(definition) = defined_symbols.get(&symbol_name) {
                definition.clone()
            } else {
                SQLExpr::Value(ValueWithSpan {
                    value: Value::Boolean(true),
                    span: Span::empty(),
                })
            };

            let expr = self.sql_to_expr_with_match_recognize_context(
                MatchRecognizeClause::Define,
                sql_expr,
                table_schema,
                partition_by,
                order_by,
                None,
                planner_context,
            )?;
            defines.push((expr, symbol_name));
        }

        // Apply window plan if DEFINE expressions contain window functions
        let window_exprs = find_window_exprs(
            defines
                .iter()
                .map(|(expr, _)| expr.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        );

        if window_exprs.is_empty() {
            let symbol_names = defines
                .iter()
                .map(|(_, name)| name.clone())
                .collect::<Vec<_>>();
            Ok((input, defines, symbol_names))
        } else {
            let window_plan =
                LogicalPlanBuilder::window_plan(input, window_exprs.clone())?;

            let defines = defines
                .iter()
                .map(|(expr, name)| {
                    let rebased_expr = rebase_expr(expr, &window_exprs, &window_plan)?;
                    Ok((rebased_expr, name.clone()))
                })
                .collect::<Result<Vec<_>>>()?;

            let symbol_names = defines
                .iter()
                .map(|(_, name)| name.clone())
                .collect::<Vec<_>>();
            Ok((window_plan, defines, symbol_names))
        }
    }

    /// Convert SQL `ROWS PER MATCH` specification from the SQL AST into the
    /// internal `RowsPerMatch` representation.
    fn convert_rows_per_match(rows_per_match: Option<SQLRowsPerMatch>) -> RowsPerMatch {
        match rows_per_match {
            None | Some(SQLRowsPerMatch::OneRow) => RowsPerMatch::OneRow,
            Some(SQLRowsPerMatch::AllRows(mode)) => {
                let converted_mode = mode.map(|m| match m {
                    SQLEmptyMatchesMode::Show => EmptyMatchesMode::Show,
                    SQLEmptyMatchesMode::Omit => EmptyMatchesMode::Omit,
                    SQLEmptyMatchesMode::WithUnmatched => EmptyMatchesMode::WithUnmatched,
                });
                RowsPerMatch::AllRows(converted_mode)
            }
        }
    }

    /// Convert SQL `AFTER MATCH SKIP` specification from the SQL AST into the
    /// internal `AfterMatchSkip` representation.
    fn convert_after_match_skip(
        after_match_skip: Option<SQLAfterMatchSkip>,
    ) -> AfterMatchSkip {
        match after_match_skip {
            None | Some(SQLAfterMatchSkip::PastLastRow) => AfterMatchSkip::PastLastRow,
            Some(SQLAfterMatchSkip::ToNextRow) => AfterMatchSkip::ToNextRow,
            Some(SQLAfterMatchSkip::ToFirst(symbol)) => {
                AfterMatchSkip::ToFirst(symbol.value)
            }
            Some(SQLAfterMatchSkip::ToLast(symbol)) => {
                AfterMatchSkip::ToLast(symbol.value)
            }
        }
    }

    /// Plan MEASURES expressions in the context of MATCH_RECOGNIZE, producing
    /// logical expressions paired with their required aliases.
    fn plan_measures_exprs(
        &self,
        measures: Vec<Measure>,
        mr_schema: &DFSchemaRef,
        partition_by: &[SQLExpr],
        order_by: &[OrderByExpr],
        rows_per_match: Option<SQLRowsPerMatch>,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<(Expr, String)>> {
        let measures_exprs = measures
            .into_iter()
            .map(|measure| {
                let expr = self.sql_to_expr_with_match_recognize_context(
                    MatchRecognizeClause::Measures,
                    measure.expr,
                    mr_schema,
                    partition_by,
                    order_by,
                    rows_per_match.clone(),
                    planner_context,
                )?;
                Ok((expr, measure.alias.value))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(measures_exprs)
    }

    /// If MEASURES contain window functions, insert a window plan above the MR
    /// plan and rebase the MEASURES expressions to reference the new plan.
    fn maybe_apply_window_and_rebase_measures(
        plan: LogicalPlan,
        measures_exprs: Vec<(Expr, String)>,
    ) -> Result<(LogicalPlan, Vec<(Expr, String)>)> {
        let window_exprs = find_window_exprs(
            measures_exprs
                .iter()
                .map(|(expr, _)| expr.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        );

        if window_exprs.is_empty() {
            Ok((plan, measures_exprs))
        } else {
            let window_plan =
                LogicalPlanBuilder::window_plan(plan, window_exprs.clone())?;
            let measures_exprs = measures_exprs
                .iter()
                .map(|(expr, name)| {
                    let rebased_expr = rebase_expr(expr, &window_exprs, &window_plan)?;
                    Ok((rebased_expr, name.clone()))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((window_plan, measures_exprs))
        }
    }

    /// If MEASURES contain aggregate functions, insert an aggregate plan above the MR
    /// plan and rebase the MEASURES expressions to reference the new plan.
    fn apply_aggregate_and_rebase_measures(
        &self,
        plan: LogicalPlan,
        measures_exprs: Vec<(Expr, String)>,
        partition_by_exprs: &[Expr],
    ) -> Result<(LogicalPlan, Vec<(Expr, String)>)> {
        // Ensure LAST_VALUE aggregate is available
        let last_value_udaf = match self.context_provider.get_aggregate_meta("last_value")
        {
            Some(f) => f,
            None => return plan_err!("LAST_VALUE aggregate function is not registered"),
        };

        // ORDER BY __mr_match_sequence_number for reduction across match rows
        let mr_order_by =
            vec![col(MrMetadataColumn::MatchSequenceNumber.as_ref()).sort(true, false)];

        // Wrap non-aggregate expressions with LAST_VALUE(expr ORDER BY __mr_match_sequence_number)
        let wrapped_measures = measures_exprs
            .into_iter()
            .map(|(expr, name)| {
                let has_agg =
                    !find_aggregate_exprs(std::slice::from_ref(&expr)).is_empty();
                if has_agg {
                    (expr, name)
                } else {
                    let agg_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
                        Arc::clone(&last_value_udaf),
                        vec![expr],
                        false,
                        None,
                        mr_order_by.clone(),
                        None,
                    ));
                    (agg_expr, name)
                }
            })
            .collect::<Vec<_>>();

        // Collect all aggregate expressions (including newly wrapped ones)
        let aggregate_exprs = find_aggregate_exprs(
            wrapped_measures
                .iter()
                .map(|(expr, _)| expr.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        );

        if aggregate_exprs.is_empty() {
            // No measures provided; nothing to aggregate
            return Ok((plan, wrapped_measures));
        }

        // Build aggregate plan grouped by PARTITION BY columns and match number
        let mut group_expr = partition_by_exprs.to_vec();
        group_expr.push(col(MrMetadataColumn::MatchNumber.as_ref()));
        let aggregate_plan = LogicalPlanBuilder::from(plan)
            .aggregate(group_expr, aggregate_exprs.clone())?
            .build()?;

        let rebased_measures = wrapped_measures
            .iter()
            .map(|(expr, name)| {
                let rebased_expr = rebase_expr(expr, &aggregate_exprs, &aggregate_plan)?;
                Ok((rebased_expr, name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((aggregate_plan, rebased_measures))
    }

    /// Apply `ROWS PER MATCH` semantics by optionally filtering output rows and
    /// projecting the correct set of columns based on the selected mode.
    fn apply_rows_per_match_semantics(
        plan: LogicalPlan,
        rows_per_match: RowsPerMatch,
        pattern: &MrPattern,
        table_schema: &DFSchemaRef,
        partition_by_exprs: &[Expr],
        measures_exprs: &[(Expr, String)],
    ) -> Result<LogicalPlan> {
        let mut plan_builder = LogicalPlanBuilder::from(plan);
        if let Some(pred) = rows_filter_expr(&rows_per_match, pattern) {
            plan_builder = plan_builder.filter(pred)?;
        }
        let projection_exprs = rows_projection_expr(
            &rows_per_match,
            table_schema,
            partition_by_exprs,
            measures_exprs,
        );
        let final_plan = plan_builder.project(projection_exprs)?.build()?;
        Ok(final_plan)
    }

    /// Convert expression using sql_to_expr with MATCH_RECOGNIZE context
    #[allow(clippy::too_many_arguments)]
    fn sql_to_expr_with_match_recognize_context(
        &self,
        clause: MatchRecognizeClause,
        sql_expr: SQLExpr,
        schema: &DFSchemaRef,
        partition_by: &[SQLExpr],
        order_by: &[OrderByExpr],
        rows_per_match: Option<SQLRowsPerMatch>,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Enable MATCH_RECOGNIZE context
        planner_context.enable_match_recognize_context(
            clause.clone(),
            rows_per_match.clone(),
            partition_by,
            order_by,
        )?;

        // Convert the expression using sql_to_expr (which includes validation)
        let result = self.sql_to_expr(sql_expr, schema, planner_context);

        // Restore the previous context
        planner_context.disable_match_recognize_context();

        let mut expr = result?;

        // Give expression planners a chance to post-process MR expressions
        let aggregate_context = matches!(clause, MatchRecognizeClause::Measures)
            && (rows_per_match.is_none()
                || matches!(rows_per_match, Some(SQLRowsPerMatch::OneRow)));
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_match_recognize(expr, aggregate_context)? {
                PlannerResult::Planned(e) | PlannerResult::Original(e) => expr = e,
            }
        }

        // Remove the eventual alias from the expression as it is redundant with the String alias in the measures clause
        if matches!(clause, MatchRecognizeClause::Measures) {
            if let Expr::Alias(Alias { expr, .. }) = expr {
                return Ok(*expr);
            }
        }
        Ok(expr)
    }

    /// Extract all named symbols from a MATCH_RECOGNIZE pattern
    fn extract_symbols_from_pattern(
        &self,
        pattern: &MatchRecognizePattern,
    ) -> Result<BTreeSet<String>> {
        let mut symbols = BTreeSet::new();

        fn collect_symbols(
            pattern: &MatchRecognizePattern,
            symbols: &mut BTreeSet<String>,
        ) {
            match pattern {
                MatchRecognizePattern::Symbol(symbol) => {
                    if let MatchRecognizeSymbol::Named(ident) = symbol {
                        symbols.insert(ident.value.clone());
                    }
                }
                MatchRecognizePattern::Exclude(symbol) => {
                    if let MatchRecognizeSymbol::Named(ident) = symbol {
                        symbols.insert(ident.value.clone());
                    }
                }
                MatchRecognizePattern::Permute(symbols_list) => {
                    for symbol in symbols_list {
                        if let MatchRecognizeSymbol::Named(ident) = symbol {
                            symbols.insert(ident.value.clone());
                        }
                    }
                }
                MatchRecognizePattern::Concat(patterns) => {
                    for p in patterns {
                        collect_symbols(p, symbols);
                    }
                }
                MatchRecognizePattern::Group(pattern) => {
                    collect_symbols(pattern, symbols);
                }
                MatchRecognizePattern::Alternation(patterns) => {
                    for p in patterns {
                        collect_symbols(p, symbols);
                    }
                }
                MatchRecognizePattern::Repetition(pattern, _) => {
                    collect_symbols(pattern, symbols);
                }
            }
        }

        collect_symbols(pattern, &mut symbols);
        Ok(symbols)
    }
}
