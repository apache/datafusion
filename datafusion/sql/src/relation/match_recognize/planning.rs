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

//! MATCH_RECOGNIZE logical planning
//!
//! This module implements end-to-end SQL-to-Relational planning for the
//! `MATCH_RECOGNIZE` clause. It translates the SQL AST into a
//! `LogicalPlan::MatchRecognize` with explicit metadata, and it injects
//! additional logical operators (Window, Filter, Aggregate) to honor ANSI SQL
//! semantics across all `ROWS PER MATCH` modes.
//!
//! High-level flow
//! ---------------
//! 1. Build the input relation and capture the table schema.
//! 2. Convert the SQL `ROWS PER MATCH` to the planning representation. For
//!    planning, `ONE ROW PER MATCH` is normalized to `ALL ROWS PER MATCH SHOW`
//!    and reduced later with an Aggregate.
//! 3. Plan `DEFINE` expressions via `plan_defines_with_optional_window`. If any
//!    DEFINE uses window functions, a `Window` is injected above the input and
//!    the DEFINE expressions are rebased. The function returns the planned
//!    `(Expr, symbol_name)` pairs and the set of base symbol names (including
//!    any implicit `TRUE` defines). Shadow symbols are not introduced yet.
//! 4. Convert `AFTER MATCH SKIP` and compile the SQL pattern into DataFusion’s
//!    internal `Pattern`. For ALL ROWS modes, analyze exclusions and:
//!    - rewrite the compiled pattern if needed,
//!    - duplicate DEFINE entries for any shadow symbols,
//!    - build an extended compile set of symbols (base + shadows and any names
//!      required only to drive filtering),
//!    - create an exclusion rewrite context used to normalize expressions, and
//!    - synthesize an exclusion filter predicate to drop rows belonging to
//!      excluded symbols. (In ONE ROW mode, this analysis is skipped.)
//! 5. Convert `PARTITION BY` and `ORDER BY` to expressions for the MR node.
//! 6. Construct a `MatchRecognize` node with an explicit
//!    `MatchRecognizeOutputSpec`:
//!    - passthrough input columns,
//!    - core metadata (`CLASSIFIER`, `MATCH_NUMBER`, `MATCH_SEQUENCE_NUMBER`),
//!    - classifier bitsets for all symbols that the operator must compute
//!      (symbols referenced by DEFINE, pattern, or exclusion filtering). The
//!      extended symbol list is also passed to the MR node to ensure
//!      `__mr_symbol_*` columns are available.
//! 7. Plan `MEASURES` in the context of the MR schema. Apply exclusion
//!    normalization (see below). If any measure uses window functions, inject a
//!    `Window` above MR and rebase the expressions.
//! 8. If an exclusion filter is required, inject a `Filter` above the measures
//!    `Window` (when present) or directly above the MR node.
//! 9. If `ONE ROW PER MATCH` was requested, wrap non-aggregate measures in
//!    `LAST_VALUE(... ORDER BY MATCH_SEQUENCE_NUMBER)` and plan an `Aggregate`
//!    to reduce each match to a single row; rebase measures accordingly.
//! 10. Apply final `ROWS PER MATCH` semantics via
//!     `apply_rows_per_match_semantics`, which prunes metadata and projects the
//!     final columns in the correct order for the chosen mode.
//!
//! Exclusion rewrites and normalization
//! ------------------------------------
//! Shadow symbols are internal, planner-generated aliases of base pattern
//! symbols used to tag rows that would otherwise be excluded in ALL ROWS modes.
//! They are not user-visible: expressions are rewritten so `CLASSIFIER()` and
//! predicates resolve back to the base symbol names.
//!
//! When `ALL ROWS PER MATCH` is in effect and exclusions are present, the
//! planner rewrites both DEFINE and MEASURES so user-visible semantics remain
//! those of the base symbols:
//! - `CLASSIFIER()` is normalized via a `CASE` expression mapping shadow names
//!   back to their base names.
//! - Bitset predicates such as `__mr_classifier_S` are expanded to include any
//!   shadow flags for `S` (e.g., by OR-ing the relevant shadow bits).
//! - A boolean predicate is synthesized to filter out rows belonging to
//!   excluded symbols; this is applied before final `ROWS PER MATCH` projection.
//!
//! All exclusion rewrites and `ROWS PER MATCH` normalization are handled during
//! SQL planning, avoiding the need for separate analyzer passes.

use crate::match_recognize::sql_pattern_to_df;
use crate::planner::{ContextProvider, MatchRecognizeClause, PlannerContext, SqlToRel};
use crate::relation::match_recognize::exclusions;
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::col;
use datafusion_expr::expr::Alias;
use datafusion_expr::logical_plan::{MatchRecognize, NamedExpr};
use datafusion_expr::match_recognize::columns;
use datafusion_expr::match_recognize::columns::MrMetadataColumn;
use datafusion_expr::match_recognize::pattern::Pattern;
use datafusion_expr::match_recognize::MatchRecognizeOutputSpec;
use datafusion_expr::match_recognize::{EmptyMatchesMode, RowsPerMatch};
use datafusion_expr::planner::PlannerResult;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    AfterMatchSkip as SQLAfterMatchSkip, Expr as SQLExpr, MatchRecognizePattern, Measure,
    OrderByExpr, RowsPerMatch as SQLRowsPerMatch, TableFactor,
};
use std::collections::{BTreeSet, HashMap};
use std::ops::Not;
use std::sync::Arc;

impl<S: ContextProvider> SqlToRel<'_, S> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn plan_match_recognize(
        &self,
        table: Box<TableFactor>,
        partition_by: Vec<SQLExpr>,
        order_by: Vec<OrderByExpr>,
        measures: Vec<Measure>,
        rows_per_match_sql: Option<SQLRowsPerMatch>,
        after_match_skip_sql: Option<SQLAfterMatchSkip>,
        pattern: MatchRecognizePattern,
        symbols: Vec<sqlparser::ast::SymbolDefinition>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // 1) Build input and initial schema
        let input = self.create_relation(*table, planner_context)?;
        let table_schema = Arc::clone(input.schema());

        // Determine SQL-facing ROWS PER MATCH early
        let rows_per_match = Self::convert_rows_per_match(rows_per_match_sql.clone());
        // Normalize MR internal rows-per-match: always plan MR with ALL ROWS SHOW when SQL requested ONE ROW
        let internal_rows_per_match = match rows_per_match {
            RowsPerMatch::OneRow => RowsPerMatch::AllRows(EmptyMatchesMode::Show),
            _ => rows_per_match.clone(),
        };

        let original_pattern = pattern.clone();

        // 2) Build DEFINE list, applying windowing if needed
        let (input, defines, define_symbol_names) = self
            .plan_defines_with_optional_window(
                input,
                &table_schema,
                &partition_by,
                &order_by,
                &original_pattern,
                &symbols,
                planner_context,
            )?;

        // 3) Convert AFTER MATCH SKIP
        let after_match_skip = Self::convert_after_match_skip(after_match_skip_sql);

        // 4) Convert pattern to DF representation and analyze exclusions (for ALL ROWS only)
        let original_df_pattern: Pattern = sql_pattern_to_df(&original_pattern);
        let (analysis_opt, planned_df_pattern) = match rows_per_match {
            RowsPerMatch::OneRow => (None, original_df_pattern.clone()),
            _ => {
                let analysis = exclusions::PatternAnalyzer::analyze(&original_df_pattern);
                let rewritten_df_pattern = analysis.rewrite_pattern();
                (Some(analysis), rewritten_df_pattern)
            }
        };

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

        // 5.b) Apply exclusion rewrites to DEFINE's and symbol lists
        let (defines, compile_symbols, classifier_bitsets, exclusion_predicate, ctx_opt) = {
            // Start from symbols collected during DEFINE planning
            let mut defines = defines;
            let mut compile_symbols: BTreeSet<String> =
                define_symbol_names.into_iter().collect();

            if let Some(analysis) = &analysis_opt {
                // Duplicate DEFINE entries for shadow symbols
                if !analysis.excluded_pairs.is_empty() {
                    let base_def_map: HashMap<String, Expr> = defines
                        .iter()
                        .map(|ne| (ne.name.clone(), ne.expr.clone()))
                        .collect();
                    for (base, shadow) in &analysis.excluded_pairs {
                        if let Some(expr) = base_def_map.get(base) {
                            defines.push(NamedExpr {
                                expr: expr.clone(),
                                name: shadow.clone(),
                            });
                        }
                    }
                }

                // Add shadow symbols and any excluded-to-filter symbols to compilation set
                for (_, shadow) in &analysis.excluded_pairs {
                    compile_symbols.insert(shadow.clone());
                }
                for name in &analysis.excluded_names_to_filter {
                    compile_symbols.insert(name.clone());
                }

                // Rewrite DEFINE expressions to normalize CLASSIFIER() and expand bitsets
                let ctx_opt = if !analysis.excluded_pairs.is_empty() {
                    Some(exclusions::ExclusionContext::new(&analysis.excluded_pairs))
                } else {
                    None
                };
                if let Some(ctx) = &ctx_opt {
                    let new_defines: Result<Vec<NamedExpr>> = defines
                        .into_iter()
                        .map(|ne| {
                            let rewritten = ctx.rewrite_expr(ne.expr)?;
                            Ok(NamedExpr {
                                expr: rewritten,
                                name: ne.name,
                            })
                        })
                        .collect();
                    defines = new_defines?;
                }

                // Determine classifier bitsets to include in MR output
                let classifier_bitsets: Vec<String> =
                    compile_symbols.iter().cloned().collect();

                // Build exclusion filter predicate (applied after MEASURES window if present)
                let exclusion_predicate = if analysis.excluded_names_to_filter.is_empty()
                {
                    None
                } else {
                    let mut names = analysis.excluded_names_to_filter.clone();
                    names.sort_unstable();
                    names.dedup();
                    let pred = names
                        .into_iter()
                        .map(|name| col(columns::classifier_bits_col_name(&name)).not())
                        .reduce(|acc, e| acc.and(e))
                        .unwrap();
                    Some(pred)
                };

                (
                    defines,
                    compile_symbols,
                    classifier_bitsets,
                    exclusion_predicate,
                    ctx_opt,
                )
            } else {
                // ONE ROW mode: no exclusion rewrites; no filter; use original symbols
                let classifier_bitsets: Vec<String> =
                    compile_symbols.iter().cloned().collect();
                (defines, compile_symbols, classifier_bitsets, None, None)
            }
        };

        // 6) Build MR node with explicit output spec
        //    - Keep passthrough columns and core metadata (classifier, match_number, match_sequence_number)
        //    - Build classifier bitsets for all symbols referenced in pattern/DEFINE/exclusion.
        let passthrough_indices: Vec<usize> = (0..table_schema.fields().len()).collect();
        let output_spec = MatchRecognizeOutputSpec::new(
            passthrough_indices,
            vec![
                MrMetadataColumn::Classifier,
                MrMetadataColumn::MatchNumber,
                MrMetadataColumn::MatchSequenceNumber,
            ],
            classifier_bitsets.clone(),
        );

        // Use extended symbol names for compilation to ensure `__mr_symbol_*` availability
        let compile_symbols_vec: Vec<String> = compile_symbols.into_iter().collect();
        let match_recognize = MatchRecognize::try_new(
            Arc::new(input.clone()),
            partition_by_exprs.clone(),
            order_by_exprs.clone(),
            after_match_skip.clone(),
            internal_rows_per_match,
            planned_df_pattern,
            compile_symbols_vec,
            defines.clone(),
            output_spec,
        )?;

        // 7) Plan MEASURES expressions and normalize for exclusions if needed
        let measures_exprs_raw = self.plan_measures_exprs(
            measures,
            &match_recognize.schema,
            &partition_by,
            &order_by,
            rows_per_match_sql.clone(),
            planner_context,
        )?;
        // Apply exclusion normalization to MEASURES if needed
        let measures_exprs = if let Some(ctx) = &ctx_opt {
            let rewritten: Result<Vec<NamedExpr>> = measures_exprs_raw
                .into_iter()
                .map(|ne| {
                    Ok(NamedExpr {
                        expr: ctx.rewrite_expr(ne.expr)?,
                        name: ne.name,
                    })
                })
                .collect();
            rewritten?
        } else {
            measures_exprs_raw
        };

        // 8) Use MR with all metadata; pruning will be handled by optimizer
        let plan = LogicalPlan::MatchRecognize(match_recognize);

        // 10) Apply windowing for MEASURES and rebase if needed
        let (plan, measures_exprs) =
            Self::maybe_apply_window_and_rebase_measures(plan, measures_exprs.clone())?;

        // 10.b) Inject exclusion filter above the window (if any) or directly above MR
        let plan = if let Some(pred) = exclusion_predicate {
            LogicalPlanBuilder::from(plan).filter(pred)?.build()?
        } else {
            plan
        };

        // 11) Apply aggregate and rebase if needed
        let (plan, measures_exprs) = match rows_per_match {
            RowsPerMatch::OneRow => self.apply_aggregate_and_rebase_measures(
                plan,
                measures_exprs,
                &partition_by_exprs,
            )?,
            _ => (plan, measures_exprs),
        };

        // 12) Apply ROWS PER MATCH semantics
        let plan = Self::apply_rows_per_match_semantics(
            plan,
            rows_per_match,
            &table_schema,
            &partition_by_exprs,
            &measures_exprs,
            ctx_opt.as_ref(),
        )?;

        Ok(plan)
    }

    /// Convert expression using sql_to_expr with MATCH_RECOGNIZE context
    #[allow(clippy::too_many_arguments)]
    pub(super) fn sql_to_expr_with_match_recognize_context(
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

        // Strip alias early for MEASURES; the string label from the clause is used instead
        if matches!(clause, MatchRecognizeClause::Measures) {
            if let Expr::Alias(Alias { expr: inner, .. }) = expr {
                expr = *inner;
            }
        }

        // Give expression planners a chance to post-process MR expressions
        let aggregate_context = matches!(clause, MatchRecognizeClause::Measures)
            && (rows_per_match.is_none()
                || matches!(rows_per_match, Some(SQLRowsPerMatch::OneRow)));
        for planner in self.context_provider.get_expr_planners() {
            expr = match planner.plan_match_recognize(expr, aggregate_context)? {
                PlannerResult::Planned(e) | PlannerResult::Original(e) => e,
            };
        }

        Ok(expr)
    }
}
