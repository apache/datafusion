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

//! DEFINE planning for MATCH_RECOGNIZE.
//!
//! This module collects all symbols referenced by the SQL pattern and explicit
//! `DEFINE` clauses, plans each symbol's predicate expression, and injects a
//! `Window` plan above the input when any `DEFINE` contains window functions.
//! It also ensures that implicit `TRUE` defines are generated for referenced
//! symbols that lack explicit definitions, so the physical operator computes
//! consistent classifier bitsets for all required symbols.
//!
use std::collections::BTreeSet;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::utils::rebase_expr;
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::logical_plan::NamedExpr;
use datafusion_expr::utils::find_window_exprs;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{self as sqlast, Expr as SQLExpr};
use sqlparser::ast::{MatchRecognizePattern, OrderByExpr};
use sqlparser::ast::{Value, ValueWithSpan};
use sqlparser::tokenizer::Span;

impl<S: ContextProvider> SqlToRel<'_, S> {
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    pub(super) fn plan_defines_with_optional_window(
        &self,
        input: LogicalPlan,
        table_schema: &DFSchemaRef,
        partition_by: &[SQLExpr],
        order_by: &[OrderByExpr],
        pattern: &MatchRecognizePattern,
        symbols: &[sqlast::SymbolDefinition],
        planner_context: &mut PlannerContext,
    ) -> Result<(LogicalPlan, Vec<NamedExpr>, Vec<String>)> {
        // Extract all named symbols referenced anywhere in the pattern (included and excluded)
        // so we generate DEFINE entries for them (TRUE when not explicitly defined).
        fn collect_pattern_symbols(
            pat: &MatchRecognizePattern,
            out: &mut BTreeSet<String>,
        ) {
            use sqlparser::ast::MatchRecognizePattern as SqlPattern;
            use sqlparser::ast::MatchRecognizeSymbol as SqlSymbol;
            match pat {
                SqlPattern::Symbol(SqlSymbol::Named(id)) => {
                    out.insert(id.value.clone());
                }
                SqlPattern::Exclude(sym) => {
                    if let SqlSymbol::Named(id) = sym {
                        out.insert(id.value.clone());
                    }
                }
                SqlPattern::Permute(list) => {
                    for s in list {
                        if let SqlSymbol::Named(id) = s {
                            out.insert(id.value.clone());
                        }
                    }
                }
                SqlPattern::Concat(parts) | SqlPattern::Alternation(parts) => {
                    for p in parts {
                        collect_pattern_symbols(p, out);
                    }
                }
                SqlPattern::Group(inner) => collect_pattern_symbols(inner, out),
                SqlPattern::Repetition(inner, _q) => collect_pattern_symbols(inner, out),
                SqlPattern::Symbol(SqlSymbol::Start)
                | SqlPattern::Symbol(SqlSymbol::End) => {
                    // Ignore start and end symbols
                }
            }
        }

        let mut pattern_symbols: BTreeSet<String> = BTreeSet::new();
        collect_pattern_symbols(pattern, &mut pattern_symbols);

        // Create a map of defined symbols for efficient lookup
        let defined_symbols: datafusion_common::HashMap<_, _> = symbols
            .iter()
            .map(|symbol| (symbol.symbol.value.clone(), symbol.definition.clone()))
            .collect();

        // Build defines vector for all symbols referenced in the pattern OR explicitly defined.
        // This ensures base symbols remain available even if only their shadow appears in the pattern.
        let mut all_symbol_names: BTreeSet<String> = pattern_symbols;
        all_symbol_names.extend(defined_symbols.keys().cloned());

        let mut defines: Vec<NamedExpr> = Vec::new();
        for symbol_name in all_symbol_names.into_iter() {
            let sql_expr = if let Some(definition) = defined_symbols.get(&symbol_name) {
                definition.clone()
            } else {
                SQLExpr::Value(ValueWithSpan {
                    value: Value::Boolean(true),
                    span: Span::empty(),
                })
            };

            let expr = self.sql_to_expr_with_match_recognize_context(
                crate::planner::MatchRecognizeClause::Define,
                sql_expr,
                table_schema,
                partition_by,
                order_by,
                None,
                planner_context,
            )?;
            defines.push(NamedExpr {
                expr,
                name: symbol_name,
            });
        }

        // Apply window plan if DEFINE expressions contain window functions
        let window_exprs = find_window_exprs(
            defines
                .iter()
                .map(|ne| ne.expr.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        );

        if window_exprs.is_empty() {
            let symbol_names =
                defines.iter().map(|ne| ne.name.clone()).collect::<Vec<_>>();
            Ok((input, defines, symbol_names))
        } else {
            let window_plan =
                LogicalPlanBuilder::window_plan(input, window_exprs.clone())?;

            let defines = defines
                .iter()
                .map(|ne| {
                    let rebased_expr =
                        rebase_expr(&ne.expr, &window_exprs, &window_plan)?;
                    Ok(NamedExpr {
                        expr: rebased_expr,
                        name: ne.name.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let symbol_names =
                defines.iter().map(|ne| ne.name.clone()).collect::<Vec<_>>();
            Ok((window_plan, defines, symbol_names))
        }
    }
}
