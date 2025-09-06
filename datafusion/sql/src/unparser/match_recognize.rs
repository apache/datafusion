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

use super::{
    ast::{MatchRecognizeRelationBuilder, QueryBuilder, RelationBuilder, SelectBuilder},
    Unparser,
};
use crate::match_recognize::df_pattern_to_sql;
use datafusion_common::{internal_err, Result};
use datafusion_expr::match_recognize::columns::classifier_bits_col_name;
use datafusion_expr::match_recognize::{
    columns::MrMetadataColumn, AfterMatchSkip, EmptyMatchesMode, RowsPerMatch,
};
use sqlparser::ast::{self, visit_expressions_mut, Ident, OrderByKind};

impl Unparser<'_> {
    pub(super) fn unparse_match_recognize(
        &self,
        mr: &datafusion_expr::logical_plan::MatchRecognize,
        query: &mut Option<QueryBuilder>,
        select: &mut SelectBuilder,
        relation: &mut RelationBuilder,
    ) -> Result<()> {
        // Build inner relation from the MR input plan
        let mut inner_relation = RelationBuilder::default();
        self.select_to_sql_recursively(
            mr.input.as_ref(),
            query,
            select,
            &mut inner_relation,
        )?;
        let inner_tf = match inner_relation.build()? {
            Some(tf) => tf,
            None => {
                return internal_err!(
                    "Failed to build inner relation for MATCH_RECOGNIZE"
                )
            }
        };

        // Partition by / Order by
        let partition_by = mr
            .partition_by
            .iter()
            .map(|e| self.expr_to_sql(e))
            .collect::<Result<Vec<_>>>()?;
        let order_by = self.sorts_to_sql(&mr.order_by)?;
        let order_by_exprs = match order_by {
            OrderByKind::Expressions(list) => list,
            OrderByKind::All(_) => {
                return internal_err!(
                    "Unsupported ORDER BY ALL in MATCH_RECOGNIZE unparser"
                );
            }
        };

        // Include minimal virtual MEASURES for MR metadata columns
        // (__mr_classifier, __mr_match_number, __mr_match_sequence_number)
        // as well as classifier bitset columns (__mr_classifier_<symbol>)
        let measures: Vec<ast::Measure> = mr
            .output_spec
            .metadata_columns
            .iter()
            .map(|meta_col: &MrMetadataColumn| {
                // Create function call expression
                let func_expr = self.mr_function_call(meta_col.measure_function_name());

                // Create Measure with expression and alias
                ast::Measure {
                    expr: func_expr,
                    alias: Ident::new(meta_col.as_ref()),
                }
            })
            .chain(mr.output_spec.classifier_bitset_symbols.iter().map(|sym| {
                let alias = classifier_bits_col_name(sym);
                // convert to Measure: `classifier() == "<SYM>" AS <alias>`
                let expr = {
                    // Build classifier() function call
                    let func_expr = self.mr_function_call(
                        MrMetadataColumn::Classifier.measure_function_name(),
                    );

                    // Build '<SYM>' string literal
                    let sym_lit =
                        ast::Expr::value(ast::Value::SingleQuotedString(sym.clone()));

                    // classifier() = '<SYM>'
                    self.binary_op_to_sql(func_expr, sym_lit, ast::BinaryOperator::Eq)
                };
                ast::Measure {
                    expr,
                    alias: Ident::new(alias),
                }
            }))
            .collect();

        // Rows per match (SQL-facing): use the rows_per_match_internal field
        let rows_per_match = match &mr.rows_per_match {
            RowsPerMatch::OneRow => ast::RowsPerMatch::OneRow,
            RowsPerMatch::AllRows(mode) => {
                let m = match mode {
                    EmptyMatchesMode::Show => ast::EmptyMatchesMode::Show,
                    EmptyMatchesMode::Omit => ast::EmptyMatchesMode::Omit,
                    EmptyMatchesMode::WithUnmatched => {
                        ast::EmptyMatchesMode::WithUnmatched
                    }
                };
                ast::RowsPerMatch::AllRows(Some(m))
            }
        };

        // After match skip
        let after_match_skip = match &mr.after_skip {
            AfterMatchSkip::PastLastRow => ast::AfterMatchSkip::PastLastRow,
            AfterMatchSkip::ToNextRow => ast::AfterMatchSkip::ToNextRow,
            AfterMatchSkip::ToFirst(s) => {
                ast::AfterMatchSkip::ToFirst(self.new_ident_quoted_if_needs(s.clone()))
            }
            AfterMatchSkip::ToLast(s) => {
                ast::AfterMatchSkip::ToLast(self.new_ident_quoted_if_needs(s.clone()))
            }
        };

        // Pattern
        let pattern_ast = df_pattern_to_sql(&mr.pattern);

        // DEFINE symbols from mr.defines (NamedExpr { expr, name })
        let symbols = mr
            .defines
            .iter()
            .map(|ne| {
                let mut def_ast = self.expr_to_sql(&ne.expr)?;
                // Strip table qualifiers to avoid being treated as symbol qualifiers
                let _ = visit_expressions_mut(&mut def_ast, |expr| {
                    if let ast::Expr::CompoundIdentifier(ids) = expr {
                        if let Some(last) = ids.last().cloned() {
                            *expr = ast::Expr::Identifier(last);
                        }
                    }
                    std::ops::ControlFlow::<()>::Continue(())
                });
                Ok(ast::SymbolDefinition {
                    symbol: self.new_ident_quoted_if_needs(ne.name.clone()),
                    definition: def_ast,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Assemble MatchRecognize relation
        let mut mr_builder = MatchRecognizeRelationBuilder::default();
        mr_builder
            .table(inner_tf)
            .partition_by(partition_by)
            .order_by(order_by_exprs)
            .measures(measures)
            .rows_per_match(Some(rows_per_match))
            .after_match_skip(Some(after_match_skip))
            .pattern(pattern_ast)
            .symbols(symbols)
            .alias(None);

        // Set as the relation for this SELECT
        relation.match_recognize(mr_builder);
        Ok(())
    }
}
