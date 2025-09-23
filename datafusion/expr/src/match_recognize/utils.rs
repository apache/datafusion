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

//! MATCH_RECOGNIZE-specific utilities

use crate::expr::Case;
use crate::logical_plan::NamedExpr;
use crate::match_recognize::columns::MrMetadataColumn;
use crate::match_recognize::pattern::{EmptyMatchesMode, RowsPerMatch};
use crate::Expr;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{plan_err, Column, DFSchemaRef, Result, ScalarValue};

/// Extract the single symbol referenced via the internal
/// `mr_symbol(value, 'SYM')` wrapper within `expr`.
///
/// Returns `Ok(None)` if no symbol predicate is present and an error if more
/// than one distinct symbol is referenced. Used during MATCH_RECOGNIZE
/// planning (e.g. to wire classifier masks and validate expressions).
pub fn find_symbol_predicate(expr: &Expr) -> Result<Option<String>> {
    let mut found_symbol: Option<String> = None;

    expr.apply(|e| match e {
        Expr::ScalarFunction(f) if f.name() == "mr_symbol" => {
            if let Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) = f.args.get(1) {
                let sym = s.to_uppercase();
                if let Some(current) = &found_symbol {
                    if current != &sym {
                        return plan_err!(
                            "Expression can reference at most one symbol predicate; found: {},{}",
                            current,
                            sym
                        );
                    }
                } else {
                    found_symbol = Some(sym);
                }
            }
            Ok(TreeNodeRecursion::Jump)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;

    Ok(found_symbol)
}

/// Build the final projection for MATCH_RECOGNIZE according to
/// `ROWS PER MATCH` semantics.
///
/// - OneRow: project only `partition_by_exprs` plus MEASURES
/// - AllRows: project all input table columns plus MEASURES
/// - With UNMATCHED: wrap each MEASURE with
///   `CASE WHEN __mr_match_number IS NOT NULL THEN <measure> END`
///   so unmatched rows yield NULL measures. MEASURES are aliased using
///   their `NamedExpr` names.
pub fn rows_projection_expr(
    rows_per_match: &RowsPerMatch,
    table_schema: &DFSchemaRef,
    partition_by_exprs: &[Expr],
    measures_exprs: &[NamedExpr],
) -> Vec<Expr> {
    let mut exprs = if matches!(rows_per_match, RowsPerMatch::OneRow) {
        partition_by_exprs.to_vec()
    } else {
        table_schema
            .columns()
            .iter()
            .map(|c| Expr::Column(c.clone()))
            .collect()
    };

    let with_unmatched = matches!(
        rows_per_match,
        RowsPerMatch::AllRows(EmptyMatchesMode::WithUnmatched)
    );

    // In WITH UNMATCHED mode, wrap each MEASURE in
    // CASE WHEN __mr_match_number IS NOT NULL THEN measure END
    // so measures evaluate to NULL on unmatched rows.
    exprs.extend(measures_exprs.iter().map(|named| {
        let projected = if with_unmatched {
            let match_number_not_null =
                Expr::Column(Column::from_name(MrMetadataColumn::MatchNumber.as_ref()))
                    .is_not_null();
            Expr::Case(Case::new(
                None,
                vec![(
                    Box::new(match_number_not_null),
                    Box::new(named.expr.clone()),
                )],
                None,
            ))
        } else {
            named.expr.clone()
        };
        projected.alias(&named.name)
    }));
    exprs
}
