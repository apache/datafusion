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

use datafusion_common::{Column, DFSchemaRef};

use crate::expr::Case;
use crate::match_recognize::columns::MrMetadataColumn;
use crate::match_recognize::pattern::{EmptyMatchesMode, RowsPerMatch};
use crate::Expr;

/// Helper: build projection list based on ROWS PER MATCH semantics
pub fn rows_projection_expr(
    rows_per_match: &RowsPerMatch,
    table_schema: &DFSchemaRef,
    partition_by_exprs: &[Expr],
    measures_exprs: &[(Expr, String)],
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
    exprs.extend(measures_exprs.iter().map(|(expr, alias)| {
        let projected = if with_unmatched {
            let match_number_not_null =
                Expr::Column(Column::from_name(MrMetadataColumn::MatchNumber.as_ref()))
                    .is_not_null();
            Expr::Case(Case::new(
                None,
                vec![(Box::new(match_number_not_null), Box::new(expr.clone()))],
                None,
            ))
        } else {
            expr.clone()
        };
        projected.alias(alias.clone())
    }));
    exprs
}
