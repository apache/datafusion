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

use std::ops::Not;

use datafusion_common::{Column, DFSchemaRef};

use crate::match_recognize::columns::MrMetadataColumn;
use crate::match_recognize::pattern::{EmptyMatchesMode, Pattern, RowsPerMatch};
use crate::Expr;

/// Build a predicate `__mr_classifier IS NOT NULL`
pub fn classifier_not_null() -> Expr {
    let col_expr = Expr::Column(Column::from_name(MrMetadataColumn::Classifier.as_ref()));
    col_expr.is_not_null()
}

/// Helper: check if a pattern contains any EXCLUDE clauses
/// If no EXCLUDE clauses are present, the IsIncludedRow filter can be optimized out
/// since all rows will have IsIncludedRow = true
pub fn pattern_contains_exclude(pattern: &Pattern) -> bool {
    match pattern {
        Pattern::Symbol(_) => false,
        Pattern::Exclude(_) => true,
        Pattern::Permute(_) => false, // Permute contains Symbols, not Patterns, so no EXCLUDE clauses
        Pattern::Concat(patterns) => patterns.iter().any(pattern_contains_exclude),
        Pattern::Group(inner) => pattern_contains_exclude(inner),
        Pattern::Alternation(patterns) => patterns.iter().any(pattern_contains_exclude),
        Pattern::Repetition(inner, _) => pattern_contains_exclude(inner),
    }
}

/// Helper: determine optional filter predicate based on ROWS PER MATCH
/// Optimizes out IsIncludedRow filter when pattern contains no EXCLUDE clauses
pub fn rows_filter_expr(
    rows_per_match: &RowsPerMatch,
    pattern: &Pattern,
) -> Option<Expr> {
    match rows_per_match {
        // ONE ROW PER MATCH (default): no filter as we return one aggregate row per match
        RowsPerMatch::OneRow => None,

        // ALL ROWS PER MATCH SHOW (default): keep non-excluded rows only
        // Optimize: if no EXCLUDE clauses, is_excluded_row is always false, so no filter needed
        RowsPerMatch::AllRows(None)
        | RowsPerMatch::AllRows(Some(EmptyMatchesMode::Show)) => {
            if pattern_contains_exclude(pattern) {
                Some(
                    Expr::Column(Column::from_name(
                        MrMetadataColumn::IsExcludedRow.as_ref(),
                    ))
                    .not(),
                )
            } else {
                None // No filter needed - all rows are included
            }
        }
        // ALL ROWS PER MATCH OMIT EMPTY MATCHES
        // Optimize: if no EXCLUDE clauses, is_excluded_row is always false, so only need classifier filter
        RowsPerMatch::AllRows(Some(EmptyMatchesMode::Omit)) => {
            if pattern_contains_exclude(pattern) {
                Some(
                    Expr::Column(Column::from_name(
                        MrMetadataColumn::IsExcludedRow.as_ref(),
                    ))
                    .not()
                    .and(classifier_not_null()),
                )
            } else {
                Some(classifier_not_null()) // Only need classifier filter
            }
        }
        // ALL ROWS PER MATCH WITH UNMATCHED ROWS – no extra filter
        RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched)) => None,
    }
}

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

    exprs.extend(
        measures_exprs
            .iter()
            .map(|(expr, alias)| expr.clone().alias(alias.clone())),
    );
    exprs
}

/// Map external SQL-facing `RowsPerMatch` to internal execution variant used by physical planning.
pub fn rows_per_match_internal(sql: &RowsPerMatch) -> RowsPerMatch {
    if matches!(
        sql,
        RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched))
    ) {
        sql.clone()
    } else {
        RowsPerMatch::AllRows(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::match_recognize::pattern::{RepetitionQuantifier, Symbol};

    #[test]
    fn test_pattern_contains_exclude() {
        // Test simple symbol - no exclude
        let pattern = Pattern::Symbol(Symbol::Named("A".to_string()));
        assert!(!pattern_contains_exclude(&pattern));

        // Test exclude symbol - contains exclude
        let pattern = Pattern::Exclude(Symbol::Named("B".to_string()));
        assert!(pattern_contains_exclude(&pattern));

        // Test concat with no exclude
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("B".to_string())),
        ]);
        assert!(!pattern_contains_exclude(&pattern));

        // Test concat with exclude
        let pattern = Pattern::Concat(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Exclude(Symbol::Named("B".to_string())),
        ]);
        assert!(pattern_contains_exclude(&pattern));

        // Test group with no exclude
        let pattern =
            Pattern::Group(Box::new(Pattern::Symbol(Symbol::Named("A".to_string()))));
        assert!(!pattern_contains_exclude(&pattern));

        // Test group with exclude
        let pattern =
            Pattern::Group(Box::new(Pattern::Exclude(Symbol::Named("B".to_string()))));
        assert!(pattern_contains_exclude(&pattern));

        // Test alternation with no exclude
        let pattern = Pattern::Alternation(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Symbol(Symbol::Named("B".to_string())),
        ]);
        assert!(!pattern_contains_exclude(&pattern));

        // Test alternation with exclude
        let pattern = Pattern::Alternation(vec![
            Pattern::Symbol(Symbol::Named("A".to_string())),
            Pattern::Exclude(Symbol::Named("B".to_string())),
        ]);
        assert!(pattern_contains_exclude(&pattern));

        // Test repetition with no exclude
        let pattern = Pattern::Repetition(
            Box::new(Pattern::Symbol(Symbol::Named("A".to_string()))),
            RepetitionQuantifier::OneOrMore,
        );
        assert!(!pattern_contains_exclude(&pattern));

        // Test repetition with exclude
        let pattern = Pattern::Repetition(
            Box::new(Pattern::Exclude(Symbol::Named("B".to_string()))),
            RepetitionQuantifier::OneOrMore,
        );
        assert!(pattern_contains_exclude(&pattern));

        // Test permute - no exclude (permute contains symbols, not patterns)
        let pattern = Pattern::Permute(vec![
            Symbol::Named("A".to_string()),
            Symbol::Named("B".to_string()),
        ]);
        assert!(!pattern_contains_exclude(&pattern));
    }

    #[test]
    fn test_rows_filter_expr_optimization() {
        // Test ONE ROW PER MATCH - should never return a filter
        let pattern = Pattern::Symbol(Symbol::Named("A".to_string()));
        let rpm = RowsPerMatch::OneRow;
        let filter = rows_filter_expr(&rpm, &pattern);
        assert!(filter.is_none());

        // Test ALL ROWS PER MATCH with no exclude pattern - should return None (no filter needed)
        let pattern = Pattern::Symbol(Symbol::Named("A".to_string()));
        let rpm = RowsPerMatch::AllRows(None);
        let filter = rows_filter_expr(&rpm, &pattern);
        assert!(filter.is_none()); // No filter needed since all rows are included

        // Test ALL ROWS PER MATCH with exclude pattern - should return IsIncludedRow filter
        let pattern = Pattern::Exclude(Symbol::Named("B".to_string()));
        let rpm = RowsPerMatch::AllRows(None);
        let filter = rows_filter_expr(&rpm, &pattern);
        assert!(filter.is_some()); // Filter needed since some rows might be excluded

        // Test ALL ROWS PER MATCH OMIT EMPTY MATCHES with no exclude - should return only classifier filter
        let pattern = Pattern::Symbol(Symbol::Named("A".to_string()));
        let rpm = RowsPerMatch::AllRows(Some(EmptyMatchesMode::Omit));
        let filter = rows_filter_expr(&rpm, &pattern);
        assert!(filter.is_some()); // Should have classifier filter but no IsIncludedRow filter

        // Test ALL ROWS PER MATCH OMIT EMPTY MATCHES with exclude - should return both filters
        let pattern = Pattern::Exclude(Symbol::Named("B".to_string()));
        let rpm = RowsPerMatch::AllRows(Some(EmptyMatchesMode::Omit));
        let filter = rows_filter_expr(&rpm, &pattern);
        assert!(filter.is_some()); // Should have both IsIncludedRow and classifier filters

        // Test ALL ROWS PER MATCH WITH UNMATCHED ROWS - should always return None
        let pattern = Pattern::Symbol(Symbol::Named("A".to_string()));
        let rpm = RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched));
        let filter = rows_filter_expr(&rpm, &pattern);
        assert!(filter.is_none()); // No filter needed for this mode
    }
}
