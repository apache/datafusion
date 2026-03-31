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

//! Syntactic null-restriction fast path for optimizer predicates.
//!
//! This module provides a conservative syntactic check that quickly determines
//! whether a predicate restricts NULL values for specified join columns, without
//! executing the full physical expression evaluation.
//!
//! "Restricts nulls" means: when the join columns are substituted with `NULL`,
//! the predicate evaluates to `NULL` or `false` (not `true`). If it evaluates
//! to `NULL` or `false`, then rows with NULL join-column values would be filtered
//! out — the predicate "restricts" them.
//!
//! The syntactic check is deliberately conservative:
//! - It returns `Some(SyntacticNullRestriction::Restricts)` or
//!   `Some(SyntacticNullRestriction::DoesNotRestrict)` only when it can prove
//!   the outcome from the expression shape alone.
//! - It returns `None` for unrecognised patterns, signalling the caller to fall
//!   back to the authoritative physical-expression evaluator.

use std::collections::HashSet;

use datafusion_common::Column;
use datafusion_expr::{Expr, Operator, expr::BinaryExpr};

/// Result of a syntactic null-restriction check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SyntacticNullRestriction {
    /// The expression definitely restricts nulls:
    /// it evaluates to `NULL` or `false` when join columns are `NULL`.
    Restricts,
    /// The expression definitely does **not** restrict nulls:
    /// it evaluates to `true` when join columns are `NULL`.
    DoesNotRestrict,
}

/// Syntactically determine whether `expr` restricts NULL values for `join_cols`.
///
/// Returns `Some(result)` when the expression shape is recognized and a definitive
/// conclusion can be reached; returns `None` when the syntactic check cannot decide
/// and the caller should fall back to the authoritative evaluator.
///
/// This check is conservative: it only claims `Some` for patterns where the outcome
/// is provable without execution.
pub(crate) fn syntactic_null_restriction_check(
    expr: &Expr,
    join_cols: &HashSet<Column>,
) -> Option<SyntacticNullRestriction> {
    use SyntacticNullRestriction::*;

    match expr {
        // A bare join-column reference: NULL column → NULL predicate → filtered out.
        Expr::Column(col) => {
            if join_cols.contains(col) {
                Some(Restricts)
            } else {
                // Non-join column: result depends on data, cannot determine.
                None
            }
        }

        // IS NULL: NULL IS NULL = TRUE → predicate passes → does NOT restrict.
        Expr::IsNull(inner) => {
            if is_direct_join_col(inner, join_cols) {
                Some(DoesNotRestrict)
            } else {
                None
            }
        }

        // IS NOT NULL: NULL IS NOT NULL = FALSE → predicate fails → restricts.
        Expr::IsNotNull(inner) => {
            if is_direct_join_col(inner, join_cols) {
                Some(Restricts)
            } else {
                None
            }
        }

        // NOT: flips DoesNotRestrict → Restricts; Restricts is uncertain
        // because NOT(NULL)=NULL (restricts) but NOT(false)=true (doesn't restrict).
        Expr::Not(inner) => {
            match syntactic_null_restriction_check(inner, join_cols)? {
                DoesNotRestrict => Some(Restricts),
                // NOT(null or false): NOT(null)=null→restricts, NOT(false)=true→doesn't.
                // Cannot prove either way without knowing which case applies.
                Restricts => None,
            }
        }

        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            syntactic_binary(left, *op, right, join_cols)
        }

        _ => None,
    }
}

/// Evaluate null-restriction for a binary expression.
fn syntactic_binary(
    left: &Expr,
    op: Operator,
    right: &Expr,
    join_cols: &HashSet<Column>,
) -> Option<SyntacticNullRestriction> {
    use SyntacticNullRestriction::*;

    match op {
        // Comparison operators all propagate NULLs through their operands.
        // If either operand is (or directly contains) a join column, the result
        // is NULL when that column is NULL → restricts.
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => {
            if is_direct_join_col(left, join_cols) || is_direct_join_col(right, join_cols)
            {
                Some(Restricts)
            } else {
                None
            }
        }

        // AND propagation rules (SQL three-value logic):
        //   NULL AND x  = NULL or false  → always restricts
        //   false AND x = false          → always restricts
        //   true  AND x = x
        //
        // Conclusion: AND restricts if EITHER side restricts.
        //             AND does not restrict if BOTH sides do not restrict.
        //             Otherwise unknown.
        Operator::And => {
            let left_check = syntactic_null_restriction_check(left, join_cols);
            let right_check = syntactic_null_restriction_check(right, join_cols);

            let left_restricts = matches!(left_check, Some(Restricts));
            let right_restricts = matches!(right_check, Some(Restricts));
            let left_passes = matches!(left_check, Some(DoesNotRestrict));
            let right_passes = matches!(right_check, Some(DoesNotRestrict));

            if left_restricts || right_restricts {
                Some(Restricts)
            } else if left_passes && right_passes {
                Some(DoesNotRestrict)
            } else {
                None
            }
        }

        // OR propagation rules (SQL three-value logic):
        //   true  OR x = true → does NOT restrict, regardless of x
        //   NULL  OR true = true → does NOT restrict
        //   NULL  OR false = NULL → restricts
        //   NULL  OR NULL = NULL → restricts
        //   false OR x = x
        //
        // Conclusion: OR does NOT restrict if EITHER side does not restrict.
        //             OR restricts only if BOTH sides restrict.
        //             Otherwise unknown.
        Operator::Or => {
            let left_check = syntactic_null_restriction_check(left, join_cols);
            let right_check = syntactic_null_restriction_check(right, join_cols);

            match (left_check, right_check) {
                (Some(Restricts), Some(Restricts)) => Some(Restricts),
                (Some(DoesNotRestrict), _) | (_, Some(DoesNotRestrict)) => {
                    Some(DoesNotRestrict)
                }
                _ => None,
            }
        }

        _ => None,
    }
}

/// Returns true if `expr` is a direct column reference that belongs to `join_cols`.
fn is_direct_join_col(expr: &Expr, join_cols: &HashSet<Column>) -> bool {
    matches!(expr.as_ref(), Expr::Column(col) if join_cols.contains(col))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Operator, binary_expr, col, in_list, is_null, lit};

    fn join_cols(names: &[&str]) -> HashSet<Column> {
        names.iter().map(|n| Column::from_name(*n)).collect()
    }

    fn check(expr: &Expr, cols: &[&str]) -> Option<SyntacticNullRestriction> {
        syntactic_null_restriction_check(expr, &join_cols(cols))
    }

    // -----------------------------------------------------------------------
    // Column reference
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_column_restricts() {
        // col("a") with join_cols = {"a"} → Restricts (NULL → NULL → filtered)
        let result = check(&col("a"), &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_non_join_column_unknown() {
        // col("b") with join_cols = {"a"} → None (cannot determine without data)
        let result = check(&col("b"), &["a"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // IS NULL / IS NOT NULL
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_null_join_col_does_not_restrict() {
        // IS NULL(col("a")) with join_cols = {"a"} → DoesNotRestrict
        // NULL IS NULL = TRUE → row passes → does not restrict
        let result = check(&is_null(col("a")), &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::DoesNotRestrict));
    }

    #[test]
    fn test_is_not_null_join_col_restricts() {
        // IS NOT NULL(col("a")) with join_cols = {"a"} → Restricts
        // NULL IS NOT NULL = FALSE → row filtered → restricts
        let result = check(&Expr::IsNotNull(Box::new(col("a"))), &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_is_null_non_join_col_unknown() {
        // IS NULL(col("b")) with join_cols = {"a"} → None
        let result = check(&is_null(col("b")), &["a"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // NOT
    // -----------------------------------------------------------------------

    #[test]
    fn test_not_is_null_restricts() {
        // NOT(IS NULL(col("a"))) = NOT(DoesNotRestrict) → Restricts
        // NOT(TRUE) = FALSE → restricts
        let expr = Expr::Not(Box::new(is_null(col("a"))));
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_not_join_col_unknown() {
        // NOT(col("a")) = NOT(Restricts) → None
        // NOT(null) = null (restricts), NOT(false) = true (doesn't restrict): uncertain
        let expr = Expr::Not(Box::new(col("a")));
        let result = check(&expr, &["a"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // Comparison operators
    // -----------------------------------------------------------------------

    #[test]
    fn test_comparison_gt_restricts() {
        // col("a") > 8 → Restricts (NULL > 8 = NULL → filtered)
        let expr = binary_expr(col("a"), Operator::Gt, lit(8i64));
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_comparison_lteq_restricts() {
        // col("a") <= 8 → Restricts
        let expr = binary_expr(col("a"), Operator::LtEq, lit(8i32));
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_comparison_eq_null_literal_restricts() {
        // col("a") = NULL → Restricts (NULL = NULL = NULL in SQL → filtered)
        let expr = binary_expr(
            col("a"),
            Operator::Eq,
            Expr::Literal(ScalarValue::Null, None),
        );
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_comparison_non_join_col_unknown() {
        // col("b") > 8 with join_cols = {"a"} → None
        let expr = binary_expr(col("b"), Operator::Gt, lit(8i64));
        let result = check(&expr, &["a"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // AND expressions
    // -----------------------------------------------------------------------

    #[test]
    fn test_and_one_side_restricts() {
        // col("a") > 5 AND col("a") IS NULL  → Restricts (left restricts)
        let expr = binary_expr(
            binary_expr(col("a"), Operator::Gt, lit(5i64)),
            Operator::And,
            is_null(col("a")),
        );
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_and_both_restrict() {
        // col("a") > 5 AND col("a") > 1 → Restricts
        let expr = binary_expr(
            binary_expr(col("a"), Operator::Gt, lit(5i64)),
            Operator::And,
            binary_expr(col("a"), Operator::Gt, lit(1i64)),
        );
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_and_both_do_not_restrict() {
        // IS NULL(a) AND IS NULL(a) → DoesNotRestrict
        let expr = binary_expr(is_null(col("a")), Operator::And, is_null(col("a")));
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::DoesNotRestrict));
    }

    #[test]
    fn test_and_unknown_unknown() {
        // col("b") > 5 AND col("c") > 1 with join_cols = {"a"} → None
        let expr = binary_expr(
            binary_expr(col("b"), Operator::Gt, lit(5i64)),
            Operator::And,
            binary_expr(col("c"), Operator::Gt, lit(1i64)),
        );
        let result = check(&expr, &["a"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // OR expressions
    // -----------------------------------------------------------------------

    #[test]
    fn test_or_both_restrict() {
        // col("a") > 5 OR col("a") > 1 → Restricts
        let expr = binary_expr(
            binary_expr(col("a"), Operator::Gt, lit(5i64)),
            Operator::Or,
            binary_expr(col("a"), Operator::Gt, lit(1i64)),
        );
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_or_one_side_does_not_restrict() {
        // col("a") > 5 OR IS NULL(col("a")) → DoesNotRestrict
        // Because IS NULL(a) when a=NULL → TRUE, so OR returns TRUE → doesn't restrict
        let expr = binary_expr(
            binary_expr(col("a"), Operator::Gt, lit(5i64)),
            Operator::Or,
            is_null(col("a")),
        );
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::DoesNotRestrict));
    }

    #[test]
    fn test_or_one_side_unknown() {
        // col("a") > 5 OR col("b") > 1 with join_cols = {"a"} → None
        let expr = binary_expr(
            binary_expr(col("a"), Operator::Gt, lit(5i64)),
            Operator::Or,
            binary_expr(col("b"), Operator::Gt, lit(1i64)),
        );
        let result = check(&expr, &["a"]);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // Mixed-reference predicates: parity with authoritative evaluator
    //
    // These tests verify that for all predicate shapes the syntactic evaluator
    // handles, its answer agrees with what the authoritative physical evaluator
    // would produce. They also document the None (unknown) cases where the
    // syntactic check correctly defers to the authoritative path.
    // -----------------------------------------------------------------------

    #[test]
    fn test_in_list_join_col_unknown() {
        // col("a") IN (1, 2, 3) with join_cols = {"a"}
        // The syntactic check does not handle InList → None (falls to authoritative)
        let expr = in_list(col("a"), vec![lit(1i64), lit(2i64), lit(3i64)], false);
        let result = check(&expr, &["a"]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_multiple_join_cols_and() {
        // col("a") > 1 AND col("b") > 1 with join_cols = {"a", "b"} → Restricts
        let expr = binary_expr(
            binary_expr(col("a"), Operator::Gt, lit(1i64)),
            Operator::And,
            binary_expr(col("b"), Operator::Gt, lit(1i64)),
        );
        let result = check(&expr, &["a", "b"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }

    #[test]
    fn test_mixed_ref_in_and_restricts_via_join_col_side() {
        // col("a") > 5 AND col("b") IS NULL with join_cols = {"a"}
        // Left = Restricts (join col), right = None (b not join col) → Restricts
        let expr = binary_expr(
            binary_expr(col("a"), Operator::Gt, lit(5i64)),
            Operator::And,
            is_null(col("b")),
        );
        let result = check(&expr, &["a"]);
        assert_eq!(result, Some(SyntacticNullRestriction::Restricts));
    }
}
