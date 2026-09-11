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

//! Simplifies predicates by reducing redundant or overlapping conditions.
//!
//! This module provides functionality to optimize logical predicates used in query planning
//! by eliminating redundant conditions, thus reducing the number of predicates to evaluate.
//! Unlike the simplifier in `simplify_expressions/simplify_exprs.rs`, which focuses on
//! general expression simplification (e.g., constant folding and algebraic simplifications),
//! this module specifically targets predicate optimization by handling containment relationships.
//! For example, it can simplify `x > 5 AND x > 6` to just `x > 6`, as the latter condition
//! encompasses the former, resulting in fewer checks during query execution.
//! Conjunctions that no value can satisfy, such as `x > 6 AND x < 5`, are replaced with
//! `false` so that later rules can prune the plan they filter.

use super::utils::{is_false, is_null};
use datafusion_common::{Column, Result, ScalarValue, internal_err};
use datafusion_expr::{BinaryExpr, Expr, Operator, lit};
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// Simplifies a list of predicates by removing redundancies.
///
/// This function takes a vector of predicate expressions and groups them by the column they reference.
/// Predicates that reference a single column and are comparison operations (e.g., >, >=, <, <=, =, !=)
/// are analyzed to remove redundant conditions. For instance, `x > 5 AND x > 6` is simplified to
/// `x > 6`. Predicates that contradict each other, such as `x > 6 AND x < 5`, reduce the whole
/// conjunction to `false`. Other predicates that do not fit this pattern are retained as-is.
///
/// # Arguments
/// * `predicates` - A vector of `Expr` representing the predicates to simplify.
///
/// # Returns
/// A `Result` containing a vector of simplified `Expr` predicates.
pub fn simplify_predicates(predicates: Vec<Expr>) -> Result<Vec<Expr>> {
    // Early return for simple cases
    if predicates.len() <= 1 {
        return Ok(predicates);
    }

    // Group predicates by their column reference
    let mut column_predicates: BTreeMap<Column, Vec<Expr>> = BTreeMap::new();
    let mut other_predicates = Vec::new();

    for pred in predicates {
        match pred {
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(
                    op,
                    Operator::Gt
                        | Operator::GtEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Eq
                        | Operator::NotEq
                ) && !is_null(&left)
                    && !is_null(&right) =>
            {
                if let (Some(col), Some(_)) =
                    (extract_column_from_expr(&left), right.as_literal())
                {
                    column_predicates
                        .entry(col)
                        .or_default()
                        .push(Expr::BinaryExpr(BinaryExpr { left, op, right }));
                } else if let (Some(_), Some(col), Some(swapped_op)) = (
                    left.as_literal(),
                    extract_column_from_expr(&right),
                    op.swap(),
                ) {
                    // Put the literal on the right so that predicates differing only in
                    // operand order compare equal below
                    column_predicates
                        .entry(col)
                        .or_default()
                        .push(Expr::BinaryExpr(BinaryExpr {
                            left: right,
                            op: swapped_op,
                            right: left,
                        }));
                } else {
                    other_predicates.push(Expr::BinaryExpr(BinaryExpr {
                        left,
                        op,
                        right,
                    }));
                }
            }
            _ => other_predicates.push(pred),
        }
    }

    // Process each column's predicates to remove redundancies
    let mut result = other_predicates;
    for (_, preds) in column_predicates {
        let simplified = simplify_column_predicates(preds)?;
        if simplified.iter().any(is_false) {
            return Ok(always_false());
        }
        result.extend(simplified);
    }

    Ok(result)
}

/// Simplifies predicates related to a single column.
///
/// This function processes a list of predicates that all reference the same column and
/// simplifies them based on their operators. It groups predicates into greater-than (>, >=),
/// less-than (<, <=), equality (=) and inequality (!=) categories, then selects the most
/// restrictive condition in each category to reduce redundancy. For example, among `x > 5`
/// and `x > 6`, only `x > 6` is retained as it is more restrictive.
///
/// The reduced conditions are then compared with each other. An equality subsumes every
/// other condition it satisfies, an inequality is dropped once a bound already excludes its
/// value, and conditions that cannot hold at the same time, such as `x > 6 AND x < 5`,
/// reduce the whole list to a single `false` literal.
///
/// # Arguments
/// * `predicates` - A vector of `Expr` representing predicates for a single column.
///
/// # Returns
/// A `Result` containing a vector of simplified `Expr` predicates for the column.
fn simplify_column_predicates(predicates: Vec<Expr>) -> Result<Vec<Expr>> {
    if predicates.len() <= 1 {
        return Ok(predicates);
    }

    // Group by operator type, but combining similar operators
    let mut greater_predicates = Vec::new(); // Combines > and >=
    let mut less_predicates = Vec::new(); // Combines < and <=
    let mut eq_predicates = Vec::new();
    let mut not_eq_predicates = Vec::new();

    for pred in predicates {
        match &pred {
            Expr::BinaryExpr(BinaryExpr { op, .. }) => match op {
                Operator::Gt | Operator::GtEq => greater_predicates.push(pred),
                Operator::Lt | Operator::LtEq => less_predicates.push(pred),
                Operator::Eq => eq_predicates.push(pred),
                Operator::NotEq => not_eq_predicates.push(pred),
                _ => unreachable!("Unexpected operator: {}", op),
            },
            _ => unreachable!("Unexpected predicate {}", pred.to_string()),
        }
    }

    // Reduce each direction to its most restrictive bound: the highest value for the
    // greater-than-style predicates and the lowest value for the less-than-style ones.
    let lower_bound = find_most_restrictive_predicate(&greater_predicates, true)?;
    let upper_bound = find_most_restrictive_predicate(&less_predicates, false)?;

    if let Some(eq_predicate) = eq_predicates.pop() {
        let (_, value) = op_and_literal(&eq_predicate)?;

        // An equality pins the column to a single value, so it subsumes every other
        // predicate on that column: either the value satisfies them, which makes them
        // redundant, or it does not and no row can pass the conjunction.
        if !satisfies_all(
            value,
            eq_predicates
                .iter()
                .chain(not_eq_predicates.iter())
                .chain(lower_bound.iter())
                .chain(upper_bound.iter()),
        )? {
            return Ok(always_false());
        }
        return Ok(vec![eq_predicate]);
    }

    // Bounds that leave no room for any value cannot be satisfied together
    if let (Some(lower), Some(upper)) = (&lower_bound, &upper_bound)
        && is_empty_range(lower, upper)?
    {
        return Ok(always_false());
    }

    // An inequality is redundant once a bound already excludes the value it rules out
    let mut result = Vec::new();
    for not_eq_predicate in not_eq_predicates {
        let (_, value) = op_and_literal(&not_eq_predicate)?;
        if satisfies_all(value, lower_bound.iter().chain(upper_bound.iter()))? {
            result.push(not_eq_predicate);
        }
    }

    result.extend(lower_bound);
    result.extend(upper_bound);

    Ok(result)
}

/// Determines whether a lower and an upper bound leave no value that satisfies both.
///
/// For example `x > 5 AND x < 3` can never be true, and neither can `x > 1 AND x < 1`
/// because both comparisons are strict. `x >= 1 AND x <= 1` on the other hand is
/// satisfied by `x = 1`.
///
/// # Arguments
/// * `lower` - A predicate using `>` or `>=`.
/// * `upper` - A predicate using `<` or `<=`.
///
/// # Returns
/// A `Result` containing `true` if the two bounds contradict each other.
fn is_empty_range(lower: &Expr, upper: &Expr) -> Result<bool> {
    let (lower_op, lower_value) = op_and_literal(lower)?;
    let (upper_op, upper_value) = op_and_literal(upper)?;

    Ok(match lower_value.try_cmp(upper_value)? {
        Ordering::Less => false,
        Ordering::Greater => true,
        Ordering::Equal => lower_op == Operator::Gt || upper_op == Operator::Lt,
    })
}

/// Determines whether a row whose column equals `value` passes all of `predicates`.
///
/// # Arguments
/// * `value` - The literal the column is known to be equal to.
/// * `predicates` - Predicates on that same column, each of the form `column <op> literal`.
///
/// # Returns
/// A `Result` containing `false` as soon as one of the predicates rejects `value`.
fn satisfies_all<'a>(
    value: &ScalarValue,
    predicates: impl IntoIterator<Item = &'a Expr>,
) -> Result<bool> {
    for predicate in predicates {
        let (op, bound) = op_and_literal(predicate)?;
        let ordering = value.try_cmp(bound)?;
        let satisfied = match op {
            Operator::Gt => ordering == Ordering::Greater,
            Operator::GtEq => ordering != Ordering::Less,
            Operator::Lt => ordering == Ordering::Less,
            Operator::LtEq => ordering != Ordering::Greater,
            Operator::Eq => ordering == Ordering::Equal,
            Operator::NotEq => ordering != Ordering::Equal,
            _ => return internal_err!("Unexpected operator: {op}"),
        };
        if !satisfied {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Extracts the operator and the literal of a `column <op> literal` predicate.
///
/// [`simplify_predicates`] normalizes the predicates it groups by column so that the
/// literal is always the right operand, so any other shape is an internal error.
///
/// # Arguments
/// * `predicate` - A reference to an `Expr` to destructure.
///
/// # Returns
/// A `Result` holding the operator and the literal the predicate compares against.
fn op_and_literal(predicate: &Expr) -> Result<(Operator, &ScalarValue)> {
    if let Expr::BinaryExpr(BinaryExpr { op, right, .. }) = predicate
        && let Some(literal) = right.as_literal()
    {
        Ok((*op, literal))
    } else {
        internal_err!("Unexpected predicate {predicate}")
    }
}

/// Builds the predicate list of a conjunction that no row can satisfy.
fn always_false() -> Vec<Expr> {
    vec![lit(false)]
}

/// Finds the most restrictive predicate from a list based on literal values.
///
/// This function iterates through a list of predicates to identify the most restrictive one
/// by comparing their literal values. For greater-than predicates, the highest value is most
/// restrictive, while for less-than predicates, the lowest value is most restrictive.
///
/// # Arguments
/// * `predicates` - A slice of `Expr` representing predicates to compare.
/// * `find_greater` - A boolean indicating whether to find the highest value (true for >, >=)
///   or the lowest value (false for <, <=).
///
/// # Returns
/// A `Result` containing an `Option<Expr>` with the most restrictive predicate, if any.
fn find_most_restrictive_predicate(
    predicates: &[Expr],
    find_greater: bool,
) -> Result<Option<Expr>> {
    if predicates.is_empty() {
        return Ok(None);
    }

    let mut most_restrictive_idx = 0;
    let mut best_value: Option<&ScalarValue> = None;

    for (idx, pred) in predicates.iter().enumerate() {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = pred {
            // Extract the literal value based on which side has it
            let scalar_value = match (right.as_literal(), left.as_literal()) {
                (Some(scalar), _) => Some(scalar),
                (_, Some(scalar)) => Some(scalar),
                _ => None,
            };

            if let Some(scalar) = scalar_value {
                if let Some(current_best) = best_value {
                    let comparison = scalar.try_cmp(current_best)?;
                    let is_better = if find_greater {
                        comparison == Ordering::Greater
                            || (comparison == Ordering::Equal && op == &Operator::Gt)
                    } else {
                        comparison == Ordering::Less
                            || (comparison == Ordering::Equal && op == &Operator::Lt)
                    };

                    if is_better {
                        best_value = Some(scalar);
                        most_restrictive_idx = idx;
                    }
                } else {
                    best_value = Some(scalar);
                    most_restrictive_idx = idx;
                }
            }
        }
    }

    Ok(Some(predicates[most_restrictive_idx].clone()))
}

/// Extracts a column reference from an expression, if present.
///
/// This function checks if the given expression is a column reference or contains one,
/// such as within a cast operation. It returns the `Column` if found.
///
/// # Arguments
/// * `expr` - A reference to an `Expr` to inspect for a column reference.
///
/// # Returns
/// An `Option<Column>` containing the column reference if found, otherwise `None`.
fn extract_column_from_expr(expr: &Expr) -> Option<Column> {
    match expr {
        Expr::Column(col) => Some(col.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::{cast, col, lit};

    #[test]
    fn test_simplify_predicates_with_cast() {
        // Test that predicates on cast expressions are not grouped with predicates on the raw column
        // a < 5 AND CAST(a AS varchar) < 'abc' AND a < 6
        // Should simplify to:
        // a < 5 AND CAST(a AS varchar) < 'abc'

        let predicates = vec![
            col("a").lt(lit(5i32)),
            cast(col("a"), DataType::Utf8).lt(lit("abc")),
            col("a").lt(lit(6i32)),
        ];

        let result = simplify_predicates(predicates).unwrap();

        // Should have 2 predicates: a < 5 and CAST(a AS varchar) < 'abc'
        assert_eq!(result.len(), 2);

        // Check that the cast predicate is preserved
        let has_cast_predicate = result.iter().any(|p| {
            matches!(p, Expr::BinaryExpr(BinaryExpr { 
                left, 
                op: Operator::Lt, 
                right 
            }) if matches!(left.as_ref(), Expr::Cast(_)) && right == &Box::new(lit("abc")))
        });
        assert!(has_cast_predicate, "Cast predicate should be preserved");

        // Check that we have the more restrictive column predicate (a < 5)
        let has_column_predicate = result.iter().any(|p| {
            matches!(p, Expr::BinaryExpr(BinaryExpr { 
                left, 
                op: Operator::Lt, 
                right 
            }) if left == &Box::new(col("a")) && right == &Box::new(lit(5i32)))
        });
        assert!(has_column_predicate, "Should have a < 5 predicate");
    }

    #[test]
    fn test_extract_column_ignores_cast() {
        // Test that extract_column_from_expr does not extract columns from cast expressions
        let cast_expr = cast(col("a"), DataType::Utf8);
        assert_eq!(extract_column_from_expr(&cast_expr), None);

        // Test that it still extracts from direct column references
        let col_expr = col("a");
        assert_eq!(extract_column_from_expr(&col_expr), Some(Column::from("a")));
    }

    #[test]
    fn test_eq_predicates_are_matched_regardless_of_operand_order() {
        // a = 5 AND 5 = a is a single condition, not a contradiction
        let predicates = vec![col("a").eq(lit(5i32)), lit(5i32).eq(col("a"))];

        let result = simplify_predicates(predicates).unwrap();

        assert_eq!(result, vec![col("a").eq(lit(5i32))]);
    }

    #[test]
    fn test_strict_bound_wins_when_literal_is_on_the_left() {
        // a >= 5 AND 5 < a is a > 5; keeping a >= 5 would let a = 5 through
        let predicates = vec![col("a").gt_eq(lit(5i32)), lit(5i32).lt(col("a"))];

        let result = simplify_predicates(predicates).unwrap();

        assert_eq!(result, vec![col("a").gt(lit(5i32))]);
    }

    #[test]
    fn test_simplify_predicates_direct_columns_only() {
        // Test that only predicates on direct columns are simplified together
        let predicates = vec![
            col("a").lt(lit(5i32)),
            col("a").lt(lit(3i32)),
            col("b").gt(lit(10i32)),
            col("b").gt(lit(20i32)),
        ];

        let result = simplify_predicates(predicates).unwrap();

        // Should have 2 predicates: a < 3 and b > 20 (most restrictive for each column)
        assert_eq!(result.len(), 2);

        // Check for a < 3
        let has_a_predicate = result.iter().any(|p| {
            matches!(p, Expr::BinaryExpr(BinaryExpr { 
                left, 
                op: Operator::Lt, 
                right 
            }) if left == &Box::new(col("a")) && right == &Box::new(lit(3i32)))
        });
        assert!(has_a_predicate, "Should have a < 3 predicate");

        // Check for b > 20
        let has_b_predicate = result.iter().any(|p| {
            matches!(p, Expr::BinaryExpr(BinaryExpr { 
                left, 
                op: Operator::Gt, 
                right 
            }) if left == &Box::new(col("b")) && right == &Box::new(lit(20i32)))
        });
        assert!(has_b_predicate, "Should have b > 20 predicate");
    }

    #[test]
    fn test_equality_subsumes_predicates_it_satisfies() {
        // The value a is pinned to passes every other predicate, so only a = 5 is left
        for predicates in [
            vec![
                col("a").eq(lit(5i32)),
                col("a").gt(lit(3i32)),
                col("a").lt_eq(lit(5i32)),
                col("a").not_eq(lit(6i32)),
            ],
            vec![col("a").eq(lit(5i32)), col("a").gt_eq(lit(5i32))],
        ] {
            let result = simplify_predicates(predicates.clone()).unwrap();

            assert_eq!(result, vec![col("a").eq(lit(5i32))], "for {predicates:?}");
        }
    }

    #[test]
    fn test_equality_rejected_by_another_predicate_is_unsatisfiable() {
        // Each of these pins a to a value that the other predicate excludes
        for predicates in [
            vec![col("a").eq(lit(5i32)), col("a").eq(lit(6i32))],
            vec![col("a").eq(lit(5i32)), col("a").not_eq(lit(5i32))],
            vec![col("a").eq(lit(5i32)), col("a").gt(lit(5i32))],
            vec![col("a").eq(lit(5i32)), col("a").gt_eq(lit(6i32))],
            vec![col("a").eq(lit(5i32)), col("a").lt(lit(3i32))],
            vec![col("a").eq(lit(5i32)), col("a").lt_eq(lit(3i32))],
        ] {
            let result = simplify_predicates(predicates.clone()).unwrap();

            assert_eq!(result, vec![lit(false)], "for {predicates:?}");
        }
    }

    #[test]
    fn test_disjoint_bounds_are_unsatisfiable() {
        // The strict bounds exclude the value they share, so none of these can hold
        for predicates in [
            vec![col("a").gt(lit(5i32)), col("a").lt(lit(3i32))],
            vec![col("a").gt(lit(1i32)), col("a").lt(lit(1i32))],
            vec![col("a").gt_eq(lit(1i32)), col("a").lt(lit(1i32))],
            vec![col("a").gt(lit(1i32)), col("a").lt_eq(lit(1i32))],
        ] {
            let result = simplify_predicates(predicates.clone()).unwrap();

            assert_eq!(result, vec![lit(false)], "for {predicates:?}");
        }
    }

    #[test]
    fn test_satisfiable_bounds_are_kept() {
        // The second case only leaves a = 1, but both bounds are inclusive so it holds
        for predicates in [
            vec![col("a").gt(lit(1i32)), col("a").lt(lit(9i32))],
            vec![col("a").gt_eq(lit(1i32)), col("a").lt_eq(lit(1i32))],
        ] {
            let result = simplify_predicates(predicates.clone()).unwrap();

            assert_eq!(result, predicates, "for {predicates:?}");
        }
    }

    #[test]
    fn test_unsatisfiable_column_discards_other_predicates() {
        // Nothing can make the conjunction true once one column contradicts itself
        let predicates = vec![
            col("b").gt(lit(0i32)),
            col("a").gt(lit(5i32)),
            col("a").lt(lit(3i32)),
        ];

        let result = simplify_predicates(predicates).unwrap();

        assert_eq!(result, vec![lit(false)]);
    }

    #[test]
    fn test_not_eq_excluded_by_a_bound_is_removed() {
        // a > 10 already rules out a = 5
        let predicates = vec![col("a").gt(lit(10i32)), col("a").not_eq(lit(5i32))];

        let result = simplify_predicates(predicates).unwrap();

        assert_eq!(result, vec![col("a").gt(lit(10i32))]);
    }

    #[test]
    fn test_not_eq_inside_the_bounds_is_kept() {
        // 5 is within a > 1, so the inequality still filters rows
        let predicates = vec![col("a").gt(lit(1i32)), col("a").not_eq(lit(5i32))];

        let result = simplify_predicates(predicates).unwrap();

        assert_eq!(
            result,
            vec![col("a").not_eq(lit(5i32)), col("a").gt(lit(1i32))]
        );
    }

    #[test]
    fn test_null_comparisons_are_left_alone() {
        // Comparisons with NULL are never true, so they carry no bound to reason
        // about. Treating NULL as an ordinary value would drop `a > NULL` for being
        // less restrictive than `a > 5`, or drop `a != NULL` for being excluded by
        // `a > 5`, and either would let rows through that must be filtered out.
        for predicates in [
            vec![
                col("a").gt(lit(ScalarValue::Int32(None))),
                col("a").gt(lit(5i32)),
            ],
            vec![
                col("a").not_eq(lit(ScalarValue::Int32(None))),
                col("a").gt(lit(5i32)),
            ],
            vec![
                col("a").gt(lit(ScalarValue::Int32(None))),
                col("a").lt(lit(3i32)),
            ],
            vec![
                col("a").gt(lit(ScalarValue::Int32(None))),
                col("a").lt(lit(ScalarValue::Int32(None))),
            ],
        ] {
            let result = simplify_predicates(predicates.clone()).unwrap();

            assert_eq!(result, predicates, "for {predicates:?}");
        }
    }
}
