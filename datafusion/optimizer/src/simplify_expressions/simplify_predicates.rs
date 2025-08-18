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

use datafusion_common::{Column, Result, ScalarValue};
use datafusion_expr::{BinaryExpr, Cast, Expr, Operator};
use std::collections::BTreeMap;

/// Simplifies a list of predicates by removing redundancies.
///
/// This function takes a vector of predicate expressions and groups them by the column they reference.
/// Predicates that reference a single column and are comparison operations (e.g., >, >=, <, <=, =)
/// are analyzed to remove redundant conditions. For instance, `x > 5 AND x > 6` is simplified to
/// `x > 6`. Other predicates that do not fit this pattern are retained as-is.
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
        match &pred {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op:
                    Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Eq,
                right,
            }) => {
                let left_col = extract_column_from_expr(left);
                let right_col = extract_column_from_expr(right);
                if let (Some(col), Some(_)) = (&left_col, right.as_literal()) {
                    column_predicates.entry(col.clone()).or_default().push(pred);
                } else if let (Some(_), Some(col)) = (left.as_literal(), &right_col) {
                    column_predicates.entry(col.clone()).or_default().push(pred);
                } else {
                    other_predicates.push(pred);
                }
            }
            _ => other_predicates.push(pred),
        }
    }

    // Process each column's predicates to remove redundancies
    let mut result = other_predicates;
    for (_, preds) in column_predicates {
        let simplified = simplify_column_predicates(preds)?;
        result.extend(simplified);
    }

    Ok(result)
}

/// Simplifies predicates related to a single column.
///
/// This function processes a list of predicates that all reference the same column and
/// simplifies them based on their operators. It groups predicates into greater-than (>, >=),
/// less-than (<, <=), and equality (=) categories, then selects the most restrictive condition
/// in each category to reduce redundancy. For example, among `x > 5` and `x > 6`, only `x > 6`
/// is retained as it is more restrictive.
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

    for pred in predicates {
        match &pred {
            Expr::BinaryExpr(BinaryExpr { left: _, op, right }) => {
                match (op, right.as_literal().is_some()) {
                    (Operator::Gt, true)
                    | (Operator::Lt, false)
                    | (Operator::GtEq, true)
                    | (Operator::LtEq, false) => greater_predicates.push(pred),
                    (Operator::Lt, true)
                    | (Operator::Gt, false)
                    | (Operator::LtEq, true)
                    | (Operator::GtEq, false) => less_predicates.push(pred),
                    (Operator::Eq, _) => eq_predicates.push(pred),
                    _ => unreachable!("Unexpected operator: {}", op),
                }
            }
            _ => unreachable!("Unexpected predicate {}", pred.to_string()),
        }
    }

    let mut result = Vec::new();

    if !eq_predicates.is_empty() {
        // If there are many equality predicates, we can only keep one if they are all the same
        if eq_predicates.len() == 1
            || eq_predicates.iter().all(|e| e == &eq_predicates[0])
        {
            result.push(eq_predicates.pop().unwrap());
        } else {
            // If they are not the same, add a false predicate
            result.push(Expr::Literal(ScalarValue::Boolean(Some(false)), None));
        }
    }

    // Handle all greater-than-style predicates (keep the most restrictive - highest value)
    if !greater_predicates.is_empty() {
        if let Some(most_restrictive) =
            find_most_restrictive_predicate(&greater_predicates, true)?
        {
            result.push(most_restrictive);
        } else {
            result.extend(greater_predicates);
        }
    }

    // Handle all less-than-style predicates (keep the most restrictive - lowest value)
    if !less_predicates.is_empty() {
        if let Some(most_restrictive) =
            find_most_restrictive_predicate(&less_predicates, false)?
        {
            result.push(most_restrictive);
        } else {
            result.extend(less_predicates);
        }
    }

    Ok(result)
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
        if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = pred {
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
                        comparison == std::cmp::Ordering::Greater
                    } else {
                        comparison == std::cmp::Ordering::Less
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
        // Handle cases where the column might be wrapped in a cast or other operation
        Expr::Cast(Cast { expr, .. }) => extract_column_from_expr(expr),
        _ => None,
    }
}
