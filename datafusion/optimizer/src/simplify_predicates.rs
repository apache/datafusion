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

use datafusion_common::{Column, Result, ScalarValue};
use datafusion_expr::{BinaryExpr, Cast, Expr, Operator};
use std::collections::HashMap;

pub(crate) fn simplify_predicates(predicates: Vec<Expr>) -> Result<Vec<Expr>> {
    // Early return for simple cases
    if predicates.len() <= 1 {
        return Ok(predicates);
    }

    // Group predicates by their column reference
    let mut column_predicates: HashMap<Column, Vec<Expr>> =
        HashMap::with_capacity(predicates.len());
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
                let left_lit = left.is_literal();
                let right_lit = right.is_literal();
                if let (Some(col), true) = (&left_col, right_lit) {
                    column_predicates.entry(col.clone()).or_default().push(pred);
                } else if let (true, Some(col)) = (left_lit, &right_col) {
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
                let right_is_literal = right.is_literal();
                match (op, right_is_literal) {
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

    // If we have equality predicates, they're the most restrictive
    if !eq_predicates.is_empty() {
        if eq_predicates.len() > 1 {
            result.push(Expr::Literal(ScalarValue::Boolean(Some(false)), None));
        } else {
            result.push(eq_predicates[0].clone());
        }
    } else {
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
    }

    Ok(result)
}

fn find_most_restrictive_predicate(
    predicates: &[Expr],
    find_greater: bool,
) -> Result<Option<Expr>> {
    if predicates.is_empty() {
        return Ok(None);
    }

    let mut most_restrictive = predicates[0].clone();
    let mut best_value: Option<ScalarValue> = None;

    for pred in predicates {
        if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = pred {
            // Extract the literal value based on which side has it
            let mut scalar_value = None;
            if right.is_literal() {
                if let Expr::Literal(scalar, None) = right.as_ref() {
                    scalar_value = Some(scalar.clone());
                }
            } else if left.is_literal() {
                if let Expr::Literal(scalar, None) = left.as_ref() {
                    scalar_value = Some(scalar.clone());
                }
            }

            if let Some(scalar) = scalar_value {
                if let Some(current_best) = &best_value {
                    if let Some(comparison) = scalar.partial_cmp(current_best) {
                        let is_better = if find_greater {
                            comparison == std::cmp::Ordering::Greater
                        } else {
                            comparison == std::cmp::Ordering::Less
                        };

                        if is_better {
                            best_value = Some(scalar);
                            most_restrictive = pred.clone();
                        }
                    }
                } else {
                    best_value = Some(scalar);
                    most_restrictive = pred.clone();
                }
            }
        }
    }

    Ok(Some(most_restrictive))
}

fn extract_column_from_expr(expr: &Expr) -> Option<Column> {
    match expr {
        Expr::Column(col) => Some(col.clone()),
        // Handle cases where the column might be wrapped in a cast or other operation
        Expr::Cast(Cast { expr, .. }) => extract_column_from_expr(expr),
        _ => None,
    }
}
