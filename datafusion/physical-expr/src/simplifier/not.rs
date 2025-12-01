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

//! Simplify NOT expressions in physical expressions
//!
//! This module provides optimizations for NOT expressions such as:
//! - Double negation elimination: NOT(NOT(expr)) -> expr
//! - NOT with binary comparisons: NOT(a = b) -> a != b
//! - NOT with IN expressions: NOT(a IN (list)) -> a NOT IN (list)
//! - De Morgan's laws: NOT(A AND B) -> NOT A OR NOT B
//! - Constant folding: NOT(TRUE) -> FALSE, NOT(FALSE) -> TRUE

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::{tree_node::Transformed, Result, ScalarValue};
use datafusion_expr::Operator;

use crate::expressions::{in_list, lit, BinaryExpr, InListExpr, Literal, NotExpr};
use crate::PhysicalExpr;

/// Attempts to simplify NOT expressions
pub(crate) fn simplify_not_expr_impl(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    // Check if this is a NOT expression
    let not_expr = match expr.as_any().downcast_ref::<NotExpr>() {
        Some(not_expr) => not_expr,
        None => return Ok(Transformed::no(expr)),
    };

    let inner_expr = not_expr.arg();

    // Handle NOT(NOT(expr)) -> expr (double negation elimination)
    if let Some(inner_not) = inner_expr.as_any().downcast_ref::<NotExpr>() {
        // We eliminated double negation, so always return transformed=true
        return Ok(Transformed::yes(Arc::clone(inner_not.arg())));
    }

    // Handle NOT(literal) -> !literal
    if let Some(literal) = inner_expr.as_any().downcast_ref::<Literal>() {
        if let ScalarValue::Boolean(Some(val)) = literal.value() {
            return Ok(Transformed::yes(lit(ScalarValue::Boolean(Some(!val)))));
        }
        if let ScalarValue::Boolean(None) = literal.value() {
            return Ok(Transformed::yes(lit(ScalarValue::Boolean(None))));
        }
    }

    // Handle NOT(IN list) -> NOT IN list
    if let Some(in_list_expr) = inner_expr.as_any().downcast_ref::<InListExpr>() {
        // Create a new InList expression with negated flag flipped
        let negated = !in_list_expr.negated();
        let new_in_list = in_list(
            Arc::clone(in_list_expr.expr()),
            in_list_expr.list().to_vec(),
            &negated,
            schema,
        )?;
        return Ok(Transformed::yes(new_in_list));
    }

    // Handle NOT(binary_expr) where we can flip the operator
    if let Some(binary_expr) = inner_expr.as_any().downcast_ref::<BinaryExpr>() {
        if let Some(negated_op) = negate_operator(binary_expr.op()) {
            // Recursively simplify the left and right expressions first
            let left_simplified = simplify_not_expr(binary_expr.left(), schema)?;
            let right_simplified = simplify_not_expr(binary_expr.right(), schema)?;

            let new_binary = Arc::new(BinaryExpr::new(
                left_simplified.data,
                negated_op,
                right_simplified.data,
            ));
            // We flipped the operator, so always return transformed=true
            return Ok(Transformed::yes(new_binary));
        }

        // Handle De Morgan's laws for AND/OR
        match binary_expr.op() {
            Operator::And => {
                // NOT(A AND B) -> NOT A OR NOT B
                let not_left: Arc<dyn PhysicalExpr> =
                    Arc::new(NotExpr::new(Arc::clone(binary_expr.left())));
                let not_right: Arc<dyn PhysicalExpr> =
                    Arc::new(NotExpr::new(Arc::clone(binary_expr.right())));

                // Recursively simplify the NOT expressions
                let simplified_left = simplify_not_expr(&not_left, schema)?;
                let simplified_right = simplify_not_expr(&not_right, schema)?;

                let new_binary = Arc::new(BinaryExpr::new(
                    simplified_left.data,
                    Operator::Or,
                    simplified_right.data,
                ));
                return Ok(Transformed::yes(new_binary));
            }
            Operator::Or => {
                // NOT(A OR B) -> NOT A AND NOT B
                let not_left: Arc<dyn PhysicalExpr> =
                    Arc::new(NotExpr::new(Arc::clone(binary_expr.left())));
                let not_right: Arc<dyn PhysicalExpr> =
                    Arc::new(NotExpr::new(Arc::clone(binary_expr.right())));

                // Recursively simplify the NOT expressions
                let simplified_left = simplify_not_expr(&not_left, schema)?;
                let simplified_right = simplify_not_expr(&not_right, schema)?;

                let new_binary = Arc::new(BinaryExpr::new(
                    simplified_left.data,
                    Operator::And,
                    simplified_right.data,
                ));
                return Ok(Transformed::yes(new_binary));
            }
            _ => {}
        }
    }

    // If no simplification possible, return the original expression
    Ok(Transformed::no(expr))
}

pub fn simplify_not_expr(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    let mut current_expr = Arc::clone(expr);
    let mut overall_transformed = false;

    loop {
        let not_simplified = simplify_not_expr_impl(Arc::clone(&current_expr), schema)?;
        if not_simplified.transformed {
            overall_transformed = true;
            current_expr = not_simplified.data;
            continue;
        }

        if let Some(binary_expr) = current_expr.as_any().downcast_ref::<BinaryExpr>() {
            let left_simplified = simplify_not_expr(binary_expr.left(), schema)?;
            let right_simplified = simplify_not_expr(binary_expr.right(), schema)?;

            if left_simplified.transformed || right_simplified.transformed {
                let new_binary = Arc::new(BinaryExpr::new(
                    left_simplified.data,
                    *binary_expr.op(),
                    right_simplified.data,
                ));
                return Ok(Transformed::yes(new_binary));
            }
        }

        break;
    }

    if overall_transformed {
        Ok(Transformed::yes(current_expr))
    } else {
        Ok(Transformed::no(current_expr))
    }
}

/// Returns the negated version of a comparison operator, if possible
fn negate_operator(op: &Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::NotEq),
        Operator::NotEq => Some(Operator::Eq),
        Operator::Lt => Some(Operator::GtEq),
        Operator::LtEq => Some(Operator::Gt),
        Operator::Gt => Some(Operator::LtEq),
        Operator::GtEq => Some(Operator::Lt),
        Operator::IsDistinctFrom => Some(Operator::IsNotDistinctFrom),
        Operator::IsNotDistinctFrom => Some(Operator::IsDistinctFrom),
        // For other operators, we can't directly negate them
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, in_list, lit, BinaryExpr, NotExpr};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int32, false),
        ])
    }

    #[test]
    fn test_double_negation_elimination() -> Result<()> {
        let schema = test_schema();

        // Create NOT(NOT(b > 5))
        let inner_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("b", &schema)?,
            Operator::Gt,
            lit(ScalarValue::Int32(Some(5))),
        ));
        let inner_not = Arc::new(NotExpr::new(Arc::clone(&inner_expr)));
        let double_not: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(inner_not));

        let result = simplify_not_expr(&double_not, &schema)?;

        assert!(result.transformed);
        // Should be simplified back to the original b > 5
        assert_eq!(result.data.to_string(), inner_expr.to_string());
        Ok(())
    }

    #[test]
    fn test_not_literal() -> Result<()> {
        let schema = test_schema();

        // NOT(TRUE) -> FALSE
        let not_true = Arc::new(NotExpr::new(lit(ScalarValue::Boolean(Some(true)))));
        let result = simplify_not_expr_impl(not_true, &schema)?;
        assert!(result.transformed);

        if let Some(literal) = result.data.as_any().downcast_ref::<Literal>() {
            assert_eq!(literal.value(), &ScalarValue::Boolean(Some(false)));
        } else {
            panic!("Expected literal result");
        }

        // NOT(FALSE) -> TRUE
        let not_false: Arc<dyn PhysicalExpr> =
            Arc::new(NotExpr::new(lit(ScalarValue::Boolean(Some(false)))));
        let result = simplify_not_expr(&not_false, &schema)?;
        assert!(result.transformed);

        if let Some(literal) = result.data.as_any().downcast_ref::<Literal>() {
            assert_eq!(literal.value(), &ScalarValue::Boolean(Some(true)));
        } else {
            panic!("Expected literal result");
        }

        Ok(())
    }

    #[test]
    fn test_negate_comparison() -> Result<()> {
        let schema = test_schema();

        // NOT(b = 5) -> b != 5
        let eq_expr = Arc::new(BinaryExpr::new(
            col("b", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Int32(Some(5))),
        ));
        let not_eq: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(eq_expr));

        let result = simplify_not_expr(&not_eq, &schema)?;
        assert!(result.transformed);

        if let Some(binary) = result.data.as_any().downcast_ref::<BinaryExpr>() {
            assert_eq!(binary.op(), &Operator::NotEq);
        } else {
            panic!("Expected binary expression result");
        }

        Ok(())
    }

    #[test]
    fn test_demorgans_law_and() -> Result<()> {
        let schema = test_schema();

        // NOT(a AND b) -> NOT a OR NOT b
        let and_expr = Arc::new(BinaryExpr::new(
            col("a", &schema)?,
            Operator::And,
            col("b", &schema)?,
        ));
        let not_and: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(and_expr));

        let result = simplify_not_expr(&not_and, &schema)?;
        assert!(result.transformed);

        if let Some(binary) = result.data.as_any().downcast_ref::<BinaryExpr>() {
            assert_eq!(binary.op(), &Operator::Or);
            // Left and right should both be NOT expressions
            assert!(binary.left().as_any().downcast_ref::<NotExpr>().is_some());
            assert!(binary.right().as_any().downcast_ref::<NotExpr>().is_some());
        } else {
            panic!("Expected binary expression result");
        }

        Ok(())
    }

    #[test]
    fn test_demorgans_law_or() -> Result<()> {
        let schema = test_schema();

        // NOT(a OR b) -> NOT a AND NOT b
        let or_expr = Arc::new(BinaryExpr::new(
            col("a", &schema)?,
            Operator::Or,
            col("b", &schema)?,
        ));
        let not_or: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(or_expr));

        let result = simplify_not_expr(&not_or, &schema)?;
        assert!(result.transformed);

        if let Some(binary) = result.data.as_any().downcast_ref::<BinaryExpr>() {
            assert_eq!(binary.op(), &Operator::And);
            // Left and right should both be NOT expressions
            assert!(binary.left().as_any().downcast_ref::<NotExpr>().is_some());
            assert!(binary.right().as_any().downcast_ref::<NotExpr>().is_some());
        } else {
            panic!("Expected binary expression result");
        }

        Ok(())
    }

    #[test]
    fn test_demorgans_with_comparison_simplification() -> Result<()> {
        let schema = test_schema();

        // NOT(b = 1 AND b = 2) -> b != 1 OR b != 2
        // This tests the combination of De Morgan's law and operator negation
        let eq1 = Arc::new(BinaryExpr::new(
            col("b", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Int32(Some(1))),
        ));
        let eq2 = Arc::new(BinaryExpr::new(
            col("b", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Int32(Some(2))),
        ));
        let and_expr = Arc::new(BinaryExpr::new(eq1, Operator::And, eq2));
        let not_and: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(and_expr));

        let result = simplify_not_expr(&not_and, &schema)?;
        assert!(result.transformed, "Expression should be transformed");

        // Verify the result is an OR expression
        if let Some(or_binary) = result.data.as_any().downcast_ref::<BinaryExpr>() {
            assert_eq!(or_binary.op(), &Operator::Or, "Top level should be OR");

            // Verify left side is b != 1
            if let Some(left_binary) =
                or_binary.left().as_any().downcast_ref::<BinaryExpr>()
            {
                assert_eq!(left_binary.op(), &Operator::NotEq, "Left should be NotEq");
            } else {
                panic!("Expected left to be a binary expression with !=");
            }

            // Verify right side is b != 2
            if let Some(right_binary) =
                or_binary.right().as_any().downcast_ref::<BinaryExpr>()
            {
                assert_eq!(right_binary.op(), &Operator::NotEq, "Right should be NotEq");
            } else {
                panic!("Expected right to be a binary expression with !=");
            }
        } else {
            panic!("Expected binary OR expression result");
        }

        Ok(())
    }

    #[test]
    fn test_not_of_not_and_not() -> Result<()> {
        let schema = test_schema();

        // NOT(NOT(a) AND NOT(b)) -> a OR b
        // This tests the combination of De Morgan's law and double negation elimination
        let not_a = Arc::new(NotExpr::new(col("a", &schema)?));
        let not_b = Arc::new(NotExpr::new(col("b", &schema)?));
        let and_expr = Arc::new(BinaryExpr::new(not_a, Operator::And, not_b));
        let not_and: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(and_expr));

        let result = simplify_not_expr(&not_and, &schema)?;
        assert!(result.transformed, "Expression should be transformed");

        // Verify the result is an OR expression
        if let Some(or_binary) = result.data.as_any().downcast_ref::<BinaryExpr>() {
            assert_eq!(or_binary.op(), &Operator::Or, "Top level should be OR");

            // Verify left side is just 'a'
            assert!(or_binary.left().as_any().downcast_ref::<NotExpr>().is_none(),
                "Left should not be a NOT expression, it should be simplified to just 'a'");

            // Verify right side is just 'b'
            assert!(or_binary.right().as_any().downcast_ref::<NotExpr>().is_none(),
                "Right should not be a NOT expression, it should be simplified to just 'b'");
        } else {
            panic!("Expected binary OR expression result");
        }

        Ok(())
    }

    #[test]
    fn test_not_in_list() -> Result<()> {
        let schema = test_schema();

        // NOT(b IN (1, 2, 3)) -> b NOT IN (1, 2, 3)
        let list = vec![
            lit(ScalarValue::Int32(Some(1))),
            lit(ScalarValue::Int32(Some(2))),
            lit(ScalarValue::Int32(Some(3))),
        ];
        let in_list_expr = in_list(col("b", &schema)?, list, &false, &schema)?;
        let not_in: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(in_list_expr));

        let result = simplify_not_expr(&not_in, &schema)?;
        assert!(result.transformed, "Expression should be transformed");

        // Verify the result is an InList expression with negated=true
        if let Some(in_list_result) = result.data.as_any().downcast_ref::<InListExpr>() {
            assert!(
                in_list_result.negated(),
                "InList should be negated (NOT IN)"
            );
            assert_eq!(
                in_list_result.list().len(),
                3,
                "Should have 3 items in list"
            );
        } else {
            panic!("Expected InListExpr result");
        }

        Ok(())
    }

    #[test]
    fn test_not_not_in_list() -> Result<()> {
        let schema = test_schema();

        // NOT(b NOT IN (1, 2, 3)) -> b IN (1, 2, 3)
        let list = vec![
            lit(ScalarValue::Int32(Some(1))),
            lit(ScalarValue::Int32(Some(2))),
            lit(ScalarValue::Int32(Some(3))),
        ];
        let not_in_list_expr = in_list(col("b", &schema)?, list, &true, &schema)?;
        let not_not_in: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(not_in_list_expr));

        let result = simplify_not_expr(&not_not_in, &schema)?;
        assert!(result.transformed, "Expression should be transformed");

        // Verify the result is an InList expression with negated=false
        if let Some(in_list_result) = result.data.as_any().downcast_ref::<InListExpr>() {
            assert!(
                !in_list_result.negated(),
                "InList should not be negated (IN)"
            );
            assert_eq!(
                in_list_result.list().len(),
                3,
                "Should have 3 items in list"
            );
        } else {
            panic!("Expected InListExpr result");
        }

        Ok(())
    }

    #[test]
    fn test_double_not_in_list() -> Result<()> {
        let schema = test_schema();

        // NOT(NOT(b IN (1, 2, 3))) -> b IN (1, 2, 3)
        let list = vec![
            lit(ScalarValue::Int32(Some(1))),
            lit(ScalarValue::Int32(Some(2))),
            lit(ScalarValue::Int32(Some(3))),
        ];
        let in_list_expr = in_list(col("b", &schema)?, list, &false, &schema)?;
        let not_in = Arc::new(NotExpr::new(in_list_expr));
        let double_not: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(not_in));

        let result = simplify_not_expr(&double_not, &schema)?;
        assert!(result.transformed, "Expression should be transformed");

        // After double negation elimination, we should get back the original IN expression
        if let Some(in_list_result) = result.data.as_any().downcast_ref::<InListExpr>() {
            assert!(
                !in_list_result.negated(),
                "InList should not be negated (IN)"
            );
            assert_eq!(
                in_list_result.list().len(),
                3,
                "Should have 3 items in list"
            );
        } else {
            panic!("Expected InListExpr result");
        }

        Ok(())
    }

    #[test]
    fn test_deeply_nested_not() -> Result<()> {
        let schema = test_schema();

        // Create a deeply nested NOT expression: NOT(NOT(NOT(...NOT(b > 5)...)))
        // This tests that we don't get stack overflow with many nested NOTs
        let inner_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("b", &schema)?,
            Operator::Gt,
            lit(ScalarValue::Int32(Some(5))),
        ));

        let mut expr = Arc::clone(&inner_expr);
        // Create 20000 layers of NOT
        for _ in 0..20000 {
            expr = Arc::new(NotExpr::new(expr));
        }

        let result = simplify_not_expr(&expr, &schema)?;

        // With 20000 NOTs (even number), should simplify back to the original expression
        assert_eq!(
            result.data.to_string(),
            inner_expr.to_string(),
            "Should simplify back to original expression"
        );

        // Manually dismantle the deep input expression to avoid Stack Overflow on Drop
        // If we just let `expr` go out of scope, Rust's recursive Drop will blow the stack.
        // We peel off layers one by one.
        while let Some(not_expr) = expr.as_any().downcast_ref::<NotExpr>() {
            // Clone the child (Arc increment).
            // Now child has 2 refs: one in parent, one in `child`.
            let child = Arc::clone(not_expr.arg());

            // Reassign `expr` to `child`.
            // This drops the old `expr` (Parent).
            // Parent refcount -> 0, Parent is dropped.
            // Parent drops its reference to Child.
            // Child refcount decrements 2 -> 1.
            // Child is NOT dropped recursively because we still hold it in `expr`
            expr = child;
        }

        Ok(())
    }
}
