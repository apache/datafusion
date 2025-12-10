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

//! Unwrap casts in binary comparisons for physical expressions
//!
//! This module provides optimization for physical expressions similar to the logical
//! optimizer's unwrap_cast module. It attempts to remove casts from comparisons to
//! literals by applying the casts to the literals if possible.
//!
//! The optimization improves performance by:
//! 1. Reducing runtime cast operations on column data
//! 2. Enabling better predicate pushdown opportunities
//! 3. Optimizing filter expressions in physical plans
//!
//! # Example
//!
//! Physical expression: `cast(column as INT64) > INT64(10)`
//! Optimized to: `column > INT32(10)` (assuming column is INT32)

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion_common::{
    Result, ScalarValue,
    tree_node::{Transformed, TreeNode},
};
use datafusion_expr::Operator;
use datafusion_expr_common::casts::try_cast_literal_to_type;

use crate::PhysicalExpr;
use crate::expressions::{BinaryExpr, CastExpr, Literal, TryCastExpr, lit};

/// Attempts to unwrap casts in comparison expressions.
pub(crate) fn unwrap_cast_in_comparison(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    expr.transform_down(|e| {
        if let Some(binary) = e.as_any().downcast_ref::<BinaryExpr>()
            && let Some(unwrapped) = try_unwrap_cast_binary(binary, schema)?
        {
            return Ok(Transformed::yes(unwrapped));
        }
        Ok(Transformed::no(e))
    })
}

/// Try to unwrap casts in binary expressions
fn try_unwrap_cast_binary(
    binary: &BinaryExpr,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // Case 1: cast(left_expr) op literal
    if let (Some((inner_expr, _cast_type)), Some(literal)) = (
        extract_cast_info(binary.left()),
        binary.right().as_any().downcast_ref::<Literal>(),
    ) && binary.op().supports_propagation()
        && let Some(unwrapped) = try_unwrap_cast_comparison(
            Arc::clone(inner_expr),
            literal.value(),
            *binary.op(),
            schema,
        )?
    {
        return Ok(Some(unwrapped));
    }

    // Case 2: literal op cast(right_expr)
    if let (Some(literal), Some((inner_expr, _cast_type))) = (
        binary.left().as_any().downcast_ref::<Literal>(),
        extract_cast_info(binary.right()),
    ) {
        // For literal op cast(expr), we need to swap the operator
        if let Some(swapped_op) = binary.op().swap()
            && binary.op().supports_propagation()
            && let Some(unwrapped) = try_unwrap_cast_comparison(
                Arc::clone(inner_expr),
                literal.value(),
                swapped_op,
                schema,
            )?
        {
            return Ok(Some(unwrapped));
        }
        // If the operator cannot be swapped, we skip this optimization case
        // but don't prevent other optimizations
    }

    Ok(None)
}

/// Extract cast information from a physical expression
///
/// If the expression is a CAST(expr, datatype) or TRY_CAST(expr, datatype),
/// returns Some((inner_expr, target_datatype)). Otherwise returns None.
fn extract_cast_info(
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<(&Arc<dyn PhysicalExpr>, &DataType)> {
    if let Some(cast) = expr.as_any().downcast_ref::<CastExpr>() {
        Some((cast.expr(), cast.cast_type()))
    } else if let Some(try_cast) = expr.as_any().downcast_ref::<TryCastExpr>() {
        Some((try_cast.expr(), try_cast.cast_type()))
    } else {
        None
    }
}

/// Try to unwrap a cast in comparison by moving the cast to the literal
fn try_unwrap_cast_comparison(
    inner_expr: Arc<dyn PhysicalExpr>,
    literal_value: &ScalarValue,
    op: Operator,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // Get the data type of the inner expression
    let inner_type = inner_expr.data_type(schema)?;

    // Try to cast the literal to the inner expression's type
    if let Some(casted_literal) = try_cast_literal_to_type(literal_value, &inner_type) {
        let literal_expr = lit(casted_literal);
        let binary_expr = BinaryExpr::new(inner_expr, op, literal_expr);
        return Ok(Some(Arc::new(binary_expr)));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;

    /// Check if an expression is a cast expression
    fn is_cast_expr(expr: &Arc<dyn PhysicalExpr>) -> bool {
        expr.as_any().downcast_ref::<CastExpr>().is_some()
            || expr.as_any().downcast_ref::<TryCastExpr>().is_some()
    }

    /// Check if a binary expression is suitable for cast unwrapping
    fn is_binary_expr_with_cast_and_literal(binary: &BinaryExpr) -> bool {
        // Check if left is cast and right is literal
        let left_cast_right_literal = is_cast_expr(binary.left())
            && binary.right().as_any().downcast_ref::<Literal>().is_some();

        // Check if left is literal and right is cast
        let left_literal_right_cast =
            binary.left().as_any().downcast_ref::<Literal>().is_some()
                && is_cast_expr(binary.right());

        left_cast_right_literal || left_literal_right_cast
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int64, false),
            Field::new("c3", DataType::Utf8, false),
        ])
    }

    #[test]
    fn test_unwrap_cast_in_binary_comparison() {
        let schema = test_schema();

        // Create: cast(c1 as INT64) > INT64(10)
        let column_expr = col("c1", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(10i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Gt, literal_expr));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should be transformed
        assert!(result.transformed);

        // The result should be: c1 > INT32(10)
        let optimized = result.data;
        let optimized_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();

        // Check that left side is no longer a cast
        assert!(!is_cast_expr(optimized_binary.left()));

        // Check that right side is a literal with the correct type and value
        let right_literal = optimized_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(right_literal.value(), &ScalarValue::Int32(Some(10)));
    }

    #[test]
    fn test_unwrap_cast_with_literal_on_left() {
        let schema = test_schema();

        // Create: INT64(10) < cast(c1 as INT64)
        let column_expr = col("c1", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(10i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::Lt, cast_expr));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should be transformed
        assert!(result.transformed);

        // The result should be equivalent to: c1 > INT32(10)
        let optimized = result.data;
        let optimized_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();

        // Check the operator was swapped
        assert_eq!(*optimized_binary.op(), Operator::Gt);
    }

    #[test]
    fn test_no_unwrap_when_types_unsupported() {
        let schema = Schema::new(vec![Field::new("f1", DataType::Float32, false)]);

        // Create: cast(f1 as FLOAT64) > FLOAT64(10.5)
        let column_expr = col("f1", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Float64, None));
        let literal_expr = lit(10.5f64);
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Gt, literal_expr));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should NOT be transformed (floating point types not supported)
        assert!(!result.transformed);
    }

    #[test]
    fn test_is_binary_expr_with_cast_and_literal() {
        let schema = test_schema();

        let column_expr = col("c1", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(10i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Gt, literal_expr));
        let binary_ref = binary_expr.as_any().downcast_ref::<BinaryExpr>().unwrap();

        assert!(is_binary_expr_with_cast_and_literal(binary_ref));
    }

    #[test]
    fn test_unwrap_cast_literal_on_left_side() {
        // Test case for: literal <= cast(column)
        // This was the specific case that caused the bug
        let schema = Schema::new(vec![Field::new(
            "decimal_col",
            DataType::Decimal128(9, 2),
            true,
        )]);

        // Create: Decimal128(400) <= cast(decimal_col as Decimal128(22, 2))
        let column_expr = col("decimal_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Decimal128(22, 2),
            None,
        ));
        let literal_expr = lit(ScalarValue::Decimal128(Some(400), 22, 2));
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::LtEq, cast_expr));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should be transformed
        assert!(result.transformed);

        // The result should be: decimal_col >= Decimal128(400, 9, 2)
        let optimized = result.data;
        let optimized_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();

        // Check operator was swapped correctly
        assert_eq!(*optimized_binary.op(), Operator::GtEq);

        // Check that left side is the column without cast
        assert!(!is_cast_expr(optimized_binary.left()));

        // Check that right side is a literal with the correct type
        let right_literal = optimized_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(
            right_literal.value().data_type(),
            DataType::Decimal128(9, 2)
        );
    }

    #[test]
    fn test_unwrap_cast_with_different_comparison_operators() {
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int32, false)]);

        // Test all comparison operators with literal on the left
        let operators = vec![
            (Operator::Lt, Operator::Gt),
            (Operator::LtEq, Operator::GtEq),
            (Operator::Gt, Operator::Lt),
            (Operator::GtEq, Operator::LtEq),
            (Operator::Eq, Operator::Eq),
            (Operator::NotEq, Operator::NotEq),
        ];

        for (original_op, expected_op) in operators {
            // Create: INT64(100) op cast(int_col as INT64)
            let column_expr = col("int_col", &schema).unwrap();
            let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
            let literal_expr = lit(100i64);
            let binary_expr =
                Arc::new(BinaryExpr::new(literal_expr, original_op, cast_expr));

            // Apply unwrap cast optimization
            let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

            // Should be transformed
            assert!(result.transformed);

            let optimized = result.data;
            let optimized_binary =
                optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();

            // Check the operator was swapped correctly
            assert_eq!(
                *optimized_binary.op(),
                expected_op,
                "Failed for operator {original_op:?} -> {expected_op:?}"
            );

            // Check that left side has no cast
            assert!(!is_cast_expr(optimized_binary.left()));

            // Check that the literal was cast to the column type
            let right_literal = optimized_binary
                .right()
                .as_any()
                .downcast_ref::<Literal>()
                .unwrap();
            assert_eq!(right_literal.value(), &ScalarValue::Int32(Some(100)));
        }
    }

    #[test]
    fn test_unwrap_cast_with_decimal_types() {
        // Test various decimal precision/scale combinations
        let test_cases = vec![
            // (column_precision, column_scale, cast_precision, cast_scale, value)
            (9, 2, 22, 2, 400),
            (10, 3, 20, 3, 1000),
            (5, 1, 10, 1, 99),
        ];

        for (col_p, col_s, cast_p, cast_s, value) in test_cases {
            let schema = Schema::new(vec![Field::new(
                "decimal_col",
                DataType::Decimal128(col_p, col_s),
                true,
            )]);

            // Test both: cast(column) op literal AND literal op cast(column)

            // Case 1: cast(column) > literal
            let column_expr = col("decimal_col", &schema).unwrap();
            let cast_expr = Arc::new(CastExpr::new(
                Arc::clone(&column_expr),
                DataType::Decimal128(cast_p, cast_s),
                None,
            ));
            let literal_expr = lit(ScalarValue::Decimal128(Some(value), cast_p, cast_s));
            let binary_expr =
                Arc::new(BinaryExpr::new(cast_expr, Operator::Gt, literal_expr));

            let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
            assert!(result.transformed);

            // Case 2: literal < cast(column)
            let cast_expr = Arc::new(CastExpr::new(
                column_expr,
                DataType::Decimal128(cast_p, cast_s),
                None,
            ));
            let literal_expr = lit(ScalarValue::Decimal128(Some(value), cast_p, cast_s));
            let binary_expr =
                Arc::new(BinaryExpr::new(literal_expr, Operator::Lt, cast_expr));

            let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
            assert!(result.transformed);
        }
    }

    #[test]
    fn test_unwrap_cast_with_null_literals() {
        // Test with NULL literals to ensure they're handled correctly
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int32, true)]);

        // Create: cast(int_col as INT64) = NULL
        let column_expr = col("int_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let null_literal = lit(ScalarValue::Int64(None));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, null_literal));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should be transformed
        assert!(result.transformed);

        // Verify the NULL was cast to the column type
        let optimized = result.data;
        let optimized_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();
        let right_literal = optimized_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(right_literal.value(), &ScalarValue::Int32(None));
    }

    #[test]
    fn test_unwrap_cast_with_try_cast() {
        // Test that TryCast expressions are also unwrapped correctly
        let schema = Schema::new(vec![Field::new("str_col", DataType::Utf8, true)]);

        // Create: try_cast(str_col as INT64) > INT64(100)
        let column_expr = col("str_col", &schema).unwrap();
        let try_cast_expr = Arc::new(TryCastExpr::new(column_expr, DataType::Int64));
        let literal_expr = lit(100i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(try_cast_expr, Operator::Gt, literal_expr));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should NOT be transformed (string to int cast not supported)
        assert!(!result.transformed);
    }

    #[test]
    fn test_unwrap_cast_preserves_non_comparison_operators() {
        // Test that non-comparison operators in AND/OR expressions are preserved
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int32, false)]);

        // Create: cast(int_col as INT64) > INT64(10) AND cast(int_col as INT64) < INT64(20)
        let column_expr = col("int_col", &schema).unwrap();

        let cast1 = Arc::new(CastExpr::new(
            Arc::clone(&column_expr),
            DataType::Int64,
            None,
        ));
        let lit1 = lit(10i64);
        let compare1 = Arc::new(BinaryExpr::new(cast1, Operator::Gt, lit1));

        let cast2 = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let lit2 = lit(20i64);
        let compare2 = Arc::new(BinaryExpr::new(cast2, Operator::Lt, lit2));

        let and_expr = Arc::new(BinaryExpr::new(compare1, Operator::And, compare2));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(and_expr, &schema).unwrap();

        // Should be transformed
        assert!(result.transformed);

        // Verify the AND operator is preserved
        let optimized = result.data;
        let and_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();
        assert_eq!(*and_binary.op(), Operator::And);

        // Both sides should have their casts unwrapped
        let left_binary = and_binary
            .left()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();
        let right_binary = and_binary
            .right()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();

        assert!(!is_cast_expr(left_binary.left()));
        assert!(!is_cast_expr(right_binary.left()));
    }

    #[test]
    fn test_try_cast_unwrapping() {
        let schema = test_schema();

        // Create: try_cast(c1 as INT64) <= INT64(100)
        let column_expr = col("c1", &schema).unwrap();
        let try_cast_expr = Arc::new(TryCastExpr::new(column_expr, DataType::Int64));
        let literal_expr = lit(100i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(try_cast_expr, Operator::LtEq, literal_expr));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should be transformed to: c1 <= INT32(100)
        assert!(result.transformed);

        let optimized = result.data;
        let optimized_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();

        // Verify the try_cast was removed
        assert!(!is_cast_expr(optimized_binary.left()));

        // Verify the literal was converted
        let right_literal = optimized_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(right_literal.value(), &ScalarValue::Int32(Some(100)));
    }

    #[test]
    fn test_non_swappable_operator() {
        // Test case with an operator that cannot be swapped
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int32, false)]);

        // Create: INT64(10) + cast(int_col as INT64)
        // The Plus operator cannot be swapped, so this should not be transformed
        let column_expr = col("int_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(10i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::Plus, cast_expr));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should NOT be transformed because Plus cannot be swapped
        assert!(!result.transformed);
    }

    #[test]
    fn test_cast_that_cannot_be_unwrapped_overflow() {
        // Test case where the literal value would overflow the target type
        let schema = Schema::new(vec![Field::new("small_int", DataType::Int8, false)]);

        // Create: cast(small_int as INT64) > INT64(1000)
        // This should NOT be unwrapped because 1000 cannot fit in Int8 (max value is 127)
        let column_expr = col("small_int", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(1000i64); // Value too large for Int8
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Gt, literal_expr));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        // Should NOT be transformed due to overflow
        assert!(!result.transformed);
    }

    #[test]
    fn test_complex_nested_expression() {
        let schema = test_schema();

        // Create a more complex expression with nested casts
        // (cast(c1 as INT64) > INT64(10)) AND (cast(c2 as INT32) = INT32(20))
        let c1_expr = col("c1", &schema).unwrap();
        let c1_cast = Arc::new(CastExpr::new(c1_expr, DataType::Int64, None));
        let c1_literal = lit(10i64);
        let c1_binary = Arc::new(BinaryExpr::new(c1_cast, Operator::Gt, c1_literal));

        let c2_expr = col("c2", &schema).unwrap();
        let c2_cast = Arc::new(CastExpr::new(c2_expr, DataType::Int32, None));
        let c2_literal = lit(20i32);
        let c2_binary = Arc::new(BinaryExpr::new(c2_cast, Operator::Eq, c2_literal));

        // Create AND expression
        let and_expr = Arc::new(BinaryExpr::new(c1_binary, Operator::And, c2_binary));

        // Apply unwrap cast optimization
        let result = unwrap_cast_in_comparison(and_expr, &schema).unwrap();

        // Should be transformed
        assert!(result.transformed);

        // Verify both sides of the AND were optimized
        let optimized = result.data;
        let and_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();

        // Left side should be: c1 > INT32(10)
        let left_binary = and_binary
            .left()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();
        assert!(!is_cast_expr(left_binary.left()));
        let left_literal = left_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(left_literal.value(), &ScalarValue::Int32(Some(10)));

        // Right side should be: c2 = INT64(20) (c2 is already INT64, literal cast to match)
        let right_binary = and_binary
            .right()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();
        assert!(!is_cast_expr(right_binary.left()));
        let right_literal = right_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(right_literal.value(), &ScalarValue::Int64(Some(20)));
    }
}
