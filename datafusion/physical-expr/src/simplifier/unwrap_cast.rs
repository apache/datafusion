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

//! Unwrap casts in physical binary comparisons by moving safe casts to literals.
//! Mirrors the logical unwrap-cast rules for physical expressions.

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion_common::{Result, ScalarValue, tree_node::Transformed};
use datafusion_expr::Operator;
use datafusion_expr_common::casts::{
    CastComparisonRewrite, try_cast_literal_for_comparison_unwrap,
};

use crate::PhysicalExpr;
use crate::expressions::{BinaryExpr, CastExpr, Literal, TryCastExpr, lit};

/// Attempts to unwrap casts in comparison expressions.
pub(crate) fn unwrap_cast_in_comparison(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
        && let Some(unwrapped) = try_unwrap_cast_binary(binary, schema)?
    {
        return Ok(Transformed::yes(unwrapped));
    }
    Ok(Transformed::no(expr))
}

/// Try to unwrap casts in binary expressions
fn try_unwrap_cast_binary(
    binary: &BinaryExpr,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // cast(left_expr) op literal
    if let (Some((inner_expr, cast_type)), Some(literal)) = (
        extract_cast_info(binary.left()),
        binary.right().downcast_ref::<Literal>(),
    ) && binary.op().supports_propagation()
        && let Some(unwrapped) = try_unwrap_cast_comparison(
            Arc::clone(inner_expr),
            cast_type,
            literal.value(),
            *binary.op(),
            schema,
        )?
    {
        return Ok(Some(unwrapped));
    }

    // literal op cast(right_expr)
    if let (Some(literal), Some((inner_expr, cast_type))) = (
        binary.left().downcast_ref::<Literal>(),
        extract_cast_info(binary.right()),
    ) && let Some(swapped_op) = binary.op().swap()
        && binary.op().supports_propagation()
        && let Some(unwrapped) = try_unwrap_cast_comparison(
            Arc::clone(inner_expr),
            cast_type,
            literal.value(),
            swapped_op,
            schema,
        )?
    {
        return Ok(Some(unwrapped));
    }

    Ok(None)
}

/// Extracts `(inner_expr, target_type)` from CAST and TRY_CAST expressions.
fn extract_cast_info(
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<(&Arc<dyn PhysicalExpr>, &DataType)> {
    if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        Some((cast.expr(), cast.cast_type()))
    } else if let Some(try_cast) = expr.downcast_ref::<TryCastExpr>() {
        Some((try_cast.expr(), try_cast.cast_type()))
    } else {
        None
    }
}

/// Try to unwrap a cast in comparison by moving the cast to the literal.
fn try_unwrap_cast_comparison(
    inner_expr: Arc<dyn PhysicalExpr>,
    cast_type: &DataType,
    literal_value: &ScalarValue,
    op: Operator,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let inner_type = inner_expr.data_type(schema)?;

    if let Some(CastComparisonRewrite::Literal(casted_literal)) =
        try_cast_literal_for_comparison_unwrap(literal_value, &inner_type, cast_type, op)
    {
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
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::{ScalarValue, tree_node::TreeNode};
    use datafusion_expr::Operator;

    fn is_cast_expr(expr: &Arc<dyn PhysicalExpr>) -> bool {
        expr.downcast_ref::<CastExpr>().is_some()
            || expr.downcast_ref::<TryCastExpr>().is_some()
    }

    fn is_binary_expr_with_cast_and_literal(binary: &BinaryExpr) -> bool {
        let left_cast_right_literal = is_cast_expr(binary.left())
            && binary.right().downcast_ref::<Literal>().is_some();

        let left_literal_right_cast = binary.left().downcast_ref::<Literal>().is_some()
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
    fn test_unwrap_cast_simple() {
        let schema = test_schema();

        let column_expr = col("c1", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(10i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Gt, literal_expr));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        assert!(result.transformed);
        let optimized = result.data.downcast_ref::<BinaryExpr>().unwrap();
        assert!(!is_cast_expr(optimized.left()));
        let right_literal = optimized.right().downcast_ref::<Literal>().unwrap();
        assert_eq!(right_literal.value(), &ScalarValue::Int32(Some(10)));
    }

    #[test]
    fn test_unwrap_cast_with_literal_on_left() {
        let schema = test_schema();

        let column_expr = col("c1", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(10i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::Lt, cast_expr));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        assert!(result.transformed);
    }

    #[test]
    fn test_no_unwrap_when_types_unsupported() {
        let schema = Schema::new(vec![Field::new("f1", DataType::Float32, false)]);

        let column_expr = col("f1", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Float64, None));
        let literal_expr = lit(10.5f64);
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Gt, literal_expr));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

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
        assert!(is_binary_expr_with_cast_and_literal(&binary_expr));
    }

    #[test]
    fn test_no_unwrap_cast_decimal_literal_on_left_side() {
        // Decimal casts are not currently unwrapped.
        let schema = Schema::new(vec![Field::new(
            "decimal_col",
            DataType::Decimal128(9, 2),
            true,
        )]);

        let column_expr = col("decimal_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Decimal128(22, 2),
            None,
        ));
        let literal_expr = lit(ScalarValue::Decimal128(Some(400), 22, 2));
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::LtEq, cast_expr));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        assert!(!result.transformed);
    }

    #[test]
    fn test_unwrap_cast_with_different_comparison_operators() {
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int32, false)]);

        let operators = vec![
            (Operator::Lt, Operator::Gt),
            (Operator::LtEq, Operator::GtEq),
            (Operator::Gt, Operator::Lt),
            (Operator::GtEq, Operator::LtEq),
            (Operator::Eq, Operator::Eq),
            (Operator::NotEq, Operator::NotEq),
        ];

        for (original_op, _expected_op) in operators {
            let column_expr = col("int_col", &schema).unwrap();
            let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
            let literal_expr = lit(100i64);
            let binary_expr =
                Arc::new(BinaryExpr::new(literal_expr, original_op, cast_expr));

            let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

            assert!(result.transformed, "expected unwrap for {original_op:?}");
        }
    }

    #[test]
    fn test_no_unwrap_timestamp_precision_downcast() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let cast_type = DataType::Timestamp(TimeUnit::Millisecond, None);

        // Nanoseconds -> milliseconds loses precision.
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            let column_expr = col("ts", &schema).unwrap();
            let cast_expr = Arc::new(CastExpr::new(column_expr, cast_type.clone(), None));
            let literal_expr = lit(ScalarValue::TimestampMillisecond(Some(1000), None));
            let binary_expr = Arc::new(BinaryExpr::new(cast_expr, op, literal_expr));
            let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
            assert!(!result.transformed, "unexpected unwrap for {op:?}");

            // Literal-left comparisons must preserve the lossy cast too.
            let column_expr = col("ts", &schema).unwrap();
            let cast_expr = Arc::new(CastExpr::new(column_expr, cast_type.clone(), None));
            let literal_expr = lit(ScalarValue::TimestampMillisecond(Some(1000), None));
            let binary_expr = Arc::new(BinaryExpr::new(literal_expr, op, cast_expr));
            let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
            assert!(
                !result.transformed,
                "unexpected literal-left unwrap for {op:?}"
            );
        }

        let column_expr = col("ts", &schema).unwrap();
        let try_cast_expr = Arc::new(TryCastExpr::new(column_expr, cast_type));
        let literal_expr = lit(ScalarValue::TimestampMillisecond(Some(1000), None));
        let binary_expr =
            Arc::new(BinaryExpr::new(try_cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let utc = Some("+0:00".into());
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, utc.clone()),
            false,
        )]);
        let column_expr = col("ts", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Timestamp(TimeUnit::Millisecond, utc.clone()),
            None,
        ));
        let literal_expr = lit(ScalarValue::TimestampMillisecond(Some(1000), utc));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);
    }

    #[test]
    fn test_no_unwrap_lossy_date_timestamp_casts() {
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("date", DataType::Date32, false),
        ]);

        let column_expr = col("ts", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Date32, None));
        let literal_expr = lit(ScalarValue::Date32(Some(0)));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let column_expr = col("ts", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Date32, None));
        let literal_expr = lit(ScalarValue::Date32(Some(0)));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::LtEq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let column_expr = col("date", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            None,
        ));
        let literal_expr = lit(ScalarValue::TimestampMillisecond(Some(0), None));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let column_expr = col("date", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            None,
        ));
        let literal_expr = lit(ScalarValue::TimestampMillisecond(Some(0), None));
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::Gt, cast_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);
    }

    #[test]
    fn test_unwrap_non_lossy_timestamp_precision_casts() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);

        let column_expr = col("ts", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ));
        let literal_expr = lit(ScalarValue::TimestampNanosecond(Some(1000), None));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(result.transformed);
        let optimized_binary = result.data.downcast_ref::<BinaryExpr>().unwrap();
        assert!(!is_cast_expr(optimized_binary.left()));

        let column_expr = col("ts", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ));
        let literal_expr = lit(ScalarValue::TimestampNanosecond(Some(1000), None));
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::Gt, cast_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(result.transformed);
        let optimized_binary = result.data.downcast_ref::<BinaryExpr>().unwrap();
        assert!(!is_cast_expr(optimized_binary.left()));
        assert_eq!(*optimized_binary.op(), Operator::Lt);

        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]);
        let column_expr = col("ts", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ));
        let literal_expr =
            lit(ScalarValue::TimestampNanosecond(Some(1_000_000_000), None));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Lt, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(result.transformed);

        let optimized_binary = result.data.downcast_ref::<BinaryExpr>().unwrap();
        assert!(!is_cast_expr(optimized_binary.left()));

        let right_literal = optimized_binary.right().downcast_ref::<Literal>().unwrap();
        assert_eq!(
            right_literal.value(),
            &ScalarValue::TimestampMillisecond(Some(1000), None)
        );

        let column_expr = col("ts", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ));
        let literal_expr =
            lit(ScalarValue::TimestampNanosecond(Some(1_000_000_001), None));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let column_expr = col("ts", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            None,
        ));
        let literal_expr =
            lit(ScalarValue::TimestampNanosecond(Some(1_000_000_000), None));
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::Gt, cast_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(result.transformed);

        let optimized_binary = result.data.downcast_ref::<BinaryExpr>().unwrap();
        assert!(!is_cast_expr(optimized_binary.left()));
        assert_eq!(*optimized_binary.op(), Operator::Lt);

        let right_literal = optimized_binary.right().downcast_ref::<Literal>().unwrap();
        assert_eq!(
            right_literal.value(),
            &ScalarValue::TimestampMillisecond(Some(1000), None)
        );
    }

    #[test]
    fn test_no_unwrap_lossy_decimal_precision_casts() {
        let schema = Schema::new(vec![Field::new(
            "decimal_col",
            DataType::Decimal128(18, 2),
            true,
        )]);

        let column_expr = col("decimal_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Decimal128(18, 1),
            None,
        ));
        let literal_expr = lit(ScalarValue::Decimal128(Some(123), 18, 1));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let column_expr = col("decimal_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Decimal128(10, 2),
            None,
        ));
        let literal_expr = lit(ScalarValue::Decimal128(Some(12345), 10, 2));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let column_expr = col("decimal_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Decimal128(12, 4),
            None,
        ));
        let literal_expr = lit(ScalarValue::Decimal128(Some(1234500), 12, 4));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let column_expr = col("decimal_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(ScalarValue::Int64(Some(16)));
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::Lt, cast_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);
    }

    #[test]
    fn test_no_unwrap_lossy_float_casts() {
        let schema = Schema::new(vec![
            Field::new("float32_col", DataType::Float32, true),
            Field::new("float64_col", DataType::Float64, true),
        ]);

        // Float64 -> Float32 loses precision.
        let column_expr = col("float64_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Float32, None));
        let literal_expr = lit(ScalarValue::Float32(Some(1.25)));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        // Float32 -> integer is lossy.
        let column_expr = col("float32_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int32, None));
        let literal_expr = lit(ScalarValue::Int32(Some(1)));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Lt, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        let column_expr = col("float32_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int32, None));
        let literal_expr = lit(ScalarValue::Int32(Some(1)));
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::GtEq, cast_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);
    }

    #[test]
    fn test_no_unwrap_source_domain_reducing_casts() {
        let schema = Schema::new(vec![
            Field::new("int32_col", DataType::Int32, true),
            Field::new("int64_col", DataType::Int64, true),
            Field::new("uint64_col", DataType::UInt64, true),
        ]);

        // Int64 -> Int32 is partial.
        let column_expr = col("int64_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int32, None));
        let literal_expr = lit(ScalarValue::Int32(Some(16)));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        // Int32 -> UInt32 is partial.
        let column_expr = col("int32_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::UInt32, None));
        let literal_expr = lit(ScalarValue::UInt32(Some(16)));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        // UInt64 -> Int64 is partial.
        let column_expr = col("uint64_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(ScalarValue::Int64(Some(16)));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Lt, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);

        // Decimal(10, 2) cannot hold the scaled Int64 domain.
        let column_expr = col("int64_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(
            column_expr,
            DataType::Decimal128(10, 2),
            None,
        ));
        let literal_expr = lit(ScalarValue::Decimal128(Some(12300), 10, 2));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Lt, literal_expr));
        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
        assert!(!result.transformed);
    }

    #[test]
    fn test_no_unwrap_cast_with_decimal_types() {
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
            assert!(!result.transformed);

            let cast_expr = Arc::new(CastExpr::new(
                column_expr,
                DataType::Decimal128(cast_p, cast_s),
                None,
            ));
            let literal_expr = lit(ScalarValue::Decimal128(Some(value), cast_p, cast_s));
            let binary_expr =
                Arc::new(BinaryExpr::new(literal_expr, Operator::Lt, cast_expr));

            let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();
            assert!(!result.transformed);
        }
    }

    #[test]
    fn test_no_unwrap_cast_with_null_literals() {
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int32, true)]);

        let column_expr = col("int_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let null_literal = lit(ScalarValue::Int64(None));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Eq, null_literal));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        assert!(result.transformed);
    }

    #[test]
    fn test_no_unwrap_cast_with_try_cast() {
        let schema = Schema::new(vec![Field::new("str_col", DataType::Utf8, true)]);

        let column_expr = col("str_col", &schema).unwrap();
        let try_cast_expr = Arc::new(TryCastExpr::new(column_expr, DataType::Int64));
        let literal_expr = lit(100i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(try_cast_expr, Operator::Gt, literal_expr));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        assert!(!result.transformed);
    }

    #[test]
    fn test_unwrap_cast_preserves_non_comparison_operators() {
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int32, false)]);

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

        let result = (and_expr as Arc<dyn PhysicalExpr>)
            .transform_down(|node| unwrap_cast_in_comparison(node, &schema))
            .unwrap();

        assert!(result.transformed);
    }

    #[test]
    fn test_unwrap_try_cast_in_comparison() {
        let schema = test_schema();

        let column_expr = col("c1", &schema).unwrap();
        let try_cast_expr = Arc::new(TryCastExpr::new(column_expr, DataType::Int64));
        let literal_expr = lit(100i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(try_cast_expr, Operator::LtEq, literal_expr));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        assert!(result.transformed);
    }

    #[test]
    fn test_non_swappable_operator() {
        let schema = Schema::new(vec![Field::new("int_col", DataType::Int32, false)]);

        let column_expr = col("int_col", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(10i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(literal_expr, Operator::Plus, cast_expr));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        assert!(!result.transformed);
    }

    #[test]
    fn test_cast_that_cannot_be_unwrapped_overflow() {
        let schema = Schema::new(vec![Field::new("small_int", DataType::Int8, false)]);

        let column_expr = col("small_int", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int64, None));
        let literal_expr = lit(1000i64);
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::Gt, literal_expr));

        let result = unwrap_cast_in_comparison(binary_expr, &schema).unwrap();

        assert!(!result.transformed);
    }

    #[test]
    fn test_unwrap_cast_with_complex_expressions() {
        let schema = test_schema();

        let c1_expr = col("c1", &schema).unwrap();
        let c1_cast = Arc::new(CastExpr::new(c1_expr, DataType::Int64, None));
        let c1_literal = lit(10i64);
        let c1_binary = Arc::new(BinaryExpr::new(c1_cast, Operator::Gt, c1_literal));

        let c2_expr = col("c2", &schema).unwrap();
        let c2_cast = Arc::new(CastExpr::new(c2_expr, DataType::Int32, None));
        let c2_literal = lit(20i32);
        let c2_binary = Arc::new(BinaryExpr::new(c2_cast, Operator::Eq, c2_literal));

        let and_expr = Arc::new(BinaryExpr::new(c1_binary, Operator::And, c2_binary));

        let result = (and_expr as Arc<dyn PhysicalExpr>)
            .transform_down(|node| unwrap_cast_in_comparison(node, &schema))
            .unwrap();

        assert!(result.transformed);
    }
}
