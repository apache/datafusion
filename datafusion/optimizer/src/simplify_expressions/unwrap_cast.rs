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

//! Unwrap casts in binary comparisons
//!
//! The functions in this module attempt to remove casts from
//! comparisons to literals ([`ScalarValue`]s) by applying the casts
//! to the literals if possible. It is inspired by the optimizer rule
//! `UnwrapCastInBinaryComparison` of Spark.
//!
//! Removing casts often improves performance because:
//! 1. The cast is done once (to the literal) rather than to every value
//! 2. Can enable other optimizations such as predicate pushdown that
//!    don't support casting
//!
//! The rule is applied to expressions of the following forms:
//!
//! 1. `cast(left_expr as data_type) comparison_op literal_expr`
//! 2. `literal_expr comparison_op cast(left_expr as data_type)`
//! 3. `cast(literal_expr) IN (expr1, expr2, ...)`
//! 4. `literal_expr IN (cast(expr1) , cast(expr2), ...)`
//!
//! ## Safety
//!
//! Unwrap is **closed by default**: it is only allowed when the cast is
//! known to preserve comparison semantics. Currently the only allowed
//! case is same-timezone timestamp casts where the target precision is
//! the same or finer than the source (e.g. `Timestamp(ms) -> Timestamp(ns)`),
//! and the literal round-trips exactly through both types.
//!
//! All other type combinations (integer, float, decimal, string, dictionary,
//! binary, etc.) are NOT unwrapped because the cast can lose precision, change
//! domain, or break comparison ordering/equality.
//!
//! # Example (allowed)
//!
//! If `ts` has DataType `Timestamp(Millisecond, None)`. Given the filter:
//!
//! ```text
//! CAST(ts AS Timestamp(Nanosecond, None)) = TimestampNanosecond(1000000, None)
//! ```
//!
//! Since the literal round-trips exactly (1000000ns / 1e6 = 1ms, 1ms * 1e6 = 1000000ns),
//! this rule will remove the cast and rewrite the expression to:
//!
//! ```text
//! ts = TimestampMillisecond(1, None)
//! ```

use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_common::{internal_err, tree_node::Transformed};
use datafusion_expr::{BinaryExpr, lit};
use datafusion_expr::{Cast, Expr, Operator, TryCast, simplify::SimplifyContext};
use datafusion_expr_common::casts::{
    CastComparisonRewrite, try_cast_literal_for_comparison_unwrap,
    try_cast_literal_to_type,
};

pub(super) fn unwrap_cast_in_comparison_for_binary(
    info: &SimplifyContext,
    cast_expr: &Expr,
    literal: &Expr,
    op: Operator,
) -> Result<Transformed<Expr>> {
    match (cast_expr, literal) {
        (
            Expr::TryCast(TryCast { expr, field }) | Expr::Cast(Cast { expr, field }),
            Expr::Literal(lit_value, _),
        ) => {
            let Ok(expr_type) = info.get_data_type(expr) else {
                return internal_err!("Can't get the data type of the expr {:?}", &expr);
            };

            if let Some(value) =
                comparison_unwrap_literal(lit_value, &expr_type, field.data_type(), op)
            {
                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                    left: expr.clone(),
                    op,
                    right: Box::new(lit(value)),
                })));
            };

            let original = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(cast_expr.clone()),
                op,
                right: Box::new(literal.clone()),
            });
            Ok(Transformed::no(original))
        }
        _ => internal_err!("Expect cast expr and literal"),
    }
}

pub(super) fn is_cast_expr_and_support_unwrap_cast_in_comparison_for_binary(
    info: &SimplifyContext,
    expr: &Expr,
    op: Operator,
    literal: &Expr,
) -> bool {
    match (expr, literal) {
        (
            Expr::TryCast(TryCast {
                expr: left_expr,
                field,
            })
            | Expr::Cast(Cast {
                expr: left_expr,
                field,
            }),
            Expr::Literal(lit_val, _),
        ) => {
            let Ok(expr_type) = info.get_data_type(left_expr) else {
                return false;
            };

            if info.get_data_type(literal).is_err() {
                return false;
            };

            comparison_unwrap_literal(lit_val, &expr_type, field.data_type(), op)
                .is_some()
        }
        _ => false,
    }
}

pub(super) fn is_cast_expr_and_support_unwrap_cast_in_comparison_for_inlist(
    info: &SimplifyContext,
    expr: &Expr,
    list: &[Expr],
) -> bool {
    let (Expr::TryCast(TryCast {
        expr: left_expr,
        field,
    })
    | Expr::Cast(Cast {
        expr: left_expr,
        field,
    })) = expr
    else {
        return false;
    };

    let Ok(expr_type) = info.get_data_type(left_expr) else {
        return false;
    };

    for right in list {
        let Ok(_right_type) = info.get_data_type(right) else {
            return false;
        };

        match right {
            Expr::Literal(lit_val, _)
                if comparison_unwrap_literal(
                    lit_val,
                    &expr_type,
                    field.data_type(),
                    Operator::Eq,
                )
                .is_some()
                    && try_cast_literal_to_type(lit_val, &expr_type).is_some() => {}
            _ => return false,
        }
    }

    true
}

/// Tries to move a cast from an expression to the literal side of a comparison.
fn comparison_unwrap_literal(
    lit_value: &ScalarValue,
    from_type: &DataType,
    to_type: &DataType,
    op: Operator,
) -> Option<ScalarValue> {
    try_cast_literal_for_comparison_unwrap(lit_value, from_type, to_type, op)
        .map(CastComparisonRewrite::into_literal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::simplify_expressions::ExprSimplifier;
    use arrow::datatypes::{Field, TimeUnit};
    use datafusion_common::{DFSchema, DFSchemaRef};
    use datafusion_expr::simplify::SimplifyContext;
    use datafusion_expr::{cast, col, in_list, try_cast};

    #[test]
    fn test_not_unwrap_cast_comparison() {
        let schema = expr_test_schema();
        // cast(INT32(c1), INT64) > INT64(c2)
        let c1_gt_c2 = cast(col("c1"), DataType::Int64).gt(col("c2"));
        assert_eq!(optimize_test(c1_gt_c2.clone(), &schema), c1_gt_c2);

        // INT32(c1) < INT32(16), the type is same
        let expr_lt = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // the 99999999999 is not within the range of MAX(int32) and MIN(int32), we don't cast the lit(99999999999) to int32 type
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(99999999999i64));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // cast(c1, UTF8) < '123', inequality with string cast is not unwrapped
        let expr_lt = cast(col("c1"), DataType::Utf8).lt(lit("123"));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // cast(c1, UTF8) = '0123', cast(cast('0123', Int32), UTF8) != '0123', so '0123' should not
        // be casted
        let expr_lt = cast(col("c1"), DataType::Utf8).lt(lit("0123"));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // cast(c1, UTF8) = 'not a number', should not be able to cast to column type
        let expr_input = cast(col("c1"), DataType::Utf8).eq(lit("not a number"));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);

        // cast(c1, UTF8) = '99999999999', where '99999999999' does not fit into int32, so it will
        // not be optimized to integer comparison
        let expr_input = cast(col("c1"), DataType::Utf8).eq(lit("99999999999"));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);
    }

    #[test]
    fn test_unwrap_cast_comparison() {
        let schema = expr_test_schema();
        // Int32→Int64 widening cast is now unwrapped: literal 16i64 is cast back to 16i32.
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(16i64));
        let expected = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
        // try_cast Int32→Int64 widening is also unwrapped.
        let expr_lt = try_cast(col("c1"), DataType::Int64).lt(lit(16i64));
        let expected = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // Int64 source domain is wider than Int32.
        let c2_eq_lit = cast(col("c2"), DataType::Int32).eq(lit(16i32));
        assert_eq!(optimize_test(c2_eq_lit.clone(), &schema), c2_eq_lit);

        // Int32 contains negative values outside the UInt32 target domain.
        let c1_eq_lit = cast(col("c1"), DataType::UInt32).eq(lit(16u32));
        assert_eq!(optimize_test(c1_eq_lit.clone(), &schema), c1_eq_lit);

        // UInt64 has values above Int64::MAX.
        let c8_lt_lit = cast(col("c8"), DataType::Int64).lt(lit(16i64));
        assert_eq!(optimize_test(c8_lt_lit.clone(), &schema), c8_lt_lit);

        // Comparison against NULL is still folded by general simplification.
        let c1_lt_lit_null = cast(col("c1"), DataType::Int64).lt(null_i64());
        let expected = null_bool();
        assert_eq!(optimize_test(c1_lt_lit_null, &schema), expected);

        // cast(INT8(NULL), INT32) < INT32(12) => INT8(NULL) < INT8(12) => BOOL(NULL)
        let lit_lt_lit = cast(null_i8(), DataType::Int32).lt(lit(12i32));
        let expected = null_bool();
        assert_eq!(optimize_test(lit_lt_lit, &schema), expected);

        // cast(c1, UTF8) = '123' is not currently unwrapped.
        let expr_input = cast(col("c1"), DataType::Utf8).eq(lit("123"));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);

        // cast(c1, UTF8) != '123' is not currently unwrapped.
        let expr_input = cast(col("c1"), DataType::Utf8).not_eq(lit("123"));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);

        // Comparison against NULL is still folded by general simplification.
        let expr_input = cast(col("c1"), DataType::Utf8).eq(lit(ScalarValue::Utf8(None)));
        let expected = null_bool();
        assert_eq!(optimize_test(expr_input, &schema), expected);
    }

    #[test]
    fn test_unwrap_cast_comparison_unsigned() {
        // UInt32→UInt64 widening is now unwrapped.
        let schema = expr_test_schema();
        let expr_input = cast(col("c6"), DataType::UInt64).eq(lit(0u64));
        let expected = col("c6").eq(lit(0u32));
        assert_eq!(optimize_test(expr_input, &schema), expected);

        // cast(c6, UTF8) = "123" is not currently unwrapped.
        let expr_input = cast(col("c6"), DataType::Utf8).eq(lit("123"));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);

        // cast(c6, UTF8) != "123" is not currently unwrapped.
        let expr_input = cast(col("c6"), DataType::Utf8).not_eq(lit("123"));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);
    }

    #[test]
    fn test_unwrap_cast_comparison_string() {
        let schema = expr_test_schema();
        let dict = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::from("value")),
        );

        // Dictionary casts are not currently unwrapped.
        let expr_input = cast(col("str1"), dict.data_type()).eq(lit(dict.clone()));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);

        // Dictionary casts are not currently unwrapped.
        let expr_input = cast(col("tag"), DataType::Utf8).eq(lit("value"));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);

        // Verify reversed argument order.
        let expr_input = lit(dict.clone()).eq(cast(col("str1"), dict.data_type()));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);
    }

    #[test]
    fn test_unwrap_cast_comparison_large_string() {
        let schema = expr_test_schema();
        // Dictionary casts are not currently unwrapped.
        let dict = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::LargeUtf8(Some("value".to_owned()))),
        );
        let expr_input = cast(col("largestr"), dict.data_type()).eq(lit(dict));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);
    }

    #[test]
    fn test_not_unwrap_cast_with_decimal_comparison() {
        let schema = expr_test_schema();
        // integer to decimal: value is out of the bounds of the decimal
        // cast(c3, INT64) = INT64(100000000000000000)
        let expr_eq = cast(col("c3"), DataType::Int64).eq(lit(100000000000000000i64));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // cast(c4, INT64) = INT64(1000) will overflow the i128
        let expr_eq = cast(col("c4"), DataType::Int64).eq(lit(1000i64));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // decimal to decimal: value will lose the scale when convert to the target data type
        // c3 = DECIMAL(12340,20,4)
        let expr_eq =
            cast(col("c3"), DataType::Decimal128(20, 4)).eq(lit_decimal(12340, 20, 4));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // decimal to integer
        // c1 = DECIMAL(123, 10, 1): value will lose the scale when convert to the target data type
        let expr_eq =
            cast(col("c1"), DataType::Decimal128(10, 1)).eq(lit_decimal(123, 10, 1));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // c1 = DECIMAL(1230, 10, 2): value will lose the scale when convert to the target data type
        let expr_eq =
            cast(col("c1"), DataType::Decimal128(10, 2)).eq(lit_decimal(1230, 10, 2));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // Decimal(10, 2) cannot represent the scaled Int64 domain.
        let expr_lt =
            cast(col("c2"), DataType::Decimal128(10, 2)).lt(lit_decimal(12300, 10, 2));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);
    }

    #[test]
    fn test_unwrap_cast_with_decimal_lit_comparison() {
        let schema = expr_test_schema();
        // Fractional decimal -> integer is lossy.
        let expr_lt = try_cast(col("c3"), DataType::Int64).lt(lit(16i64));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // c3 < INT64(NULL)
        let c1_lt_lit_null = cast(col("c3"), DataType::Int64).lt(null_i64());
        let expected = null_bool();
        assert_eq!(optimize_test(c1_lt_lit_null, &schema), expected);

        // Decimal scale downcast is lossy.
        let expr_lt =
            cast(col("c3"), DataType::Decimal128(10, 0)).lt(lit_decimal(123, 10, 0));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // Increasing scale can still reduce the integer range.
        let expr_lt =
            cast(col("c3"), DataType::Decimal128(10, 3)).lt(lit_decimal(1230, 10, 3));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // Decimal(10, 2) cannot hold the scaled Int32 domain.
        let expr_lt =
            cast(col("c1"), DataType::Decimal128(10, 2)).lt(lit_decimal(12300, 10, 2));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // Decimal casts are not currently unwrapped.
        let expr_lt =
            cast(col("c1"), DataType::Decimal128(12, 2)).lt(lit_decimal(12300, 12, 2));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);
    }

    #[test]
    fn test_not_unwrap_list_cast_lit_comparison() {
        let schema = expr_test_schema();
        // internal left type is not supported
        // FLOAT32(C5) in ...
        let expr_lt =
            cast(col("c5"), DataType::Int64).in_list(vec![lit(12i64), lit(12i64)], false);
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // cast(INT32(C1), Float32) in (FLOAT32(1.23), Float32(12), Float32(12))
        let expr_lt = cast(col("c1"), DataType::Float32)
            .in_list(vec![lit(12.0f32), lit(12.0f32), lit(1.23f32)], false);
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // INT32(C1) in (INT64(99999999999), INT64(12))
        let expr_lt = cast(col("c1"), DataType::Int64)
            .in_list(vec![lit(12i32), lit(99999999999i64)], false);
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // DECIMAL(C3) in (INT64(12), INT32(12), DECIMAL(128,12,3))
        let expr_lt = cast(col("c3"), DataType::Decimal128(12, 3)).in_list(
            vec![
                lit_decimal(12, 12, 3),
                lit_decimal(12, 12, 3),
                lit_decimal(128, 12, 3),
            ],
            false,
        );
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);
    }

    #[test]
    fn test_not_unwrap_list_cast_comparison() {
        let schema = expr_test_schema();
        // Int32→Int64 widening IN-list is now unwrapped.
        let expr_lt = cast(col("c1"), DataType::Int64).in_list(
            vec![lit(12i64), lit(23i64), lit(34i64), lit(56i64), lit(78i64)],
            false,
        );
        let expected = col("c1").in_list(
            vec![lit(12i32), lit(23i32), lit(34i32), lit(56i32), lit(78i32)],
            false,
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected);
        // Int32 cannot represent the full Int64 source domain.
        let expr_lt = cast(col("c2"), DataType::Int32).in_list(
            vec![null_i32(), lit(24i32), lit(34i64), lit(56i64), lit(78i64)],
            false,
        );
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // Decimal casts are not currently unwrapped.
        let expr_lt = cast(col("c3"), DataType::Decimal128(19, 3)).in_list(
            vec![
                lit_decimal(12000, 19, 3),
                lit_decimal(24000, 19, 3),
                lit_decimal(1280, 19, 3),
                lit_decimal(1240, 19, 3),
            ],
            false,
        );
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // cast(INT32(12), INT64) IN (.....) =>
        // INT64(12) IN (INT64(12),INT64(13),INT64(14),INT64(15),INT64(16))
        // => true
        let expr_lt = cast(lit(12i32), DataType::Int64).in_list(
            vec![lit(12i64), lit(13i64), lit(14i64), lit(15i64), lit(16i64)],
            false,
        );
        let expected = lit(true);
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    #[test]
    fn aliased() {
        let schema = expr_test_schema();
        // Int32→Int64 widening with alias is now unwrapped.
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(16i64)).alias("x");
        let expected = col("c1").lt(lit(16i32)).alias("x");
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    #[test]
    fn nested() {
        let schema = expr_test_schema();
        // Int32→Int64 widening in nested OR is now unwrapped.
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(16i64)).or(cast(
            col("c1"),
            DataType::Int64,
        )
        .gt(lit(32i64)));
        let expected = col("c1").lt(lit(16i32)).or(col("c1").gt(lit(32i32)));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    #[test]
    fn test_not_support_data_type() {
        // "c6 > 0" will be cast to `cast(c6 as float) > 0
        // but the type of c6 is uint32
        // the rewriter will not throw error and just return the original expr
        let schema = expr_test_schema();
        let expr_input = cast(col("c6"), DataType::Float64).eq(lit(0f64));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);

        // inlist for unsupported data type
        let expr_input = in_list(
            cast(col("c6"), DataType::Float64),
            // need more literals to avoid rewriting to binary expr
            vec![lit(0f64), lit(1f64), lit(2f64), lit(3f64), lit(4f64)],
            false,
        );
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);
    }

    #[test]
    /// Casts that change timestamp timezone are not unwrapped.
    fn test_not_unwrap_cast_with_timestamp_nanos_timezone() {
        let schema = expr_test_schema();
        // cast(ts_nano as Timestamp(Nanosecond, UTC)) < 1666612093000000000::Timestamp(Nanosecond, Utc))
        let expr_lt = try_cast(col("ts_nano_none"), timestamp_nano_utc_type())
            .lt(lit_timestamp_nano_utc(1666612093000000000));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);
    }

    #[test]
    fn test_not_unwrap_cast_with_timestamp_precision_downcast() {
        let schema = expr_test_schema();

        // Nanoseconds -> milliseconds loses precision.
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            let expr = binary_expr(
                cast(col("ts_nano_none"), timestamp_millis_none_type()),
                op,
                lit_timestamp_millis_none(1000),
            );
            assert_eq!(optimize_test(expr.clone(), &schema), expr);

            let literal_left = binary_expr(
                lit_timestamp_millis_none(1000),
                op,
                cast(col("ts_nano_none"), timestamp_millis_none_type()),
            );
            assert_eq!(optimize_test(literal_left.clone(), &schema), literal_left);
        }

        let try_cast_expr = try_cast(col("ts_nano_none"), timestamp_millis_none_type())
            .gt(lit_timestamp_millis_none(1000));
        assert_eq!(optimize_test(try_cast_expr.clone(), &schema), try_cast_expr);

        let timezone_expr = cast(col("ts_nano_utf"), timestamp_millis_utc_type())
            .eq(lit_timestamp_millis_utc(1000));
        assert_eq!(optimize_test(timezone_expr.clone(), &schema), timezone_expr);

        let in_list_expr = in_list(
            cast(col("ts_nano_none"), timestamp_millis_none_type()),
            vec![
                lit_timestamp_millis_none(1000),
                lit_timestamp_millis_none(1001),
            ],
            false,
        );
        assert_eq!(optimize_test(in_list_expr.clone(), &schema), in_list_expr);
    }

    #[test]
    fn test_unwrap_cast_with_non_lossy_timestamp_precision_casts() {
        let schema = expr_test_schema();

        let same_precision = cast(col("ts_nano_none"), timestamp_nano_none_type())
            .eq(lit_timestamp_nano_none(1000));
        let expected = col("ts_nano_none").eq(lit_timestamp_nano_none(1000));
        assert_eq!(optimize_test(same_precision, &schema), expected);

        let same_precision_literal_left = binary_expr(
            lit_timestamp_nano_none(1000),
            Operator::Gt,
            cast(col("ts_nano_none"), timestamp_nano_none_type()),
        );
        let expected = col("ts_nano_none").lt(lit_timestamp_nano_none(1000));
        assert_eq!(
            optimize_test(same_precision_literal_left, &schema),
            expected
        );

        let upcast = cast(col("ts_millis_none"), timestamp_nano_none_type())
            .lt(lit_timestamp_nano_none(1_000_000_000));
        let expected = col("ts_millis_none").lt(lit_timestamp_millis_none(1000));
        assert_eq!(optimize_test(upcast, &schema), expected);

        let non_boundary_upcast = cast(col("ts_millis_none"), timestamp_nano_none_type())
            .eq(lit_timestamp_nano_none(1_000_000_001));
        assert_eq!(
            optimize_test(non_boundary_upcast.clone(), &schema),
            non_boundary_upcast
        );

        let upcast_literal_left = binary_expr(
            lit_timestamp_nano_none(1_000_000_000),
            Operator::Gt,
            cast(col("ts_millis_none"), timestamp_nano_none_type()),
        );
        let expected = col("ts_millis_none").lt(lit_timestamp_millis_none(1000));
        assert_eq!(optimize_test(upcast_literal_left, &schema), expected);
    }

    #[test]
    fn test_not_unwrap_cast_with_lossy_decimal_precision_casts() {
        let schema = expr_test_schema();

        // Decimal scale downcast loses fractional precision.
        let expr =
            cast(col("c3"), DataType::Decimal128(18, 1)).eq(lit_decimal(123, 18, 1));
        assert_eq!(optimize_test(expr.clone(), &schema), expr);

        // Precision downcast narrows the integer range.
        let precision_downcast =
            cast(col("c3"), DataType::Decimal128(10, 2)).eq(lit_decimal(12345, 10, 2));
        assert_eq!(
            optimize_test(precision_downcast.clone(), &schema),
            precision_downcast
        );

        // Increasing scale can still narrow range if it reduces integer digits.
        let scale_up_range_downcast =
            cast(col("c3"), DataType::Decimal128(12, 4)).eq(lit_decimal(1234500, 12, 4));
        assert_eq!(
            optimize_test(scale_up_range_downcast.clone(), &schema),
            scale_up_range_downcast
        );

        let literal_left = binary_expr(
            lit_decimal(123, 18, 1),
            Operator::LtEq,
            cast(col("c3"), DataType::Decimal128(18, 1)),
        );
        assert_eq!(optimize_test(literal_left.clone(), &schema), literal_left);

        let in_list_expr = in_list(
            cast(col("c3"), DataType::Decimal128(18, 1)),
            vec![lit_decimal(123, 18, 1), lit_decimal(124, 18, 1)],
            false,
        );
        assert_eq!(optimize_test(in_list_expr.clone(), &schema), in_list_expr);

        let decimal_to_integer = cast(col("c3"), DataType::Int64).eq(lit(16i64));
        assert_eq!(
            optimize_test(decimal_to_integer.clone(), &schema),
            decimal_to_integer
        );
    }

    #[test]
    fn test_not_unwrap_cast_with_lossy_date_timestamp_casts() {
        let schema = expr_test_schema();

        let timestamp_to_date = cast(col("ts_nano_none"), DataType::Date32)
            .eq(lit(ScalarValue::Date32(Some(0))));
        assert_eq!(
            optimize_test(timestamp_to_date.clone(), &schema),
            timestamp_to_date
        );

        let date_to_timestamp = cast(col("date32"), timestamp_millis_none_type())
            .eq(lit_timestamp_millis_none(0));
        assert_eq!(
            optimize_test(date_to_timestamp.clone(), &schema),
            date_to_timestamp
        );

        let timestamp_to_date_inequality = cast(col("ts_nano_none"), DataType::Date32)
            .lt_eq(lit(ScalarValue::Date32(Some(0))));
        assert_eq!(
            optimize_test(timestamp_to_date_inequality.clone(), &schema),
            timestamp_to_date_inequality
        );

        let timestamp_to_date_in_list = in_list(
            cast(col("ts_nano_none"), DataType::Date32),
            vec![
                lit(ScalarValue::Date32(Some(0))),
                lit(ScalarValue::Date32(Some(1))),
            ],
            false,
        );
        assert_eq!(
            optimize_test(timestamp_to_date_in_list.clone(), &schema),
            timestamp_to_date_in_list
        );
    }

    #[test]
    fn test_not_unwrap_cast_with_lossy_float_casts() {
        let schema = expr_test_schema();

        // Float64 -> Float32 loses precision.
        let float_precision_downcast =
            cast(col("c7"), DataType::Float32).eq(lit(ScalarValue::Float32(Some(1.25))));
        assert_eq!(
            optimize_test(float_precision_downcast.clone(), &schema),
            float_precision_downcast
        );

        // Float32 -> integer is lossy.
        let float_to_integer = cast(col("c5"), DataType::Int32).lt(lit(1i32));
        assert_eq!(
            optimize_test(float_to_integer.clone(), &schema),
            float_to_integer
        );

        let literal_left =
            binary_expr(lit(1i32), Operator::GtEq, cast(col("c5"), DataType::Int32));
        assert_eq!(optimize_test(literal_left.clone(), &schema), literal_left);
    }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(Arc::clone(schema))
                .build(),
        );

        simplifier.simplify(expr).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualified_fields(
                vec![
                    Field::new("c1", DataType::Int32, false),
                    Field::new("c2", DataType::Int64, false),
                    Field::new("c3", DataType::Decimal128(18, 2), false),
                    Field::new("c4", DataType::Decimal128(38, 37), false),
                    Field::new("c5", DataType::Float32, false),
                    Field::new("c6", DataType::UInt32, false),
                    Field::new("c7", DataType::Float64, false),
                    Field::new("c8", DataType::UInt64, false),
                    Field::new("date32", DataType::Date32, false),
                    Field::new("ts_nano_none", timestamp_nano_none_type(), false),
                    Field::new("ts_millis_none", timestamp_millis_none_type(), false),
                    Field::new("ts_nano_utf", timestamp_nano_utc_type(), false),
                    Field::new("str1", DataType::Utf8, false),
                    Field::new("largestr", DataType::LargeUtf8, false),
                    Field::new("tag", dictionary_tag_type(), false),
                ]
                .into(),
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    fn null_bool() -> Expr {
        lit(ScalarValue::Boolean(None))
    }

    fn null_i8() -> Expr {
        lit(ScalarValue::Int8(None))
    }

    fn null_i32() -> Expr {
        lit(ScalarValue::Int32(None))
    }

    fn null_i64() -> Expr {
        lit(ScalarValue::Int64(None))
    }

    fn lit_decimal(value: i128, precision: u8, scale: i8) -> Expr {
        lit(ScalarValue::Decimal128(Some(value), precision, scale))
    }

    fn lit_timestamp_nano_none(ts: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(ts), None))
    }

    fn lit_timestamp_nano_utc(ts: i64) -> Expr {
        let utc = Some("+0:00".into());
        lit(ScalarValue::TimestampNanosecond(Some(ts), utc))
    }

    fn lit_timestamp_millis_none(ts: i64) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(ts), None))
    }

    fn lit_timestamp_millis_utc(ts: i64) -> Expr {
        let utc = Some("+0:00".into());
        lit(ScalarValue::TimestampMillisecond(Some(ts), utc))
    }

    fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    fn timestamp_millis_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    }

    fn timestamp_millis_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Millisecond, utc)
    }

    // this is the type that now() returns
    fn timestamp_nano_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Nanosecond, utc)
    }

    // a dictionary type for storing string tags
    fn dictionary_tag_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }
}
