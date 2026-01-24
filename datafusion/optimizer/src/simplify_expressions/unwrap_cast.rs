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
//! If the expression matches one of the forms above, the rule will
//! ensure the value of `literal` is in range(min, max) of the
//! expr's data_type, and if the scalar is within range, the literal
//! will be casted to the data type of expr on the other side, and the
//! cast will be removed from the other side.
//!
//! # Example
//!
//! If the DataType of c1 is INT32. Given the filter
//!
//! ```text
//! cast(c1 as INT64) > INT64(10)`
//! ```
//!
//! This rule will remove the cast and rewrite the expression to:
//!
//! ```text
//! c1 > INT32(10)
//! ```

use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_common::{internal_err, tree_node::Transformed};
use datafusion_expr::{BinaryExpr, lit};
use datafusion_expr::{Cast, Expr, Operator, TryCast, simplify::SimplifyContext};
use datafusion_expr_common::casts::{is_supported_type, try_cast_literal_to_type};

pub(super) fn unwrap_cast_in_comparison_for_binary(
    info: &SimplifyContext,
    cast_expr: Expr,
    literal: Expr,
    op: Operator,
) -> Result<Transformed<Expr>> {
    match (cast_expr, literal) {
        (
            Expr::TryCast(TryCast { expr, .. }) | Expr::Cast(Cast { expr, .. }),
            Expr::Literal(lit_value, _),
        ) => {
            let Ok(expr_type) = info.get_data_type(&expr) else {
                return internal_err!("Can't get the data type of the expr {:?}", &expr);
            };

            if let Some(value) = cast_literal_to_type_with_op(&lit_value, &expr_type, op)
            {
                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                    left: expr,
                    op,
                    right: Box::new(lit(value)),
                })));
            };

            // if the lit_value can be casted to the type of internal_left_expr
            // we need to unwrap the cast for cast/try_cast expr, and add cast to the literal
            let Some(value) = try_cast_literal_to_type(&lit_value, &expr_type) else {
                return internal_err!(
                    "Can't cast the literal expr {:?} to type {}",
                    &lit_value,
                    &expr_type
                );
            };
            Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op,
                right: Box::new(lit(value)),
            })))
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
                expr: left_expr, ..
            })
            | Expr::Cast(Cast {
                expr: left_expr, ..
            }),
            Expr::Literal(lit_val, _),
        ) => {
            let Ok(expr_type) = info.get_data_type(left_expr) else {
                return false;
            };

            let Ok(lit_type) = info.get_data_type(literal) else {
                return false;
            };

            if cast_literal_to_type_with_op(lit_val, &expr_type, op).is_some() {
                return true;
            }

            try_cast_literal_to_type(lit_val, &expr_type).is_some()
                && is_supported_type(&expr_type)
                && is_supported_type(&lit_type)
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
        expr: left_expr, ..
    })
    | Expr::Cast(Cast {
        expr: left_expr, ..
    })) = expr
    else {
        return false;
    };

    let Ok(expr_type) = info.get_data_type(left_expr) else {
        return false;
    };

    if !is_supported_type(&expr_type) {
        return false;
    }

    for right in list {
        let Ok(right_type) = info.get_data_type(right) else {
            return false;
        };

        if !is_supported_type(&right_type) {
            return false;
        }

        match right {
            Expr::Literal(lit_val, _)
                if try_cast_literal_to_type(lit_val, &expr_type).is_some() => {}
            _ => return false,
        }
    }

    true
}

///// Tries to move a cast from an expression (such as column) to the literal other side of a comparison operator./
///
/// Specifically, rewrites
/// ```sql
/// cast(col) <op> <literal>
/// ```
///
/// To
///
/// ```sql
/// col <op> cast(<literal>)
/// col <op> <casted_literal>
/// ```
fn cast_literal_to_type_with_op(
    lit_value: &ScalarValue,
    target_type: &DataType,
    op: Operator,
) -> Option<ScalarValue> {
    match (op, lit_value) {
        (
            Operator::Eq | Operator::NotEq,
            ScalarValue::Utf8(Some(_))
            | ScalarValue::Utf8View(Some(_))
            | ScalarValue::LargeUtf8(Some(_)),
        ) => {
            // Only try for integer types (TODO can we do this for other types
            // like timestamps)?
            use DataType::*;
            if matches!(
                target_type,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
            ) {
                let casted = lit_value.cast_to(target_type).ok()?;
                let round_tripped = casted.cast_to(&lit_value.data_type()).ok()?;
                if lit_value != &round_tripped {
                    return None;
                }
                Some(casted)
            } else {
                None
            }
        }
        _ => None,
    }
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

        // cast(c1, UTF8) < '123', only eq/not_eq should be optimized
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
        // cast(c1, INT64) < INT64(16) -> INT32(c1) < cast(INT32(16))
        // the 16 is within the range of MAX(int32) and MIN(int32), we can cast the 16 to int32(16)
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(16i64));
        let expected = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
        let expr_lt = try_cast(col("c1"), DataType::Int64).lt(lit(16i64));
        let expected = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // cast(c2, INT32) = INT32(16) => INT64(c2) = INT64(16)
        let c2_eq_lit = cast(col("c2"), DataType::Int32).eq(lit(16i32));
        let expected = col("c2").eq(lit(16i64));
        assert_eq!(optimize_test(c2_eq_lit, &schema), expected);

        // cast(c1, INT64) < INT64(NULL) => NULL
        let c1_lt_lit_null = cast(col("c1"), DataType::Int64).lt(null_i64());
        let expected = null_bool();
        assert_eq!(optimize_test(c1_lt_lit_null, &schema), expected);

        // cast(INT8(NULL), INT32) < INT32(12) => INT8(NULL) < INT8(12) => BOOL(NULL)
        let lit_lt_lit = cast(null_i8(), DataType::Int32).lt(lit(12i32));
        let expected = null_bool();
        assert_eq!(optimize_test(lit_lt_lit, &schema), expected);

        // cast(c1, UTF8) = '123' => c1 = 123
        let expr_input = cast(col("c1"), DataType::Utf8).eq(lit("123"));
        let expected = col("c1").eq(lit(123i32));
        assert_eq!(optimize_test(expr_input, &schema), expected);

        // cast(c1, UTF8) != '123' => c1 != 123
        let expr_input = cast(col("c1"), DataType::Utf8).not_eq(lit("123"));
        let expected = col("c1").not_eq(lit(123i32));
        assert_eq!(optimize_test(expr_input, &schema), expected);

        // cast(c1, UTF8) = NULL => NULL
        let expr_input = cast(col("c1"), DataType::Utf8).eq(lit(ScalarValue::Utf8(None)));
        let expected = null_bool();
        assert_eq!(optimize_test(expr_input, &schema), expected);
    }

    #[test]
    fn test_unwrap_cast_comparison_unsigned() {
        // "cast(c6, UINT64) = 0u64 => c6 = 0u32
        let schema = expr_test_schema();
        let expr_input = cast(col("c6"), DataType::UInt64).eq(lit(0u64));
        let expected = col("c6").eq(lit(0u32));
        assert_eq!(optimize_test(expr_input, &schema), expected);

        // cast(c6, UTF8) = "123" => c6 = 123
        let expr_input = cast(col("c6"), DataType::Utf8).eq(lit("123"));
        let expected = col("c6").eq(lit(123u32));
        assert_eq!(optimize_test(expr_input, &schema), expected);

        // cast(c6, UTF8) != "123" => c6 != 123
        let expr_input = cast(col("c6"), DataType::Utf8).not_eq(lit("123"));
        let expected = col("c6").not_eq(lit(123u32));
        assert_eq!(optimize_test(expr_input, &schema), expected);
    }

    #[test]
    fn test_unwrap_cast_comparison_string() {
        let schema = expr_test_schema();
        let dict = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::from("value")),
        );

        // cast(str1 as Dictionary<Int32, Utf8>) = arrow_cast('value', 'Dictionary<Int32, Utf8>') => str1 = Utf8('value1')
        let expr_input = cast(col("str1"), dict.data_type()).eq(lit(dict.clone()));
        let expected = col("str1").eq(lit("value"));
        assert_eq!(optimize_test(expr_input, &schema), expected);

        // cast(tag as Utf8) = Utf8('value') => tag = arrow_cast('value', 'Dictionary<Int32, Utf8>')
        let expr_input = cast(col("tag"), DataType::Utf8).eq(lit("value"));
        let expected = col("tag").eq(lit(dict.clone()));
        assert_eq!(optimize_test(expr_input, &schema), expected);

        // Verify reversed argument order
        // arrow_cast('value', 'Dictionary<Int32, Utf8>') = cast(str1 as Dictionary<Int32, Utf8>) => Utf8('value1') = str1
        let expr_input = lit(dict.clone()).eq(cast(col("str1"), dict.data_type()));
        let expected = col("str1").eq(lit("value"));
        assert_eq!(optimize_test(expr_input, &schema), expected);
    }

    #[test]
    fn test_unwrap_cast_comparison_large_string() {
        let schema = expr_test_schema();
        // cast(largestr as Dictionary<Int32, LargeUtf8>) = arrow_cast('value', 'Dictionary<Int32, LargeUtf8>') => str1 = LargeUtf8('value1')
        let dict = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::LargeUtf8(Some("value".to_owned()))),
        );
        let expr_input = cast(col("largestr"), dict.data_type()).eq(lit(dict));
        let expected =
            col("largestr").eq(lit(ScalarValue::LargeUtf8(Some("value".to_owned()))));
        assert_eq!(optimize_test(expr_input, &schema), expected);
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
    }

    #[test]
    fn test_unwrap_cast_with_decimal_lit_comparison() {
        let schema = expr_test_schema();
        // integer to decimal
        // c3 < INT64(16) -> c3 < (CAST(INT64(16) AS DECIMAL(18,2));
        let expr_lt = try_cast(col("c3"), DataType::Int64).lt(lit(16i64));
        let expected = col("c3").lt(lit_decimal(1600, 18, 2));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // c3 < INT64(NULL)
        let c1_lt_lit_null = cast(col("c3"), DataType::Int64).lt(null_i64());
        let expected = null_bool();
        assert_eq!(optimize_test(c1_lt_lit_null, &schema), expected);

        // decimal to decimal
        // c3 < Decimal(123,10,0) -> c3 < CAST(DECIMAL(123,10,0) AS DECIMAL(18,2)) -> c3 < DECIMAL(12300,18,2)
        let expr_lt =
            cast(col("c3"), DataType::Decimal128(10, 0)).lt(lit_decimal(123, 10, 0));
        let expected = col("c3").lt(lit_decimal(12300, 18, 2));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // c3 < Decimal(1230,10,3) -> c3 < CAST(DECIMAL(1230,10,3) AS DECIMAL(18,2)) -> c3 < DECIMAL(123,18,2)
        let expr_lt =
            cast(col("c3"), DataType::Decimal128(10, 3)).lt(lit_decimal(1230, 10, 3));
        let expected = col("c3").lt(lit_decimal(123, 18, 2));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // decimal to integer
        // c1 < Decimal(12300, 10, 2) -> c1 < CAST(DECIMAL(12300,10,2) AS INT32) -> c1 < INT32(123)
        let expr_lt =
            cast(col("c1"), DataType::Decimal128(10, 2)).lt(lit_decimal(12300, 10, 2));
        let expected = col("c1").lt(lit(123i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
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
    fn test_unwrap_list_cast_comparison() {
        let schema = expr_test_schema();
        // INT32(C1) IN (INT32(12),INT64(23),INT64(34),INT64(56),INT64(78)) ->
        // INT32(C1) IN (INT32(12),INT32(23),INT32(34),INT32(56),INT32(78))
        let expr_lt = cast(col("c1"), DataType::Int64).in_list(
            vec![lit(12i64), lit(23i64), lit(34i64), lit(56i64), lit(78i64)],
            false,
        );
        let expected = col("c1").in_list(
            vec![lit(12i32), lit(23i32), lit(34i32), lit(56i32), lit(78i32)],
            false,
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected);
        // INT32(C2) IN (INT64(NULL),INT64(24),INT64(34),INT64(56),INT64(78)) ->
        // INT32(C2) IN (INT32(NULL),INT32(24),INT32(34),INT32(56),INT32(78))
        let expr_lt = cast(col("c2"), DataType::Int32).in_list(
            vec![null_i32(), lit(24i32), lit(34i64), lit(56i64), lit(78i64)],
            false,
        );
        let expected = col("c2").in_list(
            vec![null_i64(), lit(24i64), lit(34i64), lit(56i64), lit(78i64)],
            false,
        );

        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // decimal test case
        // c3 is decimal(18,2)
        let expr_lt = cast(col("c3"), DataType::Decimal128(19, 3)).in_list(
            vec![
                lit_decimal(12000, 19, 3),
                lit_decimal(24000, 19, 3),
                lit_decimal(1280, 19, 3),
                lit_decimal(1240, 19, 3),
            ],
            false,
        );
        let expected = col("c3").in_list(
            vec![
                lit_decimal(1200, 18, 2),
                lit_decimal(2400, 18, 2),
                lit_decimal(128, 18, 2),
                lit_decimal(124, 18, 2),
            ],
            false,
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected);

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
        // c1 < INT64(16) -> c1 < cast(INT32(16))
        // the 16 is within the range of MAX(int32) and MIN(int32), we can cast the 16 to int32(16)
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(16i64)).alias("x");
        let expected = col("c1").lt(lit(16i32)).alias("x");
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    #[test]
    fn nested() {
        let schema = expr_test_schema();
        // c1 < INT64(16) OR c1 > INT64(32) -> c1 < INT32(16) OR c1 > INT32(32)
        // the 16 and 32 are within the range of MAX(int32) and MIN(int32), we can cast them to int32
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
    /// Basic integration test for unwrapping casts with different timezones
    fn test_unwrap_cast_with_timestamp_nanos() {
        let schema = expr_test_schema();
        // cast(ts_nano as Timestamp(Nanosecond, UTC)) < 1666612093000000000::Timestamp(Nanosecond, Utc))
        let expr_lt = try_cast(col("ts_nano_none"), timestamp_nano_utc_type())
            .lt(lit_timestamp_nano_utc(1666612093000000000));
        let expected =
            col("ts_nano_none").lt(lit_timestamp_nano_none(1666612093000000000));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let simplifier = ExprSimplifier::new(
            SimplifyContext::default().with_schema(Arc::clone(schema)),
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
                    Field::new("ts_nano_none", timestamp_nano_none_type(), false),
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

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
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
