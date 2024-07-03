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

//! [`UnwrapCastInComparison`] rewrites `CAST(col) = lit` to `col = CAST(lit)`

use std::cmp::Ordering;
use std::mem;
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use crate::utils::NamePreserver;
use arrow::datatypes::{
    DataType, TimeUnit, MAX_DECIMAL_FOR_EACH_PRECISION, MIN_DECIMAL_FOR_EACH_PRECISION,
};
use arrow::temporal_conversions::{MICROSECONDS, MILLISECONDS, NANOSECONDS};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{internal_err, DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::{BinaryExpr, Cast, InList, TryCast};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{lit, Expr, ExprSchemable, LogicalPlan, Operator};

/// [`UnwrapCastInComparison`] attempts to remove casts from
/// comparisons to literals ([`ScalarValue`]s) by applying the casts
/// to the literals if possible. It is inspired by the optimizer rule
/// `UnwrapCastInBinaryComparison` of Spark.
///
/// Removing casts often improves performance because:
/// 1. The cast is done once (to the literal) rather than to every value
/// 2. Can enable other optimizations such as predicate pushdown that
///    don't support casting
///
/// The rule is applied to expressions of the following forms:
///
/// 1. `cast(left_expr as data_type) comparison_op literal_expr`
/// 2. `literal_expr comparison_op cast(left_expr as data_type)`
/// 3. `cast(literal_expr) IN (expr1, expr2, ...)`
/// 4. `literal_expr IN (cast(expr1) , cast(expr2), ...)`
///
/// If the expression matches one of the forms above, the rule will
/// ensure the value of `literal` is in range(min, max) of the
/// expr's data_type, and if the scalar is within range, the literal
/// will be casted to the data type of expr on the other side, and the
/// cast will be removed from the other side.
///
/// # Example
///
/// If the DataType of c1 is INT32. Given the filter
///
/// ```text
/// Filter: cast(c1 as INT64) > INT64(10)`
/// ```
///
/// This rule will remove the cast and rewrite the expression to:
///
/// ```text
/// Filter: c1 > INT32(10)
/// ```
///
#[derive(Default)]
pub struct UnwrapCastInComparison {}

impl UnwrapCastInComparison {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for UnwrapCastInComparison {
    fn name(&self) -> &str {
        "unwrap_cast_in_comparison"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut schema = merge_schema(plan.inputs());

        if let LogicalPlan::TableScan(ts) = &plan {
            let source_schema = DFSchema::try_from_qualified_schema(
                ts.table_name.clone(),
                &ts.source.schema(),
            )?;
            schema.merge(&source_schema);
        }

        schema.merge(plan.schema());

        let mut expr_rewriter = UnwrapCastExprRewriter {
            schema: Arc::new(schema),
        };

        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let original_name = name_preserver.save(&expr)?;
            expr.rewrite(&mut expr_rewriter)?
                .map_data(|expr| original_name.restore(expr))
        })
    }
}

struct UnwrapCastExprRewriter {
    schema: DFSchemaRef,
}

impl TreeNodeRewriter for UnwrapCastExprRewriter {
    type Node = Expr;

    fn f_up(&mut self, mut expr: Expr) -> Result<Transformed<Expr>> {
        match &mut expr {
            // For case:
            // try_cast/cast(expr as data_type) op literal
            // literal op try_cast/cast(expr as data_type)
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if {
                    let Ok(left_type) = left.get_type(&self.schema) else {
                        return Ok(Transformed::no(expr));
                    };
                    let Ok(right_type) = right.get_type(&self.schema) else {
                        return Ok(Transformed::no(expr));
                    };
                    is_supported_type(&left_type)
                        && is_supported_type(&right_type)
                        && is_comparison_op(op)
                } =>
            {
                match (left.as_mut(), right.as_mut()) {
                    (
                        Expr::Literal(left_lit_value),
                        Expr::TryCast(TryCast {
                            expr: right_expr, ..
                        })
                        | Expr::Cast(Cast {
                            expr: right_expr, ..
                        }),
                    ) => {
                        // if the left_lit_value can be casted to the type of expr
                        // we need to unwrap the cast for cast/try_cast expr, and add cast to the literal
                        let Ok(expr_type) = right_expr.get_type(&self.schema) else {
                            return Ok(Transformed::no(expr));
                        };
                        let Some(value) =
                            try_cast_literal_to_type(left_lit_value, &expr_type)
                        else {
                            return Ok(Transformed::no(expr));
                        };
                        **left = lit(value);
                        // unwrap the cast/try_cast for the right expr
                        **right = mem::take(right_expr);
                        Ok(Transformed::yes(expr))
                    }
                    (
                        Expr::TryCast(TryCast {
                            expr: left_expr, ..
                        })
                        | Expr::Cast(Cast {
                            expr: left_expr, ..
                        }),
                        Expr::Literal(right_lit_value),
                    ) => {
                        // if the right_lit_value can be casted to the type of expr
                        // we need to unwrap the cast for cast/try_cast expr, and add cast to the literal
                        let Ok(expr_type) = left_expr.get_type(&self.schema) else {
                            return Ok(Transformed::no(expr));
                        };
                        let Some(value) =
                            try_cast_literal_to_type(right_lit_value, &expr_type)
                        else {
                            return Ok(Transformed::no(expr));
                        };
                        // unwrap the cast/try_cast for the left expr
                        **left = mem::take(left_expr);
                        **right = lit(value);
                        Ok(Transformed::yes(expr))
                    }
                    _ => Ok(Transformed::no(expr)),
                }
            }
            // For case:
            // try_cast/cast(expr as left_type) in (expr1,expr2,expr3)
            Expr::InList(InList {
                expr: left, list, ..
            }) => {
                let (Expr::TryCast(TryCast {
                    expr: left_expr, ..
                })
                | Expr::Cast(Cast {
                    expr: left_expr, ..
                })) = left.as_mut()
                else {
                    return Ok(Transformed::no(expr));
                };
                let Ok(expr_type) = left_expr.get_type(&self.schema) else {
                    return Ok(Transformed::no(expr));
                };
                if !is_supported_type(&expr_type) {
                    return Ok(Transformed::no(expr));
                }
                let Ok(right_exprs) = list
                    .iter()
                    .map(|right| {
                        let right_type = right.get_type(&self.schema)?;
                        if !is_supported_type(&right_type) {
                            internal_err!(
                                "The type of list expr {} is not supported",
                                &right_type
                            )?;
                        }
                        match right {
                            Expr::Literal(right_lit_value) => {
                                // if the right_lit_value can be casted to the type of internal_left_expr
                                // we need to unwrap the cast for cast/try_cast expr, and add cast to the literal
                                let Some(value) = try_cast_literal_to_type(right_lit_value, &expr_type) else {
                                    internal_err!(
                                        "Can't cast the list expr {:?} to type {:?}",
                                        right_lit_value, &expr_type
                                    )?
                                };
                                Ok(lit(value))
                            }
                            other_expr => internal_err!(
                                "Only support literal expr to optimize, but the expr is {:?}",
                                &other_expr
                            ),
                        }
                    })
                    .collect::<Result<Vec<_>>>() else {
                    return Ok(Transformed::no(expr))
                };
                **left = mem::take(left_expr);
                *list = right_exprs;
                Ok(Transformed::yes(expr))
            }
            // TODO: handle other expr type and dfs visit them
            _ => Ok(Transformed::no(expr)),
        }
    }
}

fn is_comparison_op(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq
    )
}

/// Returns true if [UnwrapCastExprRewriter] supports this data type
fn is_supported_type(data_type: &DataType) -> bool {
    is_supported_numeric_type(data_type)
        || is_supported_string_type(data_type)
        || is_supported_dictionary_type(data_type)
}

/// Returns true if [[UnwrapCastExprRewriter]] suppors this numeric type
fn is_supported_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
            | DataType::Timestamp(_, _)
    )
}

/// Returns true if [UnwrapCastExprRewriter] supports casting this value as a string
fn is_supported_string_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

/// Returns true if [UnwrapCastExprRewriter] supports casting this value as a dictionary
fn is_supported_dictionary_type(data_type: &DataType) -> bool {
    matches!(data_type,
                    DataType::Dictionary(_, inner) if is_supported_type(inner))
}

/// Convert a literal value from one data type to another
fn try_cast_literal_to_type(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_data_type = lit_value.data_type();
    if !is_supported_type(&lit_data_type) || !is_supported_type(target_type) {
        return None;
    }
    if lit_value.is_null() {
        // null value can be cast to any type of null value
        return ScalarValue::try_from(target_type).ok();
    }
    try_cast_numeric_literal(lit_value, target_type)
        .or_else(|| try_cast_string_literal(lit_value, target_type))
        .or_else(|| try_cast_dictionary(lit_value, target_type))
}

/// Convert a numeric value from one numeric data type to another
fn try_cast_numeric_literal(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_data_type = lit_value.data_type();
    if !is_supported_numeric_type(&lit_data_type)
        || !is_supported_numeric_type(target_type)
    {
        return None;
    }

    let mul = match target_type {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64 => 1_i128,
        DataType::Timestamp(_, _) => 1_i128,
        DataType::Decimal128(_, scale) => 10_i128.pow(*scale as u32),
        _ => return None,
    };
    let (target_min, target_max) = match target_type {
        DataType::UInt8 => (u8::MIN as i128, u8::MAX as i128),
        DataType::UInt16 => (u16::MIN as i128, u16::MAX as i128),
        DataType::UInt32 => (u32::MIN as i128, u32::MAX as i128),
        DataType::UInt64 => (u64::MIN as i128, u64::MAX as i128),
        DataType::Int8 => (i8::MIN as i128, i8::MAX as i128),
        DataType::Int16 => (i16::MIN as i128, i16::MAX as i128),
        DataType::Int32 => (i32::MIN as i128, i32::MAX as i128),
        DataType::Int64 => (i64::MIN as i128, i64::MAX as i128),
        DataType::Timestamp(_, _) => (i64::MIN as i128, i64::MAX as i128),
        DataType::Decimal128(precision, _) => (
            // Different precision for decimal128 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL_FOR_EACH_PRECISION[*precision as usize - 1],
            MAX_DECIMAL_FOR_EACH_PRECISION[*precision as usize - 1],
        ),
        _ => return None,
    };
    let lit_value_target_type = match lit_value {
        ScalarValue::Int8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampSecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMillisecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMicrosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampNanosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                (*v).checked_mul(mul / lit_scale_mul)
            } else if (*v) % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(*v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
        _ => None,
    };

    match lit_value_target_type {
        None => None,
        Some(value) => {
            if value >= target_min && value <= target_max {
                // the value casted from lit to the target type is in the range of target type.
                // return the target type of scalar value
                let result_scalar = match target_type {
                    DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
                    DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
                    DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
                    DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
                    DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
                    DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
                    DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
                    DataType::UInt64 => ScalarValue::UInt64(Some(value as u64)),
                    DataType::Timestamp(TimeUnit::Second, tz) => {
                        let value = cast_between_timestamp(
                            lit_data_type,
                            DataType::Timestamp(TimeUnit::Second, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampSecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                        let value = cast_between_timestamp(
                            lit_data_type,
                            DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMillisecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                        let value = cast_between_timestamp(
                            lit_data_type,
                            DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMicrosecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                        let value = cast_between_timestamp(
                            lit_data_type,
                            DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampNanosecond(value, tz.clone())
                    }
                    DataType::Decimal128(p, s) => {
                        ScalarValue::Decimal128(Some(value), *p, *s)
                    }
                    _ => {
                        return None;
                    }
                };
                Some(result_scalar)
            } else {
                None
            }
        }
    }
}

fn try_cast_string_literal(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let string_value = match lit_value {
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => s.clone(),
        _ => return None,
    };
    let scalar_value = match target_type {
        DataType::Utf8 => ScalarValue::Utf8(string_value),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(string_value),
        _ => return None,
    };
    Some(scalar_value)
}

/// Attempt to cast to/from a dictionary type by wrapping/unwrapping the dictionary
fn try_cast_dictionary(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_value_type = lit_value.data_type();
    let result_scalar = match (lit_value, target_type) {
        // Unwrap dictionary when inner type matches target type
        (ScalarValue::Dictionary(_, inner_value), _)
            if inner_value.data_type() == *target_type =>
        {
            (**inner_value).clone()
        }
        // Wrap type when target type is dictionary
        (_, DataType::Dictionary(index_type, inner_type))
            if **inner_type == lit_value_type =>
        {
            ScalarValue::Dictionary(index_type.clone(), Box::new(lit_value.clone()))
        }
        _ => {
            return None;
        }
    };
    Some(result_scalar)
}

/// Cast a timestamp value from one unit to another
fn cast_between_timestamp(from: DataType, to: DataType, value: i128) -> Option<i64> {
    let value = value as i64;
    let from_scale = match from {
        DataType::Timestamp(TimeUnit::Second, _) => 1,
        DataType::Timestamp(TimeUnit::Millisecond, _) => MILLISECONDS,
        DataType::Timestamp(TimeUnit::Microsecond, _) => MICROSECONDS,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => NANOSECONDS,
        _ => return Some(value),
    };

    let to_scale = match to {
        DataType::Timestamp(TimeUnit::Second, _) => 1,
        DataType::Timestamp(TimeUnit::Millisecond, _) => MILLISECONDS,
        DataType::Timestamp(TimeUnit::Microsecond, _) => MICROSECONDS,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => NANOSECONDS,
        _ => return Some(value),
    };

    match from_scale.cmp(&to_scale) {
        Ordering::Less => value.checked_mul(to_scale / from_scale),
        Ordering::Greater => Some(value / (from_scale / to_scale)),
        Ordering::Equal => Some(value),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    use arrow::compute::{cast_with_options, CastOptions};
    use arrow::datatypes::Field;
    use datafusion_common::tree_node::TransformedResult;
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

        // cast(c1, INT64) < INT64(NULL) => INT32(c1) < INT32(NULL)
        let c1_lt_lit_null = cast(col("c1"), DataType::Int64).lt(null_i64());
        let expected = col("c1").lt(null_i32());
        assert_eq!(optimize_test(c1_lt_lit_null, &schema), expected);

        // cast(INT8(NULL), INT32) < INT32(12) => INT8(NULL) < INT8(12)
        let lit_lt_lit = cast(null_i8(), DataType::Int32).lt(lit(12i32));
        let expected = null_i8().lt(lit(12i8));
        assert_eq!(optimize_test(lit_lt_lit, &schema), expected);
    }

    #[test]
    fn test_unwrap_cast_comparison_unsigned() {
        // "cast(c6, UINT64) = 0u64 => c6 = 0u32
        let schema = expr_test_schema();
        let expr_input = cast(col("c6"), DataType::UInt64).eq(lit(0u64));
        let expected = col("c6").eq(lit(0u32));
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
        let expected = lit("value").eq(col("str1"));
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
        let expr_input = cast(col("largestr"), dict.data_type()).eq(lit(dict.clone()));
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
        let expected = col("c3").lt(null_decimal(18, 2));
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
        // INT32(C1) IN (INT32(12),INT64(24)) -> INT32(C1) IN (INT32(12),INT32(24))
        let expr_lt =
            cast(col("c1"), DataType::Int64).in_list(vec![lit(12i64), lit(24i64)], false);
        let expected = col("c1").in_list(vec![lit(12i32), lit(24i32)], false);
        assert_eq!(optimize_test(expr_lt, &schema), expected);
        // INT32(C2) IN (INT64(NULL),INT64(24)) -> INT32(C1) IN (INT32(12),INT32(24))
        let expr_lt =
            cast(col("c2"), DataType::Int32).in_list(vec![null_i32(), lit(14i32)], false);
        let expected = col("c2").in_list(vec![null_i64(), lit(14i64)], false);

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

        // cast(INT32(12), INT64) IN (.....)
        let expr_lt = cast(lit(12i32), DataType::Int64)
            .in_list(vec![lit(13i64), lit(12i64)], false);
        let expected = lit(12i32).in_list(vec![lit(13i32), lit(12i32)], false);
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
        let expr_input =
            in_list(cast(col("c6"), DataType::Float64), vec![lit(0f64)], false);
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
        let mut expr_rewriter = UnwrapCastExprRewriter {
            schema: schema.clone(),
        };
        expr.rewrite(&mut expr_rewriter).data().unwrap()
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

    fn null_decimal(precision: u8, scale: i8) -> Expr {
        lit(ScalarValue::Decimal128(None, precision, scale))
    }

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    // this is the type that now() returns
    fn timestamp_nano_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Nanosecond, utc)
    }

    // a dictonary type for storing string tags
    fn dictionary_tag_type() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }

    #[test]
    fn test_try_cast_to_type_nulls() {
        // test that nulls can be cast to/from all integer types
        let scalars = vec![
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Decimal128(None, 3, 0),
            ScalarValue::Decimal128(None, 8, 2),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }
    }

    #[test]
    fn test_try_cast_to_type_int_in_range() {
        // test values that can be cast to/from all integer types
        let scalars = vec![
            ScalarValue::Int8(Some(123)),
            ScalarValue::Int16(Some(123)),
            ScalarValue::Int32(Some(123)),
            ScalarValue::Int64(Some(123)),
            ScalarValue::UInt8(Some(123)),
            ScalarValue::UInt16(Some(123)),
            ScalarValue::UInt32(Some(123)),
            ScalarValue::UInt64(Some(123)),
            ScalarValue::Decimal128(Some(123), 3, 0),
            ScalarValue::Decimal128(Some(12300), 8, 2),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }

        let max_i32 = ScalarValue::Int32(Some(i32::MAX));
        expect_cast(
            max_i32,
            DataType::UInt64,
            ExpectedCast::Value(ScalarValue::UInt64(Some(i32::MAX as u64))),
        );

        let min_i32 = ScalarValue::Int32(Some(i32::MIN));
        expect_cast(
            min_i32,
            DataType::Int64,
            ExpectedCast::Value(ScalarValue::Int64(Some(i32::MIN as i64))),
        );

        let max_i64 = ScalarValue::Int64(Some(i64::MAX));
        expect_cast(
            max_i64,
            DataType::UInt64,
            ExpectedCast::Value(ScalarValue::UInt64(Some(i64::MAX as u64))),
        );
    }

    #[test]
    fn test_try_cast_to_type_int_out_of_range() {
        let min_i32 = ScalarValue::Int32(Some(i32::MIN));
        let min_i64 = ScalarValue::Int64(Some(i64::MIN));
        let max_i64 = ScalarValue::Int64(Some(i64::MAX));
        let max_u64 = ScalarValue::UInt64(Some(u64::MAX));

        expect_cast(max_i64.clone(), DataType::Int8, ExpectedCast::NoValue);

        expect_cast(max_i64.clone(), DataType::Int16, ExpectedCast::NoValue);

        expect_cast(max_i64, DataType::Int32, ExpectedCast::NoValue);

        expect_cast(max_u64, DataType::Int64, ExpectedCast::NoValue);

        expect_cast(min_i64, DataType::UInt64, ExpectedCast::NoValue);

        expect_cast(min_i32, DataType::UInt64, ExpectedCast::NoValue);

        // decimal out of range
        expect_cast(
            ScalarValue::Decimal128(Some(99999999999999999999999999999999999900), 38, 0),
            DataType::Int64,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::Decimal128(Some(-9999999999999999999999999999999999), 37, 1),
            DataType::Int64,
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_decimal_cast_in_range() {
        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(3, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(123), 3, 0)),
        );

        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(8, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(123), 8, 0)),
        );

        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(8, 5),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(12300000), 8, 5)),
        );
    }

    #[test]
    fn test_try_decimal_cast_out_of_range() {
        // decimal would lose precision
        expect_cast(
            ScalarValue::Decimal128(Some(12345), 5, 2),
            DataType::Decimal128(3, 0),
            ExpectedCast::NoValue,
        );

        // decimal would lose precision
        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(2, 0),
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_cast_to_type_timestamps() {
        for time_unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let utc = Some("+00:00".into());
            // No timezone, utc timezone
            let (lit_tz_none, lit_tz_utc) = match time_unit {
                TimeUnit::Second => (
                    ScalarValue::TimestampSecond(Some(12345), None),
                    ScalarValue::TimestampSecond(Some(12345), utc),
                ),

                TimeUnit::Millisecond => (
                    ScalarValue::TimestampMillisecond(Some(12345), None),
                    ScalarValue::TimestampMillisecond(Some(12345), utc),
                ),

                TimeUnit::Microsecond => (
                    ScalarValue::TimestampMicrosecond(Some(12345), None),
                    ScalarValue::TimestampMicrosecond(Some(12345), utc),
                ),

                TimeUnit::Nanosecond => (
                    ScalarValue::TimestampNanosecond(Some(12345), None),
                    ScalarValue::TimestampNanosecond(Some(12345), utc),
                ),
            };

            // Datafusion ignores timezones for comparisons of ScalarValue
            // so double check it here
            assert_eq!(lit_tz_none, lit_tz_utc);

            // e.g. DataType::Timestamp(_, None)
            let dt_tz_none = lit_tz_none.data_type();

            // e.g. DataType::Timestamp(_, Some(utc))
            let dt_tz_utc = lit_tz_utc.data_type();

            // None <--> None
            expect_cast(
                lit_tz_none.clone(),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // None <--> Utc
            expect_cast(
                lit_tz_none.clone(),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // Utc <--> None
            expect_cast(
                lit_tz_utc.clone(),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // Utc <--> Utc
            expect_cast(
                lit_tz_utc.clone(),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // timestamp to int64
            expect_cast(
                lit_tz_utc.clone(),
                DataType::Int64,
                ExpectedCast::Value(ScalarValue::Int64(Some(12345))),
            );

            // int64 to timestamp
            expect_cast(
                ScalarValue::Int64(Some(12345)),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // int64 to timestamp
            expect_cast(
                ScalarValue::Int64(Some(12345)),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // timestamp to string (not supported yet)
            expect_cast(
                lit_tz_utc.clone(),
                DataType::LargeUtf8,
                ExpectedCast::NoValue,
            );
        }
    }

    #[test]
    fn test_try_cast_to_type_unsupported() {
        // int64 to list
        expect_cast(
            ScalarValue::Int64(Some(12345)),
            DataType::List(Arc::new(Field::new("f", DataType::Int32, true))),
            ExpectedCast::NoValue,
        );
    }

    #[derive(Debug, Clone)]
    enum ExpectedCast {
        /// test successfully cast value and it is as specified
        Value(ScalarValue),
        /// test returned OK, but could not cast the value
        NoValue,
    }

    /// Runs try_cast_literal_to_type with the specified inputs and
    /// ensure it computes the expected output, and ensures the
    /// casting is consistent with the Arrow kernels
    fn expect_cast(
        literal: ScalarValue,
        target_type: DataType,
        expected_result: ExpectedCast,
    ) {
        let actual_value = try_cast_literal_to_type(&literal, &target_type);

        println!("expect_cast: ");
        println!("  {literal:?} --> {target_type:?}");
        println!("  expected_result: {expected_result:?}");
        println!("  actual_result:   {actual_value:?}");

        match expected_result {
            ExpectedCast::Value(expected_value) => {
                let actual_value =
                    actual_value.expect("Expected cast value but got None");

                assert_eq!(actual_value, expected_value);

                // Verify that calling the arrow
                // cast kernel yields the same results
                // input array
                let literal_array = literal
                    .to_array_of_size(1)
                    .expect("Failed to convert to array of size");
                let expected_array = expected_value
                    .to_array_of_size(1)
                    .expect("Failed to convert to array of size");
                let cast_array = cast_with_options(
                    &literal_array,
                    &target_type,
                    &CastOptions::default(),
                )
                .expect("Expected to be cast array with arrow cast kernel");

                assert_eq!(
                    &expected_array, &cast_array,
                    "Result of casting {literal:?} with arrow was\n {cast_array:#?}\nbut expected\n{expected_array:#?}"
                );

                // Verify that for timestamp types the timezones are the same
                // (ScalarValue::cmp doesn't account for timezones);
                if let (
                    DataType::Timestamp(left_unit, left_tz),
                    DataType::Timestamp(right_unit, right_tz),
                ) = (actual_value.data_type(), expected_value.data_type())
                {
                    assert_eq!(left_unit, right_unit);
                    assert_eq!(left_tz, right_tz);
                }
            }
            ExpectedCast::NoValue => {
                assert!(
                    actual_value.is_none(),
                    "Expected no cast value, but got {actual_value:?}"
                );
            }
        }
    }

    #[test]
    fn test_try_cast_literal_to_timestamp() {
        // same timestamp
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123456), None)
        );

        // TimestampNanosecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123), None)
        );

        // TimestampNanosecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(Some(0), None));

        // TimestampNanosecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(0), None));

        // TimestampMicrosecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000), None)
        );

        // TimestampMicrosecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(Some(0), None));

        // TimestampMicrosecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123456789), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(123), None));

        // TimestampMillisecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000000), None)
        );

        // TimestampMillisecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123000), None)
        );
        // TimestampMillisecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123456789), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(123456), None));

        // TimestampSecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000000000), None)
        );

        // TimestampSecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123000000), None)
        );

        // TimestampSecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMillisecond(Some(123000), None)
        );

        // overflow
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(i64::MAX), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(None, None));
    }

    #[test]
    fn test_try_cast_to_string_type() {
        let scalars = vec![
            ScalarValue::from("string"),
            ScalarValue::LargeUtf8(Some("string".to_owned())),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }
    }
    #[test]
    fn test_try_cast_to_dictionary_type() {
        fn dictionary_type(t: DataType) -> DataType {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(t))
        }
        fn dictionary_value(value: ScalarValue) -> ScalarValue {
            ScalarValue::Dictionary(Box::new(DataType::Int32), Box::new(value))
        }
        let scalars = vec![
            ScalarValue::from("string"),
            ScalarValue::LargeUtf8(Some("string".to_owned())),
        ];
        for s in &scalars {
            expect_cast(
                s.clone(),
                dictionary_type(s.data_type()),
                ExpectedCast::Value(dictionary_value(s.clone())),
            );
            expect_cast(
                dictionary_value(s.clone()),
                s.data_type(),
                ExpectedCast::Value(s.clone()),
            )
        }
    }
}
