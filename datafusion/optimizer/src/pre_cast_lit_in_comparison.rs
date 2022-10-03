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

//! Pre-cast literal binary comparison rule can be only used to the binary comparison expr.
//! It can reduce adding the `Expr::Cast` to the expr instead of adding the `Expr::Cast` to literal expr.
use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::{
    DataType, MAX_DECIMAL_FOR_EACH_PRECISION, MIN_DECIMAL_FOR_EACH_PRECISION,
};
use datafusion_common::{DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr_rewriter::{ExprRewritable, ExprRewriter, RewriteRecursion};
use datafusion_expr::utils::from_plan;
use datafusion_expr::{
    binary_expr, in_list, lit, Expr, ExprSchemable, LogicalPlan, Operator,
};

/// The rule can be only used to the numeric binary comparison with literal expr, like below pattern:
/// `left_expr comparison_op literal_expr` or `literal_expr comparison_op right_expr`.
/// The data type of two sides must be signed numeric type now, and will support more data type later.
///
/// If the binary comparison expr match above rules, the optimizer will check if the value of `literal`
/// is in within range(min,max) which is the range(min,max) of the data type for `left_expr` or `right_expr`.
///
/// If this true, the literal expr will be casted to the data type of expr on the other side, and the result of
/// binary comparison will be `left_expr comparison_op cast(literal_expr, left_data_type)` or
/// `cast(literal_expr, right_data_type) comparison_op right_expr`. For better optimization,
/// the expr of `cast(literal_expr, target_type)` will be precomputed and converted to the new expr `new_literal_expr`
/// which data type is `target_type`.
/// If this false, do nothing.
///
/// This is inspired by the optimizer rule `UnwrapCastInBinaryComparison` of Spark.
/// # Example
///
/// `Filter: c1 > INT64(10)` will be optimized to `Filter: c1 > CAST(INT64(10) AS INT32),
/// and continue to be converted to `Filter: c1 > INT32(10)`, if the DataType of c1 is INT32.
///
#[derive(Default)]
pub struct PreCastLitInComparisonExpressions {}

impl PreCastLitInComparisonExpressions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for PreCastLitInComparisonExpressions {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        optimize(plan)
    }

    fn name(&self) -> &str {
        "pre_cast_lit_in_comparison"
    }
}

fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|input| optimize(input))
        .collect::<Result<Vec<_>>>()?;

    let schema = plan.schema();

    let mut expr_rewriter = PreCastLitExprRewriter {
        schema: schema.clone(),
    };

    let new_exprs = plan
        .expressions()
        .into_iter()
        .map(|expr| expr.rewrite(&mut expr_rewriter))
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, new_exprs.as_slice(), new_inputs.as_slice())
}

struct PreCastLitExprRewriter {
    schema: DFSchemaRef,
}

impl ExprRewriter for PreCastLitExprRewriter {
    fn pre_visit(&mut self, _expr: &Expr) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match &expr {
            Expr::BinaryExpr { left, op, right } => {
                let left = left.as_ref().clone();
                let right = right.as_ref().clone();
                let left_type = left.get_type(&self.schema);
                let right_type = right.get_type(&self.schema);
                // can't get the data type, just return the expr
                if left_type.is_err() || right_type.is_err() {
                    return Ok(expr.clone());
                }
                let left_type = left_type?;
                let right_type = right_type?;
                if !left_type.eq(&right_type)
                    && is_support_data_type(&left_type)
                    && is_support_data_type(&right_type)
                    && is_comparison_op(op)
                {
                    match (&left, &right) {
                        (Expr::Literal(_), Expr::Literal(_)) => {
                            // do nothing
                        }
                        (Expr::Literal(left_lit_value), _) => {
                            let casted_scalar_value =
                                try_cast_literal_to_type(left_lit_value, &right_type)?;
                            if let Some(value) = casted_scalar_value {
                                return Ok(binary_expr(lit(value), *op, right));
                            }
                        }
                        (_, Expr::Literal(right_lit_value)) => {
                            let casted_scalar_value =
                                try_cast_literal_to_type(right_lit_value, &left_type)?;
                            if let Some(value) = casted_scalar_value {
                                return Ok(binary_expr(left, *op, lit(value)));
                            }
                        }
                        (_, _) => {
                            // do nothing
                        }
                    };
                }
                // return the new binary op
                Ok(binary_expr(left, *op, right))
            }
            Expr::InList {
                expr: left_expr,
                list,
                negated,
            } => {
                let left = left_expr.as_ref().clone();
                let left_type = left.get_type(&self.schema);
                if left_type.is_err() {
                    // error data type
                    return Ok(expr);
                }
                let left_type = left_type?;
                if !is_support_data_type(&left_type) {
                    // not supported data type
                    return Ok(expr);
                }
                let right_exprs = list
                    .iter()
                    .map(|right| {
                        let right_type = right.get_type(&self.schema)?;
                        if !is_support_data_type(&right_type) {
                            return Err(DataFusionError::Internal(format!(
                                "The type of list expr {} not support",
                                &right_type
                            )));
                        }
                        match right {
                            Expr::Literal(right_lit_value) => {
                                let casted_scalar_value =
                                    try_cast_literal_to_type(right_lit_value, &left_type)?;
                                if let Some(value) = casted_scalar_value {
                                    Ok(lit(value))
                                } else {
                                    Err(DataFusionError::Internal(format!(
                                        "Can't cast the list expr {:?} to type {:?}",
                                        right_lit_value, &left_type
                                    )))
                                }
                            }
                            other_expr => Err(DataFusionError::Internal(format!(
                                "Only support literal expr to optimize, but the expr is {:?}",
                                &other_expr
                            ))),
                        }
                    })
                    .collect::<Result<Vec<_>>>();
                match right_exprs {
                    Ok(right_exprs) => Ok(in_list(left, right_exprs, *negated)),
                    Err(_) => Ok(expr),
                }
            }
            // TODO: handle other expr type and dfs visit them
            _ => Ok(expr),
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

fn is_support_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
    )
}

fn try_cast_literal_to_type(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Result<Option<ScalarValue>> {
    if lit_value.is_null() {
        // null value can be cast to any type of null value
        return Ok(Some(ScalarValue::try_from(target_type)?));
    }
    let mul = match target_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => 1_i128,
        DataType::Decimal128(_, scale) => 10_i128.pow(*scale as u32),
        other_type => {
            return Err(DataFusionError::Internal(format!(
                "Error target data type {:?}",
                other_type
            )));
        }
    };
    let (target_min, target_max) = match target_type {
        DataType::Int8 => (i8::MIN as i128, i8::MAX as i128),
        DataType::Int16 => (i16::MIN as i128, i16::MAX as i128),
        DataType::Int32 => (i32::MIN as i128, i32::MAX as i128),
        DataType::Int64 => (i64::MIN as i128, i64::MAX as i128),
        DataType::Decimal128(precision, _) => (
            // Different precision for decimal128 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL_FOR_EACH_PRECISION[*precision as usize - 1],
            MAX_DECIMAL_FOR_EACH_PRECISION[*precision as usize - 1],
        ),
        other_type => {
            return Err(DataFusionError::Internal(format!(
                "Error target data type {:?}",
                other_type
            )));
        }
    };
    let lit_value_target_type = match lit_value {
        ScalarValue::Int8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int64(Some(v)) => (*v as i128).checked_mul(mul),
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
        other_value => {
            return Err(DataFusionError::Internal(format!(
                "Invalid literal value {:?}",
                other_value
            )));
        }
    };

    match lit_value_target_type {
        None => Ok(None),
        Some(value) => {
            if value >= target_min && value <= target_max {
                // the value casted from lit to the target type is in the range of target type.
                // return the target type of scalar value
                let result_scalar = match target_type {
                    DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
                    DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
                    DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
                    DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
                    DataType::Decimal128(p, s) => {
                        ScalarValue::Decimal128(Some(value), *p, *s)
                    }
                    other_type => {
                        return Err(DataFusionError::Internal(format!(
                            "Error target data type {:?}",
                            other_type
                        )));
                    }
                };
                Ok(Some(result_scalar))
            } else {
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pre_cast_lit_in_comparison::PreCastLitExprRewriter;
    use arrow::datatypes::DataType;
    use datafusion_common::{DFField, DFSchema, DFSchemaRef, ScalarValue};
    use datafusion_expr::expr_rewriter::ExprRewritable;
    use datafusion_expr::{col, lit, Expr};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_not_cast_lit_comparison() {
        let schema = expr_test_schema();
        // INT8(NULL) < INT32(12)
        let lit_lt_lit =
            lit(ScalarValue::Int8(None)).lt(lit(ScalarValue::Int32(Some(12))));
        assert_eq!(optimize_test(lit_lt_lit.clone(), &schema), lit_lt_lit);
        // INT32(c1) > INT64(c2)
        let c1_gt_c2 = col("c1").gt(col("c2"));
        assert_eq!(optimize_test(c1_gt_c2.clone(), &schema), c1_gt_c2);

        // INT32(c1) < INT32(16), the type is same
        let expr_lt = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // the 99999999999 is not within the range of MAX(int32) and MIN(int32), we don't cast the lit(99999999999) to int32 type
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(99999999999i64));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);
    }

    #[test]
    fn test_pre_cast_lit_comparison() {
        let schema = expr_test_schema();
        // c1 < INT64(16) -> c1 < cast(INT32(16))
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
    fn test_not_cast_with_decimal_lit_comparison() {
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
    fn test_pre_cast_with_decimal_lit_comparison() {
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
    fn test_not_list_cast_lit_comparison() {
        let schema = expr_test_schema();
        // left type is not supported
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
        let expr_lt = col("c3").in_list(
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
    fn test_pre_list_cast_lit_comparison() {
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
        let expr_lt = col("c3").in_list(
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

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let mut expr_rewriter = PreCastLitExprRewriter {
            schema: schema.clone(),
        };
        expr.rewrite(&mut expr_rewriter).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "c1", DataType::Int32, false),
                    DFField::new(None, "c2", DataType::Int64, false),
                    DFField::new(None, "c3", DataType::Decimal128(18, 2), false),
                    DFField::new(None, "c4", DataType::Decimal128(38, 37), false),
                    DFField::new(None, "c5", DataType::Float32, false),
                ],
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

    fn lit_decimal(value: i128, precision: u8, scale: u8) -> Expr {
        lit(ScalarValue::Decimal128(Some(value), precision, scale))
    }

    fn null_decimal(precision: u8, scale: u8) -> Expr {
        lit(ScalarValue::Decimal128(None, precision, scale))
    }
}
