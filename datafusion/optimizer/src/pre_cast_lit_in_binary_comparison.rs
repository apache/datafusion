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
use arrow::datatypes::DataType;
use datafusion_common::{DFSchemaRef, Result, ScalarValue};
use datafusion_expr::utils::from_plan;
use datafusion_expr::{binary_expr, lit, Expr, ExprSchemable, LogicalPlan, Operator};

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
pub struct PreCastLitInBinaryComparisonExpressions {}

impl PreCastLitInBinaryComparisonExpressions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for PreCastLitInBinaryComparisonExpressions {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        optimize(plan)
    }

    fn name(&self) -> &str {
        "pre_cast_lit_in_binary_comparison"
    }
}

fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|input| optimize(input))
        .collect::<Result<Vec<_>>>()?;

    let schema = plan.schema();
    let new_exprs = plan
        .expressions()
        .into_iter()
        .map(|expr| visit_expr(expr, schema))
        .collect::<Vec<_>>();

    from_plan(plan, new_exprs.as_slice(), new_inputs.as_slice())
}

// Visit all type of expr, if the current has child expr, the child expr needed to visit first.
fn visit_expr(expr: Expr, schema: &DFSchemaRef) -> Expr {
    // traverse the expr by dfs
    match &expr {
        Expr::BinaryExpr { left, op, right } => {
            // dfs visit the left and right expr
            let left = visit_expr(*left.clone(), schema);
            let right = visit_expr(*right.clone(), schema);
            let left_type = left.get_type(schema);
            let right_type = right.get_type(schema);
            // can't get the data type, just return the expr
            if left_type.is_err() || right_type.is_err() {
                return expr.clone();
            }
            let left_type = left_type.unwrap();
            let right_type = right_type.unwrap();
            if !left_type.eq(&right_type)
                && is_support_data_type(&left_type)
                && is_support_data_type(&right_type)
                && is_comparison_op(op)
            {
                match (&left, &right) {
                    (Expr::Literal(_), Expr::Literal(_)) => {
                        // do nothing
                    }
                    (Expr::Literal(left_lit_value), _)
                        if can_integer_literal_cast_to_type(
                            left_lit_value,
                            &right_type,
                        ) =>
                    {
                        // cast the left literal to the right type
                        return binary_expr(
                            cast_to_other_scalar_expr(left_lit_value, &right_type),
                            *op,
                            right,
                        );
                    }
                    (_, Expr::Literal(right_lit_value))
                        if can_integer_literal_cast_to_type(
                            right_lit_value,
                            &left_type,
                        ) =>
                    {
                        // cast the right literal to the left type
                        return binary_expr(
                            left,
                            *op,
                            cast_to_other_scalar_expr(right_lit_value, &left_type),
                        );
                    }
                    (_, _) => {
                        // do nothing
                    }
                };
            }
            // return the new binary op
            binary_expr(left, *op, right)
        }
        // TODO: optimize in list
        // Expr::InList { .. } => {}
        // TODO: handle other expr type and dfs visit them
        _ => expr,
    }
}

fn cast_to_other_scalar_expr(origin_value: &ScalarValue, target_type: &DataType) -> Expr {
    // null case
    if origin_value.is_null() {
        // if the origin value is null, just convert to another type of null value
        // The target type must be satisfied `is_support_data_type` method, we can unwrap safely
        return lit(ScalarValue::try_from(target_type).unwrap());
    }
    // no null case
    let value: i64 = match origin_value {
        ScalarValue::Int8(Some(v)) => *v as i64,
        ScalarValue::Int16(Some(v)) => *v as i64,
        ScalarValue::Int32(Some(v)) => *v as i64,
        ScalarValue::Int64(Some(v)) => *v as i64,
        other_type => {
            panic!("Invalid type and value {:?}", other_type);
        }
    };
    lit(match target_type {
        DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
        DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(value)),
        other_type => {
            panic!("Invalid target data type {:?}", other_type);
        }
    })
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
    // TODO support decimal with other data type
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn can_integer_literal_cast_to_type(
    integer_lit_value: &ScalarValue,
    target_type: &DataType,
) -> bool {
    if integer_lit_value.is_null() {
        // null value can be cast to any type of null value
        return true;
    }
    let (target_min, target_max) = match target_type {
        DataType::Int8 => (i8::MIN as i128, i8::MAX as i128),
        DataType::Int16 => (i16::MIN as i128, i16::MAX as i128),
        DataType::Int32 => (i32::MIN as i128, i32::MAX as i128),
        DataType::Int64 => (i64::MIN as i128, i64::MAX as i128),
        _ => panic!("Error target data type {:?}", target_type),
    };
    let lit_value = match integer_lit_value {
        ScalarValue::Int8(Some(v)) => *v as i128,
        ScalarValue::Int16(Some(v)) => *v as i128,
        ScalarValue::Int32(Some(v)) => *v as i128,
        ScalarValue::Int64(Some(v)) => *v as i128,
        _ => {
            panic!("Invalid literal value {:?}", integer_lit_value)
        }
    };
    if lit_value >= target_min && lit_value <= target_max {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::pre_cast_lit_in_binary_comparison::visit_expr;
    use arrow::datatypes::DataType;
    use datafusion_common::{DFField, DFSchema, DFSchemaRef, ScalarValue};
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
        let expr_lt = col("c1").lt(lit(ScalarValue::Int32(Some(16))));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // the 99999999999 is not within the range of MAX(int32) and MIN(int32), we don't cast the lit(99999999999) to int32 type
        let expr_lt = col("c1").lt(lit(ScalarValue::Int64(Some(99999999999))));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);
    }

    #[test]
    fn test_pre_cast_lit_comparison() {
        let schema = expr_test_schema();
        // c1 < INT64(16) -> c1 < cast(INT32(16))
        // the 16 is within the range of MAX(int32) and MIN(int32), we can cast the 16 to int32(16)
        let expr_lt = col("c1").lt(lit(ScalarValue::Int64(Some(16))));
        let expected = col("c1").lt(lit(ScalarValue::Int32(Some(16))));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // INT64(c2) = INT32(16) => INT64(c2) = INT64(16)
        let c2_eq_lit = col("c2").eq(lit(ScalarValue::Int32(Some(16))));
        let expected = col("c2").eq(lit(ScalarValue::Int64(Some(16))));
        assert_eq!(optimize_test(c2_eq_lit.clone(), &schema), expected);

        // INT32(c1) < INT64(NULL) => INT32(c1) < INT32(NULL)
        let c1_lt_lit_null = col("c1").lt(lit(ScalarValue::Int64(None)));
        let expected = col("c1").lt(lit(ScalarValue::Int32(None)));
        assert_eq!(optimize_test(c1_lt_lit_null.clone(), &schema), expected);
    }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        visit_expr(expr, schema)
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "c1", DataType::Int32, false),
                    DFField::new(None, "c2", DataType::Int64, false),
                ],
                HashMap::new(),
            )
            .unwrap(),
        )
    }
}
