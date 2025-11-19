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

use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;
use datafusion_common::{
    internal_err, tree_node::Transformed, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{
    and, expr::ScalarFunction, lit, or, simplify::SimplifyInfo, BinaryExpr, Expr,
    Operator,
};

pub(super) fn unwrap_date_part_in_comparison_for_binary<S: SimplifyInfo>(
    info: &S,
    cast_expr: Expr,
    literal: Expr,
    op: Operator,
) -> Result<Transformed<Expr>> {
    let (args, lit_value) = match (cast_expr, literal) {
        (
            Expr::ScalarFunction(ScalarFunction { func, args }),
            Expr::Literal(lit_value, _),
        ) if func.name() == "date_part" => (args, lit_value),
        _ => return internal_err!("Expect date_part expr and literal"),
    };
    let expr = Box::new(args[1].clone());

    let Ok(expr_type) = info.get_data_type(&expr) else {
        return internal_err!("Can't get the data type of the expr {:?}", &expr);
    };

    // Helper to cast literal
    let cast_year = |updated_year: &ScalarValue| -> Result<ScalarValue, DataFusionError> {
        year_literal_to_type(updated_year, &expr_type).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Can't cast {lit_value} to type {expr_type}"
            ))
        })
    };

    let rewritten_expr = match op {
        Operator::Lt | Operator::GtEq => {
            let v = cast_year(&lit_value)?;
            Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op,
                right: Box::new(lit(v)),
            })
        }
        Operator::Gt => {
            let year = match lit_value {
                ScalarValue::Int32(Some(y)) => y + 1,
                _ => return internal_err!("Expected Int32 Literal"),
            };
            let updated_year = ScalarValue::Int32(Some(year));
            let v = cast_year(&updated_year)?;
            Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op: Operator::GtEq,
                right: Box::new(lit(v)),
            })
        }
        Operator::LtEq => {
            let year = match lit_value {
                ScalarValue::Int32(Some(y)) => y + 1,
                _ => return internal_err!("Expected Int32 Literal"),
            };
            let updated_year = ScalarValue::Int32(Some(year));
            let v = cast_year(&updated_year)?;
            Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op: Operator::Lt,
                right: Box::new(lit(v)),
            })
        }
        Operator::Eq => {
            let year = match lit_value {
                ScalarValue::Int32(Some(y)) => y + 1,
                _ => return internal_err!("Expected Int32 Literal"),
            };
            let updated_year = ScalarValue::Int32(Some(year));
            let lower = cast_year(&lit_value)?;
            let upper = cast_year(&updated_year)?;
            and(
                Expr::BinaryExpr(BinaryExpr {
                    left: expr.clone(),
                    op: Operator::GtEq,
                    right: Box::new(lit(lower)),
                }),
                Expr::BinaryExpr(BinaryExpr {
                    left: expr,
                    op: Operator::Lt,
                    right: Box::new(lit(upper)),
                }),
            )
        }
        Operator::NotEq => {
            let year = match lit_value {
                ScalarValue::Int32(Some(y)) => y + 1,
                _ => return internal_err!("Expected Int32 Literal"),
            };
            let updated_year = ScalarValue::Int32(Some(year));
            let lower = cast_year(&lit_value)?;
            let upper = cast_year(&updated_year)?;
            or(
                Expr::BinaryExpr(BinaryExpr {
                    left: expr.clone(),
                    op: Operator::Lt,
                    right: Box::new(lit(lower)),
                }),
                Expr::BinaryExpr(BinaryExpr {
                    left: expr,
                    op: Operator::GtEq,
                    right: Box::new(lit(upper)),
                }),
            )
        }
        _ => return internal_err!("Expect comparison operators"),
    };
    Ok(Transformed::yes(rewritten_expr))
}

pub(super) fn is_date_part_expr_and_support_unwrap_date_part_in_comparison_for_binary<
    S: SimplifyInfo,
>(
    info: &S,
    expr: &Expr,
    op: Operator,
    literal: &Expr,
) -> bool {
    match (expr, op, literal) {
        (
            Expr::ScalarFunction(ScalarFunction { func, args }),
            Operator::Eq
            | Operator::NotEq
            | Operator::Gt
            | Operator::Lt
            | Operator::GtEq
            | Operator::LtEq,
            Expr::Literal(lit_val, _),
        ) if func.name() == "date_part" => {
            let left_expr = Box::new(args[1].clone());
            let Ok(expr_type) = info.get_data_type(&left_expr) else {
                return false;
            };
            let Ok(_lit_type) = info.get_data_type(literal) else {
                return false;
            };

            year_literal_to_type(lit_val, &expr_type).is_some()
        }
        _ => false,
    }
}

// pub(super) fn is_date_part_expr_and_support_unwrap_date_part_in_comparison_for_inlist<
//     S: SimplifyInfo,
// >(
//     info: &S,
//     expr: &Expr,
//     list: &[Expr],
// ) -> bool {
//     match expr {
//         Expr::ScalarFunction(ScalarFunction { func, args })
//             if func.name() == "date_part" =>
//         {
//             let left_expr = Box::new(args[1].clone());
//             let Ok(expr_type) = info.get_data_type(&left_expr) else {
//                 return false;
//             };
//             for right in list {
//                 match right {
//                     Expr::Literal(lit_val, _)
//                         if year_literal_to_type(lit_val, &expr_type).is_some() => {}
//                     _ => return false,
//                 }
//             }
//             true
//         }
//         _ => false,
//     }
// }

/// Cast the year to the right datatype
fn year_literal_to_type(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let year = match lit_value {
        ScalarValue::Int32(Some(y)) => *y,
        _ => return None,
    };
    // Can only extract year from Date32/64 and Timestamp
    match target_type {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {}
        _ => return None,
    }

    let naive_date = NaiveDate::from_ymd_opt(year, 1, 1).expect("Invalid year");

    let casted = match target_type {
        DataType::Date32 => {
            let days = naive_date
                .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
                .num_days() as i32;
            ScalarValue::Date32(Some(days))
        }
        DataType::Date64 => {
            let milis = naive_date
                .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
                .num_milliseconds();
            ScalarValue::Date64(Some(milis))
        }
        DataType::Timestamp(unit, tz) => {
            let days = naive_date
                .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
                .num_days();
            match unit {
                TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(days * 86_400), tz.clone())
                }
                TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(days * 86_400_000), tz.clone())
                }
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(
                    Some(days * 86_400_000_000),
                    tz.clone(),
                ),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(
                    Some(days * 86_400_000_000_000),
                    tz.clone(),
                ),
            }
        }
        _ => return None,
    };

    Some(casted)
}

#[cfg(test)]
mod tests {
    use crate::simplify_expressions::ExprSimplifier;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
    use datafusion_expr::expr_fn::col;
    use datafusion_expr::or;
    use datafusion_expr::{
        and, execution_props::ExecutionProps, lit, simplify::SimplifyContext, Expr,
    };
    use datafusion_functions::datetime::expr_fn;
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn test_preimage_date_part_date32_eq() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
        let expr_lt = expr_fn::date_part(lit("year"), col("date32")).eq(lit(2024i32));
        let expected = and(
            col("date32").gt_eq(lit(ScalarValue::Date32(Some(19723)))),
            col("date32").lt(lit(ScalarValue::Date32(Some(20089)))),
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_date64_not_eq() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) <> 2024 -> c1 < 2024-01-01 AND c1 >= 2025-01-01
        let expr_lt = expr_fn::date_part(lit("year"), col("date64")).not_eq(lit(2024i32));
        let expected = or(
            col("date64").lt(lit(ScalarValue::Date64(Some(19723 * 86_400_000)))),
            col("date64").gt_eq(lit(ScalarValue::Date64(Some(20089 * 86_400_000)))),
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_lt() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_nano_none")).lt(lit(2024i32));
        let expected = col("ts_nano_none").lt(lit(ScalarValue::TimestampNanosecond(
            Some(19723 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_utc_gt() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_nano_utc")).gt(lit(2024i32));
        let expected = col("ts_nano_utc").gt_eq(lit(ScalarValue::TimestampNanosecond(
            Some(20089 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_sec_est_gt_eq() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_sec_est")).gt_eq(lit(2024i32));
        let expected = col("ts_sec_est").gt_eq(lit(ScalarValue::TimestampSecond(
            Some(19723 * 86_400),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_sec_est_lt_eq() {
        let schema = expr_test_schema();
        let expr_lt =
            expr_fn::date_part(lit("year"), col("ts_mic_pt")).lt_eq(lit(2024i32));
        let expected = col("ts_mic_pt").lt(lit(ScalarValue::TimestampMicrosecond(
            Some(20089 * 86_400_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    #[test]
    fn test_preimage_date_part_timestamp_nano_lt_swap() {
        let schema = expr_test_schema();
        let expr_lt =
            lit(2024i32).gt(expr_fn::date_part(lit("year"), col("ts_nano_none")));
        let expected = col("ts_nano_none").lt(lit(ScalarValue::TimestampNanosecond(
            Some(19723 * 86_400_000_000_000),
            None,
        )));
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

    // #[test]
    // fn test_preimage_date_part_date32_in_list() {
    //     let schema = expr_test_schema();
    //     let expr_lt = expr_fn::date_part(lit("year"), col("date32"))
    //         .in_list(vec![lit(2024i32), lit(1984i32)], false);
    //     let expected = (col("date32")
    //         .gt_eq(lit(ScalarValue::Date32(Some(19723))))
    //         .or(col("date32").lt(lit(ScalarValue::Date32(Some(20089))))))
    //     .or(col("date32")
    //         .gt_eq(lit(ScalarValue::Date32(Some(5113))))
    //         .or(col("date32").lt(lit(ScalarValue::Date32(Some(5480))))));
    //     assert_eq!(optimize_test(expr_lt, &schema), expected)
    // }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let props = ExecutionProps::new();
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&props).with_schema(Arc::clone(schema)),
        );

        simplifier.simplify(expr).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualified_fields(
                vec![
                    Field::new("date32", DataType::Date32, false),
                    Field::new("date64", DataType::Date64, false),
                    Field::new("ts_nano_none", timestamp_nano_none_type(), false),
                    Field::new("ts_nano_utc", timestamp_nano_utc_type(), false),
                    Field::new("ts_sec_est", timestamp_sec_est_type(), false),
                    Field::new("ts_mic_pt", timestamp_mic_pt_type(), false),
                ]
                .into(),
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    // this is the type that now() returns
    fn timestamp_nano_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Nanosecond, utc)
    }

    fn timestamp_sec_est_type() -> DataType {
        let est = Some("-5:00".into());
        DataType::Timestamp(TimeUnit::Second, est)
    }

    fn timestamp_mic_pt_type() -> DataType {
        let pt = Some("-8::00".into());
        DataType::Timestamp(TimeUnit::Microsecond, pt)
    }
}
