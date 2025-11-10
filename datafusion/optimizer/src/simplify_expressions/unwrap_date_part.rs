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
use datafusion_common::{Result, ScalarValue, internal_err, tree_node::Transformed};
use datafusion_expr::{BinaryExpr, Expr, Operator, expr::ScalarFunction, lit, simplify::SimplifyInfo};
use datafusion_expr_common::casts::try_cast_literal_to_type;
use datafusion_functions::datetime::{date_part, expr_fn};

pub(super) fn unwrap_date_part_in_comparison_for_binary<S: SimplifyInfo>(
    info: &S,
    cast_expr: Expr,
    literal: Expr,
    op: Operator,
) -> Result<Transformed<Expr>> {
    match (cast_expr, literal) {
        (
            Expr::ScalarFunction(ScalarFunction {
                func,
                args
            }),
            Expr::Literal(lit_value, _),
        ) if func.name() == "date_part" => {
            if let Some(value) = year_literal_to_type_with_op(&lit_value, &expr_type, op)
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
        _ => internal_err!("Expect date_part expr and literal"),
    }
}

/// This is just to extract cast the year to the right datatype
fn year_literal_to_type_with_op(
    lit_value: &ScalarValue, 
    target_type: &DataType,
    op: Operator,
) -> Option<ScalarValue> {
    match (op, lit_value) {
        (
            Operator::Eq | Operator::NotEq,
            ScalarValue::Int32(Some(year)),
        ) => {
            // Can only extract year from Date32/64 and Timestamp
            use DataType::*;
            if matches!(
                target_type,
                Date32 | Date64 | Timestamp(_,_)
            ) {
            let naive_date = NaiveDate::from_ymd_opt(*year, 1, 1).expect("Invalid year");

            let casted = match target_type {
                Date32 => {
                    let days = naive_date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?).num_days() as i32;
                    ScalarValue::Date32(Some(days))
                },
                Date64 => {
                    let milis = naive_date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?).num_milliseconds();
                    ScalarValue::Date64(Some(milis))
                },
                Timestamp(unit, tz) => {
                    let days = naive_date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?).num_days();
                    match unit {
                        TimeUnit::Second => ScalarValue::TimestampSecond(Some(days * 86_400), tz.clone()),
                        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(days * 86_400_000), tz.clone()),
                        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(days * 86_400_000_000), tz.clone()),
                        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(days * 86_400_000_000_000), tz.clone()),
                    }
                },
                _ => return None
            };

            return Some(casted)

            }
             else {
                None
            }
        }
        _ => None,
    }
}




#[cfg(test)]
mod tests {
    use crate::simplify_expressions::ExprSimplifier;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
    use datafusion_expr::expr_fn::col;
    use datafusion_expr::{
        and, execution_props::ExecutionProps, lit, simplify::SimplifyContext, Expr,
    };
    use datafusion_functions::datetime::expr_fn;
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn test_unwrap_date_part_comparison() {
        let schema = expr_test_schema();
        // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
        let expr_lt = expr_fn::date_part(lit("year"), col("c1")).eq(lit(2024i32));
        let expected = and(
            col("c1").gt_eq(lit(ScalarValue::Date32(Some(19723)))),
            col("c1").lt(lit(ScalarValue::Date32(Some(20088)))),
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected)
    }

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
                    Field::new("c1", DataType::Date32, false),
                    Field::new("c2", DataType::Date64, false),
                    Field::new("ts_nano_none", timestamp_nano_none_type(), false),
                    Field::new("ts_nano_utf", timestamp_nano_utc_type(), false),
                ]
                .into(),
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    // fn lit_timestamp_nano_none(ts: i64) -> Expr {
    //     lit(ScalarValue::TimestampNanosecond(Some(ts), None))
    // }

    // fn lit_timestamp_nano_utc(ts: i64) -> Expr {
    //     let utc = Some("+0:00".into());
    //     lit(ScalarValue::TimestampNanosecond(Some(ts), utc))
    // }

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    // this is the type that now() returns
    fn timestamp_nano_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Nanosecond, utc)
    }
}
