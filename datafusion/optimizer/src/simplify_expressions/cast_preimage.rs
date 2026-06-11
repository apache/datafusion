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

//! Preimage rewrites for cast comparisons.
//!
//! This module computes source-domain predicates for expressions such as
//! `CAST(expr AS target_type) OP literal`. For casts that are many-to-one, a
//! same-operator unwrap is not equivalent; the correct rewrite is a preimage
//! range over the input expression.

use arrow::datatypes::DataType;
use datafusion_common::{Result, internal_err, tree_node::Transformed};
use datafusion_expr::expr::InList;
use datafusion_expr::{
    BinaryExpr, Cast, Expr, Operator, TryCast, lit, simplify::SimplifyContext,
};
use datafusion_expr_common::casts::{
    CastPredicatePreimage, cast_predicate_exact_literal, cast_predicate_preimage,
};

use super::udf_preimage::rewrite_with_preimage;

pub(super) fn rewrite_cast_predicate_for_binary(
    info: &SimplifyContext,
    cast_expr: Expr,
    literal: Expr,
    op: Operator,
) -> Result<Transformed<Expr>> {
    let Some((expr, target_type)) = cast_input_and_type(cast_expr) else {
        return internal_err!("Expect cast expr");
    };
    let Expr::Literal(lit_value, _) = literal else {
        return internal_err!("Expect literal expr");
    };

    let source_type = info.get_data_type(&expr)?;
    match cast_predicate_preimage(&source_type, &target_type, op, &lit_value)? {
        Some(CastPredicatePreimage::Range(interval)) => {
            rewrite_with_preimage(interval, op, *expr)
        }
        Some(CastPredicatePreimage::Exact(value)) => {
            Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op,
                right: Box::new(lit(value)),
            })))
        }
        None => internal_err!(
            "Can't compute cast predicate preimage for source type {} target type {} literal {:?}",
            source_type,
            target_type,
            lit_value
        ),
    }
}

pub(super) fn supports_cast_predicate_for_binary(
    info: &SimplifyContext,
    expr: &Expr,
    op: Operator,
    literal: &Expr,
) -> bool {
    if !matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) {
        return false;
    }

    let Some((inner_expr, target_type)) = cast_input_and_type_ref(expr) else {
        return false;
    };
    let Expr::Literal(lit_value, _) = literal else {
        return false;
    };
    let Ok(source_type) = info.get_data_type(inner_expr) else {
        return false;
    };

    cast_predicate_preimage(&source_type, target_type, op, lit_value)
        .ok()
        .flatten()
        .is_some()
}

pub(super) fn supports_cast_predicate_for_inlist(
    info: &SimplifyContext,
    expr: &Expr,
    list: &[Expr],
) -> bool {
    let Some((inner_expr, target_type)) = cast_input_and_type_ref(expr) else {
        return false;
    };
    let Ok(source_type) = info.get_data_type(inner_expr) else {
        return false;
    };

    list.iter().all(|right| match right {
        Expr::Literal(lit_val, _) => {
            cast_predicate_exact_literal(&source_type, target_type, lit_val).is_some()
        }
        _ => false,
    })
}

pub(super) fn rewrite_cast_predicate_for_inlist(
    info: &SimplifyContext,
    expr: Expr,
    list: Vec<Expr>,
    negated: bool,
) -> Result<Transformed<Expr>> {
    let Some((inner_expr, _target_type)) = cast_input_and_type(expr) else {
        return internal_err!("Expect cast expr");
    };
    let source_type = info.get_data_type(&inner_expr)?;

    let list = list
        .into_iter()
        .map(|right| match right {
            Expr::Literal(lit_value, _) => {
                let Some(value) =
                    cast_predicate_exact_literal(&source_type, &_target_type, &lit_value)
                else {
                    return internal_err!(
                        "Can't cast the list expr {:?} to type {}",
                        lit_value,
                        &source_type
                    );
                };
                Ok(lit(value))
            }
            other_expr => internal_err!(
                "Only support literal expr to optimize, but the expr is {:?}",
                &other_expr
            ),
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Transformed::yes(Expr::InList(InList {
        expr: inner_expr,
        list,
        negated,
    })))
}

fn cast_input_and_type(cast_expr: Expr) -> Option<(Box<Expr>, DataType)> {
    match cast_expr {
        Expr::TryCast(TryCast { expr, field, .. })
        | Expr::Cast(Cast { expr, field, .. }) => Some((expr, field.data_type().clone())),
        _ => None,
    }
}

fn cast_input_and_type_ref(cast_expr: &Expr) -> Option<(&Expr, &DataType)> {
    match cast_expr {
        Expr::TryCast(TryCast { expr, field, .. })
        | Expr::Cast(Cast { expr, field, .. }) => {
            Some((expr.as_ref(), field.data_type()))
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
    use datafusion_common::{DFSchema, DFSchemaRef, ScalarValue};
    use datafusion_expr::simplify::SimplifyContext;
    use datafusion_expr::{cast, col, in_list};

    #[test]
    fn test_cast_predicate_exact_literal_unwrap() {
        let schema = expr_test_schema();

        let expr = cast(col("c1"), DataType::Int64).gt(lit(10_i64));
        let expected = col("c1").gt(lit(10_i32));
        assert_eq!(optimize_test(expr, &schema), expected);

        let expr = lit(10_i64).lt(cast(col("c1"), DataType::Int64));
        let expected = col("c1").gt(lit(10_i32));
        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_cast_predicate_string_integer_round_trip() {
        let schema = expr_test_schema();

        let expr = cast(col("c1"), DataType::Utf8).eq(lit("123"));
        let expected = col("c1").eq(lit(123_i32));
        assert_eq!(optimize_test(expr, &schema), expected);

        let expr = cast(col("c1"), DataType::Utf8).eq(lit("0123"));
        assert_eq!(optimize_test(expr.clone(), &schema), expr);
    }

    #[test]
    fn test_cast_predicate_inlist_exact_unwrap() {
        let schema = expr_test_schema();

        let expr = in_list(
            cast(col("c1"), DataType::Int64),
            vec![lit(0_i64), lit(1_i64), lit(2_i64), lit(3_i64), lit(4_i64)],
            false,
        );
        let expected = in_list(
            col("c1"),
            vec![lit(0_i32), lit(1_i32), lit(2_i32), lit(3_i32), lit(4_i32)],
            false,
        );

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_cast_preimage_timestamp_precision_narrowing_eq() {
        let schema = expr_test_schema();

        let expr =
            cast(col("ts_nano"), timestamp_millis_type()).eq(lit_timestamp_millis(1000));
        let expected = col("ts_nano")
            .gt_eq(lit_timestamp_nano(1_000_000_000))
            .and(col("ts_nano").lt(lit_timestamp_nano(1_001_000_000)));
        assert_eq!(optimize_test(expr, &schema), expected);

        let expr =
            cast(col("ts_nano"), timestamp_millis_type()).eq(lit_timestamp_millis(0));
        let expected = col("ts_nano")
            .gt_eq(lit_timestamp_nano(-999_999))
            .and(col("ts_nano").lt(lit_timestamp_nano(1_000_000)));
        assert_eq!(optimize_test(expr, &schema), expected);

        let expr =
            cast(col("ts_nano"), timestamp_millis_type()).eq(lit_timestamp_millis(-1));
        let expected = col("ts_nano")
            .gt_eq(lit_timestamp_nano(-1_999_999))
            .and(col("ts_nano").lt(lit_timestamp_nano(-999_999)));
        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_cast_preimage_timestamp_precision_narrowing_inequality() {
        let schema = expr_test_schema();

        let expr =
            cast(col("ts_nano"), timestamp_millis_type()).gt(lit_timestamp_millis(1000));
        let expected = col("ts_nano").gt_eq(lit_timestamp_nano(1_001_000_000));
        assert_eq!(optimize_test(expr, &schema), expected);

        let expr =
            cast(col("ts_nano"), timestamp_millis_type()).lt_eq(lit_timestamp_millis(-1));
        let expected = col("ts_nano").lt(lit_timestamp_nano(-999_999));
        assert_eq!(optimize_test(expr, &schema), expected);
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
                    Field::new("ts_nano", timestamp_nano_type(), false),
                ]
                .into(),
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    fn lit_timestamp_nano(ts: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(ts), None))
    }

    fn lit_timestamp_millis(ts: i64) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(ts), None))
    }

    fn timestamp_nano_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    fn timestamp_millis_type() -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    }
}
