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

use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::common::{Column, JoinType, NullEquality, not_impl_err, plan_err};
use datafusion::logical_expr::requalify_sides_if_needed;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator,
};

use substrait::proto::{JoinRel, join_rel};

pub async fn from_join_rel(
    consumer: &impl SubstraitConsumer,
    join: &JoinRel,
) -> datafusion::common::Result<LogicalPlan> {
    if join.post_join_filter.is_some() {
        return not_impl_err!("JoinRel with post_join_filter is not yet supported");
    }

    let left: LogicalPlanBuilder = LogicalPlanBuilder::from(
        consumer.consume_rel(join.left.as_ref().unwrap()).await?,
    );
    let right = LogicalPlanBuilder::from(
        consumer.consume_rel(join.right.as_ref().unwrap()).await?,
    );
    let (left, right, _requalified) = requalify_sides_if_needed(left, right)?;

    let join_type = from_substrait_jointype(join.r#type)?;
    // The join condition expression needs full input schema and not the output schema from join since we lose columns from
    // certain join types such as semi and anti joins
    let in_join_schema = left.schema().join(right.schema())?;

    // If join expression exists, parse the `on` condition expression, build join and return
    // Otherwise, build join with only the filter, without join keys
    match &join.expression.as_ref() {
        Some(expr) => {
            let on = consumer.consume_expression(expr, &in_join_schema).await?;
            // The join expression can contain both equal and non-equal ops.
            // As of datafusion 31.0.0, the equal and non equal join conditions are in separate fields.
            // So we extract each part as follows:
            // - If an Eq or IsNotDistinctFrom op is encountered, add the left column, right column and is_null_equal_nulls to `join_ons` vector
            // - Otherwise we add the expression to join_filter (use conjunction if filter already exists)
            let (join_ons, null_equality, join_filter) =
                split_eq_and_noneq_join_predicate_with_nulls_equality(&on);
            let (left_cols, right_cols): (Vec<_>, Vec<_>) =
                itertools::multiunzip(join_ons);
            left.join_detailed(
                right.build()?,
                join_type,
                (left_cols, right_cols),
                join_filter,
                null_equality,
            )?
            .build()
        }
        None => {
            let on: Vec<String> = vec![];
            left.join_detailed(
                right.build()?,
                join_type,
                (on.clone(), on),
                None,
                NullEquality::NullEqualsNothing,
            )?
            .build()
        }
    }
}

fn split_eq_and_noneq_join_predicate_with_nulls_equality(
    filter: &Expr,
) -> (Vec<(Column, Column)>, NullEquality, Option<Expr>) {
    let exprs = split_conjunction(filter);

    let mut eq_keys: Vec<(Column, Column)> = vec![];
    let mut indistinct_keys: Vec<(Column, Column)> = vec![];
    let mut accum_filters: Vec<Expr> = vec![];

    for expr in exprs {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: op @ (Operator::Eq | Operator::IsNotDistinctFrom),
                right,
            }) => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => match op {
                    Operator::Eq => eq_keys.push((l.clone(), r.clone())),
                    Operator::IsNotDistinctFrom => {
                        indistinct_keys.push((l.clone(), r.clone()))
                    }
                    _ => unreachable!(),
                },
                _ => accum_filters.push(expr.clone()),
            },
            _ => accum_filters.push(expr.clone()),
        }
    }

    let (join_keys, null_equality) =
        match (eq_keys.is_empty(), indistinct_keys.is_empty()) {
            // Mixed: use eq_keys as equijoin keys, demote indistinct keys to filter
            (false, false) => {
                for (l, r) in &indistinct_keys {
                    accum_filters.push(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(Expr::Column(l.clone())),
                        op: Operator::IsNotDistinctFrom,
                        right: Box::new(Expr::Column(r.clone())),
                    }));
                }
                (eq_keys, NullEquality::NullEqualsNothing)
            }
            // Only eq keys
            (false, true) => (eq_keys, NullEquality::NullEqualsNothing),
            // Only indistinct keys
            (true, false) => (indistinct_keys, NullEquality::NullEqualsNull),
            // No keys at all
            (true, true) => (vec![], NullEquality::NullEqualsNothing),
        };

    let join_filter = accum_filters.into_iter().reduce(Expr::and);
    (join_keys, null_equality, join_filter)
}

fn from_substrait_jointype(join_type: i32) -> datafusion::common::Result<JoinType> {
    if let Ok(substrait_join_type) = join_rel::JoinType::try_from(join_type) {
        match substrait_join_type {
            join_rel::JoinType::Inner => Ok(JoinType::Inner),
            join_rel::JoinType::Left => Ok(JoinType::Left),
            join_rel::JoinType::Right => Ok(JoinType::Right),
            join_rel::JoinType::Outer => Ok(JoinType::Full),
            join_rel::JoinType::LeftAnti => Ok(JoinType::LeftAnti),
            join_rel::JoinType::LeftSemi => Ok(JoinType::LeftSemi),
            join_rel::JoinType::LeftMark => Ok(JoinType::LeftMark),
            join_rel::JoinType::RightMark => Ok(JoinType::RightMark),
            join_rel::JoinType::RightAnti => Ok(JoinType::RightAnti),
            join_rel::JoinType::RightSemi => Ok(JoinType::RightSemi),
            _ => plan_err!("unsupported join type {substrait_join_type:?}"),
        }
    } else {
        plan_err!("invalid join type variant {join_type}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    #[test]
    fn split_only_eq_keys() {
        // equal(a, b)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::Eq,
            right: Box::new(col("b")),
        });

        let (keys, null_eq, filter) =
            split_eq_and_noneq_join_predicate_with_nulls_equality(&expr);

        assert_eq!(keys.len(), 1);
        assert_eq!(null_eq, NullEquality::NullEqualsNothing);
        assert!(filter.is_none());
    }

    #[test]
    fn split_only_indistinct_keys() {
        // is_not_distinct_from(a, b)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::IsNotDistinctFrom,
            right: Box::new(col("b")),
        });

        let (keys, null_eq, filter) =
            split_eq_and_noneq_join_predicate_with_nulls_equality(&expr);

        assert_eq!(keys.len(), 1);
        assert_eq!(null_eq, NullEquality::NullEqualsNull);
        assert!(filter.is_none());
    }

    /// Regression: mixed `equal` + `is_not_distinct_from` must demote
    /// the indistinct key to the join filter so the single NullEquality
    /// flag stays consistent (NullEqualsNothing for the eq keys).
    #[test]
    fn split_mixed_eq_and_indistinct_demotes_indistinct_to_filter() {
        // is_not_distinct_from(val_l, val_r) AND equal(id_l, id_r)
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("val_l")),
            op: Operator::IsNotDistinctFrom,
            right: Box::new(col("val_r")),
        })
        .and(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("id_l")),
            op: Operator::Eq,
            right: Box::new(col("id_r")),
        }));

        let (keys, null_eq, filter) =
            split_eq_and_noneq_join_predicate_with_nulls_equality(&expr);

        // Only the Eq key should be an equijoin key.
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].0, Column::from_name("id_l"));
        assert_eq!(keys[0].1, Column::from_name("id_r"));
        assert_eq!(null_eq, NullEquality::NullEqualsNothing);

        // The IsNotDistinctFrom predicate should be demoted to the filter.
        let filter =
            filter.expect("filter should contain the demoted indistinct predicate");
        match &filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                assert_eq!(*op, Operator::IsNotDistinctFrom);
                assert_eq!(**left, col("val_l"));
                assert_eq!(**right, col("val_r"));
            }
            other => panic!("expected BinaryExpr, got {other:?}"),
        }
    }

    #[test]
    fn split_non_column_eq_goes_to_filter() {
        // equal(literal, column) — non-column operand goes to filter
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("x".into())),
                None,
            )),
            op: Operator::Eq,
            right: Box::new(col("b")),
        });

        let (keys, _, filter) =
            split_eq_and_noneq_join_predicate_with_nulls_equality(&expr);

        assert!(keys.is_empty());
        assert!(filter.is_some());
    }
}
