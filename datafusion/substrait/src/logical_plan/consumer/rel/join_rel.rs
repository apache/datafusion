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
use datafusion::common::{not_impl_err, plan_err, Column, JoinType, NullEquality};
use datafusion::logical_expr::requalify_sides_if_needed;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator,
};

use substrait::proto::{join_rel, JoinRel};

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
            let (join_ons, nulls_equal_nulls, join_filter) =
                split_eq_and_noneq_join_predicate_with_nulls_equality(&on);
            let (left_cols, right_cols): (Vec<_>, Vec<_>) =
                itertools::multiunzip(join_ons);
            let null_equality = if nulls_equal_nulls {
                NullEquality::NullEqualsNull
            } else {
                NullEquality::NullEqualsNothing
            };
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
) -> (Vec<(Column, Column)>, bool, Option<Expr>) {
    let exprs = split_conjunction(filter);

    let mut accum_join_keys: Vec<(Column, Column)> = vec![];
    let mut accum_filters: Vec<Expr> = vec![];
    let mut nulls_equal_nulls = false;

    for expr in exprs {
        #[allow(clippy::collapsible_match)]
        match expr {
            Expr::BinaryExpr(binary_expr) => match binary_expr {
                x @ (BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }
                | BinaryExpr {
                    left,
                    op: Operator::IsNotDistinctFrom,
                    right,
                }) => {
                    nulls_equal_nulls = match x.op {
                        Operator::Eq => false,
                        Operator::IsNotDistinctFrom => true,
                        _ => unreachable!(),
                    };

                    match (left.as_ref(), right.as_ref()) {
                        (Expr::Column(l), Expr::Column(r)) => {
                            accum_join_keys.push((l.clone(), r.clone()));
                        }
                        _ => accum_filters.push(expr.clone()),
                    }
                }
                _ => accum_filters.push(expr.clone()),
            },
            _ => accum_filters.push(expr.clone()),
        }
    }

    let join_filter = accum_filters.into_iter().reduce(Expr::and);
    (accum_join_keys, nulls_equal_nulls, join_filter)
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
            _ => plan_err!("unsupported join type {substrait_join_type:?}"),
        }
    } else {
        plan_err!("invalid join type variant {join_type:?}")
    }
}
