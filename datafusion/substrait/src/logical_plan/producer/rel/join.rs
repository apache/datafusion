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

use crate::logical_plan::producer::{SubstraitProducer, make_binary_op_scalar_func};
use datafusion::common::{
    DFSchemaRef, JoinConstraint, JoinType, NullEquality, not_impl_err,
};
use datafusion::logical_expr::{Expr, Join, Operator};
use std::sync::Arc;
use substrait::proto::rel::RelType;
use substrait::proto::{Expression, JoinRel, Rel, join_rel};

pub fn from_join(
    producer: &mut impl SubstraitProducer,
    join: &Join,
) -> datafusion::common::Result<Box<Rel>> {
    let left = producer.handle_plan(join.left.as_ref())?;
    let right = producer.handle_plan(join.right.as_ref())?;
    let join_type = to_substrait_jointype(join.join_type);
    // we only support basic joins so return an error for anything not yet supported
    match join.join_constraint {
        JoinConstraint::On => {}
        JoinConstraint::Using => return not_impl_err!("join constraint: `using`"),
    }
    let in_join_schema = Arc::new(join.left.schema().join(join.right.schema())?);

    // convert filter if present
    let join_filter = match &join.filter {
        Some(filter) => Some(producer.handle_expr(filter, &in_join_schema)?),
        None => None,
    };

    // map the left and right columns to binary expressions in the form `l = r`
    // build a single expression for the ON condition, such as `l.a = r.a AND l.b = r.b`
    let eq_op = match join.null_equality {
        NullEquality::NullEqualsNothing => Operator::Eq,
        NullEquality::NullEqualsNull => Operator::IsNotDistinctFrom,
    };
    let join_on = to_substrait_join_expr(producer, &join.on, eq_op, &in_join_schema)?;

    // create conjunction between `join_on` and `join_filter` to embed all join conditions,
    // whether equal or non-equal in a single expression
    let join_expr = match &join_on {
        Some(on_expr) => match &join_filter {
            Some(filter) => Some(Box::new(make_binary_op_scalar_func(
                producer,
                on_expr,
                filter,
                Operator::And,
            ))),
            None => join_on.map(Box::new), // the join expression will only contain `join_on` if filter doesn't exist
        },
        None => match &join_filter {
            Some(_) => join_filter.map(Box::new), // the join expression will only contain `join_filter` if the `on` condition doesn't exist
            None => None,
        },
    };

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Join(Box::new(JoinRel {
            common: None,
            left: Some(left),
            right: Some(right),
            r#type: join_type as i32,
            expression: join_expr,
            post_join_filter: None,
            advanced_extension: None,
        }))),
    }))
}

fn to_substrait_join_expr(
    producer: &mut impl SubstraitProducer,
    join_conditions: &Vec<(Expr, Expr)>,
    eq_op: Operator,
    join_schema: &DFSchemaRef,
) -> datafusion::common::Result<Option<Expression>> {
    // Only support AND conjunction for each binary expression in join conditions
    let mut exprs: Vec<Expression> = vec![];
    for (left, right) in join_conditions {
        let l = producer.handle_expr(left, join_schema)?;
        let r = producer.handle_expr(right, join_schema)?;
        // AND with existing expression
        exprs.push(make_binary_op_scalar_func(producer, &l, &r, eq_op));
    }

    let join_expr: Option<Expression> =
        exprs.into_iter().reduce(|acc: Expression, e: Expression| {
            make_binary_op_scalar_func(producer, &acc, &e, Operator::And)
        });
    Ok(join_expr)
}

fn to_substrait_jointype(join_type: JoinType) -> join_rel::JoinType {
    match join_type {
        JoinType::Inner => join_rel::JoinType::Inner,
        JoinType::Left => join_rel::JoinType::Left,
        JoinType::Right => join_rel::JoinType::Right,
        JoinType::Full => join_rel::JoinType::Outer,
        JoinType::LeftAnti => join_rel::JoinType::LeftAnti,
        JoinType::LeftSemi => join_rel::JoinType::LeftSemi,
        JoinType::LeftMark => join_rel::JoinType::LeftMark,
        JoinType::RightMark => join_rel::JoinType::RightMark,
        JoinType::RightAnti => join_rel::JoinType::RightAnti,
        JoinType::RightSemi => join_rel::JoinType::RightSemi,
    }
}
