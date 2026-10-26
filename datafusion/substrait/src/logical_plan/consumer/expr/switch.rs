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

use crate::logical_plan::consumer::{SubstraitConsumer, from_literal};
use datafusion::common::{DFSchema, substrait_err};
use datafusion::logical_expr::{Case, Expr};
use substrait::proto::expression::SwitchExpression;

/// Reads Substrait's `SwitchExpression` as a `CASE` over a base expression.
///
/// The two line up: a switch compares its `match` against the literal in each
/// arm, which is what `CASE <base> WHEN <value>` does, and `CaseExpr` evaluates
/// the base once rather than per arm. A `match` that is `NULL` matches no arm
/// and answers from `ELSE`, the same as `CASE`.
pub async fn from_switch(
    consumer: &impl SubstraitConsumer,
    switch: &SwitchExpression,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    let Some(base) = switch.r#match.as_ref() else {
        return substrait_err!("Switch expression without a match expression");
    };
    let expr = Box::new(consumer.consume_expression(base, input_schema).await?);

    let mut when_then_expr = Vec::with_capacity(switch.ifs.len());
    for arm in &switch.ifs {
        let (Some(value), Some(then)) = (arm.r#if.as_ref(), arm.then.as_ref()) else {
            return substrait_err!(
                "Switch expression arm needs both a value to match and a result"
            );
        };
        when_then_expr.push((
            Box::new(from_literal(consumer, value).await?),
            Box::new(consumer.consume_expression(then, input_schema).await?),
        ));
    }

    let else_expr = match switch.r#else.as_ref() {
        Some(e) => Some(Box::new(
            consumer.consume_expression(e, input_schema).await?,
        )),
        None => None,
    };

    Ok(Expr::Case(Case {
        expr: Some(expr),
        when_then_expr,
        else_expr,
    }))
}
