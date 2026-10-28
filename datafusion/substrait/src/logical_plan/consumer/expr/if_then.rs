// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{Case, Expr};
use substrait::proto::expression::IfThen;

pub async fn from_if_then(
    consumer: &impl SubstraitConsumer,
    if_then: &IfThen,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    // Parse `ifs`
    // If the first element does not have a `then` part, then we can assume it's a base expression
    let mut when_then_expr: Vec<(Box<Expr>, Box<Expr>)> =
        Vec::with_capacity(if_then.ifs.len());
    let mut expr = None;
    for (i, if_expr) in if_then.ifs.iter().enumerate() {
        if i == 0 {
            // Check if the first element is type base expression
            if if_expr.then.is_none() {
                expr = Some(Box::new(
                    consumer
                        .consume_expression(if_expr.r#if.as_ref().unwrap(), input_schema)
                        .await?,
                ));
                continue;
            }
        }
        when_then_expr.push((
            Box::new(
                consumer
                    .consume_expression(if_expr.r#if.as_ref().unwrap(), input_schema)
                    .await?,
            ),
            Box::new(
                consumer
                    .consume_expression(if_expr.then.as_ref().unwrap(), input_schema)
                    .await?,
            ),
        ));
    }
    // Parse `else`
    let else_expr = match &if_then.r#else {
        Some(e) => Some(Box::new(
            consumer.consume_expression(e, input_schema).await?,
        )),
        None => None,
    };
    Ok(Expr::Case(Case {
        expr,
        when_then_expr,
        else_expr,
    }))
}
