// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::{SubstraitConsumer, from_substrait_rex_vec};
use datafusion::common::DFSchema;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::InList;
use substrait::proto::expression::SingularOrList;

pub async fn from_singular_or_list(
    consumer: &impl SubstraitConsumer,
    expr: &SingularOrList,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    let substrait_expr = expr.value.as_ref().unwrap();
    let substrait_list = expr.options.as_ref();
    Ok(Expr::InList(InList {
        expr: Box::new(
            consumer
                .consume_expression(substrait_expr, input_schema)
                .await?,
        ),
        list: from_substrait_rex_vec(consumer, substrait_list, input_schema).await?,
        negated: false,
    }))
}
