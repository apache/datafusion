// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::SubstraitConsumer;
use async_recursion::async_recursion;
use datafusion::common::not_impl_err;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use substrait::proto::FilterRel;

#[async_recursion]
pub async fn from_filter_rel(
    consumer: &impl SubstraitConsumer,
    filter: &FilterRel,
) -> datafusion::common::Result<LogicalPlan> {
    if let Some(input) = filter.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        if let Some(condition) = filter.condition.as_ref() {
            let expr = consumer
                .consume_expression(condition, input.schema())
                .await?;
            input.filter(expr)?.build()
        } else {
            not_impl_err!("Filter without an condition is not valid")
        }
    } else {
        not_impl_err!("Filter without an input is not valid")
    }
}
