// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::SubstraitConsumer;
use async_recursion::async_recursion;
use datafusion::common::{DFSchema, DFSchemaRef, not_impl_err};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use substrait::proto::FetchRel;

#[async_recursion]
pub async fn from_fetch_rel(
    consumer: &impl SubstraitConsumer,
    fetch: &FetchRel,
) -> datafusion::common::Result<LogicalPlan> {
    if let Some(input) = fetch.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        let empty_schema = DFSchemaRef::new(DFSchema::empty());
        // Unset offset is treated as 0 and unset count signals that ALL records
        // should be returned, so an absent expression maps to None either way.
        let offset = match &fetch.offset_expr {
            Some(expr) => Some(consumer.consume_expression(expr, &empty_schema).await?),
            None => None,
        };
        let count = match &fetch.count_expr {
            Some(expr) => Some(consumer.consume_expression(expr, &empty_schema).await?),
            None => None,
        };
        input.limit_by_expr(offset, count)?.build()
    } else {
        not_impl_err!("Fetch without an input is not valid")
    }
}
