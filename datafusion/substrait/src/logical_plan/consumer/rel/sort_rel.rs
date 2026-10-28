// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::{SubstraitConsumer, from_substrait_sorts};
use datafusion::common::not_impl_err;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use substrait::proto::SortRel;

pub async fn from_sort_rel(
    consumer: &impl SubstraitConsumer,
    sort: &SortRel,
) -> datafusion::common::Result<LogicalPlan> {
    if let Some(input) = sort.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        let sorts = from_substrait_sorts(consumer, &sort.sorts, input.schema()).await?;
        input.sort(sorts)?.build()
    } else {
        not_impl_err!("Sort without an input is not valid")
    }
}
