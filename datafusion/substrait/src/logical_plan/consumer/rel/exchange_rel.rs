// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::SubstraitConsumer;
use crate::logical_plan::consumer::from_substrait_field_reference;
use datafusion::common::{not_impl_err, substrait_err};
use datafusion::logical_expr::{LogicalPlan, Partitioning, Repartition};
use std::sync::Arc;
use substrait::proto::ExchangeRel;
use substrait::proto::exchange_rel::ExchangeKind;

pub async fn from_exchange_rel(
    consumer: &impl SubstraitConsumer,
    exchange: &ExchangeRel,
) -> datafusion::common::Result<LogicalPlan> {
    let Some(input) = exchange.input.as_ref() else {
        return substrait_err!("Unexpected empty input in ExchangeRel");
    };
    let input = Arc::new(consumer.consume_rel(input).await?);

    let Some(exchange_kind) = &exchange.exchange_kind else {
        return substrait_err!("Unexpected empty input in ExchangeRel");
    };

    // ref: https://substrait.io/relations/physical_relations/#exchange-types
    let partitioning_scheme = match exchange_kind {
        ExchangeKind::ScatterByFields(scatter_fields) => {
            let mut partition_columns = Vec::with_capacity(scatter_fields.fields.len());
            let input_schema = input.schema();
            for field_ref in &scatter_fields.fields {
                let column =
                    from_substrait_field_reference(consumer, field_ref, input_schema)?;
                partition_columns.push(column);
            }
            Partitioning::Hash(partition_columns, exchange.partition_count as usize)
        }
        ExchangeKind::RoundRobin(_) => {
            Partitioning::RoundRobinBatch(exchange.partition_count as usize)
        }
        ExchangeKind::SingleTarget(_)
        | ExchangeKind::MultiTarget(_)
        | ExchangeKind::Broadcast(_) => {
            return not_impl_err!("Unsupported exchange kind: {exchange_kind:?}");
        }
    };
    Ok(LogicalPlan::Repartition(Repartition {
        input,
        partitioning_scheme,
    }))
}
