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
            let mut partition_columns = vec![];
            let input_schema = input.schema();
            for field_ref in &scatter_fields.fields {
                let column = from_substrait_field_reference(field_ref, input_schema)?;
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
