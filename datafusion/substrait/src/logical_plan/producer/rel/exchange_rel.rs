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

use crate::logical_plan::producer::{
    SubstraitProducer, try_to_substrait_field_reference,
};
use datafusion::common::not_impl_err;
use datafusion::logical_expr::{Partitioning, Repartition};
use substrait::proto::exchange_rel::{ExchangeKind, RoundRobin, ScatterFields};
use substrait::proto::rel::RelType;
use substrait::proto::{ExchangeRel, Rel};

pub fn from_repartition(
    producer: &mut impl SubstraitProducer,
    repartition: &Repartition,
) -> datafusion::common::Result<Box<Rel>> {
    let input = producer.handle_plan(repartition.input.as_ref())?;
    let partition_count = match repartition.partitioning_scheme {
        Partitioning::RoundRobinBatch(num) => num,
        Partitioning::Hash(_, num) => num,
        Partitioning::DistributeBy(_) => {
            return not_impl_err!(
                "Physical plan does not support DistributeBy partitioning"
            );
        }
    };
    // ref: https://substrait.io/relations/physical_relations/#exchange-types
    let exchange_kind = match &repartition.partitioning_scheme {
        Partitioning::RoundRobinBatch(_) => {
            ExchangeKind::RoundRobin(RoundRobin::default())
        }
        Partitioning::Hash(exprs, _) => {
            let fields = exprs
                .iter()
                .map(|e| try_to_substrait_field_reference(e, repartition.input.schema()))
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            ExchangeKind::ScatterByFields(ScatterFields { fields })
        }
        Partitioning::DistributeBy(_) => {
            return not_impl_err!(
                "Physical plan does not support DistributeBy partitioning"
            );
        }
    };
    let exchange_rel = ExchangeRel {
        common: None,
        input: Some(input),
        exchange_kind: Some(exchange_kind),
        advanced_extension: None,
        partition_count: partition_count as i32,
        targets: vec![],
    };
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Exchange(Box::new(exchange_rel))),
    }))
}
