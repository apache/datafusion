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
use datafusion::common::{not_impl_err, substrait_err};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use substrait::proto::set_rel::SetOp;
use substrait::proto::{Rel, SetRel};

pub async fn from_set_rel(
    consumer: &impl SubstraitConsumer,
    set: &SetRel,
) -> datafusion::common::Result<LogicalPlan> {
    if set.inputs.len() < 2 {
        substrait_err!("Set operation requires at least two inputs")
    } else {
        match set.op() {
            SetOp::UnionAll => union_rels(consumer, &set.inputs, true).await,
            SetOp::UnionDistinct => union_rels(consumer, &set.inputs, false).await,
            SetOp::IntersectionPrimary => LogicalPlanBuilder::intersect(
                consumer.consume_rel(&set.inputs[0]).await?,
                union_rels(consumer, &set.inputs[1..], true).await?,
                false,
            ),
            SetOp::IntersectionMultiset => {
                intersect_rels(consumer, &set.inputs, false).await
            }
            SetOp::IntersectionMultisetAll => {
                intersect_rels(consumer, &set.inputs, true).await
            }
            SetOp::MinusPrimary => except_rels(consumer, &set.inputs, false).await,
            SetOp::MinusPrimaryAll => except_rels(consumer, &set.inputs, true).await,
            set_op => not_impl_err!("Unsupported set operator: {set_op:?}"),
        }
    }
}

async fn union_rels(
    consumer: &impl SubstraitConsumer,
    rels: &[Rel],
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let mut union_builder = Ok(LogicalPlanBuilder::from(
        consumer.consume_rel(&rels[0]).await?,
    ));
    for input in &rels[1..] {
        let rel_plan = consumer.consume_rel(input).await?;

        union_builder = if is_all {
            union_builder?.union(rel_plan)
        } else {
            union_builder?.union_distinct(rel_plan)
        };
    }
    union_builder?.build()
}

async fn intersect_rels(
    consumer: &impl SubstraitConsumer,
    rels: &[Rel],
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let mut rel = consumer.consume_rel(&rels[0]).await?;

    for input in &rels[1..] {
        rel = LogicalPlanBuilder::intersect(
            rel,
            consumer.consume_rel(input).await?,
            is_all,
        )?;
    }

    Ok(rel)
}

async fn except_rels(
    consumer: &impl SubstraitConsumer,
    rels: &[Rel],
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let mut rel = consumer.consume_rel(&rels[0]).await?;

    for input in &rels[1..] {
        rel =
            LogicalPlanBuilder::except(rel, consumer.consume_rel(input).await?, is_all)?;
    }

    Ok(rel)
}
