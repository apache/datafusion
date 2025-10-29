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

use crate::logical_plan::consumer::{from_substrait_agg_func, from_substrait_sorts};
use crate::logical_plan::consumer::{NameTracker, SubstraitConsumer};
use datafusion::common::{not_impl_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, GroupingSet, LogicalPlan, LogicalPlanBuilder};
use substrait::proto::aggregate_function::AggregationInvocation;
use substrait::proto::aggregate_rel::Grouping;
use substrait::proto::AggregateRel;

pub async fn from_aggregate_rel(
    consumer: &impl SubstraitConsumer,
    agg: &AggregateRel,
) -> datafusion::common::Result<LogicalPlan> {
    if let Some(input) = agg.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        let mut ref_group_exprs = vec![];

        for e in &agg.grouping_expressions {
            let x = consumer.consume_expression(e, input.schema()).await?;
            ref_group_exprs.push(x);
        }

        let mut group_exprs = vec![];
        let mut aggr_exprs = vec![];

        match agg.groupings.len() {
            1 => {
                group_exprs.extend_from_slice(
                    &from_substrait_grouping(
                        consumer,
                        &agg.groupings[0],
                        &ref_group_exprs,
                        input.schema(),
                    )
                    .await?,
                );
            }
            _ => {
                let mut grouping_sets = vec![];
                for grouping in &agg.groupings {
                    let grouping_set = from_substrait_grouping(
                        consumer,
                        grouping,
                        &ref_group_exprs,
                        input.schema(),
                    )
                    .await?;
                    grouping_sets.push(grouping_set);
                }
                // Single-element grouping expression of type Expr::GroupingSet.
                // Note that GroupingSet::Rollup would become GroupingSet::GroupingSets, when
                // parsed by the producer and consumer, since Substrait does not have a type dedicated
                // to ROLLUP. Only vector of Groupings (grouping sets) is available.
                group_exprs
                    .push(Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)));
            }
        };

        for m in &agg.measures {
            let filter = match &m.filter {
                Some(fil) => Some(Box::new(
                    consumer.consume_expression(fil, input.schema()).await?,
                )),
                None => None,
            };
            let agg_func = match &m.measure {
                Some(f) => {
                    let distinct = match f.invocation {
                        _ if f.invocation == AggregationInvocation::Distinct as i32 => {
                            true
                        }
                        _ if f.invocation == AggregationInvocation::All as i32 => false,
                        _ => false,
                    };
                    let order_by =
                        from_substrait_sorts(consumer, &f.sorts, input.schema()).await?;

                    from_substrait_agg_func(
                        consumer,
                        f,
                        input.schema(),
                        filter,
                        order_by,
                        distinct,
                    )
                    .await
                }
                None => {
                    not_impl_err!("Aggregate without aggregate function is not supported")
                }
            };
            aggr_exprs.push(agg_func?.as_ref().clone());
        }

        // Ensure that all expressions have a unique name
        let mut name_tracker = NameTracker::new();
        let group_exprs = group_exprs
            .iter()
            .map(|e| name_tracker.get_uniquely_named_expr(e.clone()))
            .collect::<Result<Vec<Expr>, _>>()?;

        input.aggregate(group_exprs, aggr_exprs)?.build()
    } else {
        not_impl_err!("Aggregate without an input is not valid")
    }
}

#[allow(deprecated)]
async fn from_substrait_grouping(
    consumer: &impl SubstraitConsumer,
    grouping: &Grouping,
    expressions: &[Expr],
    input_schema: &DFSchemaRef,
) -> datafusion::common::Result<Vec<Expr>> {
    let mut group_exprs = vec![];
    if !grouping.grouping_expressions.is_empty() {
        for e in &grouping.grouping_expressions {
            let expr = consumer.consume_expression(e, input_schema).await?;
            group_exprs.push(expr);
        }
        return Ok(group_exprs);
    }
    for idx in &grouping.expression_references {
        let e = &expressions[*idx as usize];
        group_exprs.push(e.clone());
    }
    Ok(group_exprs)
}
