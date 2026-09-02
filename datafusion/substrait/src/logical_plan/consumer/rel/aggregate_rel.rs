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

use crate::logical_plan::consumer::{NameTracker, SubstraitConsumer};
use crate::logical_plan::consumer::{from_substrait_agg_func, from_substrait_sorts};
use datafusion::common::{not_impl_err, substrait_datafusion_err};
use datafusion::logical_expr::{Expr, GroupingSet, LogicalPlan, LogicalPlanBuilder};
use substrait::proto::AggregateRel;
use substrait::proto::aggregate_function::AggregationInvocation;
use substrait::proto::aggregate_rel::Grouping;

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
            0 => {}
            1 => {
                group_exprs.extend_from_slice(&from_substrait_grouping(
                    &agg.groupings[0],
                    &ref_group_exprs,
                )?);
            }
            _ => {
                let mut grouping_sets = vec![];
                for grouping in &agg.groupings {
                    let grouping_set =
                        from_substrait_grouping(grouping, &ref_group_exprs)?;
                    grouping_sets.push(grouping_set);
                }
                // Single-element grouping expression of type Expr::GroupingSet.
                // Note that GroupingSet::Rollup would become GroupingSet::GroupingSets, when
                // parsed by the producer and consumer, since Substrait does not have a type dedicated
                // to ROLLUP. Only vector of Groupings (grouping sets) is available.
                group_exprs
                    .push(Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)));
            }
        }

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
            aggr_exprs.push(std::sync::Arc::unwrap_or_clone(agg_func?));
        }

        // Ensure that all expressions have a unique name. Both grouping and
        // aggregate expressions become fields in the aggregate's output schema,
        // so they share a single namespace.
        let mut name_tracker = NameTracker::new();
        let group_exprs = group_exprs
            .into_iter()
            .map(|e| name_tracker.get_uniquely_named_expr(e))
            .collect::<Result<Vec<Expr>, _>>()?;
        let aggr_exprs = aggr_exprs
            .into_iter()
            .map(|e| name_tracker.get_uniquely_named_expr(e))
            .collect::<Result<Vec<Expr>, _>>()?;

        input.aggregate(group_exprs, aggr_exprs)?.build()
    } else {
        not_impl_err!("Aggregate without an input is not valid")
    }
}

/// A grouping set names the expressions it groups by index into the
/// relation-level `grouping_expressions`.
fn from_substrait_grouping(
    grouping: &Grouping,
    expressions: &[Expr],
) -> datafusion::common::Result<Vec<Expr>> {
    grouping
        .expression_references
        .iter()
        .map(|idx| {
            expressions.get(*idx as usize).cloned().ok_or_else(|| {
                substrait_datafusion_err!(
                    "Grouping references expression {idx} but the aggregate declares {}",
                    expressions.len()
                )
            })
        })
        .collect()
}
