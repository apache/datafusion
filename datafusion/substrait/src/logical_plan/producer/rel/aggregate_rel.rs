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
    from_aggregate_function, substrait_field_ref, SubstraitProducer,
};
use datafusion::common::{internal_err, not_impl_err, DFSchemaRef, DataFusionError};
use datafusion::logical_expr::{Aggregate, Distinct, Expr, GroupingSet};
use substrait::proto::aggregate_rel::{Grouping, Measure};
use substrait::proto::rel::RelType;
use substrait::proto::{AggregateRel, Expression, Rel};

pub fn from_aggregate(
    producer: &mut impl SubstraitProducer,
    agg: &Aggregate,
) -> datafusion::common::Result<Box<Rel>> {
    let input = producer.handle_plan(agg.input.as_ref())?;
    let (grouping_expressions, groupings) =
        to_substrait_groupings(producer, &agg.group_expr, agg.input.schema())?;
    let measures = agg
        .aggr_expr
        .iter()
        .map(|e| to_substrait_agg_measure(producer, e, agg.input.schema()))
        .collect::<datafusion::common::Result<Vec<_>>>()?;

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
            common: None,
            input: Some(input),
            grouping_expressions,
            groupings,
            measures,
            advanced_extension: None,
        }))),
    }))
}

pub fn from_distinct(
    producer: &mut impl SubstraitProducer,
    distinct: &Distinct,
) -> datafusion::common::Result<Box<Rel>> {
    match distinct {
        Distinct::All(plan) => {
            // Use Substrait's AggregateRel with empty measures to represent `select distinct`
            let input = producer.handle_plan(plan.as_ref())?;
            // Get grouping keys from the input relation's number of output fields
            let grouping = (0..plan.schema().fields().len())
                .map(substrait_field_ref)
                .collect::<datafusion::common::Result<Vec<_>>>()?;

            #[allow(deprecated)]
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                    common: None,
                    input: Some(input),
                    grouping_expressions: vec![],
                    groupings: vec![Grouping {
                        grouping_expressions: grouping,
                        expression_references: vec![],
                    }],
                    measures: vec![],
                    advanced_extension: None,
                }))),
            }))
        }
        Distinct::On(_) => not_impl_err!("Cannot convert Distinct::On"),
    }
}

pub fn to_substrait_groupings(
    producer: &mut impl SubstraitProducer,
    exprs: &[Expr],
    schema: &DFSchemaRef,
) -> datafusion::common::Result<(Vec<Expression>, Vec<Grouping>)> {
    let mut ref_group_exprs = vec![];
    let groupings = match exprs.len() {
        1 => match &exprs[0] {
            Expr::GroupingSet(gs) => match gs {
                GroupingSet::Cube(_) => Err(DataFusionError::NotImplemented(
                    "GroupingSet CUBE is not yet supported".to_string(),
                )),
                GroupingSet::GroupingSets(sets) => Ok(sets
                    .iter()
                    .map(|set| {
                        parse_flat_grouping_exprs(
                            producer,
                            set,
                            schema,
                            &mut ref_group_exprs,
                        )
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()?),
                GroupingSet::Rollup(set) => {
                    let mut sets: Vec<Vec<Expr>> = vec![vec![]];
                    for i in 0..set.len() {
                        sets.push(set[..=i].to_vec());
                    }
                    Ok(sets
                        .iter()
                        .rev()
                        .map(|set| {
                            parse_flat_grouping_exprs(
                                producer,
                                set,
                                schema,
                                &mut ref_group_exprs,
                            )
                        })
                        .collect::<datafusion::common::Result<Vec<_>>>()?)
                }
            },
            _ => Ok(vec![parse_flat_grouping_exprs(
                producer,
                exprs,
                schema,
                &mut ref_group_exprs,
            )?]),
        },
        _ => Ok(vec![parse_flat_grouping_exprs(
            producer,
            exprs,
            schema,
            &mut ref_group_exprs,
        )?]),
    }?;
    Ok((ref_group_exprs, groupings))
}

pub fn parse_flat_grouping_exprs(
    producer: &mut impl SubstraitProducer,
    exprs: &[Expr],
    schema: &DFSchemaRef,
    ref_group_exprs: &mut Vec<Expression>,
) -> datafusion::common::Result<Grouping> {
    let mut expression_references = vec![];
    let mut grouping_expressions = vec![];

    for e in exprs {
        let rex = producer.handle_expr(e, schema)?;
        grouping_expressions.push(rex.clone());
        ref_group_exprs.push(rex);
        expression_references.push((ref_group_exprs.len() - 1) as u32);
    }
    #[allow(deprecated)]
    Ok(Grouping {
        grouping_expressions,
        expression_references,
    })
}

pub fn to_substrait_agg_measure(
    producer: &mut impl SubstraitProducer,
    expr: &Expr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Measure> {
    match expr {
        Expr::AggregateFunction(agg_fn) => from_aggregate_function(producer, agg_fn, schema),
        Expr::Alias(boxed_alias) => {
            to_substrait_agg_measure(producer, boxed_alias.expr.as_ref(), schema)
        }
        _ => internal_err!(
            "Expression must be compatible with aggregation. Unsupported expression: {:?}. ExpressionType: {:?}",
            expr,
            expr.variant_name()
        ),
    }
}
