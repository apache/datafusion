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

use crate::logical_plan::grouping_set::{
    GROUPING_SET_INDEX, grouping_id_column, grouping_id_from_index, grouping_set_columns,
    grouping_set_ids,
};
use crate::logical_plan::producer::{
    SubstraitProducer, from_aggregate_function, substrait_field_ref,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Column, DFSchema, DFSchemaRef, internal_err, not_impl_err};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::utils::powerset;
use datafusion::logical_expr::{Aggregate, Distinct, Expr, GroupingSet};
use std::sync::Arc;
use substrait::proto::aggregate_rel::{Grouping, Measure};
use substrait::proto::rel::RelType;
use substrait::proto::rel_common::EmitKind;
use substrait::proto::{
    AggregateRel, Expression, ProjectRel, Rel, RelCommon, rel_common,
};

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
    let is_grouping_set = groupings.len() > 1;
    let grouping_count = grouping_expressions.len();
    let measure_count = measures.len();
    let aggregate = Box::new(Rel {
        rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
            common: None,
            input: Some(input),
            grouping_expressions,
            groupings,
            measures,
            advanced_extension: None,
        }))),
    });

    if is_grouping_set {
        grouping_set_projection(producer, agg, aggregate, grouping_count, measure_count)
    } else {
        Ok(aggregate)
    }
}

/// Puts a multi-set aggregate back into DataFusion's
/// `[groups, grouping_id, measures]` schema.
///
/// Substrait's own output is `[groups, measures, grouping set index]`, so
/// besides the reordering the trailing column has to be mapped back to
/// `__grouping_id`; see [`crate::logical_plan::grouping_set`]. That map is a
/// projected expression, which leaves the `AggregateRel` itself holding the
/// index the spec defines, for a consumer that reads it.
fn grouping_set_projection(
    producer: &mut impl SubstraitProducer,
    agg: &Aggregate,
    aggregate: Box<Rel>,
    grouping_count: usize,
    measure_count: usize,
) -> datafusion::common::Result<Box<Rel>> {
    let schema = agg.schema.as_ref();
    let (grouping_id_index, _) = grouping_id_column(schema)?;
    if grouping_id_index != grouping_count {
        return internal_err!(
            "Aggregate has {grouping_id_index} grouping columns but {grouping_count} grouping expressions were written"
        );
    }
    let grouping_id_type = schema.field(grouping_id_index).data_type();
    let ids = grouping_set_ids(
        &grouping_set_columns(&agg.group_expr)?,
        &expand_grouping_sets(&agg.group_expr)?,
    )?;

    // The aggregate's output as Substrait orders it, which is what the
    // expression below is written against.
    let index_field = Field::new(GROUPING_SET_INDEX, DataType::Int32, false);
    let substrait_output = DFSchema::from_unqualified_fields(
        (0..grouping_id_index)
            .chain(grouping_id_index + 1..schema.fields().len())
            .map(|index| Arc::clone(schema.field(index)))
            .chain(std::iter::once(Arc::new(index_field)))
            .collect(),
        schema.metadata().clone(),
    )?;
    let index = Expr::Column(Column::from_name(GROUPING_SET_INDEX));
    let expression = producer.handle_expr(
        &grouping_id_from_index(&index, grouping_id_type, &ids)?,
        &Arc::new(substrait_output),
    )?;

    // A Substrait project emits its input's fields followed by its
    // expressions, so the map sits one past the aggregate's own output.
    let index_of_map = grouping_count + measure_count + 1;
    let output_mapping = (0..grouping_count)
        .chain(std::iter::once(index_of_map))
        .chain(grouping_count..grouping_count + measure_count)
        .map(|index| index as i32)
        .collect();

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Project(Box::new(ProjectRel {
            common: Some(RelCommon {
                emit_kind: Some(EmitKind::Emit(rel_common::Emit { output_mapping })),
                hint: None,
                advanced_extension: None,
            }),
            input: Some(aggregate),
            expressions: vec![expression],
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

            #[expect(deprecated)]
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
    let groupings = expand_grouping_sets(exprs)?
        .iter()
        .map(|set| parse_flat_grouping_exprs(producer, set, schema, &mut ref_group_exprs))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    Ok((ref_group_exprs, groupings))
}

/// The grouping sets an aggregate is written as, in the order they are emitted.
///
/// Substrait has no `ROLLUP` or `CUBE`, so both become a list of sets, and the
/// grouping set index of a row follows this order.
fn expand_grouping_sets(exprs: &[Expr]) -> datafusion::common::Result<Vec<Vec<Expr>>> {
    let sets = match exprs {
        [Expr::GroupingSet(gs)] => match gs {
            // Generate power set of grouping expressions
            GroupingSet::Cube(set) => powerset(set)?
                .into_iter()
                .map(|set| set.into_iter().cloned().collect())
                .collect(),
            GroupingSet::GroupingSets(sets) => sets.clone(),
            GroupingSet::Rollup(set) => {
                let mut sets: Vec<Vec<Expr>> = vec![vec![]];
                for i in 0..set.len() {
                    sets.push(set[..=i].to_vec());
                }
                sets.into_iter().rev().collect()
            }
        },
        exprs => vec![exprs.to_vec()],
    };
    Ok(sets)
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
        let reference = ref_group_exprs.iter().position(|existing| existing == &rex);
        let reference = reference.unwrap_or_else(|| {
            ref_group_exprs.push(rex);
            ref_group_exprs.len() - 1
        });
        expression_references.push(reference as u32);
    }
    #[expect(deprecated)]
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
        Expr::AggregateFunction(agg_fn) => {
            from_aggregate_function(producer, agg_fn, schema)
        }
        Expr::Alias(Alias { expr, .. }) => {
            to_substrait_agg_measure(producer, expr, schema)
        }
        _ => internal_err!(
            "Expression must be compatible with aggregation. Unsupported expression: {:?}. Expressiontype: {}",
            expr,
            expr.variant_name()
        ),
    }
}
