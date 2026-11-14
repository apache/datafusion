// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::expr::{
    contains_struct, is_literal, name_struct_literal_like,
};
use crate::logical_plan::consumer::{SubstraitConsumer, SubstraitConsumerContext};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{JoinType, NullEquality, not_impl_err, substrait_err};
use datafusion::logical_expr::{
    Aggregate, Cast, Distinct, Expr, Limit, LogicalPlan, LogicalPlanBuilder, Projection,
    Sort, SubqueryAlias, Union, requalify_sides_if_needed,
};
use std::sync::Arc;
use substrait::proto::SetRel;
use substrait::proto::set_rel::SetOp;

pub async fn from_set_rel(
    consumer: &impl SubstraitConsumer,
    set: &SetRel,
    context: &SubstraitConsumerContext,
) -> datafusion::common::Result<LogicalPlan> {
    if set.inputs.len() < 2 {
        return substrait_err!("Set operation requires at least two inputs");
    }
    let mut inputs = Vec::with_capacity(set.inputs.len());
    for input in &set.inputs {
        inputs.push(consumer.consume_rel(input, context).await?);
    }
    let mut inputs = name_struct_literal_columns(inputs)?;

    match set.op() {
        SetOp::UnionAll => union_plans(inputs, true),
        SetOp::UnionDistinct => union_plans(inputs, false),
        SetOp::IntersectionPrimary => {
            let secondary = inputs.split_off(1);
            intersect_rel(inputs.remove(0), union_plans(secondary, true)?, false)
        }
        SetOp::IntersectionMultiset => intersect_plans(inputs, false),
        SetOp::IntersectionMultisetAll => intersect_plans(inputs, true),
        SetOp::MinusPrimary => except_plans(inputs, false),
        SetOp::MinusPrimaryAll => except_plans(inputs, true),
        set_op => not_impl_err!("Unsupported set operator: {set_op:?}"),
    }
}

/// Gives a column that is a struct literal in some inputs the field names that column
/// has in the other inputs.
///
/// Substrait types carry no struct field names, so a struct literal is consumed with
/// generated names (`c0`, `c1`, ...). A query padding one input with `NULL` for a struct
/// column the other input reads from a table, as in
/// `SELECT s FROM t UNION ALL SELECT NULL FROM u`, would otherwise combine `Struct{a, b}`
/// with `Struct{c0, c1}`, which DataFusion can't cast between as it matches struct fields
/// by name.
fn name_struct_literal_columns(
    inputs: Vec<LogicalPlan>,
) -> datafusion::common::Result<Vec<LogicalPlan>> {
    let width = inputs[0].schema().fields().len();
    if inputs
        .iter()
        .any(|input| input.schema().fields().len() != width)
    {
        // let the set operation report the mismatch
        return Ok(inputs);
    }

    // for each column, the type of the first input that doesn't produce it with a literal
    let named_types = (0..width)
        .map(|column| {
            inputs
                .iter()
                .filter(|input| !projects_literal(input, column))
                .map(|input| input.schema().field(column).data_type())
                .find(|data_type| contains_struct(data_type))
                .cloned()
        })
        .collect::<Vec<_>>();
    if named_types.iter().all(Option::is_none) {
        return Ok(inputs);
    }

    inputs
        .into_iter()
        .map(|input| name_projected_struct_literals(input, &named_types))
        .collect()
}

/// Gives the struct literals in the projection producing `input`'s columns the names in
/// `named_types`, or in each input's projection for a nested set operation. The plans
/// above them (`LIMIT`, `ORDER BY`, ...) are rebuilt with the new types.
fn name_projected_struct_literals(
    input: LogicalPlan,
    named_types: &[Option<DataType>],
) -> datafusion::common::Result<LogicalPlan> {
    match input {
        LogicalPlan::Projection(projection) => {
            let exprs = projection
                .expr
                .into_iter()
                .zip(named_types)
                .map(|(mut expr, named_type)| {
                    let Some(named_type) = named_type else {
                        return Ok(expr);
                    };
                    // keep the column's name, which the plans above may refer to: an
                    // unaliased literal is named after its value, field names included
                    let name = expr.schema_name().to_string();
                    name_struct_literal_like(&mut expr, named_type);
                    expr.alias_if_changed(name)
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(LogicalPlan::Projection(Projection::try_new(
                exprs,
                projection.input,
            )?))
        }
        LogicalPlan::Union(union) => {
            let inputs = union
                .inputs
                .into_iter()
                .map(|input| {
                    name_projected_struct_literals(
                        Arc::unwrap_or_clone(input),
                        named_types,
                    )
                    .map(Arc::new)
                })
                .collect::<datafusion::common::Result<_>>()?;
            // derive the schema again, as rebuilding a union keeps its old one
            Ok(LogicalPlan::Union(Union::try_new_with_loose_types(inputs)?))
        }
        LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Distinct(Distinct::All(_))
        | LogicalPlan::SubqueryAlias(_) => {
            let child =
                name_projected_struct_literals(input.inputs()[0].clone(), named_types)?;
            input.with_new_exprs(input.expressions(), vec![child])
        }
        LogicalPlan::Aggregate(ref aggregate) if removes_duplicates(aggregate) => {
            let child =
                name_projected_struct_literals(input.inputs()[0].clone(), named_types)?;
            input.with_new_exprs(input.expressions(), vec![child])
        }
        _ => Ok(input),
    }
}

/// Returns true if `aggregate` only removes duplicate rows, keeping its input's columns:
/// it groups by each input column in order and computes nothing else. This is how a
/// `UNION` (without `ALL`) arrives from Substrait.
fn removes_duplicates(aggregate: &Aggregate) -> bool {
    let input_columns = aggregate.input.schema().columns();
    aggregate.aggr_expr.is_empty()
        && aggregate.group_expr.len() == input_columns.len()
        && aggregate
            .group_expr
            .iter()
            .zip(&input_columns)
            .all(|(expr, column)| matches!(expr, Expr::Column(c) if c == column))
}

/// Returns true if `input`'s `column` is produced by literals only: a literal in its
/// projection, or in every input of a nested set operation.
fn projects_literal(input: &LogicalPlan, column: usize) -> bool {
    match input {
        LogicalPlan::Projection(projection) => {
            projection.expr.get(column).is_some_and(is_literal)
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .all(|input| projects_literal(input, column)),
        // these pass their input's columns through unchanged
        LogicalPlan::Limit(Limit { input, .. })
        | LogicalPlan::Sort(Sort { input, .. })
        | LogicalPlan::Distinct(Distinct::All(input))
        | LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
            projects_literal(input, column)
        }
        LogicalPlan::Aggregate(aggregate) if removes_duplicates(aggregate) => {
            projects_literal(&aggregate.input, column)
        }
        _ => false,
    }
}

fn union_plans(
    inputs: Vec<LogicalPlan>,
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let mut inputs = inputs.into_iter();
    let Some(first) = inputs.next() else {
        return substrait_err!("Set operation requires at least two inputs");
    };
    let mut union_builder = Ok(LogicalPlanBuilder::from(first));
    for input in inputs {
        union_builder = if is_all {
            union_builder?.union(input)
        } else {
            union_builder?.union_distinct(input)
        };
    }
    union_builder?.build()
}

fn intersect_plans(
    inputs: Vec<LogicalPlan>,
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let mut inputs = inputs.into_iter();
    let Some(mut rel) = inputs.next() else {
        return substrait_err!("Set operation requires at least two inputs");
    };
    for input in inputs {
        rel = intersect_rel(rel, input, is_all)?;
    }
    Ok(rel)
}

/// Intersects two relations, giving the result the nullability the Substrait
/// [Set Operation rules] prescribe.
///
/// [`LogicalPlanBuilder::intersect`] compiles an intersection into a left semi
/// join, so on its own the result keeps the left input's nullability. The join
/// matches nulls with nulls, so a left row holding a null in some field only
/// survives when the right input holds a null there too. A field is therefore
/// nullable in the result only when it is nullable in *both* inputs.
///
/// Applied to each step of a chain, that gives the spec's rule for the multiset
/// intersections - a field is required when any input requires it. For
/// `INTERSECTION_PRIMARY` the right side is the union of the secondary inputs,
/// whose field is nullable exactly when some secondary input makes it nullable,
/// so the same rule yields "nullable in the primary input and in at least one
/// secondary input".
///
/// When the right input requires a field the left input leaves nullable, the
/// intersection is built as an inner join against the distinct right rows
/// instead, and that field is read from the right side. Matched rows hold equal
/// values, so the result is unchanged, and the field is non-nullable because
/// its source is: the logical and the physical planner both derive that from
/// the input schema, so the plan, the physical plan and the batches agree.
/// Joining against distinct right rows keeps each left row at most once, as the
/// semi join does.
///
/// Differing metadata does not change which path is taken. A column read from
/// the right is cast to an explicit target field carrying the left field's
/// metadata; an explicit-field cast target's metadata is used exactly as
/// given rather than merged into the source's, so this drops a key only the
/// right field carried and keeps the left field's metadata exactly. A
/// `Field`'s own metadata travels with it through any later rewrite, so this
/// holds for the plan this function returns and for any rewrite of it.
///
/// Schema-level (as opposed to per-field) metadata gets no such treatment:
/// [`build_join_schema`](datafusion::logical_expr::build_join_schema) merges
/// the two inputs' schema metadata with the left's value winning a
/// conflicting key, and that merged map is what the projection built on top
/// of the join reports - the same as the semi join
/// [`LogicalPlanBuilder::intersect`] takes for the cases this function
/// doesn't rewrite. A key only the right input's schema carries can
/// therefore still appear in the result's schema-level metadata.
///
/// [Set Operation rules]: https://substrait.io/relations/logical_relations/#set-operation
fn intersect_rel(
    left: LogicalPlan,
    right: LogicalPlan,
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let left_fields = left.schema().fields();
    let right_fields = right.schema().fields();
    // A field is read from the right side when the left leaves it nullable and
    // the right requires it. Its metadata does not matter here: it is read from
    // the right with the left field's metadata substituted for its own.
    let from_right: Vec<bool> = left_fields
        .iter()
        .zip(right_fields.iter())
        .map(|(left, right)| {
            left.is_nullable()
                && !right.is_nullable()
                && left.data_type() == right.data_type()
        })
        .collect();

    // `intersect` also reports inputs of different widths.
    if left_fields.len() != right_fields.len() || !from_right.contains(&true) {
        return LogicalPlanBuilder::intersect(left, right, is_all);
    }

    let (left, right, _) = requalify_sides_if_needed(
        LogicalPlanBuilder::from(left),
        LogicalPlanBuilder::from(right),
    )?;
    let left = if is_all { left } else { left.distinct()? };
    let right = right.distinct()?.build()?;

    let left_columns = left.schema().columns();
    let right_columns = right.schema().columns();
    let exprs = left
        .schema()
        .fields()
        .iter()
        .zip(&left_columns)
        .zip(&right_columns)
        .zip(&from_right)
        .map(|(((field, left), right), from_right)| {
            if *from_right {
                // `alias_qualified_with_metadata` would not do: `Expr::Alias`'s
                // field derivation extends the aliased expression's own
                // metadata with the alias's, so a key only the right column
                // carries would survive alongside the left field's metadata.
                // An explicit-field `Cast` target's metadata is instead used
                // exactly as given, in both the logical and the physical
                // plan, so casting to the left field's type (already proven
                // equal to the right's) and metadata drops the right's own
                // metadata outright. The qualifier and name still need
                // `alias_qualified` on top, since a `Cast`'s own field is not
                // renamed to its target field's name.
                let target_field = Arc::new(
                    Field::new(&left.name, field.data_type().clone(), false)
                        .with_metadata(field.metadata().clone()),
                );
                Expr::Cast(Cast::new_from_field(
                    Box::new(Expr::Column(right.clone())),
                    target_field,
                ))
                .alias_qualified(left.relation.clone(), &left.name)
            } else {
                Expr::Column(left.clone())
            }
        })
        .collect::<Vec<_>>();

    let joined = left
        .join_detailed(
            right,
            JoinType::Inner,
            (left_columns, right_columns),
            None,
            NullEquality::NullEqualsNull,
        )?
        .build()?;

    LogicalPlanBuilder::from(joined).project(exprs)?.build()
}

fn except_plans(
    inputs: Vec<LogicalPlan>,
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let mut inputs = inputs.into_iter();
    let Some(mut rel) = inputs.next() else {
        return substrait_err!("Set operation requires at least two inputs");
    };
    for input in inputs {
        rel = LogicalPlanBuilder::except(rel, input, is_all)?;
    }
    Ok(rel)
}
