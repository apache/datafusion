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
use datafusion::arrow::datatypes::Field;
use datafusion::common::{JoinType, NullEquality, not_impl_err, substrait_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    Cast, Expr, LogicalPlan, LogicalPlanBuilder, requalify_sides_if_needed,
};
use std::sync::Arc;
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
            SetOp::IntersectionPrimary => intersect_rel(
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
        let right = consumer.consume_rel(input).await?;
        rel = if is_all {
            LogicalPlanBuilder::intersect_all(
                rel,
                right,
                &consumer.get_function_registry().udwf("row_number")?,
            )?
        } else {
            intersect_rel(rel, right, false)?
        };
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

async fn except_rels(
    consumer: &impl SubstraitConsumer,
    rels: &[Rel],
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let mut rel = consumer.consume_rel(&rels[0]).await?;

    for input in &rels[1..] {
        let right = consumer.consume_rel(input).await?;
        rel = if is_all {
            LogicalPlanBuilder::except_all(
                rel,
                right,
                &consumer.get_function_registry().udwf("row_number")?,
            )?
        } else {
            LogicalPlanBuilder::except(rel, right, false)?
        };
    }

    Ok(rel)
}
