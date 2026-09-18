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
use datafusion::common::{JoinType, NullEquality, not_impl_err, substrait_err};
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, requalify_sides_if_needed,
};
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
        rel = intersect_rel(rel, consumer.consume_rel(input).await?, is_all)?;
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
/// [Set Operation rules]: https://substrait.io/relations/logical_relations/#set-operation
fn intersect_rel(
    left: LogicalPlan,
    right: LogicalPlan,
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let left_fields = left.schema().fields();
    let right_fields = right.schema().fields();
    // Only a field that differs from its right counterpart in nullability alone
    // is read from the right side, so every other attribute stays the left's.
    let from_right: Vec<bool> = left_fields
        .iter()
        .zip(right_fields.iter())
        .map(|(left, right)| {
            left.is_nullable()
                && !right.is_nullable()
                && left.data_type() == right.data_type()
                && left.metadata() == right.metadata()
        })
        .collect();

    // `intersect` also reports inputs of different widths. The join would merge
    // the right input's schema metadata into the result, so that must match too.
    if left_fields.len() != right_fields.len()
        || left.schema().metadata() != right.schema().metadata()
        || !from_right.contains(&true)
    {
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
    let exprs = left_columns
        .iter()
        .zip(&right_columns)
        .zip(&from_right)
        .map(|((left, right), from_right)| {
            if *from_right {
                Expr::Column(right.clone())
                    .alias_qualified(left.relation.clone(), &left.name)
            } else {
                Expr::Column(left.clone())
            }
        })
        .collect::<Vec<_>>();

    left.join_detailed(
        right,
        JoinType::Inner,
        (left_columns, right_columns),
        None,
        NullEquality::NullEqualsNull,
    )?
    .project(exprs)?
    .build()
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
