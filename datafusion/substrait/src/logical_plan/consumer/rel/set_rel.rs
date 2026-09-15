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
use datafusion::common::{DFSchema, not_impl_err, substrait_err};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Projection};
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
/// [Set Operation rules]: https://substrait.io/relations/logical_relations/#set-operation
fn intersect_rel(
    left: LogicalPlan,
    right: LogicalPlan,
    is_all: bool,
) -> datafusion::common::Result<LogicalPlan> {
    let right_nullability: Vec<bool> = right
        .schema()
        .fields()
        .iter()
        .map(|field| field.is_nullable())
        .collect();

    let plan = LogicalPlanBuilder::intersect(left, right, is_all)?;

    // `intersect` has already checked that both sides have the same width.
    let narrowed: Vec<bool> = plan
        .schema()
        .fields()
        .iter()
        .zip(&right_nullability)
        .map(|(field, right_nullable)| field.is_nullable() && !right_nullable)
        .collect();

    if !narrowed.contains(&true) {
        return Ok(plan);
    }

    let qualified_fields = plan
        .schema()
        .iter()
        .zip(&narrowed)
        .map(|((qualifier, field), narrow)| {
            let field = if *narrow {
                Arc::new(field.as_ref().clone().with_nullable(false))
            } else {
                Arc::clone(field)
            };
            (qualifier.cloned(), field)
        })
        .collect();
    let schema = Arc::new(DFSchema::new_with_metadata(
        qualified_fields,
        plan.schema().metadata().clone(),
    )?);

    let exprs = plan
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        exprs,
        Arc::new(plan),
        schema,
    )?))
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
