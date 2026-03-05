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

use super::utils::{alias_expressions, make_renamed_schema, rename_expressions};
use super::{DefaultSubstraitConsumer, SubstraitConsumer};
use crate::extensions::Extensions;
use datafusion::common::{DFSchema, not_impl_err, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Aggregate, LogicalPlan, Projection, col};
use std::sync::Arc;
use substrait::proto::{Plan, plan_rel};

/// Convert Substrait Plan to DataFusion LogicalPlan
pub async fn from_substrait_plan(
    state: &SessionState,
    plan: &Plan,
) -> datafusion::common::Result<LogicalPlan> {
    // Register function extension
    let extensions = Extensions::try_from(&plan.extensions)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported");
    }

    let consumer = DefaultSubstraitConsumer::new(&extensions, state);
    from_substrait_plan_with_consumer(&consumer, plan).await
}

/// Convert Substrait Plan to DataFusion LogicalPlan using the given consumer
pub async fn from_substrait_plan_with_consumer(
    consumer: &impl SubstraitConsumer,
    plan: &Plan,
) -> datafusion::common::Result<LogicalPlan> {
    match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => Ok(consumer.consume_rel(rel).await?),
                    plan_rel::RelType::Root(root) => {
                        let plan =
                            consumer.consume_rel(root.input.as_ref().unwrap()).await?;
                        if root.names.is_empty() {
                            return Ok(plan);
                        }
                        let renamed_schema =
                            make_renamed_schema(plan.schema(), &root.names)?;
                        if renamed_schema
                            .has_equivalent_names_and_types(plan.schema())
                            .is_ok()
                        {
                            return Ok(plan);
                        }
                        apply_renames(plan, &renamed_schema)
                    }
                },
                None => plan_err!("Cannot parse plan relation: None"),
            }
        }
        _ => not_impl_err!(
            "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
            plan.relations.len()
        ),
    }
}

/// Apply the root-level schema renames to the given plan.
///
/// The strategy depends on the plan type:
/// - **Projection**: renames (aliases + casts) are baked directly into the
///   projection expressions.
/// - **Aggregate**: only safe aliases are applied to the aggregate's
///   expressions. If struct-field casts are also needed, a wrapping Projection
///   is added on top (the physical planner rejects Cast-wrapped aggregates).
/// - **Other nodes**: a new Projection is added on top to carry the renames.
fn apply_renames(
    plan: LogicalPlan,
    renamed_schema: &DFSchema,
) -> datafusion::common::Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(p) => {
            Ok(LogicalPlan::Projection(Projection::try_new(
                rename_expressions(
                    p.expr,
                    p.input.schema(),
                    renamed_schema.fields(),
                )?,
                p.input,
            )?))
        }
        LogicalPlan::Aggregate(a) => {
            let (group_fields, expr_fields) =
                renamed_schema.fields().split_at(a.group_expr.len());
            let agg = LogicalPlan::Aggregate(Aggregate::try_new(
                a.input,
                alias_expressions(a.group_expr, group_fields)?,
                alias_expressions(a.aggr_expr, expr_fields)?,
            )?);
            // If aliasing alone didn't satisfy the target schema
            // (e.g. nested struct field renames require casts), wrap
            // in a Projection that can safely carry those casts.
            if renamed_schema
                .has_equivalent_names_and_types(agg.schema())
                .is_ok()
            {
                Ok(agg)
            } else {
                Ok(LogicalPlan::Projection(Projection::try_new(
                    rename_expressions(
                        agg.schema()
                            .columns()
                            .iter()
                            .map(|c| col(c.to_owned())),
                        agg.schema(),
                        renamed_schema.fields(),
                    )?,
                    Arc::new(agg),
                )?))
            }
        }
        _ => Ok(LogicalPlan::Projection(Projection::try_new(
            rename_expressions(
                plan.schema()
                    .columns()
                    .iter()
                    .map(|c| col(c.to_owned())),
                plan.schema(),
                renamed_schema.fields(),
            )?,
            Arc::new(plan),
        )?)),
    }
}
