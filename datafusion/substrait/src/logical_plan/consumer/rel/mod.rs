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

mod aggregate_rel;
mod cross_rel;
mod exchange_rel;
mod fetch_rel;
mod filter_rel;
mod join_rel;
mod project_rel;
mod read_rel;
mod set_rel;
mod sort_rel;

pub use aggregate_rel::*;
pub use cross_rel::*;
pub use exchange_rel::*;
pub use fetch_rel::*;
pub use filter_rel::*;
pub use join_rel::*;
pub use project_rel::*;
pub use read_rel::*;
pub use set_rel::*;
pub use sort_rel::*;

use crate::logical_plan::consumer::SubstraitConsumer;
use crate::logical_plan::consumer::utils::NameTracker;
use async_recursion::async_recursion;
use datafusion::common::{Column, not_impl_err, substrait_datafusion_err, substrait_err};
use datafusion::logical_expr::builder::project;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use std::sync::Arc;
use substrait::proto::rel::RelType;
use substrait::proto::rel_common::{Emit, EmitKind};
use substrait::proto::{Rel, RelCommon, rel_common};

/// Convert Substrait Rel to DataFusion DataFrame
#[async_recursion]
pub async fn from_substrait_rel(
    consumer: &impl SubstraitConsumer,
    relation: &Rel,
) -> datafusion::common::Result<LogicalPlan> {
    let plan: datafusion::common::Result<LogicalPlan> = match &relation.rel_type {
        Some(rel_type) => match rel_type {
            RelType::Read(rel) => consumer.consume_read(rel).await,
            RelType::Filter(rel) => consumer.consume_filter(rel).await,
            RelType::Fetch(rel) => consumer.consume_fetch(rel).await,
            RelType::Aggregate(rel) => consumer.consume_aggregate(rel).await,
            RelType::Sort(rel) => consumer.consume_sort(rel).await,
            RelType::Join(rel) => consumer.consume_join(rel).await,
            RelType::Project(rel) => consumer.consume_project(rel).await,
            RelType::Set(rel) => consumer.consume_set(rel).await,
            RelType::ExtensionSingle(rel) => consumer.consume_extension_single(rel).await,
            RelType::ExtensionMulti(rel) => consumer.consume_extension_multi(rel).await,
            RelType::ExtensionLeaf(rel) => consumer.consume_extension_leaf(rel).await,
            RelType::Cross(rel) => consumer.consume_cross(rel).await,
            RelType::Window(rel) => {
                consumer.consume_consistent_partition_window(rel).await
            }
            RelType::Exchange(rel) => consumer.consume_exchange(rel).await,
            rt => not_impl_err!("{rt:?} rel not supported yet"),
        },
        None => return substrait_err!("rel must set rel_type"),
    };
    apply_emit_kind(retrieve_rel_common(relation), plan?)
}

fn apply_emit_kind(
    rel_common: Option<&RelCommon>,
    plan: LogicalPlan,
) -> datafusion::common::Result<LogicalPlan> {
    match retrieve_emit_kind(rel_common) {
        EmitKind::Direct(_) => Ok(plan),
        EmitKind::Emit(Emit { output_mapping }) => {
            // It is valid to reference the same field multiple times in the Emit
            // In this case, we need to provide unique names to avoid collisions
            let mut name_tracker = NameTracker::new();
            match plan {
                // To avoid adding a projection on top of a projection, we apply special case
                // handling to flatten Substrait Emits. This is only applicable if none of the
                // expressions in the projection are volatile. This is to avoid issues like
                // converting a single call of the random() function into multiple calls due to
                // duplicate fields in the output_mapping.
                LogicalPlan::Projection(proj) if !contains_volatile_expr(&proj) => {
                    let mut exprs: Vec<Expr> = vec![];
                    for field in output_mapping {
                        let expr = proj.expr
                            .get(field as usize)
                            .ok_or_else(|| substrait_datafusion_err!(
                                  "Emit output field {} cannot be resolved in input schema {}",
                                  field, proj.input.schema()
                                ))?;
                        exprs.push(name_tracker.get_uniquely_named_expr(expr.clone())?);
                    }

                    let input = Arc::unwrap_or_clone(proj.input);
                    project(input, exprs)
                }
                // Otherwise we just handle the output_mapping as a projection
                _ => {
                    let input_schema = plan.schema();

                    let mut exprs: Vec<Expr> = vec![];
                    for index in output_mapping.into_iter() {
                        let column = Expr::Column(Column::from(
                            input_schema.qualified_field(index as usize),
                        ));
                        let expr = name_tracker.get_uniquely_named_expr(column)?;
                        exprs.push(expr);
                    }

                    project(plan, exprs)
                }
            }
        }
    }
}

fn retrieve_rel_common(rel: &Rel) -> Option<&RelCommon> {
    match rel.rel_type.as_ref() {
        None => None,
        Some(rt) => match rt {
            RelType::Read(r) => r.common.as_ref(),
            RelType::Filter(f) => f.common.as_ref(),
            RelType::Fetch(f) => f.common.as_ref(),
            RelType::Aggregate(a) => a.common.as_ref(),
            RelType::Sort(s) => s.common.as_ref(),
            RelType::Join(j) => j.common.as_ref(),
            RelType::Project(p) => p.common.as_ref(),
            RelType::Set(s) => s.common.as_ref(),
            RelType::ExtensionSingle(e) => e.common.as_ref(),
            RelType::ExtensionMulti(e) => e.common.as_ref(),
            RelType::ExtensionLeaf(e) => e.common.as_ref(),
            RelType::Cross(c) => c.common.as_ref(),
            RelType::Reference(_) => None,
            RelType::Write(w) => w.common.as_ref(),
            RelType::Ddl(d) => d.common.as_ref(),
            RelType::HashJoin(j) => j.common.as_ref(),
            RelType::MergeJoin(j) => j.common.as_ref(),
            RelType::NestedLoopJoin(j) => j.common.as_ref(),
            RelType::Window(w) => w.common.as_ref(),
            RelType::Exchange(e) => e.common.as_ref(),
            RelType::Expand(e) => e.common.as_ref(),
            RelType::Update(_) => None,
        },
    }
}

fn retrieve_emit_kind(rel_common: Option<&RelCommon>) -> EmitKind {
    // the default EmitKind is Direct if it is not set explicitly
    let default = EmitKind::Direct(rel_common::Direct {});
    rel_common
        .and_then(|rc| rc.emit_kind.as_ref())
        .map_or(default, |ek| ek.clone())
}

fn contains_volatile_expr(proj: &Projection) -> bool {
    proj.expr.iter().any(|e| e.is_volatile())
}
