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
use crate::logical_plan::consumer::utils::NameTracker;
use async_recursion::async_recursion;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, not_impl_err};
use datafusion::logical_expr::builder::project;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::find_window_exprs;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use std::collections::HashSet;
use std::sync::Arc;
use substrait::proto::ProjectRel;

#[async_recursion]
pub async fn from_project_rel(
    consumer: &impl SubstraitConsumer,
    p: &ProjectRel,
) -> datafusion::common::Result<LogicalPlan> {
    if let Some(input) = p.input.as_ref() {
        let input = consumer.consume_rel(input).await?;
        let original_schema = Arc::clone(input.schema());

        // Ensure that all expressions have a unique display name, so that
        // validate_unique_names does not fail when constructing the project.
        let mut name_tracker = NameTracker::new();

        // By default, a Substrait Project emits all inputs fields followed by all expressions.
        // We build the explicit expressions first, and then the input expressions to avoid
        // adding aliases to the explicit expressions (as part of ensuring unique names).
        //
        // This is helpful for plan visualization and tests, because when DataFusion produces
        // Substrait Projects it adds an output mapping that excludes all input columns
        // leaving only explicit expressions.

        let mut explicit_exprs: Vec<Expr> = vec![];
        // For WindowFunctions, we need to wrap them in a Window relation. If there are duplicates,
        // we can do the window'ing only once, then the project will duplicate the result.
        // Order here doesn't matter since LPB::window_plan sorts the expressions.
        #[allow(clippy::allow_attributes, clippy::mutable_key_type)]
        // Expr contains Arc with interior mutability but is intentionally used as hash key
        let mut window_exprs: HashSet<Expr> = HashSet::new();
        for expr in &p.expressions {
            let e = consumer
                .consume_expression(expr, input.clone().schema())
                .await?;
            // The project's builder uses columnize_expr(..) to transform
            // nested window expressions into column references.
            window_exprs.extend(find_window_exprs([&e]));
            explicit_exprs.push(name_tracker.get_uniquely_named_expr(e)?);
        }

        let input = if !window_exprs.is_empty() {
            // Window outputs must have unique names across the input schema
            // and the new window expressions.
            let mut window_names = NameTracker::new();
            window_names.reserve_schema(&original_schema);

            let mut aliased_columns: Vec<(Expr, Expr)> = vec![];
            let window_exprs = window_exprs
                .into_iter()
                .map(|window_expr| {
                    let named =
                        window_names.get_uniquely_named_expr(window_expr.clone())?;

                    if let Expr::Alias(alias) = &named {
                        aliased_columns.push((
                            window_expr,
                            Expr::Column(Column::from_name(&alias.name)),
                        ));
                    }

                    Ok(named)
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;

            // References to renamed windows must point to their new output columns.
            if !aliased_columns.is_empty() {
                explicit_exprs = explicit_exprs
                    .into_iter()
                    .map(|expr| reference_aliased_windows(expr, &aliased_columns))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
            }

            LogicalPlanBuilder::window_plan(input, window_exprs)?
        } else {
            input
        };

        let mut final_exprs: Vec<Expr> = vec![];
        for index in 0..original_schema.fields().len() {
            let e = Expr::Column(Column::from(original_schema.qualified_field(index)));
            final_exprs.push(name_tracker.get_uniquely_named_expr(e)?);
        }
        final_exprs.append(&mut explicit_exprs);
        project(input, final_exprs)
    } else {
        not_impl_err!("Projection without an input is not supported")
    }
}

/// Reference renamed window outputs while preserving the projection's
/// original output name.
fn reference_aliased_windows(
    expr: Expr,
    aliased_columns: &[(Expr, Expr)],
) -> datafusion::common::Result<Expr> {
    let saved_name = NamePreserver::new_for_projection().save(&expr);

    let rewritten = expr
        .transform_down(|node| {
            match aliased_columns
                .iter()
                .find(|(window_expr, _)| *window_expr == node)
            {
                Some((_, column)) => Ok(Transformed::new(
                    column.clone(),
                    true,
                    TreeNodeRecursion::Jump,
                )),
                None => Ok(Transformed::no(node)),
            }
        })?
        .data;

    Ok(saved_name.restore(rewritten))
}
