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

use crate::logical_plan::consumer::utils::NameTracker;
use crate::logical_plan::consumer::SubstraitConsumer;
use async_recursion::async_recursion;
use datafusion::common::{not_impl_err, Column};
use datafusion::logical_expr::builder::project;
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
        let mut window_exprs: HashSet<Expr> = HashSet::new();
        for expr in &p.expressions {
            let e = consumer
                .consume_expression(expr, input.clone().schema())
                .await?;
            // if the expression is WindowFunction, wrap in a Window relation
            if let Expr::WindowFunction(_) = &e {
                // Adding the same expression here and in the project below
                // works because the project's builder uses columnize_expr(..)
                // to transform it into a column reference
                window_exprs.insert(e.clone());
            }
            explicit_exprs.push(name_tracker.get_uniquely_named_expr(e)?);
        }

        let input = if !window_exprs.is_empty() {
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
