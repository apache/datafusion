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

use std::sync::Arc;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{logical_plan::{EmptyRelation, LogicalPlan}, utils::from_plan, Expr, Filter, Projection};

#[derive(Default)]
pub struct MergeProject;

impl MergeProject {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for MergeProject {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let outer_project = plan.as_projection()?;
        let inner_project = plan.as_projection()?;

        return Ok(Some(LogicalPlan::Projection(Projection {
            expr: merge_projection_expr(outer_project.expr, inner_project.expr),
            input: inner_project.input.clone(),
            schema: outer_project.schema,
            alias: outer_project.alias,
        })));
    }

    fn name(&self) -> &str {
        "merge_filter"
    }
}
