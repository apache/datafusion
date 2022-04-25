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

use crate::execution::context::ExecutionProps;
use crate::optimizer::optimizer::OptimizerRule;
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr, LogicalPlan};

pub struct CheckAnalysis {}

impl CheckAnalysis {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for CheckAnalysis {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> datafusion_common::Result<LogicalPlan> {
        for expr in &plan.expressions() {
            match expr {
                Expr::UnresolvedColumn(col) => {
                    let schemas = plan.all_schemas();
                    if schemas.is_empty() {
                        // TODO is this reachable?
                        return Err(DataFusionError::Plan(format!(
                            "Invalid identifier '{}'",
                            col
                        )))

                    } else {
                        // TODO show all schemas / merge schemas?
                        return Err(DataFusionError::Plan(format!(
                            "Invalid identifier '{}' for schema {}",
                            col, schemas[0]
                        )))
                    }
                }
                _ => {}
            }
        }
        for input in &plan.inputs() {
            self.optimize(input, execution_props)?;
        }
        Ok(plan.clone())
    }

    fn name(&self) -> &str {
        "CheckAnalysis"
    }
}
