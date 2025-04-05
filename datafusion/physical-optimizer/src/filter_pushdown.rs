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

use datafusion_common::{config::ConfigOptions, DataFusionError, Result};
use datafusion_physical_plan::{
    execution_plan::ExecutionPlanFilterPushdownResult, ExecutionPlan,
};

use crate::PhysicalOptimizerRule;

/// A physical optimizer rule that pushes down filters in the execution plan.
/// See [`ExecutionPlan::try_pushdown_filters`] for a detailed description of the algorithm.
#[derive(Debug)]
pub struct PushdownFilter {}

impl Default for PushdownFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl PushdownFilter {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for PushdownFilter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match plan.try_pushdown_filters(&plan, &Vec::new())? {
            ExecutionPlanFilterPushdownResult::NotPushed => Ok(plan),
            ExecutionPlanFilterPushdownResult::Pushed { inner, support } => {
                if !support.is_empty() {
                    return Err(
                        DataFusionError::Plan(
                            format!("PushdownFilter: plan returned support length does not match filters length: {} != 0", support.len()
                        ))
                    );
                }
                Ok(inner)
            }
        }
    }

    fn name(&self) -> &str {
        "PushdownFilter"
    }

    fn schema_check(&self) -> bool {
        true // Filter pushdown does not change the schema of the plan
    }
}
