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

use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::limit::GlobalLimitExec;

use crate::physical_optimizer::FFI_PhysicalOptimizerRule;

/// A rule that wraps the input plan in a GlobalLimitExec with skip=0, fetch=10.
/// This produces an observable change in the plan tree that tests can verify.
#[derive(Debug)]
struct AddLimitRule;

impl PhysicalOptimizerRule for AddLimitRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GlobalLimitExec::new(plan, 0, Some(10))))
    }

    fn name(&self) -> &str {
        "add_limit_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

pub(crate) extern "C" fn create_physical_optimizer_rule() -> FFI_PhysicalOptimizerRule {
    let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> = Arc::new(AddLimitRule);
    FFI_PhysicalOptimizerRule::new(rule, None)
}
