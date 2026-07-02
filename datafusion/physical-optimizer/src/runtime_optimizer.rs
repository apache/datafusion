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

//! [`InsertRuntimeOptimizer`] wraps the (now-final) plan root in a
//! [`RuntimeOptimizerExec`] with the default set of runtime rules. It
//! does nothing else — buffer insertion happens in a separate, targeted
//! rule (today: [`InsertStageBoundariesAtBreakers`]). The split lets
//! future adaptive optimizations (partition coalescing, skew handling)
//! introduce their own targeted insertion rules without touching the
//! RTO-wrapping logic.

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::runtime_optimizer::{RuntimeOptimizerExec, RuntimeRule};
use datafusion_physical_plan::runtime_rules::SwapBuildSideIfInverted;

#[derive(Default, Debug)]
pub struct InsertRuntimeOptimizer;

impl InsertRuntimeOptimizer {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for InsertRuntimeOptimizer {
    fn name(&self) -> &str {
        "InsertRuntimeOptimizer"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Don't re-wrap if we've already been inserted (multi-pass loops).
        if plan.downcast_ref::<RuntimeOptimizerExec>().is_some() {
            return Ok(plan);
        }
        let rules: Vec<Arc<dyn RuntimeRule>> =
            vec![Arc::new(SwapBuildSideIfInverted::new())];
        Ok(Arc::new(RuntimeOptimizerExec::new(plan, rules)))
    }

    fn schema_check(&self) -> bool {
        true
    }
}
