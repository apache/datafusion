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

//! [`InsertRuntimeOptimizer`] wraps the root of the physical plan in a
//! [`RuntimeOptimizerExec`]. Future commits add `PipelineBreakerBuffer`s
//! beneath pipeline-breaking operators and a `Vec<RuntimeRule>` that the
//! root coordinator runs once those buffers signal ready.
//!
//! Today the wrapper is a passthrough; this rule exists so the operator
//! has a stable insertion point that subsequent commits can build on.

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::runtime_optimizer::RuntimeOptimizerExec;

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
        // Don't re-wrap if we've already been inserted (e.g. multi-pass
        // optimization loops).
        if plan.downcast_ref::<RuntimeOptimizerExec>().is_some() {
            return Ok(plan);
        }
        Ok(Arc::new(RuntimeOptimizerExec::new(plan)))
    }

    fn schema_check(&self) -> bool {
        true
    }
}
