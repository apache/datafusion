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

//! [`InsertRuntimeOptimizer`] does two things in one pass:
//!
//! 1. Walks the plan tree, wrapping every pipeline-breaking operator
//!    (currently `AggregateExec` and `SortExec`) in a
//!    [`PipelineBreakerBuffer`]. The buffer is the synchronization point
//!    where runtime stats become observable.
//! 2. Wraps the resulting plan root in a [`RuntimeOptimizerExec`]. The
//!    root operator coordinates: once all buffers signal ready it runs a
//!    `Vec<RuntimeRule>` over the plan, mutates adaptive operators in
//!    place via their typed methods, and releases the buffers.
//!
//! Today both wrappers are passthrough — this rule only installs them so
//! the structural shape is visible in EXPLAIN. Follow-up commits add the
//! synchronization protocol and the first runtime rule.

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::pipeline_breaker_buffer::PipelineBreakerBuffer;
use datafusion_physical_plan::runtime_optimizer::RuntimeOptimizerExec;
use datafusion_physical_plan::sorts::sort::SortExec;

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

        // Phase 1: wrap each pipeline breaker in a PipelineBreakerBuffer.
        let with_buffers = plan
            .transform_up(|node| {
                if is_pipeline_breaker(&node)
                    && node.downcast_ref::<PipelineBreakerBuffer>().is_none()
                {
                    let buffered: Arc<dyn ExecutionPlan> =
                        Arc::new(PipelineBreakerBuffer::new(node));
                    Ok(Transformed::yes(buffered))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;

        // Phase 2: wrap the root.
        Ok(Arc::new(RuntimeOptimizerExec::new(with_buffers)))
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns true for operators that absorb their entire input before
/// emitting (the canonical "pipeline breaker" definition). Start with
/// the obvious cases; extend as more rules need other breakers.
fn is_pipeline_breaker(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.downcast_ref::<AggregateExec>().is_some()
        || plan.downcast_ref::<SortExec>().is_some()
}
