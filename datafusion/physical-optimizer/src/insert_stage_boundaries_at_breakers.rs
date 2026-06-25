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

//! [`InsertStageBoundariesAtBreakers`] wraps every pipeline-breaking
//! operator (currently `AggregateExec` and `SortExec`) in a
//! [`StageBoundaryBuffer`]. The boundary is where runtime stats become
//! observable to [`RuntimeOptimizerExec`]; without one above a breaker,
//! downstream rules see only static estimates for that subtree.
//!
//! This rule is the temporary, pre-HashJoin-shift form. A follow-up
//! commit retargets it onto HashJoin inputs (and renames accordingly)
//! so the build-side-swap rule has both sides gated. The split between
//! "wrap root in RTO" and "insert boundaries" stays — future adaptive
//! rules (partition coalescing, skew handling) each add their own
//! targeted insertion rule.
//!
//! [`RuntimeOptimizerExec`]: datafusion_physical_plan::runtime_optimizer::RuntimeOptimizerExec

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::stage_boundary_buffer::StageBoundaryBuffer;

#[derive(Default, Debug)]
pub struct InsertStageBoundariesAtBreakers;

impl InsertStageBoundariesAtBreakers {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for InsertStageBoundariesAtBreakers {
    fn name(&self) -> &str {
        "InsertStageBoundariesAtBreakers"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let with_buffers = plan
            .transform_up(|node| {
                if is_pipeline_breaker(&node)
                    && node.downcast_ref::<StageBoundaryBuffer>().is_none()
                {
                    let buffered: Arc<dyn ExecutionPlan> =
                        Arc::new(StageBoundaryBuffer::new(node, 0));
                    Ok(Transformed::yes(buffered))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;
        Ok(with_buffers)
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns true for operators that absorb their entire input before
/// emitting any output — the canonical "pipeline breaker" definition.
///
/// We can't use `pipeline_behavior() == EmissionType::Final` here even
/// though it sounds equivalent. That flag describes an operator's output
/// emission semantics and is inherited from descendants — so `Projection`,
/// `Repartition`, and `HashJoin` above a Final-emitting `AggregateExec`
/// all report `Final` too. We need the *originator* of the pipeline
/// break, not every operator downstream of one.
///
/// `EmissionType::Final && all children != Final` (the transition-point
/// filter) is closer but still misses cascading breakers like
/// `AggregateExec(FinalPartitioned)` whose children are themselves
/// Final because of the `Partial` aggregate below.
///
/// So: hardcoded match against the operators we want to instrument.
/// Extend as more rules need other breakers.
fn is_pipeline_breaker(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.downcast_ref::<AggregateExec>().is_some()
        || plan.downcast_ref::<SortExec>().is_some()
}
