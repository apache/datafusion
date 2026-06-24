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
//! 2. Wraps the resulting plan root in a [`RuntimeOptimizerExec`], which
//!    walks the subtree on each `poll_next` and releases ready buffers.
//!
//! A shared [`AtomicWaker`] is constructed here and threaded into both
//! the buffers and the wrapping RTO so buffers can wake the coordinator
//! from inside spawned subtasks (e.g. `RepartitionExec` internals). The
//! default is permissive release — the next commit will add a
//! `Vec<RuntimeRule>` that runs between "all ready" and "release," able
//! to mutate adaptive operators (`HashJoinExec::flip_sides`) or hold
//! specific buffers.

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::pipeline_breaker_buffer::PipelineBreakerBuffer;
use datafusion_physical_plan::runtime_optimizer::{
    RuntimeOptimizerExec, RuntimeRule, SwapBuildSideIfInverted,
};
use datafusion_physical_plan::sorts::sort::SortExec;
use futures::task::AtomicWaker;

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

        // Shared coordinator-wake. Every buffer in this subplan and the
        // wrapping RTO hold a clone — buffers wake it, RTO registers on
        // it. Per-poll cx.waker() doesn't reach across spawned-task
        // boundaries (e.g. RepartitionExec's internals), this does.
        let rto_waker = Arc::new(AtomicWaker::new());

        // Phase 1: wrap each pipeline breaker in a PipelineBreakerBuffer.
        let with_buffers = plan
            .transform_up(|node| {
                if is_pipeline_breaker(&node)
                    && node.downcast_ref::<PipelineBreakerBuffer>().is_none()
                {
                    let buffered: Arc<dyn ExecutionPlan> = Arc::new(
                        PipelineBreakerBuffer::new(node, Arc::clone(&rto_waker)),
                    );
                    Ok(Transformed::yes(buffered))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;

        // Phase 2: wrap the root with the default rule set.
        let rules: Vec<Arc<dyn RuntimeRule>> =
            vec![Arc::new(SwapBuildSideIfInverted::new())];
        Ok(Arc::new(RuntimeOptimizerExec::new(
            with_buffers,
            rto_waker,
            rules,
        )))
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
