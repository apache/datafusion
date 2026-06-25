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

//! Coordinator wrapper at the root of the plan. On every poll, it walks
//! its subtree to release any [`StageBoundaryBuffer`] whose `is_ready`
//! flag has flipped, then runs each registered [`RuntimeRule`] over the
//! plan. Rules observe runtime stats (via `runtime_row_count` and
//! similar) and mutate adaptive operators in place (e.g.
//! `HashJoinExec::flip_sides`).
//!
//! Buffers notify us via a shared [`AtomicWaker`] (`rto_waker`) that we
//! register on each `poll_next` — `cx.waker()` is task-local and won't
//! cross a spawned-subtask boundary like `RepartitionExec`'s internals,
//! but `AtomicWaker.wake()` does.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use futures::task::AtomicWaker;
use futures::{Stream, StreamExt};
use log::info;

use crate::joins::HashJoinExec;
use crate::stage_boundary_buffer::StageBoundaryBuffer;
use crate::statistics::StatisticsArgs;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

/// A runtime adaptive-execution rule that may inspect the plan tree and
/// mutate adaptive operators in place. Rules are called from
/// [`RuntimeOptimizerExec`] on every poll, *after* ready buffers have
/// been released. They are expected to be cheap and idempotent (track
/// their own "already fired" state if they should only run once per
/// query).
pub trait RuntimeRule: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &str;
    fn evaluate(&self, plan: &Arc<dyn ExecutionPlan>);
}

#[derive(Debug)]
pub struct RuntimeOptimizerExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    /// Shared with every `StageBoundaryBuffer` in this subplan.
    /// Buffers wake this AtomicWaker when `is_ready` flips; we register
    /// the current task's waker on it during each `poll_next` so a
    /// wake-up from inside a spawned subtask reaches the actual
    /// top-of-plan task.
    rto_waker: Arc<AtomicWaker>,
    rules: Vec<Arc<dyn RuntimeRule>>,
}

impl RuntimeOptimizerExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        rto_waker: Arc<AtomicWaker>,
        rules: Vec<Arc<dyn RuntimeRule>>,
    ) -> Self {
        let cache = Arc::clone(input.properties());
        Self {
            input,
            cache,
            rto_waker,
            rules,
        }
    }
}

impl DisplayAs for RuntimeOptimizerExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RuntimeOptimizerExec")
    }
}

impl ExecutionPlan for RuntimeOptimizerExec {
    fn name(&self) -> &'static str {
        "RuntimeOptimizerExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            Arc::clone(&self.rto_waker),
            self.rules.clone(),
        )))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child = self.input.execute(partition, Arc::clone(&context))?;
        // Side-channel drive every StageBoundaryBuffer in the subtree.
        // Buffers don't pull from their inputs until primed, so without
        // this the consumers above them would sit Pending forever (a
        // HashJoin in CollectLeft mode never polls its probe side until
        // build completes — but build itself is gated behind a buffer).
        // prime() is idempotent across partitions.
        prime_all_buffers(&self.input, &context)?;
        let schema = self.schema();
        let stream = CoordinatorStream {
            child,
            plan: Arc::clone(&self.input),
            rto_waker: Arc::clone(&self.rto_waker),
            rules: self.rules.clone(),
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn prime_all_buffers(
    plan: &Arc<dyn ExecutionPlan>,
    ctx: &Arc<TaskContext>,
) -> Result<()> {
    if let Some(buffer) = plan.downcast_ref::<StageBoundaryBuffer>() {
        buffer.prime(ctx)?;
    }
    for child in plan.children() {
        prime_all_buffers(child, ctx)?;
    }
    Ok(())
}

struct CoordinatorStream {
    child: SendableRecordBatchStream,
    plan: Arc<dyn ExecutionPlan>,
    rto_waker: Arc<AtomicWaker>,
    rules: Vec<Arc<dyn RuntimeRule>>,
}

impl Stream for CoordinatorStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        // Register before walking so a buffer flipping is_ready *after*
        // we walked but *before* we return Pending still wakes us.
        this.rto_waker.register(cx.waker());

        // Phase 1: set the permissive default — every ready buffer is
        // proposed for release.
        propose_release_for_ready_buffers(&this.plan);

        // Phase 2: rules may veto specific buffers (set
        // streaming_enabled=false) or mutate adaptive operators.
        for rule in &this.rules {
            rule.evaluate(&this.plan);
        }

        // Phase 3: commit — actually start streaming on any buffer that
        // is still enabled.
        start_streaming_on_enabled_buffers(&this.plan);

        this.child.poll_next_unpin(cx)
    }
}

fn propose_release_for_ready_buffers(plan: &Arc<dyn ExecutionPlan>) {
    if let Some(buffer) = plan.downcast_ref::<StageBoundaryBuffer>() {
        buffer.set_streaming_enabled(buffer.is_ready());
    }
    for child in plan.children() {
        propose_release_for_ready_buffers(child);
    }
}

fn start_streaming_on_enabled_buffers(plan: &Arc<dyn ExecutionPlan>) {
    if let Some(buffer) = plan.downcast_ref::<StageBoundaryBuffer>()
        && buffer.streaming_enabled()
    {
        buffer.start_streaming();
    }
    for child in plan.children() {
        start_streaming_on_enabled_buffers(child);
    }
}

// ---------------------------------------------------------------------------
// SwapBuildSideIfInverted — first concrete RuntimeRule.
//
// When a HashJoinExec's current build side (the LEFT child under
// `mode=CollectLeft`) ends up larger at runtime than the probe side,
// the static planner made the wrong choice — it picked build based on
// (Inexact) estimates. The fix is `HashJoinExec::flip_sides()`, which
// isn't implemented yet; for now this rule only logs intent so we can
// verify the detection logic end-to-end.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct SwapBuildSideIfInverted {
    fired: AtomicBool,
}

impl SwapBuildSideIfInverted {
    pub fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}

impl Default for SwapBuildSideIfInverted {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeRule for SwapBuildSideIfInverted {
    fn name(&self) -> &str {
        "SwapBuildSideIfInverted"
    }

    fn evaluate(&self, plan: &Arc<dyn ExecutionPlan>) {
        if self.fired.load(Ordering::Relaxed) {
            return;
        }
        self.walk_for_swap(plan);
    }
}

impl SwapBuildSideIfInverted {
    fn walk_for_swap(&self, plan: &Arc<dyn ExecutionPlan>) {
        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            let children = join.children();
            if children.len() == 2 {
                // Current HashJoinExec: LEFT child is the build side
                // under `mode=CollectLeft`.
                let left = side_runtime_rows(children[0]);
                let right = side_runtime_rows(children[1]);
                if let (Some(l), Some(r)) = (left, right)
                    && l > r
                    && self
                        .fired
                        .compare_exchange(
                            false,
                            true,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                {
                    info!(
                        "SwapBuildSideIfInverted: would flip HashJoinExec — \
                         current build (left) = {l} rows, probe (right) = {r} \
                         rows. flip_sides() not yet implemented; logging \
                         intent only."
                    );
                }
            }
            return;
        }
        for child in plan.children() {
            self.walk_for_swap(child);
        }
    }
}

/// Row count of a join input's subtree. Trusts `runtime_row_count`
/// to propagate correctly through passthrough operators (Projection,
/// StageBoundaryBuffer-when-ready, etc.); no recursive descent. The
/// StageBoundaryBuffer in the chain returns `None` until its own
/// `is_ready`, which is the natural gate — rules only see runtime
/// stats once the underlying breaker is actually done.
///
/// Falls back to plan-time `statistics()` for pure static-source
/// subtrees (e.g. a small in-memory table behind a
/// `CoalescePartitionsExec`).
fn side_runtime_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    if let Some(rows) = sum_runtime_rows_across_partitions(plan) {
        return Some(rows);
    }
    plan.statistics_with_args(&StatisticsArgs::new())
        .ok()
        .and_then(|s| s.num_rows.get_value().copied())
}

fn sum_runtime_rows_across_partitions(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let n = plan.output_partitioning().partition_count();
    let mut total: usize = 0;
    for p in 0..n {
        // Require every partition to report — partial sums are not
        // meaningful for adaptive decisions.
        total += plan.runtime_row_count(p)?;
    }
    Some(total)
}
