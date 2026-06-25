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

//! Coordinator wrapper at the root of the plan. On every poll it walks
//! its subtree to find the lowest stage whose [`StageBoundaryBuffer`]s
//! are all ready but haven't started streaming yet — the "just completed"
//! stage. When that exists, it fires each registered [`RuntimeRule`]
//! exactly once for that stage, releases the stage's boundaries, then
//! primes the next stage's boundaries against the (possibly replanned)
//! plan. Stage 0 is primed at execute() time; stages 1+ are primed
//! lazily after their predecessor releases — that's what lets a replan
//! at stage K rebuild stage-(K+1)'s input subtree and have the drain
//! task run against the new subtree.
//!
//! Each buffer owns its own AtomicWaker; RTO walks the subtree per poll
//! and registers the consumer-task waker on each. Drain tasks wake on
//! their own buffer's waker, which then wakes the consumer. Per-buffer
//! wakers decouple buffer insertion from RTO insertion at planning time:
//! `InsertStageBoundariesAtBreakers` and `InsertRuntimeOptimizer` are
//! independent optimizer rules.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_execution::TaskContext;
use futures::{Stream, StreamExt};
use log::info;

use crate::stage_boundary_buffer::StageBoundaryBuffer;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

/// A runtime adaptive-execution rule. Shape is identical to
/// `datafusion_physical_optimizer::PhysicalOptimizerRule::optimize` —
/// the trait lives in `physical-plan` rather than reusing the upstream
/// trait directly only because `physical-plan` cannot depend on
/// `physical-optimizer` (the dependency runs the other way). The dual
/// shape is the migration story: any static `PhysicalOptimizerRule`
/// can be made runtime-aware by reading state from
/// `StageBoundaryBuffer`s in the plan tree it receives, and a future
/// upstream unification of the two traits requires no change to call
/// sites.
///
/// RTO invokes `optimize` exactly once per stage-completion event with
/// its current plan; the returned plan replaces RTO's plan. Rules
/// identify the just-completed stage by walking the plan and finding
/// `StageBoundaryBuffer`s where `is_ready() && !streaming_started()`.
pub trait RuntimeRule: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &str;
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

#[derive(Debug)]
pub struct RuntimeOptimizerExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    rules: Vec<Arc<dyn RuntimeRule>>,
}

impl RuntimeOptimizerExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, rules: Vec<Arc<dyn RuntimeRule>>) -> Self {
        let cache = Arc::clone(input.properties());
        Self {
            input,
            cache,
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
        // Prime stage 0 only. Higher stages are primed lazily by
        // CoordinatorStream after their predecessor releases — that's
        // what lets a replan that rebuilds stage-K's input subtree
        // (e.g. swapping a nested HashJoin) take effect: the freshly
        // built boundaries on the new plan are primed *after* replan,
        // so the drain task runs against the post-replan subtree.
        // buffer.prime() touches buffer.input.execute() (the subtree
        // below the boundary) but leaves the boundary's consumer-side
        // rx untouched, so HashJoin can take it later via the lazy
        // execute in CoordinatorStream.
        prime_buffers_at_stage(&self.input, 0, &context)?;
        let schema = self.schema();
        let stream = CoordinatorStream {
            child: None,
            plan: Arc::clone(&self.input),
            rules: self.rules.clone(),
            context: Arc::clone(&context),
            partition,
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Walks the subtree and primes every `StageBoundaryBuffer` whose
/// `stage()` matches `stage`. Other boundaries are skipped — they're
/// either already running (lower stage) or waiting their turn (higher
/// stage). `buffer.prime()` is idempotent, so calling it on an already
/// primed boundary is harmless.
fn prime_buffers_at_stage(
    plan: &Arc<dyn ExecutionPlan>,
    stage: usize,
    ctx: &Arc<TaskContext>,
) -> Result<()> {
    if let Some(buffer) = plan.downcast_ref::<StageBoundaryBuffer>()
        && buffer.stage() == stage
    {
        buffer.prime(ctx)?;
    }
    for child in plan.children() {
        prime_buffers_at_stage(child, stage, ctx)?;
    }
    Ok(())
}

fn register_consumer_waker_on_buffers(plan: &Arc<dyn ExecutionPlan>, waker: &Waker) {
    if let Some(buffer) = plan.downcast_ref::<StageBoundaryBuffer>() {
        buffer.register_consumer_waker(waker);
    }
    for child in plan.children() {
        register_consumer_waker_on_buffers(child, waker);
    }
}

struct CoordinatorStream {
    /// Lazily created. `None` until every stage has been processed (rules
    /// fired, boundaries released, next stage primed) and no boundary
    /// remains gated. Then `self.plan.execute(...)` is called — all
    /// `StageBoundaryBuffer` rxs are still available at that point
    /// because stage-N's drain task only starts after stage-(N-1)
    /// releases (lazy priming), so no operator above a boundary has
    /// been executed before replan.
    child: Option<SendableRecordBatchStream>,
    plan: Arc<dyn ExecutionPlan>,
    rules: Vec<Arc<dyn RuntimeRule>>,
    /// Captured at execute() time; threaded into RuntimeRule::optimize so
    /// rules see the session config (target_partitions, etc.) the same
    /// way static `PhysicalOptimizerRule`s do, and used to lazily execute
    /// the final plan.
    context: Arc<TaskContext>,
    partition: usize,
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
        register_consumer_waker_on_buffers(&this.plan, cx.waker());

        // Find the lowest stage whose boundaries are all ready and none
        // released — the "just completed" stage. Rules fire once for
        // that stage, then we release. Higher stages wait their turn
        // naturally because they can't drain until lower stages release.
        if let Some((stage, boundaries)) = find_just_completed_stage(&this.plan) {
            info!(
                "RTO: stage {stage} ready ({} boundaries); firing {} rule(s) \
                 before release",
                boundaries.len(),
                this.rules.len(),
            );
            let config = this.context.session_config().options();
            let mut current_plan = Arc::clone(&this.plan);
            for rule in &this.rules {
                current_plan = match rule.optimize(Arc::clone(&current_plan), config) {
                    Ok(p) => p,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
            }
            this.plan = current_plan;
            for buffer in &boundaries {
                buffer
                    .downcast_ref::<StageBoundaryBuffer>()
                    .expect(
                        "find_just_completed_stage only returns StageBoundaryBuffer Arcs",
                    )
                    .start_streaming();
            }
            info!(
                "RTO: stage {stage} released; downstream consumers can now \
                 drain the buffered data"
            );
            // Prime the next stage in the (possibly replanned) plan now
            // that stage K's data is flowing. Any boundaries rebuilt by
            // the rule's transform_up are fresh — they get their drain
            // tasks here, against the post-replan subtree.
            if let Err(e) = prime_buffers_at_stage(&this.plan, stage + 1, &this.context) {
                return Poll::Ready(Some(Err(e)));
            }
        }

        // Lazily execute the plan once all stages have been processed.
        // While any boundary is still gated, defer execution so the
        // post-replan plan can take the consumer-side rxs intact.
        if this.child.is_none() {
            if any_buffer_pending(&this.plan) {
                return Poll::Pending;
            }
            match this.plan.execute(this.partition, Arc::clone(&this.context)) {
                Ok(stream) => this.child = Some(stream),
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }

        this.child.as_mut().unwrap().poll_next_unpin(cx)
    }
}

/// True if any `StageBoundaryBuffer` in the subtree has not yet started
/// streaming. RTO uses this to gate lazy execution of `self.plan` until
/// all stages have been processed (rules run, boundaries released).
fn any_buffer_pending(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(buffer) = plan.downcast_ref::<StageBoundaryBuffer>()
        && !buffer.streaming_started()
    {
        return true;
    }
    plan.children().iter().any(|c| any_buffer_pending(c))
}

/// Walks the plan tree, groups every `StageBoundaryBuffer` by stage,
/// and returns the lowest stage where every boundary is ready and none
/// has started streaming yet. Returns `None` if no such stage exists
/// (either nothing is ready, or every ready stage has already fired).
fn find_just_completed_stage(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<(usize, Vec<Arc<dyn ExecutionPlan>>)> {
    let mut by_stage: BTreeMap<usize, Vec<Arc<dyn ExecutionPlan>>> = BTreeMap::new();
    collect_boundaries_by_stage(plan, &mut by_stage);
    by_stage.into_iter().find(|(_, bufs)| {
        bufs.iter().all(|b| {
            let buffer = b.downcast_ref::<StageBoundaryBuffer>().expect(
                "find_just_completed_stage only inserts StageBoundaryBuffer Arcs",
            );
            buffer.is_ready() && !buffer.streaming_started()
        })
    })
}

fn collect_boundaries_by_stage(
    plan: &Arc<dyn ExecutionPlan>,
    out: &mut BTreeMap<usize, Vec<Arc<dyn ExecutionPlan>>>,
) {
    if let Some(buffer) = plan.downcast_ref::<StageBoundaryBuffer>() {
        out.entry(buffer.stage())
            .or_default()
            .push(Arc::clone(plan));
        return;
    }
    for child in plan.children() {
        collect_boundaries_by_stage(child, out);
    }
}
