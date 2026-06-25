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
//! exactly once for that stage, then releases the stage's boundaries.
//! Stage K+1's boundaries can't fill until stage K is released, so
//! stages naturally serialize without an explicit veto mechanism.
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
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_execution::TaskContext;
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
            rules: self.rules.clone(),
            context: Arc::clone(&context),
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

fn register_consumer_waker_on_buffers(plan: &Arc<dyn ExecutionPlan>, waker: &Waker) {
    if let Some(buffer) = plan.downcast_ref::<StageBoundaryBuffer>() {
        buffer.register_consumer_waker(waker);
    }
    for child in plan.children() {
        register_consumer_waker_on_buffers(child, waker);
    }
}

struct CoordinatorStream {
    child: SendableRecordBatchStream,
    plan: Arc<dyn ExecutionPlan>,
    rules: Vec<Arc<dyn RuntimeRule>>,
    /// Captured at execute() time; threaded into RuntimeRule::optimize so
    /// rules see the session config (target_partitions, etc.) the same
    /// way static `PhysicalOptimizerRule`s do.
    context: Arc<TaskContext>,
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
        }

        this.child.poll_next_unpin(cx)
    }
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

// ---------------------------------------------------------------------------
// SwapBuildSideIfInverted — first concrete RuntimeRule.
//
// When a HashJoinExec's current build side ends up larger at runtime
// than the probe side, the static planner made the wrong choice — it
// picked build based on (Inexact) estimates. This rule walks the plan
// looking for joins whose children are `StageBoundaryBuffer`s in the
// just-completed state (`is_ready && !streaming_started`); if l > r,
// it logs intent. The actual `HashJoinExec::swap_inputs` call lands in
// the next commit (#14).
// ---------------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct SwapBuildSideIfInverted;

impl SwapBuildSideIfInverted {
    pub fn new() -> Self {
        Self
    }
}

impl RuntimeRule for SwapBuildSideIfInverted {
    fn name(&self) -> &str {
        "SwapBuildSideIfInverted"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if just_completed_stage_of_join(join).is_none() {
                return Ok(Transformed::no(node));
            }
            let children = join.children();
            // Current HashJoinExec: LEFT child is the build side under
            // `mode=CollectLeft`.
            let left = side_runtime_rows(children[0]);
            let right = side_runtime_rows(children[1]);
            if let (Some(l), Some(r)) = (left, right)
                && l > r
            {
                info!(
                    "SwapBuildSideIfInverted: would flip HashJoinExec — \
                     current build (left) = {l} rows, probe (right) = {r} \
                     rows. swap_inputs() wiring lands in the next commit; \
                     logging intent only."
                );
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
    }
}

/// Returns `Some(stage)` if `join`'s two children are both
/// `StageBoundaryBuffer`s at the same stage in the just-completed state
/// (`is_ready && !streaming_started`). Otherwise `None`.
fn just_completed_stage_of_join(join: &HashJoinExec) -> Option<usize> {
    let children = join.children();
    if children.len() != 2 {
        return None;
    }
    let left = children[0].downcast_ref::<StageBoundaryBuffer>()?;
    let right = children[1].downcast_ref::<StageBoundaryBuffer>()?;
    if left.stage() != right.stage() {
        return None;
    }
    if left.is_ready()
        && !left.streaming_started()
        && right.is_ready()
        && !right.streaming_started()
    {
        Some(left.stage())
    } else {
        None
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
