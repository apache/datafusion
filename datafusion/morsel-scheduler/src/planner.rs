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

//! Split an `ExecutionPlan` into [`Pipeline`]s at pipeline breakers.
//!
//! # Breakers recognised in v1
//!
//! * [`CoalescePartitionsExec`] — N input partitions → 1 output.
//! * [`SortPreservingMergeExec`] — N input partitions → 1 output
//!   (merge sort across partitions).
//!
//! When one of these nodes is encountered, its children are detached
//! and become new upstream pipelines. The children are replaced in the
//! plan with [`InboxSourceExec`] stubs whose inboxes are fed by those
//! upstream pipelines. The breaker node itself stays in the downstream
//! pipeline.
//!
//! # Breakers explicitly **not** cut in v1
//!
//! * [`RepartitionExec`] — cutting here requires reimplementing
//!   partitioning (hash / round-robin) in the upstream side. Left
//!   intact; the leaf-wrapping policy in [`crate::dispatch::wrap_leaves`]
//!   spreads its input fetchers across workers.
//! * [`SortExec`], `HashJoinExec` build-side, `NestedLoopJoinExec` —
//!   not yet treated as explicit cut points.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use crate::inbox::{DEFAULT_INBOX_CAPACITY, InboxSender, InboxSourceExec, inbox};
use crate::pipeline::{Pipeline, PipelineGraph};

/// `true` if `plan` should be treated as a pipeline breaker (cut
/// point) in v1.
pub fn is_breaker(plan: &dyn ExecutionPlan) -> bool {
    plan.is::<CoalescePartitionsExec>() || plan.is::<SortPreservingMergeExec>()
}

/// Split `root` into a pipeline graph using the default inbox
/// capacity.
pub fn plan_to_pipelines(root: &Arc<dyn ExecutionPlan>) -> Result<PipelineGraph> {
    plan_to_pipelines_with_capacity(root, DEFAULT_INBOX_CAPACITY)
}

/// Split `root` into a pipeline graph, using `capacity` as the bounded
/// capacity of every inter-pipeline inbox.
pub fn plan_to_pipelines_with_capacity(
    root: &Arc<dyn ExecutionPlan>,
    capacity: usize,
) -> Result<PipelineGraph> {
    let mut pending: Vec<PendingUpstream> = Vec::new();
    let rewritten_root = cut(Arc::clone(root), &mut pending, capacity)?;

    let mut pipelines = Vec::with_capacity(pending.len() + 1);
    pipelines.push(Pipeline {
        plan: rewritten_root,
        output_senders: None,
    });
    for u in pending {
        pipelines.push(Pipeline {
            plan: u.plan,
            output_senders: Some(u.senders),
        });
    }

    Ok(PipelineGraph {
        pipelines,
        final_pipeline: 0,
    })
}

struct PendingUpstream {
    plan: Arc<dyn ExecutionPlan>,
    senders: Vec<InboxSender>,
}

/// Walk `plan` in place, replacing each breaker's children with
/// [`InboxSourceExec`] stubs and recording the detached subtrees as
/// new upstream pipelines in `pending`.
fn cut(
    plan: Arc<dyn ExecutionPlan>,
    pending: &mut Vec<PendingUpstream>,
    capacity: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if is_breaker(plan.as_ref()) {
        let mut stub_children = Vec::with_capacity(plan.children().len());
        for child in plan.children() {
            let child = Arc::clone(child);
            let n = child.properties().partitioning.partition_count();
            let schema = child.schema();
            let mut senders = Vec::with_capacity(n);
            let mut receivers = Vec::with_capacity(n);
            for _ in 0..n {
                let (s, r) = inbox(capacity);
                senders.push(s);
                receivers.push(r);
            }
            let stub: Arc<dyn ExecutionPlan> =
                Arc::new(InboxSourceExec::new(schema, receivers));
            let rewritten_child = cut(child, pending, capacity)?;
            pending.push(PendingUpstream {
                plan: rewritten_child,
                senders,
            });
            stub_children.push(stub);
        }
        plan.with_new_children(stub_children)
    } else {
        let child_refs: Vec<Arc<dyn ExecutionPlan>> =
            plan.children().iter().map(|c| Arc::clone(*c)).collect();
        if child_refs.is_empty() {
            return Ok(plan);
        }
        let mut new_children = Vec::with_capacity(child_refs.len());
        for child in child_refs {
            new_children.push(cut(child, pending, capacity)?);
        }
        plan.with_new_children(new_children)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inbox::InboxSourceExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_plan::empty::EmptyExec;
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
    }

    #[test]
    fn no_breaker_stays_as_one_pipeline() {
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(schema()).with_partitions(4));
        let graph = plan_to_pipelines(&plan).unwrap();
        assert_eq!(graph.pipelines.len(), 1);
        assert_eq!(graph.final_pipeline, 0);
        assert!(graph.pipelines[0].output_senders.is_none());
    }

    #[test]
    fn coalesce_root_is_cut() {
        let child: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(schema()).with_partitions(4));
        let root: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(child));

        let graph = plan_to_pipelines(&root).unwrap();
        assert_eq!(graph.pipelines.len(), 2);

        let final_p = &graph.pipelines[graph.final_pipeline];
        assert!(final_p.output_senders.is_none());
        // The child of the (now-rewritten) Coalesce should be an InboxSourceExec.
        let children = final_p.plan.children();
        assert_eq!(children.len(), 1);
        assert!(children[0].as_ref().is::<InboxSourceExec>());

        let upstream = &graph.pipelines[1];
        let senders = upstream.output_senders.as_ref().unwrap();
        assert_eq!(senders.len(), 4);
        assert_eq!(upstream.partition_count(), 4);
    }
}
