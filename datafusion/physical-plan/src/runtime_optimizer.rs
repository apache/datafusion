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

//! Coordinator wrapper at the root of the plan. On every poll, walks its
//! subtree and releases any [`PipelineBreakerBuffer`] whose `is_ready`
//! flag has flipped. Buffers schedule re-polls via `cx.waker().wake_by_ref()`
//! when they become ready, so the wrapper sees the state change without
//! any explicit notification channel.
//!
//! Base case today is unconditional release: as soon as a buffer reports
//! ready, `set_go_ahead` is called. A future `Vec<RuntimeRule>` will run
//! between "all ready" and "release," allowing rules to mutate adaptive
//! operators (e.g. `HashJoinExec::flip_sides`) or hold specific buffers
//! by leaving `go_ahead` false.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use futures::task::AtomicWaker;
use futures::{Stream, StreamExt};

use crate::pipeline_breaker_buffer::PipelineBreakerBuffer;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct RuntimeOptimizerExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    /// Shared with every `PipelineBreakerBuffer` in this subplan.
    /// Buffers wake this AtomicWaker when `is_ready` flips; we register
    /// the current task's waker on it during each `poll_next` so a
    /// wake-up from inside a spawned subtask (e.g. one of
    /// `RepartitionExec`'s internals) reaches the actual top-of-plan
    /// task — `cx.waker()` propagation alone can't cross that boundary.
    rto_waker: Arc<AtomicWaker>,
}

impl RuntimeOptimizerExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, rto_waker: Arc<AtomicWaker>) -> Self {
        let cache = Arc::clone(input.properties());
        Self {
            input,
            cache,
            rto_waker,
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
        let child = self.input.execute(partition, context)?;
        let schema = self.schema();
        let stream = CoordinatorStream {
            child,
            plan: Arc::clone(&self.input),
            rto_waker: Arc::clone(&self.rto_waker),
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Wraps the child stream. Each poll registers our task waker with the
/// shared `rto_waker` (so any buffer in the subtree can pull us out of
/// `Pending` when its `is_ready` flips), walks the subtree releasing
/// ready buffers, then forwards the child poll.
struct CoordinatorStream {
    child: SendableRecordBatchStream,
    plan: Arc<dyn ExecutionPlan>,
    rto_waker: Arc<AtomicWaker>,
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
        release_ready_buffers(&this.plan);
        this.child.poll_next_unpin(cx)
    }
}

/// Walk the subtree; for every `PipelineBreakerBuffer` whose `is_ready`
/// flag is set, call `set_go_ahead`. Idempotent — released buffers
/// short-circuit on the next call.
fn release_ready_buffers(plan: &Arc<dyn ExecutionPlan>) {
    if let Some(buffer) = plan.downcast_ref::<PipelineBreakerBuffer>()
        && buffer.is_ready()
    {
        buffer.set_go_ahead();
    }
    for child in plan.children() {
        release_ready_buffers(child);
    }
}
