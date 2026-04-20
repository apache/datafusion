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

//! [`InboxExec`] — leaf `ExecutionPlan` whose per-partition streams are
//! fed by an upstream breaker's output via [`ExecutionPipeline::push`].
//!
//! PR apache/datafusion#2226 calls these "inboxes". When
//! [`PipelinePlanner`](crate::plan::PipelinePlanner) cuts at a breaker,
//! the surrounding pull-based subtree is rewritten to replace the breaker
//! position (or each of a multi-child node's children) with an
//! `InboxExec`. The breaker's output is then *pushed* into the
//! corresponding inbox; the wrapped subtree's ordinary
//! `plan.execute(partition)` call pulls from the inbox stream as if
//! nothing had changed.
//!
//! The per-partition channel is a plain [`Mutex<VecDeque<RecordBatch>>`]
//! plus a stored [`Waker`] — not a `tokio::sync::mpsc`. For our exactly
//! single-producer / single-consumer use case this is materially cheaper
//! than tokio mpsc (no atomic ref counting on every send, no linked-list
//! management).

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::error::DataFusionError;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::Stream;
use parking_lot::Mutex;

/// Per-partition state shared between the push side (scheduler) and the
/// pull side (wrapped `plan.execute(p)` stream).
struct InboxPartition {
    queue: Mutex<VecDeque<RecordBatch>>,
    waker: Mutex<Option<Waker>>,
    closed: AtomicBool,
    /// Set to true once the stream's `poll_next` sees an EOS so we don't
    /// have to keep touching the closed atomic after we've observed it.
    drained: AtomicBool,
}

impl InboxPartition {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            waker: Mutex::new(None),
            closed: AtomicBool::new(false),
            drained: AtomicBool::new(false),
        }
    }

    #[inline]
    fn wake(&self) {
        let waker = self.waker.lock().take();
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// Sender side of an [`InboxExec`]'s per-partition queues. One
/// `InboxSendGroup` corresponds to one child of the surrounding
/// `ExecutionPipeline`'s rewritten plan.
#[derive(Debug)]
pub(crate) struct InboxSendGroup {
    partitions: Vec<Arc<InboxPartition>>,
}

impl std::fmt::Debug for InboxPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboxPartition")
            .field("queue_len", &self.queue.lock().len())
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish()
    }
}

impl InboxSendGroup {
    /// Push a batch into `partition`. Wakes the consumer only on the
    /// empty→non-empty transition.
    pub(crate) fn push(&self, partition: usize, batch: RecordBatch) -> Result<()> {
        let p = &self.partitions[partition];
        if p.drained.load(Ordering::Relaxed) {
            // Consumer dropped its stream (query cancelled). Nothing to do.
            return Ok(());
        }
        let was_empty = {
            let mut q = p.queue.lock();
            let was_empty = q.is_empty();
            q.push_back(batch);
            was_empty
        };
        if was_empty {
            p.wake();
        }
        Ok(())
    }

    /// Close the sender for `partition` — the consumer's stream will
    /// yield `Ready(None)` once its queue is drained.
    pub(crate) fn close(&self, partition: usize) {
        let p = &self.partitions[partition];
        p.closed.store(true, Ordering::SeqCst);
        p.wake();
    }
}

/// Leaf `ExecutionPlan` backed by N push-side queues — one per output
/// partition. `execute(p, _)` claims the stream for partition `p`; a
/// given partition may only be executed once.
pub struct InboxExec {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    partitions: Vec<Arc<InboxPartition>>,
    executed: Mutex<Vec<bool>>,
}

impl std::fmt::Debug for InboxExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboxExec")
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl InboxExec {
    /// Build a new inbox matching `properties` (schema, partitioning,
    /// ordering, emission, boundedness preserved). Returns the
    /// `ExecutionPlan` wrapper and the sender group keyed by partition.
    pub(crate) fn new(
        schema: SchemaRef,
        properties: Arc<PlanProperties>,
    ) -> (Arc<Self>, InboxSendGroup) {
        let n = properties.partitioning.partition_count();
        let partitions: Vec<_> =
            (0..n).map(|_| Arc::new(InboxPartition::new())).collect();
        let sender_handles = partitions.iter().map(Arc::clone).collect();
        let inbox = Arc::new(Self {
            schema,
            properties,
            partitions,
            executed: Mutex::new(vec![false; n]),
        });
        let group = InboxSendGroup {
            partitions: sender_handles,
        };
        (inbox, group)
    }
}

impl DisplayAs for InboxExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "InboxExec(partitions={})", self.partitions.len())
    }
}

impl ExecutionPlan for InboxExec {
    fn name(&self) -> &'static str {
        "InboxExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &dyn PhysicalExpr,
        )
            -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "InboxExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        {
            let mut executed = self.executed.lock();
            if executed[partition] {
                return Err(DataFusionError::Internal(format!(
                    "InboxExec partition {partition} executed twice",
                )));
            }
            executed[partition] = true;
        }
        Ok(Box::pin(InboxStream {
            schema: Arc::clone(&self.schema),
            state: Arc::clone(&self.partitions[partition]),
        }))
    }
}

struct InboxStream {
    schema: SchemaRef,
    state: Arc<InboxPartition>,
}

impl Drop for InboxStream {
    fn drop(&mut self) {
        // Tell the producer it can stop pushing on this partition.
        self.state.drained.store(true, Ordering::Relaxed);
    }
}

impl Stream for InboxStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Fast path: take a batch if one is queued.
        {
            let mut q = self.state.queue.lock();
            if let Some(batch) = q.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
        }
        // Closed? Drain done.
        if self.state.closed.load(Ordering::SeqCst) {
            // Race: a push could have raced between our pop attempt and
            // this check. Re-check the queue.
            let mut q = self.state.queue.lock();
            if let Some(batch) = q.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            return Poll::Ready(None);
        }
        // Park. Ordering matters: install waker FIRST, then recheck the
        // queue + closed flag to avoid losing a wake that slipped in
        // between the initial check and waker install.
        *self.state.waker.lock() = Some(cx.waker().clone());
        {
            let mut q = self.state.queue.lock();
            if let Some(batch) = q.pop_front() {
                // Wake-up from our own installed waker is harmless; drop
                // it explicitly to avoid a redundant re-poll.
                let _ = self.state.waker.lock().take();
                return Poll::Ready(Some(Ok(batch)));
            }
        }
        if self.state.closed.load(Ordering::SeqCst) {
            let _ = self.state.waker.lock().take();
            return Poll::Ready(None);
        }
        Poll::Pending
    }
}

impl RecordBatchStream for InboxStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
