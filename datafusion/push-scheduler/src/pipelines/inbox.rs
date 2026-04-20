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
//! fed by channels the scheduler fills from an upstream breaker's output.
//!
//! PR apache/datafusion#2226 calls these "inboxes". When
//! [`PipelinePlanner`](crate::plan::PipelinePlanner) cuts at a breaker,
//! the surrounding pull-based subtree is rewritten to replace the breaker
//! position (or, for multi-child nodes, each of the original children)
//! with an `InboxExec`. The breaker's output is then *pushed* into the
//! corresponding channels via [`ExecutionPipeline::push`], and the wrapped
//! subtree's ordinary `plan.execute(partition)` call pulls from the
//! `InboxExec` stream as if nothing had changed.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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

/// Sender side of an [`InboxExec`]'s per-partition channels. One
/// `InboxSendGroup` corresponds to one child of the ExecutionPipeline's
/// wrapped plan.
#[derive(Debug)]
pub(crate) struct InboxSendGroup {
    senders: Vec<Mutex<Option<tokio::sync::mpsc::UnboundedSender<Result<RecordBatch>>>>>,
}

impl InboxSendGroup {
    /// Push a batch into `partition`. Ignores the send if the receiver
    /// was dropped (query cancelled) — the outer pipeline will observe
    /// cancellation through its own output channel.
    pub(crate) fn push(&self, partition: usize, batch: RecordBatch) -> Result<()> {
        if let Some(sender) = &*self.senders[partition].lock() {
            let _ = sender.send(Ok(batch));
        }
        Ok(())
    }

    /// Drop the sender for `partition` so the receiver's stream yields
    /// `Ready(None)`.
    pub(crate) fn close(&self, partition: usize) {
        let _ = self.senders[partition].lock().take();
    }
}

/// Leaf `ExecutionPlan` backed by N tokio mpsc channels — one per output
/// partition. `execute(p, _)` claims the receiver for partition `p` and
/// returns a stream that drains it. A given partition may only be
/// executed once.
pub struct InboxExec {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    receivers:
        Vec<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<Result<RecordBatch>>>>>,
}

impl std::fmt::Debug for InboxExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboxExec")
            .field("partitions", &self.receivers.len())
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
        let partitions = properties.partitioning.partition_count();
        let mut senders = Vec::with_capacity(partitions);
        let mut receivers = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            senders.push(Mutex::new(Some(tx)));
            receivers.push(Mutex::new(Some(rx)));
        }
        let inbox = Arc::new(Self {
            schema,
            properties,
            receivers,
        });
        let group = InboxSendGroup { senders };
        (inbox, group)
    }
}

impl DisplayAs for InboxExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "InboxExec(partitions={})", self.receivers.len())
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
        let receiver = self.receivers[partition].lock().take().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "InboxExec partition {partition} executed twice",
            ))
        })?;
        Ok(Box::pin(InboxStream {
            schema: Arc::clone(&self.schema),
            receiver,
        }))
    }
}

/// Trait bound helper — `ExecutionPlan: Any + Debug + DisplayAs + Send + Sync`.
/// We derive `Any` via the blanket impl in `std::any::Any` because
/// `InboxExec: 'static`.
const _: fn() = || {
    fn assert_any<T: Any + ?Sized>() {}
    assert_any::<dyn ExecutionPlan>();
};

struct InboxStream {
    schema: SchemaRef,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Result<RecordBatch>>,
}

impl Stream for InboxStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl RecordBatchStream for InboxStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
