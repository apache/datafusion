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

//! Per-pipeline inbox used as the morsel buffer between pipelines.
//!
//! An [`Inbox`] is a bounded mpsc channel carrying `Result<RecordBatch>`
//! items. When the planner cuts a plan at a pipeline breaker, it
//! creates one inbox per upstream output partition: the upstream
//! pipeline feeds the [`InboxSender`] half and the downstream pipeline
//! reads the [`Inbox`] half through an [`InboxSourceExec`] leaf.
//!
//! The bounded capacity provides natural backpressure: when the
//! downstream pipeline lags, the upstream pipeline's
//! [`InboxSender::send`] await blocks, preventing unbounded queueing.
//!
//! # Modes
//!
//! [`InboxSourceExec`] supports two modes:
//!
//! * **Independent** — one bounded channel per upstream / downstream
//!   partition. Preserves partition identity for operators that need
//!   it (`SortPreservingMergeExec`, partition-preserving `SortExec`).
//!
//! * **Shared** — a single bounded MPMC-style queue into which *all*
//!   upstream partitions push and from which *any* downstream
//!   consumer may pull. Enables **morsel-granularity stealing**:
//!   whichever downstream worker is idle grabs the next morsel. Used
//!   for breakers where input partition identity is irrelevant to the
//!   operator immediately above the cut (`RepartitionExec`).

use std::fmt::{self, Formatter};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::error::DataFusionError;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream;
use tokio::sync::mpsc;

/// Default bounded capacity for inter-pipeline inboxes, measured in
/// batches. Small enough to cap memory buildup, large enough to let a
/// fast producer and slow consumer overlap.
pub const DEFAULT_INBOX_CAPACITY: usize = 4;

/// Producer half of an [`Inbox`].
#[derive(Debug, Clone)]
pub struct InboxSender {
    tx: mpsc::Sender<Result<RecordBatch>>,
}

impl InboxSender {
    /// Send a single morsel. Returns an error if the consumer
    /// pipeline has dropped its receiver.
    pub async fn send(&self, item: Result<RecordBatch>) -> Result<()> {
        self.tx.send(item).await.map_err(|_| {
            DataFusionError::Internal(
                "morsel inbox receiver dropped before producer finished".to_string(),
            )
        })
    }

    /// `true` if the consumer has dropped the receiver.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// Consumer half of an inbox.
#[derive(Debug)]
pub struct Inbox {
    rx: mpsc::Receiver<Result<RecordBatch>>,
}

/// Create a new inbox pair with the given bounded capacity.
pub fn inbox(capacity: usize) -> (InboxSender, Inbox) {
    let (tx, rx) = mpsc::channel(capacity);
    (InboxSender { tx }, Inbox { rx })
}

/// Shared-morsel receiver: the consumer half of a bounded queue that
/// **any** number of downstream tasks may drain concurrently.
///
/// Multiple consumers race for the next morsel: whichever task
/// acquires the internal mutex first returns with the head of the
/// queue. Concurrent pulls therefore serialize on the mutex for the
/// microseconds-long `recv` call, but the morsels they return are
/// processed in parallel by the respective callers — giving
/// morsel-granularity stealing across the downstream pool.
pub struct SharedInbox {
    rx: tokio::sync::Mutex<mpsc::Receiver<Result<RecordBatch>>>,
}

impl SharedInbox {
    fn new(rx: mpsc::Receiver<Result<RecordBatch>>) -> Self {
        Self {
            rx: tokio::sync::Mutex::new(rx),
        }
    }

    /// Pull the next morsel. Returns `None` when every sender has
    /// been dropped and the queue is drained.
    pub async fn recv(&self) -> Option<Result<RecordBatch>> {
        self.rx.lock().await.recv().await
    }
}

/// Create a shared-morsel setup: `n` `InboxSender` clones that all
/// feed one [`SharedInbox`]. Dropping all `n` senders closes the
/// queue.
pub fn shared_inbox(capacity: usize, n: usize) -> (Vec<InboxSender>, SharedInbox) {
    let (tx, rx) = mpsc::channel(capacity);
    let senders = (0..n).map(|_| InboxSender { tx: tx.clone() }).collect();
    drop(tx);
    (senders, SharedInbox::new(rx))
}

/// Leaf `ExecutionPlan` that materialises the downstream half of a
/// pipeline cut. Two layouts are supported, selected by the planner
/// based on the breaker type — see the module docs.
enum Mode {
    /// One [`Inbox`] per output partition; each is taken once by its
    /// corresponding `execute(p)` call.
    Independent(Mutex<Vec<Option<Inbox>>>),
    /// A single [`SharedInbox`]; `execute(p)` returns a stream that
    /// races with other partitions' streams to pull morsels from the
    /// shared queue.
    Shared(Arc<SharedInbox>),
}

/// Downstream source for a pipeline cut.
pub struct InboxSourceExec {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    mode: Mode,
}

impl InboxSourceExec {
    /// Independent-inbox mode: one inbox per output partition.
    pub fn new(schema: SchemaRef, inboxes: Vec<Inbox>) -> Self {
        let partitions = inboxes.len();
        Self {
            schema: Arc::clone(&schema),
            properties: Self::properties(schema, partitions),
            mode: Mode::Independent(Mutex::new(inboxes.into_iter().map(Some).collect())),
        }
    }

    /// Shared-morsel mode: all `partitions` output streams drain the
    /// same underlying queue, picking up whichever morsel is next.
    pub fn new_shared(schema: SchemaRef, inbox: SharedInbox, partitions: usize) -> Self {
        Self {
            schema: Arc::clone(&schema),
            properties: Self::properties(schema, partitions),
            mode: Mode::Shared(Arc::new(inbox)),
        }
    }

    fn properties(schema: SchemaRef, partitions: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }

    /// Number of output partitions the downstream operator will see.
    pub fn partitions(&self) -> usize {
        self.properties.partitioning.partition_count()
    }

    /// `true` if morsel-granularity stealing is enabled.
    pub fn is_shared(&self) -> bool {
        matches!(self.mode, Mode::Shared(_))
    }
}

impl fmt::Debug for InboxSourceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxSourceExec")
            .field("partitions", &self.partitions())
            .field("shared", &self.is_shared())
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for InboxSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        if self.is_shared() {
            write!(
                f,
                "InboxSourceExec: partitions={}, shared-morsel",
                self.partitions()
            )
        } else {
            write!(f, "InboxSourceExec: partitions={}", self.partitions())
        }
    }
}

impl ExecutionPlan for InboxSourceExec {
    fn name(&self) -> &str {
        "InboxSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "InboxSourceExec is a leaf and takes no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let n = self.partitions();
        if partition >= n {
            return Err(DataFusionError::Internal(format!(
                "InboxSourceExec partition {partition} out of range (have {n})"
            )));
        }
        let schema = Arc::clone(&self.schema);
        match &self.mode {
            Mode::Independent(slots) => {
                let inbox = {
                    let mut slots = slots.lock().expect("inbox mutex poisoned");
                    slots[partition].take().ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "InboxSourceExec partition {partition} already executed"
                        ))
                    })?
                };
                // Convert the receiver into a stream; each recv yields
                // the next morsel or None at end-of-stream.
                let stream = stream::unfold(inbox, |mut inbox| async move {
                    inbox.rx.recv().await.map(|item| (item, inbox))
                });
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
            Mode::Shared(shared) => {
                // Each execute(p) returns its own stream but all
                // streams pull from the same queue. `recv` is
                // serialised on a mutex for the (microsecond)
                // duration of the pull only; processing happens in
                // parallel in the respective callers.
                let shared = Arc::clone(shared);
                let stream = stream::unfold(shared, |shared| async move {
                    let item = shared.recv().await?;
                    Some((item, shared))
                });
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
    }

    fn make_batch(schema: &SchemaRef, vals: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(vals.to_vec())) as _],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn inbox_delivers_morsels_in_order() {
        let schema = test_schema();
        let (tx0, rx0) = inbox(4);
        let (tx1, rx1) = inbox(4);
        let exec = Arc::new(InboxSourceExec::new(Arc::clone(&schema), vec![rx0, rx1]));
        let task_ctx = Arc::new(TaskContext::default());

        // Buffer capacity (4) exceeds messages; fill eagerly then drop the
        // senders so the receiver reaches end-of-stream after draining.
        tx0.send(Ok(make_batch(&schema, &[1]))).await.unwrap();
        tx0.send(Ok(make_batch(&schema, &[2]))).await.unwrap();
        drop(tx0);
        tx1.send(Ok(make_batch(&schema, &[10]))).await.unwrap();
        drop(tx1);

        let s0 = exec.execute(0, Arc::clone(&task_ctx)).unwrap();
        let s1 = exec.execute(1, task_ctx).unwrap();

        use futures::StreamExt;
        let b0: Vec<_> = s0.collect().await;
        let b1: Vec<_> = s1.collect().await;
        assert_eq!(b0.len(), 2);
        assert_eq!(b1.len(), 1);
    }

    #[tokio::test]
    async fn double_execute_same_partition_errors() {
        let schema = test_schema();
        let (_tx, rx) = inbox(1);
        let exec = Arc::new(InboxSourceExec::new(Arc::clone(&schema), vec![rx]));
        let ctx = Arc::new(TaskContext::default());
        assert!(exec.execute(0, Arc::clone(&ctx)).is_ok());
        assert!(exec.execute(0, ctx).is_err());
    }

    #[tokio::test]
    async fn shared_inbox_streams_race_for_morsels() {
        // Two streams draining the same shared inbox. Producers push
        // four batches; the two consumers together should receive all
        // four exactly once, in some order.
        let schema = test_schema();
        let (senders, shared) = shared_inbox(8, 2);
        let exec = Arc::new(InboxSourceExec::new_shared(Arc::clone(&schema), shared, 2));
        assert!(exec.is_shared());
        let task_ctx = Arc::new(TaskContext::default());

        // Push 4 batches across both producers.
        senders[0]
            .send(Ok(make_batch(&schema, &[1])))
            .await
            .unwrap();
        senders[1]
            .send(Ok(make_batch(&schema, &[2])))
            .await
            .unwrap();
        senders[0]
            .send(Ok(make_batch(&schema, &[3])))
            .await
            .unwrap();
        senders[1]
            .send(Ok(make_batch(&schema, &[4])))
            .await
            .unwrap();
        drop(senders);

        let s0 = exec.execute(0, Arc::clone(&task_ctx)).unwrap();
        let s1 = exec.execute(1, task_ctx).unwrap();

        use futures::StreamExt;
        let (b0, b1) = futures::join!(s0.collect::<Vec<_>>(), s1.collect::<Vec<_>>());

        // Together the two consumers see all four batches.
        let total = b0.len() + b1.len();
        assert_eq!(total, 4);
        let mut values: Vec<i32> = b0
            .into_iter()
            .chain(b1)
            .flat_map(|r| {
                let batch = r.unwrap();
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect();
        values.sort();
        assert_eq!(values, vec![1, 2, 3, 4]);
    }
}
