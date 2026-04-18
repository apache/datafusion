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

/// Leaf `ExecutionPlan` that materialises one output partition per
/// inbox. Each call to [`ExecutionPlan::execute`] consumes the
/// corresponding [`Inbox`] exactly once; calling a partition twice is
/// a planning bug and returns an error.
///
/// This is the downstream half of a pipeline cut: upstream pipelines
/// push morsels into the inboxes' [`InboxSender`], and the pipeline
/// rooted above this exec reads them by partition.
pub struct InboxSourceExec {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// One slot per output partition; taken on first `execute`.
    inboxes: Mutex<Vec<Option<Inbox>>>,
}

impl InboxSourceExec {
    pub fn new(schema: SchemaRef, inboxes: Vec<Inbox>) -> Self {
        let partitions = inboxes.len();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            properties,
            inboxes: Mutex::new(inboxes.into_iter().map(Some).collect()),
        }
    }

    /// Number of output partitions (equal to the number of inboxes).
    pub fn partitions(&self) -> usize {
        self.properties.partitioning.partition_count()
    }
}

impl fmt::Debug for InboxSourceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InboxSourceExec")
            .field("partitions", &self.partitions())
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for InboxSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "InboxSourceExec: partitions={}", self.partitions())
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
        let inbox = {
            let mut slots = self.inboxes.lock().expect("inbox mutex poisoned");
            let n_slots = slots.len();
            let slot = slots.get_mut(partition).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "InboxSourceExec partition {partition} out of range (have {n_slots})"
                ))
            })?;
            slot.take().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "InboxSourceExec partition {partition} already executed"
                ))
            })?
        };

        let schema = Arc::clone(&self.schema);
        // Convert the receiver into a stream; each recv yields the next
        // morsel or None when the upstream pipeline has dropped its
        // sender, which we map to end-of-stream.
        let stream = stream::unfold(inbox, |mut inbox| async move {
            inbox.rx.recv().await.map(|item| (item, inbox))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
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
}
