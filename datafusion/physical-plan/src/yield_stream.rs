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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::execution_plan::CardinalityEffect;
use crate::execution_plan::CardinalityEffect::Equal;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use datafusion_common::{Result, Statistics};
use datafusion_execution::TaskContext;
use futures::{Stream, StreamExt};

/// Number of batches to yield before voluntarily returning Pending.
/// This allows long-running operators to periodically yield control
/// back to the executor (e.g., to handle cancellation).
const YIELD_BATCHES: usize = 64;

/// A stream that yields batches of data, yielding control back to the executor every `YIELD_BATCHES` batches
///
/// This can be useful to allow operators that might not yield to check for cancellation
pub struct YieldStream {
    inner: SendableRecordBatchStream,
    batches_processed: usize,
}

impl YieldStream {
    pub fn new(inner: SendableRecordBatchStream) -> Self {
        Self {
            inner,
            batches_processed: 0,
        }
    }
}

// Stream<Item = Result<RecordBatch>> to poll_next_unpin
impl Stream for YieldStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.batches_processed >= YIELD_BATCHES {
            self.batches_processed = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.batches_processed += 1;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Pending => {
                self.batches_processed = 0;
                Poll::Pending
            }

            other => other,
        }
    }
}

// RecordBatchStream schema()
impl RecordBatchStream for YieldStream {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

#[derive(Debug)]
pub struct YieldStreamExec {
    child: Arc<dyn ExecutionPlan>,

    properties: PlanProperties,
}

impl YieldStreamExec {
    pub fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        let properties = child.properties().clone();
        Self { child, properties }
    }
}

impl DisplayAs for YieldStreamExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "YieldStreamExec child={}", self.child.name())
    }
}

impl ExecutionPlan for YieldStreamExec {
    fn name(&self) -> &str {
        "YieldStreamExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.child.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Use Arc::clone on children[0] rather than calling clone() directly
        Ok(Arc::new(YieldStreamExec::new(Arc::clone(&children[0]))))
    }

    fn execute(
        &self,
        partition: usize,
        task_ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, Arc::clone(&task_ctx))?;
        let yield_stream = YieldStream::new(child_stream);
        Ok(Box::pin(yield_stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_none() {
            self.child.partition_statistics(partition)
        } else {
            Ok(Statistics::new_unknown(&self.schema()))
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // YieldStreamExec does not change the order of the input data
        self.child.maintains_input_order()
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::RecordBatchStreamAdapter;
    use arrow_schema::SchemaRef;
    use futures::{stream, StreamExt};

    /// Helper: construct a SendableRecordBatchStream containing `n` empty batches
    fn make_empty_batches(n: usize) -> SendableRecordBatchStream {
        let schema: SchemaRef = Arc::new(Schema::empty());
        let schema_for_stream = Arc::clone(&schema);

        let s =
            stream::iter((0..n).map(move |_| {
                Ok(RecordBatch::new_empty(Arc::clone(&schema_for_stream)))
            }));

        Box::pin(RecordBatchStreamAdapter::new(schema, s))
    }

    #[tokio::test]
    async fn yield_less_than_threshold() -> Result<()> {
        let count = YIELD_BATCHES - 10;
        let inner = make_empty_batches(count);
        let out: Vec<_> = YieldStream::new(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_equal_to_threshold() -> Result<()> {
        let count = YIELD_BATCHES;
        let inner = make_empty_batches(count);
        let out: Vec<_> = YieldStream::new(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_more_than_threshold() -> Result<()> {
        let count = YIELD_BATCHES + 20;
        let inner = make_empty_batches(count);
        let out: Vec<_> = YieldStream::new(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }
}
