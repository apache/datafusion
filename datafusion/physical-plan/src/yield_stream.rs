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

use crate::execution_plan::CardinalityEffect::{self, Equal};
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use datafusion_common::{internal_err, Result, Statistics};
use datafusion_execution::TaskContext;

use futures::{Stream, StreamExt};

/// An identity stream that passes batches through as is, but yields control
/// back to the runtime every `period` batches. This stream is useful to
/// construct a mechanism that allows operators that do not directly cooperate
/// with the runtime to  check/support cancellation.
pub struct YieldStream {
    inner: SendableRecordBatchStream,
    batches_processed: usize,
    period: usize,
}

impl YieldStream {
    pub fn new(inner: SendableRecordBatchStream, mut period: usize) -> Self {
        if period == 0 {
            period = usize::MAX;
        }
        Self {
            inner,
            batches_processed: 0,
            period,
        }
    }
}

impl Stream for YieldStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.batches_processed >= self.period {
            self.batches_processed = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let value = self.inner.poll_next_unpin(cx);
        match value {
            Poll::Ready(Some(Ok(_))) => {
                self.batches_processed += 1;
            }
            Poll::Pending => {
                self.batches_processed = 0;
            }
            _ => {}
        }
        value
    }
}

impl RecordBatchStream for YieldStream {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

/// This operator wraps any other execution plan and to "adapt" it to cooperate
/// with the runtime by yielding control back to the runtime every `frequency`
/// batches. This is useful for operators that do not natively support yielding
/// control, allowing them to be used in a runtime that requires yielding for
/// cancellation or other purposes.
///
/// # Note
/// If your ExecutionPlan periodically yields control back to the scheduler
/// implement [`ExecutionPlan::with_cooperative_yields`] to avoid the need for this
/// node.
#[derive(Debug)]
pub struct YieldStreamExec {
    /// The child execution plan that this operator "wraps" to make it
    /// cooperate with the runtime.
    child: Arc<dyn ExecutionPlan>,
    /// The frequency at which the operator yields control back to the runtime.
    frequency: usize,
}

impl YieldStreamExec {
    /// Create a new `YieldStreamExec` operator that wraps the given child
    /// execution plan and yields control back to the runtime every `frequency`
    /// batches.
    pub fn new(child: Arc<dyn ExecutionPlan>, frequency: usize) -> Self {
        Self { frequency, child }
    }

    /// Returns the child execution plan this operator "wraps" to make it
    /// cooperate with the runtime.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.child
    }

    /// Returns the period at which the operator yields control back to the
    /// runtime.
    pub fn yield_period(&self) -> usize {
        self.frequency
    }
}

impl DisplayAs for YieldStreamExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "YieldStreamExec frequency={}", self.frequency)
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
        self.child.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("YieldStreamExec requires exactly one child");
        }
        Ok(Arc::new(YieldStreamExec::new(
            children.swap_remove(0),
            self.frequency,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        task_ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, task_ctx)?;
        let yield_stream = YieldStream::new(child_stream, self.frequency);
        Ok(Box::pin(yield_stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.child.partition_statistics(partition)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.child.maintains_input_order()
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        Equal
    }
}

/// Wraps `stream` inside a `YieldStream` depending on the `cooperative` flag.
/// Yielding period is extracted from `context`.
pub fn wrap_yield_stream(
    mut stream: SendableRecordBatchStream,
    context: &TaskContext,
    cooperative: bool,
) -> SendableRecordBatchStream {
    if cooperative {
        let period = context.session_config().options().optimizer.yield_period;
        if period > 0 {
            stream = Box::pin(YieldStream::new(stream, period));
        }
    }
    stream
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::RecordBatchStreamAdapter;

    use arrow_schema::SchemaRef;

    use futures::{stream, StreamExt};

    // Frequency testing:
    // Number of batches to yield before yielding control back to the executor
    const YIELD_BATCHES: usize = 64;

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
        let out = YieldStream::new(inner, YIELD_BATCHES)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_equal_to_threshold() -> Result<()> {
        let count = YIELD_BATCHES;
        let inner = make_empty_batches(count);
        let out = YieldStream::new(inner, YIELD_BATCHES)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_more_than_threshold() -> Result<()> {
        let count = YIELD_BATCHES + 20;
        let inner = make_empty_batches(count);
        let out = YieldStream::new(inner, YIELD_BATCHES)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(out.len(), count);
        Ok(())
    }
}
