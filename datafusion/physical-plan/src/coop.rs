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

use crate::execution_plan::SchedulingType;
use crate::stream::RecordBatchStreamAdapter;
use futures::{FutureExt, Stream};
use pin_project_lite::pin_project;

pin_project! {
    /// An identity stream that passes batches through as is, but consumes cooperative
    /// scheduling budget per returned [`RecordBatch`](RecordBatch).
    pub struct CooperativeStream<T>
    where
        T: RecordBatchStream,
    {
        #[pin]
        inner: T,
    }
}

impl<T> CooperativeStream<T>
where
    T: RecordBatchStream,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Stream for CooperativeStream<T>
where
    T: RecordBatchStream,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // TODO replace with this implementation when possible
        // See https://github.com/tokio-rs/tokio/issues/7403
        // let coop = ready!(tokio::task::coop::poll_proceed(cx));
        // let value = self.project().inner.poll_next(cx);
        // if value.is_ready() {
        //     coop.made_progress();
        // }
        // value

        if !tokio::task::coop::has_budget_remaining() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let value = self.project().inner.poll_next(cx);
        if value.is_ready() {
            // This is a temporary placeholder implementation
            let mut budget = Box::pin(tokio::task::coop::consume_budget());
            let _ = budget.poll_unpin(cx);
        }
        value
    }
}

impl<T> RecordBatchStream for CooperativeStream<T>
where
    T: RecordBatchStream,
{
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

/// This execution plan is a decorator for other execution plans that wraps the `Stream` created
/// by an execution plan using the [`make_cooperative`] function.
#[derive(Debug)]
pub struct CooperativeExec {
    /// The child execution plan that this operator "wraps" to make it
    /// cooperate with the runtime.
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl CooperativeExec {
    /// Creates a new `CooperativeExec` operator that wraps the given child
    /// execution plan.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = input
            .properties()
            .clone()
            .with_scheduling_type(SchedulingType::Cooperative);

        Self { input, properties }
    }

    /// Returns the child execution plan this operator "wraps" to make it
    /// cooperate with the runtime.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for CooperativeExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "CooperativeExec")
    }
}

impl ExecutionPlan for CooperativeExec {
    fn name(&self) -> &str {
        "CooperativeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.input.maintains_input_order()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("CooperativeExec requires exactly one child");
        }
        Ok(Arc::new(CooperativeExec::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        task_ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.input.execute(partition, task_ctx)?;
        Ok(make_cooperative(child_stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        Equal
    }
}

/// Create a cooperative wrapper around the given [`RecordBatchStream`].
///
pub fn cooperative<T>(stream: T) -> CooperativeStream<T>
where
    T: RecordBatchStream + Send + 'static,
{
    CooperativeStream::new(stream)
}

/// Wraps a `SendableRecordBatchStream` inside a `CooperativeStream`.
/// Since this function takes a dynamic `RecordBatchStream` the implementation
/// can only delegate to the given stream using a virtual function call.
/// You can use the generic function [`cooperative`] to avoid this.
pub fn make_cooperative(stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
    // TODO is there a more elegant way to overload cooperative
    Box::pin(cooperative(RecordBatchStreamAdapter::new(
        stream.schema(),
        stream,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::RecordBatchStreamAdapter;

    use arrow_schema::SchemaRef;

    use futures::{stream, StreamExt};

    // This is the hardcoded value Tokio uses
    const TASK_BUDGET: usize = 128;

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
        let count = TASK_BUDGET - 10;
        let inner = make_empty_batches(count);
        let out = make_cooperative(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_equal_to_threshold() -> Result<()> {
        let count = TASK_BUDGET;
        let inner = make_empty_batches(count);
        let out = make_cooperative(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_more_than_threshold() -> Result<()> {
        let count = TASK_BUDGET + 20;
        let inner = make_empty_batches(count);
        let out = make_cooperative(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }
}
