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

//! Stream wrappers for physical operators

use std::sync::Arc;

use crate::error::Result;
use crate::physical_plan::displayable;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use futures::stream::BoxStream;
use futures::{Future, Stream, StreamExt};
use log::debug;
use pin_project_lite::pin_project;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;

use super::metrics::BaselineMetrics;
use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};

/// Builder for [`RecordBatchReceiverStream`] that propagates errors
/// and panic's correctly.
///
/// [`RecordBatchReceiverStream`] can be used when there are one or
/// more tasks spawned which produce RecordBatches and send them to a
/// single `Receiver`.
pub struct RecordBatchReceiverStreamBuilder {
    tx: Sender<Result<RecordBatch>>,
    rx: Receiver<Result<RecordBatch>>,
    schema: SchemaRef,
    join_set: JoinSet<()>,
}

impl RecordBatchReceiverStreamBuilder {
    /// create new channels with the specified buffer size
    pub fn new(schema: SchemaRef, capacity: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);

        Self {
            tx,
            rx,
            schema,
            join_set: JoinSet::new(),
        }
    }

    /// Get a handle for sending [`RecordBatch`]es to the output
    pub fn tx(&self) -> Sender<Result<RecordBatch>> {
        self.tx.clone()
    }

    /// Spawn task that will be aborted if this builder (or the stream
    /// built from it) are dropped
    ///
    /// this is often used to spawn tasks that write to the sender
    /// retrieved from `Self::tx`
    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = ()>,
        F: Send + 'static,
    {
        self.join_set.spawn(task);
    }

    /// Spawn a blocking task that will be aborted if this builder (or the stream
    /// built from it) are dropped
    ///
    /// this is often used to spawn tasks that write to the sender
    /// retrieved from `Self::tx`
    pub fn spawn_blocking<F>(&mut self, f: F)
    where
        F: FnOnce(),
        F: Send + 'static,
    {
        self.join_set.spawn_blocking(f);
    }

    /// runs the input_partition of the `input` ExecutionPlan on the
    /// tokio threadpool and writes its outputs to this stream
    pub(crate) fn run_input(
        &mut self,
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
    ) {
        let output = self.tx();

        self.spawn(async move {
            let mut stream = match input.execute(partition, context) {
                Err(e) => {
                    // If send fails, plan being torn down,
                    // there is no place to send the error.
                    output.send(Err(e)).await.ok();
                    debug!(
                        "Stopping execution: error executing input: {}",
                        displayable(input.as_ref()).one_line()
                    );
                    return;
                }
                Ok(stream) => stream,
            };

            while let Some(item) = stream.next().await {
                // If send fails, plan being torn down,
                // there is no place to send the error.
                if output.send(item).await.is_err() {
                    debug!(
                        "Stopping execution: output is gone, plan cancelling: {}",
                        displayable(input.as_ref()).one_line()
                    );
                    return;
                }
            }
        });
    }

    /// Create a stream of all `RecordBatch`es written to `tx`
    pub fn build(self) -> SendableRecordBatchStream {
        let Self {
            tx,
            rx,
            schema,
            mut join_set,
        } = self;

        // don't need tx
        drop(tx);

        // future that checks the result of the join set
        let check = async move {
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(()) => continue, // nothing to report
                    // This means a tokio task error, likely a panic
                    Err(e) => {
                        if e.is_panic() {
                            // resume on the main thread
                            std::panic::resume_unwind(e.into_panic());
                        } else {
                            return Some(Err(DataFusionError::Execution(format!(
                                "Task error: {e}"
                            ))));
                        }
                    }
                }
            }
            None
        };

        let check_stream = futures::stream::once(check)
            // unwrap Option / only return the error
            .filter_map(|item| async move { item });

        let inner = ReceiverStream::new(rx).chain(check_stream).boxed();

        Box::pin(RecordBatchReceiverStream { schema, inner })
    }
}

/// Adapter for a tokio [`ReceiverStream`] that implements the
/// [`SendableRecordBatchStream`] interface and propagates panics and
/// errors.  Use [`Self::builder`] to construct one.
pub struct RecordBatchReceiverStream {
    schema: SchemaRef,
    inner: BoxStream<'static, Result<RecordBatch>>,
}

impl RecordBatchReceiverStream {
    /// Create a builder with an internal buffer of capacity batches.
    pub fn builder(
        schema: SchemaRef,
        capacity: usize,
    ) -> RecordBatchReceiverStreamBuilder {
        RecordBatchReceiverStreamBuilder::new(schema, capacity)
    }
}

impl Stream for RecordBatchReceiverStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for RecordBatchReceiverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pin_project! {
    /// Combines a [`Stream`] with a [`SchemaRef`] implementing
    /// [`RecordBatchStream`] for the combination
    pub struct RecordBatchStreamAdapter<S> {
        schema: SchemaRef,

        #[pin]
        stream: S,
    }
}

impl<S> RecordBatchStreamAdapter<S> {
    /// Creates a new [`RecordBatchStreamAdapter`] from the provided schema and stream
    pub fn new(schema: SchemaRef, stream: S) -> Self {
        Self { schema, stream }
    }
}

impl<S> std::fmt::Debug for RecordBatchStreamAdapter<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordBatchStreamAdapter")
            .field("schema", &self.schema)
            .finish()
    }
}

impl<S> Stream for RecordBatchStreamAdapter<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> RecordBatchStream for RecordBatchStreamAdapter<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Stream wrapper that records `BaselineMetrics` for a particular
/// partition
pub(crate) struct ObservedStream {
    inner: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl ObservedStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        Self {
            inner,
            baseline_metrics,
        }
    }
}

impl RecordBatchStream for ObservedStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for ObservedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    use crate::{execution::context::SessionContext, test::exec::PanicingExec};

    #[tokio::test]
    #[should_panic(expected = "PanickingStream did panic")]
    async fn record_batch_receiver_stream_propagates_panics() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let num_partitions = 10;
        let input = PanicingExec::new(schema.clone(), num_partitions);
        consume(input).await
    }

    #[tokio::test]
    #[should_panic(expected = "PanickingStream did panic: 1")]
    async fn record_batch_receiver_stream_propagates_panics_one() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        // make 2 partitions, second panics before the first
        let num_partitions = 2;
        let input = PanicingExec::new(schema.clone(), num_partitions)
            .with_partition_panic(0, 10)
            .with_partition_panic(1, 3); // partition 1 should panic first (after 3 )

        consume(input).await
    }

    /// Consumes all the input's partitions into a
    /// RecordBatchReceiverStream and runs it to completion
    async fn consume(input: PanicingExec) {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let input = Arc::new(input);
        let num_partitions = input.output_partitioning().partition_count();

        // Configure a RecordBatchReceiverStream to consume all the input partitions
        let mut builder =
            RecordBatchReceiverStream::builder(input.schema(), num_partitions);
        for partition in 0..num_partitions {
            builder.run_input(input.clone(), partition, task_ctx.clone());
        }
        let mut stream = builder.build();

        // drain the stream until it is complete, panic'ing on error
        while let Some(next) = stream.next().await {
            next.unwrap();
        }
    }
}
