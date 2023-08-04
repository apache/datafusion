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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::physical_plan::common::{AbortOnDropMany, AbortOnDropSingle};
use crate::physical_plan::displayable;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use futures::stream::BoxStream;
use futures::{Future, SinkExt, Stream, StreamExt};
use log::debug;
use pin_project_lite::pin_project;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::{JoinHandle, JoinSet};

use super::metrics::BaselineMetrics;
use super::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};

/// Builder for [`RecordBatchReceiverStream`] that propagates errors
/// and panic's correctly.
///
/// [`RecordBatchReceiverStream`] is used to spawn one or more tasks
/// that produce `RecordBatch`es and send them to a single
/// `Receiver` which can improve parallelism.
///
/// This also handles propagating panic`s and canceling the tasks.
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
    ///
    /// If the input partition produces an error, the error will be
    /// sent to the output stream and no further results are sent.
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
                    // If send fails, the plan being torn down, there
                    // is no place to send the error and no reason to continue.
                    output.send(Err(e)).await.ok();
                    debug!(
                        "Stopping execution: error executing input: {}",
                        displayable(input.as_ref()).one_line()
                    );
                    return;
                }
                Ok(stream) => stream,
            };

            // Transfer batches from inner stream to the output tx
            // immediately.
            while let Some(item) = stream.next().await {
                let is_err = item.is_err();

                // If send fails, plan being torn down, there is no
                // place to send the error and no reason to continue.
                if output.send(item).await.is_err() {
                    debug!(
                        "Stopping execution: output is gone, plan cancelling: {}",
                        displayable(input.as_ref()).one_line()
                    );
                    return;
                }

                // stop after the first error is encontered (don't
                // drive all streams to completion)
                if is_err {
                    debug!(
                        "Stopping execution: plan returned error: {}",
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

        // future that checks the result of the join set, and propagates panic if seen
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
                            // This should only occur if the task is
                            // cancelled, which would only occur if
                            // the JoinSet were aborted, which in turn
                            // would imply that the receiver has been
                            // dropped and this code is not running
                            return Some(Err(DataFusionError::Internal(format!(
                                "Non Panic Task error: {e}"
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

        // Convert the receiver into a stream
        let rx_stream = futures::stream::unfold(rx, |mut rx| async move {
            let next_item = rx.recv().await;
            next_item.map(|next_item| (next_item, rx))
        });

        // Merge the streams together so whichever is ready first
        // produces the batch
        let inner = futures::stream::select(rx_stream, check_stream).boxed();

        Box::pin(RecordBatchReceiverStream { schema, inner })
    }
}

/// This object is used to construct multiple broadcast streams from a single
/// `SendableRecordBatchStream`. It broadcasts the stream by spawning new tokio
/// tasks, potentially improving parallelism. This struct also manages task
/// cancellations and error propagation correctly.
///
/// Construct a new instance using `Self::new` or `Self::with_capacity`.
pub struct RecordBatchBroadcastStreamsBuilder {
    txs: Vec<futures::channel::mpsc::Sender<Result<RecordBatch>>>,
    rxs: Vec<futures::channel::mpsc::Receiver<Result<RecordBatch>>>,
    schema: SchemaRef,
}

impl RecordBatchBroadcastStreamsBuilder {
    /// Constructs a new `RecordBatchBroadcastStreamsBuilder` with the provided
    /// schema and a specified number of broadcast channels. Each channel has a
    /// default capacity of 100.
    pub fn new(schema: SchemaRef, n_broadcast: usize) -> Self {
        let capacity = 100;
        let (txs, rxs) = (0..n_broadcast)
            .map(|_| futures::channel::mpsc::channel(capacity))
            .unzip();
        Self { txs, rxs, schema }
    }

    /// Constructs a new `RecordBatchBroadcastStreamsBuilder` with the provided
    /// schema, a specified number of broadcast channels, and the specified
    /// channel capacity.
    pub fn with_capacity(schema: SchemaRef, n_broadcast: usize, capacity: usize) -> Self {
        let (txs, rxs) = (0..n_broadcast)
            .map(|_| futures::channel::mpsc::channel(capacity))
            .unzip();
        Self { txs, rxs, schema }
    }

    /// Broadcasts an `input` stream to multiple output streams.
    ///
    /// This function creates a new task to pull data from the input and sends it
    /// to the output partitions based on the desired partitioning. It also waits
    /// for the input-consuming task to complete in a separate task, and propagates
    /// any errors (including panics) to all output channels.
    ///
    /// # Arguments
    ///
    /// * `input` - The input execution plan.
    /// * `partition` - The partition to execute the plan on.
    /// * `context` - The task context for the execution.
    ///
    /// # Returns
    ///
    /// This function returns a tuple consisting of:
    /// * A vector of `SendableRecordBatchStream`s, with each stream representing
    ///   an individual output stream,
    /// * An `AbortHelper` to manage the underlying tasks. It's crucial to retain
    ///   this `AbortHelper` and prevent it from being dropped inadvertently.
    ///   Dropping the `AbortHelper` will cease all communication by aborting
    ///   all associated tasks.
    pub fn broadcast(
        self,
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<SendableRecordBatchStream>, Arc<AbortOnDropMany<()>>)> {
        let stream = input.execute(partition, context)?;
        assert_eq!(self.schema, input.schema());

        // Create a task to pull data from the input stream and send to output channels
        let input_task: JoinHandle<Result<()>> =
            tokio::spawn(Self::pull_from_input(stream, self.txs.clone()));

        // Create a task to wait for the input-consuming task to complete
        let join_handle = vec![tokio::spawn(Self::wait_for_task(
            AbortOnDropSingle::new(input_task),
            self.txs,
        ))];

        let abort_helper = Arc::new(AbortOnDropMany(join_handle));

        let streams = self
            .rxs
            .into_iter()
            .map(|rx| {
                Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), rx))
                    as SendableRecordBatchStream
            })
            .collect();

        Ok((streams, abort_helper))
    }

    /// Pulls data from the specified input plan and feeds it to output
    /// partitions based on the desired partitioning.
    ///
    /// The argument `txs` denotes the target channels for each output partition.
    async fn pull_from_input(
        mut stream: SendableRecordBatchStream,
        mut txs: Vec<futures::channel::mpsc::Sender<Result<RecordBatch>>>,
    ) -> Result<()> {
        while let Some(item) = stream.next().await {
            // Clone the item for each broadcast channel
            if let Ok(item) = &item {
                for channel in txs.iter_mut() {
                    channel
                        .send(Ok(item.clone()))
                        .await
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                }
            }
        }
        Ok(())
    }

    /// Waits for `input_task`, which is consuming one of the inputs to complete.
    /// Upon each successful completion, sends a `None` to each of the output tx
    /// channels to signal one of the inputs is done. Upon error, propagates the
    /// errors to all output tx channels.
    async fn wait_for_task(
        input_task: AbortOnDropSingle<Result<()>>,
        mut txs: Vec<futures::channel::mpsc::Sender<Result<RecordBatch>>>,
    ) {
        // Wait for completion and propagate error. Note that we ignore errors
        // on send (.ok), as this means the receiver has already shut down.
        match input_task.await {
            // Error in joining task
            Err(e) => {
                let e = Arc::new(e);

                for tx in txs.iter_mut() {
                    let err = Err(DataFusionError::Context(
                        "Broadcast Error".to_string(),
                        Box::new(DataFusionError::External(Box::new(Arc::clone(&e)))),
                    ));
                    tx.send(err).await.ok();
                }
            }
            // Error from running input task
            Ok(Err(e)) => {
                let e = Arc::new(e);

                for tx in txs.iter_mut() {
                    // Wrap it, because we need to send errors to all output partitions
                    let err = Err(DataFusionError::External(Box::new(e.clone())));
                    tx.send(err).await.ok();
                }
            }
            // Input task completed successfully
            Ok(Ok(())) => {}
        }
    }
}

/// A [`SendableRecordBatchStream`] that combines [`RecordBatch`]es from multiple inputs,
/// on new tokio Tasks,  increasing the potential parallelism.
///
/// This structure also handles propagating panics and cancelling the
/// underlying tasks correctly.
///
/// Use [`Self::builder`] to construct one.
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
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// Stream wrapper that records `BaselineMetrics` for a particular
/// `[SendableRecordBatchStream]` (likely a partition)
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
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    use crate::test::exec::{
        assert_strong_count_converges_to_zero, BlockingExec, MockExec, PanicExec,
    };

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]))
    }

    #[tokio::test]
    #[should_panic(expected = "PanickingStream did panic")]
    async fn record_batch_receiver_stream_propagates_panics() {
        let schema = schema();

        let num_partitions = 10;
        let input = PanicExec::new(schema.clone(), num_partitions);
        consume(input, 10).await
    }

    #[tokio::test]
    #[should_panic(expected = "PanickingStream did panic: 1")]
    async fn record_batch_receiver_stream_propagates_panics_early_shutdown() {
        let schema = schema();

        // make 2 partitions, second partition panics before the first
        let num_partitions = 2;
        let input = PanicExec::new(schema.clone(), num_partitions)
            .with_partition_panic(0, 10)
            .with_partition_panic(1, 3); // partition 1 should panic first (after 3 )

        // ensure that the panic results in an early shutdown (that
        // everything stops after the first panic).

        // Since the stream reads every other batch: (0,1,0,1,0,panic)
        // so should not exceed 5 batches prior to the panic
        let max_batches = 5;
        consume(input, max_batches).await
    }

    #[tokio::test]
    async fn record_batch_receiver_stream_drop_cancel() {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = schema();

        // Make an input that never proceeds
        let input = BlockingExec::new(schema.clone(), 1);
        let refs = input.refs();

        // Configure a RecordBatchReceiverStream to consume the input
        let mut builder = RecordBatchReceiverStream::builder(schema, 2);
        builder.run_input(Arc::new(input), 0, task_ctx.clone());
        let stream = builder.build();

        // input should still be present
        assert!(std::sync::Weak::strong_count(&refs) > 0);

        // drop the stream, ensure the refs go to zero
        drop(stream);
        assert_strong_count_converges_to_zero(refs).await;
    }

    #[tokio::test]
    /// Ensure that if an error is received in one stream, the
    /// `RecordBatchReceiverStream` stops early and does not drive
    /// other streams to completion.
    async fn record_batch_receiver_stream_error_does_not_drive_completion() {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = schema();

        // make an input that will error twice
        let error_stream = MockExec::new(
            vec![
                Err(DataFusionError::Execution("Test1".to_string())),
                Err(DataFusionError::Execution("Test2".to_string())),
            ],
            schema.clone(),
        )
        .with_use_task(false);

        let mut builder = RecordBatchReceiverStream::builder(schema, 2);
        builder.run_input(Arc::new(error_stream), 0, task_ctx.clone());
        let mut stream = builder.build();

        // get the first result, which should be an error
        let first_batch = stream.next().await.unwrap();
        let first_err = first_batch.unwrap_err();
        assert_eq!(first_err.to_string(), "Execution error: Test1");

        // There should be no more batches produced (should not get the second error)
        assert!(stream.next().await.is_none());
    }

    /// Consumes all the input's partitions into a
    /// RecordBatchReceiverStream and runs it to completion
    ///
    /// panic's if more than max_batches is seen,
    async fn consume(input: PanicExec, max_batches: usize) {
        let task_ctx = Arc::new(TaskContext::default());

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
        let mut num_batches = 0;
        while let Some(next) = stream.next().await {
            next.unwrap();
            num_batches += 1;
            assert!(
                num_batches < max_batches,
                "Got the limit of {num_batches} batches before seeing panic"
            );
        }
    }
}
