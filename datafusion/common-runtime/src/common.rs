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

use futures::{Stream, StreamExt};
use std::sync::Arc;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::task::{JoinError, JoinHandle};

use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};

use crate::trace_utils::{trace_block, trace_future};

/// Helper that  provides a simple API to spawn a single task and join it.
/// Provides guarantees of aborting on `Drop` to keep it cancel-safe.
/// Note that if the task was spawned with `spawn_blocking`, it will only be
/// aborted if it hasn't started yet.
///
/// Technically, it's just a wrapper of a `JoinHandle` overriding drop.
#[derive(Debug)]
pub struct SpawnedTask<R> {
    inner: JoinHandle<R>,
}

impl<R: 'static> SpawnedTask<R> {
    pub fn spawn<T>(task: T) -> Self
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn here as SpawnedTask handles aborting/cancelling the task on Drop
        #[allow(clippy::disallowed_methods)]
        let inner = tokio::task::spawn(trace_future(task));
        Self { inner }
    }

    pub fn spawn_blocking<T>(task: T) -> Self
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn_blocking here as SpawnedTask handles aborting/cancelling the task on Drop
        #[allow(clippy::disallowed_methods)]
        let inner = tokio::task::spawn_blocking(trace_block(task));
        Self { inner }
    }

    /// Joins the task, returning the result of join (`Result<R, JoinError>`).
    /// Same as awaiting the spawned task, but left for backwards compatibility.
    pub async fn join(self) -> Result<R, JoinError> {
        self.await
    }

    /// Joins the task and unwinds the panic if it happens.
    pub async fn join_unwind(self) -> Result<R, JoinError> {
        self.await.map_err(|e| {
            // `JoinError` can be caused either by panic or cancellation. We have to handle panics:
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                // Cancellation may be caused by two reasons:
                // 1. Abort is called, but since we consumed `self`, it's not our case (`JoinHandle` not accessible outside).
                // 2. The runtime is shutting down.
                log::warn!("SpawnedTask was polled during shutdown");
                e
            }
        })
    }
}

impl<R> Future for SpawnedTask<R> {
    type Output = Result<R, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<R> Drop for SpawnedTask<R> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

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
    buffer: Option<Result<RecordBatch>>,
}

impl YieldStream {
    pub fn new(inner: SendableRecordBatchStream) -> Self {
        Self {
            inner,
            batches_processed: 0,
            buffer: None,
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
        let this = &mut *self;

        if let Some(batch) = this.buffer.take() {
            return Poll::Ready(Some(batch));
        }

        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                this.batches_processed += 1;
                if this.batches_processed >= YIELD_BATCHES {
                    this.batches_processed = 0;
                    // We need to buffer the batch when we return Poll::Pending,
                    // so that we can return it on the next poll.
                    // Otherwise, the next poll will miss the batch and return None.
                    this.buffer = Some(Ok(batch));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(Some(Ok(batch)))
                }
            }
            other => other,
        }
    }
}

// RecordBatchStream schema()
impl RecordBatchStream for YieldStream {
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::{pending, Pending};

    use tokio::{runtime::Runtime, sync::oneshot};

    use arrow::datatypes::SchemaRef;
    use arrow_schema::Schema;
    use datafusion_common::Result;
    use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
    use futures::{stream, Stream, StreamExt, TryStreamExt};
    use std::{
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    #[tokio::test]
    async fn runtime_shutdown() {
        let rt = Runtime::new().unwrap();
        #[allow(clippy::async_yields_async)]
        let task = rt
            .spawn(async {
                SpawnedTask::spawn(async {
                    let fut: Pending<()> = pending();
                    fut.await;
                    unreachable!("should never return");
                })
            })
            .await
            .unwrap();

        // caller shutdown their DF runtime (e.g. timeout, error in caller, etc)
        rt.shutdown_background();

        // race condition
        // poll occurs during shutdown (buffered stream poll calls, etc)
        assert!(matches!(
            task.join_unwind().await,
            Err(e) if e.is_cancelled()
        ));
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn panic_resume() {
        // this should panic w/o an `unwrap`
        SpawnedTask::spawn(async { panic!("foo") })
            .join_unwind()
            .await
            .ok();
    }

    #[tokio::test]
    async fn cancel_not_started_task() {
        let (sender, receiver) = oneshot::channel::<i32>();
        let task = SpawnedTask::spawn(async {
            // Shouldn't be reached.
            sender.send(42).unwrap();
        });

        drop(task);

        // If the task was cancelled, the sender was also dropped,
        // and awaiting the receiver should result in an error.
        assert!(receiver.await.is_err());
    }

    #[tokio::test]
    async fn cancel_ongoing_task() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let task = SpawnedTask::spawn(async move {
            sender.send(1).await.unwrap();
            // This line will never be reached because the channel has a buffer
            // of 1.
            sender.send(2).await.unwrap();
        });
        // Let the task start.
        assert_eq!(receiver.recv().await.unwrap(), 1);
        drop(task);

        // The sender was dropped so we receive `None`.
        assert!(receiver.recv().await.is_none());
    }

    /// A tiny adapter that turns any `Stream<Item = Result<RecordBatch>>`
    /// into a `RecordBatchStream` by carrying along a schema.
    struct EmptyBatchStream {
        inner: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
        schema: SchemaRef,
    }

    impl Stream for EmptyBatchStream {
        type Item = Result<RecordBatch>;
        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }

    impl RecordBatchStream for EmptyBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    /// Helper: construct a SendableRecordBatchStream containing `n` empty batches
    fn make_empty_batches(n: usize) -> SendableRecordBatchStream {
        let schema: SchemaRef = Arc::new(Schema::empty());
        let schema_for_stream = Arc::clone(&schema);

        let s =
            stream::iter((0..n).map(move |_| {
                Ok(RecordBatch::new_empty(Arc::clone(&schema_for_stream)))
            }))
            .boxed();

        Box::pin(EmptyBatchStream { inner: s, schema })
    }

    #[tokio::test]
    async fn yield_less_than_threshold() -> Result<()> {
        let count = YIELD_BATCHES - 10;
        let inner = make_empty_batches(count);
        let out: Vec<_> = YieldStream::new(inner).try_collect::<Vec<_>>().await?;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_equal_to_threshold() -> Result<()> {
        let count = YIELD_BATCHES;
        let inner = make_empty_batches(count);
        let out: Vec<_> = YieldStream::new(inner).try_collect::<Vec<_>>().await?;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_more_than_threshold() -> Result<()> {
        let count = YIELD_BATCHES + 20;
        let inner = make_empty_batches(count);
        let out: Vec<_> = YieldStream::new(inner).try_collect::<Vec<_>>().await?;
        assert_eq!(out.len(), count);
        Ok(())
    }
}
