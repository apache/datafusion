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

//! [`Task`], [`TaskWaker`], and [`ExecutionResults`] — the scheduling unit
//! and the query-level harness that owns it. Ported from PR
//! apache/datafusion#2226 `task.rs`.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::channel::mpsc;
use futures::task::ArcWake;
use futures::{Stream, StreamExt, ready};
use log::{debug, trace};

use crate::plan::{PipelinePlan, RoutablePipeline};
use crate::scheduler::{Spawner, is_worker, spawn_local, spawn_local_fifo};

/// Spawn every output partition of every pipeline in `plan` as an initial
/// [`Task`]. Returns an [`ExecutionResults`] whose output partitions stream
/// the final pipeline's results back to the caller.
pub fn spawn_plan(plan: PipelinePlan, spawner: Spawner) -> ExecutionResults {
    debug!(
        "Spawning plan: {} pipelines, {} output partitions",
        plan.pipelines.len(),
        plan.output_partitions
    );

    let (senders, receivers) = (0..plan.output_partitions)
        .map(|_| mpsc::unbounded())
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let context = Arc::new(ExecutionContext {
        spawner,
        pipelines: plan.pipelines,
        schema: plan.schema,
        output: senders,
    });

    for (pipeline_idx, routable) in context.pipelines.iter().enumerate() {
        for partition in 0..routable.pipeline.output_partitions() {
            let task = Task {
                context: Arc::clone(&context),
                waker: Arc::new(TaskWaker {
                    context: Arc::downgrade(&context),
                    wake_count: AtomicUsize::new(1),
                    pipeline: pipeline_idx,
                    partition,
                }),
            };
            context.spawner.spawn(task);
        }
    }

    let streams = receivers
        .into_iter()
        .map(|receiver| ExecutionResultStream {
            receiver,
            context: Arc::clone(&context),
        })
        .collect();

    ExecutionResults {
        schema: Arc::clone(&context.schema),
        kind: ExecutionResultsKind::Scheduled(streams),
    }
}

/// One schedulable unit — an output partition of a pipeline that *may* be
/// able to make progress.
pub struct Task {
    /// Shared query state. Holds the pipeline list, output channels, and
    /// spawner used for re-enqueueing.
    pub(crate) context: Arc<ExecutionContext>,
    pub(crate) waker: Arc<TaskWaker>,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = &self.context.pipelines[self.waker.pipeline].output;
        f.debug_struct("Task")
            .field("pipeline", &self.waker.pipeline)
            .field("partition", &self.waker.partition)
            .field("output", output)
            .finish()
    }
}

impl Task {
    fn handle_error(
        &self,
        partition: usize,
        routable: &RoutablePipeline,
        error: DataFusionError,
    ) {
        // Surface the error on this partition's output channel.
        self.context.send_query_output(partition, Err(error));

        // Close the downstream link so its task observes EOS for this
        // partition. We use a best-effort empty push-error to keep the
        // API simple; the important thing is the close.
        if let Some(link) = routable.output {
            trace!(
                "Closing pipeline: {link:?}, partition: {} (error propagation)",
                self.waker.partition,
            );
            self.context.pipelines[link.pipeline]
                .pipeline
                .close(link.child, self.waker.partition);
        } else {
            // No downstream — mark this output partition finished.
            self.context.finish(partition);
        }
    }

    /// Core worker step. Calls
    /// [`Pipeline::poll_partition`](crate::pipeline::Pipeline::poll_partition)
    /// and routes the result. MUST be called on a worker thread.
    pub fn do_work(self) {
        debug_assert!(is_worker(), "Task::do_work called outside of worker pool");
        if self.context.is_cancelled() {
            return;
        }

        // Capture the wake count prior to calling poll_partition; this
        // lets us detect concurrent wake-ups and reschedule correctly.
        let wake_count = self.waker.wake_count.load(Ordering::SeqCst);

        let node = self.waker.pipeline;
        let partition = self.waker.partition;

        let waker = futures::task::waker_ref(&self.waker);
        let mut cx = Context::from_waker(&waker);

        let pipelines = &self.context.pipelines;
        let routable = &pipelines[node];
        let poll = routable.pipeline.poll_partition(&mut cx, partition);
        // Release the waker borrow so `self` can be moved below.
        let _ = waker;
        match poll {
            Poll::Ready(Some(Ok(batch))) => {
                trace!("Poll {self:?}: Ok rows={}", batch.num_rows());
                match routable.output {
                    Some(link) => {
                        let push_result = pipelines[link.pipeline]
                            .pipeline
                            .push(batch, link.child, partition);
                        if let Err(e) = push_result {
                            self.handle_error(partition, routable, e);
                            return;
                        }
                    }
                    None => self.context.send_query_output(partition, Ok(batch)),
                }

                // Reschedule ourselves AFTER routing the batch. Using FIFO
                // so freshly awoken tasks triggered by the `push` above
                // get a chance to run first.
                spawn_local_fifo(self);
            }
            Poll::Ready(Some(Err(e))) => {
                trace!("Poll {self:?}: Err {e:?}");
                self.handle_error(partition, routable, e);
            }
            Poll::Ready(None) => {
                trace!("Poll {self:?}: None");
                match routable.output {
                    Some(link) => {
                        pipelines[link.pipeline]
                            .pipeline
                            .close(link.child, partition);
                    }
                    None => self.context.finish(partition),
                }
            }
            Poll::Pending => {
                trace!("Poll {self:?}: Pending");
                // Try to reset the wake count to 0. If that fails, a wake
                // happened during poll_partition and we must reschedule.
                let reset = self.waker.wake_count.compare_exchange(
                    wake_count,
                    0,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                if reset.is_err() {
                    trace!("Wakeup during poll_partition: {self:?}");
                    spawn_local(self);
                }
            }
        }
    }
}

/// Per-query shared state — pipelines, output channels, and the spawner.
pub(crate) struct ExecutionContext {
    pub(crate) spawner: Spawner,
    pub(crate) pipelines: Vec<RoutablePipeline>,
    pub schema: SchemaRef,
    output: Vec<mpsc::UnboundedSender<Option<Result<RecordBatch>>>>,
}

impl std::fmt::Debug for ExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext")
            .field("pipelines", &self.pipelines.len())
            .field("output_partitions", &self.output.len())
            .finish()
    }
}

impl Drop for ExecutionContext {
    fn drop(&mut self) {
        debug!("ExecutionContext dropped");
    }
}

impl ExecutionContext {
    fn is_cancelled(&self) -> bool {
        self.output.iter().all(|x| x.is_closed())
    }

    fn send_query_output(&self, partition: usize, output: Result<RecordBatch>) {
        let _ = self.output[partition].unbounded_send(Some(output));
    }

    fn finish(&self, partition: usize) {
        let _ = self.output[partition].unbounded_send(None);
    }
}

/// Waker for a [`Task`]. Implements [`ArcWake`] so it can be turned into a
/// `std::task::Waker` via [`futures::task::waker_ref`]. On wake, atomically
/// bumps `wake_count` — if the value prior to the increment was 0 the task
/// is re-enqueued, otherwise the wakeup is coalesced.
pub(crate) struct TaskWaker {
    context: Weak<ExecutionContext>,
    wake_count: AtomicUsize,
    pipeline: usize,
    partition: usize,
}

impl ArcWake for TaskWaker {
    fn wake(self: Arc<Self>) {
        if self.wake_count.fetch_add(1, Ordering::SeqCst) != 0 {
            trace!("Ignoring duplicate wakeup");
            return;
        }

        if let Some(context) = self.context.upgrade() {
            let task = Task {
                context,
                waker: Arc::clone(&self),
            };
            if is_worker() {
                spawn_local(task);
            } else {
                task.context.spawner.clone().spawn(task);
            }
        } else {
            trace!("Dropped wakeup (context gone)");
        }
    }

    fn wake_by_ref(arc_self: &Arc<Self>) {
        ArcWake::wake(Arc::clone(arc_self))
    }
}

// ---------------------------------------------------------------------------
// Caller-facing result streams.
// ---------------------------------------------------------------------------

/// Results of scheduling a plan. Drop to cancel.
///
/// Two shapes:
///
/// * **Scheduled** — a [`PipelinePlan`] with ≥1 breaker cut is driving
///   execution on the scheduler's worker pool. Each output partition is
///   fed by its own mpsc channel from a `Task`.
/// * **Direct** — the plan had no breakers (no `RepartitionExec`,
///   `CoalescePartitionsExec`, or cuttable `SortExec`), so the scheduler
///   is a no-op: we hand back the raw `ExecutionPlan::execute(p)` streams
///   unchanged. Avoids all scheduler overhead for trivially simple
///   queries.
pub struct ExecutionResults {
    schema: SchemaRef,
    kind: ExecutionResultsKind,
}

enum ExecutionResultsKind {
    Scheduled(Vec<ExecutionResultStream>),
    Direct(Vec<SendableRecordBatchStream>),
}

impl ExecutionResults {
    pub(crate) fn direct(
        schema: SchemaRef,
        streams: Vec<SendableRecordBatchStream>,
    ) -> Self {
        Self {
            schema,
            kind: ExecutionResultsKind::Direct(streams),
        }
    }

    /// Merge all partitions into one [`SendableRecordBatchStream`].
    pub fn stream(self) -> SendableRecordBatchStream {
        match self.kind {
            ExecutionResultsKind::Scheduled(streams) => {
                // Each `ExecutionResultStream` already holds its own
                // `Arc<ExecutionContext>`, so the task state stays alive
                // until all partitions are drained.
                Box::pin(RecordBatchStreamAdapter::new(
                    self.schema,
                    futures::stream::select_all(streams),
                ))
            }
            ExecutionResultsKind::Direct(mut streams) => {
                if streams.len() == 1 {
                    streams.remove(0)
                } else {
                    Box::pin(RecordBatchStreamAdapter::new(
                        self.schema,
                        futures::stream::select_all(streams),
                    ))
                }
            }
        }
    }

    /// Return one [`SendableRecordBatchStream`] per output partition.
    pub fn stream_partitioned(self) -> Vec<SendableRecordBatchStream> {
        match self.kind {
            ExecutionResultsKind::Scheduled(streams) => {
                streams.into_iter().map(|s| Box::pin(s) as _).collect()
            }
            ExecutionResultsKind::Direct(streams) => streams,
        }
    }
}

/// One output partition's result stream.
struct ExecutionResultStream {
    receiver: mpsc::UnboundedReceiver<Option<Result<RecordBatch>>>,
    context: Arc<ExecutionContext>,
}

impl Stream for ExecutionResultStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let opt = ready!(self.receiver.poll_next_unpin(cx)).flatten();
        Poll::Ready(opt)
    }
}

impl RecordBatchStream for ExecutionResultStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.context.schema)
    }
}
