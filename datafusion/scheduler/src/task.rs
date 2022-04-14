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

use crate::{is_worker, spawn_local, spawn_local_fifo, Query};
use futures::task::ArcWake;
use log::{debug, trace};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

/// Spawns a query using the provided [`Spawner`]
pub fn spawn_query(query: Arc<Query>) {
    debug!("Spawning query: {:#?}", query);

    let spawner = query.spawner();

    for (pipeline_idx, query_pipeline) in query.pipelines().iter().enumerate() {
        for partition in 0..query_pipeline.pipeline.output_partitions() {
            spawner.spawn(Task {
                query: query.clone(),
                waker: Arc::new(TaskWaker {
                    query: Arc::downgrade(&query),
                    wake_count: AtomicUsize::new(1),
                    pipeline: pipeline_idx,
                    partition,
                }),
            });
        }
    }
}

/// A [`Task`] identifies an output partition within a given pipeline that may be able to
/// make progress. The [`Scheduler`][super::Scheduler] maintains a list of outstanding
/// [`Task`] and distributes them amongst its worker threads.
///
/// A [`Query`] is considered completed when it has no outstanding [`Task`]
pub struct Task {
    /// Maintain a link to the [`Query`] this is necessary to be able to
    /// route the output of the partition to its destination, and also because
    /// when [`Query`] is dropped it signals completion of query execution
    query: Arc<Query>,

    /// A [`ArcWake`] that can be used to re-schedule this [`Task`] for execution
    waker: Arc<TaskWaker>,
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let output = self.query.pipelines()[self.waker.pipeline].output;

        f.debug_struct("Task")
            .field("pipeline", &self.waker.pipeline)
            .field("partition", &self.waker.partition)
            .field("output", &output)
            .finish()
    }
}

impl Task {
    /// Call [`Pipeline::poll_partition`] attempting to make progress on query execution
    pub fn do_work(self) {
        assert!(is_worker(), "Task::do_work called outside of worker pool");
        if self.query.is_cancelled() {
            return;
        }

        // Capture the wake count prior to calling [`Pipeline::poll_partition`]
        // this allows us to detect concurrent wake ups and handle them correctly
        //
        // We aren't using the wake count to synchronise other memory, and so can
        // use relaxed memory ordering
        let wake_count = self.waker.wake_count.load(Ordering::Relaxed);

        let node = self.waker.pipeline;
        let partition = self.waker.partition;

        let waker = futures::task::waker_ref(&self.waker);
        let mut cx = Context::from_waker(&*waker);

        let pipelines = self.query.pipelines();
        let routable = &pipelines[node];
        match routable.pipeline.poll_partition(&mut cx, partition) {
            Poll::Ready(Some(Ok(batch))) => {
                trace!("Poll {:?}: Ok: {}", self, batch.num_rows());
                match routable.output {
                    Some(link) => {
                        trace!(
                            "Publishing batch to pipeline {:?} partition {}",
                            link,
                            partition
                        );
                        pipelines[link.pipeline]
                            .pipeline
                            .push(batch, link.child, partition)
                    }
                    None => {
                        trace!("Publishing batch to output");
                        self.query.send_query_output(Ok(batch))
                    }
                }

                // Reschedule this pipeline again
                //
                // We want to prioritise running tasks triggered by the most recent
                // batch, so reschedule with FIFO ordering
                //
                // Note: We must schedule after we have routed the batch, otherwise
                // we introduce a potential ordering race where the newly scheduled
                // task runs before this task finishes routing the output
                spawn_local_fifo(self);
            }
            Poll::Ready(Some(Err(e))) => {
                trace!("Poll {:?}: Error: {:?}", self, e);
                self.query.send_query_output(Err(e));
                if let Some(link) = routable.output {
                    trace!("Closing pipeline: {:?}, partition: {}", link, partition);
                    pipelines[link.pipeline]
                        .pipeline
                        .close(link.child, partition)
                }
            }
            Poll::Ready(None) => {
                trace!("Poll {:?}: None", self);
                if let Some(link) = routable.output {
                    trace!("Closing pipeline: {:?}, partition: {}", link, partition);
                    pipelines[link.pipeline]
                        .pipeline
                        .close(link.child, partition)
                }
            }
            Poll::Pending => {
                trace!("Poll {:?}: Pending", self);
                // Attempt to reset the wake count with the value obtained prior
                // to calling [`Pipeline::poll_partition`].
                //
                // If this fails it indicates a wakeup was received whilst executing
                // [`Pipeline::poll_partition`] and we should reschedule the task
                let reset = self.waker.wake_count.compare_exchange(
                    wake_count,
                    0,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );

                if reset.is_err() {
                    trace!("Wakeup triggered whilst polling: {:?}", self);
                    spawn_local(self);
                }
            }
        }
    }
}

struct TaskWaker {
    /// Store a weak reference to the query to avoid reference cycles if this
    /// [`Waker`] is stored within a [`Pipeline`] owned by the [`Query`]
    query: Weak<Query>,

    /// A counter that stores the number of times this has been awoken
    ///
    /// A value > 0, implies the task is either in the ready queue or
    /// currently being executed
    ///
    /// `TaskWaker::wake` always increments the `wake_count`, however, it only
    /// re-enqueues the [`Task`] if the value prior to increment was 0
    ///
    /// This ensures that a given [`Task`] is not enqueued multiple times
    ///
    /// We store an integer, as opposed to a boolean, so that wake ups that
    /// occur during [`Pipeline::poll_partition`] can be detected and handled
    /// after it has finished executing
    ///
    wake_count: AtomicUsize,

    /// The index of the pipeline within `query` to poll
    pipeline: usize,

    /// The partition of the pipeline within `query` to poll
    partition: usize,
}

impl ArcWake for TaskWaker {
    fn wake(self: Arc<Self>) {
        if self.wake_count.fetch_add(1, Ordering::Relaxed) != 0 {
            trace!("Ignoring duplicate wakeup");
            return;
        }

        if let Some(query) = self.query.upgrade() {
            let task = Task {
                query,
                waker: self.clone(),
            };

            trace!("Wakeup {:?}", task);

            // If called from a worker, spawn to the current worker's
            // local queue, otherwise reschedule on any worker
            match crate::is_worker() {
                true => spawn_local(task),
                false => task.query.spawner().clone().spawn(task),
            }
        } else {
            trace!("Dropped wakeup");
        }
    }

    fn wake_by_ref(s: &Arc<Self>) {
        ArcWake::wake(s.clone())
    }
}
