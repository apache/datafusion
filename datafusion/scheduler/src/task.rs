use crate::{spawn_local, Query, Spawner};
use futures::task::ArcWake;
use log::{debug, trace};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

/// Spawns a query using the provided [`Spawner`]
pub fn spawn_query(spawner: Spawner, query: Arc<Query>) {
    debug!("Spawning query: {:#?}", query);

    for (pipeline_idx, query_pipeline) in query.pipelines().iter().enumerate() {
        for partition in 0..query_pipeline.pipeline.output_partitions() {
            spawner.spawn(Task {
                query: query.clone(),
                waker: Arc::new(TaskWaker {
                    query: Arc::downgrade(&query),
                    pipeline: pipeline_idx,
                    partition,
                }),
            })
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
        f.debug_struct("Task")
            .field("pipeline", &self.waker.pipeline)
            .field("partition", &self.waker.partition)
            .finish()
    }
}

impl Task {
    /// Call [`Pipeline::poll_partition`] attempting to make progress on query execution
    pub fn do_work(self) {
        if self.query.is_cancelled() {
            return;
        }

        let node = self.waker.pipeline;
        let partition = self.waker.partition;

        let waker = futures::task::waker_ref(&self.waker);
        let mut cx = Context::from_waker(&*waker);

        let pipelines = self.query.pipelines();
        let routable = &pipelines[node];
        match routable.pipeline.poll_partition(&mut cx, partition) {
            Poll::Ready(Some(Ok(batch))) => {
                trace!("Poll {:?}: Ok: {}", self, batch.num_rows());

                // Reschedule this pipeline again
                //
                // Tasks are scheduled in a LIFO fashion, so spawn this before
                // routing the batch, as routing may trigger a wakeup, and allow
                // us to process the batch immediately
                spawn_local(Self {
                    query: self.query.clone(),
                    waker: self.waker.clone(),
                });

                match routable.output {
                    Some(link) => {
                        trace!(
                            "Published batch to node {:?} partition {}",
                            link,
                            partition
                        );
                        pipelines[link.pipeline]
                            .pipeline
                            .push(batch, link.child, partition)
                    }
                    None => {
                        trace!("Published batch to output");
                        self.query.send_query_output(Ok(batch))
                    }
                }
            }
            Poll::Ready(Some(Err(e))) => {
                trace!("Poll {:?}: Error: {:?}", self, e);
                self.query.send_query_output(Err(e));
                if let Some(link) = routable.output {
                    pipelines[link.pipeline]
                        .pipeline
                        .close(link.child, partition)
                }
            }
            Poll::Ready(None) => {
                trace!("Poll {:?}: None", self);
                if let Some(link) = routable.output {
                    pipelines[link.pipeline]
                        .pipeline
                        .close(link.child, partition)
                }
            }
            Poll::Pending => trace!("Poll {:?}: Pending", self),
        }
    }
}

struct TaskWaker {
    /// Store a weak reference to the query to avoid reference cycles if this
    /// [`Waker`] is stored within a [`Pipeline`] owned by the [`Query`]
    query: Weak<Query>,

    /// The index of the pipeline within `query` to poll
    pipeline: usize,

    /// The partition of the pipeline within `query` to poll
    partition: usize,
}

impl ArcWake for TaskWaker {
    fn wake(self: Arc<Self>) {
        if let Some(query) = self.query.upgrade() {
            let item = Task {
                query,
                waker: self.clone(),
            };
            trace!("Wakeup {:?}", item);
            spawn_local(item)
        } else {
            trace!("Dropped wakeup");
        }
    }

    fn wake_by_ref(s: &Arc<Self>) {
        ArcWake::wake(s.clone())
    }
}
