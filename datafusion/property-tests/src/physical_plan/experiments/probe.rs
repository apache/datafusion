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

//! [`StreamProbe`]: records what happens to the streams it observes.

use std::cell::RefCell;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};

static NEXT_PROBE_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Ids of the probes whose streams are being polled on this thread,
    /// innermost last
    static POLLING: RefCell<Vec<u64>> = const { RefCell::new(vec![]) };
}

/// Marks a probe's stream as being polled on this thread until dropped
struct PollingGuard;

impl PollingGuard {
    fn enter(id: u64) -> Self {
        POLLING.with(|polling| polling.borrow_mut().push(id));
        Self
    }
}

impl Drop for PollingGuard {
    fn drop(&mut self) {
        POLLING.with(|polling| polling.borrow_mut().pop());
    }
}

/// What happened to the streams of one partition in a stream experiment.
/// Counts cover every stream created for the partition.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionObservation {
    /// Number of streams created for the partition
    pub streams_created: usize,
    /// Number of streams dropped
    pub streams_dropped: usize,
    /// Number of calls to `poll_next` that did not happen while the node's own
    /// output was being polled on the same thread, for example polls from a
    /// spawned task or from `execute`. Only counted for the node's inputs.
    pub polls_without_demand: usize,
    /// Number of rows returned
    pub rows: usize,
    /// Number of errors returned
    pub errors: usize,
}

impl PartitionObservation {
    /// Number of streams created and not yet dropped
    pub fn streams_alive(&self) -> usize {
        self.streams_created.saturating_sub(self.streams_dropped)
    }
}

/// Records what happens to the streams it observes. A probe is a handle to
/// shared state, so clones record into, and read from, the same
/// observations.
///
/// A probe created with [`Self::with_consumer`] also counts polls that are not
/// driven by the consumer: polls that happen while no stream observed by the
/// consumer probe is being polled on the same thread.
#[derive(Debug, Clone)]
pub(crate) struct StreamProbe {
    id: u64,
    consumer: Option<u64>,
    partitions: Arc<Mutex<Vec<PartitionObservation>>>,
}

impl StreamProbe {
    pub(crate) fn new() -> Self {
        Self {
            id: NEXT_PROBE_ID.fetch_add(1, Ordering::Relaxed),
            consumer: None,
            partitions: Arc::default(),
        }
    }

    /// A probe that also counts polls not driven by the streams that
    /// `consumer` observes
    pub(crate) fn with_consumer(consumer: &StreamProbe) -> Self {
        Self {
            consumer: Some(consumer.id),
            ..Self::new()
        }
    }

    /// Wrap `stream`, the stream of `partition`, so that it records into this
    /// probe
    pub(crate) fn observe(
        &self,
        partition: usize,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        self.record(partition, |observation| observation.streams_created += 1);
        Box::pin(ObservedStream {
            inner: stream,
            probe: self.clone(),
            partition,
        })
    }

    /// Observations of at least `count` partitions, padded with empty
    /// observations for partitions without streams
    pub(crate) fn partitions(&self, count: usize) -> Vec<PartitionObservation> {
        let mut partitions = self.lock().clone();
        if partitions.len() < count {
            partitions.resize_with(count, PartitionObservation::default);
        }
        partitions
    }

    /// Number of streams created and not yet dropped, over all partitions
    pub(crate) fn streams_alive(&self) -> usize {
        self.lock()
            .iter()
            .map(PartitionObservation::streams_alive)
            .sum()
    }

    fn lock(&self) -> MutexGuard<'_, Vec<PartitionObservation>> {
        // Observations stay usable even if a stream panicked while recording
        self.partitions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record(&self, partition: usize, f: impl FnOnce(&mut PartitionObservation)) {
        let mut partitions = self.lock();
        if partitions.len() <= partition {
            partitions.resize_with(partition + 1, PartitionObservation::default);
        }
        f(&mut partitions[partition]);
    }
}

/// A stream that records what happens to it in a [`StreamProbe`]
struct ObservedStream {
    inner: SendableRecordBatchStream,
    probe: StreamProbe,
    partition: usize,
}

impl Stream for ObservedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let demanded = self.probe.consumer.is_none_or(|consumer| {
            POLLING.with(|polling| polling.borrow().contains(&consumer))
        });
        if !demanded {
            self.probe.record(self.partition, |observation| {
                observation.polls_without_demand += 1
            });
        }
        let poll = {
            let _guard = PollingGuard::enter(self.probe.id);
            self.inner.poll_next_unpin(cx)
        };
        match &poll {
            Poll::Ready(Some(Ok(batch))) => {
                let rows = batch.num_rows();
                self.probe
                    .record(self.partition, |observation| observation.rows += rows);
            }
            Poll::Ready(Some(Err(_))) => {
                self.probe
                    .record(self.partition, |observation| observation.errors += 1);
            }
            Poll::Ready(None) | Poll::Pending => {}
        }
        poll
    }
}

impl RecordBatchStream for ObservedStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Drop for ObservedStream {
    fn drop(&mut self) {
        self.probe.record(self.partition, |observation| {
            observation.streams_dropped += 1
        });
    }
}
