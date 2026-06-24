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

//! Synchronization wrapper above a pipeline-breaking operator. Holds the
//! breaker's first batch per partition so that runtime stats become
//! observable to the coordinator
//! ([`crate::runtime_optimizer::RuntimeOptimizerExec`]) before downstream
//! consumers see any data.
//!
//! Three flags govern behavior:
//! - `is_ready` (mechanical): set automatically when every input partition
//!   has produced its first poll result. Runtime stats from the breaker
//!   become derivable.
//! - `streaming_enabled` (rule-controlled proposal): reset each poll cycle
//!   by RTO — set to `is_ready` as the permissive default. Rules can
//!   flip it back to `false` to veto release. No side effects.
//! - `streaming_started` (actual emission control): only RTO flips this,
//!   via `start_streaming()`, which also wakes per-partition wakers.
//!   Once true, partition streams emit their held batches and continue
//!   forwarding.
//!
//! Coordination uses a shared [`AtomicWaker`] (`rto_waker`) populated at
//! plan time by [`crate::runtime_optimizer::RuntimeOptimizerExec`]. The
//! buffer wakes it when `is_ready` flips; the coordinator is registered
//! on it via its own `poll_next`. We use this side-channel rather than
//! `cx.waker()` because the latter is task-local — inside a spawned task
//! (e.g. one of `RepartitionExec`'s internals) it never reaches the
//! top-of-plan task.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use futures::task::AtomicWaker;
use futures::{Stream, StreamExt};

use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct PipelineBreakerBuffer {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    state: Arc<Mutex<BufferState>>,
    /// Shared with `RuntimeOptimizerExec`. Buffer wakes this whenever
    /// `is_ready` flips so the coordinator (which may live above a
    /// task-spawning operator like `RepartitionExec`) gets re-polled.
    /// Per-partition `cx.waker()` alone is insufficient — it's the
    /// local task's waker, which doesn't propagate across the spawned
    /// task boundary that `RepartitionExec` introduces.
    rto_waker: Arc<AtomicWaker>,
}

#[derive(Debug)]
struct BufferState {
    /// First batch per input partition. Absent for partitions whose input
    /// terminated empty.
    held: HashMap<usize, RecordBatch>,
    /// Partitions whose first poll has completed (with or without a batch).
    seen: HashSet<usize>,
    is_ready: bool,
    /// Rule-controllable proposal. Reset by RTO each poll cycle: set
    /// to `is_ready` as the permissive default, then rules may flip it
    /// to false to veto.
    streaming_enabled: bool,
    /// Actual emission control. Only RTO flips this via `start_streaming`.
    /// Per-partition streams check this; while false, they hold their
    /// first batch and return Pending.
    streaming_started: bool,
    num_partitions: usize,
    /// Wakers stashed by per-partition streams while awaiting
    /// `streaming_started`.
    wakers: HashMap<usize, Waker>,
}

impl PipelineBreakerBuffer {
    pub fn new(input: Arc<dyn ExecutionPlan>, rto_waker: Arc<AtomicWaker>) -> Self {
        let num_partitions = input.output_partitioning().partition_count();
        let cache = Arc::clone(input.properties());
        Self {
            input,
            cache,
            state: Arc::new(Mutex::new(BufferState {
                held: HashMap::new(),
                seen: HashSet::new(),
                is_ready: false,
                streaming_enabled: false,
                streaming_started: false,
                num_partitions,
                wakers: HashMap::new(),
            })),
            rto_waker,
        }
    }

    /// True once every input partition has produced its first poll result.
    pub fn is_ready(&self) -> bool {
        self.state.lock().unwrap().is_ready
    }

    /// Rule-controllable proposal flag. RTO resets this to `is_ready` at
    /// the start of each poll cycle as the permissive default; rules may
    /// then flip it to false to veto release this cycle. No side effects.
    pub fn streaming_enabled(&self) -> bool {
        self.state.lock().unwrap().streaming_enabled
    }

    pub fn set_streaming_enabled(&self, enabled: bool) {
        self.state.lock().unwrap().streaming_enabled = enabled;
    }

    /// True once `start_streaming` has been called. After this, per-
    /// partition streams emit their held batches and resume forwarding.
    pub fn streaming_started(&self) -> bool {
        self.state.lock().unwrap().streaming_started
    }

    /// Start actual emission: flips `streaming_started` and wakes all
    /// per-partition wakers. Idempotent. Only RTO should call this.
    pub fn start_streaming(&self) {
        let wakers = {
            let mut state = self.state.lock().unwrap();
            if state.streaming_started {
                return;
            }
            state.streaming_started = true;
            state.wakers.drain().map(|(_, w)| w).collect::<Vec<_>>()
        };
        for w in wakers {
            w.wake();
        }
    }
}

impl DisplayAs for PipelineBreakerBuffer {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PipelineBreakerBuffer")
    }
}

impl ExecutionPlan for PipelineBreakerBuffer {
    fn name(&self) -> &'static str {
        "PipelineBreakerBuffer"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            Arc::clone(&self.rto_waker),
        )))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let stream = BufferStream {
            phase: Phase::NeedFirstBatch,
            input,
            partition,
            state: Arc::clone(&self.state),
            rto_waker: Arc::clone(&self.rto_waker),
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
enum Phase {
    /// Haven't yet pulled the first batch from this input partition.
    NeedFirstBatch,
    /// First batch absorbed (or input was empty). Holding until RTO calls
    /// `start_streaming`.
    WaitForStreaming,
    /// `streaming_started` is set; emit our held batch (if any) then
    /// transition to streaming.
    EmitHeld,
    /// Pass through input batches as they arrive.
    Streaming,
}

struct BufferStream {
    phase: Phase,
    input: SendableRecordBatchStream,
    partition: usize,
    rto_waker: Arc<AtomicWaker>,
    state: Arc<Mutex<BufferState>>,
}

impl Stream for BufferStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        loop {
            match this.phase {
                Phase::NeedFirstBatch => {
                    let next = match this.input.poll_next_unpin(cx) {
                        Poll::Ready(x) => x,
                        Poll::Pending => return Poll::Pending,
                    };
                    let became_ready = {
                        let mut state = this.state.lock().unwrap();
                        state.seen.insert(this.partition);
                        match next {
                            Some(Ok(batch)) => {
                                state.held.insert(this.partition, batch);
                            }
                            Some(Err(e)) => {
                                return Poll::Ready(Some(Err(e)));
                            }
                            None => { /* empty partition; nothing to hold */ }
                        }
                        if state.seen.len() == state.num_partitions && !state.is_ready {
                            state.is_ready = true;
                            true
                        } else {
                            false
                        }
                    };
                    if became_ready {
                        // Wake the coordinator. `cx.waker()` alone is
                        // task-local — if we're inside a spawned task
                        // (e.g. one of RepartitionExec's internals), it
                        // never reaches RTO. The shared AtomicWaker is
                        // populated by RTO on each of its own polls and
                        // wakes the actual top-of-plan task.
                        this.rto_waker.wake();
                    }
                    this.phase = Phase::WaitForStreaming;
                }
                Phase::WaitForStreaming => {
                    let mut state = this.state.lock().unwrap();
                    if state.streaming_started {
                        drop(state);
                        this.phase = Phase::EmitHeld;
                        continue;
                    }
                    state.wakers.insert(this.partition, cx.waker().clone());
                    return Poll::Pending;
                }
                Phase::EmitHeld => {
                    let held = this.state.lock().unwrap().held.remove(&this.partition);
                    this.phase = Phase::Streaming;
                    if let Some(batch) = held {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    // Empty-partition case: fall through to streaming.
                }
                Phase::Streaming => {
                    return this.input.poll_next_unpin(cx);
                }
            }
        }
    }
}
