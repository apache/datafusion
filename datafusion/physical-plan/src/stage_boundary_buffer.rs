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

//! Stage-boundary buffer between an input subtree and its consumer. The
//! boundary IS a pipeline breaker: it drains its input fully via tasks
//! spawned by [`StageBoundaryBuffer::prime`], stages the result, and
//! releases to the consumer only when
//! [`crate::runtime_optimizer::RuntimeOptimizerExec`] (RTO) decides.
//! Each boundary carries a `stage` number assigned by the optimizer at
//! insertion time so RTO and EXPLAIN can talk about stages explicitly.
//!
//! Lifecycle, separating fill from drain:
//! - `prime(ctx)` (called by RTO): spawns one drain task per input
//!   partition; each pulls until EOF, ferrying batches through an
//!   unbounded mpsc per partition. Idempotent.
//! - `execute(p, _)` (called by the consumer): hands back the
//!   per-partition receiver wrapped in a gated stream. Never touches
//!   `input.execute`; that's prime's job.
//!
//! Three flags govern release:
//! - `is_ready` (mechanical): set automatically when every drain task
//!   has hit EOF. Runtime stats from the input become derivable.
//! - `streaming_enabled` (rule-controlled proposal): reset each poll
//!   cycle by RTO — set to `is_ready` as the permissive default. Rules
//!   can flip it to false to veto release. No side effects.
//! - `streaming_started` (actual emission control): only RTO flips this,
//!   via `start_streaming()`. Once true, the gated consumer streams
//!   start forwarding from their receivers.
//!
//! Coordination uses a shared [`AtomicWaker`] (`rto_waker`) populated at
//! plan time by RTO. The buffer wakes it when `is_ready` flips; the
//! coordinator is registered on it via its own `poll_next`. Side-channel
//! instead of `cx.waker()` because the latter is task-local — inside a
//! spawned task (e.g. one of `RepartitionExec`'s internals) it never
//! reaches the top-of-plan task.
//!
//! Memory: every primed buffer holds its full input until release.
//! Spill is a follow-up; OomGuard catches genuine OOM in the meantime.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use futures::task::AtomicWaker;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;

use crate::statistics::StatisticsArgs;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream, Statistics,
};

// TODO(spill): the per-partition channels below currently hold the full
// materialized input in memory. A follow-up will swap each channel for a
// spillable buffer (writing to disk under MemoryPool pressure) so that
// large stage boundaries don't OOM. OomGuard is the safety net until then.
type PartitionTxs = Vec<Option<mpsc::UnboundedSender<Result<RecordBatch>>>>;
type PartitionRxs = Vec<Option<mpsc::UnboundedReceiver<Result<RecordBatch>>>>;

#[derive(Debug)]
pub struct StageBoundaryBuffer {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    /// Stage number assigned by the inserting optimizer rule. Lowest
    /// stage runs first; once released, its consumers (next stage up)
    /// can prime. Used by EXPLAIN, logs, and (eventually) RTO ordering.
    stage: usize,
    /// Each buffer owns its own AtomicWaker rather than sharing one
    /// across the subtree. RTO registers the consumer-task waker on
    /// this per poll cycle (cheap walk); the drain task wakes here
    /// once `is_ready` flips. Per-buffer wakers decouple the buffer
    /// from RTO at planning time — `InsertStageBoundariesAtBreakers`
    /// and `InsertRuntimeOptimizer` no longer have to coordinate to
    /// share a single waker.
    rto_waker: Arc<AtomicWaker>,
    /// Gate flags + drain progress + consumer-side wakers.
    state: Arc<Mutex<BufferState>>,
    /// Per-partition sender. Moved out (`Option::take`) into the drain
    /// task at `prime()`. Independent from `rxs` because `prime` and
    /// `execute` arrive in unspecified order.
    txs: Arc<Mutex<PartitionTxs>>,
    /// Per-partition receiver. Moved out into the consumer stream at
    /// `execute()`.
    rxs: Arc<Mutex<PartitionRxs>>,
    /// Handles to spawned drain tasks. Auto-abort on Drop via
    /// `SpawnedTask::Drop`, so query cancellation cleans up cleanly.
    drain_tasks: Arc<Mutex<Vec<SpawnedTask<()>>>>,
}

#[derive(Debug)]
struct BufferState {
    /// Partitions whose drain task reached EOF (success or error). When
    /// `drained_count == num_partitions`, `is_ready` flips.
    drained_count: usize,
    is_ready: bool,
    /// Rule-controllable proposal. Reset by RTO each poll cycle: set to
    /// `is_ready` as the permissive default, then rules may flip it
    /// to false to veto.
    streaming_enabled: bool,
    /// Actual emission control. Only RTO flips this via `start_streaming`.
    streaming_started: bool,
    num_partitions: usize,
    /// Wakers stashed by consumer streams while gated on `streaming_started`.
    wakers: HashMap<usize, Waker>,
}

impl StageBoundaryBuffer {
    pub fn new(input: Arc<dyn ExecutionPlan>, stage: usize) -> Self {
        let num_partitions = input.output_partitioning().partition_count();
        let cache = Arc::clone(input.properties());
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..num_partitions)
            .map(|_| {
                let (tx, rx) = mpsc::unbounded_channel();
                (Some(tx), Some(rx))
            })
            .unzip();
        Self {
            input,
            cache,
            stage,
            rto_waker: Arc::new(AtomicWaker::new()),
            state: Arc::new(Mutex::new(BufferState {
                drained_count: 0,
                is_ready: false,
                streaming_enabled: false,
                streaming_started: false,
                num_partitions,
                wakers: HashMap::new(),
            })),
            txs: Arc::new(Mutex::new(txs)),
            rxs: Arc::new(Mutex::new(rxs)),
            drain_tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn stage(&self) -> usize {
        self.stage
    }

    /// Register a waker to be woken when this buffer's drain reaches EOF
    /// (i.e. `is_ready` flips). RTO calls this per `poll_next` for each
    /// buffer in its subtree, threading its own consumer waker through.
    pub fn register_consumer_waker(&self, waker: &Waker) {
        self.rto_waker.register(waker);
    }

    /// Spawn drain tasks for every input partition. Each task pulls
    /// `input.execute(p, ctx)` to EOF, ferrying batches and errors
    /// through the per-partition channel; once EOF is reached, marks
    /// the partition drained and wakes RTO if this was the last one.
    /// Idempotent. Only RTO should call this.
    pub fn prime(&self, ctx: &Arc<TaskContext>) -> Result<()> {
        let mut tasks = self.drain_tasks.lock().unwrap();
        if !tasks.is_empty() {
            return Ok(());
        }
        let mut txs = self.txs.lock().unwrap();
        for (partition, slot) in txs.iter_mut().enumerate() {
            let Some(tx) = slot.take() else {
                continue;
            };
            let stream = self.input.execute(partition, Arc::clone(ctx))?;
            let state = Arc::clone(&self.state);
            let rto_waker = Arc::clone(&self.rto_waker);
            tasks.push(SpawnedTask::spawn(drain_partition(
                partition, stream, tx, state, rto_waker,
            )));
        }
        Ok(())
    }

    /// True once every drain task has reached EOF.
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

    /// True once `start_streaming` has been called.
    pub fn streaming_started(&self) -> bool {
        self.state.lock().unwrap().streaming_started
    }

    /// Start actual emission: flips `streaming_started` and wakes all
    /// per-partition consumer wakers. Idempotent. Only RTO should call this.
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

async fn drain_partition(
    partition: usize,
    mut stream: SendableRecordBatchStream,
    tx: mpsc::UnboundedSender<Result<RecordBatch>>,
    state: Arc<Mutex<BufferState>>,
    rto_waker: Arc<AtomicWaker>,
) {
    while let Some(item) = stream.next().await {
        // tx.send returning Err means the consumer dropped its receiver
        // (query cancelled while we were mid-drain). Stop pulling.
        let is_err = item.is_err();
        if tx.send(item).is_err() {
            break;
        }
        if is_err {
            break;
        }
    }
    let became_ready = {
        let mut state = state.lock().unwrap();
        state.drained_count += 1;
        if state.drained_count == state.num_partitions && !state.is_ready {
            state.is_ready = true;
            true
        } else {
            false
        }
    };
    if became_ready {
        // cx.waker() inside this spawned task is task-local and won't
        // reach the top-of-plan task. The shared AtomicWaker bridges.
        rto_waker.wake();
    }
    let _ = partition; // reserved for future per-partition diagnostics
}

impl Drop for StageBoundaryBuffer {
    fn drop(&mut self) {
        // SpawnedTask aborts on Drop. Clearing the Vec triggers that
        // for every drain task. Safe to ignore poisoned lock — at Drop
        // time we just want to release resources.
        if let Ok(mut tasks) = self.drain_tasks.lock() {
            tasks.clear();
        }
    }
}

impl DisplayAs for StageBoundaryBuffer {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "StageBoundaryBuffer: stage={}", self.stage)
    }
}

impl ExecutionPlan for StageBoundaryBuffer {
    fn name(&self) -> &'static str {
        "StageBoundaryBuffer"
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
        // Fresh waker is fine: with_new_children is called at planning
        // time before RTO registers anything, so there's nothing to
        // preserve.
        Ok(Arc::new(Self::new(children.swap_remove(0), self.stage)))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    /// Gated passthrough: rules only see runtime stats once every drain
    /// task has reached EOF (signalled by `is_ready`). Before that the
    /// child's per-partition counts are partial / not meaningful for
    /// adaptive decisions.
    fn runtime_row_count(&self, partition: usize) -> Option<usize> {
        if !self.is_ready() {
            return None;
        }
        self.input.runtime_row_count(partition)
    }

    /// Passthrough: the buffer doesn't change row counts or column stats.
    /// Without this override, the default impl at execution_plan.rs:528
    /// returns Statistics::new_unknown — so wrapping any subtree in a
    /// buffer would blackhole the static stats `side_runtime_rows`
    /// falls back to when runtime stats aren't yet available.
    fn statistics_with_args(&self, args: &StatisticsArgs) -> Result<Arc<Statistics>> {
        args.compute_child_statistics(&self.input, args.partition())
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Take ownership of this partition's receiver. prime() owns the
        // sender side; we just gate-and-forward here.
        let rx = self
            .rxs
            .lock()
            .unwrap()
            .get_mut(partition)
            .and_then(|slot| slot.take())
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "StageBoundaryBuffer::execute called twice (or partition \
                     {partition} out of range)"
                ))
            })?;
        let stream = ConsumerStream {
            partition,
            rx,
            state: Arc::clone(&self.state),
        };
        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

struct ConsumerStream {
    partition: usize,
    rx: mpsc::UnboundedReceiver<Result<RecordBatch>>,
    state: Arc<Mutex<BufferState>>,
}

impl Stream for ConsumerStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        // Gate: don't emit anything until RTO releases the boundary.
        {
            let mut state = this.state.lock().unwrap();
            if !state.streaming_started {
                state.wakers.insert(this.partition, cx.waker().clone());
                return Poll::Pending;
            }
        }
        this.rx.poll_recv(cx)
    }
}
