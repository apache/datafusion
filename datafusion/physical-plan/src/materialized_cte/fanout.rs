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

//! The streaming fan-out of one body partition of a materialized CTE to the
//! scans that read it.
//!
//! A [`Fanout`] has a log of batches, and one cursor for each reader. A
//! spawned task, the producer, pulls the body partition and appends to the
//! log. A reader reads the log at its cursor. The log drops (retires) the
//! batches that no reader can read again.
//!
//! The producer appends a batch only when the fastest active reader is less
//! than [`WINDOW`] batches behind the end of the log. While a spill file is
//! open, the batches take no memory, and the lead can be [`SPILL_WINDOW`]
//! batches. With no active reader, it reads ahead [`WINDOW`] batches. A reader that is slow, or that is not
//! polled, never stops the producer. Its unread batches stay in the log, in
//! memory or in spill files. See the module documentation of
//! `materialized_cte` for why this cannot deadlock.
//!
//! # Spill
//!
//! The log is a sequence of chunks:
//!
//! - `Memory`: batches in memory. Only the last chunk is a `Memory` chunk.
//! - `Spilling`: batches in memory that the producer writes to a spill file.
//!   Readers read them from memory. When the file is complete, the chunk
//!   becomes a `Disk` chunk, and its memory is released.
//! - `Open`: batches that the producer wrote to a spill file that is not
//!   complete. Readers cannot read them yet. A reader that reaches an `Open`
//!   chunk asks the producer to complete the file.
//! - `Disk`: batches in a complete spill file.
//!
//! The producer writes spill files without the lock of the fan-out, so
//! readers can read while it writes. Only the producer changes the chunks
//! from `Memory` to `Spilling`, from `Spilling` to `Disk` and from `Open` to
//! `Disk`, so the positions of the batches do not change.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use arrow::record_batch::RecordBatch;
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion_execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use futures::{FutureExt, Stream, StreamExt};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::common::panic_message;
use crate::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, SpillMetrics, Time,
};
use crate::spill::in_progress_spill_file::InProgressSpillFile;
use crate::spill::spill_manager::SpillManager;
use crate::{ExecutionPlan, ExecutionPlanProperties};

/// The number of batches that the producer can append ahead of the fastest
/// active reader.
pub(super) const WINDOW: usize = 4;

/// A batch larger than this is written to a spill file in pieces of about
/// this size, so that a reader needs less memory to decode it.
const SPILL_PIECE_BYTES: usize = 2 * 1024 * 1024;

/// While the producer writes to a spill file, it can append this many
/// batches ahead of the fastest active reader. The batches are on disk, so
/// they take no memory, and a larger file means fewer files.
const SPILL_WINDOW: usize = 64;

/// An `Open` spill file is completed when it has this many bytes.
const SPILL_FILE_BYTES: usize = 64 * 1024 * 1024;

/// The metrics of the fan-outs of one CTE in one run.
#[derive(Clone)]
pub(super) struct FanoutMetrics {
    buffered_rows: Count,
    retired_batches: Count,
    /// The peak of `buffered_bytes`. It is not named `peak_mem_used`,
    /// because it counts only the batches of the logs, not the memory of
    /// the readers that decode spill files.
    peak_buffered_bytes: Gauge,
    /// The bytes in memory in all fan-outs of the CTE.
    buffered_bytes: Arc<AtomicUsize>,
}

impl FanoutMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            buffered_rows: MetricBuilder::new(metrics).global_counter("buffered_rows"),
            retired_batches: MetricBuilder::new(metrics)
                .global_counter("retired_batches"),
            peak_buffered_bytes: MetricBuilder::new(metrics)
                .global_gauge("peak_buffered_bytes"),
            buffered_bytes: Arc::default(),
        }
    }

    fn grow(&self, bytes: usize) {
        let total = self.buffered_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak_buffered_bytes.set_max(total);
    }

    fn shrink(&self, bytes: usize) {
        self.buffered_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Cursor {
    /// No reader has this slot yet. A scan partition that is executed later
    /// can read from position 0.
    NotReached,
    /// A reader has this slot, but has not polled. It reads from position 0.
    Claimed,
    /// The absolute position of the next batch that the reader reads.
    Active(usize),
    /// The reader finished, failed, or was dropped.
    Finished,
}

/// A complete spill file of the log.
#[derive(Clone)]
struct DiskChunk {
    start: usize,
    len: usize,
    file: Arc<dyn SpillFile>,
    max_piece_size: usize,
    /// The piece count of each batch in the file.
    pieces: Arc<Vec<usize>>,
}

/// A part of the log. See the module documentation.
enum Chunk {
    Memory {
        start: usize,
        batches: VecDeque<RecordBatch>,
    },
    Spilling {
        start: usize,
        batches: Arc<Vec<RecordBatch>>,
    },
    Open {
        start: usize,
        len: usize,
    },
    Disk(DiskChunk),
}

impl Chunk {
    fn start(&self) -> usize {
        match self {
            Chunk::Memory { start, .. }
            | Chunk::Spilling { start, .. }
            | Chunk::Open { start, .. } => *start,
            Chunk::Disk(disk) => disk.start,
        }
    }

    fn end(&self) -> usize {
        match self {
            Chunk::Memory { start, batches } => start + batches.len(),
            Chunk::Spilling { start, batches } => start + batches.len(),
            Chunk::Open { start, len } => start + len,
            Chunk::Disk(disk) => disk.start + disk.len,
        }
    }

    /// The memory that the chunk holds.
    fn memory(&self) -> usize {
        match self {
            Chunk::Memory { batches, .. } => {
                batches.iter().map(RecordBatch::get_array_memory_size).sum()
            }
            Chunk::Spilling { batches, .. } => {
                batches.iter().map(RecordBatch::get_array_memory_size).sum()
            }
            Chunk::Open { .. } | Chunk::Disk(_) => 0,
        }
    }
}

/// What a reader finds at its cursor.
enum Read {
    Batch(RecordBatch),
    Disk(DiskChunk),
    /// The batch is in an `Open` spill file.
    NeedFile,
    End(Option<Arc<DataFusionError>>),
    Wait,
}

/// The parts of a fan-out that hold the body, to drop without the lock.
#[derive(Default)]
struct Stopped {
    _producer: Option<SpawnedTask<()>>,
    _pending: Option<SendableRecordBatchStream>,
}

struct State {
    /// The absolute position of the first batch in `chunks`.
    base: usize,
    /// The absolute position after the last batch in `chunks`.
    head: usize,
    chunks: VecDeque<Chunk>,
    /// The stream of the body partition until the first reader polls. Then
    /// the producer owns it.
    pending: Option<SendableRecordBatchStream>,
    done: bool,
    /// The producer stopped before the end of the body, because no reader
    /// could read. The log is not complete.
    aborted: bool,
    error: Option<Arc<DataFusionError>>,
    cursors: Vec<Cursor>,
    reader_wakers: Vec<Option<Waker>>,
    producer_waker: Option<Waker>,
    producer: Option<SpawnedTask<()>>,
    /// The holds. While a hold exists, no batch is retired.
    holds: usize,
    /// Set when the output is dropped. The producer stops.
    shut: bool,
    /// A reader waits for the `Open` spill file.
    file_requested: bool,
    /// Holds the batches of the `Memory` and `Spilling` chunks.
    reservation: MemoryReservation,
    /// The memory size of the last batch, as an estimate of the next one.
    last_batch_size: usize,
}

impl State {
    /// Whether the producer may append a batch.
    ///
    /// It must append when the fastest active reader is at the head, because
    /// that reader waits. Even if the batch then goes to disk.
    ///
    /// Else it reads
    /// ahead only a few batches, and only while the next batch fits in memory
    /// under `memory_cap`, so that readers that read in step do not spill.
    /// While a spill file is open (`writing`), the batches take no memory, and
    /// the producer can read further ahead. Thus the producer waits only
    /// while every active reader has a batch to read.
    fn may_append(&self, writing: bool, memory_cap: usize) -> bool {
        let fastest = self
            .cursors
            .iter()
            .filter_map(|c| match c {
                Cursor::Active(p) => Some(*p),
                _ => None,
            })
            .max();
        if fastest == Some(self.head) {
            return true;
        }
        let lead = self.head - fastest.unwrap_or(self.base);
        if writing {
            return fastest.is_some() && lead < SPILL_WINDOW;
        }
        lead < WINDOW && self.reservation.size() + self.last_batch_size <= memory_cap
    }

    /// Whether no reader can read the log again.
    fn all_finished(&self) -> bool {
        self.holds == 0 && self.cursors.iter().all(|c| *c == Cursor::Finished)
    }

    /// Return the producer and the body stream that it did not start, to
    /// drop, if no reader can read again. To drop them aborts the body. A
    /// body stream that is kept can stop the other partitions of a body that
    /// couples them. Drop them without the lock.
    fn stop_if_finished(&mut self) -> Stopped {
        if !self.all_finished() {
            return Stopped::default();
        }
        if !self.done {
            self.aborted = true;
        }
        Stopped {
            _producer: self.producer.take(),
            _pending: self.pending.take(),
        }
    }

    /// Drop the batches below the slowest reader that can still read.
    fn retire(&mut self, metrics: &FanoutMetrics) {
        if self.holds > 0 {
            return;
        }
        let min = self
            .cursors
            .iter()
            .filter_map(|c| match c {
                Cursor::Active(p) => Some(*p),
                Cursor::NotReached | Cursor::Claimed => Some(0),
                Cursor::Finished => None,
            })
            .min()
            .unwrap_or(self.head);
        while let Some(chunk) = self.chunks.front_mut() {
            if chunk.end() <= min {
                let memory = chunk.memory();
                self.reservation.shrink(memory);
                metrics.shrink(memory);
                metrics.retired_batches.add(chunk.end() - chunk.start());
                self.chunks.pop_front();
                continue;
            }
            if let Chunk::Memory { start, batches } = chunk {
                while *start < min {
                    let Some(batch) = batches.pop_front() else {
                        break;
                    };
                    let memory = batch.get_array_memory_size();
                    self.reservation.shrink(memory);
                    metrics.shrink(memory);
                    metrics.retired_batches.add(1);
                    *start += 1;
                }
            }
            break;
        }
        self.base = self.chunks.front().map_or(self.head, Chunk::start);
    }

    fn read(&self, pos: usize) -> Result<Read> {
        if self.shut {
            return Err(internal_datafusion_err!(
                "MaterializedCte: the output was released before the read"
            ));
        }
        // An error ends every reader, also a reader whose next batch is in an
        // `Open` spill file that will not be completed.
        if self.done && self.error.is_some() {
            return Ok(Read::End(self.error.clone()));
        }
        if pos >= self.head {
            if self.done {
                return Ok(Read::End(self.error.clone()));
            }
            return Ok(Read::Wait);
        }
        let Some(chunk) = self
            .chunks
            .iter()
            .find(|c| c.start() <= pos && pos < c.end())
        else {
            return Err(internal_datafusion_err!(
                "MaterializedCte: the batch at {pos} was retired before it was read"
            ));
        };
        Ok(match chunk {
            Chunk::Memory { start, batches } => Read::Batch(batches[pos - start].clone()),
            Chunk::Spilling { start, batches } => {
                Read::Batch(batches[pos - start].clone())
            }
            Chunk::Open { .. } if self.done => {
                return Err(internal_datafusion_err!(
                    "MaterializedCte: a spill file was not completed"
                ));
            }
            Chunk::Open { .. } => Read::NeedFile,
            Chunk::Disk(disk) => Read::Disk(disk.clone()),
        })
    }

    fn wake_producer(&mut self) {
        if let Some(w) = self.producer_waker.take() {
            w.wake();
        }
    }

    fn wake_readers(&mut self) {
        for w in self.reader_wakers.iter_mut().filter_map(Option::take) {
            w.wake();
        }
    }
}

/// The shared fan-out of one body partition.
pub(super) struct Fanout {
    name: String,
    pool: Arc<dyn MemoryPool>,
    /// The most memory that the log keeps before it spills. It leaves the
    /// rest of the pool to the other operators, which may not spill.
    memory_cap: usize,
    /// The reader slots that each scan has. Slots after these are for scan
    /// partitions that are executed again.
    scans: usize,
    spill_manager: SpillManager,
    metrics: FanoutMetrics,
    producer_wait_time: Time,
    /// Wakes the producer while it waits for the body, when a reader needs
    /// the `Open` spill file.
    file_request: Notify,
    state: Mutex<State>,
}

/// The parameters of the fan-outs of one CTE output.
pub(super) struct FanoutOptions {
    pub name: String,
    pub body: Arc<dyn ExecutionPlan>,
    pub scans: usize,
    pub metrics: ExecutionPlanMetricsSet,
}

impl Fanout {
    pub(super) fn new(
        options: &FanoutOptions,
        partition: usize,
        context: &Arc<TaskContext>,
        metrics: FanoutMetrics,
    ) -> Arc<Self> {
        let name = format!("MaterializedCte[{}][{partition}]", options.name);
        let partitions = options.body.output_partitioning().partition_count();
        let reservation = MemoryConsumer::new(name.clone())
            .with_can_spill(true)
            .register(context.memory_pool());
        let spill_manager = SpillManager::new(
            context.runtime_env(),
            SpillMetrics::new(&options.metrics, partition),
            options.body.schema(),
        )
        .with_compression_type(context.session_config().spill_compression());
        // Execute the body partition now, while the context is at hand. The
        // fan-out keeps no reference to the context: it is in the `RunState`
        // of the context, so that would be a cycle.
        let (pending, error) = match options.body.execute(partition, Arc::clone(context))
        {
            Ok(stream) => (Some(stream), None),
            Err(e) => (None, Some(Arc::new(e))),
        };
        Arc::new(Self {
            name,
            pool: Arc::clone(context.memory_pool()),
            memory_cap: match context.memory_pool().memory_limit() {
                MemoryLimit::Finite(limit) => limit / (2 * partitions.max(1)),
                MemoryLimit::Infinite | MemoryLimit::Unknown => usize::MAX,
            },
            scans: options.scans,
            spill_manager,
            metrics,
            producer_wait_time: MetricBuilder::new(&options.metrics)
                .subset_time("producer_wait_time", partition),
            file_request: Notify::new(),
            state: Mutex::new(State {
                base: 0,
                head: 0,
                chunks: VecDeque::new(),
                done: error.is_some(),
                pending,
                aborted: false,
                error,
                cursors: vec![Cursor::NotReached; options.scans],
                reader_wakers: vec![None; options.scans],
                producer_waker: None,
                producer: None,
                holds: 0,
                shut: false,
                file_requested: false,
                last_batch_size: 0,
                reservation,
            }),
        })
    }

    /// Start the producer, which pulls the body partition, if it did not
    /// start. Called by the first reader that polls.
    fn start(self: &Arc<Self>, state: &mut State) {
        if state.shut {
            return;
        }
        if let Some(stream) = state.pending.take() {
            let task = SpawnedTask::spawn(produce(Arc::clone(self), stream));
            state.producer = Some(task);
        }
    }

    /// Register a reader for reader slot `slot`. The reader does not read
    /// until it is polled, but the log keeps every batch for it.
    ///
    /// A slot that was used before gets a new cursor if no batch was retired.
    /// Else this returns `None`: the batches are gone, and the caller must
    /// compute the body again. This is also the case when the producer
    /// stopped before the end of the body.
    pub(super) fn open(self: &Arc<Self>, slot: usize) -> Option<FanoutReader> {
        let mut state = self.state.lock();
        if state.shut || (state.aborted && !state.done) {
            return None;
        }
        // A scan that was bound after the output was created has no slot of
        // its own. It gets an extra slot, as a second execution does.
        let slot = if self.is_free_scan_slot(&state, slot) {
            slot
        } else if state.base == 0 {
            // Reuse a finished extra slot, so that the slots do not grow
            // with each execution under a hold.
            let reuse = (self.scans..state.cursors.len())
                .find(|&s| state.cursors[s] == Cursor::Finished);
            match reuse {
                Some(slot) => slot,
                None => {
                    state.cursors.push(Cursor::NotReached);
                    state.reader_wakers.push(None);
                    state.cursors.len() - 1
                }
            }
        } else {
            return None;
        };
        state.cursors[slot] = Cursor::Claimed;
        Some(FanoutReader {
            fanout: Arc::clone(self),
            slot,
            finished: false,
            disk: None,
            replay: None,
        })
    }

    /// Whether the log can keep `size` more bytes in memory.
    fn memory_is_free(&self, state: &State, size: usize) -> bool {
        let free = self.try_grow(state, size);
        if free {
            state.reservation.shrink(size);
        }
        free
    }

    /// Whether `slot` is the slot of a scan, and no reader has it.
    fn is_free_scan_slot(&self, state: &State, slot: usize) -> bool {
        slot < self.scans && state.cursors.get(slot) == Some(&Cursor::NotReached)
    }

    pub(super) fn add_hold(&self) {
        self.state.lock().holds += 1;
    }

    pub(super) fn remove_hold(&self) {
        let task = {
            let mut state = self.state.lock();
            state.holds -= 1;
            state.retire(&self.metrics);
            state.wake_producer();
            state.stop_if_finished()
        };
        drop(task);
    }

    /// Stop the producer and release the log, at once. Called when the
    /// output of the run is released.
    pub(super) fn shut_down(&self) {
        let (task, pending, chunks) = {
            let mut state = self.state.lock();
            state.shut = true;
            state.reservation.free();
            state.wake_readers();
            (
                state.producer.take(),
                state.pending.take(),
                std::mem::take(&mut state.chunks),
            )
        };
        self.metrics
            .shrink(chunks.iter().map(Chunk::memory).sum::<usize>());
        // Drop the task, which aborts the body, and delete the spill files,
        // without the lock.
        drop(task);
        drop(pending);
        drop(chunks);
    }

    #[cfg(test)]
    pub(super) fn spill_paths(&self) -> Vec<std::path::PathBuf> {
        self.state
            .lock()
            .chunks
            .iter()
            .filter_map(|c| match c {
                Chunk::Disk(disk) => disk.file.path().map(|p| p.to_path_buf()),
                _ => None,
            })
            .collect()
    }

    fn try_grow(&self, state: &State, size: usize) -> bool {
        state.reservation.size() + size <= self.memory_cap
            && state.reservation.try_grow(size).is_ok()
    }

    /// Append `batch` to the in-memory tail, if memory allows and no spill
    /// file is open. Return `batch` if it does not fit.
    fn try_append_in_memory(
        &self,
        batch: RecordBatch,
        writing: bool,
    ) -> Option<RecordBatch> {
        let size = batch.get_array_memory_size();
        let mut state = self.state.lock();
        state.last_batch_size = size;
        if writing || !self.try_grow(&state, size) {
            return Some(batch);
        }
        self.metrics.grow(size);
        let head = state.head;
        match state.chunks.back_mut() {
            Some(Chunk::Memory { batches, .. }) => batches.push_back(batch),
            _ => state.chunks.push_back(Chunk::Memory {
                start: head,
                batches: VecDeque::from([batch]),
            }),
        }
        state.head += 1;
        state.wake_readers();
        None
    }

    /// Append `batch`. If memory is short, write the in-memory tail to a
    /// spill file, and write `batch` and the next batches to an open spill
    /// file.
    fn append(&self, batch: RecordBatch, open: &mut Option<OpenFile>) -> Result<()> {
        self.metrics.buffered_rows.add(batch.num_rows());
        let Some(batch) = self.try_append_in_memory(batch, open.is_some()) else {
            return Ok(());
        };
        if open.is_none() {
            // Move the in-memory tail to disk, and write the next batches to
            // one open file. Memory is used again when that file is complete.
            self.spill_tail()?;
            let start = self.state.lock().head;
            *open = Some(OpenFile::new(&self.spill_manager, &self.name, start)?);
        }
        self.append_to_file(&batch, open)
    }

    /// Write `batch` to the open spill file, without the lock.
    fn append_to_file(
        &self,
        batch: &RecordBatch,
        open: &mut Option<OpenFile>,
    ) -> Result<()> {
        let Some(file) = open.as_mut() else {
            return Ok(());
        };
        file.write(batch)?;
        let full = file.bytes >= SPILL_FILE_BYTES;
        {
            let mut state = self.state.lock();
            let head = state.head;
            match state.chunks.back_mut() {
                Some(Chunk::Open { len, .. }) => *len += 1,
                _ => state.chunks.push_back(Chunk::Open {
                    start: head,
                    len: 1,
                }),
            }
            state.head += 1;
            state.wake_readers();
        }
        if full {
            self.complete_file(open)?;
        }
        Ok(())
    }

    /// Complete the open spill file and make its batches readable.
    fn complete_file(&self, open: &mut Option<OpenFile>) -> Result<()> {
        let Some(mut file) = open.take() else {
            return Ok(());
        };
        let disk = file.finish()?;
        let mut state = self.state.lock();
        state.file_requested = false;
        if let Some(disk) = disk
            && let Some(chunk) = state
                .chunks
                .iter_mut()
                .find(|c| matches!(c, Chunk::Open { start, .. } if *start == disk.start))
        {
            *chunk = Chunk::Disk(disk);
        }
        // Else every reader passed the chunk, and it was retired.
        state.wake_readers();
        Ok(())
    }

    /// Move the in-memory tail of the log to a spill file. Readers read the
    /// tail from memory while the file is written.
    fn spill_tail(&self) -> Result<()> {
        let (start, batches) = {
            let mut state = self.state.lock();
            let Some(Chunk::Memory { start, batches }) = state.chunks.back_mut() else {
                return Ok(());
            };
            if batches.is_empty() {
                state.chunks.pop_back();
                return Ok(());
            }
            let start = *start;
            let batches =
                Arc::new(std::mem::take(batches).into_iter().collect::<Vec<_>>());
            state.chunks.pop_back();
            state.chunks.push_back(Chunk::Spilling {
                start,
                batches: Arc::clone(&batches),
            });
            (start, batches)
        };
        let mut file = OpenFile::new(&self.spill_manager, &self.name, start)?;
        for batch in batches.iter() {
            file.write(batch)?;
        }
        let disk = file.finish()?;
        let mut state = self.state.lock();
        let Some(position) = state
            .chunks
            .iter()
            .position(|c| matches!(c, Chunk::Spilling { start: s, .. } if *s == start))
        else {
            // Every reader passed the chunk, and it was retired.
            return Ok(());
        };
        let memory = state.chunks[position].memory();
        match disk {
            Some(disk) => state.chunks[position] = Chunk::Disk(disk),
            None => {
                return Err(internal_datafusion_err!(
                    "MaterializedCte: a spill file of {} batches is empty",
                    batches.len()
                ));
            }
        }
        state.reservation.shrink(memory);
        self.metrics.shrink(memory);
        Ok(())
    }
}

/// A spill file that the producer writes.
struct OpenFile {
    file: InProgressSpillFile,
    start: usize,
    pieces: Vec<usize>,
    max_piece_size: usize,
    bytes: usize,
}

impl OpenFile {
    fn new(spill_manager: &SpillManager, name: &str, start: usize) -> Result<Self> {
        Ok(Self {
            file: spill_manager.create_in_progress_file(name)?,
            start,
            pieces: vec![],
            max_piece_size: 0,
            bytes: 0,
        })
    }

    /// Write `batch` in pieces of about `SPILL_PIECE_BYTES`.
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let size = batch.get_array_memory_size();
        let rows = batch.num_rows();
        let piece_rows = rows
            .div_ceil(size.div_ceil(SPILL_PIECE_BYTES).max(1))
            .max(1);
        let mut pieces = 0;
        for offset in (0..rows.max(1)).step_by(piece_rows) {
            let piece = batch.slice(offset, piece_rows.min(rows - offset));
            let piece_size = self.file.append_batch(&piece)?;
            self.max_piece_size = self.max_piece_size.max(piece_size);
            pieces += 1;
        }
        self.pieces.push(pieces);
        self.bytes += size;
        Ok(())
    }

    fn finish(&mut self) -> Result<Option<DiskChunk>> {
        let Some(file) = self.file.finish()? else {
            return Ok(None);
        };
        Ok(Some(DiskChunk {
            start: self.start,
            len: self.pieces.len(),
            file,
            max_piece_size: self.max_piece_size,
            pieces: Arc::new(std::mem::take(&mut self.pieces)),
        }))
    }
}

/// What the producer does next.
enum Step {
    Append,
    CompleteFile,
    Stop,
}

async fn produce(fanout: Arc<Fanout>, mut stream: SendableRecordBatchStream) {
    let mut open: Option<OpenFile> = None;
    loop {
        // Wait at the gate.
        let mut waited: Option<Instant> = None;
        let has_open = open.is_some();
        // The size of a batch in the open file, to see if memory is free.
        let batch_size = open.as_ref().map_or(0, |f| f.bytes / f.pieces.len().max(1));
        let step = std::future::poll_fn(|cx| {
            let mut state = fanout.state.lock();
            if state.shut {
                return Poll::Ready(Step::Stop);
            }
            if state.all_finished() {
                if !state.done {
                    state.aborted = true;
                }
                return Poll::Ready(Step::Stop);
            }
            // A reader waits for the open file. Complete it when memory is
            // free again, so that the next batches go to memory, or when the
            // producer cannot append.
            if has_open
                && state.file_requested
                && (fanout.memory_is_free(&state, batch_size)
                    || !state.may_append(has_open, fanout.memory_cap))
            {
                return Poll::Ready(Step::CompleteFile);
            }
            if state.may_append(has_open, fanout.memory_cap) {
                return Poll::Ready(Step::Append);
            }
            waited.get_or_insert_with(Instant::now);
            state.producer_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
        if let Some(start) = waited {
            fanout.producer_wait_time.add_elapsed(start);
        }
        let result = match step {
            Step::Stop => return,
            Step::CompleteFile => fanout.complete_file(&mut open),
            Step::Append => {
                let next = tokio::select! {
                    biased;
                    next = AssertUnwindSafe(stream.next()).catch_unwind() => next,
                    // A reader needs the open file while the body has no
                    // batch ready.
                    _ = fanout.file_request.notified(), if has_open => {
                        // A permit can be left from an earlier request.
                        if !fanout.state.lock().file_requested {
                            continue;
                        }
                        if let Err(e) = fanout.complete_file(&mut open) {
                            fail(&fanout, e);
                            return;
                        }
                        continue;
                    }
                };
                // Catch a panic of the body so that it is an error of the
                // readers, not an early end of the log, as `BufferExec` does.
                match next {
                    Ok(Some(Ok(batch))) => fanout.append(batch, &mut open),
                    Ok(Some(Err(e))) => Err(e),
                    Ok(None) => {
                        if let Err(e) = fanout.complete_file(&mut open) {
                            fail(&fanout, e);
                            return;
                        }
                        let mut state = fanout.state.lock();
                        state.done = true;
                        state.wake_readers();
                        return;
                    }
                    Err(panic) => Err(internal_datafusion_err!(
                        "MaterializedCte body panicked: {}",
                        panic_message(&*panic)
                    )),
                }
            }
        };
        if let Err(e) = result {
            fail(&fanout, e);
            return;
        }
    }
}

fn fail(fanout: &Fanout, error: DataFusionError) {
    let mut state = fanout.state.lock();
    state.error = Some(Arc::new(error));
    state.done = true;
    state.wake_readers();
}

/// A reader of a [`Fanout`].
pub(super) struct FanoutReader {
    fanout: Arc<Fanout>,
    slot: usize,
    /// Set by `finish`. A new reader can reuse the slot after that, so a
    /// second `finish` must not change the slot.
    finished: bool,
    /// The spill file that the reader reads.
    disk: Option<DiskRead>,
    /// Memory for the piece that the reader decodes from a spill file.
    replay: Option<MemoryReservation>,
}

/// The read of one spill file.
struct DiskRead {
    stream: SendableRecordBatchStream,
    chunk: DiskChunk,
    /// The position of the batch of the next piece, and the index of the
    /// next piece in that batch.
    batch: usize,
    piece: usize,
}

impl FanoutReader {
    fn finish(&mut self) {
        self.disk = None;
        self.replay = None;
        if std::mem::replace(&mut self.finished, true) {
            return;
        }
        let task = {
            let mut state = self.fanout.state.lock();
            if state.cursors[self.slot] == Cursor::Finished {
                return;
            }
            state.cursors[self.slot] = Cursor::Finished;
            state.reader_wakers[self.slot] = None;
            state.retire(&self.fanout.metrics);
            state.wake_producer();
            state.stop_if_finished()
        };
        drop(task);
    }

    /// Move the cursor past `pos`, and retire.
    fn advance(&mut self, pos: usize) {
        let mut state = self.fanout.state.lock();
        state.cursors[self.slot] = Cursor::Active(pos + 1);
        state.retire(&self.fanout.metrics);
        state.wake_producer();
    }

    /// The cursor. The first call makes the reader active and starts the
    /// producer.
    fn cursor(&mut self) -> Option<usize> {
        if self.finished {
            return None;
        }
        let mut state = self.fanout.state.lock();
        if state.cursors[self.slot] == Cursor::Claimed {
            state.cursors[self.slot] = Cursor::Active(0);
            self.fanout.start(&mut state);
            state.wake_producer();
        }
        let cursor = state.cursors[self.slot];
        match cursor {
            Cursor::Active(p) => Some(p),
            _ => None,
        }
    }

    fn open_disk(&mut self, chunk: DiskChunk) -> Result<()> {
        let pool = &self.fanout.pool;
        let name = &self.fanout.name;
        let replay = self.replay.get_or_insert_with(|| {
            MemoryConsumer::new(format!("{name} replay")).register(pool)
        });
        replay.try_resize(chunk.max_piece_size)?;
        let stream = self.fanout.spill_manager.read_spill_as_stream_unbuffered(
            Arc::clone(&chunk.file),
            Some(chunk.max_piece_size),
        )?;
        self.disk = Some(DiskRead {
            stream,
            batch: chunk.start,
            piece: 0,
            chunk,
        });
        Ok(())
    }

    /// Read the batch at the cursor from the log, with one lock. Return
    /// `None` if the batch is in a spill file that the reader now reads.
    fn poll_memory(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Option<Poll<Option<Result<RecordBatch>>>> {
        if self.finished {
            return Some(Poll::Ready(None));
        }
        let read = {
            let mut state = self.fanout.state.lock();
            if state.cursors[self.slot] == Cursor::Claimed {
                state.cursors[self.slot] = Cursor::Active(0);
                self.fanout.start(&mut state);
                state.wake_producer();
            }
            let Cursor::Active(pos) = state.cursors[self.slot] else {
                return Some(Poll::Ready(None));
            };
            let read = state.read(pos);
            match read {
                Ok(Read::Batch(_)) => {
                    state.cursors[self.slot] = Cursor::Active(pos + 1);
                    state.retire(&self.fanout.metrics);
                    state.wake_producer();
                }
                Ok(Read::Wait) => {
                    // This reader waits at the head, so the gate is open.
                    state.reader_wakers[self.slot] = Some(cx.waker().clone());
                    state.wake_producer();
                }
                Ok(Read::NeedFile) => {
                    state.reader_wakers[self.slot] = Some(cx.waker().clone());
                    state.file_requested = true;
                    state.wake_producer();
                    self.fanout.file_request.notify_one();
                }
                _ => {}
            }
            read
        };
        Some(match read {
            Ok(Read::Batch(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(Read::Disk(chunk)) => {
                if let Err(e) = self.open_disk(chunk) {
                    self.finish();
                    return Some(Poll::Ready(Some(Err(e))));
                }
                return None;
            }
            Ok(Read::End(error)) => {
                self.finish();
                Poll::Ready(error.map(|e| Err(DataFusionError::Shared(e))))
            }
            Ok(Read::Wait | Read::NeedFile) => Poll::Pending,
            Err(e) => {
                self.finish();
                Poll::Ready(Some(Err(e)))
            }
        })
    }

    fn close_disk(&mut self) {
        self.disk = None;
        if let Some(replay) = &self.replay {
            replay.free();
        }
    }
}

impl Stream for FanoutReader {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.disk.is_none() {
                match this.poll_memory(cx) {
                    Some(poll) => return poll,
                    None => continue,
                }
            }
            let Some(pos) = this.cursor() else {
                return Poll::Ready(None);
            };
            if let Some(disk) = this.disk.as_mut() {
                match futures::ready!(disk.stream.poll_next_unpin(cx)) {
                    Some(Ok(piece)) => {
                        let at = disk.batch;
                        let Some(&pieces) = disk.chunk.pieces.get(at - disk.chunk.start)
                        else {
                            this.finish();
                            return Poll::Ready(Some(Err(internal_datafusion_err!(
                                "MaterializedCte: a spill file has more batches than expected"
                            ))));
                        };
                        disk.piece += 1;
                        let last = disk.piece == pieces;
                        if last {
                            disk.batch += 1;
                            disk.piece = 0;
                        }
                        // Skip the batches that the reader read before the
                        // chunk went to disk.
                        if at < pos {
                            continue;
                        }
                        if last {
                            this.advance(at);
                        }
                        return Poll::Ready(Some(Ok(piece)));
                    }
                    Some(Err(e)) => {
                        this.finish();
                        return Poll::Ready(Some(Err(e)));
                    }
                    None => this.close_disk(),
                }
            }
        }
    }
}

impl Drop for FanoutReader {
    fn drop(&mut self) {
        self.finish();
    }
}
