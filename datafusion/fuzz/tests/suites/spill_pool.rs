// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Seeded scenarios checking concurrent writes, reader progress, ordering, and
//! spill-file cleanup. Seeds reproduce the workload and delay policy; thread
//! scheduling may still vary.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::instant::Instant;
use datafusion_execution::disk_manager::{
    DiskManager, DiskManagerBuilder, DiskManagerMode,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::{SpillFile, SpillWriter, TempFileFactory};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion_physical_plan::spill::SpillManager;
use datafusion_physical_plan::spill::spill_pool::{SpillPoolSink, mpsc_channel};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use test_utils::fuzz::FuzzTest;

pub(crate) struct SpillPoolFuzzer;

impl FuzzTest for SpillPoolFuzzer {
    type Dataset = ();

    fn name(&self) -> &str {
        "spill_pool_scenario"
    }

    fn prepare_dataset(&self, _: u64) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn run_case(&self, _: &(), seed: u64) -> Result<()> {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        const READ_TIMEOUT: Duration = Duration::from_secs(5);

        let mut rng = StdRng::seed_from_u64(seed);
        let n_writers = rng.random_range(1..=4);
        let batch_bytes = batch_with_id(0, 10).get_array_memory_size();
        // Rotate after every batch, after a few batches, or never.
        let max_file_size = match rng.random_range(0..3) {
            0 => batch_bytes / 2,
            1 => batch_bytes * 3,
            _ => usize::MAX / 2,
        };
        let n_phases = rng.random_range(1..=6);
        let context = format!(
            "seed {seed}: {n_writers} writers, max_file_size {max_file_size}, {n_phases} phases"
        );

        let pauser = Arc::new(PhasePauser::default());
        let write_hook: WriteHook = {
            let pauser = Arc::clone(&pauser);
            Arc::new(move || pauser.on_io())
        };
        // Some files are slow to read back, so an older file's bytes can arrive
        // after a newer file's. Batches must still come back in file order for a
        // single writer.
        let read_hook: ReadHook = {
            let rng = Mutex::new(StdRng::seed_from_u64(seed ^ 0x5EED_4EAD));
            Arc::new(move || {
                let mut rng = rng.lock();
                if rng.random_bool(0.5) {
                    Duration::ZERO
                } else {
                    Duration::from_millis(rng.random_range(1..=20))
                }
            })
        };
        let (spill_manager, disk_manager) =
            spill_manager_with_io_hooks(write_hook, read_hook)?;
        let (writer, mut reader) = mpsc_channel(max_file_size, spill_manager);
        let mut sinks: Vec<Option<SpillPoolSink>> =
            (0..n_writers).map(|_| Some(writer.new_sink())).collect();
        drop(writer);

        let mut pushed: Vec<i32> = vec![];
        let mut read: Vec<i32> = vec![];

        for phase in 0..n_phases {
            let alive: Vec<usize> =
                (0..n_writers).filter(|w| sinks[*w].is_some()).collect();
            if alive.is_empty() {
                break;
            }

            // A random non-empty subset of the live writers each pushes a few
            // batches, concurrently, held so that their pushes overlap.
            let mut pushing: Vec<usize> = alive
                .iter()
                .copied()
                .filter(|_| rng.random_bool(0.6))
                .collect();
            if pushing.is_empty() {
                pushing.push(alive[rng.random_range(0..alive.len())]);
            }
            pauser.begin_phase(pushing.len());
            let mut threads = Vec::with_capacity(pushing.len());
            for w in pushing {
                let sink = sinks[w].take().unwrap();
                let batches: Vec<RecordBatch> = (0..rng.random_range(1..=3))
                    .map(|_| {
                        let id = pushed.len() as i32 + 1;
                        pushed.push(id);
                        batch_with_id(id, rng.random_range(1..=40))
                    })
                    .collect();
                threads.push((
                    w,
                    std::thread::spawn(move || {
                        for batch in &batches {
                            sink.push_batch(batch).unwrap();
                        }
                        sink
                    }),
                ));
            }
            pauser.release_phase();
            for (w, thread) in threads {
                sinks[w] = Some(thread.join().unwrap());
            }

            // Quiescent point: no push is in progress and none will happen until
            // the reader is done. Every batch pushed so far must be readable now,
            // in any amount. `RepartitionExec` relies on exactly this: it blocks
            // on the reader once per pushed batch while its channel gate keeps
            // the writers parked. Sometimes leave a backlog for later phases.
            let read_now = rng.random_range(0..=pushed.len() - read.len());
            for _ in 0..read_now {
                let next = tokio::time::timeout(READ_TIMEOUT, reader.next())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "{context}, phase {phase}: reader stalled with {} of {} \
                             batches unread and {} writers alive",
                            pushed.len() - read.len(),
                            pushed.len(),
                            alive.len()
                        )
                    });
                let batch = next.unwrap_or_else(|| {
                    panic!("{context}, phase {phase}: EOF while writers are alive")
                })?;
                read.push(id_of(&batch));
            }

            // With nothing left to read the reader must wait, not signal EOF.
            if read.len() == pushed.len() && rng.random_bool(0.3) {
                let probe =
                    tokio::time::timeout(Duration::from_millis(50), reader.next()).await;
                assert!(
                    probe.is_err(),
                    "{context}, phase {phase}: reader yielded {probe:?} with nothing \
                     pushed and writers alive"
                );
            }

            // Dropping one of several writers must not disturb anything.
            if alive.len() > 1 && rng.random_bool(0.3) {
                sinks[alive[rng.random_range(0..alive.len())]] = None;
            }
        }

        // Once the last writer is gone the reader must hand out the backlog and
        // then signal EOF.
        sinks.clear();
        while read.len() < pushed.len() {
            let next = tokio::time::timeout(READ_TIMEOUT, reader.next())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{context}: reader stalled with {} of {} batches unread after \
                         all writers were dropped",
                        pushed.len() - read.len(),
                        pushed.len()
                    )
                });
            let batch = next.unwrap_or_else(|| {
                panic!(
                    "{context}: EOF with {} batches unread",
                    pushed.len() - read.len()
                )
            })?;
            read.push(id_of(&batch));
        }
        let eof = tokio::time::timeout(READ_TIMEOUT, reader.next())
            .await
            .unwrap_or_else(|_| {
                panic!("{context}: reader stalled instead of signalling EOF after all writers were dropped")
            });
        assert!(eof.is_none(), "{context}: batch after everything was read");

        let mut expected = pushed.clone();
        expected.sort_unstable();
        let mut actual = read.clone();
        actual.sort_unstable();
        assert_eq!(actual, expected, "{context}: batches lost or duplicated");
        if n_writers == 1 {
            assert_eq!(read, pushed, "{context}: single-writer pool must be FIFO");
        }

        drop(reader);
        assert_eq!(
            disk_manager.used_disk_space(),
            0,
            "{context}: spill files not released"
        );
        Ok(())
    }
}

fn create_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
}

type WriteHook = Arc<dyn Fn() + Send + Sync>;
type ReadHook = Arc<dyn Fn() -> Duration + Send + Sync>;

/// Test double for a spill file that runs `write_hook` before every disk
/// write or flush, and consults `read_hook` before every read. Writers write
/// while holding their file's lock, so a hook that blocks pauses a
/// `push_batch` at exactly the point where it has a file checked out for
/// writing; a read delay makes an older file's bytes arrive after a newer
/// file's. Data still goes to real temporary files.
struct IoHookFactory {
    inner: Arc<DiskManager>,
    write_hook: WriteHook,
    read_hook: ReadHook,
}

impl TempFileFactory for IoHookFactory {
    fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>> {
        Ok(Arc::new(IoHookFile {
            inner: self.inner.create_tmp_file(description)?,
            write_hook: Arc::clone(&self.write_hook),
            read_hook: Arc::clone(&self.read_hook),
        }))
    }
}

struct IoHookFile {
    inner: Arc<dyn SpillFile>,
    write_hook: WriteHook,
    read_hook: ReadHook,
}

impl SpillFile for IoHookFile {
    fn path(&self) -> Option<&std::path::Path> {
        self.inner.path()
    }

    fn size(&self) -> Option<u64> {
        self.inner.size()
    }

    fn read_stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<bytes::Bytes>> + Send>>> {
        let delay = (self.read_hook)();
        let mut inner = Some(self.inner.read_stream()?);
        Ok(Box::pin(
            futures::stream::once(tokio::time::sleep(delay))
                .flat_map(move |_| inner.take().expect("polled once")),
        ))
    }

    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        Ok(Box::new(IoHookWriter {
            inner: self.inner.open_writer()?,
            hook: Arc::clone(&self.write_hook),
        }))
    }
}

struct IoHookWriter {
    inner: Box<dyn SpillWriter>,
    hook: WriteHook,
}

impl std::io::Write for IoHookWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        (self.hook)();
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (self.hook)();
        self.inner.flush()
    }
}

impl SpillWriter for IoHookWriter {
    fn finish(&mut self) -> Result<()> {
        self.inner.finish()
    }
}

/// A `SpillManager` whose files run `write_hook` before every disk write and
/// consult `read_hook` before every read, plus the `DiskManager` that owns
/// the files so tests can check disk usage.
fn spill_manager_with_io_hooks(
    write_hook: WriteHook,
    read_hook: ReadHook,
) -> Result<(Arc<SpillManager>, Arc<DiskManager>)> {
    let disk_manager = Arc::new(DiskManagerBuilder::default().build()?);
    let runtime = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
            DiskManagerMode::Custom(Arc::new(IoHookFactory {
                inner: Arc::clone(&disk_manager),
                write_hook,
                read_hook,
            })),
        ))
        .build_arc()?;
    let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let spill_manager =
        Arc::new(SpillManager::new(runtime, metrics, create_test_schema()));
    Ok((spill_manager, disk_manager))
}

/// Pauses every writer of a "phase" inside its first disk write, so that
/// several `push_batch` calls overlap at the point where each holds a file
/// checked out for writing. A writer that blocks earlier (for example on
/// another writer's file lock) never arrives; `release_phase` stops waiting
/// for it after a short while.
#[derive(Default)]
struct PhasePauser {
    state: Mutex<PhaseState>,
    changed: parking_lot::Condvar,
}

#[derive(Default)]
struct PhaseState {
    phase: u64,
    expected: usize,
    held: usize,
    released: bool,
}

thread_local! {
    /// The phase in which the current writer thread has already been held.
    static HELD_IN_PHASE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl PhasePauser {
    fn begin_phase(&self, expected: usize) {
        let mut state = self.state.lock();
        state.phase += 1;
        state.expected = expected;
        state.held = 0;
        state.released = false;
    }

    /// Called by writer threads before every disk write.
    fn on_io(&self) {
        let mut state = self.state.lock();
        if state.released
            || state.held >= state.expected
            || HELD_IN_PHASE.get() == state.phase
        {
            return;
        }
        HELD_IN_PHASE.set(state.phase);
        state.held += 1;
        self.changed.notify_all();
        while !state.released {
            self.changed.wait(&mut state);
        }
    }

    /// Waits until every writer of the phase is held (or 300 ms have
    /// passed), then releases them all at once.
    fn release_phase(&self) {
        let mut state = self.state.lock();
        let deadline = Instant::now() + Duration::from_millis(300);
        while state.held < state.expected {
            if self.changed.wait_until(&mut state, deadline).timed_out() {
                break;
            }
        }
        state.released = true;
        self.changed.notify_all();
    }
}

fn batch_with_id(id: i32, rows: usize) -> RecordBatch {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![id; rows]));
    RecordBatch::try_new(create_test_schema(), vec![a]).unwrap()
}

fn id_of(batch: &RecordBatch) -> i32 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0)
}
