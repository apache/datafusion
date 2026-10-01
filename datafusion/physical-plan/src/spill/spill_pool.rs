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
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;
use std::task::Waker;

use parking_lot::Mutex;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, SpillFile};

use super::in_progress_spill_file::InProgressSpillFile;
use super::spill_manager::SpillManager;

/// Shared state between the writer and readers of a spill pool.
/// This contains the queue of files and coordination state.
///
/// # Locking Design
///
/// This struct uses **fine-grained locking** with nested `Arc<Mutex<>>`:
/// - `SpillPoolShared` is wrapped in `Arc<Mutex<>>` (outer lock)
/// - Each `ActiveSpillFileShared` is wrapped in `Arc<Mutex<>>` (inner lock)
///
/// This enables:
/// 1. **Short critical sections**: The outer lock is held only for queue operations
/// 2. **I/O outside locks**: Disk I/O happens while holding only the file-specific lock
/// 3. **Concurrent operations**: Reader can access the queue while writer does I/O
///
/// **Lock ordering discipline**: Never hold both locks simultaneously to prevent deadlock.
/// Always: acquire outer lock → release outer lock → acquire inner lock (if needed).
struct SpillPoolShared {
    /// Files created by writers that the reader has not picked up yet, in creation
    /// order. The reader moves them into its own list on every poll (see
    /// [`SpillPoolReader`]), so this queue only ever holds files the reader has not
    /// seen. Each file has its own lock to enable concurrent reader/writer access.
    new_files: VecDeque<Arc<Mutex<ActiveSpillFileShared>>>,
    /// SpillManager for creating files and tracking metrics
    spill_manager: Arc<SpillManager>,
    /// Pool-level waker to notify when new files are available (single reader)
    waker: Option<Waker>,
    /// FIFO queue of open write files. The queue may contain multiple items when multiple
    /// writers concurrently write to the pool.
    /// Each write file has its own lock to allow I/O without blocking queue access.
    open_write_files: VecDeque<Arc<Mutex<ActiveSpillFileShared>>>,
    /// Number of `SpillPoolWriter` instances that have not been dropped yet. As long as this value
    /// is greater than zero, readers should assume batches may still be pushed. This prevents
    /// premature EOF signaling.
    remaining_writer_count: usize,
}

impl SpillPoolShared {
    /// Creates a new shared pool state
    fn new(spill_manager: Arc<SpillManager>) -> Self {
        Self {
            new_files: VecDeque::new(),
            spill_manager,
            waker: None,
            open_write_files: VecDeque::new(),
            remaining_writer_count: 1,
        }
    }

    /// Registers a waker to be notified when new data is available (pool-level)
    fn register_waker(&mut self, waker: Waker) {
        self.waker = Some(waker);
    }

    /// Wakes the pool-level reader
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// Writer for a spill pool that can be cloned to produce additional writers.
///
/// Created by [`mpsc_channel`]. See that function for architecture diagrams and usage
/// examples.
pub struct SpillPoolWriter {
    /// The underlying shared writer. Kept private and never cloned, so this pool always has
    /// exactly one writer.
    inner: SpillPoolSink,
}

impl SpillPoolWriter {
    /// Spills a batch to the pool, rotating files when necessary.
    ///
    /// See [`mpsc_channel`] for the rotation semantics.
    ///
    /// # Errors
    ///
    /// Returns an error if disk I/O fails or disk quota is exceeded.
    pub fn push_batch(&self, batch: &RecordBatch) -> Result<()> {
        self.inner.push_batch(batch)
    }
}

impl SpillPoolWriter {
    /// Returns a new sink that can be used to spill batches to the pool.
    ///
    /// As an alternative to this function, it is also possible to clone the writer. The benefit
    /// of this method is that the output type matches the type used by [`spsc_channel`]. This
    /// enables cost-free abstraction for producers over SPSC and MPSC channels.
    pub fn new_sink(&self) -> SpillPoolSink {
        // Increment `remaining_writer_count`. The corresponding decrement is done in the `Drop`
        // implementation of `SpillPoolWriter`.
        self.inner.shared.lock().remaining_writer_count += 1;
        SpillPoolSink {
            max_file_size_bytes: self.inner.max_file_size_bytes,
            shared: Arc::clone(&self.inner.shared),
        }
    }
}

impl Clone for SpillPoolWriter {
    fn clone(&self) -> Self {
        Self {
            inner: self.new_sink(),
        }
    }
}

impl Drop for SpillPoolSink {
    fn drop(&mut self) {
        let mut shared = self.shared.lock();

        shared.remaining_writer_count -= 1;
        let is_last_writer = shared.remaining_writer_count == 0;

        if !is_last_writer {
            // Other writer clones are still active; do not finalize or
            // signal EOF to readers.
            return;
        }

        // Finalize any spill files that were not finished yet
        if !shared.open_write_files.is_empty() {
            let files = mem::take(&mut shared.open_write_files);
            drop(shared);

            for file in files {
                let mut file_shared = file.lock();

                // Finish the current writer if it exists
                if let Some(mut writer) = file_shared.writer.take() {
                    // Ignore errors on drop - we're in destructor
                    let _ = writer.finish();
                }

                // Mark as finished so readers know not to wait for more data
                file_shared.writer_finished = true;

                // Wake reader waiting on this file (it's now finished)
                file_shared.wake();
                drop(file_shared);
            }

            shared = self.shared.lock();
        }

        // Wake pool-level readers
        shared.wake();
    }
}

/// Single writer for a spill pool that cannot be cloned.
///
/// Created by [`spsc_channel`] and [`SpillPoolWriter::new_sink`].
pub struct SpillPoolSink {
    /// Maximum size in bytes before rotating to a new file.
    /// Typically set from configuration `datafusion.execution.max_spill_file_size_bytes`.
    max_file_size_bytes: usize,
    /// Shared state with readers (includes current_write_file for coordination)
    shared: Arc<Mutex<SpillPoolShared>>,
}

impl SpillPoolSink {
    /// Spills a batch to the pool, rotating files when necessary.
    ///
    /// See [`spsc_channel`] for overall architecture and examples.
    ///
    /// # Errors
    ///
    /// Returns an error if disk I/O fails or disk quota is exceeded.
    pub fn push_batch(&self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            // Skip empty batches
            return Ok(());
        }

        let batch_size = batch.get_array_memory_size();

        // Fine-grained locking: Lock shared state briefly for queue access
        let mut shared = self.shared.lock();

        // Create new file if there is none available to append to
        let write_file = if !shared.open_write_files.is_empty() {
            shared.open_write_files.pop_front().unwrap()
        } else {
            let spill_manager = Arc::clone(&shared.spill_manager);
            // Release shared lock before disk I/O (fine-grained locking)
            drop(shared);

            let writer = spill_manager.create_in_progress_file("SpillPool")?;
            // Clone the file so readers can access it immediately
            let file = Arc::clone(writer.file().expect(
                "InProgressSpillFile should always have a file when it is first created",
            ));

            let file_shared = Arc::new(Mutex::new(ActiveSpillFileShared {
                writer: Some(writer),
                file: Some(file), // Set immediately so readers can access it
                batches_written: 0,
                estimated_size: 0,
                writer_finished: false,
                waker: None,
            }));

            // Re-acquire lock and push to shared queue
            shared = self.shared.lock();
            shared.new_files.push_back(Arc::clone(&file_shared));
            shared.wake(); // Wake readers waiting for new files
            file_shared
        };

        // Release shared lock before file I/O (fine-grained locking)
        // This allows readers to access the queue while we do disk I/O
        drop(shared);

        // Write batch to current file - lock only the specific file
        let mut file_shared = write_file.lock();

        // Append the batch
        if let Some(ref mut writer) = file_shared.writer {
            writer.append_batch(batch)?;
            // make sure we flush the writer for readers
            writer.flush()?;
            file_shared.batches_written += 1;
            file_shared.estimated_size += batch_size;
        }

        // Wake reader waiting on this specific file
        file_shared.wake();

        let max_file_size_reached = file_shared.estimated_size > self.max_file_size_bytes;

        if max_file_size_reached {
            // Finish the IPC writer
            if let Some(mut writer) = file_shared.writer.take() {
                writer.finish()?;
            }
            // Mark as finished so readers know not to wait for more data
            file_shared.writer_finished = true;
            // Wake reader waiting on this file (it's now finished)
            file_shared.wake();

            // Don't place `write_file` back in the `open_write_files` queue so we don't
            // try writing to it again
        } else {
            // Release file lock
            drop(file_shared);
            // Put back the current file for further writing
            let mut shared = self.shared.lock();
            shared.open_write_files.push_back(write_file);
        }

        Ok(())
    }
}

/// Creates a paired writer and reader for a spill pool with SPSC (single-producer,
/// single-consumer) semantics and strict FIFO ordering.
///
/// If you need a spill pool that supports several producers, use [`mpsc_channel`] instead.
///
/// The reader can start reading immediately after the writer appends a batch
/// to the spill file, without waiting for the file to be sealed, while the writer continues to
/// write more data.
///
/// Internally this coordinates rotating spill files based on size limits, and
/// handles asynchronous notification between the writer and reader using wakers.
/// This ensures that we manage disk usage efficiently while allowing concurrent
/// I/O between the writer and reader.
///
/// # Data Flow Overview
///
/// 1. Writer write batch `B0` to F1
/// 2. Writer write batch `B1` to F1, notices the size limit exceeded, finishes F1.
/// 3. Reader read `B0` from F1
/// 4. Reader read `B1`, no more batch to read -> wait on the waker
/// 5. Writer write batch `B2` to a new file `F2`, wake up the waiting reader.
/// 6. Reader read `B2` from F2.
/// 7. Repeat until writer is dropped.
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────────────┐
/// │                            SpillPool                                    │
/// │                                                                         │
/// │  Writer Side              Shared State              Reader Side         │
/// │  ───────────              ────────────              ───────────         │
/// │                                                                         │
/// │  SpillPoolSink      ┌────────────────────┐    RecordBatchStream         │
/// │       │             │  VecDeque<File>    │          │                   │
/// │       │             │  ┌────┐┌────┐      │          │                   │
/// │  push_batch()       │  │ F1 ││ F2 │ ...  │      next().await            │
/// │       │             │  └────┘└────┘      │          │                   │
/// │       ▼             │                    │          ▼                   │
/// │  ┌─────────┐        │                    │    ┌──────────┐              │
/// │  │Current  │───────▶│ Coordination:      │◀───│ Current  │              │
/// │  │Write    │        │ - Wakers           │    │ Read     │              │
/// │  │File     │        │ - Batch counts     │    │ File     │              │
/// │  └─────────┘        │ - Writer status    │    └──────────┘              │
/// │       │             └────────────────────┘           │                  │
/// │       │                                              │                  │
/// │  Size > limit?                                Read all batches?         │
/// │       │                                              │                  │
/// │       ▼                                              ▼                  │
/// │  Rotate to new file                            Pop from queue           │
/// └─────────────────────────────────────────────────────────────────────────┘
///
/// Writer produces → Shared queue → Reader consumes
/// ```
///
/// # File State Machine
///
/// Each file in the pool coordinates between writer and reader:
///
/// ```text
///                Writer View              Reader View
///                ───────────              ───────────
///
/// Created        writer: Some(..)         batches_read: 0
///                batches_written: 0       (waiting for data)
///                       │
///                       ▼
/// Writing        append_batch()           Can read if:
///                batches_written++        batches_read < batches_written
///                wake readers
///                       │                        │
///                       │                        ▼
///                ┌──────┴──────┐          poll_next() → batch
///                │             │          batches_read++
///                ▼             ▼
///          Size > limit?  More data?
///                │             │
///                │             └─▶ Yes ──▶ Continue writing
///                ▼
///          finish()                   Reader catches up:
///          writer_finished = true     batches_read == batches_written
///          wake readers                       │
///                │                            ▼
///                └─────────────────────▶ Returns Poll::Ready(None)
///                                       File complete, pop from queue
/// ```
///
/// # Arguments
///
/// * `max_file_size_bytes` - Maximum size per file before rotation. When a file
///   exceeds this size, the writer automatically rotates to a new file.
/// * `spill_manager` - Manager for file creation and metrics tracking
///
/// # Returns
///
/// A tuple of `(SpillPoolSink, SendableRecordBatchStream)` that share the same
/// underlying pool. The reader is returned as a stream for immediate use with
/// async stream combinators.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use arrow::array::{ArrayRef, Int32Array};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use arrow::record_batch::RecordBatch;
/// use datafusion_execution::runtime_env::RuntimeEnv;
/// use futures::StreamExt;
///
/// # use datafusion_physical_plan::spill::spill_pool;
/// # use datafusion_physical_plan::spill::SpillManager; // Re-exported for doctests
/// # use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
/// #
/// # #[tokio::main]
/// # async fn main() -> datafusion_common::Result<()> {
/// # // Setup for the example (typically comes from TaskContext in production)
/// # let env = Arc::new(RuntimeEnv::default());
/// # let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
/// # let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
/// # let spill_manager = Arc::new(SpillManager::new(env, metrics, schema.clone()));
/// #
/// // Create channel with 1MB file size limit
/// let (writer, mut reader) = spill_pool::spsc_channel(1024 * 1024, spill_manager);
///
/// // Spawn writer and reader concurrently; writer wakes reader via wakers
/// let writer_task = tokio::spawn(async move {
///     for i in 0..5 {
///         let array: ArrayRef = Arc::new(Int32Array::from(vec![i; 100]));
///         let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
///         writer.push_batch(&batch)?;
///     }
///     // Explicitly drop writer to finalize the spill file and wake the reader
///     drop(writer);
///     datafusion_common::Result::<()>::Ok(())
/// });
///
/// let reader_task = tokio::spawn(async move {
///     let mut batches_read = 0;
///     while let Some(result) = reader.next().await {
///         let _batch = result?;
///         batches_read += 1;
///     }
///     datafusion_common::Result::<usize>::Ok(batches_read)
/// });
///
/// let (writer_res, reader_res) = tokio::join!(writer_task, reader_task);
/// writer_res
///     .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))??;
/// let batches_read = reader_res
///     .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))??;
///
/// assert_eq!(batches_read, 5);
/// # Ok(())
/// # }
/// ```
///
/// # Why rotate files?
///
/// File rotation ensures we don't end up with unreferenced disk usage.
/// If we used a single file for all spilled data, we would end up with
/// unreferenced data at the beginning of the file that has already been read
/// by readers but we can't delete because you can't truncate from the start of a file.
///
/// Consider the case of a query like `SELECT * FROM large_table WHERE false`.
/// Obviously this query produces no output rows, but if we had a spilling operator
/// in the middle of this query between the scan and the filter it would see the entire
/// `large_table` flow through it and thus would spill all of that data to disk.
/// So we'd end up using up to `size(large_table)` bytes of disk space.
/// If instead we use file rotation, and as long as the readers can keep up with the writer,
/// then we can ensure that once a file is fully read by all readers it can be deleted,
/// thus bounding the maximum disk usage to roughly `max_file_size_bytes`.
pub fn spsc_channel(
    max_file_size_bytes: usize,
    spill_manager: Arc<SpillManager>,
) -> (SpillPoolSink, SendableRecordBatchStream) {
    let schema = Arc::clone(spill_manager.schema());
    let shared = Arc::new(Mutex::new(SpillPoolShared::new(spill_manager)));

    let writer = SpillPoolSink {
        max_file_size_bytes,
        shared: Arc::clone(&shared),
    };

    let reader = SpillPoolReader::new(shared, schema);

    (writer, Box::pin(reader))
}

/// Alias for [`mpsc_channel`].
#[deprecated(since = "55.0.0", note = "Use mpsc_channel instead")]
pub fn channel(
    max_file_size_bytes: usize,
    spill_manager: Arc<SpillManager>,
) -> (SpillPoolWriter, SendableRecordBatchStream) {
    mpsc_channel(max_file_size_bytes, spill_manager)
}

/// Creates a paired writer and reader for a spill pool with MPSC (multi-producer,
/// single-consumer) semantics. See [`spsc_channel`] for the general architecture description
/// of the spill pool.
///
/// Additional writers can be created by cloning the returned [`SpillPoolWriter`].
///
/// In contrast to [`spsc_channel`], this implementation provides no guarantees regarding
/// the read order of the returned [`SendableRecordBatchStream`].
///
/// If you need strict end-to-end FIFO (a single writer whose batches are read back in exact
/// write order), use [`spsc_channel`] instead.
///
/// # File Management
///
/// The shared channel uses the same size-based rotation trigger as the [single producer channel](spsc_channel).
/// All writers share the same pool of write files and coordinate file rotation. The number of open
/// files is kept as small as possible. When more writes occur concurrently than there are open write
/// files an additional file will be opened to write to. This prevents multiple writers from blocking
/// each other.
///
/// When the last writer clone is dropped, it finalizes any remaining open write files so that all
/// written data can be accessed by the reader.
///
/// # Returns
///
/// A tuple of `(SpillPoolWriter, SendableRecordBatchStream)` that share the same
/// underlying pool. The reader is returned as a stream for immediate use with
/// async stream combinators. The writer can be cloned to create additional writers.
pub fn mpsc_channel(
    max_file_size_bytes: usize,
    spill_manager: Arc<SpillManager>,
) -> (SpillPoolWriter, SendableRecordBatchStream) {
    let (inner, reader) = spsc_channel(max_file_size_bytes, spill_manager);
    (SpillPoolWriter { inner }, reader)
}

/// Shared state between writer and readers for an active spill file.
/// Protected by a Mutex to coordinate between concurrent readers and the writer.
struct ActiveSpillFileShared {
    /// Writer handle - taken (set to None) when finish() is called
    writer: Option<InProgressSpillFile>,
    /// The spill file, set when the writer finishes.
    /// Taken by the reader when creating a stream (the file stays open via file handles).
    file: Option<Arc<dyn SpillFile>>,
    /// Total number of batches written to this file
    batches_written: usize,
    /// Estimated size in bytes of data written to this file
    estimated_size: usize,
    /// Whether the writer has finished writing to this file
    writer_finished: bool,
    /// Waker for reader waiting on this specific file (SPSC: only one reader)
    waker: Option<Waker>,
}

impl ActiveSpillFileShared {
    /// Registers a waker to be notified when new data is written to this file
    fn register_waker(&mut self, waker: Waker) {
        self.waker = Some(waker);
    }

    /// Wakes the reader waiting on this file
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

/// Reader state for a SpillPoolFile (owned by individual SpillPoolFile instances).
/// This is kept separate from the shared state to avoid holding locks during I/O.
struct SpillPoolFileReader {
    /// The actual stream reading from disk
    stream: SendableRecordBatchStream,
    /// Number of batches this reader has consumed
    batches_read: usize,
}

struct SpillPoolFile {
    /// Shared coordination state (contains writer and batch counts)
    shared: Arc<Mutex<ActiveSpillFileShared>>,
    /// Reader state (lazy-initialized, owned by this SpillPoolFile)
    reader: Option<SpillPoolFileReader>,
    /// Spill manager for creating readers
    spill_manager: Arc<SpillManager>,
}

/// Outcome of polling a single file of the pool, see [`SpillPoolFile::poll_file`].
enum FilePoll {
    /// A batch was read from the file, or reading it failed.
    Item(Result<RecordBatch>),
    /// The file's stream ended: every batch written to it has been read and the
    /// writer has finished it (the caller verifies the latter).
    Done,
    /// Every batch written so far has been read, but the writer has not finished
    /// the file. The file's waker is registered, so the task is woken when the
    /// writer appends another batch or finishes the file.
    CaughtUp,
    /// The file has unread batches, but the underlying I/O is not ready yet. The
    /// I/O has registered the task's waker.
    Pending,
}

impl SpillPoolFile {
    /// Polls the file for its next batch.
    ///
    /// Unlike a plain stream poll this tells the caller *why* no batch is
    /// available, so [`SpillPoolReader`] can move on to a newer file when this
    /// one is only waiting for its writer, but keeps waiting on it while its
    /// data is still being read from disk (which keeps batches in file order).
    fn poll_file(&mut self, cx: &mut std::task::Context<'_>) -> FilePoll {
        use std::task::Poll;

        // Step 1: Lock shared state and check coordination
        let file = {
            let mut shared = self.shared.lock();

            // Determine if we can read
            let batches_read = self.reader.as_ref().map_or(0, |r| r.batches_read);

            if batches_read < shared.batches_written {
                // More data available to read - take the file if we don't have a reader yet
                if self.reader.is_none() {
                    shared.file.take()
                } else {
                    None
                }
            } else if shared.writer_finished {
                // No more data and writer is done - EOF
                return FilePoll::Done;
            } else {
                // Caught up to writer, but writer still active - register waker and wait
                shared.register_waker(cx.waker().clone());
                return FilePoll::CaughtUp;
            }
        }; // Lock released here

        // Step 2: Lazy-create reader stream if needed
        if self.reader.is_none() {
            if let Some(file) = file {
                // we want this unbuffered because files are actively being written to
                match self
                    .spill_manager
                    .read_spill_as_stream_unbuffered(file, None)
                {
                    Ok(stream) => {
                        self.reader = Some(SpillPoolFileReader {
                            stream,
                            batches_read: 0,
                        });
                    }
                    Err(e) => return FilePoll::Item(Err(e)),
                }
            } else {
                // File not available yet (writer hasn't finished or already taken)
                // Register waker and wait for file to be ready
                let mut shared = self.shared.lock();
                shared.register_waker(cx.waker().clone());
                return FilePoll::CaughtUp;
            }
        }

        // Step 3: Poll the reader stream (no lock held)
        let Some(reader) = &mut self.reader else {
            // Should not reach here, but handle gracefully
            return FilePoll::Done;
        };
        match reader.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Successfully read a batch - increment counter
                reader.batches_read += 1;
                FilePoll::Item(Ok(batch))
            }
            Poll::Ready(Some(Err(e))) => FilePoll::Item(Err(e)),
            Poll::Ready(None) => {
                // Stream exhausted unexpectedly
                // This shouldn't happen if coordination is correct, but handle gracefully
                FilePoll::Done
            }
            Poll::Pending => FilePoll::Pending,
        }
    }
}

/// A stream that reads from a SpillPool. The reader guarantees FIFO order if a single writer is used.
///
/// Created by [`spsc_channel`]. See that function for architecture diagrams and usage examples.
///
/// The stream automatically handles file rotation and reads from completed files.
/// When no data is available, it returns `Poll::Pending` and registers a waker to
/// be notified when the writer produces more data.
///
/// # Infinite Stream Semantics
///
/// This stream never returns `None` (`Poll::Ready(None)`) on its own - it will keep
/// waiting for the writer to produce more data. The stream ends only when:
/// - The reader is dropped
/// - The writer is dropped AND all queued data has been consumed
///
/// This makes it suitable for continuous streaming scenarios where the writer may
/// produce data intermittently.
///
/// # Reading from several open files
///
/// With a single writer ([`spsc_channel`]) at most one file is open for writing at
/// a time and every earlier file is finished, so reading the files oldest first is
/// exactly FIFO. With several writers ([`mpsc_channel`]) concurrent pushes can
/// leave more than one file open, and a batch may land in a newer file while the
/// oldest file is drained but not finished. The reader therefore polls every file
/// it knows about, oldest first, and returns the first batch that is available.
/// A file that is merely caught up with its writer is skipped; the reader only
/// waits on a file while that file has unread batches whose bytes are still being
/// read from disk, which keeps batches in file order (strict FIFO for one writer).
///
/// This matters for [`RepartitionExec`](crate::repartition::RepartitionExec), which
/// sends a "spilled" marker through its channel after every `push_batch` and then
/// blocks on this stream until it yields a batch. If the reader only ever drained the
/// oldest file, it could wait for a batch that had been written to a newer file; with
/// the channel gate closed the writers could not push anything to wake it, so the
/// query would deadlock (<https://github.com/apache/datafusion/issues/24883>).
pub struct SpillPoolReader {
    /// Shared reference to the spill pool
    shared: Arc<Mutex<SpillPoolShared>>,
    /// Files this reader has picked up from the pool and not fully consumed yet, in
    /// creation order. Each carries its own lazily-created stream and read position.
    files: VecDeque<SpillPoolFile>,
    /// Schema of the spilled data
    schema: SchemaRef,
}

impl SpillPoolReader {
    /// Creates a new reader from shared pool state.
    ///
    /// This is private - use the [`spsc_channel`] function to create a reader/writer pair.
    ///
    /// # Arguments
    ///
    /// * `shared` - Shared reference to the pool state
    fn new(shared: Arc<Mutex<SpillPoolShared>>, schema: SchemaRef) -> Self {
        Self {
            shared,
            files: VecDeque::new(),
            schema,
        }
    }
}

impl Stream for SpillPoolReader {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        // `Self: Unpin`; reborrow once so `files` and `shared` can be used together
        let this = &mut *self;

        loop {
            // Pick up the files writers have created since the last poll, in
            // creation order.
            {
                let mut shared = this.shared.lock();
                while let Some(file_shared) = shared.new_files.pop_front() {
                    let spill_manager = Arc::clone(&shared.spill_manager);
                    this.files.push_back(SpillPoolFile {
                        shared: file_shared,
                        reader: None,
                        spill_manager,
                    });
                }
            } // Lock released here

            // Poll the files oldest first and return the first available batch.
            // A file that is finished and fully read is dropped, which releases
            // its disk space. A file that is only waiting for its writer has
            // registered its own waker, so we move on to the next file instead
            // of waiting on it.
            let mut idx = 0;
            while idx < this.files.len() {
                match this.files[idx].poll_file(cx) {
                    FilePoll::Item(item) => {
                        // Got a batch (or an error), return it
                        return Poll::Ready(Some(item));
                    }
                    FilePoll::Done => {
                        // File stream exhausted
                        // Check if this file is marked as writer_finished
                        let writer_finished =
                            { this.files[idx].shared.lock().writer_finished };

                        if writer_finished {
                            // File is complete, drop it and move on to the next
                            this.files.remove(idx);
                        } else {
                            // Stream exhausted but writer not finished - unexpected
                            // This shouldn't happen with proper coordination
                            return Poll::Ready(None);
                        }
                    }
                    FilePoll::Pending => {
                        // The oldest file with unread batches is still being read
                        // from disk. Wait for it rather than skipping ahead, so
                        // batches come back in file order (FIFO for one writer).
                        return Poll::Pending;
                    }
                    FilePoll::CaughtUp => {
                        // Nothing to read here until its writer appends more; a
                        // newer file may have unread batches, so try the next one
                        idx += 1;
                    }
                }
            }

            // No known file has an unread batch
            let mut shared = this.shared.lock();

            if !shared.new_files.is_empty() {
                // A writer created a file while we were polling; pick it up
                continue;
            }

            if this.files.is_empty() && shared.remaining_writer_count == 0 {
                // Writer is done and no more files will be added - EOF
                return Poll::Ready(None);
            }

            // Writer still active: register the pool-level waker so we are
            // notified when a new file is created or the last writer is dropped.
            // Each pending file has registered its own waker for new batches.
            shared.register_waker(cx.waker().clone());
            return Poll::Pending;
        }
    }
}

impl RecordBatchStream for SpillPoolReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::instant::Instant;
    use datafusion_common::{DataFusionError, exec_datafusion_err};
    use datafusion_common_runtime::{JoinSet, SpawnedTask};
    use datafusion_execution::disk_manager::{
        DiskManager, DiskManagerBuilder, DiskManagerMode,
    };
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_execution::{SpillFile, SpillWriter, TempFileFactory};
    use std::pin::Pin;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::{self, Sender};
    use std::time::Duration;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn create_test_batch(start: i32, count: usize) -> RecordBatch {
        let schema = create_test_schema();
        let a: ArrayRef = Arc::new(Int32Array::from(
            (start..start + count as i32).collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, vec![a]).unwrap()
    }

    fn create_spill_channel(
        max_file_size: usize,
    ) -> (SpillPoolSink, SendableRecordBatchStream) {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = create_test_schema();
        let spill_manager = Arc::new(SpillManager::new(env, metrics, schema));

        spsc_channel(max_file_size, spill_manager)
    }

    fn create_shared_spill_channel(
        max_file_size: usize,
    ) -> (SpillPoolWriter, SendableRecordBatchStream) {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = create_test_schema();
        let spill_manager = Arc::new(SpillManager::new(env, metrics, schema));

        mpsc_channel(max_file_size, spill_manager)
    }

    fn create_spill_channel_with_metrics(
        max_file_size: usize,
    ) -> (SpillPoolSink, SendableRecordBatchStream, SpillMetrics) {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = create_test_schema();
        let spill_manager = Arc::new(SpillManager::new(env, metrics.clone(), schema));

        let (writer, reader) = spsc_channel(max_file_size, spill_manager);
        (writer, reader, metrics)
    }

    #[tokio::test]
    async fn test_basic_write_and_read() -> Result<()> {
        let (writer, mut reader) = create_spill_channel(1024 * 1024);

        // Write one batch
        let batch1 = create_test_batch(0, 10);
        writer.push_batch(&batch1)?;

        // Read the batch
        let result = reader.next().await.unwrap()?;
        assert_eq!(result.num_rows(), 10);

        // Write another batch
        let batch2 = create_test_batch(10, 5);
        writer.push_batch(&batch2)?;
        // Read the second batch
        let result = reader.next().await.unwrap()?;
        assert_eq!(result.num_rows(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_single_batch_write_read() -> Result<()> {
        let (writer, mut reader) = create_spill_channel(1024 * 1024);

        // Write one batch
        let batch = create_test_batch(0, 5);
        writer.push_batch(&batch)?;

        // Read it back
        let result = reader.next().await.unwrap()?;
        assert_eq!(result.num_rows(), 5);

        // Verify the actual data
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 0);
        assert_eq!(col.value(4), 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_batches_sequential() -> Result<()> {
        let (writer, mut reader) = create_spill_channel(1024 * 1024);

        // Write multiple batches
        for i in 0..5 {
            let batch = create_test_batch(i * 10, 10);
            writer.push_batch(&batch)?;
        }

        // Read all batches and verify FIFO order
        for i in 0..5 {
            let result = reader.next().await.unwrap()?;
            assert_eq!(result.num_rows(), 10);

            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), i * 10, "Batch {i} not in FIFO order");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_writer() -> Result<()> {
        let (_writer, reader) = create_spill_channel(1024 * 1024);

        // Reader should pend since no batches were written
        let mut reader = reader;
        let result =
            tokio::time::timeout(Duration::from_millis(100), reader.next()).await;

        assert!(result.is_err(), "Reader should timeout on empty writer");

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_batch_skipping() -> Result<()> {
        let (writer, mut reader) = create_spill_channel(1024 * 1024);

        // Write empty batch
        let empty_batch = create_test_batch(0, 0);
        writer.push_batch(&empty_batch)?;

        // Write non-empty batch
        let batch = create_test_batch(0, 5);
        writer.push_batch(&batch)?;

        // Should only read the non-empty batch
        let result = reader.next().await.unwrap()?;
        assert_eq!(result.num_rows(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_rotation_triggered_by_size() -> Result<()> {
        // Set a small max_file_size to trigger rotation after one batch
        let batch1 = create_test_batch(0, 10);
        let batch_size = batch1.get_array_memory_size() + 1;

        let (writer, mut reader, metrics) = create_spill_channel_with_metrics(batch_size);

        // Write first batch (should fit in first file)
        writer.push_batch(&batch1)?;

        // Check metrics after first batch - file created but not finalized yet
        assert_eq!(
            metrics.spill_file_count.value(),
            1,
            "Should have created 1 file after first batch"
        );
        assert_eq!(
            metrics.spilled_bytes.value(),
            320,
            "Spilled bytes should reflect data written (header + 1 batch)"
        );
        assert_eq!(
            metrics.spilled_rows.value(),
            10,
            "Should have spilled 10 rows from first batch"
        );

        // Write second batch (should trigger rotation - finalize first file)
        let batch2 = create_test_batch(10, 10);
        assert!(
            batch2.get_array_memory_size() <= batch_size,
            "batch2 size {} exceeds limit {batch_size}",
            batch2.get_array_memory_size(),
        );
        assert!(
            batch1.get_array_memory_size() + batch2.get_array_memory_size() > batch_size,
            "Combined size {} does not exceed limit to trigger rotation",
            batch1.get_array_memory_size() + batch2.get_array_memory_size()
        );
        writer.push_batch(&batch2)?;

        // Check metrics after rotation - first file finalized, but second file not created yet
        // (new file created lazily on next push_batch call)
        assert_eq!(
            metrics.spill_file_count.value(),
            1,
            "Should still have 1 file (second file not created until next write)"
        );
        assert!(
            metrics.spilled_bytes.value() > 0,
            "Spilled bytes should be > 0 after first file finalized (got {})",
            metrics.spilled_bytes.value()
        );
        assert_eq!(
            metrics.spilled_rows.value(),
            20,
            "Should have spilled 20 total rows (10 + 10)"
        );

        // Write a third batch to confirm rotation occurred (creates second file)
        let batch3 = create_test_batch(20, 5);
        writer.push_batch(&batch3)?;

        // Now check that second file was created
        assert_eq!(
            metrics.spill_file_count.value(),
            2,
            "Should have created 2 files after writing to new file"
        );
        assert_eq!(
            metrics.spilled_rows.value(),
            25,
            "Should have spilled 25 total rows (10 + 10 + 5)"
        );

        // Read all three batches
        let result1 = reader.next().await.unwrap()?;
        assert_eq!(result1.num_rows(), 10);

        let result2 = reader.next().await.unwrap()?;
        assert_eq!(result2.num_rows(), 10);

        let result3 = reader.next().await.unwrap()?;
        assert_eq!(result3.num_rows(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_rotations() -> Result<()> {
        let batches = (0..10)
            .map(|i| create_test_batch(i * 10, 10))
            .collect::<Vec<_>>();

        let batch_size = batches[0].get_array_memory_size() * 2 + 1;

        // Very small max_file_size to force frequent rotations
        let (writer, mut reader, metrics) = create_spill_channel_with_metrics(batch_size);

        // Write many batches to cause multiple rotations
        for i in 0..10 {
            let batch = create_test_batch(i * 10, 10);
            writer.push_batch(&batch)?;
        }

        // Check metrics after all writes - should have multiple files due to rotations
        // With batch_size = 2 * one_batch + 1, each file fits ~2 batches before rotating
        // 10 batches should create multiple files (exact count depends on rotation timing)
        let file_count = metrics.spill_file_count.value();
        assert!(
            file_count >= 4,
            "Should have created at least 4 files with multiple rotations (got {file_count})"
        );
        assert!(
            metrics.spilled_bytes.value() > 0,
            "Spilled bytes should be > 0 after rotations (got {})",
            metrics.spilled_bytes.value()
        );
        assert_eq!(
            metrics.spilled_rows.value(),
            100,
            "Should have spilled 100 total rows (10 batches * 10 rows)"
        );

        // Read all batches and verify order
        for i in 0..10 {
            let result = reader.next().await.unwrap()?;
            assert_eq!(result.num_rows(), 10);

            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(
                col.value(0),
                i * 10,
                "Batch {i} not in correct order after rotations"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_single_batch_larger_than_limit() -> Result<()> {
        // Very small limit
        let (writer, mut reader, metrics) = create_spill_channel_with_metrics(100);

        // Write a batch that exceeds the limit
        let large_batch = create_test_batch(0, 100);
        writer.push_batch(&large_batch)?;

        // Check metrics after large batch - should trigger rotation immediately
        assert_eq!(
            metrics.spill_file_count.value(),
            1,
            "Should have created 1 file for large batch"
        );
        assert_eq!(
            metrics.spilled_rows.value(),
            100,
            "Should have spilled 100 rows from large batch"
        );

        // Should still write and read successfully
        let result = reader.next().await.unwrap()?;
        assert_eq!(result.num_rows(), 100);

        // Next batch should go to a new file
        let batch2 = create_test_batch(100, 10);
        writer.push_batch(&batch2)?;

        // Check metrics after second batch - should have rotated to a new file
        assert_eq!(
            metrics.spill_file_count.value(),
            2,
            "Should have created 2 files after rotation"
        );
        assert_eq!(
            metrics.spilled_rows.value(),
            110,
            "Should have spilled 110 total rows (100 + 10)"
        );

        let result2 = reader.next().await.unwrap()?;
        assert_eq!(result2.num_rows(), 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_very_small_max_file_size() -> Result<()> {
        // Test with just 1 byte max (extreme case)
        let (writer, mut reader) = create_spill_channel(1);

        // Any batch will exceed this limit
        let batch = create_test_batch(0, 5);
        writer.push_batch(&batch)?;

        // Should still work
        let result = reader.next().await.unwrap()?;
        assert_eq!(result.num_rows(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_exact_size_boundary() -> Result<()> {
        // Create a batch and measure its approximate size
        let batch = create_test_batch(0, 10);
        let batch_size = batch.get_array_memory_size();

        // Set max_file_size to exactly the batch size
        let (writer, mut reader, metrics) = create_spill_channel_with_metrics(batch_size);

        // Write first batch (exactly at the size limit)
        writer.push_batch(&batch)?;

        // Check metrics after first batch - should NOT rotate yet (size == limit, not >)
        assert_eq!(
            metrics.spill_file_count.value(),
            1,
            "Should have created 1 file after first batch at exact boundary"
        );
        assert_eq!(
            metrics.spilled_rows.value(),
            10,
            "Should have spilled 10 rows from first batch"
        );

        // Write second batch (exceeds the limit, should trigger rotation)
        let batch2 = create_test_batch(10, 10);
        writer.push_batch(&batch2)?;

        // Check metrics after second batch - rotation triggered, first file finalized
        // Note: second file not created yet (lazy creation on next write)
        assert_eq!(
            metrics.spill_file_count.value(),
            1,
            "Should still have 1 file after rotation (second file created lazily)"
        );
        assert_eq!(
            metrics.spilled_rows.value(),
            20,
            "Should have spilled 20 total rows (10 + 10)"
        );
        // Verify first file was finalized by checking spilled_bytes
        assert!(
            metrics.spilled_bytes.value() > 0,
            "Spilled bytes should be > 0 after file finalization (got {})",
            metrics.spilled_bytes.value()
        );

        // Both should be readable
        let result1 = reader.next().await.unwrap()?;
        assert_eq!(result1.num_rows(), 10);

        let result2 = reader.next().await.unwrap()?;
        assert_eq!(result2.num_rows(), 10);

        // Spill another batch, now we should see the second file created
        let batch3 = create_test_batch(20, 5);
        writer.push_batch(&batch3)?;
        assert_eq!(
            metrics.spill_file_count.value(),
            2,
            "Should have created 2 files after writing to new file"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_reader_writer() -> Result<()> {
        let (writer, mut reader) = create_spill_channel(1024 * 1024);

        // Spawn writer task
        let writer_handle = SpawnedTask::spawn(async move {
            for i in 0..10 {
                let batch = create_test_batch(i * 10, 10);
                writer.push_batch(&batch).unwrap();
                // Small delay to simulate real concurrent work
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        // Reader task (runs concurrently)
        let reader_handle = SpawnedTask::spawn(async move {
            let mut count = 0;
            for i in 0..10 {
                let result = reader.next().await.unwrap().unwrap();
                assert_eq!(result.num_rows(), 10);

                let col = result
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(col.value(0), i * 10);
                count += 1;
            }
            count
        });

        // Wait for both to complete
        writer_handle.await.unwrap();
        let batches_read = reader_handle.await.unwrap();
        assert_eq!(batches_read, 10);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_concurrent_writers() -> Result<()> {
        let (writer, mut reader) = create_shared_spill_channel(1024 * 1024);

        // Spawn writer tasks
        let mut writer_join_set = JoinSet::new();
        for w in 0..10 {
            let writer = writer.clone();
            writer_join_set.spawn(async move {
                for b in 0..10 {
                    let batch = create_test_batch((w * 100) + (b * 10), 10);
                    writer.push_batch(&batch).unwrap();
                }
            });
        }
        drop(writer);

        // Reader task (runs concurrently)
        let reader_handle = SpawnedTask::spawn(async move {
            let mut batch_order = vec![];
            loop {
                match reader.next().await {
                    None => break,
                    Some(batch) => {
                        let batch = batch.unwrap();

                        assert_eq!(batch.num_rows(), 10);

                        let col = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        batch_order.push(col.value(0) / 10);
                    }
                }
            }
            batch_order
        });

        // Wait for both to complete
        writer_join_set.join_all().await;
        let mut batch_order = reader_handle.await.unwrap();

        // When used with multiple writers, order is not guaranteed
        batch_order.sort_unstable();
        assert_eq!(batch_order, (0i32..100i32).collect::<Vec<_>>());

        Ok(())
    }

    #[tokio::test]
    async fn test_reader_catches_up_to_writer() -> Result<()> {
        let (writer, mut reader) = create_spill_channel(1024 * 1024);

        let (reader_waiting_tx, reader_waiting_rx) = tokio::sync::oneshot::channel();
        let (first_read_done_tx, first_read_done_rx) = tokio::sync::oneshot::channel();

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum ReadWriteEvent {
            ReadStart,
            Read(usize),
            Write(usize),
        }

        let events = Arc::new(Mutex::new(vec![]));
        // Start reader first (will pend)
        let reader_events = Arc::clone(&events);
        let reader_handle = SpawnedTask::spawn(async move {
            reader_events.lock().push(ReadWriteEvent::ReadStart);
            reader_waiting_tx
                .send(())
                .expect("reader_waiting channel closed unexpectedly");
            let result = reader.next().await.unwrap().unwrap();
            reader_events
                .lock()
                .push(ReadWriteEvent::Read(result.num_rows()));
            first_read_done_tx
                .send(())
                .expect("first_read_done channel closed unexpectedly");
            let result = reader.next().await.unwrap().unwrap();
            reader_events
                .lock()
                .push(ReadWriteEvent::Read(result.num_rows()));
        });

        // Wait until the reader is pending on the first batch
        reader_waiting_rx
            .await
            .expect("reader should signal when waiting");

        // Now write a batch (should wake the reader)
        let batch = create_test_batch(0, 5);
        events.lock().push(ReadWriteEvent::Write(batch.num_rows()));
        writer.push_batch(&batch)?;

        // Wait for the reader to finish the first read before allowing the
        // second write. This ensures deterministic ordering of events:
        // 1. The reader starts and pends on the first `next()`
        // 2. The first write wakes the reader
        // 3. The reader processes the first batch and signals completion
        // 4. The second write is issued, ensuring consistent event ordering
        first_read_done_rx
            .await
            .expect("reader should signal when first read completes");

        // Write another batch
        let batch = create_test_batch(5, 10);
        events.lock().push(ReadWriteEvent::Write(batch.num_rows()));
        writer.push_batch(&batch)?;

        // Reader should complete
        reader_handle.await.unwrap();
        let events = events.lock().clone();
        assert_eq!(
            events,
            vec![
                ReadWriteEvent::ReadStart,
                ReadWriteEvent::Write(5),
                ReadWriteEvent::Read(5),
                ReadWriteEvent::Write(10),
                ReadWriteEvent::Read(10)
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reader_starts_after_writer_finishes() -> Result<()> {
        let (writer, reader) = create_spill_channel(128);

        // Writer writes all data
        for i in 0..5 {
            let batch = create_test_batch(i * 10, 10);
            writer.push_batch(&batch)?;
        }

        drop(writer);

        // Now start reader
        let mut reader = reader;
        let mut count = 0;
        for i in 0..5 {
            let result = reader.next().await.unwrap()?;
            assert_eq!(result.num_rows(), 10);

            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), i * 10);
            count += 1;
        }

        assert_eq!(count, 5, "Should read all batches after writer finishes");

        Ok(())
    }

    #[tokio::test]
    async fn test_writer_drop_finalizes_file() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = create_test_schema();
        let spill_manager =
            Arc::new(SpillManager::new(Arc::clone(&env), metrics.clone(), schema));

        let (writer, mut reader) = spsc_channel(1024 * 1024, spill_manager);

        // Write some batches
        for i in 0..5 {
            let batch = create_test_batch(i * 10, 10);
            writer.push_batch(&batch)?;
        }

        // Check metrics before drop - spilled_bytes already reflects written data
        let spilled_bytes_before = metrics.spilled_bytes.value();
        assert_eq!(
            spilled_bytes_before, 1088,
            "Spilled bytes should reflect data written (header + 5 batches)"
        );

        // Explicitly drop the writer - this should finalize the current file
        drop(writer);

        // Check metrics after drop - spilled_bytes should be > 0 now
        let spilled_bytes_after = metrics.spilled_bytes.value();
        assert!(
            spilled_bytes_after > 0,
            "Spilled bytes should be > 0 after writer is dropped (got {spilled_bytes_after})"
        );

        // Verify reader can still read all batches
        let mut count = 0;
        for i in 0..5 {
            let result = reader.next().await.unwrap()?;
            assert_eq!(result.num_rows(), 10);

            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), i * 10);
            count += 1;
        }

        assert_eq!(count, 5, "Should read all batches after writer is dropped");

        Ok(())
    }

    /// Verifies that the reader stays alive as long as any writer clone exists.
    ///
    /// `SpillPoolWriter` is `Clone`, and in non-preserve-order repartitioning
    /// mode multiple input partition tasks share clones of the same writer.
    /// The reader must not see EOF until **all** clones have been dropped,
    /// even if the queue is temporarily empty between writes from different
    /// clones.
    ///
    /// The test sequence is:
    ///
    /// 1. writer1 writes a batch, then is dropped.
    /// 2. The reader consumes that batch (queue is now empty).
    /// 3. writer2 (still alive) writes a batch.
    /// 4. The reader must see that batch.
    /// 5. EOF is only signalled after writer2 is also dropped.
    #[tokio::test]
    async fn test_clone_drop_does_not_signal_eof_prematurely() -> Result<()> {
        let (writer1, mut reader) = create_shared_spill_channel(1024 * 1024);
        let writer2 = writer1.clone();

        // Synchronization: tell writer2 when it may proceed.
        let (proceed_tx, proceed_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn writer2 — it waits for the signal before writing.
        let writer2_handle = SpawnedTask::spawn(async move {
            proceed_rx.await.unwrap();
            writer2.push_batch(&create_test_batch(10, 10)).unwrap();
            // writer2 is dropped here (last clone → true EOF)
        });

        // Writer1 writes one batch, then drops.
        writer1.push_batch(&create_test_batch(0, 10))?;
        drop(writer1);

        // Read writer1's batch.
        let batch1 = reader.next().await.unwrap()?;
        assert_eq!(batch1.num_rows(), 10);
        let col = batch1
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 0);

        // Signal writer2 to write its batch. It will execute when the
        // current task yields (i.e. when reader.next() returns Pending).
        proceed_tx.send(()).unwrap();

        // The reader should wait (Pending) for writer2's data, not EOF.
        let batch2 = tokio::time::timeout(Duration::from_secs(5), reader.next())
            .await
            .expect("Reader timed out — should not hang");

        assert!(
            batch2.is_some(),
            "Reader must not return EOF while a writer clone is still alive"
        );
        let batch2 = batch2.unwrap()?;
        assert_eq!(batch2.num_rows(), 10);
        let col = batch2
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 10);

        writer2_handle.await.unwrap();

        // All writers dropped — reader should see real EOF now.
        assert!(reader.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_disk_usage_decreases_as_files_consumed() -> Result<()> {
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        // Test configuration
        const NUM_BATCHES: usize = 3;
        const ROWS_PER_BATCH: usize = 100;

        // Step 1: Create a test batch and measure its size
        let batch = create_test_batch(0, ROWS_PER_BATCH);
        let batch_size = batch.get_array_memory_size();

        // Step 2: Configure file rotation to approximately 1 batch per file
        // Create a custom RuntimeEnv so we can access the DiskManager
        let runtime = Arc::new(RuntimeEnvBuilder::default().build()?);
        let disk_manager = Arc::clone(&runtime.disk_manager);

        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = create_test_schema();
        let spill_manager = Arc::new(SpillManager::new(runtime, metrics.clone(), schema));

        let (writer, mut reader) = spsc_channel(batch_size - 1, spill_manager);

        // Step 3: Write NUM_BATCHES batches to create approximately NUM_BATCHES files
        for i in 0..NUM_BATCHES {
            let start = (i * ROWS_PER_BATCH) as i32;
            writer.push_batch(&create_test_batch(start, ROWS_PER_BATCH))?;
        }

        // Check how many files were created (should be at least a few due to file rotation)
        let file_count = metrics.spill_file_count.value();
        assert_eq!(
            file_count, NUM_BATCHES,
            "Expected at {NUM_BATCHES} files with rotation, got {file_count}"
        );

        // Step 4: Verify initial disk usage reflects all files
        let initial_disk_usage = disk_manager.used_disk_space();
        assert!(
            initial_disk_usage > 0,
            "Expected disk usage > 0 after writing batches, got {initial_disk_usage}"
        );

        // Step 5: Read NUM_BATCHES - 1 batches (all but 1)
        // As each file is fully consumed, it should be dropped and disk usage should decrease
        for i in 0..(NUM_BATCHES - 1) {
            let result = reader.next().await.unwrap()?;
            assert_eq!(result.num_rows(), ROWS_PER_BATCH);

            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), (i * ROWS_PER_BATCH) as i32);
        }

        // Step 6: Verify disk usage decreased but is not zero (at least 1 batch remains)
        let partial_disk_usage = disk_manager.used_disk_space();
        assert!(
            partial_disk_usage > 0
                && partial_disk_usage < (batch_size * NUM_BATCHES * 2) as u64,
            "Disk usage should be > 0 with remaining batches"
        );
        assert!(
            partial_disk_usage < initial_disk_usage,
            "Disk usage should have decreased after reading most batches: initial={initial_disk_usage}, partial={partial_disk_usage}"
        );

        // Step 7: Read the final batch
        let result = reader.next().await.unwrap()?;
        assert_eq!(result.num_rows(), ROWS_PER_BATCH);

        // Step 8: Drop writer first to signal no more data will be written
        // The reader has infinite stream semantics and will wait for the writer
        // to be dropped before returning None
        drop(writer);

        // Verify we've read all batches - now the reader should return None
        assert!(
            reader.next().await.is_none(),
            "Should have no more batches to read"
        );

        // Step 9: Drop reader to release all references
        drop(reader);

        // Step 10: Verify complete cleanup - disk usage should be 0
        let final_disk_usage = disk_manager.used_disk_space();
        assert_eq!(
            final_disk_usage, 0,
            "Disk usage should be 0 after all files dropped, got {final_disk_usage}"
        );

        Ok(())
    }

    type WriteHook = Arc<dyn Fn() + Send + Sync>;
    /// What a read of a spill file does. The two failure modes reach the two
    /// different error paths of [`SpillPoolFile::poll_file`]: one fails while it
    /// builds the stream, the other fails while it polls a stream that is
    /// already built.
    enum ReadBehavior {
        /// Read the real bytes, after this delay.
        Delay(Duration),
        /// Fail to open the file, so the stream is never built.
        FailOpen(DataFusionError),
        /// Open the file, then give this error as the first item of the stream.
        FailFirstItem(DataFusionError),
    }

    type ReadHook = Arc<dyn Fn() -> ReadBehavior + Send + Sync>;

    /// Test double for a spill file that runs `write_hook` before every disk
    /// write or flush, and consults `read_hook` before every read. Writers write
    /// while holding their file's lock, so a hook that blocks pauses a
    /// `push_batch` at exactly the point where it has a file checked out for
    /// writing; a read delay makes an older file's bytes arrive after a newer
    /// file's, and a read error fails the read. Data still goes to real
    /// temporary files.
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
            let delay = match (self.read_hook)() {
                ReadBehavior::Delay(delay) => delay,
                ReadBehavior::FailOpen(e) => return Err(e),
                ReadBehavior::FailFirstItem(e) => {
                    return Ok(Box::pin(futures::stream::once(async move { Err(e) })));
                }
            };
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

    /// Holds the first disk write on the pool until the test releases it, and
    /// reports every other write so a test can wait for a second writer to
    /// reach a file of its own instead of sleeping.
    struct WriteGate {
        taken: AtomicBool,
        entered: Barrier,
        release: Barrier,
        other_writes: Sender<()>,
    }

    impl WriteGate {
        fn hold_if_first(&self) {
            if self.taken.swap(true, Ordering::SeqCst) {
                // A write only gets here once its writer holds a file of its
                // own: a writer that had to wait for the held writer's file
                // lock would not reach a write of its own at all.
                let _ = self.other_writes.send(());
            } else {
                self.entered.wait();
                self.release.wait();
            }
        }
    }

    /// Regression test for <https://github.com/apache/datafusion/issues/24883>.
    ///
    /// Two writers push concurrently. The first writer is held inside its first
    /// write to disk (holding its file's lock) while the second writer pushes.
    /// On the unfixed pool that leaves two open files with one batch each, and
    /// the reader, which only ever drained the oldest file, parked on that file
    /// once it had read its single batch even though the second batch was on
    /// disk. `RepartitionExec` blocks on this stream once per pushed batch and,
    /// with its channel gate closed, cannot push anything else to wake it.
    ///
    /// The reader must yield both batches while both writers are still alive.
    #[tokio::test]
    async fn test_reader_does_not_wait_on_drained_file_while_another_has_data()
    -> Result<()> {
        let (other_writes_tx, other_writes) = mpsc::channel();
        let gate = Arc::new(WriteGate {
            taken: AtomicBool::new(false),
            entered: Barrier::new(2),
            release: Barrier::new(2),
            other_writes: other_writes_tx,
        });
        let write_hook: WriteHook = {
            let gate = Arc::clone(&gate);
            Arc::new(move || gate.hold_if_first())
        };
        let (spill_manager, _disk_manager) = spill_manager_with_io_hooks(
            write_hook,
            Arc::new(|| ReadBehavior::Delay(Duration::ZERO)),
        )?;

        let (writer1, mut reader) = mpsc_channel(1024 * 1024, Arc::clone(&spill_manager));
        let writer2 = writer1.clone();

        // Writer 1 pushes and is held inside its first disk write, holding its
        // file's lock. The writers run on plain threads because `push_batch`
        // blocks.
        let writer1 = std::thread::spawn(move || {
            writer1.push_batch(&create_test_batch(0, 10)).unwrap();
            writer1
        });
        gate.entered.wait();

        // Writer 2 pushes while writer 1 is held.
        let writer2 = std::thread::spawn(move || {
            writer2.push_batch(&create_test_batch(10, 10)).unwrap();
            writer2
        });
        // Wait for writer 2 to reach a write of its own, so that writer 1 is
        // only released once the pool really holds two open files. Sleeping
        // instead would let a loaded runner schedule writer 2 after writer 1
        // was released, and writer 2 would then reuse writer 1's returned file:
        // a single-file run that says nothing about the case under test.
        other_writes
            .recv_timeout(Duration::from_secs(30))
            .expect("writer 2 must write to a file of its own while writer 1 is held");
        gate.release.wait();
        let writer1 = writer1.join().unwrap();
        let writer2 = writer2.join().unwrap();
        assert_eq!(
            spill_manager.metrics.spill_file_count.value(),
            2,
            "the writers must have written to two different files"
        );

        // Both batches are on disk and both writers are still alive, so no file
        // is finished. The reader must yield both batches without waiting for a
        // writer.
        let mut values = vec![];
        for _ in 0..2 {
            let batch = tokio::time::timeout(Duration::from_secs(5), reader.next())
                .await
                .expect(
                    "reader waited on a drained file while another file had an unread batch",
                )
                .expect("reader must not signal EOF while writers are alive")?;
            assert_eq!(batch.num_rows(), 10);
            values.push(id_of(&batch));
        }
        // Multiple writers: the order between their batches is not guaranteed.
        values.sort_unstable();
        assert_eq!(values, vec![0, 10]);

        // Only once every writer is gone does the reader signal EOF.
        drop(writer1);
        drop(writer2);
        assert!(reader.next().await.is_none());

        Ok(())
    }

    /// A spill file that cannot be read must fail the reader rather than stall
    /// it. `RepartitionExec` blocks on this stream after every spilled marker,
    /// so an error that is swallowed here would hang the query exactly like the
    /// deadlock this module guards against.
    ///
    /// Both error paths of `SpillPoolFile::poll_file` are covered: the file that
    /// does not open, so the failure comes from the construction of the stream,
    /// and the file that opens and then gives an error, so the failure comes
    /// from a poll of a stream that the reader already holds.
    #[tokio::test]
    async fn test_read_errors_are_reported_to_the_reader() -> Result<()> {
        async fn assert_reader_reports(behavior: fn() -> ReadBehavior) -> Result<()> {
            let (spill_manager, _disk_manager) =
                spill_manager_with_io_hooks(Arc::new(|| {}), Arc::new(behavior))?;
            let (writer, mut reader) = spsc_channel(1024 * 1024, spill_manager);

            // Nothing reads the file until the reader does, so every read that
            // the pool makes is a read of the batch below.
            writer.push_batch(&create_test_batch(0, 10))?;

            let item = tokio::time::timeout(Duration::from_secs(5), reader.next())
                .await
                .expect("reader must report the read error instead of waiting")
                .expect("reader must report the read error instead of signalling EOF");
            let err = item.expect_err("a failed read must not produce a batch");
            assert!(
                err.to_string().contains("injected spill read failure"),
                "unexpected error: {err}"
            );
            Ok(())
        }

        assert_reader_reports(|| {
            ReadBehavior::FailOpen(exec_datafusion_err!("injected spill read failure"))
        })
        .await?;
        assert_reader_reports(|| {
            ReadBehavior::FailFirstItem(exec_datafusion_err!(
                "injected spill read failure"
            ))
        })
        .await?;

        Ok(())
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

    /// One randomly generated scenario, see [`spill_pool_scenario_fuzz`].
    async fn run_spill_pool_scenario(seed: u64) -> Result<()> {
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
                    ReadBehavior::Delay(Duration::ZERO)
                } else {
                    ReadBehavior::Delay(Duration::from_millis(rng.random_range(1..=20)))
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

    /// Randomized scenarios against the pool's contract: after any set of
    /// overlapping pushes the reader can drain everything without further
    /// writer activity (no lost wakeups, no parking on one file while another
    /// has data), it never signals EOF while a writer is alive, it returns
    /// exactly the pushed batches (in order for a single writer), and the
    /// files are released when it is dropped.
    ///
    /// `DATAFUSION_SPILL_POOL_FUZZ_ITERATIONS` and
    /// `DATAFUSION_SPILL_POOL_FUZZ_SEED` select how many scenarios run and
    /// from which seed; a failure names its seed so it can be replayed.
    #[tokio::test]
    async fn spill_pool_scenario_fuzz() -> Result<()> {
        let env_u64 = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let first_seed = env_u64("DATAFUSION_SPILL_POOL_FUZZ_SEED", 0);
        let iterations = env_u64("DATAFUSION_SPILL_POOL_FUZZ_ITERATIONS", 50);
        // `saturating_add` so a replay seed near `u64::MAX` cannot overflow.
        for seed in first_seed..first_seed.saturating_add(iterations) {
            run_spill_pool_scenario(seed).await?;
        }
        Ok(())
    }
}
