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
use std::sync::Arc;
use std::task::Waker;

use parking_lot::Mutex;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};

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
    /// Queue of ALL files (including the current write file if it exists).
    /// Readers always read from the front of this queue (FIFO).
    /// Each file has its own lock to enable concurrent reader/writer access.
    files: VecDeque<Arc<Mutex<ActiveSpillFileShared>>>,
    /// SpillManager for creating files and tracking metrics
    spill_manager: Arc<SpillManager>,
    /// Pool-level waker to notify when new files are available (single reader)
    waker: Option<Waker>,
    /// Whether the writer has been dropped (no more files will be added)
    writer_dropped: bool,
    /// Writer's reference to the current file (shared by all cloned writers).
    /// Has its own lock to allow I/O without blocking queue access.
    current_write_file: Option<Arc<Mutex<ActiveSpillFileShared>>>,
}

impl SpillPoolShared {
    /// Creates a new shared pool state
    fn new(spill_manager: Arc<SpillManager>) -> Self {
        Self {
            files: VecDeque::new(),
            spill_manager,
            waker: None,
            writer_dropped: false,
            current_write_file: None,
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

/// Writer for a spill pool. Provides coordinated write access with FIFO semantics.
///
/// Created by [`channel`]. See that function for architecture diagrams and usage examples.
///
/// The writer is `Clone`, allowing multiple writers to coordinate on the same pool.
/// All clones share the same current write file and coordinate file rotation.
/// The writer automatically manages file rotation based on the `max_file_size_bytes`
/// configured in [`channel`]. When the last writer clone is dropped, it finalizes the
/// current file so readers can access all written data.
#[derive(Clone)]
pub struct SpillPoolWriter {
    /// Maximum size in bytes before rotating to a new file.
    /// Typically set from configuration `datafusion.execution.max_spill_file_size_bytes`.
    max_file_size_bytes: usize,
    /// Shared state with readers (includes current_write_file for coordination)
    shared: Arc<Mutex<SpillPoolShared>>,
}

impl SpillPoolWriter {
    /// Spills a batch to the pool, rotating files when necessary.
    ///
    /// If the current file would exceed `max_file_size_bytes` after adding
    /// this batch, the file is finalized and a new one is started.
    ///
    /// See [`channel`] for overall architecture and examples.
    ///
    /// # File Rotation Logic
    ///
    /// ```text
    /// push_batch()
    ///      │
    ///      ▼
    /// Current file exists?
    ///      │
    ///      ├─ No ──▶ Create new file ──▶ Add to shared queue
    ///      │                               Wake readers
    ///      ▼
    /// Write batch to current file
    ///      │
    ///      ▼
    /// estimated_size > max_file_size_bytes?
    ///      │
    ///      ├─ No ──▶ Keep current file for next batch
    ///      │
    ///      ▼
    /// Yes: finish() current file
    ///      Mark writer_finished = true
    ///      Wake readers
    ///      │
    ///      ▼
    /// Next push_batch() creates new file
    /// ```
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

        // Create new file if we don't have one yet
        if shared.current_write_file.is_none() {
            let spill_manager = Arc::clone(&shared.spill_manager);
            // Release shared lock before disk I/O (fine-grained locking)
            drop(shared);

            let writer = spill_manager.create_in_progress_file("SpillPool")?;
            // Clone the file so readers can access it immediately
            let file = writer.file().expect("InProgressSpillFile should always have a file when it is first created").clone();

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
            shared.files.push_back(Arc::clone(&file_shared));
            shared.current_write_file = Some(file_shared);
            shared.wake(); // Wake readers waiting for new files
        }

        let current_write_file = shared.current_write_file.take();
        // Release shared lock before file I/O (fine-grained locking)
        // This allows readers to access the queue while we do disk I/O
        drop(shared);

        // Write batch to current file - lock only the specific file
        if let Some(current_file) = current_write_file {
            // Now lock just this file for I/O (separate from shared lock)
            let mut file_shared = current_file.lock();

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

            // Check if we need to rotate
            let needs_rotation = file_shared.estimated_size > self.max_file_size_bytes;

            if needs_rotation {
                // Finish the IPC writer
                if let Some(mut writer) = file_shared.writer.take() {
                    writer.finish()?;
                }
                // Mark as finished so readers know not to wait for more data
                file_shared.writer_finished = true;
                // Wake reader waiting on this file (it's now finished)
                file_shared.wake();
                // Don't put back current_write_file - let it rotate
            } else {
                // Release file lock
                drop(file_shared);
                // Put back the current file for further writing
                let mut shared = self.shared.lock();
                shared.current_write_file = Some(current_file);
            }
        }

        Ok(())
    }
}

impl Drop for SpillPoolWriter {
    fn drop(&mut self) {
        let mut shared = self.shared.lock();

        // Finalize the current file when the last writer is dropped
        if let Some(current_file) = shared.current_write_file.take() {
            // Release shared lock before locking file
            drop(shared);

            let mut file_shared = current_file.lock();

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
            shared = self.shared.lock();
        }

        // Mark writer as dropped and wake pool-level readers
        shared.writer_dropped = true;
        shared.wake();
    }
}

/// Creates a paired writer and reader for a spill pool with MPSC (multi-producer, single-consumer)
/// semantics.
///
/// This is the recommended way to create a spill pool. The writer is `Clone`, allowing
/// multiple producers to coordinate writes to the same pool. The reader can consume batches
/// in FIFO order. The reader can start reading immediately after a writer appends a batch
/// to the spill file, without waiting for the file to be sealed, while writers continue to
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
/// │  SpillPoolWriter    ┌────────────────────┐    SpillPoolReader           │
/// │       │             │  VecDeque<File>    │          │                   │
/// │       │             │  ┌────┐┌────┐      │          │                   │
/// │  push_batch()       │  │ F1 ││ F2 │ ...  │      next().await            │
/// │       │             │  └────┘└────┘      │          │                   │
/// │       ▼             │   (FIFO order)     │          ▼                   │
/// │  ┌─────────┐        │                    │    ┌──────────┐              │
/// │  │Current  │───────▶│ Coordination:      │◀───│ Current  │              │
/// │  │Write    │        │ - Wakers           │    │ Read     │              │
/// │  │File     │        │ - Batch counts     │    │ File     │              │
/// │  └─────────┘        │ - Writer status    │    └──────────┘              │
/// │       │             └────────────────────┘          │                   │
/// │       │                                              │                  │
/// │  Size > limit?                                Read all batches?         │
/// │       │                                              │                  │
/// │       ▼                                              ▼                  │
/// │  Rotate to new file                            Pop from queue           │
/// └─────────────────────────────────────────────────────────────────────────┘
///
/// Writer produces → Shared FIFO queue → Reader consumes
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
/// A tuple of `(SpillPoolWriter, SendableRecordBatchStream)` that share the same
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
/// let (writer, mut reader) = spill_pool::channel(1024 * 1024, spill_manager);
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
pub fn channel(
    max_file_size_bytes: usize,
    spill_manager: Arc<SpillManager>,
) -> (SpillPoolWriter, SendableRecordBatchStream) {
    let schema = Arc::clone(spill_manager.schema());
    let shared = Arc::new(Mutex::new(SpillPoolShared::new(spill_manager)));

    let writer = SpillPoolWriter {
        max_file_size_bytes,
        shared: Arc::clone(&shared),
    };

    let reader = SpillPoolReader::new(shared, schema);

    (writer, Box::pin(reader))
}

/// Shared state between writer and readers for an active spill file.
/// Protected by a Mutex to coordinate between concurrent readers and the writer.
struct ActiveSpillFileShared {
    /// Writer handle - taken (set to None) when finish() is called
    writer: Option<InProgressSpillFile>,
    /// The spill file, set when the writer finishes.
    /// Taken by the reader when creating a stream (the file stays open via file handles).
    file: Option<RefCountedTempFile>,
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

/// Reader state for a SpillFile (owned by individual SpillFile instances).
/// This is kept separate from the shared state to avoid holding locks during I/O.
struct SpillFileReader {
    /// The actual stream reading from disk
    stream: SendableRecordBatchStream,
    /// Number of batches this reader has consumed
    batches_read: usize,
}

struct SpillFile {
    /// Shared coordination state (contains writer and batch counts)
    shared: Arc<Mutex<ActiveSpillFileShared>>,
    /// Reader state (lazy-initialized, owned by this SpillFile)
    reader: Option<SpillFileReader>,
    /// Spill manager for creating readers
    spill_manager: Arc<SpillManager>,
}

impl Stream for SpillFile {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        // Step 1: Lock shared state and check coordination
        let (should_read, file) = {
            let mut shared = self.shared.lock();

            // Determine if we can read
            let batches_read = self.reader.as_ref().map_or(0, |r| r.batches_read);

            if batches_read < shared.batches_written {
                // More data available to read - take the file if we don't have a reader yet
                let file = if self.reader.is_none() {
                    shared.file.take()
                } else {
                    None
                };
                (true, file)
            } else if shared.writer_finished {
                // No more data and writer is done - EOF
                return Poll::Ready(None);
            } else {
                // Caught up to writer, but writer still active - register waker and wait
                shared.register_waker(cx.waker().clone());
                return Poll::Pending;
            }
        }; // Lock released here

        // Step 2: Lazy-create reader stream if needed
        if self.reader.is_none() && should_read {
            if let Some(file) = file {
                // we want this unbuffered because files are actively being written to
                match self
                    .spill_manager
                    .read_spill_as_stream_unbuffered(file, None)
                {
                    Ok(stream) => {
                        self.reader = Some(SpillFileReader {
                            stream,
                            batches_read: 0,
                        });
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                }
            } else {
                // File not available yet (writer hasn't finished or already taken)
                // Register waker and wait for file to be ready
                let mut shared = self.shared.lock();
                shared.register_waker(cx.waker().clone());
                return Poll::Pending;
            }
        }

        // Step 3: Poll the reader stream (no lock held)
        if let Some(reader) = &mut self.reader {
            match reader.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // Successfully read a batch - increment counter
                    reader.batches_read += 1;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // Stream exhausted unexpectedly
                    // This shouldn't happen if coordination is correct, but handle gracefully
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            // Should not reach here, but handle gracefully
            Poll::Ready(None)
        }
    }
}

/// A stream that reads from a SpillPool in FIFO order.
///
/// Created by [`channel`]. See that function for architecture diagrams and usage examples.
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
pub struct SpillPoolReader {
    /// Shared reference to the spill pool
    shared: Arc<Mutex<SpillPoolShared>>,
    /// Current SpillFile we're reading from
    current_file: Option<SpillFile>,
    /// Schema of the spilled data
    schema: SchemaRef,
}

impl SpillPoolReader {
    /// Creates a new reader from shared pool state.
    ///
    /// This is private - use the `channel()` function to create a reader/writer pair.
    ///
    /// # Arguments
    ///
    /// * `shared` - Shared reference to the pool state
    fn new(shared: Arc<Mutex<SpillPoolShared>>, schema: SchemaRef) -> Self {
        Self {
            shared,
            current_file: None,
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

        loop {
            // If we have a current file, try to read from it
            if let Some(ref mut file) = self.current_file {
                match file.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        // Got a batch, return it
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        // Error reading batch
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        // Current file stream exhausted
                        // Check if this file is marked as writer_finished
                        let writer_finished = { file.shared.lock().writer_finished };

                        if writer_finished {
                            // File is complete, pop it from the queue and move to next
                            let mut shared = self.shared.lock();
                            shared.files.pop_front();
                            drop(shared); // Release lock

                            // Clear current file and continue loop to get next file
                            self.current_file = None;
                            continue;
                        } else {
                            // Stream exhausted but writer not finished - unexpected
                            // This shouldn't happen with proper coordination
                            return Poll::Ready(None);
                        }
                    }
                    Poll::Pending => {
                        // File not ready yet (waiting for writer)
                        // Register waker so we get notified when writer adds more batches
                        let mut shared = self.shared.lock();
                        shared.register_waker(cx.waker().clone());
                        return Poll::Pending;
                    }
                }
            }

            // No current file, need to get the next one
            let mut shared = self.shared.lock();

            // Peek at the front of the queue (don't pop yet)
            if let Some(file_shared) = shared.files.front() {
                // Create a SpillFile from the shared state
                let spill_manager = Arc::clone(&shared.spill_manager);
                let file_shared = Arc::clone(file_shared);
                drop(shared); // Release lock before creating SpillFile

                self.current_file = Some(SpillFile {
                    shared: file_shared,
                    reader: None,
                    spill_manager,
                });

                // Continue loop to poll the new file
                continue;
            }

            // No files in queue - check if writer is done
            if shared.writer_dropped {
                // Writer is done and no more files will be added - EOF
                return Poll::Ready(None);
            }

            // Writer still active, register waker that will get notified when new files are added
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
    use datafusion_common_runtime::SpawnedTask;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use futures::StreamExt;

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
    ) -> (SpillPoolWriter, SendableRecordBatchStream) {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = create_test_schema();
        let spill_manager = Arc::new(SpillManager::new(env, metrics, schema));

        channel(max_file_size, spill_manager)
    }

    fn create_spill_channel_with_metrics(
        max_file_size: usize,
    ) -> (SpillPoolWriter, SendableRecordBatchStream, SpillMetrics) {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = create_test_schema();
        let spill_manager = Arc::new(SpillManager::new(env, metrics.clone(), schema));

        let (writer, reader) = channel(max_file_size, spill_manager);
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
            tokio::time::timeout(std::time::Duration::from_millis(100), reader.next())
                .await;

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
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
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

        let (writer, mut reader) = channel(1024 * 1024, spill_manager);

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

        let (writer, mut reader) = channel(batch_size, spill_manager);

        // Step 3: Write NUM_BATCHES batches to create approximately NUM_BATCHES files
        for i in 0..NUM_BATCHES {
            let start = (i * ROWS_PER_BATCH) as i32;
            writer.push_batch(&create_test_batch(start, ROWS_PER_BATCH))?;
        }

        // Check how many files were created (should be at least a few due to file rotation)
        let file_count = metrics.spill_file_count.value();
        assert_eq!(
            file_count,
            NUM_BATCHES - 1,
            "Expected at {} files with rotation, got {file_count}",
            NUM_BATCHES - 1
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
}
